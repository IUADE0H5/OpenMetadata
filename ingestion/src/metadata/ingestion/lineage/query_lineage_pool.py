#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Two-phase batch lineage parsing for LineageSource.query_lineage_processor.

Phase 1 (this module's ``QueryLineagePool``): every statement in a chunk is parsed once per distinct
*shape* (literals masked - a MERGE re-issued with other partition values yields the same edges), in a
persistent process pool, because parsing is CPU-bound and single-threaded under the GIL while
``generate_lineage_with_processes`` fans a lineage run out across threads, not processes. Workers
cannot return a real ``LineageParser`` (its ``LineageRunner`` is not reliably picklable), so they
return ``ParsedLineage``: a plain dataclass carrying exactly what
``metadata.ingestion.lineage.sql_lineage.get_lineage_by_query`` reads off a ``LineageParser``.

Phase 2 (``resolve_batch_tables``): before ``get_lineage_by_query`` runs per statement, every distinct
table referenced anywhere in the batch is resolved once each against the catalog, concurrently
(I/O-bound), through the same cache ``get_lineage_by_query`` will hit -
``metadata.ingestion.lineage.sql_lineage.search_table_entities``'s own ``search_cache``. This module
adds no second cache; it only warms that one ahead of time and counts what it already tracks (see
``_already_cached``).
"""

import time
import traceback
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, List, Optional, Set, Tuple  # noqa: UP035

from collate_sqllineage.core.models import DataFunction

from metadata.generated.schema.metadataIngestion.parserconfig.queryParserConfig import (
    QueryParserType,
)
from metadata.generated.schema.type.tableQuery import TableQuery
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.lineage.sql_lineage import (
    get_table_fqn_from_query_name,
    normalize_table_params_by_service,
    populate_column_lineage_map,
    search_cache,
    search_table_entities,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.processor.query_parser import (
    DEFAULT_PARSE_PROCESSES,
    PARALLEL_PARSE_THRESHOLD,
    PARSE_CACHE_SIZE,
    statement_shape,
)
from metadata.utils.logger import ingestion_logger
from metadata.utils.lru_cache import LRUCache

logger = ingestion_logger()

# Phase 2 is I/O-bound (ES + API round trips), so a fresh thread pool per chunk is cheap enough not
# to need the persistent-pool treatment Phase 1 gets; this just bounds how many of a chunk's distinct
# tables are resolved at once.
DEFAULT_RESOLVE_WORKERS = 8


@dataclass(frozen=True)
class ParsedLineage:
    """Picklable stand-in for a ``LineageParser``, carrying exactly what ``get_lineage_by_query`` (and
    the helpers it calls) read off one: ``masked_query``, ``query_hash``, the source/target/
    intermediate table names, and the column lineage map.

    Table references are plain ``str(table)`` - the only operations ``get_lineage_by_query`` performs
    on a source/target/intermediate table are ``str()`` and, for source tables,
    ``isinstance(_, DataFunction)`` (to route table-valued UDF calls through their own ES-backed
    resolution). A worker cannot ship a real ``DataFunction`` and have that check still pass on the far
    side of a pickle boundary, so a statement whose source tables include one is flagged via
    ``has_udf_source`` instead: the caller re-parses it with a real ``LineageParser`` rather than using
    this stand-in, keeping UDF lineage exactly as accurate as before at the cost of losing the batch
    speedup for that one (rare) statement.

    ``column_lineage_map`` is the output of ``populate_column_lineage_map()``, computed in the worker
    against the real sqllineage ``Column`` objects (their ``_parent``/``.parent`` internals are not
    something worth reconstructing across a pickle boundary) - already a plain ``{str: {str: [(str,
    str), ...]}}`` dict, so it ships as-is. See ``sql_lineage.get_lineage_by_query`` for the one-branch
    change that reads this instead of calling ``populate_column_lineage_map`` itself when a
    ``ParsedLineage`` is passed in.
    """

    masked_query: str
    query_hash: str
    source_tables: Tuple[str, ...] = ()  # noqa: UP006
    target_tables: Tuple[str, ...] = ()  # noqa: UP006
    intermediate_tables: Tuple[str, ...] = ()  # noqa: UP006
    column_lineage_map: dict = field(default_factory=dict)
    has_udf_source: bool = False
    parse_failed: bool = False
    failure_reason: Optional[str] = None  # noqa: UP045


def _shape_key(query: str, dialect: Dialect, parser_type: QueryParserType) -> tuple:
    return (statement_shape(query), dialect.value, getattr(parser_type, "value", parser_type))


def _parse_lineage_in_worker(args: Tuple[str, str, str]) -> ParsedLineage:  # noqa: UP006
    """Runs in a worker process: build a real ``LineageParser`` and reduce it to a picklable
    ``ParsedLineage``. Dialect/parser type cross the process boundary as their ``.value`` rather than
    the enum instance, matching ``query_parser._parse_in_worker`` - safer under `spawn`, where the
    worker re-imports the enum module fresh rather than sharing the parent's instances."""
    query, dialect_value, parser_type_value = args
    dialect = Dialect(dialect_value)
    parser_type = QueryParserType(parser_type_value)
    try:
        parser = LineageParser(query, dialect=dialect, parser_type=parser_type)
    except Exception as exc:
        # LineageParser catches its own parser errors internally (query_parsing_success=False) and
        # essentially never raises; this branch only guards against something unexpected escaping it.
        logger.debug(traceback.format_exc())
        return ParsedLineage(
            masked_query=query,
            query_hash=LineageParser.get_query_hash(query),
            parse_failed=True,
            failure_reason=str(exc),
        )
    sources = [table for table in parser.source_tables if not _is_builtin_table_function(table)]
    targets = list(parser.target_tables)
    intermediates = list(parser.intermediate_tables)
    if not parser.query_parsing_success and not (sources or targets):
        return ParsedLineage(
            masked_query=parser.masked_query or query,
            query_hash=parser.query_hash,
            parse_failed=True,
            failure_reason=parser.query_parsing_failure_reason,
        )
    # `query_parsing_success` false with tables present is the sqlparse fallback: sqlglot and
    # sqlfluff gave up (Athena's MERGE syntax, for one) and the in-process path still builds edges
    # from what sqlparse found. Shipping those tables keeps the pool at parity with that path; the
    # reason is kept for the debug capture, which reports such statements as degraded parses.
    return ParsedLineage(
        # A query that clean_raw_query() filters out entirely (e.g. CREATE TRIGGER) leaves
        # LineageParser.parser (and therefore masked_query) None while query_parsing_success stays
        # True - fall back to the raw query text so this field is never None despite success.
        masked_query=parser.masked_query or query,
        query_hash=parser.query_hash,
        source_tables=tuple(str(table) for table in sources),
        target_tables=tuple(str(table) for table in targets),
        intermediate_tables=tuple(str(table) for table in intermediates),
        column_lineage_map=populate_column_lineage_map(parser.column_lineage) if parser.query_parsing_success else {},
        has_udf_source=any(isinstance(table, DataFunction) for table in sources),
        failure_reason=None if parser.query_parsing_success else parser.query_parsing_failure_reason,
    )


_BUILTIN_TABLE_FUNCTIONS = frozenset({"unnest", "lateral", "table"})


def _is_builtin_table_function(table: Any) -> bool:
    """`UNNEST(...)` and friends surface as `DataFunction` sources. They are SQL built-ins, never a
    stored procedure the catalog could resolve, so treating them as UDFs would only force every
    statement that expands an array back onto the slow in-process path for nothing."""
    if not isinstance(table, DataFunction):
        return False
    name = str(table).rsplit(".", 1)[-1].strip('"').lower()
    return name in _BUILTIN_TABLE_FUNCTIONS


@dataclass
class LineageParseStats:
    """What the two-phase lineage pipeline did in this run, exposed for metrics: parsing (how many
    statements, how many were distinct shapes actually parsed, how long) and table resolution (how
    many distinct tables, how many needed a real server call vs. were already warm), plus how many
    lineage edge requests were actually built. Picked up automatically by any metrics reporter that
    calls a step's ``metric_values()`` - see ``LineageSource.metric_values``."""

    statements: int = 0
    distinct_shapes: int = 0
    parses: int = 0
    parse_failures: int = 0
    parse_seconds: float = 0.0
    distinct_tables: int = 0
    table_lookups: int = 0
    table_cache_hits: int = 0
    requests: int = 0
    batches: int = 0

    @property
    def statements_per_second(self) -> float:
        return round(self.statements / self.parse_seconds, 3) if self.parse_seconds > 0 else 0.0

    def as_metrics(self) -> dict:
        return {
            "lineage_statements": self.statements,
            "lineage_distinct_shapes": self.distinct_shapes,
            "lineage_parses": self.parses,
            "lineage_parse_failures": self.parse_failures,
            "lineage_parse_seconds": round(self.parse_seconds, 3),
            "lineage_distinct_tables": self.distinct_tables,
            "lineage_table_lookups": self.table_lookups,
            "lineage_table_cache_hits": self.table_cache_hits,
            "lineage_requests": self.requests,
            "lineage_statements_per_second": self.statements_per_second,
        }


class QueryLineagePool:
    """Owns the persistent process pool and the cross-batch shape cache for Phase 1. One instance is
    meant to live for a whole run (see ``get_default_pool``); tests are free to construct their own to
    keep cache state isolated.
    """

    def __init__(
        self,
        processes: int = DEFAULT_PARSE_PROCESSES,
        cache_size: int = PARSE_CACHE_SIZE,
        parallel_threshold: int = PARALLEL_PARSE_THRESHOLD,
    ):
        self.processes = max(1, processes)
        self.parallel_threshold = parallel_threshold
        self._cache: LRUCache = LRUCache(cache_size)
        self._pool: Optional[ProcessPoolExecutor] = None  # noqa: UP045
        self.stats = LineageParseStats()

    def _workers(self) -> ProcessPoolExecutor:
        """The pool lives as long as the run: spawning workers costs seconds (each imports the whole
        framework), and a lineage run hands this pool one chunk after another."""
        if self._pool is None:
            self._pool = ProcessPoolExecutor(max_workers=self.processes)
        return self._pool

    def parse_batch(
        self,
        table_queries: List[TableQuery],  # noqa: UP006
        dialect: Dialect,
        parser_type: QueryParserType,
    ) -> List[Optional[ParsedLineage]]:  # noqa: UP006, UP045
        """Parse a chunk's statements, deduplicated by shape against the cross-batch LRU. Returns one
        entry per input, aligned by index; a failed parse is still returned (``parse_failed=True``),
        never raised."""
        if not table_queries:
            return []

        self.stats.batches += 1
        self.stats.statements += len(table_queries)
        keys = [_shape_key(table_query.query, dialect, parser_type) for table_query in table_queries]
        self.stats.distinct_shapes += len(set(keys))

        to_parse: "OrderedDict[tuple, str]" = OrderedDict()  # noqa: UP037
        for key, table_query in zip(keys, table_queries, strict=True):
            if key not in self._cache and key not in to_parse:
                to_parse[key] = table_query.query

        if to_parse:
            started = time.perf_counter()
            jobs = [(query, key[1], key[2]) for key, query in to_parse.items()]
            if len(to_parse) >= self.parallel_threshold and self.processes > 1:
                logger.info(
                    f"Parsing {len(to_parse)} distinct lineage statement shapes with {self.processes} processes"
                )
                parsed_results = list(self._workers().map(_parse_lineage_in_worker, jobs, chunksize=8))
            else:
                parsed_results = [_parse_lineage_in_worker(job) for job in jobs]
            self.stats.parse_seconds += time.perf_counter() - started

            for key, parsed in zip(to_parse.keys(), parsed_results, strict=True):
                self._cache.put(key, parsed)
                self.stats.parses += 1
                if parsed.parse_failed:
                    self.stats.parse_failures += 1

        return [self._cache.get(key) for key in keys]

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None


def _already_cached(
    metadata: OpenMetadata,
    service_names: List[str],  # noqa: UP006
    database: Optional[str],  # noqa: UP045
    database_schema: Optional[str],  # noqa: UP045
    table: str,
) -> bool:
    """Peek at ``search_table_entities``'s own cache the same way it checks it internally, without
    performing a lookup - used only to split ``resolve_batch_tables``'s work into the
    ``lineage_table_lookups`` (real server calls) and ``lineage_table_cache_hits`` metrics. The cache
    itself, and the decision to also cache a miss, lives entirely in ``search_table_entities``; this
    does not duplicate that, only reads it."""
    for service_name in service_names:
        normalized_db, normalized_schema = normalize_table_params_by_service(
            metadata, service_name, database, database_schema
        )
        if (service_name, normalized_db, normalized_schema, table) in search_cache:
            return True
    return False


def resolve_batch_tables(
    metadata: OpenMetadata,
    parsed_items: List[Tuple[TableQuery, Optional[ParsedLineage]]],  # noqa: UP006, UP045
    process_cross_database_lineage: bool,
    cross_database_service_names: List[str],  # noqa: UP006
    stats: LineageParseStats,
    max_workers: int = DEFAULT_RESOLVE_WORKERS,
) -> None:
    """Resolve every distinct table referenced by ``parsed_items`` against the catalog, once each,
    concurrently (I/O-bound: ES + API calls). After this call, ``get_lineage_by_query`` for these same
    statements resolves every table from ``search_table_entities``'s warm cache with no further server
    round trips - the same tables recur across statements (a handful of write targets, repeated
    thousands of times), so this turns O(statements x tables per statement) server calls into
    O(distinct tables).
    """
    seen: Set[tuple] = set()  # noqa: UP006
    work: List[Tuple[List[str], Optional[str], Optional[str], str]] = []  # noqa: UP006, UP045

    for table_query, parsed in parsed_items:
        if parsed is None or parsed.parse_failed:
            continue
        service_names = [table_query.serviceName]
        if process_cross_database_lineage and cross_database_service_names:
            service_names.extend(cross_database_service_names)

        raw_tables = (*parsed.source_tables, *parsed.target_tables, *parsed.intermediate_tables)
        for raw_table in raw_tables:
            database_query, schema_query, table = get_table_fqn_from_query_name(raw_table)
            if not table:
                continue
            database = database_query or table_query.databaseName
            schema = schema_query or table_query.databaseSchema
            key = (tuple(service_names), database, schema, table)
            if key in seen:
                continue
            seen.add(key)
            work.append((service_names, database, schema, table))

    stats.distinct_tables += len(work)
    if not work:
        return

    def _resolve(item: Tuple[List[str], Optional[str], Optional[str], str]) -> bool:  # noqa: UP006, UP045
        service_names, database, schema, table = item
        cached = _already_cached(metadata, service_names, database, schema, table)
        search_table_entities(metadata, service_names, database, schema, table)
        return cached

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for cached in pool.map(_resolve, work):
            if cached:
                stats.table_cache_hits += 1
            else:
                stats.table_lookups += 1


_default_pool: Optional[QueryLineagePool] = None  # noqa: UP045


def get_default_pool() -> QueryLineagePool:
    """The pool ``lineage_processors.query_lineage_processor`` uses: one process pool and one shape
    cache shared across every chunk of a run, since chunks are handed to threads within a single
    process (``LineageSource.generate_lineage_with_processes`` - multiprocessing is disabled there,
    see its docstring), so parsing a shape once benefits every later chunk that repeats it.

    Deliberately module-level, like ``sql_lineage.search_cache``: ``query_lineage_processor``'s
    signature is a public seam that downstream packages patch by swapping the module attribute
    wholesale (a debug-capture processor that mirrors the stock one, for instance);
    adding a parameter here to thread an instance through would break that patch, since the positional
    args tuple built in ``lineage_source.py`` is shared by both the stock and the patched function.
    """
    global _default_pool  # noqa: PLW0603
    if _default_pool is None:
        _default_pool = QueryLineagePool()
    return _default_pool


_last_stats: Optional[LineageParseStats] = None  # noqa: UP045


def default_pool_stats() -> LineageParseStats:
    """The run's counters, also after `close_default_pool()`: the metrics reporter's final push
    happens in the workflow's own shutdown, after the source closed the pool, and must not read a
    fresh, empty pool."""
    if _default_pool is not None:
        return _default_pool.stats
    return _last_stats if _last_stats is not None else LineageParseStats()


def close_default_pool() -> None:
    global _default_pool, _last_stats  # noqa: PLW0603
    if _default_pool is not None:
        _last_stats = _default_pool.stats
        _default_pool.close()
        _default_pool = None
