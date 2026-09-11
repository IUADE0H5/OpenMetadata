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
Query parser implementation
"""

import datetime
import os
import re
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple  # noqa: UP035

from cachetools import LRUCache
from pydantic import Field

from metadata.config.common import ConfigModel
from metadata.generated.schema.metadataIngestion.parserconfig.queryParserConfig import (
    QueryParserType,
)
from metadata.generated.schema.type.basic import DateTime
from metadata.generated.schema.type.queryParserData import ParsedData, QueryParserData
from metadata.generated.schema.type.tableQuery import TableQueries, TableQuery
from metadata.generated.schema.type.tableUsageCount import TableColumnJoin
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Processor
from metadata.ingestion.lineage.models import ConnectionTypeDialectMapper, Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.helpers import TRANSIENT_TABLE_PATTERNS, is_transient_table_name
from metadata.utils.logger import ingestion_logger
from metadata.utils.time_utils import datetime_to_timestamp

logger = ingestion_logger()

# Distinct statements parsed per run are far fewer than rows (scheduled jobs repeat the same SQL every
# day), and parsing is the cost; the cache holds (tables, joins) per (query, dialect, parser type).
PARSE_CACHE_SIZE = 20_000  # shapes are small tuples; a day of the dev lake is ~1,700 shapes
# Parsing is CPU-bound (sqlglot / sqllineage), so distinct statements of a batch are parsed in worker
# processes; below this many uncached statements the pool start-up costs more than it saves.
PARALLEL_PARSE_THRESHOLD = 50
DEFAULT_PARSE_PROCESSES = max(1, min(8, os.cpu_count() or 1))


class QueryParserProcessorConfig(ConfigModel):
    processes: int = DEFAULT_PARSE_PROCESSES
    # Regexes, matched in full against the table component of a parsed reference, that name tables
    # which never exist in the catalog; added to the built-in `TRANSIENT_TABLE_PATTERNS`.
    transientTablePatterns: List[str] = Field(default_factory=list)  # noqa: N815, UP006

    @property
    def transient_table_patterns(self) -> Tuple[str, ...]:  # noqa: UP006
        return TRANSIENT_TABLE_PATTERNS + tuple(self.transientTablePatterns)


_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_NUMBER_LITERAL = re.compile(r"(?<![\w.])\d+(?:\.\d+)?(?![\w.])")
_WHITESPACE = re.compile(r"\s+")


def statement_shape(query: str) -> str:
    """The statement with its literals replaced by `?` and whitespace collapsed.

    The tables and joins a statement touches do not depend on its literals, and query logs are
    dominated by the same statement re-issued with other partition values, ids or dates: one day of
    the dev lake holds 21,140 statements, 8,079 distinct texts, but only 1,695 distinct shapes. Keying
    the parse cache on the shape turns most of a day into cache hits. Identifiers are untouched:
    quoted identifiers use double quotes, and numbers glued to a word (`table2`, `v1_2`) are kept.
    """
    shaped = _STRING_LITERAL.sub("?", query)
    shaped = _NUMBER_LITERAL.sub("?", shaped)
    return _WHITESPACE.sub(" ", shaped).strip()


def _parse_key(record: TableQuery, dialect: Dialect, parser_type: QueryParserType) -> tuple:
    return (statement_shape(record.query), dialect.value, getattr(parser_type, "value", parser_type))


def _drop_transient_tables(
    tables: List[str],  # noqa: UP006
    joins: Dict[str, List[TableColumnJoin]],  # noqa: UP006
    patterns: Tuple[str, ...],  # noqa: UP006
) -> Tuple[List[str], Dict[str, List[TableColumnJoin]]]:  # noqa: UP006
    """Remove transient tables from the table list and from both sides of the join map."""
    kept_tables = [table for table in tables if not is_transient_table_name(table, patterns)]
    kept_joins: Dict[str, List[TableColumnJoin]] = {}  # noqa: UP006
    for table, column_joins in joins.items():
        if is_transient_table_name(table, patterns):
            continue
        pruned = []
        for column_join in column_joins:
            joined_with = [c for c in column_join.joinedWith if not is_transient_table_name(c.table, patterns)]
            if joined_with:
                pruned.append(TableColumnJoin(tableColumn=column_join.tableColumn, joinedWith=joined_with))
        if pruned:
            kept_joins[table] = pruned
    return kept_tables, kept_joins


# `SELECT * FROM "db"."table"` (an optional leading comment, optional quotes, optional trailing
# semicolon) reads exactly one table and joins nothing; running a full SQL parser on it is pure
# cost. Query logs hold these by the thousand: synthesised access records, previews, probes.
_TRIVIAL_SELECT = re.compile(
    r"^\s*(?:/\*.*?\*/\s*|--[^\n]*\n\s*)*SELECT\s+\*\s+FROM\s+"
    r'(?:"?([A-Za-z0-9_]+)"?\s*\.\s*)?"?([A-Za-z0-9_]+)"?\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)


def trivial_select_table(query: str) -> Optional[str]:  # noqa: UP045
    """`schema.table` (or `table`) when the statement is a bare `SELECT * FROM` of one table, else None."""
    match = _TRIVIAL_SELECT.match(query)
    if not match:
        return None
    schema, table = match.group(1), match.group(2)
    return f"{schema}.{table}" if schema else table


def _parse_tables_and_joins(
    query: str,
    dialect: Dialect,
    parser_type: QueryParserType,
    transient_table_patterns: Tuple[str, ...] = TRANSIENT_TABLE_PATTERNS,  # noqa: UP006
) -> tuple:
    """(clean table list, joins) for the statement, or () when it involves no catalog table."""
    trivial = trivial_select_table(query)
    if trivial is not None:
        tables, joins = _drop_transient_tables([trivial], {}, transient_table_patterns)
        return (tables, joins) if tables else ()
    lineage_parser = LineageParser(query, dialect=dialect, parser_type=parser_type)
    if not lineage_parser.involved_tables:
        return ()
    tables, joins = _drop_transient_tables(
        lineage_parser.clean_table_list, lineage_parser.table_joins, transient_table_patterns
    )
    return (tables, joins) if tables else ()


@dataclass
class ParserStats:
    """What the parser step did in this run, exposed for metrics: how many statements it saw, how
    many it actually parsed (the rest were cache hits), and how long the parsing took. A run's
    throughput is invisible in the step record counts, which only say how many rows went through."""

    statements: int = 0
    distinct_statements: int = 0  # distinct within each batch, summed over batches (bounded)
    parses: int = 0
    cache_hits: int = 0
    no_tables: int = 0
    failures: int = 0
    batches: int = 0
    parse_seconds: float = 0.0

    @property
    def statements_per_second(self) -> float:
        return round(self.statements / self.parse_seconds, 3) if self.parse_seconds > 0 else 0.0

    def as_metrics(self) -> dict:
        return {
            "parser_statements": self.statements,
            "parser_distinct_statements": self.distinct_statements,
            "parser_parses": self.parses,
            "parser_cache_hits": self.cache_hits,
            "parser_no_tables": self.no_tables,
            "parser_failures": self.failures,
            "parser_batches": self.batches,
            "parser_seconds": round(self.parse_seconds, 3),
            "parser_statements_per_second": self.statements_per_second,
        }


class _LayeredCache:
    """Batch-local results first, then the bounded cross-batch LRU; writes go to both."""

    def __init__(self, batch: dict, lru: LRUCache):
        self._batch = batch
        self._lru = lru

    def get(self, key):
        parsed = self._batch.get(key)
        return parsed if parsed is not None else self._lru.get(key)

    def __setitem__(self, key, value) -> None:
        self._batch[key] = value
        self._lru[key] = value


def _parse_in_worker(args: tuple):
    query, dialect_value, parser_type_value, transient_table_patterns = args
    try:
        return _parse_tables_and_joins(
            query, Dialect(dialect_value), QueryParserType(parser_type_value), transient_table_patterns
        )
    except Exception:  # the caller re-parses inline to surface the error with its own handling
        return None


def parse_sql_statement(
    record: TableQuery,
    dialect: Dialect,
    parser_type: QueryParserType = QueryParserType.Auto,
    cache: Optional[LRUCache] = None,  # noqa: UP045
    transient_table_patterns: Tuple[str, ...] = TRANSIENT_TABLE_PATTERNS,  # noqa: UP006
    stats: Optional[ParserStats] = None,  # noqa: UP045
) -> Optional[ParsedData]:  # noqa: UP045
    """
    Use the lineage parser and work with the tokens
    to convert a RAW SQL statement into
    QueryParserData.
    :param record: TableQuery from usage
    :param dialect: dialect used to compute lineage
    :param cache: optional bounded cache of parse results, so a statement repeated across rows is parsed once
    :param transient_table_patterns: table references matching these are dropped from the result
    :param stats: optional run counters; cache hits, parses and parse time are accounted here
    :return: QueryParserData
    """
    start_time = record.analysisDate
    if isinstance(start_time, DateTime):
        start_date = start_time.root.date()
        start_time = datetime.datetime.strptime(str(start_date.isoformat()), "%Y-%m-%d")
    start_time = datetime_to_timestamp(start_time, milliseconds=True)

    key = _parse_key(record, dialect, parser_type)
    parsed = cache.get(key) if cache is not None else None
    if parsed is None:
        started = time.perf_counter()
        parsed = _parse_tables_and_joins(record.query, dialect, parser_type, transient_table_patterns)
        if stats is not None:
            stats.parses += 1
            stats.parse_seconds += time.perf_counter() - started
        if cache is not None:
            cache[key] = parsed
    elif stats is not None:
        stats.cache_hits += 1
    if not parsed:
        if stats is not None:
            stats.no_tables += 1
        return None
    tables, joins = parsed
    return ParsedData(
        tables=list(tables),
        joins=deepcopy(joins),
        databaseName=record.databaseName,
        databaseSchema=record.databaseSchema,
        sql=record.query,
        query_type=record.query_type,
        exclude_usage=record.exclude_usage,
        dialect=dialect.value,
        userName=record.userName,
        date=str(start_time),
        serviceName=record.serviceName,
        duration=record.duration,
        cost=record.cost,
    )


class QueryParserProcessor(Processor):
    """Extension of the `Processor` class"""

    config: ConfigModel

    def __init__(
        self,
        config: ConfigModel,
        metadata: OpenMetadata,
        connection_type: str,
    ):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.connection_type = connection_type
        self._parse_cache: LRUCache = LRUCache(maxsize=PARSE_CACHE_SIZE)
        self.stats = ParserStats()
        self._pool: Optional[ProcessPoolExecutor] = None  # noqa: UP045

    @property
    def name(self) -> str:
        return "Query Parser"

    def metric_values(self) -> dict:
        """Run counters for a metrics reporter; any step may expose this and be picked up generically."""
        return self.stats.as_metrics()

    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,  # noqa: UP045
        **kwargs,
    ):
        config = QueryParserProcessorConfig.model_validate(config_dict or {})
        connection_type = kwargs.pop("connection_type", "")
        return cls(config, metadata, connection_type)

    @property
    def processes(self) -> int:
        return getattr(self.config, "processes", DEFAULT_PARSE_PROCESSES)

    @property
    def transient_table_patterns(self) -> Tuple[str, ...]:  # noqa: UP006
        return getattr(self.config, "transient_table_patterns", TRANSIENT_TABLE_PATTERNS)

    def _prefill_cache(self, queries: List[TableQuery], dialect: Dialect) -> dict:  # noqa: UP006
        """Parse the batch's distinct, not-yet-cached statements in worker processes.

        Results go into a batch-local dict as well as the LRU: a day of query logs can hold more distinct
        statements than the LRU keeps, and evictions while the batch is still being walked would make the
        loop re-parse what the workers already did."""
        batch_cache: dict = {}
        keys: dict = {}  # shape key -> one representative statement to parse
        for query in queries:
            key = _parse_key(query, dialect, QueryParserType.Auto)
            if key not in self._parse_cache and key not in keys:
                keys[key] = query.query
        if len(keys) < PARALLEL_PARSE_THRESHOLD or self.processes <= 1:
            return batch_cache
        logger.info(f"Parsing {len(keys)} distinct statements with {self.processes} processes")
        jobs = [(keys[key], key[1], key[2], self.transient_table_patterns) for key in keys]
        started = time.perf_counter()
        for key, parsed in zip(keys, self._workers().map(_parse_in_worker, jobs, chunksize=16), strict=True):
            if parsed is not None:
                batch_cache[key] = parsed
                self._parse_cache[key] = parsed
                self.stats.parses += 1
        self.stats.parse_seconds += time.perf_counter() - started
        return batch_cache

    def _workers(self) -> ProcessPoolExecutor:
        """The pool lives as long as the step: spawning workers costs seconds (each imports the whole
        framework), and a usage run hands the parser one batch per scanned day."""
        if self._pool is None:
            self._pool = ProcessPoolExecutor(max_workers=self.processes)
        return self._pool

    def _run(self, record: TableQueries) -> Optional[Either[QueryParserData]]:  # noqa: UP045
        if record is None or record.queries is None:
            return None

        data = []
        success_cnt = 0
        failed_cnt = 0
        total_cnt = len(record.queries)
        dialect = ConnectionTypeDialectMapper.dialect_of(self.connection_type)
        self.stats.batches += 1
        self.stats.statements += total_cnt
        self.stats.distinct_statements += len({_parse_key(q, dialect, QueryParserType.Auto) for q in record.queries})
        batch_cache: dict = {}
        try:
            batch_cache = self._prefill_cache(record.queries, dialect)
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Parallel parsing unavailable, parsing inline: {exc}")
        cache = _LayeredCache(batch_cache, self._parse_cache)

        for table_query in record.queries:
            try:
                parsed_sql = parse_sql_statement(
                    table_query,
                    dialect,
                    cache=cache,
                    transient_table_patterns=self.transient_table_patterns,
                    stats=self.stats,
                )
                if parsed_sql:
                    data.append(parsed_sql)
                success_cnt += 1
            except Exception as exc:
                failed_cnt += 1
                self.stats.failures += 1
                logger.debug(traceback.format_exc())
                logger.warning(f"Error processing query [{table_query.query}]: {exc}")
            cur_total_cnt = success_cnt + failed_cnt
            if cur_total_cnt % 1000 == 0 or cur_total_cnt == total_cnt:
                logger.info(
                    f"Total query count:{cur_total_cnt} / {total_cnt}."
                    f" Current success count: {success_cnt}."
                    f" Current failed count: {failed_cnt}."
                )
        return Either(right=QueryParserData(parsedData=data))

    def close(self):
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None
