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
Two-phase batch lineage parsing (query_lineage_pool): a statement repeated across a chunk is parsed
once per shape across all cores, and every distinct table referenced by a chunk is resolved once each
before get_lineage_by_query builds edges.
"""

import uuid
from unittest.mock import patch

import pytest

from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.metadataIngestion.parserconfig.queryParserConfig import (
    QueryParserType,
)
from metadata.generated.schema.type.tableQuery import TableQuery
from metadata.ingestion.lineage import query_lineage_pool
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.lineage.query_lineage_pool import (
    ParsedLineage,
    QueryLineagePool,
    resolve_batch_tables,
)
from metadata.ingestion.lineage.sql_lineage import (
    database_service_type_cache,
    get_lineage_by_query,
    search_cache,
)

MERGE_SQL = (
    "MERGE INTO target_db.target_schema.target_tbl t "
    "USING source_db.source_schema.source_tbl s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET t.val = s.val "
    "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)"
)
JOIN_INSERT_SQL = (
    "INSERT INTO sales.summary SELECT o.id, c.name FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id"
)
UNPARSEABLE_SQL = "!!! not sql at all ???"


def _row(sql, service="svc"):
    return TableQuery(query=sql, serviceName=service, databaseName="db", databaseSchema="schema")


@pytest.fixture(autouse=True)
def _isolate_search_caches():
    """search_table_entities's cache (and the database-service-type cache it depends on) is a module
    global shared with production code - tests that care about lookup counts must not see another
    test's entries, and must not leave their own behind."""
    search_cache.clear()
    database_service_type_cache.clear()
    yield
    search_cache.clear()
    database_service_type_cache.clear()


class _FakeService:
    """Just enough shape for get_database_service_type to read a connection type off it, so that
    lookup succeeds and caches on its first call instead of failing (and therefore never caching,
    and being retried) on every table resolution."""

    class name:  # noqa: N801
        root = "svc"

    class connection:  # noqa: N801
        class config:  # noqa: N801
            class type:  # noqa: N801
                value = "Athena"


def _table_entity(fqn: str, columns=("id", "name")) -> Table:
    return Table(
        id=uuid.uuid4(),
        name=fqn.rsplit(".", maxsplit=1)[-1],
        fullyQualifiedName=fqn,
        columns=[{"name": c, "dataType": "STRING", "fullyQualifiedName": f"{fqn}.{c}"} for c in columns],
    )


class _CountingMetadata:
    """A fake OMeta client that resolves each Table entity_type lookup to a real (minimal) Table
    entity keyed by the table name embedded in the FQN search string - real entities, so
    get_lineage_by_query can build a genuine edge and not just avoid crashing - while counting real
    ES lookups for Table entities, the ones search_table_entities itself performs. DatabaseService
    lookups (get_database_service_type, used internally to normalize params per connection type) are
    answered but not counted: an unrelated implementation detail, cached after its first call
    regardless of which table is being resolved."""

    def __init__(self, tables_by_name: dict = None):  # noqa: RUF013
        self.tables_by_name = tables_by_name or {}
        self.es_search_from_fqn_calls = []

    def es_search_from_fqn(self, entity_type, fqn_search_string):
        if entity_type is DatabaseService:
            return [_FakeService()]
        self.es_search_from_fqn_calls.append((entity_type, fqn_search_string))
        table_name = fqn_search_string.split(".")[-1].strip('"')
        entity = self.tables_by_name.get(table_name)
        # Any truthy, non-empty result short-circuits search_table_entities before it falls back to
        # fqn.build/get_by_name, which this fake does not implement.
        return [entity] if entity else [object()]


def test_parse_batch_round_trips_a_merge_and_a_join_statement_with_column_lineage():
    pool = QueryLineagePool(processes=1)
    merge_parsed, join_parsed = pool.parse_batch(
        [_row(MERGE_SQL), _row(JOIN_INSERT_SQL)], Dialect.ANSI, QueryParserType.Auto
    )

    assert not merge_parsed.parse_failed
    assert merge_parsed.source_tables == ("source_db.source_schema.source_tbl",)
    assert merge_parsed.target_tables == ("target_db.target_schema.target_tbl",)
    assert merge_parsed.column_lineage_map == {
        "target_db.target_schema.target_tbl": {"source_db.source_schema.source_tbl": [("id", "id"), ("val", "val")]}
    }

    assert not join_parsed.parse_failed
    assert sorted(join_parsed.source_tables) == ["sales.customers", "sales.orders"]
    assert join_parsed.target_tables == ("sales.summary",)
    assert join_parsed.column_lineage_map == {
        "sales.summary": {"sales.orders": [("id", "id")], "sales.customers": [("name", "name")]}
    }

    assert pool.stats.statements == 2
    assert pool.stats.parses == 2
    assert pool.stats.parse_failures == 0


def test_parse_batch_dedupes_statements_that_differ_only_in_literals():
    pool = QueryLineagePool(processes=1)
    rows = [
        _row("INSERT INTO sales.summary SELECT id FROM sales.orders WHERE day = '2026-09-01'"),
        _row("INSERT INTO sales.summary SELECT id FROM sales.orders WHERE day = '2026-09-02'"),
        _row("INSERT INTO sales.summary SELECT id FROM sales.orders WHERE day = '2026-09-03'"),
    ]

    with patch.object(query_lineage_pool, "LineageParser", wraps=LineageParser) as spy:
        results = pool.parse_batch(rows, Dialect.ANSI, QueryParserType.Auto)

    assert spy.call_count == 1
    assert pool.stats.statements == 3
    assert pool.stats.distinct_shapes == 1
    assert pool.stats.parses == 1
    assert all(parsed.target_tables == ("sales.summary",) for parsed in results)


def test_parse_batch_persists_the_shape_cache_across_calls():
    pool = QueryLineagePool(processes=1)
    pool.parse_batch([_row(JOIN_INSERT_SQL)], Dialect.ANSI, QueryParserType.Auto)

    with patch.object(query_lineage_pool, "LineageParser", wraps=LineageParser) as spy:
        pool.parse_batch([_row(JOIN_INSERT_SQL)], Dialect.ANSI, QueryParserType.Auto)

    assert spy.call_count == 0, "the second batch must be served entirely from the cross-batch cache"
    assert pool.stats.batches == 2
    assert pool.stats.statements == 2
    assert pool.stats.parses == 1


def test_parse_batch_isolates_a_failing_statement_without_raising():
    pool = QueryLineagePool(processes=1)
    rows = [_row(UNPARSEABLE_SQL), _row(JOIN_INSERT_SQL)]

    bad, good = pool.parse_batch(rows, Dialect.ANSI, QueryParserType.Auto)

    assert bad.parse_failed is True
    assert bad.failure_reason
    assert bad.source_tables == () and bad.target_tables == ()
    assert good.parse_failed is False
    assert good.target_tables == ("sales.summary",)
    assert pool.stats.statements == 2
    assert pool.stats.parses == 2
    assert pool.stats.parse_failures == 1


def test_resolve_batch_tables_resolves_each_distinct_table_once():
    """The core Phase 2 guarantee: N statements referencing the same 3 tables cost exactly 3 server
    lookups, and a second batch over the same tables costs none - the tables come from search_table_
    entities's own cache, already warmed by the first call."""
    metadata = _CountingMetadata()
    stats = query_lineage_pool.LineageParseStats()

    # 3 distinct shapes referencing exactly 3 distinct tables between them
    shapes = [
        ParsedLineage(
            masked_query="q1", query_hash="h1", source_tables=("db.sales.orders",), target_tables=("db.sales.summary",)
        ),
        ParsedLineage(
            masked_query="q2",
            query_hash="h2",
            source_tables=("db.sales.customers",),
            target_tables=("db.sales.summary",),
        ),
        ParsedLineage(
            masked_query="q3",
            query_hash="h3",
            source_tables=("db.sales.orders", "db.sales.customers"),
            target_tables=("db.sales.summary",),
        ),
    ]
    # 9 statements (3 repeats of each shape) - the repetition is the point: real audit logs reissue
    # the same handful of write statements thousands of times.
    parsed_items = [(_row(f"stmt {i}"), shapes[i % 3]) for i in range(9)]

    resolve_batch_tables(metadata, parsed_items, False, [], stats)

    assert stats.distinct_tables == 3
    assert stats.table_lookups == 3
    assert stats.table_cache_hits == 0
    assert len(metadata.es_search_from_fqn_calls) == 3

    # A second batch over the same 3 tables: every reference is now a cache hit, no new server calls.
    resolve_batch_tables(metadata, parsed_items, False, [], stats)

    assert stats.distinct_tables == 6  # cumulative across calls, like the other stat counters
    assert stats.table_lookups == 3  # unchanged
    assert stats.table_cache_hits == 3
    assert len(metadata.es_search_from_fqn_calls) == 3  # unchanged


def test_resolve_batch_tables_then_get_lineage_by_query_makes_no_further_server_calls():
    """End to end: after Phase 2 warms the cache, get_lineage_by_query's own table resolution
    (search_table_entities, called from get_table_entities_from_query) must not touch the network."""
    metadata = _CountingMetadata(
        tables_by_name={
            "orders": _table_entity("svc.db.sales.orders", columns=("id",)),
            "customers": _table_entity("svc.db.sales.customers", columns=("name",)),
            "summary": _table_entity("svc.db.sales.summary", columns=("id", "name")),
        }
    )
    stats = query_lineage_pool.LineageParseStats()
    parsed = ParsedLineage(
        masked_query=JOIN_INSERT_SQL,
        query_hash="h",
        source_tables=("sales.orders", "sales.customers"),
        target_tables=("sales.summary",),
        column_lineage_map={"sales.summary": {"sales.orders": [("id", "id")], "sales.customers": [("name", "name")]}},
    )
    table_query = _row(JOIN_INSERT_SQL)

    resolve_batch_tables(metadata, [(table_query, parsed)], False, [], stats)
    calls_after_resolve = len(metadata.es_search_from_fqn_calls)
    assert calls_after_resolve == 3  # orders, customers, summary

    lineages = list(
        get_lineage_by_query(
            metadata,
            query=table_query.query,
            service_names=[table_query.serviceName],
            database_name=table_query.databaseName,
            schema_name=table_query.databaseSchema,
            dialect=Dialect.ANSI,
            lineage_parser=parsed,
        )
    )

    assert len(metadata.es_search_from_fqn_calls) == calls_after_resolve  # no further server calls
    assert any(lineage.right for lineage in lineages)


def test_metrics_dict_shape():
    stats = query_lineage_pool.LineageParseStats(
        statements=10,
        distinct_shapes=2,
        parses=2,
        parse_failures=1,
        parse_seconds=1.5,
        distinct_tables=3,
        table_lookups=3,
        table_cache_hits=27,
        requests=15,
    )

    values = stats.as_metrics()

    assert set(values) == {
        "lineage_statements",
        "lineage_distinct_shapes",
        "lineage_parses",
        "lineage_parse_failures",
        "lineage_parse_seconds",
        "lineage_distinct_tables",
        "lineage_table_lookups",
        "lineage_table_cache_hits",
        "lineage_requests",
        "lineage_statements_per_second",
    }
    assert values["lineage_statements_per_second"] == round(10 / 1.5, 3)


def test_the_worker_pool_is_created_once_and_shut_down_on_close():
    pool = QueryLineagePool()
    assert pool._pool is None
    first = pool._workers()
    assert pool._workers() is first
    pool.close()
    assert pool._pool is None


def test_a_degraded_parse_still_ships_the_tables_sqlparse_found(monkeypatch):
    """sqlglot and sqlfluff give up on Athena's MERGE syntax; the in-process path still builds edges
    from the sqlparse fallback, so the worker must not report such a statement as failed."""
    from types import SimpleNamespace

    from metadata.ingestion.lineage import query_lineage_pool as module

    fake = SimpleNamespace(
        query_parsing_success=False,
        query_parsing_failure_reason="Query parsing with SqlFluff failed",
        masked_query="MERGE INTO s.t USING s.u ON ?",
        query_hash="h1",
        source_tables=["s.u"],
        target_tables=["s.t"],
        intermediate_tables=[],
        column_lineage=[],
    )
    monkeypatch.setattr(module, "LineageParser", lambda *a, **k: fake)

    parsed = module._parse_lineage_in_worker(
        ("MERGE INTO s.t USING s.u ON 1=1 WHEN MATCHED THEN UPDATE SET a=1", "athena", "Auto")
    )

    assert parsed.parse_failed is False
    assert parsed.source_tables == ("s.u",) and parsed.target_tables == ("s.t",)
    assert parsed.failure_reason == "Query parsing with SqlFluff failed"
    assert parsed.column_lineage_map == {}


def test_a_parse_with_nothing_found_is_reported_failed(monkeypatch):
    from types import SimpleNamespace

    from metadata.ingestion.lineage import query_lineage_pool as module

    fake = SimpleNamespace(
        query_parsing_success=False,
        query_parsing_failure_reason="unparsable",
        masked_query=None,
        query_hash="h2",
        source_tables=[],
        target_tables=[],
        intermediate_tables=[],
        column_lineage=[],
    )
    monkeypatch.setattr(module, "LineageParser", lambda *a, **k: fake)

    parsed = module._parse_lineage_in_worker(("garbage", "athena", "Auto"))

    assert parsed.parse_failed is True and parsed.failure_reason == "unparsable"


def test_unnest_is_not_a_udf_source_but_a_real_function_is(monkeypatch):
    from types import SimpleNamespace

    from metadata.ingestion.lineage import query_lineage_pool as module

    unnest = module.DataFunction("<default>.unnest")
    udf = module.DataFunction("<default>.my_table_function")
    fake = SimpleNamespace(
        query_parsing_success=True,
        query_parsing_failure_reason=None,
        masked_query="q",
        query_hash="h3",
        source_tables=[unnest, "s.u"],
        target_tables=["s.t"],
        intermediate_tables=[],
        column_lineage=[],
    )
    monkeypatch.setattr(module, "LineageParser", lambda *a, **k: fake)
    parsed = module._parse_lineage_in_worker(("q", "athena", "Auto"))
    assert parsed.has_udf_source is False and parsed.source_tables == ("s.u",)

    fake.source_tables = [udf, "s.u"]
    parsed = module._parse_lineage_in_worker(("q", "athena", "Auto"))
    assert parsed.has_udf_source is True and str(udf) in parsed.source_tables
