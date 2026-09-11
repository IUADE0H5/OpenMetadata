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
"""A statement repeated across usage rows is parsed once; per-row attributes still come from each row."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from metadata.config.common import ConfigModel
from metadata.generated.schema.type.tableQuery import TableQueries, TableQuery
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.processor import query_parser
from metadata.ingestion.processor.query_parser import QueryParserProcessor

JOIN_SQL = "SELECT a.id FROM sales.orders a JOIN sales.customers b ON a.customer_id = b.id"
OTHER_SQL = "SELECT id FROM staging.events"  # not a trivial select: it must reach the parser


def _row(sql, user, date="2026-09-01"):
    return TableQuery(
        query=sql,
        userName=user,
        analysisDate=datetime.fromisoformat(date),
        serviceName="svc",
        databaseName="db",
        databaseSchema="sales",
        duration=10,
    )


def _processor():
    return QueryParserProcessor(ConfigModel(), MagicMock(), "athena")


def test_repeated_statements_are_parsed_once_and_keep_per_row_attributes():
    processor = _processor()
    rows = [
        _row(JOIN_SQL, "alice"),
        _row(JOIN_SQL, "bob", "2026-09-02"),
        _row(OTHER_SQL, "alice"),
        _row(JOIN_SQL, "carol"),
    ]

    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        parsed = processor._run(TableQueries(queries=rows)).right.parsedData

    assert spy.call_count == 2
    assert [p.userName for p in parsed] == ["alice", "bob", "alice", "carol"]
    assert (
        sorted(parsed[0].tables)
        == sorted(parsed[1].tables)
        == sorted(parsed[3].tables)
        == ["sales.customers", "sales.orders"]
    )
    assert parsed[0].date != parsed[1].date
    assert parsed[2].tables == ["staging.events"]


def test_cache_hits_hand_out_independent_join_objects():
    processor = _processor()
    first, second = processor._run(TableQueries(queries=[_row(JOIN_SQL, "a"), _row(JOIN_SQL, "b")])).right.parsedData

    first.joins["sales.orders"].clear()

    assert second.joins["sales.orders"], "a cache hit must not share mutable joins with an earlier row"


def test_statements_without_tables_are_cached_as_misses_and_skipped():
    processor = _processor()

    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        result = processor._run(TableQueries(queries=[_row("SHOW TABLES", "a"), _row("SHOW TABLES", "b")]))

    assert spy.call_count == 1
    assert result.right.parsedData == []


def test_dialect_is_part_of_the_cache_key():
    processor = _processor()
    processor._run(TableQueries(queries=[_row(OTHER_SQL, "a")]))
    other = QueryParserProcessor(ConfigModel(), MagicMock(), "mysql")

    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        other._run(TableQueries(queries=[_row(OTHER_SQL, "a")]))

    assert spy.call_count == 1


def test_large_batches_are_parsed_in_worker_processes_with_identical_results():
    from metadata.ingestion.processor.query_parser import PARALLEL_PARSE_THRESHOLD, QueryParserProcessorConfig

    rows = [
        _row(f"SELECT c{i} FROM sales.t{i} JOIN sales.d{i} ON t{i}.k = d{i}.k", "u")
        for i in range(PARALLEL_PARSE_THRESHOLD + 5)
    ]
    serial = QueryParserProcessor(QueryParserProcessorConfig(processes=1), MagicMock(), "athena")
    parallel = QueryParserProcessor(QueryParserProcessorConfig(processes=2), MagicMock(), "athena")

    expected = serial._run(TableQueries(queries=rows)).right.parsedData
    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        actual = parallel._run(TableQueries(queries=rows)).right.parsedData

    assert spy.call_count == 0, "every statement was served from the prefilled cache"
    # clean_table_list is set-derived; its order differs between processes (per-process hash seeds)
    assert [sorted(p.tables) for p in actual] == [sorted(p.tables) for p in expected]
    assert [p.joins for p in actual] == [p.joins for p in expected]


def test_batches_larger_than_the_lru_do_not_reparse_in_the_main_process():
    from metadata.ingestion.processor.query_parser import PARALLEL_PARSE_THRESHOLD, QueryParserProcessorConfig

    rows = [_row(f"SELECT c{i} FROM sales.t{i}", "u") for i in range(PARALLEL_PARSE_THRESHOLD + 10)]
    processor = QueryParserProcessor(QueryParserProcessorConfig(processes=2), MagicMock(), "athena")
    processor._parse_cache = query_parser.LRUCache(maxsize=8)
    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        parsed = processor._run(TableQueries(queries=rows + rows)).right.parsedData

    assert spy.call_count == 0
    assert len(parsed) == 2 * len(rows)


def test_run_counters_expose_statements_parses_cache_hits_and_time():
    processor = _processor()
    rows = [_row(JOIN_SQL, "alice"), _row(JOIN_SQL, "bob"), _row(OTHER_SQL, "alice"), _row(JOIN_SQL, "carol")]

    processor._run(TableQueries(queries=rows))
    values = processor.metric_values()

    assert processor.stats.statements == 4
    assert processor.stats.distinct_statements == 2
    assert processor.stats.parses == 2
    assert processor.stats.cache_hits == 2
    assert processor.stats.batches == 1
    assert processor.stats.no_tables == 0
    assert processor.stats.failures == 0
    assert processor.stats.parse_seconds > 0
    assert values["parser_statements_per_second"] > 0
    assert set(values) == {
        "parser_statements",
        "parser_distinct_statements",
        "parser_parses",
        "parser_cache_hits",
        "parser_no_tables",
        "parser_failures",
        "parser_batches",
        "parser_seconds",
        "parser_statements_per_second",
    }


def test_run_counters_accumulate_across_batches_and_count_statements_without_tables():
    processor = _processor()

    processor._run(TableQueries(queries=[_row(JOIN_SQL, "alice")]))
    processor._run(TableQueries(queries=[_row(JOIN_SQL, "bob"), _row("SELECT 1", "bob")]))

    assert processor.stats.batches == 2
    assert processor.stats.statements == 3
    assert processor.stats.parses == 2  # JOIN_SQL once, SELECT 1 once
    assert processor.stats.cache_hits == 1
    assert processor.stats.no_tables == 1


def test_statement_shape_masks_literals_and_whitespace_but_not_identifiers():
    from metadata.ingestion.processor.query_parser import statement_shape

    a = "SELECT * FROM  sales.orders2 WHERE day = '2026-09-07' AND id = 42 AND v1_2 = 3.5"
    b = "SELECT * FROM sales.orders2\n WHERE day = 'x' AND id = 7 AND v1_2 = 8"

    assert (
        statement_shape(a) == statement_shape(b) == "SELECT * FROM sales.orders2 WHERE day = ? AND id = ? AND v1_2 = ?"
    )
    assert statement_shape("SELECT 'it''s' FROM t") == "SELECT ? FROM t"
    assert statement_shape("SELECT * FROM t WHERE x = 'a' AND y = 'b'") != statement_shape(
        "SELECT * FROM u WHERE x = 'a'"
    )


def test_statements_that_differ_only_in_literals_are_parsed_once():
    processor = _processor()
    rows = [
        _row("SELECT a.id FROM sales.orders a WHERE a.day = '2026-09-01' AND a.batch = 1", "alice"),
        _row("SELECT a.id FROM sales.orders a WHERE a.day = '2026-09-02' AND a.batch = 2", "bob"),
        _row("SELECT a.id FROM sales.orders a WHERE a.day = '2026-09-03' AND a.batch = 3", "carol"),
    ]
    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        parsed = processor._run(TableQueries(queries=rows)).right.parsedData

    assert spy.call_count == 1
    assert processor.stats.parses == 1 and processor.stats.cache_hits == 2
    assert processor.stats.distinct_statements == 1
    assert [p.sql for p in parsed] == [r.query for r in rows]  # each row keeps its own statement text
    assert all(sorted(p.tables) == ["sales.orders"] for p in parsed)


def test_trivial_selects_skip_the_parser_and_still_yield_their_table():
    from metadata.ingestion.processor.query_parser import trivial_select_table

    assert trivial_select_table('/* {"app": "x"} */ SELECT * FROM "raw_db"."events"') == "raw_db.events"
    assert trivial_select_table("select * from sales.orders;") == "sales.orders"
    assert trivial_select_table("SELECT * FROM orders") == "orders"
    assert trivial_select_table("SELECT * FROM sales.orders WHERE id = 1") is None
    assert trivial_select_table("SELECT a FROM sales.orders") is None

    processor = _processor()
    with patch.object(query_parser, "LineageParser", wraps=LineageParser) as spy:
        parsed = processor._run(
            TableQueries(queries=[_row('SELECT * FROM "sales"."orders"', "alice")])
        ).right.parsedData
    assert spy.call_count == 0
    assert parsed[0].tables == ["sales.orders"] and parsed[0].joins == {}


def test_the_worker_pool_is_created_once_and_shut_down_on_close():
    processor = _processor()
    assert processor._pool is None
    first = processor._workers()
    assert processor._workers() is first
    processor.close()
    assert processor._pool is None
