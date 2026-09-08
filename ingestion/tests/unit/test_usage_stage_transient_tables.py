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
Table references that never exist in a catalog (awswrangler CTAS staging tables, sqllineage's
`unnest` placeholder, Iceberg metadata tables) are dropped by the usage stage, so no usage,
query or join is attributed to them and the bulk sink never looks them up.
"""

import json
from unittest.mock import MagicMock

import pytest

from metadata.generated.schema.type.queryParserData import ParsedData, QueryParserData
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.stage.table_usage import TableStageConfig, TableUsageStage
from metadata.utils.helpers import TRANSIENT_TABLE_PATTERNS, is_transient_table_name

TEMP = "temp_table_6e154afab897449a8e11d5367933b124"
CTAS_SQL = (
    f'CREATE TABLE "staging"."{TEMP}" WITH (format = \'PARQUET\') AS '
    'WITH src AS (SELECT e.id, t.item FROM "raw"."events" e CROSS JOIN UNNEST(e.items) AS t(item)) '
    "SELECT * FROM src"
)
MERGE_SQL = (
    f'MERGE INTO "gold"."audit_log" target USING "staging"."{TEMP}" source ON (target.id = source.id) '
    "WHEN MATCHED THEN UPDATE SET x = source.x WHEN NOT MATCHED THEN INSERT (id, x) VALUES (source.id, source.x)"
)
UNNEST_JOIN_SQL = "SELECT t.x FROM db.tbl CROSS JOIN UNNEST(arr) AS t(x) JOIN db.other o ON o.k = t.x"
TEMP_ONLY_SQL = f'SELECT count(*) FROM "staging"."{TEMP}"'
ICEBERG_META_SQL = 'SELECT * FROM "db"."tbl$snapshots" s JOIN db.tbl t ON t.snapshot_id = s.snapshot_id'
SCRATCH_JOIN_SQL = "SELECT * FROM analytics.scratch_42 s JOIN analytics.facts f ON f.id = s.id"


def _parsed(sql: str) -> ParsedData:
    """What the query parser processor hands the stage: the parser's raw table list and joins."""
    parser = LineageParser(sql, dialect=Dialect.ATHENA)
    return ParsedData(
        tables=parser.clean_table_list,
        joins=dict(parser.table_joins),
        databaseName="default",
        databaseSchema="staging",
        sql=sql,
        dialect=Dialect.ATHENA.value,
        userName="alice",
        date="1756684800000",
        serviceName="svc",
    )


def _stage(tmp_path, **config):
    metadata = MagicMock()
    metadata.get_by_name.return_value = None
    return TableUsageStage(TableStageConfig(filename=str(tmp_path / "stage"), **config), metadata)


def _run(stage, *sqls):
    staged = [either.right for either in stage._run(QueryParserData(parsedData=[_parsed(sql) for sql in sqls]))]
    return sorted(staged)


def _staged_files(tmp_path):
    records = []
    for file in (tmp_path / "stage").iterdir():
        if file.name.endswith("_query"):
            continue
        records.extend(json.loads(json.loads(line)) for line in file.read_text().splitlines())
    return records


@pytest.mark.parametrize(
    "name",
    [
        TEMP,
        f"staging.{TEMP}",
        f"awsdatacatalog.staging.{TEMP}",
        f'"staging"."{TEMP}"',
        "TEMP_TABLE_6E154AFAB897449A8E11D5367933B124",
        "unnest",
        "default.unnest",
        "db.tbl$snapshots",
        "db.tbl$history",
    ],
)
def test_transient_names_match_the_built_in_patterns(name):
    assert is_transient_table_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "temp_table_1",  # too short to be an awswrangler staging table
        "temp_table_6e154afab897449a8e11d5367933b12z",  # not hex
        "my_temp_table_6e154afab897449a8e11d5367933b124_v2",  # only a substring matches
        "staging.temp_ldap_roles",
        "db.unnest_results",
        "db.tbl",
        "",
        None,
    ],
)
def test_real_names_do_not_match_the_built_in_patterns(name):
    assert not is_transient_table_name(name)


def test_extra_patterns_extend_the_built_in_list():
    patterns = TRANSIENT_TABLE_PATTERNS + (r"scratch_\d+",)

    assert is_transient_table_name("analytics.scratch_42", patterns)
    assert is_transient_table_name(f"staging.{TEMP}", patterns)
    assert not is_transient_table_name("analytics.scratch_42")


def test_parser_still_reports_the_transient_tables_the_stage_must_drop():
    assert f"staging.{TEMP}" in _parsed(MERGE_SQL).tables
    assert "unnest" in _parsed(UNNEST_JOIN_SQL).tables


def test_ctas_into_a_staging_table_is_attributed_to_the_source_table_only(tmp_path):
    assert _run(_stage(tmp_path), CTAS_SQL) == ["raw.events"]


def test_merge_from_a_staging_table_keeps_the_target_and_drops_the_join_to_it(tmp_path):
    stage = _stage(tmp_path)

    assert _run(stage, MERGE_SQL) == ["gold.audit_log"]
    assert stage.table_usage[("gold.audit_log", "1756684800000")].joins == [], (
        "a join against a table that will never resolve is dropped with it"
    )


def test_unnest_placeholder_is_dropped_from_tables_and_joined_with(tmp_path):
    stage = _stage(tmp_path)

    assert _run(stage, UNNEST_JOIN_SQL) == ["db.other", "db.tbl"]
    joined_with = [
        column.table for usage in stage.table_usage.values() for join in usage.joins for column in join.joinedWith
    ]
    assert "unnest" not in joined_with


def test_iceberg_metadata_table_is_dropped_and_the_real_table_survives(tmp_path):
    stage = _stage(tmp_path)

    assert _run(stage, ICEBERG_META_SQL) == ["db.tbl"]
    assert stage.table_usage[("db.tbl", "1756684800000")].joins == []


def test_statement_touching_only_transient_tables_stages_no_usage(tmp_path):
    stage = _stage(tmp_path)

    assert _run(stage, TEMP_ONLY_SQL) == []
    assert stage.table_usage == {}
    assert stage.table_queries == {}


def test_pipeline_config_adds_patterns_on_top_of_the_built_in_ones(tmp_path):
    extended = _stage(tmp_path / "a", transientTablePatterns=[r"scratch_\d+"])
    default = _stage(tmp_path / "b")

    assert _run(extended, SCRATCH_JOIN_SQL) == ["analytics.facts"]
    assert extended.table_usage[("analytics.facts", "1756684800000")].joins == []
    assert _run(default, SCRATCH_JOIN_SQL) == ["analytics.facts", "analytics.scratch_42"]
    assert _run(extended, MERGE_SQL) == ["gold.audit_log"]


def test_nothing_transient_reaches_the_staged_files_the_sink_reads(tmp_path):
    stage = _stage(tmp_path)
    list(stage._run(QueryParserData(parsedData=[_parsed(CTAS_SQL), _parsed(MERGE_SQL), _parsed(TEMP_ONLY_SQL)])))

    records = _staged_files(tmp_path)

    assert sorted(record["table"] for record in records) == ["gold.audit_log", "raw.events"]
    assert all(TEMP not in json.dumps(record["joins"]) for record in records)
    assert [len(record["sqlQueries"]) for record in records] == [1, 1]
