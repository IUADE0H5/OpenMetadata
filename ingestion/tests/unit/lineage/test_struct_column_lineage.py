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
Column lineage through struct field extraction (`SELECT after_metadata.eventid AS metadata_eventid
FROM raw`). The SQL parser reports `after_metadata` as if it were a table, so the source column
never matched the raw table and every such pair was dropped.
"""

from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName, Uuid
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.lineage.sql_lineage import (
    _build_table_lineage,
    get_column_lineage,
    populate_column_lineage_map,
)

RAW = "svc.db.raw.kafka_events"
CURATED = "svc.db.curated.kafka_events"


def _col(table_fqn, name, data_type=DataType.STRING, children=None):
    return Column(
        name=name.split(".")[-1],
        dataType=data_type,
        fullyQualifiedName=FullyQualifiedEntityName(f"{table_fqn}.{name}"),
        children=children,
    )


def _raw_table():
    return Table(
        id=Uuid("11111111-1111-1111-1111-111111111111"),
        name=EntityName("kafka_events"),
        fullyQualifiedName=FullyQualifiedEntityName(RAW),
        columns=[
            _col(RAW, "kafka_offset", DataType.BIGINT),
            _col(
                RAW,
                "after_metadata",
                DataType.STRUCT,
                children=[_col(RAW, "after_metadata.eventid"), _col(RAW, "after_metadata.eventname")],
            ),
            _col(
                RAW,
                "after_payload",
                DataType.STRUCT,
                children=[
                    _col(RAW, "after_payload.tin"),
                    _col(
                        RAW,
                        "after_payload.address",
                        DataType.STRUCT,
                        children=[_col(RAW, "after_payload.address.city")],
                    ),
                ],
            ),
        ],
    )


def _curated_table():
    return Table(
        id=Uuid("22222222-2222-2222-2222-222222222222"),
        name=EntityName("kafka_events"),
        fullyQualifiedName=FullyQualifiedEntityName(CURATED),
        columns=[
            _col(CURATED, "kafka_offset", DataType.BIGINT),
            _col(CURATED, "metadata_eventid"),
            _col(CURATED, "metadata_eventname"),
            _col(CURATED, "payload_tin"),
            _col(CURATED, "payload_city"),
        ],
    )


def _map():
    return {
        "curated.kafka_events": {
            "raw.kafka_events": [("kafka_offset", "kafka_offset")],
            "<default>.after_metadata": [("metadata_eventid", "eventid"), ("metadata_eventname", "eventname")],
            "<default>.after_payload": [("payload_tin", "tin")],
            "<default>.after_payload.address": [("payload_city", "city")],
            "<default>.not_a_struct": [("payload_tin", "whatever")],
        }
    }


def _pairs(lineage):
    return sorted(
        (cl.fromColumns[0].root.split(RAW + ".")[1], cl.toColumn.root.split(CURATED + ".")[1]) for cl in lineage
    )


def test_struct_fields_resolve_to_nested_columns_of_the_single_source_table():
    lineage = get_column_lineage(
        to_entity=_curated_table(),
        from_entity=_raw_table(),
        to_table_raw_name="curated.kafka_events",
        from_table_raw_name="raw.kafka_events",
        column_lineage_map=_map(),
        resolve_struct_sources=True,
    )

    assert _pairs(lineage) == [
        ("after_metadata.eventid", "metadata_eventid"),
        ("after_metadata.eventname", "metadata_eventname"),
        ("after_payload.address.city", "payload_city"),
        ("after_payload.tin", "payload_tin"),
        ("kafka_offset", "kafka_offset"),
    ]


def test_struct_resolution_is_off_by_default_and_keeps_plain_columns():
    lineage = get_column_lineage(
        to_entity=_curated_table(),
        from_entity=_raw_table(),
        to_table_raw_name="curated.kafka_events",
        from_table_raw_name="raw.kafka_events",
        column_lineage_map=_map(),
    )

    assert _pairs(lineage) == [("kafka_offset", "kafka_offset")]


def test_struct_keys_never_attach_to_a_table_without_that_struct():
    other = _curated_table()
    lineage = get_column_lineage(
        to_entity=_curated_table(),
        from_entity=other,
        to_table_raw_name="curated.kafka_events",
        from_table_raw_name="some.other_table",
        column_lineage_map=_map(),
        resolve_struct_sources=True,
    )

    assert lineage == []


def test_build_table_lineage_carries_struct_columns_when_enabled():
    result = _build_table_lineage(
        from_entity=_raw_table(),
        to_entity=_curated_table(),
        from_table_raw_name="raw.kafka_events",
        to_table_raw_name="curated.kafka_events",
        masked_query="INSERT ...",
        column_lineage_map=_map(),
        resolve_struct_sources=True,
    )

    assert result.right is not None
    assert len(result.right.lineage_details.columnsLineage) == 5


def test_parser_output_for_struct_extraction_resolves_end_to_end():
    sql = (
        "INSERT INTO curated.kafka_events (kafka_offset, metadata_eventid, payload_city) "
        "SELECT kafka_offset, after_metadata.eventid AS metadata_eventid, after_payload.address.city AS payload_city "
        "FROM raw.kafka_events"
    )
    parser = LineageParser(sql, dialect=Dialect.ATHENA)
    lineage_map = populate_column_lineage_map(parser.column_lineage)
    assert [str(t) for t in parser.source_tables] == ["raw.kafka_events"]

    lineage = get_column_lineage(
        to_entity=_curated_table(),
        from_entity=_raw_table(),
        to_table_raw_name="curated.kafka_events",
        from_table_raw_name="raw.kafka_events",
        column_lineage_map=lineage_map,
        resolve_struct_sources=True,
    )

    assert ("after_metadata.eventid", "metadata_eventid") in _pairs(lineage)
    assert ("after_payload.address.city", "payload_city") in _pairs(lineage)
    assert ("kafka_offset", "kafka_offset") in _pairs(lineage)


def test_ambiguous_struct_name_is_skipped_not_guessed():
    raw = _raw_table()
    raw.columns.append(
        _col(
            RAW,
            "before_payload",
            DataType.STRUCT,
            children=[
                _col(
                    RAW,
                    "before_payload.address",
                    DataType.STRUCT,
                    children=[_col(RAW, "before_payload.address.city")],
                )
            ],
        )
    )
    lineage = get_column_lineage(
        to_entity=_curated_table(),
        from_entity=raw,
        to_table_raw_name="curated.kafka_events",
        from_table_raw_name="raw.kafka_events",
        column_lineage_map={"curated.kafka_events": {"<default>.address": [("payload_city", "city")]}},
        resolve_struct_sources=True,
    )

    assert lineage == []
