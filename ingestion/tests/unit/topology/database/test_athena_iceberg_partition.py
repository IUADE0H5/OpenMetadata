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
Iceberg partitioning is hidden: Glue reports no partition keys, so query authors could not see which
columns prune. The spec lives in the table's metadata.json behind Glue's `metadata_location`.
"""

import json
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from metadata.generated.schema.entity.data.table import PartitionIntervalTypes
from metadata.ingestion.source.database.athena.iceberg_partition import (
    get_iceberg_partition_columns,
)


def _metadata_v2(spec_fields, schema_fields=None):
    fields = schema_fields or [
        {"id": 1, "name": "calendarday", "type": "date"},
        {"id": 2, "name": "metadata_eventcreatedat", "type": "timestamp"},
        {"id": 3, "name": "tin", "type": "string"},
        {"id": 4, "name": "region", "type": "string"},
    ]
    return {
        "format-version": 2,
        "current-schema-id": 7,
        "schemas": [{"schema-id": 0, "fields": []}, {"schema-id": 7, "fields": fields}],
        "default-spec-id": 3,
        "partition-specs": [
            {"spec-id": 0, "fields": [{"name": "old", "transform": "identity", "source-id": 4, "field-id": 1000}]},
            {"spec-id": 3, "fields": spec_fields},
        ],
    }


def _clients(metadata_json, location="s3://bucket/warehouse/db/t/metadata/00012-abc.metadata.json"):
    glue = MagicMock()
    glue.get_table.return_value = {"Table": {"Parameters": {"table_type": "ICEBERG", "metadata_location": location}}}
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": BytesIO(json.dumps(metadata_json).encode())}
    return glue, s3


def test_transforms_map_to_source_column_interval_type_and_interval():
    glue, s3 = _clients(
        _metadata_v2(
            [
                {"name": "calendarday", "transform": "identity", "source-id": 1, "field-id": 1000},
                {"name": "metadata_eventcreatedat_day", "transform": "day", "source-id": 2, "field-id": 1001},
                {"name": "tin_bucket", "transform": "bucket[16]", "source-id": 3, "field-id": 1002},
                {"name": "region_trunc", "transform": "truncate[4]", "source-id": 4, "field-id": 1003},
            ]
        )
    )

    columns = get_iceberg_partition_columns(glue, s3, "db", "t")

    assert [(c.columnName, c.intervalType, c.interval) for c in columns] == [
        ("calendarday", PartitionIntervalTypes.COLUMN_VALUE, "identity"),
        ("metadata_eventcreatedat", PartitionIntervalTypes.TIME_UNIT, "day"),
        ("tin", PartitionIntervalTypes.OTHER, "bucket[16]"),
        ("region", PartitionIntervalTypes.OTHER, "truncate[4]"),
    ]
    s3.get_object.assert_called_once_with(Bucket="bucket", Key="warehouse/db/t/metadata/00012-abc.metadata.json")


def test_reads_the_default_spec_not_an_older_one():
    glue, s3 = _clients(
        _metadata_v2([{"name": "calendarday", "transform": "identity", "source-id": 1, "field-id": 1000}])
    )

    assert [c.columnName for c in get_iceberg_partition_columns(glue, s3, "db", "t")] == ["calendarday"]


def test_void_transform_is_dropped_and_empty_spec_yields_empty_list():
    glue, s3 = _clients(_metadata_v2([{"name": "region", "transform": "void", "source-id": 4, "field-id": 1000}]))
    assert get_iceberg_partition_columns(glue, s3, "db", "t") == []

    glue, s3 = _clients(_metadata_v2([]))
    assert get_iceberg_partition_columns(glue, s3, "db", "t") == []


def test_format_v1_metadata_uses_schema_and_partition_spec():
    meta = {
        "format-version": 1,
        "schema": {"fields": [{"id": 1, "name": "ts", "type": "timestamp"}]},
        "partition-spec": [{"name": "ts_hour", "transform": "hour", "source-id": 1, "field-id": 1000}],
    }
    glue, s3 = _clients(meta)

    columns = get_iceberg_partition_columns(glue, s3, "db", "t")

    assert [(c.columnName, c.intervalType, c.interval) for c in columns] == [
        ("ts", PartitionIntervalTypes.TIME_UNIT, "hour")
    ]


@pytest.mark.parametrize(
    "params",
    [{"table_type": "ICEBERG"}, {"table_type": "ICEBERG", "metadata_location": "not-an-s3-uri"}, {}],
)
def test_missing_or_bad_metadata_location_returns_none(params):
    glue = MagicMock()
    glue.get_table.return_value = {"Table": {"Parameters": params}}

    assert get_iceberg_partition_columns(glue, MagicMock(), "db", "t") is None


def test_unresolvable_source_id_is_skipped_not_fatal():
    glue, s3 = _clients(_metadata_v2([{"name": "ghost", "transform": "identity", "source-id": 99, "field-id": 1000}]))

    assert get_iceberg_partition_columns(glue, s3, "db", "t") == []


def test_catalog_id_is_forwarded_to_glue():
    glue, s3 = _clients(_metadata_v2([]))
    get_iceberg_partition_columns(glue, s3, "db", "t", catalog_id="123456789012")

    glue.get_table.assert_called_once_with(DatabaseName="db", Name="t", CatalogId="123456789012")


def test_source_falls_back_to_iceberg_spec_when_glue_has_no_partition_keys():
    from metadata.ingestion.source.database.athena.metadata import AthenaSource

    src = AthenaSource.__new__(AthenaSource)
    src.service_connection = MagicMock()
    src.service_connection.catalogId = None
    src._partition_columns_cache = {}
    src.glue_client, src.s3_client = _clients(
        _metadata_v2([{"name": "metadata_eventcreatedat_day", "transform": "day", "source-id": 2, "field-id": 1001}])
    )
    inspector = MagicMock()
    inspector.get_columns.return_value = []

    found, partition = src.get_table_partition_details("t", "db", inspector)

    assert found is True
    assert [(c.columnName, c.interval) for c in partition.columns] == [("metadata_eventcreatedat", "day")]


def test_source_reports_no_partition_for_unpartitioned_iceberg_table():
    from metadata.ingestion.source.database.athena.metadata import AthenaSource

    src = AthenaSource.__new__(AthenaSource)
    src.service_connection = MagicMock()
    src.service_connection.catalogId = None
    src._partition_columns_cache = {}
    src.glue_client, src.s3_client = _clients(_metadata_v2([]))
    inspector = MagicMock()
    inspector.get_columns.return_value = []

    assert src.get_table_partition_details("t", "db", inspector) == (False, None)


def test_partitioned_iceberg_tables_keep_the_iceberg_type():
    from metadata.generated.schema.entity.data.table import TableType
    from metadata.ingestion.source.database.common_db_source import PARTITION_PRESERVED_TABLE_TYPES

    assert TableType.Iceberg in PARTITION_PRESERVED_TABLE_TYPES
    assert TableType.View in PARTITION_PRESERVED_TABLE_TYPES
    assert TableType.Regular not in PARTITION_PRESERVED_TABLE_TYPES
