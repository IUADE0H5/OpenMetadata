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
Athena table metrics, taken from Iceberg's own snapshot totals.

`sizeInByte` and `createDateTime` are only ever set by a dialect's table metric computer, and
Athena never had one, so an Athena table profile has always shown a blank size and no creation
date. The row count was a real `count(*)` per table per run, which on a lake is a billed scan for
a number Iceberg already holds.
"""

import json
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from metadata.profiler.orm.functions import table_metric_computer as module
from metadata.profiler.orm.functions.table_metric_computer import (
    AthenaTableMetricComputer,
    table_metric_computer_factory,
)
from metadata.profiler.orm.registry import Dialects
from metadata.utils.glue_catalog import declared_size_bytes, table_stats

KYIV = timezone(timedelta(hours=3))
CREATED = datetime(2026, 6, 9, 15, 9, 46, tzinfo=KYIV)

# What the base computer returns when it runs `count(*)`; the shape the caller reads with _asdict().
FallbackRow = namedtuple("Result", ["rowCount", "columnCount", "columnNames"])
FALLBACK = FallbackRow(rowCount=41, columnCount=2, columnNames="id,name")


def _metadata(snapshots, current):
    return {"format-version": 2, "current-snapshot-id": current, "snapshots": snapshots}


def _snapshot(snapshot_id, *, records=None, size=None):
    summary = {"operation": "append"}
    if records is not None:
        summary["total-records"] = records
    if size is not None:
        summary["total-files-size"] = size
    return {"snapshot-id": snapshot_id, "timestamp-ms": 1_700_000_000_000, "summary": summary}


@pytest.fixture
def computer(monkeypatch):
    """A computer wired to a stub lake: one Glue entry, one metadata document, no Athena."""
    table = Table("t", MetaData(), Column("id", Integer), Column("name", String), schema="db")
    runner = MagicMock()
    runner.raw_dataset = table
    runner.dataset = table
    runner.schema_name = "db"
    runner.table_name = "t"
    runner.select_first_from_table.return_value = FALLBACK
    runner._session.get_bind().url.database = "1234"

    glue, s3 = MagicMock(), MagicMock()
    monkeypatch.setattr(module, "glue_and_s3_clients", lambda config: (glue, s3))

    def build(metadata, *, create_time=CREATED, iceberg=True, parameters=None):
        parameters = dict(parameters or {})
        if iceberg:
            parameters["table_type"] = "ICEBERG"
            parameters["metadata_location"] = "s3://bucket/db/t/metadata/00001-a.metadata.json"
        glue.get_table.return_value = {"Table": {"Parameters": parameters, "CreateTime": create_time}}
        s3.get_object.return_value = {"Body": BytesIO(json.dumps(metadata).encode())}
        conn_config = MagicMock()
        conn_config.catalogId = None
        instance = table_metric_computer_factory.construct(
            Dialects.Athena, runner=runner, metrics=[], conn_config=conn_config, entity=MagicMock()
        )
        return instance, runner

    return build


def test_the_athena_dialect_resolves_to_this_computer():
    """The registration is the whole feature: upstream removed Athena's entry in #29446 and the
    dialect has fallen through to the base computer, which emits neither field, ever since."""
    assert table_metric_computer_factory._constructs[Dialects.Athena] is AthenaTableMetricComputer


def test_totals_come_from_the_current_snapshot_and_cost_no_query(computer):
    instance, runner = computer(_metadata([_snapshot(7, records=11396417, size=9916145)], current=7))

    result = instance.compute()._asdict()

    assert result["rowCount"] == 11396417
    assert result["sizeInBytes"] == 9916145
    assert result["columnNames"] == "id,name"
    # The point of the whole computer: the number was already known, so nothing was scanned.
    runner.select_first_from_table.assert_not_called()


def test_create_time_is_converted_to_utc_not_relabelled(computer):
    """profiler/processor/core.py stamps the value with `replace(tzinfo=utc)`, which reinterprets
    rather than converts. A Glue CreateTime at +03:00 handed over as-is lands three hours early."""
    instance, _ = computer(_metadata([_snapshot(1, records=1)], current=1))

    created = instance.compute()._asdict()["createDateTime"]

    assert created == CREATED
    assert created.utcoffset() == timedelta(0)
    assert created.replace(tzinfo=timezone.utc) == CREATED, "surviving core.py's replace() intact"


def test_a_snapshot_without_a_row_count_still_reports_size_and_creation(computer):
    """The summary keys are optional in the spec. A writer that omits `total-records` must cost the
    count, not the other two fields."""
    instance, runner = computer(_metadata([_snapshot(3, size=512)], current=3))

    result = instance.compute()._asdict()

    assert result["rowCount"] == FALLBACK.rowCount
    assert result["sizeInBytes"] == 512
    assert result["createDateTime"] is not None
    runner.select_first_from_table.assert_called_once()


def test_a_hive_table_keeps_its_count_and_gains_size_and_creation(computer):
    """The lake is not all Iceberg: ~1,800 tables in the profiled layers are Hive-style, and 563 of
    them carry a crawler's totalSize. None of that changes how their row count is produced."""
    instance, runner = computer({}, iceberg=False, parameters={"totalSize": "4096", "classification": "parquet"})

    result = instance.compute()._asdict()

    assert result["rowCount"] == FALLBACK.rowCount, "still counted, never guessed"
    assert result["sizeInBytes"] == 4096
    assert result["createDateTime"] is not None
    runner.select_first_from_table.assert_called_once()


def test_a_hive_table_with_nothing_recorded_is_computed_exactly_as_before(computer):
    """The no-regression case: a table the catalog says nothing useful about must come out of this
    computer byte-for-byte as the base one produced it, plus the creation date Glue always has."""
    instance, runner = computer({}, iceberg=False, create_time=None)

    assert instance.compute() is FALLBACK, "not merely equal: untouched"
    runner.select_first_from_table.assert_called_once()


def test_hive_row_counts_in_the_catalog_are_never_believed(computer):
    """Hive writes numRows as -1 or leaves it stale for months. Today's count(*) is correct, and
    trading a correct number for a cheap wrong one is a regression however tempting the saving."""
    instance, runner = computer({}, iceberg=False, parameters={"numRows": "999999", "recordCount": "999999"})

    assert instance.compute()._asdict()["rowCount"] == FALLBACK.rowCount
    runner.select_first_from_table.assert_called_once()


@pytest.mark.parametrize(
    "parameters, expected",
    [
        ({"totalSize": "4096"}, 4096),
        ({"sizeKey": "77"}, 77),
        ({"totalSize": "12", "sizeKey": "99"}, 12),
        ({"totalSize": "-1"}, None),
        ({"totalSize": "unknown"}, None),
        ({}, None),
        ({"numRows": "5"}, None),
    ],
)
def test_the_catalog_size_is_read_defensively(parameters, expected):
    assert declared_size_bytes({"Parameters": parameters}) == expected


def test_a_rolled_back_table_reports_what_readers_see(computer):
    """Snapshots outlive a rollback, so the newest one can be a version no query returns."""
    instance, _ = computer(
        _metadata([_snapshot(1, records=10, size=100), _snapshot(2, records=999, size=9999)], current=1)
    )

    result = instance.compute()._asdict()

    assert (result["rowCount"], result["sizeInBytes"]) == (10, 100)


def test_a_table_never_written_to_is_counted_rather_than_guessed(computer):
    """`current-snapshot-id: -1` is an empty table, but reporting 0 would be an inference. The
    count is free on an empty table; the creation date is the field that was actually missing."""
    instance, runner = computer(_metadata([], current=-1))

    result = instance.compute()._asdict()

    assert result["rowCount"] == FALLBACK.rowCount
    assert result["createDateTime"] is not None
    assert "sizeInBytes" not in result
    runner.select_first_from_table.assert_called_once()


def test_unreadable_metadata_degrades_to_the_old_path(computer, monkeypatch):
    """A missing s3:GetObject permission must cost the size, never the profile."""
    instance, runner = computer(_metadata([_snapshot(1, records=5)], current=1))
    monkeypatch.setattr(module, "load_catalog_table", MagicMock(side_effect=PermissionError("AccessDenied")))

    assert instance.compute() is FALLBACK
    runner.select_first_from_table.assert_called_once()


@pytest.mark.parametrize(
    "summary, expected",
    [
        ({"total-records": "77776", "total-files-size": "9916145"}, (77776, 9916145)),
        ({"total-records": 5, "total-files-size": 19152}, (5, 19152)),
        ({}, (None, None)),
        ({"total-records": "not-a-number"}, (None, None)),
    ],
)
def test_summary_values_are_read_whether_written_as_strings_or_numbers(summary, expected):
    """The spec says the summary map is string to string; engines write both. A `None` here has to
    mean "the snapshot does not say", never zero -- a zero would publish a wrong row count."""
    stats = table_stats(_metadata([{"snapshot-id": 1, "summary": summary}], current=1))

    assert (stats.row_count, stats.size_bytes) == expected


def test_clients_are_built_once_per_credential_set_not_once_per_table():
    """`AWSClient.create_session` resolves refreshable credentials eagerly, so with an assume-role
    connection a fresh client is a fresh AssumeRole. Per table that is one STS call per table."""
    from metadata.generated.schema.security.credentials.awsCredentials import AWSCredentials
    from metadata.utils import glue_catalog as catalog

    built = []

    class _Client:
        def __init__(self, config):
            built.append(config)

        def get_glue_client(self):
            return object()

        def get_s3_client(self):
            return object()

    original, catalog.AWSClient = catalog.AWSClient, _Client
    catalog._clients.clear()
    try:
        config = AWSCredentials(awsRegion="eu-central-1", assumeRoleArn="arn:aws:iam::1:role/r")
        same = AWSCredentials(awsRegion="eu-central-1", assumeRoleArn="arn:aws:iam::1:role/r")
        other = AWSCredentials(awsRegion="eu-central-1", assumeRoleArn="arn:aws:iam::2:role/r")

        first = catalog.glue_and_s3_clients(config)
        assert catalog.glue_and_s3_clients(same) is first, "a second table must reuse the session"
        assert catalog.glue_and_s3_clients(other) is not first, "a different role is a different session"
        assert len(built) == 2
    finally:
        catalog.AWSClient = original
        catalog._clients.clear()


def test_the_client_cache_cannot_grow_without_bound():
    from metadata.generated.schema.security.credentials.awsCredentials import AWSCredentials
    from metadata.utils import glue_catalog as catalog

    class _Client:
        def __init__(self, config):
            pass

        def get_glue_client(self):
            return object()

        def get_s3_client(self):
            return object()

    original, catalog.AWSClient = catalog.AWSClient, _Client
    catalog._clients.clear()
    try:
        for index in range(catalog._MAX_CACHED_SESSIONS * 3):
            catalog.glue_and_s3_clients(AWSCredentials(awsRegion=f"eu-central-{index}"))
        assert len(catalog._clients) == catalog._MAX_CACHED_SESSIONS
    finally:
        catalog.AWSClient = original
        catalog._clients.clear()
