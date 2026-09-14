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
"""What the usage sink says and counts while it publishes: one line per staged file, progress on the
shared counters the stage seeded, per-table success at DEBUG, and no cost work when the flag is off."""

import json
import logging
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from metadata.generated.schema.type.tableUsageCount import TableUsageCount
from metadata.ingestion.bulksink.metadata_usage import MetadataUsageBulkSink, MetadataUsageSinkConfig
from metadata.ingestion.progress.modes import ProgressMode
from metadata.ingestion.progress.tracking import share_progress_tracking


class _Source:
    progress_mode = ProgressMode.MANUAL


@pytest.fixture
def staging():
    path = Path(tempfile.mkdtemp())
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _usage_file(staging, tables):
    with (staging / "svc_1702000000000").open("w") as file:
        for name in tables:
            record = TableUsageCount(
                table=name,
                date="1702000000000",
                databaseName="db",
                databaseSchema="s",
                count=2,
                joins=[],
                serviceName="svc",
            )
            file.write(json.dumps(record.model_dump_json()) + "\n")


def _cost_file(staging, n):
    with (staging / "svc_1702000000000_query").open("w") as file:
        for i in range(n):
            file.write(
                json.dumps(
                    {
                        "queryHash": f"h{i}",
                        "date": "1702000000000",
                        "cost": 1.0,
                        "count": 1,
                        "query": f"SELECT {i} FROM s.t",
                        "dialect": "trino",
                        "totalDuration": 1,
                    }
                )
                + "\n"
            )


def _entity(name):
    table = MagicMock()
    table.id.root = uuid4()
    table.name.root = name
    table.fullyQualifiedName.root = f"svc.db.s.{name}"
    return table


def _sink(staging):
    sink = MetadataUsageBulkSink(config=MetadataUsageSinkConfig(filename=str(staging)), metadata=MagicMock())
    source = _Source()
    share_progress_tracking(source, sink)
    return sink, source


def test_one_line_per_usage_file_with_resolution_and_publish_counts(staging, caplog):
    _usage_file(staging, ["t1", "t2", "missing"])
    sink, source = _sink(staging)
    sink.process_query_cost = False
    source._progress_tracking.manual.seed_scope_total("Usage records", "batch 1", 3)

    def lookup(table_name, **_):
        return None if table_name == "missing" else [_entity(table_name)]

    with (
        patch("metadata.ingestion.bulksink.metadata_usage.get_table_entities_from_query", side_effect=lookup),
        caplog.at_level(logging.DEBUG),
    ):
        sink.run()

    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Usage file ")]
    assert len(lines) == 1
    assert "3 table-day records, 1 not in the catalogue, 2 tables resolved" in lines[0]
    assert "usage published for 2 tables" in lines[0]
    # per-table success is DEBUG, "could not fetch" is DEBUG: the file line carries the counts
    assert [r.levelno for r in caplog.records if "Table usage published for" in r.getMessage()] == [logging.DEBUG] * 2
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert source._progress_tracking.registry.global_counters() == [("Usage records", 3, 3)]


def test_cost_records_are_published_and_counted_only_when_the_flag_is_on(staging, caplog):
    _cost_file(staging, 4)
    sink, source = _sink(staging)
    source._progress_tracking.manual.seed_scope_total("Query costs", "batch 1", 4)
    with caplog.at_level(logging.INFO):
        sink.handle_query_cost()
    assert sink.metadata.publish_query_cost.call_count == 4
    assert source._progress_tracking.registry.global_counters() == [("Query costs", 4, 4)]
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("Query-cost file "))
    assert line.startswith("Query-cost file svc_1702000000000_query: 4 records published in")

    off, _ = _sink(staging)
    off.process_query_cost = False
    off.handle_query_cost()
    assert not off.metadata.publish_query_cost.called
