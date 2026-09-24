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
Sink behaviour for lineage-heavy runs:
- checkPatchLineage controls the per-edge pre-write GET (initial-load fast path)
- the entity/query buffers stay consistent when driven by several threads at once
"""

import threading
import uuid
from unittest.mock import MagicMock

from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type import basic
from metadata.generated.schema.type.basic import SqlQuery
from metadata.generated.schema.type.bulkOperationResult import BulkOperationResult
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.sink.metadata_rest import MetadataRestSink, MetadataRestSinkConfig


def _add_lineage_request() -> AddLineageRequest:
    return AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=uuid.uuid4(), type="table"),
            toEntity=EntityReference(id=uuid.uuid4(), type="table"),
        )
    )


def _sink(**config) -> MetadataRestSink:
    metadata = MagicMock()
    metadata.add_lineage.return_value = {"entity": {"fullyQualifiedName": "svc.db.schema.table"}}
    return MetadataRestSink(MetadataRestSinkConfig(**config), metadata)


class TestCheckPatchLineage:
    """checkPatchLineage decides whether each edge does the pre-write getLineageEdge GET."""

    def test_defaults_to_true(self):
        sink = _sink()
        sink.write_lineage(_add_lineage_request())
        assert sink.metadata.add_lineage.call_args.kwargs["check_patch"] is True

    def test_false_skips_the_pre_write_get(self):
        sink = _sink(checkPatchLineage=False)
        sink.write_lineage(_add_lineage_request())
        assert sink.metadata.add_lineage.call_args.kwargs["check_patch"] is False

    def test_return_lineage_always_false(self):
        """The per-edge full-graph GET stays disabled regardless of check_patch."""
        sink = _sink(checkPatchLineage=False)
        sink.write_lineage(_add_lineage_request())
        assert sink.metadata.add_lineage.call_args.kwargs["return_lineage"] is False


class TestConcurrentQueryBuffer:
    """The query buffer must not lose or double-count records when several threads write to it."""

    def test_no_query_lost_under_concurrent_writers(self):
        flushed: list = []
        lock = threading.Lock()

        def bulk(entities, **_):
            with lock:
                flushed.extend(entities)
            return BulkOperationResult(status=basic.Status.success, successRequest=[], failedRequest=[])

        sink = _sink(bulk_sink_batch_size=50)
        sink.metadata.bulk_create_or_update.side_effect = bulk

        total = 500

        def writer(start: int):
            for i in range(start, start + 50):
                sink.write_query(CreateQueryRequest(query=SqlQuery(f"select {i}"), service="svc"))

        threads = [threading.Thread(target=writer, args=(base * 50,)) for base in range(total // 50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Everything is either already flushed or still buffered - nothing dropped, nothing doubled.
        assert len(flushed) + len(sink.query_buffer) == total
        # Distinct SQL => distinct checksums => no accidental dedup collapsed any of them.
        flushed_sql = {model_query(q) for q in flushed} | {model_query(q) for q in sink.query_buffer}
        assert len(flushed_sql) == total


def model_query(request: CreateQueryRequest) -> str:
    return request.query.root if hasattr(request.query, "root") else str(request.query)
