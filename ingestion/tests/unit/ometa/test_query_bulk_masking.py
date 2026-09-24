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
"""The bulk query path masks once per statement shape and looks queries up on threads."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.type.basic import FullyQualifiedEntityName, SqlQuery, Timestamp
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.mixins import query_mixin
from metadata.ingestion.ometa.mixins.query_mixin import OMetaQueryMixin


class _Mixin(OMetaQueryMixin):
    def __init__(self):
        self.client = MagicMock()
        self.looked_up = []

    def get_by_name(self, entity, fqn, fields=None):
        self.looked_up.append(fqn)
        return

    def bulk_create_or_update(self, requests):
        result = MagicMock()
        result.numberOfRowsFailed.root = 0
        self.created = requests
        return result

    def get_suffix(self, entity):
        return "/queries"


def _pair(text):
    return (
        CreateQueryRequest(
            query=SqlQuery(text),
            queryDate=Timestamp(1702000000000),
            service=FullyQualifiedEntityName("svc"),
            dialect="athena",
        ),
        EntityReference(id=uuid4(), type="table"),
    )


def test_same_shape_statements_are_masked_once_and_collapse_into_one_query():
    query_mixin._masked_by_shape.clear()
    mixin = _Mixin()
    pairs = [
        _pair("SELECT a FROM s.t WHERE dt = '2026-01-01' AND id = 1"),
        _pair("SELECT a FROM s.t WHERE dt = '2026-02-02' AND id = 2"),
    ]
    with patch.object(query_mixin, "mask_query", wraps=query_mixin.mask_query) as masked:
        mixin.ingest_queries_bulk(pairs, threads=1)
    assert masked.call_count == 1  # one shape, masked once, remembered for the next flush
    assert len(mixin.looked_up) == 1 and len(mixin.created) == 1
    assert len(mixin.created[0].queryUsedIn.root) == 2  # both tables attached to the one query
    with patch.object(query_mixin, "mask_query", wraps=query_mixin.mask_query) as masked:
        _Mixin().ingest_queries_bulk([_pair("SELECT a FROM s.t WHERE dt = '2027-03-03' AND id = 3")], threads=1)
    assert masked.call_count == 0  # served from the shape cache across flushes


def test_lookups_run_on_threads_and_the_results_are_merged():
    query_mixin._masked_by_shape.clear()
    mixin = _Mixin()
    pairs = [_pair(f"SELECT c{i} FROM s.t{i}") for i in range(20)]
    mixin.ingest_queries_bulk(pairs, threads=4)
    assert len(mixin.looked_up) == 20 and len(mixin.created) == 20


def test_new_shapes_are_masked_in_worker_processes_when_there_are_enough():
    query_mixin._masked_by_shape.clear()
    mixin = _Mixin()
    n = query_mixin.MASK_POOL_THRESHOLD
    pairs = [_pair(f"SELECT c{i} FROM s.t{i} WHERE id = {i}") for i in range(n)]
    with patch.object(query_mixin, "mask_query") as inline:
        mixin.ingest_queries_bulk(pairs, threads=1, processes=2)
    assert inline.call_count == 0  # every shape came back from the pool, nothing masked inline
    assert len(query_mixin._masked_by_shape) == n and len(mixin.created) == n
