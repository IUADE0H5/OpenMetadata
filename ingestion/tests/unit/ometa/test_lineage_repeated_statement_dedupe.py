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
A statement that runs many times a day produces the same lineage edge many times per run. The
edge lookup is LRU-cached, so unless the cache follows what was just written every later write
merges against a stale copy and the JSON patch re-adds the same column pairs - one duplicate per
execution (a real edge reached 552 pairs for 23 columns).
"""

from unittest.mock import MagicMock

import pytest

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.basic import FullyQualifiedEntityName
from metadata.generated.schema.type.entityLineage import ColumnLineage, EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityLineage import Source as LineageSource
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.mixins.lineage_mixin import OMetaLineageMixin, search_cache

FROM_ID = "d311bdf2-c4a9-4be3-9937-3b26309759af"
TO_ID = "abea43f7-ccc2-4daf-9dfb-115549461244"
RAW = "svc.db.raw.events"
CURATED = "svc.db.curated.events"


class CachedLineage(OMetaLineageMixin):
    """Real merge, real patch construction, real edge cache; only HTTP is stubbed.
    `server_edge` is what the server would return on the first lookup."""

    def __init__(self, server_edge):
        self.client = MagicMock()
        self.client.patch = MagicMock(return_value={"ok": True})
        self._server_edge = server_edge
        self.lookups = 0

    def _get_lineage_edge_for_references(self, from_entity, to_entity):
        key = self._lineage_edge_cache_key(from_entity, to_entity)
        if key in search_cache:
            return search_cache.get(key)
        self.lookups += 1
        if self._server_edge is not None:
            search_cache.put(key, self._server_edge)
        return self._server_edge

    def get_lineage_edge_by_name(self, from_type, from_fqn, to_type, to_fqn):
        key = self._lineage_edge_name_cache_key(from_type, from_fqn, to_type, to_fqn)
        if key in search_cache:
            return search_cache.get(key)
        self.lookups += 1
        if self._server_edge is not None:
            search_cache.put(key, self._server_edge)
        return self._server_edge

    def get_suffix(self, entity):
        return "/lineage"


@pytest.fixture(autouse=True)
def _clear_cache():
    search_cache.clear()
    yield
    search_cache.clear()


def _pairs(*names):
    return [
        ColumnLineage(
            fromColumns=[FullyQualifiedEntityName(f"{RAW}.after_metadata.{n}")],
            toColumn=FullyQualifiedEntityName(f"{CURATED}.metadata_{n}"),
        )
        for n in names
    ]


def _request(*names, sql="MERGE INTO curated ..."):
    return AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=FROM_ID, type="table", fullyQualifiedName=RAW),
            toEntity=EntityReference(id=TO_ID, type="table", fullyQualifiedName=CURATED),
            lineageDetails=LineageDetails(
                source=LineageSource.QueryLineage, sqlQuery=sql, columnsLineage=_pairs(*names)
            ),
        )
    )


def test_merge_collapses_duplicates_already_stored_and_keeps_order():
    mixin = CachedLineage(server_edge=None)
    stored = [
        {"fromColumns": ["a"], "toColumn": "x", "function": None},
        {"fromColumns": ["b"], "toColumn": "y"},
        {"fromColumns": ["a"], "toColumn": "x"},
    ]

    merged = mixin._merge_column_lineage(stored, [{"fromColumns": ["c"], "toColumn": "z"}])

    assert [(m["fromColumns"], m["toColumn"]) for m in merged] == [(["a"], "x"), (["b"], "y"), (["c"], "z")]


def test_repeated_statement_patches_an_existing_edge_once():
    mixin = CachedLineage(server_edge={"edge": {"columnsLineage": []}})

    for _ in range(24):
        mixin.add_lineage(_request("eventid", "eventname"), check_patch=True, return_lineage=False)

    assert mixin.client.patch.call_count == 1
    assert mixin.client.put.call_count == 0
    assert mixin.lookups == 1


def test_repeated_statement_creates_a_new_edge_once():
    mixin = CachedLineage(server_edge=None)

    for _ in range(24):
        mixin.add_lineage(_request("eventid", "eventname"), check_patch=True, return_lineage=False)

    assert mixin.client.put.call_count == 1
    assert mixin.client.patch.call_count == 0


def test_new_columns_on_a_later_statement_are_still_patched_in():
    mixin = CachedLineage(server_edge={"edge": {"columnsLineage": []}})
    mixin.add_lineage(_request("eventid"), check_patch=True, return_lineage=False)
    mixin.add_lineage(_request("eventid", "eventname"), check_patch=True, return_lineage=False)
    mixin.add_lineage(_request("eventid", "eventname"), check_patch=True, return_lineage=False)

    assert mixin.client.patch.call_count == 2


def test_repeated_statement_by_name_patches_once():
    mixin = CachedLineage(server_edge={"edge": {"columnsLineage": []}})
    details = LineageDetails(source=LineageSource.QueryLineage, sqlQuery="MERGE ...", columnsLineage=_pairs("eventid"))

    for _ in range(10):
        mixin.add_lineage_by_name(
            RAW, "table", CURATED, "table", lineage_details=details, check_patch=True, return_lineage=False
        )

    assert mixin.client.patch.call_count == 1
    assert mixin.client.put.call_count == 0
