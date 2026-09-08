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
Deduplicated bulk ingestion of table queries: one lookup per distinct query, PUT /queries/bulk for
new ones, additive relation PUTs for existing ones. The HTTP client is the boundary being faked.
"""

import hashlib
import json
from uuid import uuid4

from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.entity.data.query import Query
from metadata.generated.schema.type.basic import FullyQualifiedEntityName, SqlQuery
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.lineage.masker import mask_query
from metadata.ingestion.ometa.mixins.query_mixin import OMetaQueryMixin
from metadata.ingestion.ometa.ometa_api import OpenMetadata

SERVICE = "svc"
SERVICE_REF = EntityReference(id=uuid4(), type="databaseService", name=SERVICE)


class FakeREST:
    def __init__(self):
        self.puts = []
        self.fail_bulk = False

    def put(self, path, data=None, json=None):
        self.puts.append((path, json if json is not None else data))
        if path.startswith("/queries/bulk"):
            failed = len(json) if self.fail_bulk else 0
            return {
                "numberOfRowsProcessed": len(json) - failed,
                "numberOfRowsFailed": failed,
                "successRequest": [],
                "failedRequest": [],
            }
        if path == "/queries":
            body = json_loads(data)
            return {
                "id": str(uuid4()),
                "name": hashlib.md5(body["query"].encode()).hexdigest(),
                "query": body["query"],
                "service": SERVICE_REF.model_dump(mode="json", exclude_none=True),
            }
        return {}


def json_loads(data):
    return json.loads(data)


class FakeOMeta(OMetaQueryMixin):
    """Just enough of OpenMetadata for the mixin: a client, a name lookup and the bulk helper."""

    get_suffix = staticmethod(OpenMetadata.get_suffix)
    bulk_create_or_update = OpenMetadata.bulk_create_or_update
    _execute_bulk_operation = OpenMetadata._execute_bulk_operation
    _group_entities_by_type = OpenMetadata._group_entities_by_type

    def __init__(self, existing=None):
        self.client = FakeREST()
        self.existing = existing or {}
        self.lookups = []

    def get_by_name(self, entity, fqn, fields=None, nullable=True, include=None):
        self.lookups.append((fqn, tuple(fields or ())))
        return self.existing.get(fqn)


def _hash(text):
    """Queries are stored masked; the checksum is taken over the masked text."""
    return hashlib.md5(mask_query(text, None).encode()).hexdigest()


def _request(sql, users=None, used_by=None, exclude_usage=False):
    return CreateQueryRequest(
        query=SqlQuery(sql),
        service=FullyQualifiedEntityName(SERVICE),
        users=[FullyQualifiedEntityName(u) for u in users] if users else None,
        usedBy=used_by,
        exclude_usage=exclude_usage,
    )


def _table_ref(table_id=None):
    return EntityReference(id=table_id or uuid4(), type="table")


def _bulk_payloads(client):
    return [payload for path, payload in client.puts if path.startswith("/queries/bulk")]


def test_distinct_queries_are_looked_up_once_and_created_in_one_bulk_call_with_merged_usage():
    api = FakeOMeta()
    t1, t2 = _table_ref(), _table_ref()

    api.ingest_queries_bulk(
        [
            (_request("SELECT 1", users=["alice"]), t1),
            (_request("SELECT 1", users=["bob"], used_by=["airflow"]), t2),
            (_request("SELECT b FROM other"), t1),
        ]
    )

    assert [fqn for fqn, _ in api.lookups] == [
        f"{SERVICE}.{_hash('SELECT 1')}",
        f"{SERVICE}.{_hash('SELECT b FROM other')}",
    ]
    assert all(fields == ("queryUsedIn", "users") for _, fields in api.lookups)
    (payload,) = _bulk_payloads(api.client)
    assert [q["query"] for q in payload] == [mask_query("SELECT 1", None), mask_query("SELECT b FROM other", None)]
    assert {ref["id"] for ref in payload[0]["queryUsedIn"]} == {str(t1.id.root), str(t2.id.root)}
    assert payload[0]["users"] == ["alice", "bob"]
    assert payload[0]["usedBy"] == ["airflow"]
    assert [ref["id"] for ref in payload[1]["queryUsedIn"]] == [str(t1.id.root)]
    assert "users" not in payload[1]
    assert not [p for p, _ in api.client.puts if not p.startswith("/queries/bulk")]


def test_existing_query_only_receives_the_relations_it_lacks():
    known_table, new_table = _table_ref(), _table_ref()
    existing = Query(
        id=uuid4(),
        name=_hash("SELECT 1"),
        query=SqlQuery("SELECT 1"),
        service=SERVICE_REF,
        queryUsedIn=[known_table],
        users=[EntityReference(id=uuid4(), type="user", fullyQualifiedName="alice")],
        usedBy=["airflow"],
    )
    api = FakeOMeta(existing={f"{SERVICE}.{_hash('SELECT 1')}": existing})

    api.ingest_queries_bulk(
        [
            (_request("SELECT 1", users=["alice", "carol"], used_by=["airflow", "dbt"]), known_table),
            (_request("SELECT 1"), new_table),
        ]
    )

    assert _bulk_payloads(api.client) == []
    puts = dict(api.client.puts)
    query_id = str(existing.id.root)
    assert [ref["id"] for ref in json.loads(puts[f"/queries/{query_id}/usage"])] == [str(new_table.id.root)]
    assert json.loads(puts[f"/queries/{query_id}/users"]) == ["carol"]
    assert json.loads(puts[f"/queries/{query_id}/usedBy"]) == ["dbt"]


def test_existing_query_with_nothing_missing_causes_no_writes():
    table = _table_ref()
    existing = Query(
        id=uuid4(), name=_hash("SELECT 1"), query=SqlQuery("SELECT 1"), service=SERVICE_REF, queryUsedIn=[table]
    )
    api = FakeOMeta(existing={f"{SERVICE}.{_hash('SELECT 1')}": existing})

    api.ingest_queries_bulk([(_request("SELECT 1"), table)])

    assert api.client.puts == []


def test_excluded_queries_never_reach_the_server():
    api = FakeOMeta()

    api.ingest_queries_bulk([(_request("INSERT INTO t VALUES (1)", exclude_usage=True), _table_ref())])

    assert api.lookups == [] and api.client.puts == []


def test_batches_respect_the_batch_size():
    api = FakeOMeta()

    api.ingest_queries_bulk([(_request(f"SELECT c{i} FROM t{i}"), _table_ref()) for i in range(5)], batch_size=2)

    assert [len(p) for p in _bulk_payloads(api.client)] == [2, 2, 1]


def test_failed_bulk_batch_falls_back_to_the_per_query_path():
    api = FakeOMeta()
    api.client.fail_bulk = True
    table = _table_ref()

    api.ingest_queries_bulk([(_request("SELECT 1", users=["alice"]), table)])

    paths = [p for p, _ in api.client.puts]
    assert paths[0].startswith("/queries/bulk")
    assert paths[1] == "/queries"
    assert paths[2].endswith("/usage") and paths[3].endswith("/users")


def test_get_or_create_query_looks_up_by_service_qualified_name():
    api = FakeOMeta()

    api._get_or_create_query(_request("SELECT 1"))

    # the legacy path receives already-masked text, so the checksum is over the text as given
    assert api.lookups[0][0] == f"{SERVICE}.{hashlib.md5(b'SELECT 1').hexdigest()}"
