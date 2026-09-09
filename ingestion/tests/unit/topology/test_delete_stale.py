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
Validate that stale-entity detection is delegated to the server and only falls back to the
legacy client-side paginate-and-diff against older servers.
"""

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from metadata.generated.schema.type.bulkOperationResult import BulkOperationResult
from metadata.ingestion.api.delete import delete_entity_from_source
from metadata.ingestion.api.models import Either
from metadata.ingestion.models.barrier import Barrier
from metadata.ingestion.ometa.client import APIError
from metadata.ingestion.ometa.ometa_api import OpenMetadata


class MockEntity:
    """Stand-in entity type; only used as a key for get_suffix / list_all_entities."""


class _Fqn(BaseModel):
    root: str


class _MockEntity(BaseModel):
    """A minimal pydantic Entity for the legacy diff path (DeleteEntity.entity is a BaseModel)."""

    fullyQualifiedName: _Fqn  # noqa: N815


def _entity(fqn: str) -> _MockEntity:
    return _MockEntity(fullyQualifiedName=_Fqn(root=fqn))


class TestDeleteStaleEntitiesMixin:
    """Unit tests for OpenMetadata.delete_stale_entities."""

    def test_builds_request_and_returns_result(self):
        metadata = MagicMock()
        metadata.get_suffix.return_value = "/tables"
        metadata.client.delete.return_value = {
            "status": "success",
            "numberOfRowsProcessed": 1,
            "numberOfRowsPassed": 1,
            "numberOfRowsFailed": 0,
        }

        result = OpenMetadata.delete_stale_entities(
            metadata,
            entity=MockEntity,
            scope_params={"databaseSchema": "svc.db.sch"},
            live_fqns={"svc.db.sch.t1", "svc.db.sch.t2"},
            recursive=True,
        )

        assert isinstance(result, BulkOperationResult)
        metadata.client.delete.assert_called_once()
        url, kwargs = (
            metadata.client.delete.call_args.args[0],
            metadata.client.delete.call_args.kwargs,
        )
        assert url == "/tables/deleteStale"
        body = kwargs["json"]
        assert body["scopeFqn"] == "svc.db.sch"
        assert body["scopeEntityType"] == "databaseSchema"
        assert body["recursive"] is True
        assert sorted(body["seenFqns"]) == ["svc.db.sch.t1", "svc.db.sch.t2"]

    def test_returns_none_when_endpoint_missing(self):
        """A 404 from the server means the endpoint is unavailable - fall back, don't raise."""
        metadata = MagicMock()
        metadata.get_suffix.return_value = "/tables"
        http_error = MagicMock()
        http_error.response.status_code = 404
        metadata.client.delete.side_effect = APIError({"message": "not found"}, http_error)

        result = OpenMetadata.delete_stale_entities(
            metadata,
            entity=MockEntity,
            scope_params={"database": "svc.db"},
            live_fqns=[],
            recursive=True,
        )

        assert result is None

    def test_raises_on_non_404_errors(self):
        metadata = MagicMock()
        metadata.get_suffix.return_value = "/tables"
        http_error = MagicMock()
        http_error.response.status_code = 500
        metadata.client.delete.side_effect = APIError({"message": "boom"}, http_error)

        with pytest.raises(APIError):
            OpenMetadata.delete_stale_entities(
                metadata,
                entity=MockEntity,
                scope_params={"database": "svc.db"},
                live_fqns=[],
            )

    def test_missing_scope_returns_empty_result(self):
        """A missing scope is a server-side no-op: it returns 200 + an empty BulkOperationResult
        (zero rows), which the client passes through so the caller does not fall back to legacy."""
        metadata = MagicMock()
        metadata.get_suffix.return_value = "/tables"
        metadata.client.delete.return_value = {
            "status": "success",
            "numberOfRowsProcessed": 0,
            "numberOfRowsPassed": 0,
            "numberOfRowsFailed": 0,
        }

        result = OpenMetadata.delete_stale_entities(
            metadata,
            entity=MockEntity,
            scope_params={"databaseSchema": "svc.db.sch"},
            live_fqns=[],
        )

        assert isinstance(result, BulkOperationResult)
        assert result.numberOfRowsProcessed.root == 0

    def test_requires_scope(self):
        metadata = MagicMock()
        with pytest.raises(ValueError):
            OpenMetadata.delete_stale_entities(
                metadata,
                entity=MockEntity,
                scope_params=None,
                live_fqns=[],
            )


class TestDeleteEntityFromSource:
    """Unit tests for delete_entity_from_source - server-first with a legacy fallback."""

    def test_delegates_to_server_and_yields_nothing(self):
        """When the server handles stale deletion, nothing is pushed through the sink."""
        metadata = MagicMock()
        metadata.delete_stale_entities.return_value = BulkOperationResult(
            numberOfRowsProcessed=2, numberOfRowsPassed=2, numberOfRowsFailed=0
        )

        results = list(
            delete_entity_from_source(
                metadata=metadata,
                entity_type=MockEntity,
                entity_source_state={"svc.db.sch.t1"},
                recursive=True,
                params={"databaseSchema": "svc.db.sch"},
            )
        )

        barriers = [r for r in results if isinstance(r.right, Barrier)]
        deletes = [r for r in results if not isinstance(r.right, Barrier)]
        assert len(barriers) == 1
        assert deletes == []
        metadata.delete_stale_entities.assert_called_once_with(
            entity=MockEntity,
            scope_params={"databaseSchema": "svc.db.sch"},
            live_fqns={"svc.db.sch.t1"},
            recursive=True,
        )
        # The connector must not paginate the API itself when the server handled it.
        metadata.list_all_entities.assert_not_called()

    def test_falls_back_to_legacy_when_endpoint_missing(self):
        """An old server (delete_stale_entities returns None) falls back to client-side diff."""
        metadata = MagicMock()
        metadata.delete_stale_entities.return_value = None
        metadata.list_all_entities.return_value = [
            _entity("svc.db.sch.t1"),
            _entity("svc.db.sch.t2"),
            _entity("svc.db.sch.t3"),
        ]

        results = list(
            delete_entity_from_source(
                metadata=metadata,
                entity_type=MockEntity,
                entity_source_state={"svc.db.sch.t1"},
                recursive=True,
                params={"databaseSchema": "svc.db.sch"},
            )
        )

        metadata.list_all_entities.assert_called_once()
        deleted_fqns = {r.right.entity.fullyQualifiedName.root for r in results if not isinstance(r.right, Barrier)}
        assert deleted_fqns == {"svc.db.sch.t2", "svc.db.sch.t3"}


def _scope_sent_to_server(params):
    """Run a stale deletion and return the scope dict that reached delete_stale_entities."""
    metadata = MagicMock()
    metadata.delete_stale_entities.return_value = BulkOperationResult(
        numberOfRowsProcessed=1, numberOfRowsPassed=1, numberOfRowsFailed=0
    )
    list(
        delete_entity_from_source(
            metadata=metadata,
            entity_type=MockEntity,
            entity_source_state={"svc.d1"},
            recursive=True,
            params=params,
        )
    )
    return metadata.delete_stale_entities.call_args.kwargs["scope_params"]


class TestServiceScopeIsSentAsFqn:
    """
    deleteStale resolves the scope against the stored FQN. Callers pass the service *name*, which
    for a name needing quotes (a dot, e.g. a version number) is not the FQN - so the scope never
    resolved and stale deletion silently deleted nothing.
    """

    def test_service_name_with_a_dot_is_sent_quoted(self):
        assert _scope_sent_to_server({"service": "Looker- 2.0 Test"}) == {"service": '"Looker- 2.0 Test"'}

    def test_service_name_without_a_dot_is_sent_unchanged(self):
        assert _scope_sent_to_server({"service": "Looker Prod"}) == {"service": "Looker Prod"}

    def test_already_quoted_service_name_is_not_double_quoted(self):
        assert _scope_sent_to_server({"service": '"Looker- 2.0 Test"'}) == {"service": '"Looker- 2.0 Test"'}

    def test_non_service_scopes_are_never_quoted(self):
        """These already arrive as FQNs; quoting a multi-part FQN would break a working scope."""
        assert _scope_sent_to_server({"databaseSchema": "svc.db.sch"}) == {"databaseSchema": "svc.db.sch"}
        assert _scope_sent_to_server({"database": "svc.db"}) == {"database": "svc.db"}

    def test_service_stays_the_first_key(self):
        """delete_stale_entities reads the scope from the first entry, so order must survive."""
        scope = _scope_sent_to_server({"service": "Drive. 2.0", "directory": "d1"})
        assert next(iter(scope.items())) == ("service", '"Drive. 2.0"')

    def test_caller_params_are_not_mutated(self):
        """The legacy fallback reuses the caller's dict and needs the raw name in it."""
        params = {"service": "Looker- 2.0 Test"}
        _scope_sent_to_server(params)
        assert params == {"service": "Looker- 2.0 Test"}

    def test_unquotable_service_name_is_passed_through(self):
        """A name quote_name rejects must not blow up the whole delete stage."""
        assert _scope_sent_to_server({"service": 'Looker "Prod"'}) == {"service": 'Looker "Prod"'}

    def test_legacy_fallback_lists_by_the_raw_service_name(self):
        """`?service=` matches a service by name, so the fallback must not get the quoted FQN."""
        metadata = MagicMock()
        metadata.delete_stale_entities.return_value = None
        metadata.list_all_entities.return_value = [_entity("svc.d1"), _entity("svc.d2")]

        results = list(
            delete_entity_from_source(
                metadata=metadata,
                entity_type=MockEntity,
                entity_source_state={"svc.d1"},
                recursive=True,
                params={"service": "Looker- 2.0 Test"},
            )
        )

        assert metadata.list_all_entities.call_args.kwargs["params"] == {"service": "Looker- 2.0 Test"}
        deleted_fqns = {r.right.entity.fullyQualifiedName.root for r in results if not isinstance(r.right, Barrier)}
        assert deleted_fqns == {"svc.d2"}

    def test_quoted_fqn_reaches_the_wire(self):
        """End to end through the real client: the request body carries the service FQN."""
        metadata = MagicMock()
        metadata.get_suffix.return_value = "/dashboards"
        metadata.client.delete.return_value = {
            "status": "success",
            "numberOfRowsProcessed": 3,
            "numberOfRowsPassed": 3,
            "numberOfRowsFailed": 0,
        }
        metadata.delete_stale_entities = lambda **kwargs: OpenMetadata.delete_stale_entities(metadata, **kwargs)

        list(
            delete_entity_from_source(
                metadata=metadata,
                entity_type=MockEntity,
                entity_source_state={'"Looker- 2.0 Test".1'},
                recursive=True,
                params={"service": "Looker- 2.0 Test"},
            )
        )

        body = metadata.client.delete.call_args.kwargs["json"]
        assert body["scopeFqn"] == '"Looker- 2.0 Test"'
        assert body["scopeEntityType"] == "service"


class TestStaleDeleteGuard:
    """A run that produced nothing for a scope must not empty that scope unless told it may."""

    @staticmethod
    def _metadata(live_total: int):
        metadata = MagicMock()
        metadata.get_suffix.return_value = "/tables"
        metadata.delete_stale_entities.return_value = BulkOperationResult(
            status="success", numberOfRowsProcessed=0, numberOfRowsPassed=0, numberOfRowsFailed=0
        )
        metadata.list_entities.return_value = MagicMock(total=live_total)
        return metadata

    @staticmethod
    def _run(metadata, seen, guard, scope="svc.db.sch"):
        from metadata.ingestion.api.delete import delete_entity_from_source

        results = list(
            delete_entity_from_source(
                metadata=metadata,
                entity_type=MockEntity,
                entity_source_state=seen,
                recursive=True,
                params={"databaseSchema": scope},
                guard=guard,
            )
        )
        return [r.left for r in results if isinstance(r, Either) and r.left is not None]

    def test_a_scope_the_run_saw_tables_in_is_reconciled_normally(self):
        from metadata.ingestion.api.delete import StaleDeleteGuard

        metadata = self._metadata(live_total=5)
        errors = self._run(metadata, {"svc.db.sch.t1"}, StaleDeleteGuard())
        assert errors == []
        metadata.delete_stale_entities.assert_called_once()
        metadata.list_entities.assert_not_called()

    def test_a_scope_with_nothing_seen_and_nothing_live_needs_no_delete(self):
        from metadata.ingestion.api.delete import StaleDeleteGuard

        metadata = self._metadata(live_total=0)
        errors = self._run(metadata, set(), StaleDeleteGuard())
        assert errors == []
        metadata.delete_stale_entities.assert_not_called()

    def test_emptying_a_scope_is_refused_by_default_and_reported(self):
        from metadata.ingestion.api.delete import StaleDeleteGuard

        metadata = self._metadata(live_total=524)
        errors = self._run(metadata, {"svc.db.other.t1"}, StaleDeleteGuard())
        metadata.delete_stale_entities.assert_not_called()
        assert len(errors) == 1
        assert "svc.db.sch" in errors[0].name
        assert "524" in errors[0].error and "allowEmptyingSchema" in errors[0].error

    def test_the_flag_allows_one_scope_to_be_emptied_per_run(self):
        from metadata.ingestion.api.delete import StaleDeleteGuard

        guard = StaleDeleteGuard(allow_empty_scope=True)
        first = self._metadata(live_total=10)
        assert self._run(first, set(), guard, scope="svc.db.a") == []
        first.delete_stale_entities.assert_called_once()
        second = self._metadata(live_total=10)
        errors = self._run(second, set(), guard, scope="svc.db.b")
        second.delete_stale_entities.assert_not_called()
        assert len(errors) == 1 and "allowEmptyingMultipleSchemas" in errors[0].error

    def test_the_second_flag_restores_the_unrestricted_behaviour(self):
        from metadata.ingestion.api.delete import StaleDeleteGuard

        guard = StaleDeleteGuard(allow_empty_scope=True, allow_multiple_empty_scopes=True)
        for scope in ("svc.db.a", "svc.db.b", "svc.db.c"):
            metadata = self._metadata(live_total=10)
            assert self._run(metadata, set(), guard, scope=scope) == []
            metadata.delete_stale_entities.assert_called_once()

    def test_without_a_guard_nothing_changes(self):
        metadata = self._metadata(live_total=524)
        assert self._run(metadata, set(), None) == []
        metadata.delete_stale_entities.assert_called_once()
