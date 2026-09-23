#  Copyright 2026 Collate
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
Test the Fabric getDefinition client: LRO polling, cache-by-lastUpdatedTimeUtc,
transport-error retry, and bounded parallel fetch.
"""

import base64
from unittest.mock import MagicMock, patch

import pytest
import requests

from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import (
    PowerBIConnection,
)
from metadata.ingestion.source.dashboard.powerbi.fabric_client import (
    FabricApiClient,
    FabricDefinitionResult,
)

CONNECTION_CONFIG = {
    "clientId": "client_id",
    "clientSecret": "client_secret",
    "tenantId": "tenant_id",
}


def _response(status_code: int, json_body=None, headers=None, text=""):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = text
    resp.json.return_value = json_body or {}
    resp.raise_for_status = MagicMock()
    return resp


def _definition_body(parts):
    """Build a getDefinition-shaped result body from {path: raw_str}."""
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(raw.encode("utf-8")).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path, raw in parts.items()
            ]
        }
    }


@pytest.fixture
def client():
    connection = PowerBIConnection.model_validate(CONNECTION_CONFIG)
    api_client = FabricApiClient(connection)
    # `msal_client` is built lazily (see its docstring) - patch `msal` before this
    # first access so building it can't make a real network call in a test.
    with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.msal"):
        api_client.msal_client.acquire_token_silent = MagicMock(return_value={"access_token": "tok"})
    api_client._session = MagicMock()  # pylint: disable=protected-access
    return api_client


class TestMsalClientIsLazy:
    def test_construction_never_builds_msal_client(self):
        """`FabricApiClient.__init__` must do zero network I/O - `msal` is not
        patched here at all, so if construction touched it, this would hang or
        fail against the real Microsoft endpoint instead of just passing."""
        connection = PowerBIConnection.model_validate(CONNECTION_CONFIG)
        api_client = FabricApiClient(connection)
        assert api_client._msal_client is None  # pylint: disable=protected-access

    def test_msal_client_built_once_on_first_access(self):
        connection = PowerBIConnection.model_validate(CONNECTION_CONFIG)
        api_client = FabricApiClient(connection)
        with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.msal") as mock_msal:
            first = api_client.msal_client
            second = api_client.msal_client
        assert first is second
        mock_msal.ConfidentialClientApplication.assert_called_once()


class TestGetDefinitionSync:
    """A 200 response returns the definition inline, no polling."""

    def test_200_response_decodes_parts_without_polling(self, client):
        client._session.post.return_value = _response(200, _definition_body({"report.json": "{}"}))

        result = client.get_report_definition("ws1", "rep1")

        assert result == FabricDefinitionResult(parts={"report.json": b"{}"}, from_cache=False)
        client._session.get.assert_not_called()


class TestLroPolling:
    """202 -> Location; poll until Succeeded, then GET .../result."""

    def test_polls_until_succeeded_then_fetches_result(self, client):
        post_resp = _response(202, headers={"Location": "https://fabric/ops/op1", "Retry-After": "0"})
        running_poll = _response(200, {"status": "Running"})
        succeeded_poll = _response(200, {"status": "Succeeded"})
        result_resp = _response(200, _definition_body({"model.tmdl": "table Sales"}))

        client._session.post.return_value = post_resp
        client._session.get.side_effect = [running_poll, succeeded_poll, result_resp]

        with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.sleep"):
            result = client.get_semantic_model_definition("ws1", "model1")

        assert result.parts == {"model.tmdl": b"table Sales"}
        assert result.from_cache is False
        assert client._session.get.call_count == 3
        # The 3rd GET is the result fetch, at .../result off the operation location.
        result_call_url = client._session.get.call_args_list[2].args[0]
        assert result_call_url == "https://fabric/ops/op1/result"

    def test_lro_failed_status_gives_up_without_result_fetch(self, client):
        post_resp = _response(202, headers={"Location": "https://fabric/ops/op2", "Retry-After": "0"})
        failed_poll = _response(200, {"status": "Failed"})
        client._session.post.return_value = post_resp
        client._session.get.return_value = failed_poll

        with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.sleep"):
            result = client.get_report_definition("ws1", "rep2")

        assert result is None
        # Only the status poll, never a .../result fetch.
        assert client._session.get.call_count == 1

    def test_lro_never_succeeding_times_out(self, client):
        post_resp = _response(202, headers={"Location": "https://fabric/ops/op3", "Retry-After": "0"})
        running_poll = _response(200, {"status": "Running"})
        client._session.post.return_value = post_resp
        client._session.get.return_value = running_poll

        with (
            patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.sleep"),
            patch(
                "metadata.ingestion.source.dashboard.powerbi.fabric_client.MAX_POLL_ATTEMPTS",
                3,
            ),
        ):
            result = client.get_report_definition("ws1", "rep3")

        assert result is None
        assert client._session.get.call_count == 3


class TestTransportErrorRetry:
    """A transport error (e.g. ReadTimeout) mid-poll retries the whole operation."""

    def test_read_timeout_during_poll_retries_whole_operation(self, client):
        post_resp = _response(202, headers={"Location": "https://fabric/ops/op4", "Retry-After": "0"})
        succeeded_poll = _response(200, {"status": "Succeeded"})
        result_resp = _response(200, _definition_body({"report.json": "{}"}))

        client._session.post.return_value = post_resp
        # First attempt: POST succeeds (202), then the poll GET times out.
        # Second attempt: the whole operation (POST -> poll -> result) succeeds.
        client._session.get.side_effect = [
            requests.exceptions.ReadTimeout("timed out"),
            succeeded_poll,
            result_resp,
        ]

        with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.sleep"):
            result = client.get_report_definition("ws1", "rep4")

        assert result.parts == {"report.json": b"{}"}
        # POST re-issued for the retried attempt.
        assert client._session.post.call_count == 2

    def test_gives_up_after_max_operation_attempts(self, client):
        client._session.post.side_effect = requests.exceptions.ConnectionError("refused")

        with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.sleep"):
            result = client.get_report_definition("ws1", "rep5")

        assert result is None
        from metadata.ingestion.source.dashboard.powerbi.fabric_client import (
            MAX_OPERATION_ATTEMPTS,
        )

        assert client._session.post.call_count == MAX_OPERATION_ATTEMPTS


class TestDefinitionCache:
    """Cache by (item id, lastUpdatedTimeUtc): unchanged items are skipped within a run."""

    def test_same_last_updated_time_is_served_from_cache(self, client):
        client._session.post.return_value = _response(200, _definition_body({"report.json": "{}"}))

        first = client.get_report_definition("ws1", "rep1", last_updated_time_utc="2026-01-01T00:00:00Z")
        second = client.get_report_definition("ws1", "rep1", last_updated_time_utc="2026-01-01T00:00:00Z")

        assert first.from_cache is False
        assert second.from_cache is True
        assert second.parts == first.parts
        client._session.post.assert_called_once()

    def test_changed_last_updated_time_refetches(self, client):
        client._session.post.return_value = _response(200, _definition_body({"report.json": "{}"}))

        client.get_report_definition("ws1", "rep1", last_updated_time_utc="2026-01-01T00:00:00Z")
        second = client.get_report_definition("ws1", "rep1", last_updated_time_utc="2026-02-01T00:00:00Z")

        assert second.from_cache is False
        assert client._session.post.call_count == 2

    def test_failed_fetch_is_not_cached(self, client):
        client._session.post.side_effect = requests.exceptions.ConnectionError("refused")

        with patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.sleep"):
            first = client.get_report_definition("ws1", "rep1", last_updated_time_utc="t1")

        assert first is None
        assert "rep1" not in [key[0] for key in client._definitions_cache]


class TestParallelFetch:
    def test_fetch_definitions_returns_results_in_order(self, client):
        results = client.fetch_definitions(
            [
                lambda: FabricDefinitionResult(parts={"a": b"1"}, from_cache=False),
                lambda: FabricDefinitionResult(parts={"b": b"2"}, from_cache=False),
                lambda: None,
            ]
        )
        assert results == [
            FabricDefinitionResult(parts={"a": b"1"}, from_cache=False),
            FabricDefinitionResult(parts={"b": b"2"}, from_cache=False),
            None,
        ]

    def test_fetch_definitions_caps_worker_pool(self, client):
        import metadata.ingestion.source.dashboard.powerbi.fabric_client as fabric_client_module

        seen_max_workers = {}
        real_executor = fabric_client_module.ThreadPoolExecutor

        class RecordingExecutor(real_executor):
            def __init__(self, *args, max_workers=None, **kwargs):
                seen_max_workers["value"] = max_workers
                super().__init__(*args, max_workers=max_workers, **kwargs)

        with patch.object(fabric_client_module, "ThreadPoolExecutor", RecordingExecutor):
            client.fetch_definitions([lambda: None] * 10)

        assert seen_max_workers["value"] == fabric_client_module.MAX_PARALLEL_REQUESTS

    def test_fetch_definitions_empty_is_noop(self, client):
        assert client.fetch_definitions([]) == []


class TestDecodeParts:
    def test_non_base64_payload_type_kept_as_utf8(self, client):
        body = {"definition": {"parts": [{"path": "note.txt", "payload": "hello", "payloadType": "Raw"}]}}
        assert client._decode_parts(body) == {"note.txt": b"hello"}  # pylint: disable=protected-access

    def test_missing_definition_decodes_empty(self, client):
        assert client._decode_parts({}) == {}  # pylint: disable=protected-access
        assert client._decode_parts(None) == {}  # pylint: disable=protected-access


class TestListLastUpdated:
    """`list_reports_last_updated` / `list_semantic_models_last_updated`: pagination
    via `continuationUri`, honest reporting of a missing field, and a clean `None`
    (never a partial mapping) on failure."""

    def test_single_page(self, client):
        client._session.get.return_value = _response(
            200,
            {"value": [{"id": "r1", "lastUpdatedTimeUtc": "2026-01-01T00:00:00Z"}, {"id": "r2"}]},
        )

        result = client.list_reports_last_updated("ws1")

        assert result == {"r1": "2026-01-01T00:00:00Z", "r2": None}
        client._session.get.assert_called_once_with(
            "https://api.fabric.microsoft.com/v1/workspaces/ws1/reports",
            headers={"Authorization": "Bearer tok"},
            timeout=60,
        )

    def test_follows_continuation_uri_across_pages(self, client):
        page1 = _response(
            200,
            {
                "value": [{"id": "m1", "lastUpdatedTimeUtc": "t1"}],
                "continuationUri": "https://api.fabric.microsoft.com/v1/workspaces/ws1/semanticModels?token=abc",
            },
        )
        page2 = _response(200, {"value": [{"id": "m2", "lastUpdatedTimeUtc": "t2"}]})
        client._session.get.side_effect = [page1, page2]

        result = client.list_semantic_models_last_updated("ws1")

        assert result == {"m1": "t1", "m2": "t2"}
        assert client._session.get.call_count == 2
        second_call_url = client._session.get.call_args_list[1].args[0]
        assert second_call_url == "https://api.fabric.microsoft.com/v1/workspaces/ws1/semanticModels?token=abc"

    def test_transport_error_returns_none_not_partial(self, client):
        page1 = _response(200, {"value": [{"id": "r1", "lastUpdatedTimeUtc": "t1"}], "continuationUri": "https://x"})
        client._session.get.side_effect = [page1, requests.exceptions.ConnectionError("refused")]

        result = client.list_reports_last_updated("ws1")

        assert result is None

    def test_http_error_returns_none(self, client):
        resp = _response(500, {})
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        client._session.get.return_value = resp

        assert client.list_reports_last_updated("ws1") is None

    def test_no_auth_token_returns_none_without_a_call(self, client):
        client.msal_client.acquire_token_silent = MagicMock(return_value=None)
        client.msal_client.acquire_token_for_client = MagicMock(return_value=None)

        assert client.list_reports_last_updated("ws1") is None
        client._session.get.assert_not_called()
