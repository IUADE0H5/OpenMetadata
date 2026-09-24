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
Fabric REST client for Power BI report / semantic-model `getDefinition` calls.

Only used when `PowerbiSource.report_column_usage_enabled` is True (see metadata.py).
Separate from `PowerBiApiClient` (client.py): different host
(`api.fabric.microsoft.com`, not `api.powerbi.com`), a different token scope, and a
long-running-operation (LRO) response shape - a 202 with a `Location` to poll - that the
shared `TrackedREST`/`REST` JSON-body helpers don't expose. Same SPN credentials and
tenant as the Power BI client.

getDefinition call shape (verified against a live tenant):
    POST {base}/workspaces/{wid}/reports/{rid}/getDefinition
    POST {base}/workspaces/{wid}/semanticModels/{mid}/getDefinition?format=TMDL
Either returns 200 with the body inline, or 202 with `Location`/`Retry-After` headers to
poll (`GET {location}` until `status` is `Succeeded`/`Failed`, then
`GET {location}/result`). The result body is `{"definition": {"parts": [...]}}`, each part
carrying `path`, `payload` (base64 when `payloadType` is `InlineBase64`) and `payloadType`.
"""

import base64
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import sleep
from typing import Callable, Mapping, Optional, Sequence  # noqa: UP035

import msal
import requests
from cachetools import LRUCache

from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import (
    PowerBIConnection,
)
from metadata.ingestion.ometa.http_adapter import mount_resilient_adapter
from metadata.utils.logger import utils_logger

logger = utils_logger()

FABRIC_API_BASE_URL = "https://api.fabric.microsoft.com/v1"
# Fabric's own scope, not the Power BI `scope` connection field (that one stays
# `https://analysis.windows.net/powerbi/api/.default` and is not reused here).
FABRIC_SCOPE = ["https://api.fabric.microsoft.com/.default"]

AUTH_TOKEN_MAX_RETRIES = 5
AUTH_TOKEN_RETRY_WAIT_SECONDS = 120

# One poll takes ~11s against a live tenant; Fabric's own Retry-After is honoured
# (capped) rather than a fixed sleep. ~10 minutes of polling before giving up on
# one operation.
DEFAULT_POLL_RETRY_AFTER_SECONDS = 5
MAX_POLL_SLEEP_SECONDS = 10
MAX_POLL_ATTEMPTS = 60
POST_TIMEOUT_SECONDS = 120
POLL_TIMEOUT_SECONDS = 120
RESULT_TIMEOUT_SECONDS = 120
LIST_TIMEOUT_SECONDS = 60

# The whole getDefinition operation (POST + poll + result) is retried on a transport
# error - a `ReadTimeout` mid-poll has been observed live - rather than any single HTTP
# call: a timeout partway through an LRO leaves no cheap way to resume from where it
# failed, so the simplest correct thing is to start the operation over.
MAX_OPERATION_ATTEMPTS = 3
OPERATION_RETRY_WAIT_SECONDS = 5

# Fabric's quota is 200 calls/min per bucket; capped well under that so a normal
# ingestion run never needs a token-bucket limiter on top of this.
MAX_PARALLEL_REQUESTS = 4

# Bounded per CLAUDE.md's cache rule. One tenant's reports + models realistically stay
# far under this within a single run; beyond it we degrade to re-fetching rather than
# grow unbounded.
_MAX_CACHED_DEFINITIONS = 2_000

_INLINE_BASE64 = "InlineBase64"


@dataclass(frozen=True)
class FabricDefinitionResult:
    """Decoded `definition.parts[]` from one getDefinition call: path -> raw bytes.

    `from_cache=True` means this run already fetched the same (item id,
    lastUpdatedTimeUtc) pair and the network call was skipped.
    """

    parts: Mapping[str, bytes]
    from_cache: bool


class FabricOperationError(Exception):
    """A Fabric getDefinition operation did not succeed after its own retries."""


class FabricApiClient:
    """REST client for Fabric's report / semanticModel `getDefinition` LRO."""

    def __init__(self, config: PowerBIConnection, base_url: str = FABRIC_API_BASE_URL) -> None:
        self.config = config
        self._base_url = base_url.rstrip("/")
        self._msal_client = None
        self._session = requests.Session()
        mount_resilient_adapter(self._session)
        # Keyed by (item_id, lastUpdatedTimeUtc): unchanged items are skipped within a
        # run without a network call.
        self._definitions_cache: LRUCache[tuple, Mapping[str, bytes]] = LRUCache(maxsize=_MAX_CACHED_DEFINITIONS)

    @property
    def msal_client(self):
        """Built on first access, not at construction.

        `msal.ConfidentialClientApplication.__init__` always performs a tenant
        OIDC-discovery network call (`Authority.__init__`'s own docstring: "We
        always do a tenant discovery" - `validate_authority=False` only skips the
        separate *instance* discovery, not this one). This client is built eagerly
        in `PowerbiSource.__init__`, before any connection test runs, so
        constructing it must never touch the network on its own - only an actual
        token request (`_get_auth_token`, called from `get_report_definition` /
        `get_semantic_model_definition`) should.
        """
        if self._msal_client is None:
            self._msal_client = msal.ConfidentialClientApplication(
                client_id=self.config.clientId,
                client_credential=self.config.clientSecret.get_secret_value(),
                authority=(self.config.authorityURI or "") + self.config.tenantId,
                validate_authority=False,
            )
        return self._msal_client

    # --- auth -----------------------------------------------------------------

    def _get_auth_token(self) -> Optional[str]:  # noqa: UP045
        response_data = self._get_auth_token_from_cache()
        if not response_data:
            response_data = self._generate_new_auth_token()
        access_token = (response_data or {}).get("access_token")
        if not access_token:
            logger.warning("Failed to generate the Fabric access token.")
            return None
        return access_token

    def _generate_new_auth_token(self) -> Optional[dict]:  # noqa: UP045
        retry = AUTH_TOKEN_MAX_RETRIES
        while retry:
            try:
                return self.msal_client.acquire_token_for_client(scopes=FABRIC_SCOPE)
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug(traceback.format_exc())
                logger.warning(f"Error generating new Fabric auth token: {exc}")
                retry -= 1
                if retry:
                    sleep(AUTH_TOKEN_RETRY_WAIT_SECONDS)
        return None

    def _get_auth_token_from_cache(self) -> Optional[dict]:  # noqa: UP045
        try:
            return self.msal_client.acquire_token_silent(scopes=FABRIC_SCOPE, account=None)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug(traceback.format_exc())
            logger.debug(f"Error getting Fabric token from cache: {exc}")
            return None

    def _auth_headers(self) -> Optional[dict]:  # noqa: UP045
        token = self._get_auth_token()
        if not token:
            return None
        return {"Authorization": f"Bearer {token}"}

    # --- public API -------------------------------------------------------------

    def get_report_definition(
        self,
        workspace_id: str,
        report_id: str,
        last_updated_time_utc: Optional[str] = None,  # noqa: UP045
    ) -> Optional["FabricDefinitionResult"]:
        """Fetch a report's `getDefinition` parts, cached by (report_id, lastUpdatedTimeUtc)."""
        path = f"/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
        return self._get_definition_cached(report_id, last_updated_time_utc, path)

    def get_semantic_model_definition(
        self,
        workspace_id: str,
        model_id: str,
        last_updated_time_utc: Optional[str] = None,  # noqa: UP045
    ) -> Optional["FabricDefinitionResult"]:
        """Fetch a semantic model's `getDefinition` (TMDL) parts, cached by (model_id, lastUpdatedTimeUtc)."""
        path = f"/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL"
        return self._get_definition_cached(model_id, last_updated_time_utc, path)

    def list_reports_last_updated(self, workspace_id: str) -> Optional[Mapping[str, Optional[str]]]:  # noqa: UP045
        """`{report id: lastUpdatedTimeUtc}` for every report in the workspace, one
        paginated listing call. `None` on failure (never a partial mapping - a
        caller can't tell "this item wasn't in a partial page" from "this item was
        deleted" if it got one back); an item that itself lacks the field is kept
        with a `None` value rather than dropped or guessed at.
        """
        return self._list_items_last_updated(f"/workspaces/{workspace_id}/reports")

    def list_semantic_models_last_updated(self, workspace_id: str) -> Optional[Mapping[str, Optional[str]]]:  # noqa: UP045
        """`{model id: lastUpdatedTimeUtc}` for every semantic model in the workspace - see `list_reports_last_updated`."""
        return self._list_items_last_updated(f"/workspaces/{workspace_id}/semanticModels")

    def _list_items_last_updated(self, path: str) -> Optional[Mapping[str, Optional[str]]]:  # noqa: UP045
        headers = self._auth_headers()
        if not headers:
            return None
        result: dict = {}
        url: Optional[str] = f"{self._base_url}{path}"  # noqa: UP045
        try:
            while url:
                response = self._session.get(url, headers=headers, timeout=LIST_TIMEOUT_SECONDS)
                response.raise_for_status()
                body = response.json() or {}
                for item in body.get("value") or []:
                    item_id = item.get("id")
                    if item_id:
                        result[item_id] = item.get("lastUpdatedTimeUtc")
                # Fabric hands back a ready-to-call absolute URL for the next page;
                # its absence means this was the last one.
                url = body.get("continuationUri") or None
        except requests.exceptions.RequestException as exc:
            logger.warning(f"Error listing {path}: {exc}")
            logger.debug(traceback.format_exc())
            return None
        return result

    def fetch_definitions(
        self,
        fetchers: Sequence[Callable[[], Optional["FabricDefinitionResult"]]],
    ) -> list:
        """Run several `get_*_definition` calls concurrently, capped at `MAX_PARALLEL_REQUESTS`.

        Each `fetchers` entry is a zero-arg callable (typically
        `functools.partial(self.get_report_definition, ...)`); order of results matches
        `fetchers`. Kept generic over report/model so a caller can mix both kinds in one
        bounded batch instead of running two separate pools.
        """
        if not fetchers:
            return []
        with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_REQUESTS, len(fetchers))) as pool:
            return list(pool.map(lambda fetch: fetch(), fetchers))

    # --- cache + fetch ------------------------------------------------------------

    def _get_definition_cached(
        self,
        item_id: str,
        last_updated_time_utc: Optional[str],  # noqa: UP045
        path: str,
    ) -> Optional["FabricDefinitionResult"]:
        cache_key = (item_id, last_updated_time_utc)
        cached = self._definitions_cache.get(cache_key)
        if cached is not None:
            return FabricDefinitionResult(parts=cached, from_cache=True)
        parts = self._fetch_definition_with_retry(path)
        if parts is None:
            return None
        self._definitions_cache[cache_key] = parts
        return FabricDefinitionResult(parts=parts, from_cache=False)

    def _fetch_definition_with_retry(self, path: str) -> Optional[Mapping[str, bytes]]:  # noqa: UP045
        attempt = 0
        while attempt < MAX_OPERATION_ATTEMPTS:
            attempt += 1
            try:
                return self._run_get_definition_operation(path)
            except requests.exceptions.RequestException as exc:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Transport error on Fabric getDefinition {path} "
                    f"(attempt {attempt}/{MAX_OPERATION_ATTEMPTS}): {exc}"
                )
                if attempt < MAX_OPERATION_ATTEMPTS:
                    sleep(OPERATION_RETRY_WAIT_SECONDS * attempt)
            except FabricOperationError as exc:
                logger.warning(f"Fabric getDefinition {path} did not succeed: {exc}")
                return None
        logger.warning(f"Giving up on Fabric getDefinition {path} after {MAX_OPERATION_ATTEMPTS} attempts")
        return None

    def _run_get_definition_operation(self, path: str) -> Mapping[str, bytes]:
        headers = self._auth_headers()
        if not headers:
            raise FabricOperationError("no auth token")
        url = f"{self._base_url}{path}"
        response = self._session.post(url, headers=headers, timeout=POST_TIMEOUT_SECONDS)
        if response.status_code == 200:
            body = response.json()
        elif response.status_code == 202:
            body = self._poll_operation(response, headers)
        else:
            raise FabricOperationError(f"HTTP {response.status_code} for POST {url}: {response.text[:500]}")
        return self._decode_parts(body)

    def _poll_operation(self, post_response: "requests.Response", headers: dict) -> dict:
        location = post_response.headers.get("Location") or post_response.headers.get("location")
        if not location:
            raise FabricOperationError("202 response carried no Location header")
        retry_after = self._parse_retry_after(post_response)
        attempts = 0
        status = None
        poll_body: dict = {}
        while attempts < MAX_POLL_ATTEMPTS:
            sleep(min(retry_after, MAX_POLL_SLEEP_SECONDS))
            attempts += 1
            poll_response = self._session.get(location, headers=headers, timeout=POLL_TIMEOUT_SECONDS)
            poll_response.raise_for_status()
            poll_body = poll_response.json()
            status = poll_body.get("status")
            retry_after = self._parse_retry_after(poll_response, default=retry_after)
            if status in ("Succeeded", "Failed"):
                break
        if status != "Succeeded":
            raise FabricOperationError(f"LRO did not succeed after {attempts} poll(s) (status={status})")
        result_response = self._session.get(
            location.rstrip("/") + "/result", headers=headers, timeout=RESULT_TIMEOUT_SECONDS
        )
        result_response.raise_for_status()
        return result_response.json()

    @staticmethod
    def _parse_retry_after(response: "requests.Response", default: int = DEFAULT_POLL_RETRY_AFTER_SECONDS) -> int:
        raw = response.headers.get("Retry-After")
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    @staticmethod
    def _decode_parts(body: Optional[dict]) -> Mapping[str, bytes]:  # noqa: UP045
        definition = (body or {}).get("definition") or {}
        parts = definition.get("parts") or []
        decoded: dict = {}
        for part in parts:
            part_path = part.get("path")
            payload = part.get("payload")
            if not part_path or payload is None:
                continue
            if part.get("payloadType") == _INLINE_BASE64:
                decoded[part_path] = base64.b64decode(payload)
            else:
                decoded[part_path] = payload.encode("utf-8")
        return decoded
