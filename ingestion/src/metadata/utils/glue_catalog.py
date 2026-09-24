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
What a Glue catalog entry -- and, for an Iceberg table, its metadata document -- can say about a
table without reading the table.

Glue records when a table was created, and for Hive-style tables what a crawler last measured. An
Iceberg table additionally keeps its schemas, partition specs and per-snapshot totals in one JSON
document in object storage, to which the catalog holds only a pointer, in
`Parameters.metadata_location`. Together that is a table's partitioning, size, and (for Iceberg) an
exact row count, for one GetTable and at most one GetObject.

This lives in `utils` rather than beside the Athena connector because both a connector (which reads
the partition spec) and the profiler (which reads the totals) need it, and the profiler may not
import a connector -- `ingestion/.importlinter` puts `ingestion.source` above `profiler`.
"""

import json
import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Tuple  # noqa: UP035

from metadata.clients.aws_client import AWSClient
from metadata.generated.schema.security.credentials.awsCredentials import AWSCredentials
from metadata.utils.logger import utils_logger

logger = utils_logger()

S3_URI = re.compile(r"^s3a?://([^/]+)/(.+)$")

# One entry per distinct set of AWS credentials. A workflow has one service connection, so this is
# effectively a singleton; the cap is here because an unbounded client cache is an unbounded leak.
_MAX_CACHED_SESSIONS = 8
_clients: "OrderedDict[tuple, Tuple[Any, Any]]" = OrderedDict()  # noqa: UP006
_clients_lock = threading.Lock()


def _session_key(config: AWSCredentials) -> tuple:
    """The fields that decide which AWS identity and endpoint a session talks to.

    Deliberately not the secret itself: a key is kept in memory for the life of the process, and
    two configs that agree on all of this but differ only in their secret access key would have to
    come from two service connections inside one workflow, which cannot happen.
    """
    return (
        config.awsRegion,
        config.assumeRoleArn,
        config.assumeRoleSessionName,
        config.profileName,
        str(config.endPointURL) if config.endPointURL else None,
        config.awsAccessKeyId,
    )


def glue_and_s3_clients(config: AWSCredentials) -> Tuple[Any, Any]:  # noqa: UP006
    """A cached Glue and S3 client pair for `config`.

    Built once per credential set, not once per table. `AWSClient.create_session` resolves
    refreshable credentials eagerly, so with `assumeRoleArn` set a fresh client means a fresh
    AssumeRole call -- at one per table that is thousands of STS calls in a profiler run.
    """
    key = _session_key(config)
    with _clients_lock:
        cached = _clients.get(key)
        if cached is not None:
            _clients.move_to_end(key)
            return cached
    client = AWSClient(config)
    pair = (client.get_glue_client(), client.get_s3_client())
    with _clients_lock:
        _clients[key] = pair
        _clients.move_to_end(key)
        while len(_clients) > _MAX_CACHED_SESSIONS:
            _clients.popitem(last=False)
    return pair


# Iceberg writes -1 rather than omitting the key when a table has never been committed to.
NO_SNAPSHOT = -1


@dataclass(frozen=True)
class CatalogTable:
    """A table's Glue entry, and its Iceberg metadata document when it has one.

    `iceberg` is None for a Hive-style table, which is not a failure: the entry alone still carries
    a creation time and, often, a crawler's size. Callers that need both pay for one GetTable.
    """

    entry: dict
    iceberg: Optional[dict] = None  # noqa: UP045

    @property
    def is_iceberg(self) -> bool:
        return self.iceberg is not None


def load_catalog_table(
    glue_client: Any,
    s3_client: Any,
    database: str,
    table: str,
    catalog_id: Optional[str] = None,  # noqa: UP045
) -> Optional[CatalogTable]:  # noqa: UP045
    """The catalog entry for one table, with its Iceberg document when there is a readable one.

    `None` only when the catalog entry itself cannot be read. An entry that names no
    `metadata_location`, or whose document is gone, unreadable or not JSON, still comes back -- as a
    `CatalogTable` with no `iceberg`, which is exactly the Hive-style case. Every caller treats all
    of this as an optimisation over querying the table, so a failure degrades to the slower path
    rather than failing the run.
    """
    params = {"DatabaseName": database, "Name": table}
    if catalog_id:
        params["CatalogId"] = catalog_id
    try:
        entry = glue_client.get_table(**params).get("Table") or {}
    except Exception as exc:
        logger.debug(f"Could not read the catalog entry for {database}.{table}: {exc}")
        return None

    location = (entry.get("Parameters") or {}).get("metadata_location")
    match = S3_URI.match(location or "")
    if not match:
        return CatalogTable(entry=entry)
    try:
        body = s3_client.get_object(Bucket=match.group(1), Key=match.group(2))["Body"].read()
        return CatalogTable(entry=entry, iceberg=json.loads(body))
    except Exception as exc:
        logger.warning(f"Could not read Iceberg metadata for {database}.{table} at {location}: {exc}")
        return CatalogTable(entry=entry)


def created_at(entry: dict) -> Optional[datetime]:  # noqa: UP045
    """When the catalog first saw the table, in UTC. Glue returns it in the catalog's own zone."""
    created = entry.get("CreateTime")
    return created.astimezone(timezone.utc) if isinstance(created, datetime) else None


# What a Hive-style table's size has been called by the writers that record one, best first.
_SIZE_PARAMETERS = ("totalSize", "sizeKey")


def declared_size_bytes(entry: dict) -> Optional[int]:  # noqa: UP045
    """The size a crawler or engine last wrote into the catalog entry, for a non-Iceberg table.

    As stale as whatever last ran the crawler, and absent on most tables -- but the field it fills
    is otherwise empty, and a size is not a number anything decides on. A row count is, which is why
    `numRows` is deliberately not read here: Hive writes -1 or a stale value freely, and replacing a
    correct `count(*)` with that would be a regression rather than a saving.
    """
    parameters = entry.get("Parameters") or {}
    for key in _SIZE_PARAMETERS:
        raw = parameters.get(key)
        if raw is None:
            continue
        try:
            size = int(raw)
        except (TypeError, ValueError):
            logger.debug(f"Catalog parameter {key}={raw!r} is not an integer")
            continue
        if size >= 0:
            return size
    return None


def current_snapshot(metadata: dict) -> Optional[dict]:  # noqa: UP045
    """The snapshot `current-snapshot-id` points at, or `None` for a table never written to.

    Falling back to the newest snapshot would be wrong: the current pointer is what a reader of the
    table sees, and a table can carry snapshots that have been rolled back past.
    """
    current = metadata.get("current-snapshot-id")
    if current is None or current == NO_SNAPSHOT:
        return None
    return next(
        (snapshot for snapshot in metadata.get("snapshots") or [] if snapshot.get("snapshot-id") == current),
        None,
    )


def _summary_int(snapshot: dict, key: str) -> Optional[int]:  # noqa: UP045
    """Summary values are strings in the spec, and writers disagree about that in practice."""
    raw = (snapshot.get("summary") or {}).get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.debug(f"Iceberg snapshot summary {key}={raw!r} is not an integer")
        return None


@dataclass(frozen=True)
class IcebergTableStats:
    """Totals the current snapshot already knows, so nothing has to be counted.

    Either may be `None`: the keys are optional in the spec and an engine is free not to write
    them. A `None` means "this snapshot does not say", never "zero".
    """

    row_count: Optional[int] = None  # noqa: UP045
    size_bytes: Optional[int] = None  # noqa: UP045


def table_stats(metadata: dict) -> IcebergTableStats:
    """Row count and size of the current snapshot, from its own summary."""
    snapshot = current_snapshot(metadata)
    if snapshot is None:
        return IcebergTableStats()
    return IcebergTableStats(
        row_count=_summary_int(snapshot, "total-records"),
        size_bytes=_summary_int(snapshot, "total-files-size"),
    )
