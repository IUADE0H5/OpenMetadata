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
Partition spec of an Iceberg table behind Athena/Glue.

Iceberg partitioning is hidden: Glue's `PartitionKeys` is empty and the transforms (`day(ts)`,
`bucket[16](id)`) only exist in the table's metadata.json, whose location Glue stores in
`Parameters.metadata_location`. Query authors need the *source* column and the transform to write
a predicate that prunes.
"""

import json
import re
from typing import Any, List, Optional  # noqa: UP035

from metadata.generated.schema.entity.data.table import (
    PartitionColumnDetails,
    PartitionIntervalTypes,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

_S3_URI = re.compile(r"^s3a?://([^/]+)/(.+)$")
_TIME_TRANSFORMS = {"year", "month", "day", "hour"}


def _interval_type(transform: str) -> PartitionIntervalTypes:
    if transform == "identity":
        return PartitionIntervalTypes.COLUMN_VALUE
    if transform in _TIME_TRANSFORMS:
        return PartitionIntervalTypes.TIME_UNIT
    return PartitionIntervalTypes.OTHER


def _current_fields(metadata: dict) -> dict:
    """id -> name of the current schema; v2 keeps a list of schemas, v1 a single one."""
    if "schemas" in metadata:
        schema = next(
            (s for s in metadata["schemas"] if s.get("schema-id") == metadata.get("current-schema-id")),
            metadata["schemas"][-1],
        )
    else:
        schema = metadata.get("schema") or {}
    return {field["id"]: field["name"] for field in schema.get("fields", [])}


def _default_spec_fields(metadata: dict) -> list:
    if "partition-specs" in metadata:
        spec = next(
            (s for s in metadata["partition-specs"] if s.get("spec-id") == metadata.get("default-spec-id")),
            metadata["partition-specs"][-1],
        )
        return spec.get("fields", [])
    return metadata.get("partition-spec") or []


def get_iceberg_partition_columns(
    glue_client: Any,
    s3_client: Any,
    database: str,
    table: str,
    catalog_id: Optional[str] = None,  # noqa: UP045
) -> Optional[List[PartitionColumnDetails]]:  # noqa: UP006, UP045
    """
    Read the default partition spec of an Iceberg table from its current metadata.json.

    Returns None when the table has no readable Iceberg metadata (not Iceberg, no
    `metadata_location`, unreadable object) and an empty list for an unpartitioned table.
    """
    params = {"DatabaseName": database, "Name": table}
    if catalog_id:
        params["CatalogId"] = catalog_id
    location = ((glue_client.get_table(**params).get("Table") or {}).get("Parameters") or {}).get("metadata_location")
    match = _S3_URI.match(location or "")
    if not match:
        return None
    try:
        body = s3_client.get_object(Bucket=match.group(1), Key=match.group(2))["Body"].read()
        metadata = json.loads(body)
    except Exception as exc:
        logger.warning(f"Could not read Iceberg metadata for {database}.{table} at {location}: {exc}")
        return None

    fields = _current_fields(metadata)
    columns: List[PartitionColumnDetails] = []  # noqa: UP006
    for spec_field in _default_spec_fields(metadata):
        transform = str(spec_field.get("transform") or "")
        source = fields.get(spec_field.get("source-id"))
        if transform == "void" or not source:
            continue
        columns.append(
            PartitionColumnDetails(columnName=source, intervalType=_interval_type(transform), interval=transform)
        )
    return columns
