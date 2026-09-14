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
Given query data about tables, store the results
in a temporary file (i.e., the stage)
to be further processed by the BulkSink.
"""

import csv
import json
import os
import shutil
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple  # noqa: UP035

from cachetools import LRUCache
from pydantic import Field

from metadata.config.common import ConfigModel
from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.entity.teams.user import User
from metadata.generated.schema.type.queryParserData import ParsedData, QueryParserData
from metadata.generated.schema.type.tableUsageCount import TableColumnJoin, TableUsageCount
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Stage
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.progress.tracking import shared_progress
from metadata.utils.constants import UTF_8
from metadata.utils.helpers import (
    TRANSIENT_TABLE_PATTERNS,
    get_query_hash,
    init_staging_dir,
    is_transient_table_name,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class TableStageConfig(ConfigModel):
    filename: str
    # Regexes, matched in full against the table component of a parsed reference, naming tables that
    # never exist in the catalog; added to the built-in `TRANSIENT_TABLE_PATTERNS`.
    transientTablePatterns: List[str] = Field(default_factory=list)  # noqa: N815, UP006
    # CSV appended with one row per (table, day, principal) and the number of statements counted -
    # what the published usage numbers are made of. Kept after the run (the staging directory is
    # not), so a count that looks wrong can be traced to the principals behind it. A local path, or
    # `s3://bucket/key.csv` / `s3://bucket/prefix/` (then `usage_audit_<UTC time>.csv` under it) for
    # runners whose filesystem ends with the pod; the upload happens when the stage closes.
    usageAuditFile: Optional[str] = None  # noqa: N815, UP045

    @property
    def transient_table_patterns(self) -> Tuple[str, ...]:  # noqa: UP006
        return TRANSIENT_TABLE_PATTERNS + tuple(self.transientTablePatterns)


class TableUsageStage(Stage):
    """
    Stage implementation for Table Usage data.

    Converts QueryParserData into TableUsageCount
    and stores it in files partitioned by date.
    """

    config: TableStageConfig

    def __init__(
        self,
        config: TableStageConfig,
        metadata: OpenMetadata,
    ):
        super().__init__()
        self.config = config
        self.metadata = metadata
        self.table_usage = {}
        self.table_queries = {}
        self.query_cost = {}
        init_staging_dir(self.config.filename)
        self.wrote_something = False
        self.service_name = ""
        self.process_query_cost = True  # set by the usage workflow from the source's processQueryCostAnalysis
        self._batches = 0
        self._audit: Dict[Tuple[str, str, str], int] = {}  # noqa: UP006  # (table, date, principal) -> statements, per batch
        self._audit_local = self._audit_local_path()
        # Query logs repeat a handful of principals thousands of times; without this the stage
        # spends most of a run on GET /users/name/{fqn}. Misses are cached too.
        self._user_lookup_cache: LRUCache = LRUCache(maxsize=1000)

    @property
    def name(self) -> str:
        return "Table Usage"

    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,  # noqa: UP045
    ):
        config = TableStageConfig.model_validate(config_dict)
        return cls(config, metadata)

    def init_location(self) -> None:
        """
        Prepare the usage location
        """
        location = Path(self.config.filename)
        if location.is_dir():
            logger.info("Location exists, cleaning it up")
            shutil.rmtree(self.config.filename)
        logger.info(f"Creating the directory to store staging data in {location}")
        location.mkdir(parents=True, exist_ok=True)

    def _get_user_entity(self, username: str) -> Tuple[Optional[List[str]], Optional[List[str]]]:  # noqa: UP006, UP045
        """
        From the user received in the query history call - who executed the query in the db -
        return if we find any users in OM that match, plus the user that we found in the db record.
        """
        if not username:
            return None, None
        if username in self._user_lookup_cache:
            user_fqn = self._user_lookup_cache[username]
        else:
            user = self.metadata.get_by_name(entity=User, fqn=username)
            user_fqn = user.fullyQualifiedName.root if user else None
            self._user_lookup_cache[username] = user_fqn
        if user_fqn:
            return [user_fqn], [username]
        return None, [username]

    def _add_sql_query(self, record, table):
        users, used_by = self._get_user_entity(record.userName)
        if self.table_queries.get((table, record.date)):
            self.service_name = record.serviceName
            self.table_queries[(table, record.date)].append(
                CreateQueryRequest(
                    query=record.sql,
                    query_type=record.query_type,
                    exclude_usage=record.exclude_usage,
                    users=users,
                    queryDate=record.date,
                    dialect=record.dialect,
                    usedBy=used_by,
                    duration=record.duration,
                    service=record.serviceName,
                )
            )
        else:
            self.table_queries[(table, record.date)] = [
                CreateQueryRequest(
                    query=record.sql,
                    query_type=record.query_type,
                    exclude_usage=record.exclude_usage,
                    users=users,
                    queryDate=record.date,
                    usedBy=used_by,
                    dialect=record.dialect,
                    duration=record.duration,
                    service=record.serviceName,
                )
            ]

    def _is_transient(self, table: Optional[str]) -> bool:  # noqa: UP045
        return is_transient_table_name(table, self.config.transient_table_patterns)

    def _catalog_joins(self, table_joins: Optional[List[TableColumnJoin]]) -> List[TableColumnJoin]:  # noqa: UP006, UP045
        """Joins of a table, without the ones against tables that will never resolve."""
        kept = []
        for column_join in table_joins or []:
            joined_with = [column for column in column_join.joinedWith if not self._is_transient(column.table)]
            if joined_with:
                kept.append(TableColumnJoin(tableColumn=column_join.tableColumn, joinedWith=joined_with))
        return kept

    def _handle_table_usage(self, parsed_data: ParsedData, table: str) -> Iterable[Either[str]]:
        table_joins = self._catalog_joins(parsed_data.joins.get(table))
        try:
            self._add_sql_query(record=parsed_data, table=table)
            if self.config.usageAuditFile:
                key = (table, parsed_data.date, parsed_data.userName or "")
                self._audit[key] = self._audit.get(key, 0) + 1
            table_usage_count = self.table_usage.get((table, parsed_data.date))
            if table_usage_count is not None:
                table_usage_count.count = table_usage_count.count + 1
                if table_joins:
                    table_usage_count.joins.extend(table_joins)
            else:
                joins = []
                if table_joins:
                    joins.extend(table_joins)

                table_usage_count = TableUsageCount(
                    table=table,
                    databaseName=parsed_data.databaseName,
                    date=parsed_data.date,
                    joins=joins,
                    serviceName=parsed_data.serviceName,
                    sqlQueries=[],
                    databaseSchema=parsed_data.databaseSchema,
                )
            self.table_usage[(table, parsed_data.date)] = table_usage_count

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=table,
                    error=f"Error in staging record [{exc}]",
                    stackTrace=traceback.format_exc(),
                )
            )
        yield Either(right=table)

    def _handle_query_cost(self, parsed_data: ParsedData):
        query_hash = get_query_hash(parsed_data.sql)
        if (query_hash, parsed_data.date) in self.query_cost:
            self.query_cost[(query_hash, parsed_data.date)].update(
                {
                    "cost": self.query_cost[(query_hash, parsed_data.date)]["cost"] + (parsed_data.cost or 0),
                    "count": self.query_cost[(query_hash, parsed_data.date)]["count"] + 1,
                    "totalDuration": self.query_cost[(query_hash, parsed_data.date)]["totalDuration"]
                    + (parsed_data.duration or 0),
                }
            )
        else:
            self.query_cost[(query_hash, parsed_data.date)] = {
                "cost": parsed_data.cost or 0,
                "count": 1,
                "query": parsed_data.sql,
                "dialect": parsed_data.dialect,
                "totalDuration": parsed_data.duration or 0,
            }

    def _run(self, record: QueryParserData) -> Iterable[Either[str]]:
        """
        Process the parsed data and store it in a file
        """
        if not record or not record.parsedData:
            return
        self.table_usage = {}
        self.table_queries = {}
        self._audit = {}
        # Reset with the other per-batch maps: kept across batches, every dump appended the earlier
        # days' cost records to their files again, one more copy per batch.
        self.query_cost = {}
        for parsed_data in record.parsedData:
            if parsed_data is None:
                continue
            for table in parsed_data.tables:
                if self._is_transient(table):
                    logger.debug(f"Skipping transient table [{table}] in query [{parsed_data.sql}]")
                    continue
                yield from self._handle_table_usage(parsed_data=parsed_data, table=table)
            if self.process_query_cost:
                self._handle_query_cost(parsed_data)
        self.dump_data_to_file()
        self._batches += 1
        progress = shared_progress(self)
        if progress is not None:  # the totals the sink will work through, known before it starts
            progress.seed_scope_total("Usage records", f"batch {self._batches}", len(self.table_usage))
            if self.process_query_cost:
                progress.seed_scope_total("Query costs", f"batch {self._batches}", len(self.query_cost))
        logger.info(
            f"Staged {len(self.table_usage):,} table-day usage records"
            + (f" and {len(self.query_cost):,} query-cost records" if self.process_query_cost else "")
            + f" from {len(record.parsedData):,} parsed statements"
        )
        self.status.record_count += len(self.table_usage)  # the heartbeat shows table-days, not statement-table pairs

    def dump_data_to_file(self):
        """
        Dump the table usage data to a file.
        """
        if self._audit_local and self._audit:
            path = self._audit_local
            path.parent.mkdir(parents=True, exist_ok=True)
            new = not path.exists() or path.stat().st_size == 0
            with path.open("a", encoding=UTF_8, newline="") as file:
                writer = csv.writer(file)
                if new:
                    writer.writerow(["service", "table", "date", "principal", "statements"])
                for (table, date, principal), count in sorted(self._audit.items()):
                    writer.writerow([self.service_name, table, date, principal, count])
        for key, value in self.table_usage.items():
            if value:
                value.sqlQueries = self.table_queries.get(key, [])
                data = value.model_dump_json()
                with open(  # noqa: PTH123
                    os.path.join(self.config.filename, f"{value.serviceName}_{key[1]}"),  # noqa: PTH118
                    "a+",
                    encoding=UTF_8,
                ) as file:
                    file.write(json.dumps(data))
                    file.write("\n")

        for key, value in self.query_cost.items():
            if value:
                data = {
                    "queryHash": key[0],
                    "date": key[1],
                    "cost": value["cost"],
                    "count": value["count"],
                    "query": value["query"],
                    "dialect": value["dialect"],
                    "totalDuration": value["totalDuration"],
                }
                with open(  # noqa: PTH123
                    os.path.join(self.config.filename, f"{self.service_name}_{key[1]}_query"),  # noqa: PTH118
                    "a+",
                    encoding=UTF_8,
                ) as file:
                    file.write(json.dumps(data))
                    file.write("\n")

    def _audit_local_path(self) -> Optional[Path]:  # noqa: UP045
        target = self.config.usageAuditFile
        if not target:
            return None
        if not target.startswith("s3://"):
            return Path(target)
        handle, name = tempfile.mkstemp(prefix="usage_audit_", suffix=".csv")
        os.close(handle)
        return Path(name)

    def _audit_s3_key(self) -> Tuple[str, str]:  # noqa: UP006
        bucket, _, key = self.config.usageAuditFile[len("s3://") :].partition("/")
        if not key or key.endswith("/"):
            key = f"{key}usage_audit_{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%SZ')}.csv"
        return bucket, key

    def close(self) -> None:
        """Data is dumped per batch; only an S3 audit target has work left: the upload."""
        target = self.config.usageAuditFile
        if not target or not target.startswith("s3://") or self._audit_local is None:
            return
        try:
            if self._audit_local.exists() and self._audit_local.stat().st_size > 0:
                bucket, key = self._audit_s3_key()
                _s3_client().upload_file(str(self._audit_local), bucket, key)
                logger.info(f"Usage audit uploaded to s3://{bucket}/{key}")
        except Exception as exc:
            logger.warning(f"Usage audit upload to {target} failed: {exc}")
        finally:
            self._audit_local.unlink(missing_ok=True)


def _s3_client():
    import boto3  # base dependency (the secrets manager); imported here so the stage never needs it otherwise

    return boto3.client("s3")
