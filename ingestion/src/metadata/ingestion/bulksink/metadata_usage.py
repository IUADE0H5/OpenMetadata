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
BulkSink class used for Usage workflows.

It sends Table queries and usage counts to Entities,
as well as populating JOIN information.

It picks up the information from reading the files
produced by the stage. At the end, the path is removed.
"""

import json
import os
import shutil
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple  # noqa: UP035

from cachetools import LRUCache
from pydantic import ValidationError

from metadata.config.common import ConfigModel
from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import (
    DatabaseSchema,
)
from metadata.generated.schema.entity.data.table import (
    ColumnJoins,
    JoinedWith,
    Table,
    TableJoins,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.entity.teams.user import User
from metadata.generated.schema.type.basic import Timestamp
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.lifeCycle import AccessDetails, LifeCycle
from metadata.generated.schema.type.tableUsageCount import (
    QueryCostWrapper,
    TableColumn,
    TableUsageCount,
)
from metadata.generated.schema.type.usageRequest import UsageRequest
from metadata.ingestion.api.steps import BulkSink
from metadata.ingestion.lineage.masker import mask_query
from metadata.ingestion.lineage.sql_lineage import (
    get_column_fqn,
    get_table_entities_from_query,
)
from metadata.ingestion.ometa.client import APIError
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.utils import model_str
from metadata.utils import fqn
from metadata.utils.constants import UTF_8
from metadata.utils.life_cycle_utils import get_query_type
from metadata.utils.logger import ingestion_logger
from metadata.utils.time_utils import convert_timestamp

logger = ingestion_logger()

LRU_CACHE_SIZE = 4096
# (query, table) pairs held back before they are sent through the bulk query API; bounds memory
# on large usage files while keeping the number of bulk calls low.
QUERY_FLUSH_SIZE = 2000


class MetadataUsageSinkConfig(ConfigModel):
    filename: str
    # Concurrent HTTP writes (table usage, lifecycle, joins) at the end of each usage file.
    threads: int = 8


class MetadataUsageBulkSink(BulkSink):
    """
    BulkSink implementation to send:
    - table usage
    - table queries
    - frequent joins
    """

    config: MetadataUsageSinkConfig

    def __init__(
        self,
        config: MetadataUsageSinkConfig,
        metadata: OpenMetadata,
    ):
        super().__init__()
        self.config = config
        self.service_name = None
        self.wrote_something = False
        self.metadata = metadata
        self.table_join_dict = {}
        self.table_usage_map = {}
        self._pending_queries: List[Tuple[CreateQueryRequest, EntityReference]] = []  # noqa: UP006
        self._life_cycles: Dict[str, Tuple[Table, LifeCycle]] = {}  # noqa: UP006
        self._deferred_writes: List[Callable[[], None]] = []  # noqa: UP006
        self._user_ref_cache: LRUCache = LRUCache(maxsize=LRU_CACHE_SIZE)
        self._masked_text_cache: LRUCache = LRUCache(maxsize=LRU_CACHE_SIZE)
        self.today = datetime.today().strftime("%Y-%m-%d")

    @property
    def name(self) -> str:
        return "OpenMetadata"

    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,  # noqa: UP045
    ):
        config = MetadataUsageSinkConfig.model_validate(config_dict)
        return cls(config, metadata)

    def __populate_table_usage_map(self, table_entity: Table, table_usage: TableUsageCount) -> None:
        """
        Method Either initialise the map data or
        update existing data with information from new queries on the same table
        """
        if not self.table_usage_map.get(table_entity.id.root):
            self.table_usage_map[table_entity.id.root] = {
                "table_entity": table_entity,
                "usage_count": table_usage.count,
                "usage_date": table_usage.date,
                "database": table_usage.databaseName,
                "database_schema": table_usage.databaseSchema,
            }
            logger.debug(
                f"[UsageSink] Added new table usage entry for {table_entity.id.root} "
                f"(count={table_usage.count}, date={table_usage.date})"
            )
        else:
            self.table_usage_map[table_entity.id.root]["usage_count"] += table_usage.count
            logger.debug(
                f"[UsageSink] Updated usage count for {table_entity.id.root} "
                f"(+={table_usage.count}, total={self.table_usage_map[table_entity.id.root]['usage_count']})"
            )

    def __publish_usage_records(self) -> None:
        """
        Method to publish SQL Queries, Table Usage
        """
        jobs = []
        for _, value_dict in self.table_usage_map.items():  # noqa: PERF102
            try:
                table_usage_request = UsageRequest(
                    date=datetime.fromtimestamp(convert_timestamp(value_dict["usage_date"])).strftime("%Y-%m-%d"),
                    count=value_dict["usage_count"],
                )
            except ValidationError as err:
                logger.debug(traceback.format_exc())
                logger.warning(f"Cannot construct UsageRequest from {value_dict['table_entity']}: {err}")
                continue
            jobs.append(self._publish_usage_job(value_dict["table_entity"], table_usage_request))
        self._run_concurrently(jobs)

    def _publish_usage_job(self, table_entity: Table, table_usage_request: UsageRequest) -> Callable[[], None]:
        name = table_entity.fullyQualifiedName.root

        def job() -> None:
            try:
                self.metadata.publish_table_usage(table_entity, table_usage_request)
                logger.info(f"Successfully table usage published for {name}")
                self.status.scanned(f"Table: {name}")
            except Exception as exc:
                error = f"Failed to update usage for {name} :{exc}"
                logger.debug(traceback.format_exc())
                logger.warning(error)
                self.status.failed(StackTraceError(name=name, error=error, stackTrace=traceback.format_exc()))

        return job

    def _defer(self, name: str, write: Callable[[], None]) -> None:
        """Queue an independent per-table HTTP write for the concurrent flush."""

        def job() -> None:
            try:
                write()
            except Exception as exc:
                error = f"Failed to publish usage data for {name}: {exc}"
                logger.debug(traceback.format_exc())
                logger.warning(error)
                self.status.failed(StackTraceError(name=name, error=error, stackTrace=traceback.format_exc()))

        self._deferred_writes.append(job)

    def _flush_deferred_writes(self) -> None:
        jobs, self._deferred_writes = self._deferred_writes, []
        life_cycles, self._life_cycles = self._life_cycles, {}
        for table_entity, life_cycle in life_cycles.values():
            jobs.append(self._life_cycle_job(table_entity, life_cycle))
        self._run_concurrently(jobs)

    def _life_cycle_job(self, table_entity: Table, life_cycle: LifeCycle) -> Callable[[], None]:
        name = table_entity.fullyQualifiedName.root

        def job() -> None:
            try:
                self.metadata.patch_life_cycle(entity=table_entity, life_cycle=life_cycle)
            except Exception as exc:
                error = f"Unable to patch life cycle data for table {name}: {exc}"
                logger.debug(traceback.format_exc())
                self.status.failed(StackTraceError(name=name, error=error, stackTrace=traceback.format_exc()))

        return job

    def _run_concurrently(self, jobs: List[Callable[[], None]]) -> None:  # noqa: UP006
        if not jobs:
            return
        if self.config.threads <= 1 or len(jobs) == 1:
            for job in jobs:
                job()
            return
        with ThreadPoolExecutor(max_workers=self.config.threads) as pool:
            list(pool.map(lambda job: job(), jobs))

    def iterate_files(self, usage_files: bool = True):
        """
        Iterate through files in the given directory
        """
        check_dir = os.path.isdir(self.config.filename)  # noqa: PTH112
        if check_dir:
            for filename in os.listdir(self.config.filename):  # noqa: PTH208
                full_file_name = os.path.join(self.config.filename, filename)  # noqa: PTH118
                if not os.path.isfile(full_file_name):  # noqa: PTH113
                    continue
                # if usage_files is True, then we want to iterate through files does not end with query
                # if usage_files is False, then we want to iterate through files that end with query
                if filename.endswith("query") ^ usage_files:
                    with open(full_file_name, encoding=UTF_8) as file:  # noqa: PTH123
                        yield file

    def handle_table_usage(self) -> None:
        """
        Handle table usage.
        """
        for file_handler in self.iterate_files():
            self.table_usage_map = {}
            for usage_record in file_handler.readlines():
                record = json.loads(usage_record)
                table_usage = TableUsageCount(**json.loads(record))

                self.service_name = table_usage.serviceName
                table_entities = None
                try:
                    logger.debug(
                        f"[UsageSink] Fetching table entities for "
                        f"service={self.service_name}, "
                        f"database={table_usage.databaseName}, "
                        f"schema={table_usage.databaseSchema}, "
                        f"table={table_usage.table}"
                    )

                    table_entities = get_table_entities_from_query(
                        metadata=self.metadata,
                        service_names=self.service_name,
                        database_name=table_usage.databaseName,
                        database_schema=table_usage.databaseSchema,
                        table_name=table_usage.table,
                    )
                except Exception as exc:
                    logger.debug(traceback.format_exc())
                    logger.warning(f"Cannot get table entities from query table {table_usage.table}: {exc}")

                if not table_entities:
                    logger.warning(f"Could not fetch table {table_usage.databaseName}.{table_usage.table}")
                    continue

                self.get_table_usage_and_joins(table_entities, table_usage)

            self._flush_queries()
            self._flush_deferred_writes()
            self.__publish_usage_records()

    def handle_query_cost(self) -> None:
        """One cost record per (statement, day): the text is masked once per distinct statement and the
        records are posted concurrently."""
        for file_handler in self.iterate_files(usage_files=False):
            jobs = []
            for usage_record in file_handler.readlines():
                cost_record = QueryCostWrapper(**json.loads(usage_record))
                jobs.append(self._query_cost_job(cost_record, self._masked_text(cost_record)))
            self._run_concurrently(jobs)

    def _masked_text(self, cost_record: QueryCostWrapper) -> str:
        key = (cost_record.query, cost_record.dialect)
        if key not in self._masked_text_cache:
            self._masked_text_cache[key] = mask_query(cost_record.query, cost_record.dialect) or cost_record.query
        return self._masked_text_cache[key]

    def _query_cost_job(self, cost_record: QueryCostWrapper, masked_query: str) -> Callable[[], None]:
        def job() -> None:
            try:
                self.metadata.publish_query_cost(cost_record, self.service_name, masked_query=masked_query)
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(f"Failed to publish query cost for query={cost_record.query[:100]}...: {exc}")

        return job

    # Check here how to properly pick up ES and/or table query data
    def run(self) -> None:
        self.handle_table_usage()
        self.handle_query_cost()

    def get_table_usage_and_joins(self, table_entities: List[Table], table_usage: TableUsageCount):  # noqa: UP006
        """
        For the list of tables, compute usage with already existing seen
        tables and publish the join information.
        """
        for table_entity in table_entities:
            logger.debug(f"Processing table entity {table_entity.name.root}")
            if table_entity is not None:
                table_join_request = None
                try:
                    self.__populate_table_usage_map(table_usage=table_usage, table_entity=table_entity)
                    table_join_request = self.__get_table_joins(table_entity=table_entity, table_usage=table_usage)
                    logger.debug(f"table join request {table_join_request}")

                    if table_join_request is not None and len(table_join_request.columnJoins) > 0:
                        self._defer(
                            table_entity.fullyQualifiedName.root,
                            lambda entity=table_entity, joins=table_join_request: (
                                self.metadata.publish_frequently_joined_with(entity, joins)
                            ),
                        )

                    if table_usage.sqlQueries:
                        self._queue_queries(table_entity, table_usage.sqlQueries)
                        self._get_table_life_cycle_data(table_entity=table_entity, table_usage=table_usage)
                except APIError as err:
                    if err.status_code == 409:
                        logger.warning(f"Entity already exists for {table_usage.table}, skipping: {err}")
                    else:
                        error = f"Failed to update query join for {table_usage}: {err}"
                        logger.debug(traceback.format_exc())
                        logger.warning(error)
                        self.status.failed(
                            StackTraceError(
                                name=table_usage.table,
                                error=error,
                                stackTrace=traceback.format_exc(),
                            )
                        )
                except Exception as exc:
                    name = table_entity.name.root
                    error = f"Error getting usage and join information for {name}: {exc}"
                    logger.debug(traceback.format_exc())
                    logger.warning(error)
                    self.status.failed(StackTraceError(name=name, error=error, stackTrace=traceback.format_exc()))
            else:
                logger.warning(
                    f"Could not fetch table {table_usage.databaseName}.{table_usage.databaseSchema}.{table_usage.table}"
                )
                self.status.warning(f"Table: {table_usage.table}", reason="Could not fetch table")

    def _queue_queries(self, table_entity: Table, queries: List[CreateQueryRequest]) -> None:  # noqa: UP006
        table_ref = EntityReference(id=table_entity.id.root, type="table")
        self._pending_queries.extend((query, table_ref) for query in queries)
        if len(self._pending_queries) >= QUERY_FLUSH_SIZE:
            self._flush_queries()

    def _flush_queries(self) -> None:
        """Send the queued (query, table) pairs through the deduplicating bulk path."""
        if not self._pending_queries:
            return
        pending, self._pending_queries = self._pending_queries, []
        try:
            self.metadata.ingest_queries_bulk(pending)
        except APIError as err:
            if err.status_code == 409:
                logger.warning(f"Entity already exists while ingesting queries, skipping: {err}")
            else:
                error = f"Failed to ingest {len(pending)} table queries: {err}"
                logger.debug(traceback.format_exc())
                logger.warning(error)
                self.status.failed(StackTraceError(name="queries", error=error, stackTrace=traceback.format_exc()))
        except Exception as exc:
            error = f"Failed to ingest {len(pending)} table queries: {exc}"
            logger.debug(traceback.format_exc())
            logger.warning(error)
            self.status.failed(StackTraceError(name="queries", error=error, stackTrace=traceback.format_exc()))

    def __get_table_joins(self, table_entity: Table, table_usage: TableUsageCount) -> TableJoins:
        """
        Method to get Table Joins
        """
        # TODO: Clean up how we are passing dates from query parsing to here to use timestamps instead of strings
        start_date = datetime.fromtimestamp(int(table_usage.date) / 1000)
        table_joins: TableJoins = TableJoins(columnJoins=[], directTableJoins=[], startDate=start_date.date())
        column_joins_dict = {}
        for column_join in table_usage.joins:
            joined_with = {}
            if column_join.tableColumn is None or len(column_join.joinedWith) == 0:
                continue

            if column_join.tableColumn.column in column_joins_dict:
                joined_with = column_joins_dict[column_join.tableColumn.column]
            else:
                column_joins_dict[column_join.tableColumn.column] = {}

            for column in column_join.joinedWith:
                joined_column_fqn = self.__get_column_fqn(table_usage.databaseName, table_usage.databaseSchema, column)
                if str(joined_column_fqn) in joined_with.keys():  # noqa: SIM118
                    column_joined_with = joined_with[str(joined_column_fqn)]
                    column_joined_with.joinCount += 1
                    joined_with[str(joined_column_fqn)] = column_joined_with
                elif joined_column_fqn is not None:
                    joined_with[str(joined_column_fqn)] = JoinedWith(
                        fullyQualifiedName=str(joined_column_fqn), joinCount=1
                    )
                else:
                    logger.debug(f"Skipping join columns for {column} {joined_column_fqn}")
            column_joins_dict[column_join.tableColumn.column] = joined_with

        for key, value in column_joins_dict.items():
            key_name = get_column_fqn(table_entity=table_entity, column=key)
            if not key_name:
                logger.warning(f"Could not find column {key} in table {table_entity.fullyQualifiedName.root}")
                continue
            table_joins.columnJoins.append(
                ColumnJoins(columnName=fqn.split(key_name)[-1], joinedWith=list(value.values()))
            )
        return table_joins

    def __get_column_fqn(self, database: str, database_schema: str, table_column: TableColumn) -> Optional[str]:  # noqa: RET503, UP045
        """
        Method to get column fqn
        """
        table_entities = get_table_entities_from_query(
            metadata=self.metadata,
            service_names=self.service_name,
            database_name=database,
            database_schema=database_schema,
            table_name=table_column.table,
        )
        if not table_entities:
            return None

        for table_entity in table_entities:
            return get_column_fqn(table_entity=table_entity, column=table_column.column)

    def _user_reference(self, user_fqn) -> Optional[EntityReference]:  # noqa: UP045
        key = model_str(user_fqn)
        if key not in self._user_ref_cache:
            self._user_ref_cache[key] = self.metadata.get_entity_reference(entity=User, fqn=key)
        return self._user_ref_cache[key]

    def _merge_life_cycle(self, table_entity: Table, life_cycle: LifeCycle) -> None:
        """Keep the latest access per lifecycle stage across every usage record of the table, so the
        table is patched once per file instead of once per record."""
        key = str(table_entity.id.root)
        if key not in self._life_cycles:
            self._life_cycles[key] = (table_entity, life_cycle)
            return
        _, merged = self._life_cycles[key]
        for stage in LifeCycle.model_fields:
            incoming = getattr(life_cycle, stage)
            current = getattr(merged, stage)
            if incoming and (not current or current.timestamp.root < incoming.timestamp.root):
                setattr(merged, stage, incoming)

    def _get_table_life_cycle_data(self, table_entity: Table, table_usage: TableUsageCount):
        """
        Method to call the lifeCycle API to store the data.
        We iterate over all the queries of a table entity and pick the life cycle
        data according to the query.
        The life cycle data will only be added if the current lifecycle datetime is less the datetime of
        the query being processed.
        """
        try:
            life_cycle = LifeCycle()
            for create_query in table_usage.sqlQueries:
                user = None
                process_user = None
                if create_query.users:
                    user = self._user_reference(create_query.users[0])
                elif create_query.usedBy:
                    process_user = create_query.usedBy[0]
                query_type = get_query_type(create_query=create_query)
                if query_type:
                    access_details = AccessDetails(
                        timestamp=Timestamp(create_query.queryDate.root),
                        accessedBy=user,
                        accessedByAProcess=process_user,
                    )
                    life_cycle_attr = getattr(life_cycle, query_type)
                    if not life_cycle_attr or life_cycle_attr.timestamp.root < access_details.timestamp.root:
                        setattr(life_cycle, query_type, access_details)

            self._merge_life_cycle(table_entity, life_cycle)

        except Exception as err:
            error = f"Unable to get life cycle data for table {table_entity.fullyQualifiedName}: {err}"
            self.status.failed(
                StackTraceError(
                    name=table_usage.table,
                    error=error,
                    stackTrace=traceback.format_exc(),
                )
            )

    def close(self):
        if Path(self.config.filename).exists():
            shutil.rmtree(self.config.filename)
        try:
            self.metadata.compute_percentile(Table, self.today)
            self.metadata.compute_percentile(DatabaseSchema, self.today)
            self.metadata.compute_percentile(Database, self.today)
        except APIError as err:
            logger.debug(traceback.format_exc())
            logger.error(f"Failed to publish compute.percentile: {err}")

        self.metadata.close()
