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
Mixin class containing Query specific methods

To be used by OpenMetadata class
"""

import hashlib
import json
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Tuple, Union  # noqa: UP035

from cachetools import LRUCache

from metadata.generated.schema.api.data.createQuery import CreateQueryRequest
from metadata.generated.schema.api.data.createQueryCostRecord import (
    CreateQueryCostRecordRequest,
)
from metadata.generated.schema.entity.data.dashboard import Dashboard
from metadata.generated.schema.entity.data.query import Query
from metadata.generated.schema.entity.data.queryCostRecord import QueryCostRecord
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.type.basic import SqlQuery, Uuid
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.tableUsageCount import QueryCostWrapper
from metadata.ingestion.lineage.masker import mask_query, statement_shape
from metadata.ingestion.ometa.client import REST
from metadata.ingestion.ometa.utils import model_str
from metadata.utils.logger import ometa_logger

logger = ometa_logger()

# Queries per PUT /queries/bulk call; query text can run to several KB each.
QUERY_BULK_BATCH_SIZE = 100
# (statement shape, dialect) -> masked text; bounded, shared by every flush of a run
_masked_by_shape: LRUCache = LRUCache(maxsize=20_000)
MASK_POOL_THRESHOLD = 64  # fewer new shapes than this and the pool start-up costs more than it saves


def _mask_in_worker(args: Tuple[str, Optional[str]]) -> Optional[str]:  # noqa: UP006, UP045
    text, dialect = args
    try:
        return mask_query(text, dialect)
    except Exception:
        return None


@dataclass
class _PendingQuery:
    """One distinct (service, query) with every relation seen for it in this batch."""

    request: CreateQueryRequest
    used_in: Dict[str, EntityReference] = field(default_factory=dict)  # noqa: UP006
    users: Dict[str, None] = field(default_factory=dict)  # noqa: UP006  - ordered set of user FQNs
    used_by: Dict[str, None] = field(default_factory=dict)  # noqa: UP006


class OMetaQueryMixin:
    """
    OpenMetadata API methods related to Queries.

    To be inherited by OpenMetadata
    """

    client: REST

    def _get_query_hash(self, query: str) -> str:
        result = hashlib.md5(query.encode())
        return str(result.hexdigest())

    @staticmethod
    def _query_fqn(service: Optional[str], query_hash: str) -> str:  # noqa: UP045
        """The server names a query by its checksum under the service: ``<service>.<md5>``."""
        return f"{service}.{query_hash}" if service else query_hash

    def _get_or_create_query(self, query: CreateQueryRequest) -> Optional[Query]:  # noqa: UP045
        if query.query.root is None:
            return None
        query_hash = self._get_query_hash(query=query.query.root)
        service = model_str(query.service) if query.service else None
        query_entity = self.get_by_name(entity=Query, fqn=self._query_fqn(service, query_hash))
        if query_entity is None:
            resp = self.client.put(self.get_suffix(Query), data=query.model_dump_json())
            if resp and resp.get("id"):
                query_entity = Query(**resp)
        return query_entity

    def ingest_entity_queries_data(self, entity: Union[Table, Dashboard], queries: List[CreateQueryRequest]) -> None:  # noqa: UP006, UP007
        """
        PUT queries for an entity

        :param entity: Entity to update
        :param queries: CreateQueryRequest to add
        """
        for create_query in queries:
            if not create_query.exclude_usage:
                create_query.query.root = mask_query(create_query.query.root, create_query.dialect)
                query = self._get_or_create_query(create_query)
                if query:
                    # Add Query Usage
                    table_ref = EntityReference(id=entity.id.root, type="table")
                    # convert object to json array string
                    table_ref_json = "[" + table_ref.model_dump_json() + "]"
                    self.client.put(
                        f"{self.get_suffix(Query)}/{model_str(query.id)}/usage",
                        data=table_ref_json,
                    )

                    # Add Query Users
                    user_fqn_list = create_query.users
                    if user_fqn_list:
                        self.client.put(
                            f"{self.get_suffix(Query)}/{model_str(query.id)}/users",
                            data=json.dumps([model_str(user_fqn) for user_fqn in user_fqn_list]),
                        )

                    # Add Query used by
                    user_list = create_query.usedBy
                    if user_list:
                        self.client.put(
                            f"{self.get_suffix(Query)}/{model_str(query.id)}/usedBy",
                            data=json.dumps(user_list),
                        )

    def ingest_queries_bulk(
        self,
        queries: Iterable[Tuple[CreateQueryRequest, EntityReference]],  # noqa: UP006
        batch_size: int = QUERY_BULK_BATCH_SIZE,
        threads: int = 1,
        processes: int = 1,
    ) -> None:
        """
        Attach many (query, entity) pairs at once.

        ``threads`` fans out the one lookup per distinct query (and the relation PUTs for queries
        that already exist): each is a ~100 ms round trip and a day of usage holds tens of
        thousands of distinct statements, so serially this step alone ran for hours. ``processes``
        masks the distinct statement shapes in worker processes: masking is CPU-bound (tens of ms
        per statement, hundreds for a long MERGE) and the GIL keeps threads from helping.

        The per-entity path (``ingest_entity_queries_data``) costs a lookup, a create and up to three
        relation PUTs for every pair, and a query that touches N tables is sent N times. Here each
        distinct query is looked up once; new queries go through ``PUT /queries/bulk`` with their
        usage, users and usedBy set at creation, and existing queries receive only the relations
        they lack, additively, so relations recorded by earlier runs are kept.
        """
        queries = list(queries)
        self._mask_shapes(queries, processes)
        pending: Dict[Tuple[Optional[str], str], _PendingQuery] = {}  # noqa: UP006, UP045
        for create_query, entity_ref in queries:
            if create_query.exclude_usage or create_query.query.root is None:
                continue
            masked = self._masked(create_query.query.root, create_query.dialect)
            if masked is None:
                continue
            service = model_str(create_query.service) if create_query.service else None
            key = (service, self._get_query_hash(masked))
            entry = pending.get(key)
            if entry is None:
                entry = pending[key] = _PendingQuery(
                    request=create_query.model_copy(update={"query": SqlQuery(masked)})
                )
            entry.used_in.setdefault(model_str(entity_ref.id), entity_ref)
            for user in create_query.users or []:
                entry.users.setdefault(model_str(user), None)
            for used_by in create_query.usedBy or []:
                entry.used_by.setdefault(used_by, None)

        def _lookup(item: Tuple[Tuple[Optional[str], str], _PendingQuery]) -> Tuple[Optional[Query], _PendingQuery]:  # noqa: UP006, UP045
            (service, query_hash), entry = item
            existing = self.get_by_name(
                entity=Query, fqn=self._query_fqn(service, query_hash), fields=["queryUsedIn", "users"]
            )
            if existing is not None:
                self._attach_query_relations(existing, entry)
            return existing, entry

        to_create: List[_PendingQuery] = []  # noqa: UP006
        if threads > 1 and len(pending) > 1:
            with ThreadPoolExecutor(max_workers=threads) as pool:
                results = list(pool.map(_lookup, pending.items()))
        else:
            results = [_lookup(item) for item in pending.items()]
        to_create = [entry for existing, entry in results if existing is None]

        for start in range(0, len(to_create), batch_size):
            self._bulk_create_queries(to_create[start : start + batch_size])

    def _mask_shapes(self, queries, processes: int) -> None:
        """Fill the shape cache for every shape this flush needs and does not have yet, in
        ``processes`` workers when there are enough of them to be worth the pool."""
        todo: Dict[Tuple[str, Optional[str]], str] = {}  # noqa: UP006, UP045
        for create_query, _ in queries:
            text = create_query.query.root
            if create_query.exclude_usage or text is None:
                continue
            key = (statement_shape(text), create_query.dialect)
            if key not in _masked_by_shape and key not in todo:
                todo[key] = text
        if processes <= 1 or len(todo) < MASK_POOL_THRESHOLD:
            return
        keys = list(todo)
        started = time.perf_counter()
        masked = self._mask_pool(processes).map(_mask_in_worker, [(todo[key], key[1]) for key in keys], chunksize=8)
        for key, result in zip(keys, masked, strict=True):
            if result is not None:  # a worker failure is masked inline by _masked, with its logging
                _masked_by_shape[key] = result
        logger.info(
            f"Masked {len(keys):,} new statement shapes for {len(queries):,} queries "
            f"in {processes} processes, {time.perf_counter() - started:.0f}s"
        )

    def _mask_pool(self, processes: int) -> ProcessPoolExecutor:
        """One pool per client for the run: spawning a worker imports the whole framework (about ten
        seconds for eight of them) and the usage sink flushes queries every few thousand pairs, so a
        pool per flush spent longer starting workers than masking."""
        pool = getattr(self, "_query_mask_pool", None)
        if pool is None:
            pool = ProcessPoolExecutor(max_workers=processes)
            self._query_mask_pool = pool
        return pool

    def close_query_mask_pool(self) -> None:
        pool = getattr(self, "_query_mask_pool", None)
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
            self._query_mask_pool = None

    def _masked(self, text: str, dialect: Optional[str]) -> Optional[str]:  # noqa: UP045
        """Masking is the cost of this path: tens of ms per statement, and a day of usage holds
        tens of thousands of distinct texts that are the same statement re-issued with other
        literals (measured: 14,553 texts, 2,039 shapes in 400 table-day records). Masking replaces
        exactly those literals, so two statements with one shape mask to one text: mask once per
        shape, and remember it across flushes and files."""
        key = (statement_shape(text), dialect)
        if key not in _masked_by_shape:
            _masked_by_shape[key] = mask_query(text, dialect)
        return _masked_by_shape[key]

    def _bulk_create_queries(self, batch: List[_PendingQuery]) -> None:  # noqa: UP006
        requests = [
            CreateQueryRequest.model_validate(
                {
                    **entry.request.model_dump(exclude_none=True),
                    "queryUsedIn": [ref.model_dump(exclude_none=True) for ref in entry.used_in.values()],
                    "users": list(entry.users) or None,
                    "usedBy": list(entry.used_by) or None,
                }
            )
            for entry in batch
        ]
        result = self.bulk_create_or_update(requests)  # pyright: ignore[reportAttributeAccessIssue]
        if not result.numberOfRowsFailed.root:
            return
        logger.warning(
            f"{result.numberOfRowsFailed.root} of {len(batch)} queries failed in bulk create; retrying them one by one"
        )
        for entry in batch:
            try:
                query = self._get_or_create_query(entry.request)
                if query:
                    self._attach_query_relations(query, entry)
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(f"Failed to ingest query {entry.request.query.root[:80]!r}: {exc}")

    def _attach_query_relations(self, query: Query, entry: _PendingQuery) -> None:
        """PUT only the usage, users and usedBy the stored query does not have yet."""
        suffix = f"{self.get_suffix(Query)}/{model_str(query.id)}"
        have_used_in = {model_str(ref.id) for ref in (query.queryUsedIn.root if query.queryUsedIn else [])}
        missing_used_in = [ref for ref_id, ref in entry.used_in.items() if ref_id not in have_used_in]
        if missing_used_in:
            self.client.put(
                f"{suffix}/usage",
                data="[" + ",".join(ref.model_dump_json() for ref in missing_used_in) + "]",
            )
        have_users = {model_str(ref.fullyQualifiedName) for ref in (query.users or []) if ref.fullyQualifiedName}
        missing_users = [user for user in entry.users if user not in have_users]
        if missing_users:
            self.client.put(f"{suffix}/users", data=json.dumps(missing_users))
        have_used_by = set(query.usedBy or [])
        missing_used_by = [used_by for used_by in entry.used_by if used_by not in have_used_by]
        if missing_used_by:
            self.client.put(f"{suffix}/usedBy", data=json.dumps(missing_used_by))

    def get_entity_queries(
        self,
        entity_id: Uuid | str,
        fields: Optional[List[str]] = None,  # noqa: UP006, UP045
    ) -> Optional[List[Query]]:  # noqa: UP006, UP045
        """Get the queries attached to a table

        Args:
            entity_id (Union[Uuid,str]): entity id of given entity
            fields (Optional[List[str]]): list of fields to be included in response


        Returns:
            Optional[List[Query]]: List of queries
        """
        fields_str = "&fields=" + ",".join(fields) if fields else ""
        res = self.client.get(f"{self.get_suffix(Query)}?entityId={model_str(entity_id)}&{fields_str}")
        if res and res.get("data"):
            return [Query(**query) for query in res.get("data")]
        return None

    @lru_cache(maxsize=5000)  # noqa: B019
    def __get_query_by_hash(self, query_hash: str, service_name: str) -> Optional[Query]:  # noqa: UP045
        return self.get_by_name(entity=Query, fqn=f"{service_name}.{query_hash}")

    def publish_query_cost(
        self,
        query_cost_data: QueryCostWrapper,
        service_name: str,
        masked_query: Optional[str] = None,  # noqa: UP045
    ):
        """
        Create Query Cost Record

        Args:
            query_cost_record: QueryCostWrapper
            masked_query: the already-masked text, when the caller masks (and caches) it itself
        """
        if masked_query is None:
            masked_query = mask_query(query_cost_data.query, query_cost_data.dialect)

        # mask_query can return None if it fails to parse the query
        # Fall back to the original query so we can still track cost
        if masked_query is None:
            masked_query = query_cost_data.query

        query_hash = self._get_query_hash(masked_query)

        query = self.__get_query_by_hash(query_hash=query_hash, service_name=service_name)
        if not query:
            return None

        create_request = CreateQueryCostRecordRequest(
            timestamp=int(query_cost_data.date),
            jsonSchema="queryCostRecord",
            queryReference=EntityReference(id=query.id.root, type="query"),
            cost=query_cost_data.cost,
            count=query_cost_data.count,
            totalDuration=query_cost_data.totalDuration,
        )

        return self.client.post(self.get_suffix(QueryCostRecord), data=create_request.model_dump_json())
