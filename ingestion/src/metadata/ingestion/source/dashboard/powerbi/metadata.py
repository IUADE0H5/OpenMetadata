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
"""PowerBI source module"""

import re
import traceback
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import (  # noqa: UP035
    Any,
    Callable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Union,
)

from pydantic import EmailStr
from pydantic_core import PydanticCustomError

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.chart import Chart, ChartType
from metadata.generated.schema.entity.data.dashboard import Dashboard, DashboardType
from metadata.generated.schema.entity.data.dashboardDataModel import (
    DashboardDataModel,
    DataModelType,
)
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import (
    PowerBIConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.dashboardService import (
    DashboardServiceType,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
    SourceUrl,
)
from metadata.generated.schema.type.entityLineage import ColumnLineage
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.generated.schema.type.filterPattern import FilterPattern
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.lineage.models import Dialect
from metadata.ingestion.lineage.parser import LineageParser
from metadata.ingestion.lineage.sql_lineage import get_column_fqn
from metadata.ingestion.models.barrier import Barrier
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.ometa.utils import model_str
from metadata.ingestion.progress.modes import ProgressMode
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource
from metadata.ingestion.source.dashboard.powerbi.constants import (
    ATHENA_DATABASES_EXPRESSION_KW,
    ATHENA_DEFAULT_CATALOG,
    BIGQUERY_QUERY_EXPRESSION_KW,
    DATABRICKS_QUERY_EXPRESSION_KW,
    DEFAULT_REPORTS_PREFIX,
    MAX_PROJECT_FILTER_SIZE,
    ODBC_DATASOURCE_EXPRESSION_KW,
    ODBC_QUERY_EXPRESSION_KW,
    OWNER_ACCESS_RIGHTS_KEYWORDS,
    POWERBI_APP_PRINCIPAL_TYPE,
    POWERBI_GROUP_PRINCIPAL_TYPE,
    POWERBI_USER_PRINCIPAL_TYPE,
    POWERBI_VISUAL_TYPE_TO_CHART_TYPE,
    POWERBI_WRITE_WORKSPACE_ROLES,
    RDL_REPORT_FORMAT,
    RDL_REPORTS_PREFIX,
    SNOWFLAKE_QUERY_EXPRESSION_KW,
    SQL_DATABASE_EXPRESSION_KW,
    SQL_LINE_COMMENT_PATTERN,
    is_write_dataset_right,
)
from metadata.ingestion.source.dashboard.powerbi.databricks_parser import (
    parse_databricks_native_query_source,
)
from metadata.ingestion.source.dashboard.powerbi.fabric_client import FabricApiClient
from metadata.ingestion.source.dashboard.powerbi.models import (
    Dataflow,
    DataflowExportResponse,
    Datamart,
    Dataset,
    Group,
    PowerBiColumns,
    PowerBIDashboard,
    PowerBiMeasureModel,
    PowerBiMeasures,
    PowerBIPrincipal,
    PowerBIReport,
    PowerBiTable,
    PowerBITableSource,
    ReportPage,
    UpstreaDataflow,
)
from metadata.ingestion.source.dashboard.powerbi.workspace_state import WorkspaceState
from metadata.ingestion.source.database.column_helpers import truncate_column_name
from metadata.ingestion.source.database.column_type_parser import ColumnTypeParser
from metadata.utils import fqn
from metadata.utils.filters import (
    filter_by_chart,
    filter_by_dashboard,
    filter_by_datamodel,
)
from metadata.utils.fqn import build_es_fqn_search_string
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


@dataclass(frozen=True)
class LineageTargetSpec:
    """OM entity type and `fqn.build` kwarg for a lineage target kind.

    `DATAMODEL_TARGET` and `DASHBOARD_TARGET` are the valid instances.
    """

    entity_type: type
    fqn_kwarg: str


DATAMODEL_TARGET = LineageTargetSpec(
    entity_type=DashboardDataModel,
    fqn_kwarg="data_model_name",
)
DASHBOARD_TARGET = LineageTargetSpec(
    entity_type=Dashboard,
    fqn_kwarg="dashboard_name",
)


# --- Report-column-usage feature: structural contract for the objects
# `report_definition.py`, `tmdl.py` and `column_usage.py` produce. -------------------
#
# These are `Protocol`s, not imports of the real dataclasses: the parser modules are
# built in parallel (a sibling task) and may not exist in every checkout yet, and this
# connector must import cleanly either way. A `Protocol` gives real structural type
# checking against the real dataclasses once they land (matching field names satisfy it
# automatically) without a hard `import` dependency. The actual parsing calls
# (`_parse_report_definition`, `_parse_semantic_model_definition`,
# `_compute_column_usage`) import the real modules lazily and degrade to a logged no-op
# if the module isn't present yet - see those methods.
class FieldRefLike(Protocol):
    kind: str  # "column" | "measure" | "hierarchy_level"
    table: str
    name: str


class VisualDefinitionLike(Protocol):
    visual_id: str
    page_id: str
    page_display_name: Optional[str]  # noqa: UP045
    visual_type: str
    title: Optional[str]  # noqa: UP045
    refs: List[FieldRefLike]  # noqa: UP006
    is_data_visual: bool


class ReportDefinitionLike(Protocol):
    format: Optional[str]  # noqa: UP045
    visuals: List[VisualDefinitionLike]  # noqa: UP006


class ColumnUseLike(Protocol):
    table: str
    column: str
    via_measure: Optional[str]  # noqa: UP045


class ModelColumnUsageLike(Protocol):
    business_columns: frozenset
    used: frozenset
    unused: frozenset
    wholly_unused_tables: frozenset
    per_visual: Mapping[Tuple[str, str], List[ColumnUseLike]]  # noqa: UP006
    dangling: Mapping[str, List[FieldRefLike]]  # noqa: UP006
    dax_unresolved: set
    counters: Mapping[str, int]


class DataflowSourceRefLike(Protocol):
    workspace_id: Optional[str]  # noqa: UP045
    dataflow_id: str
    entity: str


class ColumnMappingLike(Protocol):
    mapped: Mapping[str, str]
    unmapped: List[str]  # noqa: UP006


class PowerbiSource(DashboardServiceSource):
    """PowerBi Source Class"""

    progress_mode = ProgressMode.MANUAL

    config: WorkflowSource
    metadata_config: OpenMetadataConnection

    # `metric_values()` keys - plain counters an external metrics reporter
    # can read after ingestion without re-deriving them from logs.
    METRIC_DATAFLOWS_FETCHED = "dataflows_fetched"
    # Recognized Athena/ODBC M query blocks, counted once per block regardless
    # of loadEnabled - "seen" means detected, not "successfully dispatched".
    METRIC_ATHENA_ODBC_QUERIES_SEEN = "athena_odbc_queries_seen"
    # Distinct (data model, table reference) pairs per run; resolved + unresolved
    # == parsed by construction (see `_record_source_reference`).
    METRIC_SOURCE_REFERENCES_PARSED = "source_references_parsed"
    METRIC_SOURCE_REFERENCES_RESOLVED = "source_references_resolved"
    METRIC_SOURCE_REFERENCES_UNRESOLVED = "source_references_unresolved"
    # Only queries recognized as Athena/ODBC/Sql.Database sourced (i.e. that
    # would otherwise have reached the lineage parser) and skipped because
    # loadEnabled is not true - not every disabled query in the document.
    METRIC_QUERIES_SKIPPED_LOAD_DISABLED = "queries_skipped_load_disabled"
    # Dataset->dataflow links whose workspaceObjectId is not one of the
    # workspaces this run processed (outside projectFilterPattern, or not
    # visible to the caller); never resolved, logged once per foreign workspace.
    METRIC_UPSTREAM_LINKS_OUTSIDE_SCOPE = "upstream_links_outside_scope"

    # Non-admin owner resolution (`_get_owner_ref_non_admin`) only - the admin
    # scan's owner path (`_get_owner_ref_admin`) isn't metered, it always had
    # the entity's `users` inline.
    # `owners_assigned_*` is always per asset: a report that inherits its
    # owners from a cached dataset computation still gets its own increment
    # here (`_get_owner_ref_non_admin` bumps it once per top-level
    # `get_owner_ref` call, independent of whether the underlying
    # `_compute_*_owner_refs` call was a cache hit).
    METRIC_OWNERS_ASSIGNED_DATAMODELS = "owners_assigned_datamodels"
    METRIC_OWNERS_ASSIGNED_DATAFLOWS = "owners_assigned_dataflows"
    METRIC_OWNERS_ASSIGNED_REPORTS = "owners_assigned_reports"
    METRIC_OWNERS_ASSIGNED_DASHBOARDS = "owners_assigned_dashboards"
    # Per-(owning asset, principal) counters, not per dependent that reaches
    # that asset: `_compute_datamodel_owner_refs`/`_compute_dataflow_owner_refs`
    # memoise the owner set per dataset/dataflow id in `WorkspaceState`
    # (cleared per workspace), so a dataset with three dependent reports
    # counts its principals once here, not three times - a report/dashboard
    # inheriting a cached owner set never re-enters principal resolution.
    # A future counter that is instead meant to count once per dependent
    # traversal (rather than once per owning asset) should carry an
    # `_occurrences` suffix to say so, not this shape.
    METRIC_OWNER_PRINCIPALS_SKIPPED_APP = "owner_principals_skipped_app"
    METRIC_OWNER_PRINCIPALS_SKIPPED_VIEWER = "owner_principals_skipped_viewer"
    METRIC_OWNER_PRINCIPALS_UNRESOLVED = "owner_principals_unresolved"
    METRIC_ASSETS_WITHOUT_OWNER = "assets_without_owner"

    # Report -> semantic-model column usage (`report_column_usage_enabled`). Off by
    # default - see the class attribute below; every key here still seeds at 0
    # regardless, same as every other METRIC_*.
    METRIC_REPORT_DEFINITIONS_FETCHED = "report_definitions_fetched"
    METRIC_REPORT_DEFINITIONS_CACHE_SKIPPED = "report_definitions_cache_skipped"
    METRIC_REPORT_DEFINITIONS_FAILED = "report_definitions_failed"
    METRIC_MODEL_DEFINITIONS_FETCHED = "model_definitions_fetched"
    METRIC_MODEL_DEFINITIONS_CACHE_SKIPPED = "model_definitions_cache_skipped"
    METRIC_MODEL_DEFINITIONS_FAILED = "model_definitions_failed"
    METRIC_REPORTS_FORMAT_LEGACY = "reports_format_legacy"
    METRIC_REPORTS_FORMAT_PBIR = "reports_format_pbir"
    METRIC_REPORTS_FORMAT_UNKNOWN = "reports_format_unknown"
    METRIC_VISUALS_DATA = "visuals_data"
    METRIC_VISUALS_NON_DATA = "visuals_non_data"
    METRIC_VISUALS_SKIPPED = "visuals_skipped"
    METRIC_COLUMN_REFS_RESOLVED = "column_refs_resolved"
    METRIC_COLUMN_REFS_DANGLING = "column_refs_dangling"
    METRIC_MEASURES_RESOLVED_TRANSITIVELY = "measures_resolved_transitively"
    METRIC_DAX_UNRESOLVED = "dax_unresolved"
    METRIC_REPORT_VISUAL_CHARTS_CREATED = "report_visual_charts_created"
    METRIC_MODEL_COLUMNS_INGESTED = "model_columns_ingested"
    METRIC_AUTO_DATE_TABLES_SKIPPED = "auto_date_tables_skipped"
    METRIC_COLUMN_LINEAGE_EMITTED = "column_lineage_emitted"
    METRIC_COLUMN_LINEAGE_VERIFIED = "column_lineage_verified"
    METRIC_COLUMN_LINEAGE_DROPPED_BY_SERVER = "column_lineage_dropped_by_server"
    METRIC_DATAFLOW_MODEL_COLUMNS_MAPPED = "dataflow_model_columns_mapped"
    METRIC_DATAFLOW_MODEL_COLUMNS_UNMAPPED = "dataflow_model_columns_unmapped"
    METRIC_MODEL_COLUMNS_USED = "model_columns_used"
    METRIC_MODEL_COLUMNS_UNUSED = "model_columns_unused"

    # Report -> semantic-model column usage: fetches report/model definitions from
    # Fabric (`fabric_client.py`), ingests model columns from TMDL instead of the
    # (always-empty, non-admin) push-dataset tables call, creates one Chart per data
    # visual, and emits model-column -> chart / dataflow-column -> model-column
    # lineage. Off by default: `powerBIConnection.json` has `additionalProperties:
    # false` so there is no config flag for it - a subclass (e.g. a bank-specific one)
    # opts in by overriding this ClassVar to True. With it False, behaviour is
    # byte-for-byte identical to before this feature existed - see
    # `test_report_column_usage_disabled_is_unchanged` for the proof.
    report_column_usage_enabled: ClassVar[bool] = False

    # Bounded per CLAUDE.md's cache rule: this de-dupes (data model, table
    # reference) pairs for one connector run. A tenant's total distinct
    # references realistically stays far under this; beyond it we degrade to
    # per-occurrence counting (resolved/unresolved still stay consistent with
    # parsed) rather than grow unbounded.
    _MAX_TRACKED_SOURCE_REFERENCES = 50_000
    # Caps the volume of "unresolved reference" INFO log lines per run.
    _MAX_UNRESOLVED_LOGGED = 50

    def __init__(
        self,
        config: WorkflowSource,
        metadata: OpenMetadata,
    ):
        super().__init__(config, metadata)
        self.pagination_entity_per_page = min(100, self.service_connection.pagination_entity_per_page)
        self.datamodel_file_mappings = []
        self.state = WorkspaceState()
        # Every declared METRIC_* key starts at 0, not merely absent. A bare
        # Counter() only emits a key once it's first incremented, so a metric
        # that is legitimately always 0 for a run (e.g. no unresolved owners)
        # is indistinguishable on the far side of a Prometheus scrape from
        # "this code path never ran" - exactly the failure mode that hid a
        # real bug for a full run. Seeding every key here, from the class's
        # own METRIC_* attributes rather than a hand-maintained list, makes
        # that impossible to forget when a metric is added.
        self._metrics = Counter(dict.fromkeys(self._all_metric_keys(), 0))
        self._counted_source_references: set = set()
        self._unresolved_logged_count = 0
        # Fabric client for report/model `getDefinition` calls - only constructed
        # when the feature is switched on, so a connector run with it off never even
        # builds the extra msal client. Built eagerly here (not lazily on first use)
        # so a config problem surfaces once, consistently, rather than differently
        # depending on which workspace happens to hit it first - but unlike
        # `PowerBiApiClient` (client.py), which sits behind the connection-test
        # lifecycle (`connection.py`'s `PowerBIConnection._get_client`), this
        # constructor runs before `test_connection()` and must never let a bad SPN
        # or an unreachable tenant abort `__init__` itself: every consumer already
        # treats `fabric_client is None` as "feature unavailable this run" (logs +
        # a `*_failed` metric), the same degrade-gracefully contract as an
        # individual fetch failing.
        self.fabric_client: Optional[FabricApiClient] = None  # noqa: UP045
        if self.report_column_usage_enabled:
            try:
                self.fabric_client = FabricApiClient(self.service_connection)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning(f"Could not construct the Fabric API client: {exc}")
                logger.debug(traceback.format_exc())
        # (from_entity_id, to_entity_id, edge_kind, emitted_column_count) for every
        # column-lineage edge yielded this run - read back and compared against what
        # the server actually stored once the workspace's lineage barrier flushes.
        self._pending_column_lineage_edges: List[Tuple[str, str, str, int]] = []  # noqa: UP006

    @classmethod
    def _all_metric_keys(cls) -> List[str]:  # noqa: UP006
        """Every ``METRIC_*`` class attribute's value, walking the full MRO so a
        subclass's own metrics are included too - the single source of truth
        for ``metric_values()``'s full key set.
        """
        return [
            name
            for name in (getattr(cls, attr) for attr in dir(cls) if attr.startswith("METRIC_"))
            if isinstance(name, str)
        ]

    def metric_values(self) -> dict[str, int]:
        """Cheap, side-effect free snapshot of dataflow/M-parsing volume counters.

        Always returns the full, stable set of ``METRIC_*`` keys (each starts
        at 0 in ``__init__``) - never a partial dict missing a key that just
        happened not to be incremented this run.

        A subclass's metrics reporter picks this up automatically after
        ingestion; see the ``METRIC_*`` class attributes for the keys.
        """
        return dict(self._metrics)

    def on_datamodel_column_usage(
        self,
        datamodel_entity: DashboardDataModel,
        usage: ModelColumnUsageLike,
    ) -> None:
        """Overridable hook: called once per semantic model with its computed
        report-column usage, so a subclass can act on it (e.g. tag unused columns).

        No-op by default - generic ingestion never writes anything from this hook on
        its own. Only called when `report_column_usage_enabled` is True, once per
        model, after that model entity (with its persisted columns) exists in
        OpenMetadata - i.e. after the per-workspace lineage-flush Barrier, the same
        point `yield_dashboard_lineage_details` already relies on for every other
        cross-entity lookup in this connector.
        """
        return

    def _record_source_reference(
        self,
        datamodel_entity: DashboardDataModel,
        fqn_search_string: str,
        resolved: bool,
    ) -> None:
        """Count one distinct (data model, table reference) pair, once per run.

        Keeps ``source_references_parsed == resolved + unresolved`` true by
        construction: both counters are updated together for the same key,
        and a key already seen (e.g. the same table reached via two different
        M queries in one dataflow) is not counted again. Unresolved references
        are logged at INFO, once per pair, up to ``_MAX_UNRESOLVED_LOGGED``.
        """
        key = (model_str(datamodel_entity.name), fqn_search_string)
        if key in self._counted_source_references:
            return
        if len(self._counted_source_references) < self._MAX_TRACKED_SOURCE_REFERENCES:
            self._counted_source_references.add(key)
        self._metrics[self.METRIC_SOURCE_REFERENCES_PARSED] += 1
        self._metrics[
            self.METRIC_SOURCE_REFERENCES_RESOLVED if resolved else self.METRIC_SOURCE_REFERENCES_UNRESOLVED
        ] += 1
        if not resolved and self._unresolved_logged_count < self._MAX_UNRESOLVED_LOGGED:
            self._unresolved_logged_count += 1
            logger.info(
                "Unresolved PowerBI source reference: data model=[%s] fqn_search_string=[%s]",
                datamodel_entity.displayName or model_str(datamodel_entity.name),
                fqn_search_string,
            )

    def get_org_workspace_data(self) -> Iterable[Optional[Group]]:  # noqa: UP045
        """
        fetch all the workspace data for non-admin users
        """
        filter_pattern = self.source_config.projectFilterPattern
        paginated_filter_patterns = self._paginate_project_filter_pattern(filter_pattern)
        if len(paginated_filter_patterns) > 1:
            logger.info(
                f"Paginating workspace fetch with {len(paginated_filter_patterns)}"
                f" batches to accommodate OData filter node limit"
            )

        # Fetch every batch's workspace list up front (`fetch_all_workspaces`
        # already returns a materialized list per batch) so a dataset->dataflow
        # link's `workspaceObjectId` can be judged against the complete set of
        # workspaces this run will process, not just the ones seen so far in
        # this generator's stream.
        workspace_batches = [
            self.client.api_client.fetch_all_workspaces(pattern) or [] for pattern in paginated_filter_patterns
        ]
        known_workspace_ids = {workspace.id for batch in workspace_batches for workspace in batch}
        outside_scope_link_counts: Counter = Counter()

        workspace_total = 0
        for workspaces in workspace_batches:
            if workspaces:
                workspace_total += len(workspaces)
                self.progress_tracking.manual.set_total("Workspaces", workspace_total)
                for workspace in workspaces:
                    # add the dashboards to the workspace
                    workspace.dashboards.extend(
                        self.client.api_client.fetch_all_org_dashboards(group_id=workspace.id) or []
                    )
                    for dashboard in workspace.dashboards:
                        # add the tiles to the dashboards
                        dashboard.tiles.extend(
                            self.client.api_client.fetch_all_org_tiles(group_id=workspace.id, dashboard_id=dashboard.id)
                            or []
                        )

                    # add the reports to the workspaces
                    workspace.reports.extend(self.client.api_client.fetch_all_org_reports(group_id=workspace.id) or [])

                    # add the datasets to the workspaces
                    workspace.datasets.extend(
                        self.client.api_client.fetch_all_org_datasets(group_id=workspace.id) or []
                    )
                    for dataset in workspace.datasets:
                        # add the tables to the datasets
                        dataset.tables.extend(
                            self.client.api_client.fetch_dataset_tables(group_id=workspace.id, dataset_id=dataset.id)
                            or []
                        )
                        # add this dataset's ACL, for non-admin owner resolution
                        # (see `_compute_datamodel_owner_refs`) - this endpoint
                        # may list principals who aren't workspace members.
                        dataset.dataset_principals = [
                            principal
                            for principal in (
                                PowerBIPrincipal.from_dataset_user(user)
                                for user in self.client.api_client.fetch_dataset_users(
                                    group_id=workspace.id, dataset_id=dataset.id
                                )
                                or []
                            )
                            if principal
                        ]

                    # add this workspace's membership, for non-admin owner
                    # resolution (see `_compute_dataflow_owner_refs` and
                    # `_compute_datamodel_owner_refs`) - non-admin entities
                    # never carry `users` inline the way the admin scan does.
                    workspace.workspace_principals = [
                        principal
                        for principal in (
                            PowerBIPrincipal.from_workspace_user(user)
                            for user in self.client.api_client.fetch_group_users(group_id=workspace.id) or []
                        )
                        if principal
                    ]

                    # add the dataflows to the workspace, and each dataflow's own
                    # upstream dataflows (non-admin has no bulk equivalent of the
                    # admin scan's inline upstreamDataflows, so this is per-dataflow)
                    workspace.dataflows.extend(
                        self.client.api_client.fetch_all_org_dataflows(group_id=workspace.id) or []
                    )
                    for dataflow in workspace.dataflows:
                        dataflow.upstreamDataflows.extend(
                            self.client.api_client.fetch_dataflow_upstream(
                                group_id=workspace.id, dataflow_id=dataflow.id
                            )
                            or []
                        )

                    # non-admin datasets never carry upstreamDataflows inline (unlike
                    # the admin scan); the link only exists via this workspace-wide call
                    dataset_by_id = {dataset.id: dataset for dataset in workspace.datasets}
                    for link in self.client.api_client.fetch_dataset_to_dataflow_links(group_id=workspace.id) or []:
                        dataset = dataset_by_id.get(link.datasetObjectId)
                        if dataset is None or not link.dataflowObjectId:
                            continue
                        if link.workspaceObjectId and link.workspaceObjectId not in known_workspace_ids:
                            # The target dataflow's workspace isn't ingested this
                            # run, so it can never resolve - don't even try.
                            outside_scope_link_counts[link.workspaceObjectId] += 1
                            continue
                        dataset.upstreamDataflows.append(UpstreaDataflow(targetDataflowId=link.dataflowObjectId))

                    yield workspace
            else:
                logger.error("Unable to fetch any PowerBI workspaces")

        for foreign_workspace_id, count in outside_scope_link_counts.items():
            self._metrics[self.METRIC_UPSTREAM_LINKS_OUTSIDE_SCOPE] += count
            logger.info(
                "PowerBI dataset-to-dataflow links reference workspace [%s], which is outside "
                "this run's scope (%d link(s)); not resolved.",
                foreign_workspace_id,
                count,
            )

    def _paginate_project_filter_pattern(self, filter_pattern):
        """
        paginate include filters if more then `10` filters
        in single call
        """
        if not filter_pattern:
            # default include filter needed to include all
            # workspaces
            return [FilterPattern(includes=[".*"])]
        # case handling if only exclude filters
        # are provided.
        paginated_include_filters = [filter_pattern]
        if filter_pattern.includes:
            # if include filters are present then paginate them
            # in the batch of `MAX_PROJECT_FILTER_SIZE` while
            # keeping exclude filters same across all batches
            include_filters = [
                filter_pattern.includes[i : i + MAX_PROJECT_FILTER_SIZE]
                for i in range(0, len(filter_pattern.includes), MAX_PROJECT_FILTER_SIZE)
            ]
            paginated_include_filters = []
            for include_filter in include_filters:
                filter_pattern_copy = deepcopy(filter_pattern)
                filter_pattern_copy.includes = include_filter
                paginated_include_filters.append(filter_pattern_copy)
        return paginated_include_filters

    def get_admin_workspace_data(self) -> Iterable[Optional[Group]]:  # noqa: UP045
        """
        fetch all the workspace data
        """
        filter_pattern = self.source_config.projectFilterPattern
        paginated_filter_patterns = self._paginate_project_filter_pattern(filter_pattern)
        if len(paginated_filter_patterns) > 1:
            logger.info(
                f"Paginating workspace fetch with {len(paginated_filter_patterns)}"
                f" batches to accommodate OData filter node limit"
            )
        active_workspace_total = 0
        for filter_pattern in paginated_filter_patterns:
            workspaces = self.client.api_client.fetch_all_workspaces(filter_pattern)
            if workspaces:
                workspace_id_list = [workspace.id for workspace in workspaces]
                workspace_name_by_id = {workspace.id: workspace.name for workspace in workspaces}

                # Start the scan of the available workspaces for dashboard metadata
                workspace_paginated_list = [
                    workspace_id_list[i : i + self.pagination_entity_per_page]
                    for i in range(0, len(workspace_id_list), self.pagination_entity_per_page)
                ]
                count = 1
                for workspace_ids_chunk in workspace_paginated_list:
                    logger.info(f"Scanning {count}/{len(workspace_paginated_list)} set of workspaces")
                    workspace_scan = self.client.api_client.initiate_workspace_scan(workspace_ids_chunk)
                    if not workspace_scan:
                        logger.error(
                            f"Error initiating workspace scan for ids:{str(workspace_ids_chunk)}\n moving to next set of workspaces"  # noqa: RUF010
                        )
                        count += 1
                        continue

                    # Keep polling the scan status endpoint to check if scan is succeeded
                    workspace_scan_status = self.client.api_client.wait_for_scan_complete(scan_id=workspace_scan.id)
                    if not workspace_scan_status:
                        logger.error(
                            f"Max poll hit to scan status for scan_id: {workspace_scan.id}, moving to next set of workspaces"
                        )
                        count += 1
                        continue

                    # Get scan result for successfull scan
                    response = self.client.api_client.fetch_workspace_scan_result(scan_id=workspace_scan.id)
                    if not response:
                        logger.error(f"Error getting workspace scan result for scan_id: {workspace_scan.id}")
                        count += 1
                        continue
                    active_workspaces = []
                    skipped_by_state = Counter()
                    scan_workspace_ids = set()
                    for workspace in response.workspaces:
                        scan_workspace_ids.add(workspace.id)
                        if workspace.state == "Active":
                            active_workspaces.append(workspace)
                        else:
                            skipped_by_state[workspace.state or "<missing>"] += 1
                    missing_from_scan = set(workspace_ids_chunk) - scan_workspace_ids
                    logger.info(
                        "PowerBI workspace scan summary: requested=%s, returned=%s, active=%s, skippedByState=%s, missingFromScan=%s",
                        len(workspace_ids_chunk),
                        len(response.workspaces),
                        len(active_workspaces),
                        dict(skipped_by_state),
                        len(missing_from_scan),
                    )
                    if skipped_by_state or missing_from_scan:
                        skipped_workspaces = [
                            f"{workspace.id}:{workspace.name}:{workspace.state or '<missing>'}"
                            for workspace in response.workspaces
                            if workspace.state != "Active"
                        ]
                        missing_workspaces = [
                            f"{workspace_id}:{workspace_name_by_id.get(workspace_id)}"
                            for workspace_id in sorted(missing_from_scan)
                        ]
                        logger.debug(
                            "PowerBI workspace scan skipped details: nonActive=%s, missingFromScan=%s",
                            skipped_workspaces,
                            missing_workspaces,
                        )
                    active_workspace_total += len(active_workspaces)
                    self.progress_tracking.manual.set_total("Workspaces", active_workspace_total)
                    yield from active_workspaces
                    count += 1
            else:
                logger.error("Unable to fetch any PowerBI workspaces")

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None):  # noqa: UP045
        config = WorkflowSource.model_validate(config_dict)
        connection: PowerBIConnection = config.serviceConnection.root.config
        if not isinstance(connection, PowerBIConnection):
            raise InvalidSourceException(f"Expected PowerBIConnection, but got {connection}")
        return cls(config, metadata)

    def _prepare_workspace_data(self) -> Iterable[Group]:
        """
        - Since we get all the required info i.e. reports, dashboards, charts, datasets
          with workflow scan approach, we are populating bulk data for workspace.
        - Some individual APIs are not able to yield data with details.
        - Workspaces that failed to fetch (None) are filtered out at the producer.
        """
        producer = (
            self.get_admin_workspace_data() if self.service_connection.useAdminApis else self.get_org_workspace_data()
        )
        for workspace in producer:
            if workspace is None:
                continue
            yield workspace

    def _progress_group_name(self) -> str:
        workspace = self.context.get().workspace  # pyright: ignore[reportAttributeAccessIssue]
        return str(getattr(workspace, "name", None) or getattr(workspace, "id", "<unknown>"))

    def get_dashboard(self) -> Any:
        """
        Method to iterate through dashboard lists filter dashboards & yield dashboard details
        """
        self._declare_progress_groups("Workspaces", None)
        for workspace in self._prepare_workspace_data():
            workspace_name = str(getattr(workspace, "name", None) or getattr(workspace, "id", "<unknown>"))
            opened = False
            try:
                self.state.enter(workspace)
                self.context.get().workspace = workspace  # pyright: ignore[reportAttributeAccessIssue]
                for dashboard in self.get_dashboards_list() or []:
                    dashboard_details = self.get_dashboard_details(dashboard)
                    dashboard_name = self.get_dashboard_name(dashboard_details)
                    if not dashboard_name:
                        logger.debug(
                            "Skipping PowerBI dashboard with empty name on workspace [%s]",
                            workspace.name,  # pyright: ignore[reportOptionalMemberAccess]
                        )
                        continue
                    if filter_by_dashboard(
                        self.source_config.dashboardFilterPattern,
                        dashboard_name,
                    ):
                        self.status.filter(
                            dashboard_name,
                            "Dashboard Filtered Out",
                        )
                        continue
                    self.state.add_filtered_dashboard(dashboard_details)
                self._open_group_progress(
                    workspace_name,
                    {
                        "Dashboard": len(self.state.filtered_dashboards),
                        "Chart": None,
                        "DashboardDataModel": None,
                    },
                )
                opened = True
                yield workspace
            except Exception as exc:  # pylint: disable=broad-except
                ws_name = getattr(workspace, "name", None) or getattr(workspace, "id", "<unknown>")
                logger.warning("Failed to process PowerBI workspace '%s': %s", ws_name, exc)
                self.status.failed(
                    StackTraceError(
                        name=f"Workspace {ws_name}",
                        error=f"Failed to process workspace '{ws_name}': {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )
            finally:
                if opened:
                    self._close_group_progress(workspace_name)
                self.state.exit()

    def get_dashboards_list(
        self,
    ) -> Optional[List[Union[PowerBIDashboard, PowerBIReport]]]:  # noqa: UP006, UP007, UP045
        """
        Get List of all dashboards
        """
        return self.context.get().workspace.reports + self.context.get().workspace.dashboards  # pyright: ignore[reportAttributeAccessIssue]

    def get_dashboard_name(self, dashboard: Union[PowerBIDashboard, PowerBIReport]) -> str | None:  # noqa: UP007  # pyright: ignore[reportIncompatibleMethodOverride]
        """
        Get Dashboard Name
        """
        if isinstance(dashboard, PowerBIDashboard):
            return dashboard.displayName
        return dashboard.name

    def get_dashboard_details(
        self,
        dashboard: Union[PowerBIDashboard, PowerBIReport],  # noqa: UP007
    ) -> Union[PowerBIDashboard, PowerBIReport]:  # noqa: UP007
        """
        Get Dashboard Details
        """
        return dashboard

    def _get_dashboard_url(self, workspace_id: str, dashboard_id: str) -> str:
        """
        Method to build the dashboard url
        """
        return (
            f"{clean_uri(self.service_connection.hostPort)}/groups/"
            f"{workspace_id}/dashboards/{dashboard_id}?experience=power-bi"
        )

    def _get_report_url(self, workspace_id: str, dashboard_details: PowerBIReport) -> str:
        """
        Method to build the dashboard url
        """
        page_id = ""
        dashboard_id = dashboard_details.id
        reports_prefix = DEFAULT_REPORTS_PREFIX
        if isinstance(dashboard_details.format, str) and dashboard_details.format == RDL_REPORT_FORMAT:
            reports_prefix = RDL_REPORTS_PREFIX
        try:
            pages: Optional[List[ReportPage]] = self.client.api_client.fetch_report_pages(workspace_id, dashboard_id)  # noqa: UP006, UP045
            if (
                pages and pages[0].name
            ):  # if there are pages and page has name then only add page id in url:  # if there are pages and page has name then only add page id in url
                # get first page out of multiple pages otherwise
                # get page if of single page
                page_id = pages[0].name
            page_id = f"/{page_id}" if page_id else ""
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error building report page url: {exc}")
        # https://app.powerbi.com/groups/4e57dcbb-***/reports/a2902011-***/098b***?experience=power-bi
        return (
            f"{clean_uri(self.service_connection.hostPort)}/groups/"
            f"{workspace_id}/{reports_prefix}/{dashboard_id}{page_id}?experience=power-bi"
        )

    def _get_dataset_url(self, workspace_id: str, dataset_id: str) -> str:
        """
        Method to build the dataset url
        """
        return (
            f"{clean_uri(self.service_connection.hostPort)}/groups/"
            f"{workspace_id}/datasets/{dataset_id}/details?experience=power-bi"
        )

    def _get_dataflow_url(self, workspace_id: str, dataflow_id: str) -> str:
        """
        Method to build the dataset url
        """
        return (
            f"{clean_uri(self.service_connection.hostPort)}/groups/"
            f"{workspace_id}/dataflows/{dataflow_id}?experience=power-bi"
        )

    def _get_datamart_url(self, workspace_id: str, datamart_id: str) -> str:
        """
        Method to build the datamart url
        """
        return (
            f"{clean_uri(self.service_connection.hostPort)}/groups/"
            f"{workspace_id}/datamarts/{datamart_id}?experience=power-bi"
        )

    def _get_chart_url(self, report_id: Optional[str], workspace_id: str, dashboard_id: str) -> str:  # noqa: UP045
        """
        Method to build the chart url
        """
        chart_url_postfix = f"reports/{report_id}" if report_id else f"dashboards/{dashboard_id}"
        return f"{clean_uri(self.service_connection.hostPort)}/groups/{workspace_id}/{chart_url_postfix}"

    def yield_dashboard(self, dashboard_details: Group) -> Iterable[Either[CreateDashboardRequest]]:
        """
        Method to Get Dashboard Entity, Dashboard Charts & Lineage
        """
        try:
            for dashboard in self.state.filtered_dashboards:
                dashboard_details = self.get_dashboard_details(dashboard)
                if isinstance(dashboard_details, PowerBIDashboard):
                    dashboard_chart_ids = self.state.pop_dashboard_chart_ids(dashboard_details.id)
                    dashboard_request = CreateDashboardRequest(
                        name=EntityName(dashboard_details.id),
                        sourceUrl=SourceUrl(
                            self._get_dashboard_url(
                                workspace_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                                dashboard_id=dashboard_details.id,
                            )
                        ),
                        project=self.get_project_name(dashboard_details),
                        displayName=dashboard_details.displayName,
                        dashboardType=DashboardType.Dashboard,
                        charts=[
                            FullyQualifiedEntityName(
                                fqn.build(
                                    self.metadata,
                                    entity_type=Chart,
                                    service_name=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                                    chart_name=chart,
                                )
                            )
                            for chart in dashboard_chart_ids
                        ],
                        service=FullyQualifiedEntityName(self.context.get().dashboard_service),  # pyright: ignore[reportAttributeAccessIssue]
                        owners=self.get_owner_ref(dashboard_details=dashboard_details),
                    )
                else:
                    description = Markdown(dashboard_details.description) if dashboard_details.description else None
                    # Report-visual charts only exist when the feature is on (see
                    # `yield_dashboard_chart`); with it off, `charts` stays unset here
                    # exactly as before the feature existed, not an explicit `[]`
                    # (which would serialize differently).
                    report_charts_kwarg: dict = {}
                    if self.report_column_usage_enabled:
                        report_chart_ids = self.state.pop_dashboard_chart_ids(dashboard_details.id)
                        if report_chart_ids:
                            report_charts_kwarg["charts"] = [
                                FullyQualifiedEntityName(chart_fqn)
                                for chart in report_chart_ids
                                if (
                                    chart_fqn := fqn.build(
                                        self.metadata,
                                        entity_type=Chart,
                                        service_name=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                                        chart_name=chart,
                                    )
                                )
                            ]
                    dashboard_request = CreateDashboardRequest(
                        name=EntityName(dashboard_details.id),
                        dashboardType=DashboardType.Report,
                        sourceUrl=SourceUrl(
                            self._get_report_url(
                                workspace_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                                dashboard_details=dashboard_details,
                            )
                        ),
                        project=self.get_project_name(dashboard_details),
                        displayName=dashboard_details.name,
                        description=description,
                        service=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                        owners=self.get_owner_ref(dashboard_details=dashboard_details),
                        **report_charts_kwarg,
                    )
                yield Either(right=dashboard_request)
                self.register_record(dashboard_request=dashboard_request)
                self._advance_group_progress(self._progress_group_name(), "Dashboard")
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name=dashboard_details.name,
                    error=f"Error creating dashboard [{dashboard_details}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_dashboard_chart(self, dashboard_details: Group) -> Iterable[Either[CreateChartRequest]]:
        """Get chart method
        Args:
            dashboard_details:
        Returns:
            Iterable[Chart]
        """
        for dashboard in self.state.filtered_dashboards:
            dashboard_details = self.get_dashboard_details(dashboard)
            if isinstance(dashboard_details, PowerBIDashboard):
                charts = dashboard_details.tiles
                for chart in charts or []:
                    try:
                        chart_title = chart.title
                        chart_display_name = chart_title if chart_title else chart.id
                        if filter_by_chart(self.source_config.chartFilterPattern, chart_display_name):
                            self.status.filter(chart_display_name, "Chart Pattern not Allowed")
                            continue
                        chart_request = CreateChartRequest(
                            name=EntityName(chart.id),
                            displayName=chart_display_name,
                            chartType=ChartType.Other.value,
                            sourceUrl=SourceUrl(
                                self._get_chart_url(
                                    report_id=chart.reportId,
                                    workspace_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                                    dashboard_id=dashboard_details.id,
                                )
                            ),
                            service=FullyQualifiedEntityName(self.context.get().dashboard_service),  # pyright: ignore[reportAttributeAccessIssue]
                        )
                        yield Either(right=chart_request)
                        self.state.add_dashboard_chart(dashboard_details.id, chart.id)
                        self.register_record_chart(chart_request=chart_request)
                        self._advance_group_progress(self._progress_group_name(), "Chart")
                    except Exception as exc:
                        yield Either(
                            left=StackTraceError(
                                name=chart.title,
                                error=f"Error creating chart [{chart.title}]: {exc}",
                                stackTrace=traceback.format_exc(),
                            )
                        )
            elif isinstance(dashboard_details, PowerBIReport) and self.report_column_usage_enabled:
                yield from self._yield_report_visual_charts(dashboard_details)

    @staticmethod
    def _visual_chart_name(report_id: str, visual_id: str) -> str:
        """Stable, service-unique Chart entity name for one report visual."""
        return f"{report_id}_{visual_id}"

    def _get_report_visual_url(self, workspace_id: str, report_id: str, page_id: Optional[str]) -> str:  # noqa: UP045
        """Deep link to the report page a visual lives on - same URL shape as `_get_report_url`."""
        page_suffix = f"/{page_id}" if page_id else ""
        return (
            f"{clean_uri(self.service_connection.hostPort)}/groups/"
            f"{workspace_id}/{DEFAULT_REPORTS_PREFIX}/{report_id}{page_suffix}?experience=power-bi"
        )

    def _get_report_definition(
        self, dashboard_details: PowerBIReport, workspace_id: str
    ) -> Optional[ReportDefinitionLike]:  # noqa: UP045
        """Fetch (Fabric, cached by lastUpdatedTimeUtc within this run) and parse a
        report's definition. Returns `None` - logging and metering why - on any
        failure: no Fabric client (feature off), the fetch itself failing, or the
        `report_definition` parser module not being available yet.
        """
        cached = self.state.get_report_definition(dashboard_details.id)
        if cached is not None:
            return cached  # pyright: ignore[reportReturnType]
        if not self.fabric_client:
            return None
        result = self.fabric_client.get_report_definition(workspace_id, dashboard_details.id)
        if result is None:
            self._metrics[self.METRIC_REPORT_DEFINITIONS_FAILED] += 1
            return None
        if result.from_cache:
            self._metrics[self.METRIC_REPORT_DEFINITIONS_CACHE_SKIPPED] += 1
        else:
            self._metrics[self.METRIC_REPORT_DEFINITIONS_FETCHED] += 1
        report_definition = self._parse_report_definition(result.parts)
        if report_definition is not None:
            self.state.cache_report_definition(dashboard_details.id, report_definition)
        return report_definition

    def _parse_report_definition(self, parts: Mapping[str, bytes]) -> Optional[Any]:  # noqa: UP045
        """Deferred import of `report_definition.parse_report_definition` (a sibling
        module built in parallel with this one - see the `*Like` Protocols above).
        Importing lazily, inside the call, means this connector still imports cleanly
        before that module lands; once it does, this starts working with no other
        change here.
        """
        try:
            from metadata.ingestion.source.dashboard.powerbi.report_definition import (
                parse_report_definition,
            )
        except ImportError:
            logger.debug("report_definition.parse_report_definition is not available yet")
            return None
        try:
            report_definition = parse_report_definition(parts)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Error parsing report definition: {exc}")
            logger.debug(traceback.format_exc())
            return None
        report_format = getattr(report_definition, "format", None)
        if report_format == "legacy":
            self._metrics[self.METRIC_REPORTS_FORMAT_LEGACY] += 1
        elif report_format == "pbir":
            self._metrics[self.METRIC_REPORTS_FORMAT_PBIR] += 1
        else:
            self._metrics[self.METRIC_REPORTS_FORMAT_UNKNOWN] += 1
        return report_definition

    def _yield_report_visual_charts(self, dashboard_details: PowerBIReport) -> Iterable[Either[CreateChartRequest]]:
        """One Chart per data visual in `dashboard_details`'s report definition.

        Non-data visuals (`is_data_visual=False`) get no Chart, only a metric.
        """
        workspace_id = self.context.get().workspace.id  # pyright: ignore[reportAttributeAccessIssue]
        report_definition = self._get_report_definition(dashboard_details, workspace_id)
        if report_definition is None:
            return
        for visual in report_definition.visuals or []:
            try:
                if not visual.is_data_visual:
                    self._metrics[self.METRIC_VISUALS_NON_DATA] += 1
                    continue
                self._metrics[self.METRIC_VISUALS_DATA] += 1
                chart_name = self._visual_chart_name(dashboard_details.id, visual.visual_id)
                display_name = visual.title or f"{visual.page_display_name or visual.page_id} / {visual.visual_type}"
                if filter_by_chart(self.source_config.chartFilterPattern, display_name):
                    self.status.filter(display_name, "Chart Pattern not Allowed")
                    continue
                chart_type = ChartType(POWERBI_VISUAL_TYPE_TO_CHART_TYPE.get(visual.visual_type, ChartType.Other.value))
                chart_request = CreateChartRequest(
                    name=EntityName(chart_name),
                    displayName=display_name,
                    chartType=chart_type,
                    sourceUrl=SourceUrl(
                        self._get_report_visual_url(
                            workspace_id=workspace_id,
                            report_id=dashboard_details.id,
                            page_id=visual.page_id,
                        )
                    ),
                    service=FullyQualifiedEntityName(self.context.get().dashboard_service),  # pyright: ignore[reportAttributeAccessIssue]
                )
                yield Either(right=chart_request)  # pyright: ignore[reportCallIssue]
                self.state.add_dashboard_chart(dashboard_details.id, chart_name)
                self.register_record_chart(chart_request=chart_request)
                self._metrics[self.METRIC_REPORT_VISUAL_CHARTS_CREATED] += 1
                self._advance_group_progress(self._progress_group_name(), "Chart")
            except Exception as exc:  # pylint: disable=broad-except
                self._metrics[self.METRIC_VISUALS_SKIPPED] += 1
                yield Either(  # pyright: ignore[reportCallIssue]
                    left=StackTraceError(
                        name=getattr(visual, "visual_id", "visual"),
                        error=f"Error creating chart for visual [{getattr(visual, 'visual_id', '?')}] "
                        f"on report [{dashboard_details.id}]: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def _get_child_measures(self, table: PowerBiTable) -> List[Column]:  # noqa: UP006
        """
        Extract the measures of the table
        """
        measures = []
        for measure in table.measures or []:
            if not measure.name:
                logger.debug(
                    "Skipping PowerBI measure with empty name on table [%s]",
                    table.name,
                )
                continue
            try:
                measure_type = DataType.MEASURE_VISIBLE
                if measure.isHidden:
                    measure_type = DataType.MEASURE_HIDDEN
                expression_text = f"Expression : {measure.expression}" if measure.expression else ""
                description_text = f"Description : {measure.description}" if measure.description else ""
                description_field_text = f"{expression_text}\n\n{description_text}"
                parsed_measure = PowerBiMeasureModel(
                    dataType=measure_type,
                    dataTypeDisplay=measure_type,
                    name=truncate_column_name(measure.name),
                    displayName=measure.name,
                    description=description_field_text,
                )
                measures.append(Column(**parsed_measure.model_dump()))
            except Exception as err:
                logger.debug(traceback.format_exc())
                logger.debug(f"Error processing datamodel nested measure: {err}")
        return measures

    def _get_child_columns(self, table: PowerBiTable) -> List[Column]:  # noqa: UP006
        """
        Extract the child columns from the fields
        """
        columns = []
        for column in table.columns or []:
            if not column.name:
                logger.debug(
                    "Skipping PowerBI column with empty name on table [%s]",
                    table.name,
                )
                continue
            try:
                parsed_column = {
                    "dataTypeDisplay": (column.dataType if column.dataType else DataType.UNKNOWN.value),
                    "dataType": ColumnTypeParser.get_column_type(column.dataType if column.dataType else None),
                    "name": truncate_column_name(column.name),
                    "displayName": column.name,
                    "description": column.description,
                }
                if column.dataType and column.dataType == DataType.ARRAY.value:
                    parsed_column["arrayDataType"] = DataType.UNKNOWN
                columns.append(Column(**parsed_column))
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(f"Error processing datamodel nested column: {exc}")
        return columns

    def _get_column_info(self, dataset: Dataset) -> Optional[List[Column]]:  # noqa: UP006, UP045
        """Build columns from dataset"""
        datasource_columns = []
        for table in dataset.tables or []:
            if not table.name:
                logger.debug(
                    "Skipping PowerBI table with empty name on dataset [id=%s]",
                    dataset.id,
                )
                continue
            try:
                table_display_name = None
                if self.service_connection.displayTableNameFromSource:
                    table_display_name = self.parse_table_name_from_source(table=table)
                    if table_display_name:
                        logger.debug(f"Parsed Table display name: {table_display_name} for table: {table.name}")
                if not table_display_name:
                    table_display_name = table.name
                parsed_table = {
                    "dataTypeDisplay": "PowerBI Table",
                    "dataType": DataType.TABLE,
                    "name": truncate_column_name(table.name),
                    "displayName": table_display_name,
                    "description": table.description,
                    "children": [],
                }
                child_columns = self._get_child_columns(table=table)
                child_measures = self._get_child_measures(table=table)
                if child_columns:
                    parsed_table["children"] = child_columns
                if child_measures:
                    parsed_table["children"].extend(child_measures)
                datasource_columns.append(Column(**parsed_table))
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(f"Error to yield datamodel column: {exc}")
        return datasource_columns

    def _get_dataflow_column_info(self, dataflow_export: DataflowExportResponse) -> Optional[List[Column]]:  # noqa: UP006, UP045
        """Build columns from dataflow export response entities"""
        datasource_columns = []
        for entity in dataflow_export.entities or []:
            if not entity.name:
                logger.debug("Skipping PowerBI dataflow column entity with empty name")
                continue
            try:
                parsed_table = {
                    "dataTypeDisplay": "PowerBI Table",
                    "dataType": DataType.TABLE,
                    "name": truncate_column_name(entity.name),
                    "displayName": entity.name,
                    "description": entity.description,
                    "children": [],
                }
                child_columns = []
                for attribute in entity.attributes or []:
                    if not attribute.name:
                        logger.debug(
                            "Skipping PowerBI dataflow attribute(column entity) with empty name on entity [%s]",
                            entity.name,
                        )
                        continue
                    try:
                        parsed_column = {
                            "dataTypeDisplay": (attribute.dataType if attribute.dataType else DataType.UNKNOWN.value),
                            "dataType": ColumnTypeParser.get_column_type(
                                attribute.dataType if attribute.dataType else None
                            ),
                            "name": truncate_column_name(attribute.name),
                            "displayName": attribute.name,
                            "description": attribute.description,
                        }
                        if attribute.dataType and attribute.dataType == DataType.ARRAY.value:
                            parsed_column["arrayDataType"] = DataType.UNKNOWN
                        child_columns.append(Column(**parsed_column))
                    except Exception as exc:
                        logger.debug(traceback.format_exc())
                        logger.warning(f"Error processing dataflow entity attribute: {exc}")
                if child_columns:
                    parsed_table["children"] = child_columns
                datasource_columns.append(Column(**parsed_table))
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(f"Error to yield dataflow entity column: {exc}")
        return datasource_columns

    def _get_datamodels_list(self) -> List[Union[Dataset, Dataflow, Datamart]]:  # noqa: UP006, UP007
        """
        Get All the Powerbi Datasets, Dataflows, and Datamarts
        """
        workspace = self.context.get().workspace  # pyright: ignore[reportAttributeAccessIssue]
        return workspace.datasets + workspace.dataflows + (workspace.datamarts or [])

    def _filtered_datamodels(self) -> list:
        """Filtered datamodels for the current workspace, memoised on first call."""
        cached = self.state.filtered_datamodels
        if cached is not None:
            return cached
        filtered: list = []
        for dataset in self._get_datamodels_list() or []:
            if not dataset.name:
                logger.debug(
                    "Skipping PowerBI data model with empty name [id=%s]",
                    dataset.id,
                )
                continue
            if filter_by_datamodel(self.source_config.dataModelFilterPattern, dataset.name):
                self.status.filter(dataset.name, "Data model filtered out.")
                continue
            filtered.append(dataset)
        self.state.set_filtered_datamodels(filtered)
        return filtered

    def _get_semantic_model_definition(self, dataset: Dataset, workspace_id: str) -> Optional[object]:  # noqa: UP045
        """Fetch (Fabric TMDL, cached by lastUpdatedTimeUtc within this run) and parse
        a semantic model's definition. `None` on any failure - no Fabric client, the
        fetch failing, or the `tmdl` parser module not being available yet.
        """
        cached = self.state.get_semantic_model_definition(dataset.id)
        if cached is not None:
            return cached
        if not self.fabric_client:
            return None
        result = self.fabric_client.get_semantic_model_definition(workspace_id, dataset.id)
        if result is None:
            self._metrics[self.METRIC_MODEL_DEFINITIONS_FAILED] += 1
            return None
        if result.from_cache:
            self._metrics[self.METRIC_MODEL_DEFINITIONS_CACHE_SKIPPED] += 1
        else:
            self._metrics[self.METRIC_MODEL_DEFINITIONS_FETCHED] += 1
        model_definition = self._parse_semantic_model_definition(result.parts)
        if model_definition is not None:
            self.state.cache_semantic_model_definition(dataset.id, model_definition)
        return model_definition

    def _parse_semantic_model_definition(self, parts: Mapping[str, bytes]) -> Optional[object]:  # noqa: UP045
        """Deferred import of `tmdl.parse_tmdl` - see `_parse_report_definition` for why."""
        try:
            from metadata.ingestion.source.dashboard.powerbi.tmdl import parse_tmdl
        except ImportError:
            logger.debug("tmdl.parse_tmdl is not available yet")
            return None
        try:
            return parse_tmdl(parts)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Error parsing semantic model TMDL definition: {exc}")
            logger.debug(traceback.format_exc())
            return None

    def _tmdl_table_to_powerbi_table(self, table: object) -> Optional[PowerBiTable]:  # noqa: UP045
        """Adapt one TMDL table (from `tmdl.parse_tmdl`) into the same `PowerBiTable`
        shape the non-admin push-dataset tables call used to produce, so
        `_get_column_info`/`_get_child_columns`/`_get_child_measures` run unchanged
        against TMDL-sourced tables. Auto-date tables are the caller's job to skip.
        """
        name = getattr(table, "name", None)
        if not name:
            return None
        try:
            columns = [
                PowerBiColumns(
                    name=getattr(column, "name", None),
                    dataType=getattr(column, "data_type", None),
                    description=getattr(column, "description", None),
                )
                for column in getattr(table, "columns", None) or []
                if getattr(column, "name", None)
            ]
            measures = [
                PowerBiMeasures(
                    name=getattr(measure, "name", None),
                    expression=getattr(measure, "expression", None),
                    description=getattr(measure, "description", None),
                    isHidden=getattr(measure, "is_hidden", False),
                )
                for measure in getattr(table, "measures", None) or []
                if getattr(measure, "name", None)
            ]
            # Set directly rather than via `partitions=` - `PowerBiTable`'s
            # `extract_source_from_partitions` validator expects `partitions` as raw
            # dicts (it's a `mode="before"` validator meant for the push-dataset
            # tables API's own JSON body) and does `partitions[0].get("source")`,
            # which raises on an already-built `PowerBIPartition` object. Setting
            # `source` ourselves is simpler and skips that branch entirely (the
            # validator only derives `source` from `partitions` when `source` is
            # absent). Keeps the existing M-based lineage parsing
            # (`_parse_table_info_from_source_exp` et al.) working unchanged against
            # TMDL-sourced tables, same as for the push-dataset-tables response.
            source_expression = next(
                (
                    partition_source
                    for partition in getattr(table, "partitions", None) or []
                    if (partition_source := getattr(partition, "source", None))
                ),
                None,
            )
            return PowerBiTable(
                name=name,
                columns=columns,
                measures=measures,
                description=getattr(table, "description", None),
                source=[PowerBITableSource(expression=source_expression)] if source_expression else None,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Error adapting TMDL table [{name}] to PowerBiTable: {exc}")
            logger.debug(traceback.format_exc())
            return None

    def _replace_dataset_tables_with_tmdl(self, dataset: Dataset) -> None:
        """Populate `dataset.tables` from the semantic model's TMDL definition,
        in place, skipping auto-date tables. A no-op (dataset keeps whatever
        `tables` it already had) if the definition can't be fetched or parsed.
        """
        workspace_id = self.context.get().workspace.id  # pyright: ignore[reportAttributeAccessIssue]
        model_definition = self._get_semantic_model_definition(dataset, workspace_id)
        if model_definition is None:
            return
        tables: List[PowerBiTable] = []  # noqa: UP006
        for table in getattr(model_definition, "tables", None) or []:
            if getattr(table, "is_auto_date", False):
                self._metrics[self.METRIC_AUTO_DATE_TABLES_SKIPPED] += 1
                continue
            powerbi_table = self._tmdl_table_to_powerbi_table(table)
            if powerbi_table is not None:
                tables.append(powerbi_table)
                self._metrics[self.METRIC_MODEL_COLUMNS_INGESTED] += len(powerbi_table.columns or [])
        if tables:
            dataset.tables = tables

    def yield_datamodel(self, dashboard_details: Group) -> Iterable[Either[CreateDashboardDataModelRequest]]:
        """
        Get All the Powerbi Datasets
        """
        if not self.source_config.includeDataModels:
            return
        try:
            datasets = self._filtered_datamodels()
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name="datamodels",
                    error=f"Error fetching PowerBI data models: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )
            return
        for dataset in datasets:
            try:
                if isinstance(dataset, Dataset):
                    data_model_type = DataModelType.PowerBIDataModel.value
                    if self.report_column_usage_enabled:
                        # Replaces `dataset.tables` with TMDL-derived tables before
                        # `_get_column_info` reads them - the non-admin push-dataset
                        # tables call this otherwise relies on always returns none.
                        self._replace_dataset_tables_with_tmdl(dataset)
                    datamodel_columns = self._get_column_info(dataset)
                    source_url = self._get_dataset_url(
                        workspace_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                        dataset_id=dataset.id,
                    )
                elif isinstance(dataset, Dataflow):
                    data_model_type = DataModelType.PowerBIDataFlow.value
                    datamodel_columns = []
                    source_url = self._get_dataflow_url(
                        workspace_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                        dataflow_id=dataset.id,
                    )
                    # dataflow export api for detailed metadata
                    # admin api: https://api.powerbi.com/v1.0/myorg/admin/dataflows/DATAFLOW_ID/export
                    # non-admin api: https://api.powerbi.com/v1.0/myorg/groups/GROUP_ID/dataflows/DATAFLOW_ID
                    dataflow_export = self.client.api_client.fetch_dataflow_export(
                        dataflow_id=dataset.id,
                        group_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                    )
                    if dataflow_export:
                        self._metrics[self.METRIC_DATAFLOWS_FETCHED] += 1
                        self.state.cache_dataflow_export(dataset.id, dataflow_export)
                        datamodel_columns = self._get_dataflow_column_info(dataflow_export)
                elif isinstance(dataset, Datamart):
                    data_model_type = DataModelType.PowerBIDatamart.value
                    datamodel_columns = []
                    source_url = self._get_datamart_url(
                        workspace_id=self.context.get().workspace.id,  # pyright: ignore[reportAttributeAccessIssue]
                        datamart_id=dataset.id,
                    )
                else:
                    logger.warning(f"Unknown dataset type: {type(dataset)}, name: {dataset.name}")
                    continue
                data_model_request = CreateDashboardDataModelRequest(  # pyright: ignore[reportCallIssue]
                    name=EntityName(dataset.id),
                    displayName=dataset.name,
                    description=(Markdown(dataset.description) if dataset.description else None),
                    service=FullyQualifiedEntityName(self.context.get().dashboard_service),  # pyright: ignore[reportAttributeAccessIssue]
                    dataModelType=data_model_type,
                    serviceType=DashboardServiceType.PowerBI.value,
                    columns=datamodel_columns,
                    project=self.get_project_name(dashboard_details=dataset),
                    owners=self.get_owner_ref(dashboard_details=dataset),
                    sourceUrl=SourceUrl(source_url),
                )
                yield Either(right=data_model_request)  # pyright: ignore[reportCallIssue]
                self.register_record_datamodel(datamodel_request=data_model_request)
                self._advance_group_progress(self._progress_group_name(), "DashboardDataModel")
            except Exception as exc:
                dataset_name = dataset.name or dataset.id or ""
                yield Either(  # pyright: ignore[reportCallIssue]
                    left=StackTraceError(
                        name=dataset_name,
                        error=f"Error yielding Data Model [{dataset_name}]: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def create_report_dashboard_lineage(
        self,
        dashboard_details: PowerBIDashboard,
    ) -> Iterable[Either[AddLineageRequest]]:
        """Create lineage between tile-pinned reports and the dashboard.

        Reports referenced by tiles may live in a different workspace; we
        resolve them via the cross-workspace registry on `WorkspaceState`
        rather than walking a global `workspace_data` list.
        """
        try:
            dashboard_fqn = fqn.build(
                self.metadata,
                entity_type=Dashboard,
                service_name=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                dashboard_name=dashboard_details.id,
            )
            if not dashboard_fqn:
                logger.warning(
                    "Cannot build Dashboard FQN for tile-pinned report lineage: dashboard=%s",
                    dashboard_details.id,
                )
                return
            dashboard_entity = self.metadata.get_by_name(entity=Dashboard, fqn=dashboard_fqn)
            if not dashboard_entity:
                logger.debug(
                    "Dashboard entity not found for tile-pinned report lineage: dashboard=%s",
                    dashboard_details.id,
                )
                return
            tile_report_ids = [
                chart.reportId for chart in dashboard_details.tiles or [] if self.state.is_known_report(chart.reportId)
            ]
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name="Report and Dashboard Lineage",
                    error=f"Error resolving dashboard for tile-pinned report lineage [{dashboard_details.id}]: {exc}",
                    stackTrace=traceback.format_exc(),
                ),
                right=None,
            )
            return
        yield from self._emit_om_target_lineage(
            to_entity=dashboard_entity,
            target_ids=tile_report_ids,
            target=DASHBOARD_TARGET,
            error_name="Report and Dashboard Lineage",
        )

    def _get_dataset_ids_from_report_datasources(self, report_id: str) -> List[str]:  # noqa: UP006
        """
        Fetch report datasources and extract dataset IDs from connectionDetails.database.
        The database field follows the pattern: sobe_wowvirtualserver-{DATASET_ID}
        """
        dataset_ids = []
        workspace_id = self.context.get().workspace.id  # pyright: ignore[reportAttributeAccessIssue]
        datasources = self.client.api_client.fetch_report_datasources(group_id=workspace_id, report_id=report_id)
        if not datasources:
            return dataset_ids
        for datasource in datasources:
            if datasource.connectionDetails and datasource.connectionDetails.database:
                match = re.match(
                    r"sobe_wowvirtualserver-(.+)",
                    datasource.connectionDetails.database,
                )
                if match:
                    dataset_ids.append(match.group(1))
        if dataset_ids:
            logger.debug(f"Extracted dataset IDs from report datasources API call for report_id={report_id}")
        return dataset_ids

    def _resolve_report_dataset_ids(self, dashboard_details: PowerBIReport) -> List[str]:  # noqa: UP006
        """Dataset ids `dashboard_details` links to: its own `datasetId` field, or -
        when that's absent - whatever `_get_dataset_ids_from_report_datasources`
        extracts from its datasources. Shared by `create_datamodel_report_lineage`
        and the column-usage pass so both agree on which model(s) a report uses.
        """
        if dashboard_details.datasetId:
            return [dashboard_details.datasetId]
        return self._get_dataset_ids_from_report_datasources(report_id=dashboard_details.id)

    def create_datamodel_report_lineage(
        self,
        db_service_prefix: Optional[str],  # noqa: UP045
        dashboard_details: PowerBIReport,
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        create the lineage between datamodel and report

        Pre-existing signature said `Either[CreateDashboardRequest]`, which this
        method never actually yields (it's lineage, not a dashboard request) - fixed
        while wiring column lineage through it, which is what made the mismatch
        start failing basedpyright.
        """
        try:
            logger.debug(f"Processing to create datamodel and report lineage for report: {dashboard_details.id}")
            report_fqn = fqn.build(
                self.metadata,
                entity_type=Dashboard,
                service_name=self.config.serviceName,
                dashboard_name=dashboard_details.id,
            )
            report_entity = self.metadata.get_by_name(
                entity=Dashboard,
                fqn=report_fqn,
            )
            if not report_entity:
                logger.debug(
                    f"Report entity not found to create lineage between datamodel and report for report: {dashboard_details.id}"
                )
                return
            dataset_ids = self._resolve_report_dataset_ids(dashboard_details)

            if dataset_ids:
                for dataset_id in dataset_ids:
                    datamodel_fqn = fqn.build(
                        self.metadata,
                        entity_type=DashboardDataModel,
                        service_name=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                        data_model_name=dataset_id,
                    )
                    datamodel_entity = self.metadata.get_by_name(
                        entity=DashboardDataModel,
                        fqn=datamodel_fqn,
                    )
                    if not datamodel_entity:
                        logger.debug(
                            f"Data model entity not found for dataset_id={str(dataset_id)} while creating lineage with report={str(dashboard_details.id)}"  # noqa: RUF010
                        )
                    if datamodel_entity and report_entity:
                        logger.debug(
                            f"Creating lineage between datamodel={str(dataset_id)} and report={str(dashboard_details.id)}"  # noqa: RUF010
                        )
                        column_lineage = None
                        if self.report_column_usage_enabled:
                            usage = self.state.get_column_usage(dataset_id)
                            if usage is not None:
                                column_lineage = self._create_datamodel_report_column_lineage(
                                    datamodel_entity=datamodel_entity,
                                    report_id=dashboard_details.id,
                                    usage=usage,  # pyright: ignore[reportArgumentType]
                                )
                        lineage_request = self._get_add_lineage_request(
                            to_entity=report_entity,
                            from_entity=datamodel_entity,
                            column_lineage=column_lineage,  # pyright: ignore[reportArgumentType]
                        )
                        if column_lineage:
                            self._track_column_lineage_edge(
                                from_entity=datamodel_entity,
                                to_entity=report_entity,
                                edge_kind="datamodel_report",
                                emitted_count=len(column_lineage),
                            )
                        if lineage_request is not None:
                            yield lineage_request
            else:
                logger.debug(
                    f"Skipping datamodel and report lineage for report: {dashboard_details.id} as datasetId is not found on api response and also could not be extracted from report datasources API call"
                )

        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name="Datamodel and Report Lineage",
                    error=(f"Error to yield datamodel and report lineage details: {exc}"),
                    stackTrace=traceback.format_exc(),
                )
            )

    def _create_datamodel_report_column_lineage(
        self,
        datamodel_entity: DashboardDataModel,
        report_id: str,
        usage: ModelColumnUsageLike,
    ) -> List[ColumnLineage]:  # noqa: UP006
        """Model column -> chart lineage for one report, from `usage.per_visual`.

        A column reached through two different measures produces two `ColumnLineage`
        entries (not merged into one) - `LineageRepository.validateLineageDetails`
        filters `columnsLineage` per-entry and never dedupes/merges entries that share
        a `toColumn`, so repeated `toColumn`s round-trip intact (see
        `openmetadata-service/.../jdbi3/LineageRepository.java:718-751`). Each entry's
        `function` is `measure:<name>` for a column reached via a measure, unset for a
        direct column reference - `via_measure` on `ColumnUse` already carries exactly
        the name to use (the first, visual-facing measure on the path).
        """
        column_lineage: List[ColumnLineage] = []  # noqa: UP006
        service_name = self.context.get().dashboard_service  # pyright: ignore[reportAttributeAccessIssue]
        for (visual_report_id, visual_id), column_uses in (usage.per_visual or {}).items():
            if visual_report_id != report_id:
                continue
            chart_name = self._visual_chart_name(report_id, visual_id)
            chart_fqn = fqn.build(
                self.metadata,
                entity_type=Chart,
                service_name=service_name,
                chart_name=chart_name,
            )
            if not chart_fqn:
                continue
            chart_entity = self.metadata.get_by_name(entity=Chart, fqn=chart_fqn)
            if not chart_entity:
                continue
            to_column = chart_entity.fullyQualifiedName.root
            for use in column_uses or []:
                from_column_fqn = self._get_downstream_data_model_column_fqn(
                    data_model_entity=datamodel_entity,
                    table_name=use.table,
                    column=use.column,
                )
                if not from_column_fqn:
                    self._metrics[self.METRIC_COLUMN_REFS_DANGLING] += 1
                    continue
                self._metrics[self.METRIC_COLUMN_REFS_RESOLVED] += 1
                via_measure = getattr(use, "via_measure", None)
                entry_kwargs: dict = {"fromColumns": [from_column_fqn], "toColumn": to_column}
                if via_measure:
                    entry_kwargs["function"] = f"measure:{via_measure}"
                    self._metrics[self.METRIC_MEASURES_RESOLVED_TRANSITIVELY] += 1
                column_lineage.append(ColumnLineage(**entry_kwargs))
                self._metrics[self.METRIC_COLUMN_LINEAGE_EMITTED] += 1
        return column_lineage

    def _track_column_lineage_edge(
        self,
        from_entity: Union[DashboardDataModel, Dashboard],  # noqa: UP007
        to_entity: Union[DashboardDataModel, Dashboard],  # noqa: UP007
        edge_kind: str,
        emitted_count: int,
    ) -> None:
        """Remember one column-lineage edge so `_verify_column_lineage_edges` can read
        it back, once this workspace's lineage writes have flushed, and compare what
        the server actually stored against what was emitted.
        """
        self._pending_column_lineage_edges.append(
            (str(from_entity.id.root), str(to_entity.id.root), edge_kind, emitted_count)
        )

    def _verify_column_lineage_edges(self) -> None:
        """Read back every tracked column-lineage edge and compare its stored
        `columnsLineage` length against what was emitted, counting the difference as
        server-side drops (a `fromColumn`/`toColumn` `validateLineageDetails` filtered
        out - see `_create_datamodel_report_column_lineage`'s docstring). Clears the
        pending list either way, so a verification pass never double-counts.
        """
        pending = self._pending_column_lineage_edges
        self._pending_column_lineage_edges = []
        for from_id, to_id, _edge_kind, emitted_count in pending:
            try:
                edge = self.metadata.get_lineage_edge(from_id, to_id)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning(f"Error reading back column lineage edge {from_id}->{to_id}: {exc}")
                logger.debug(traceback.format_exc())
                continue
            stored_count = len((edge or {}).get("lineageDetails", {}).get("columnsLineage") or [])
            verified = min(stored_count, emitted_count)
            self._metrics[self.METRIC_COLUMN_LINEAGE_VERIFIED] += verified
            if stored_count < emitted_count:
                self._metrics[self.METRIC_COLUMN_LINEAGE_DROPPED_BY_SERVER] += emitted_count - stored_count

    @staticmethod
    def _get_data_model_column_fqn(data_model_entity: DashboardDataModel, column: str) -> Optional[str]:  # noqa: UP045
        """
        Get fqn of column if exist in data model entity or its child columns
        """
        try:
            if not data_model_entity:
                return None
            for tbl_column in data_model_entity.columns:
                for child_column in tbl_column.children or []:
                    if column.lower() == child_column.name.root.lower():
                        return child_column.fullyQualifiedName.root
            return None  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error to get data_model_column_fqn {exc}")
            logger.debug(traceback.format_exc())

    def parse_table_name_from_source(self, table: PowerBiTable) -> Optional[str]:  # noqa: UP045
        """
        Parse the snowflake table name
        """
        try:
            if not isinstance(table.source, list):
                return None
            source_expression = table.source[0].expression
            if not source_expression:
                logger.debug(f"No source expression found for table: {table.name}")
                return None

            if "Snowflake.Databases" in source_expression:
                # snowflake expression
                table_match = re.search(r'\[Name=(?:"([^"]+)"|([^,]+)),Kind="Table"\]', source_expression)
                view_match = re.search(r'\[Name=(?:"([^"]+)"|([^,]+)),Kind="View"\]', source_expression)
                table = table_match.group(1) if table_match else None
                view = view_match.group(1) if view_match else None
                return table if table else view

            # other general expressions
            table_match = re.findall(r'\[Name="([^"]+)"\]', source_expression)
            table = None
            if isinstance(table_match, list):
                table = table_match[1] if len(table_match) > 1 else None
            return table  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error to parse display table name: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_expression_regex_exp(self, match: re.Match, datamodel_entity: DashboardDataModel) -> Optional[str]:  # noqa: UP045
        """parse snowflake regex expression"""
        try:
            if not match:
                return None
            elif match.group(1):  # noqa: RET505
                return match.group(1)
            elif match.group(2):
                dataset = self.state.find_dataset(datamodel_entity.name.root)
                if dataset and dataset.expressions:
                    # find keyword from dataset expressions
                    for dexpression in dataset.expressions:
                        if not dexpression.expression:
                            logger.debug(
                                f"No expression value found inside dataset"
                                f"({dataset.name}) expressions' name={dexpression.name}"
                            )
                            continue
                        if dexpression.name == match.group(2):
                            pattern = r'^"([^"]+)"\s+meta'
                            kw_match = re.search(pattern, dexpression.expression)
                            if kw_match:
                                return kw_match.group(1)
        except Exception as exc:
            logger.debug(f"Error to parse snowflake regex expression: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_redshift_source(self, source_expression: str) -> Optional[List[dict]]:  # noqa: UP006, UP045
        try:
            db_match = re.search(r'AmazonRedshift\.Database\("[^"]+","([^"]+)"\)', source_expression)
            if not db_match:
                # not valid redshift source
                return None
            schema_table_match = re.findall(r'\[Name="([^"]+)"\]', source_expression)

            database = db_match.group(1) if db_match else None
            schema = table = None
            if isinstance(schema_table_match, list):
                schema = schema_table_match[0] if len(schema_table_match) > 0 else None
                table = schema_table_match[1] if len(schema_table_match) > 1 else None

            if table:  # atlease table should be fetched
                return [{"database": database, "schema": schema, "table": table}]
            return None  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error to parse redshift table source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_bigquery_query_source(self, source_expression: str) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """
        Parse BigQuery Value.NativeQuery source expressions containing inline SQL.

        Example:
        Value.NativeQuery(GoogleBigQuery.Database([BillingProject="project"]){[Name="project"]}[Data],
            "SELECT ... FROM `dataset.table` ...", null, [EnableFolding=true])
        """
        try:
            # Strip M language block comments (/* ... */) and line comments (//)
            cleaned_expression = re.sub(r"/\*.*?\*/", "", source_expression, flags=re.DOTALL)
            cleaned_expression = re.sub(SQL_LINE_COMMENT_PATTERN, "", cleaned_expression)

            # Extract the project from BillingProject parameter
            billing_match = re.search(r'BillingProject="([^"]+)"', cleaned_expression)
            project = billing_match.group(1) if billing_match else None

            # Extract the SQL query string (second argument to Value.NativeQuery)
            # Use a pattern that handles doubled quotes ("") inside M expression strings
            sql_match = re.search(
                r'\[Data\],\s*"((?:[^"]|"")+)"(?:,\s*null|\s*\))',
                cleaned_expression,
                re.DOTALL,
            )
            if not sql_match:
                logger.debug("SQL query not found in BigQuery NativeQuery expression")
                return None

            sql_query = sql_match.group(1).replace('""', '"')
            sql_query = sql_query.replace("#(lf)", "\n")
            sql_query = sql_query.replace("#(tab)", "\t")
            sql_query = re.sub(r"--[^\n]*", "", sql_query)
            sql_query = re.sub(SQL_LINE_COMMENT_PATTERN, "", sql_query)
            sql_query = re.sub(r"\s+", " ", sql_query).strip()

            logger.debug(f"Extracted BigQuery SQL query: {sql_query[:200]}")

            try:
                parser = LineageParser(
                    sql_query,
                    dialect=Dialect.BIGQUERY,
                    timeout_seconds=30,
                    parser_type=self.get_query_parser_type(),
                )
            except Exception as parser_exc:
                logger.debug(f"LineageParser failed for BigQuery query: {parser_exc}")
                return None

            if not parser.source_tables:
                logger.debug("No tables found in BigQuery query through parser")
                return None

            lineage_tables_list = []
            for source_table in parser.source_tables:
                database = project
                schema = None
                if hasattr(source_table, "schema") and source_table.schema:
                    schema_str = (
                        source_table.schema.raw_name
                        if hasattr(source_table.schema, "raw_name")
                        else str(source_table.schema)
                    )
                    if "." in schema_str:
                        parts = schema_str.split(".")
                        database = parts[0]
                        schema = parts[1] if len(parts) > 1 else None
                    else:
                        schema = schema_str

                table_name = source_table.raw_name
                if table_name:
                    logger.debug(f"BigQuery NativeQuery table found: {database}.{schema}.{table_name}")
                    lineage_tables_list.append(
                        {
                            "database": database,
                            "schema": schema,
                            "table": table_name,
                        }
                    )
            return lineage_tables_list or None  # noqa: TRY300

        except Exception as exc:
            logger.debug(f"Error parsing BigQuery query source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_bigquery_source(
        self,
        source_expression: str,
        datamodel_entity: DashboardDataModel,
        table: PowerBiTable,
    ) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """
        Parse BigQuery source from Power Query M expressions.
        Handles direct BigQuery connections, Value.NativeQuery with inline SQL,
        and references to dataset expressions.

        Examples:
        1. Direct: GoogleBigQuery.Database()[Name="project"][Data][Name="dataset",Kind="Schema"][Data][Name="table",Kind="Table"][Data]
        2. NativeQuery: Value.NativeQuery(GoogleBigQuery.Database([BillingProject="project"]){...}[Data], "SELECT ... FROM `dataset.table`", ...)
        3. Via expression reference: Source = S_PJ_CODE (where S_PJ_CODE is a dataset expression)
        """
        try:
            # Check if source expression references a dataset expression
            # Pattern: Source = <expression_name>
            source_ref_match = re.search(
                r'Source\s*=\s*([A-Za-z0-9_#"&\s]+?)\s*,',
                source_expression,
                re.MULTILINE,
            )

            if source_ref_match:
                ref_name = source_ref_match.group(1).strip().strip('"').strip("#").strip('"')
                logger.debug(f"Table source references expression: {ref_name}, resolving...")

                # Fetch the dataset to get its expressions
                dataset = self.state.find_dataset(datamodel_entity.name.root)
                if dataset and dataset.expressions:
                    for dexpression in dataset.expressions:
                        if dexpression.name == ref_name and dexpression.expression:
                            logger.debug(f"Found referenced expression '{ref_name}', checking for BigQuery")
                            # Recursively parse the referenced expression
                            return self._parse_bigquery_source(dexpression.expression, datamodel_entity, table)

            # Check if this is a direct BigQuery connection
            if "GoogleBigQuery.Database" not in source_expression:
                logger.debug(
                    "GoogleBigQuery.Database not found in source expression "
                    f"for datamodel: {datamodel_entity.name.root}, table: {table.name}"
                )
                return None

            # Handle Value.NativeQuery with inline SQL
            if BIGQUERY_QUERY_EXPRESSION_KW in source_expression:
                logger.debug(
                    "Parsing BigQuery NativeQuery source expression "
                    f"through query parser: {source_expression}\nfor "
                    f"datamodel: {datamodel_entity.name.root}, table: {table.name}"
                )
                return self._parse_bigquery_query_source(source_expression)

            logger.debug(f"Found GoogleBigQuery.Database in expression")  # noqa: F541
            # Extract project, dataset (schema), and table from BigQuery M expression
            # Pattern: [Name="project"][Data][Name="dataset",Kind="Schema"][Data][Name="table",Kind="Table"]

            # Extract all Name= patterns
            name_matches = re.findall(r'\[Name="([^"]+)"(?:,Kind="([^"]+)")?\]', source_expression)

            if not name_matches:
                logger.debug(
                    "No Name patterns found in BigQuery expression for "
                    f"datamodel: {datamodel_entity.name.root}, table: {table.name}"
                )
                return None

            # BigQuery structure: project -> dataset (Schema) -> table (Table/View)
            project = None
            dataset = None
            table_name = None

            for name, kind in name_matches:
                if kind == "Schema":
                    dataset = name
                elif kind == "Table" or kind == "View":  # noqa: PLR1714
                    table_name = name
                elif not kind and not project:
                    # First Name without Kind is likely the project
                    project = name

            logger.debug(f"Extracted BigQuery info: project={project}, dataset={dataset}, table={table_name}")
            if not table_name:
                logger.debug(
                    "Table name not found in Parsing BigQuery source expression for "
                    f"datamodel: {datamodel_entity.name.root}, powerbi table ({table.name}): {source_expression}"
                )
            if table_name:
                return [{"database": project, "schema": dataset, "table": table_name}]

            return None  # noqa: TRY300

        except Exception as exc:
            logger.debug(f"Error to parse BigQuery table source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_snowflake_query_source(self, source_expression: str) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """
        Parse snowflake query source
        source expressions like `Value.NativeQuery(Snowflake.Databases())`
        """
        try:
            logger.debug(f"parsing source expression through query parser: {source_expression[:100]}")

            # Look for SQL query after [Data],
            # The pattern needs to handle the concatenated strings with & operators
            m = re.search(
                r'\[Data\],\s*"(.+?)"(?:,\s*null|\s*\))',
                source_expression,
                re.IGNORECASE | re.DOTALL,
            )

            if not m:
                logger.debug("sql query not found in source expression")
                return None

            # Extract and clean the SQL query
            sql_query = m.group(1).replace('""', '"')

            # Handle PowerBI parameter concatenation (e.g., "& Database &")
            # For now, replace parameter references with wildcards for parsing
            sql_query_cleaned = re.sub(r'"?\s*&\s*\w+\s*&\s*"?\.?', "%.", sql_query)

            logger.debug(f"Extracted SQL query: {sql_query}")
            logger.debug(f"Cleaned SQL query: {sql_query_cleaned}")

            if not sql_query_cleaned:
                logger.debug("Empty SQL query after extraction")
                return None

            # Clean the query for parser
            # 1. Replace % with a placeholder database name
            parser_query = sql_query_cleaned.replace("%", "PLACEHOLDER_DB")

            # 2. Remove PowerBI line feed markers #(lf) and clean up the query
            parser_query = parser_query.replace("#(lf)", "\n")

            # 3. Remove SQL comments that might cause issues (// style comments)
            parser_query = re.sub(SQL_LINE_COMMENT_PATTERN, "", parser_query)

            # 4. Clean up excessive whitespace
            parser_query = re.sub(r"\s+", " ", parser_query).strip()

            logger.debug(f"Attempting LineageParser with cleaned query: {parser_query[:200]}")

            try:
                parser = LineageParser(
                    parser_query,
                    dialect=Dialect.SNOWFLAKE,
                    timeout_seconds=30,
                    parser_type=self.get_query_parser_type(),
                )
                query_hash = parser.query_hash
            except Exception as parser_exc:
                logger.debug(f"LineageParser failed with error: {parser_exc}")
                logger.debug(f"Failed query was: {parser_query[:200]}...")
                return None

            if parser.source_tables:
                logger.debug(f"[{query_hash}] LineageParser found {len(parser.source_tables)} source table(s)")
                for table in parser.source_tables:
                    schema_name = table.schema if hasattr(table, "schema") else "N/A"
                    logger.debug(f"[{query_hash}] source table: {table.raw_name}, schema: {schema_name}")
                lineage_tables_list = []
                for source_table in parser.source_tables:
                    # source_table = parser.source_tables[0]

                    # Extract database from schema's parent if it exists
                    database = None
                    schema = None

                    if hasattr(source_table, "schema") and source_table.schema:
                        # Log what we have in the schema object
                        logger.debug(f"Schema object type: {type(source_table.schema)}, value: {source_table.schema}")

                        # Get schema as string first
                        schema_str = (
                            source_table.schema.raw_name
                            if hasattr(source_table.schema, "raw_name")
                            else str(source_table.schema)
                        )

                        # If schema contains dots, it might be database.schema format
                        if "." in schema_str:
                            parts = schema_str.split(".")
                            if len(parts) == 2:
                                # Format: database.schema
                                # Check for placeholder (case insensitive)
                                database = parts[0] if parts[0].upper() != "PLACEHOLDER_DB" else None
                                schema = parts[1]
                            else:
                                # Just use as is
                                schema = schema_str
                        else:
                            schema = schema_str
                            # Check if schema has a parent (database)
                            if hasattr(source_table.schema, "parent") and source_table.schema.parent:
                                database = (
                                    source_table.schema.parent.raw_name
                                    if hasattr(source_table.schema.parent, "raw_name")
                                    else str(source_table.schema.parent)
                                )

                    # Filter out placeholder values (case insensitive)
                    if database and database.upper() == "PLACEHOLDER_DB":
                        database = None

                    table = source_table.raw_name

                    if table:
                        logger.debug(f"tables found = {database}.{schema}.{table}")
                        lineage_tables_list.append(
                            {
                                "database": database,
                                "schema": schema,
                                "table": table,
                            }
                        )
                return lineage_tables_list
            logger.debug("tables in query not found through parser")
            return None  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error parsing snowflake query source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_catalog_table_definition(
        self, source_expression: str, datamodel_entity: DashboardDataModel
    ) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """parse catalog table definition"""
        db_match = re.search(r'\[Name=(?:"([^"]+)"|([^,]+)),Kind="Database"\]', source_expression)
        schema_match = re.search(r'\[Name=(?:"([^"]+)"|([^,]+)),Kind="Schema"\]', source_expression)
        table_match = re.search(r'\[Name=(?:"([^"]+)"|([^,]+)),Kind="Table"\]', source_expression)
        view_match = re.search(r'\[Name=(?:"([^"]+)"|([^,]+)),Kind="View"\]', source_expression)
        try:
            database = self._parse_expression_regex_exp(db_match, datamodel_entity)
            schema = self._parse_expression_regex_exp(schema_match, datamodel_entity)
            table = self._parse_expression_regex_exp(table_match, datamodel_entity)
            view = self._parse_expression_regex_exp(view_match, datamodel_entity)
            if table or view:  # at least table or view should be present
                return [
                    {
                        "database": database,
                        "schema": schema,
                        "table": table if table else view,
                    }
                ]
        except Exception as exc:
            logger.debug(f"Error to parse databricks table source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_databricks_source(
        self, source_expression: str, datamodel_entity: DashboardDataModel
    ) -> Optional[List[dict]]:  # noqa: UP006, UP045
        if not any(
            source_type in source_expression for source_type in ("Databricks.Catalogs", "DatabricksMultiCloud.Catalogs")
        ):
            return None
        dataset = self.state.find_dataset(datamodel_entity.name.root)
        if dataset and dataset.expressions:
            try:
                if DATABRICKS_QUERY_EXPRESSION_KW in source_expression:
                    return parse_databricks_native_query_source(
                        source_expression,
                        dataset,
                        parser_type=self.get_query_parser_type(),
                    )
                else:  # noqa: RET505
                    return self._parse_catalog_table_definition(source_expression, datamodel_entity)
            except Exception as exc:
                logger.debug(f"Error to parse databricks table source: {exc}")
                logger.debug(traceback.format_exc())
        return None

    def _parse_snowflake_source(
        self, source_expression: str, datamodel_entity: DashboardDataModel
    ) -> Optional[List[dict]]:  # noqa: UP006, UP045
        try:
            if "Snowflake.Databases" not in source_expression:
                # Not a snowflake valid expression
                return None
            if SNOWFLAKE_QUERY_EXPRESSION_KW in source_expression:
                # snowflake query source identified
                return self._parse_snowflake_query_source(source_expression)
            return self._parse_catalog_table_definition(source_expression, datamodel_entity)
        except Exception as exc:
            logger.debug(f"Error to parse snowflake table source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def resolve_source_database(self, table_info: dict) -> Optional[str]:  # noqa: UP045
        """Database to resolve a parsed M source against.

        Defaults to whatever the M parser found (``None`` for an Athena source
        whose catalog level is the ``AwsDataCatalog`` placeholder). A subclass
        may override this to map a data-source name (``table_info["dsn"]``,
        set by ``_parse_athena_source``/``_extract_tables_from_sql``) onto a
        database/catalog when the expression itself doesn't name one - e.g. to
        tell apart two AWS accounts that are ingested as two OM databases
        under one PowerBI service but only differ by DSN.
        """
        return table_info.get("database")

    def _parse_athena_source(self, source_expression: str) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """
        Parse Power Query M expressions sourced from the Athena PowerBI
        connector (``AmazonAthena.Databases``) or a generic ODBC DSN pointed
        at Athena (``Odbc.Query`` / ``Odbc.DataSource``).

        The Athena connector's catalog navigation always has exactly three
        levels - Database, Schema, Table (or View) - reached through
        ``Source{[Name = "...", Kind = "..."]}[Data]`` records; step names
        vary (``Navigation`` vs ``#"Navigation 3"``) so matching is done on
        the record itself, never on step names. The ``Kind="Database"`` level
        is Athena's default-catalog placeholder (``AwsDataCatalog``) unless a
        real federated catalog is configured, so a placeholder value is
        dropped rather than returned as an OM database - see
        ``resolve_source_database`` for how a subclass can still use it (via
        the returned ``dsn``) to pick an OM database.
        """
        try:
            is_athena = ATHENA_DATABASES_EXPRESSION_KW in source_expression
            is_odbc = (
                ODBC_QUERY_EXPRESSION_KW in source_expression or ODBC_DATASOURCE_EXPRESSION_KW in source_expression
            )
            if not is_athena and not is_odbc:
                return None
            self._metrics[self.METRIC_ATHENA_ODBC_QUERIES_SEEN] += 1

            if is_athena:
                dsn_match = re.search(r'AmazonAthena\.Databases\(\s*"([^"]+)"', source_expression)
            else:
                dsn_match = re.search(r'Odbc\.(?:Query|DataSource)\(\s*"dsn=([^"]+?)"', source_expression)
            dsn = dsn_match.group(1) if dsn_match else None

            query_match = re.search(
                r'Odbc\.Query\(\s*"dsn=[^"]+"\s*,\s*"((?:[^"]|"")*)"',
                source_expression,
                re.DOTALL,
            )
            if query_match:
                sql_query = query_match.group(1)
                return self._extract_tables_from_sql(sql_query, database=None, server=dsn, dialect=Dialect.ATHENA)

            nav_matches = re.findall(
                r'\{\[\s*Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"([^"]+)"\s*\]\}',
                source_expression,
            )
            if not nav_matches:
                return None
            kind_to_name = {kind: name for name, kind in nav_matches}
            table = kind_to_name.get("Table") or kind_to_name.get("View")
            if not table:
                return None
            catalog = kind_to_name.get("Database")
            database = None if catalog == ATHENA_DEFAULT_CATALOG else catalog
            return [
                {
                    "database": database,
                    "schema": kind_to_name.get("Schema"),
                    "table": table,
                    "dsn": dsn,
                }
            ]
        except Exception as exc:
            logger.debug(f"Error to parse Athena/ODBC table source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _parse_table_info_from_source_exp(
        self, table: PowerBiTable, datamodel_entity: DashboardDataModel
    ) -> Optional[List[dict]]:  # noqa: UP006, UP045
        try:
            if not isinstance(table.source, list):
                return None
            source_expression = table.source[0].expression
            if not source_expression:
                logger.debug(f"No source expression found for table: {table.name}")
                return None

            # parse snowflake source
            table_info_list = self._parse_snowflake_source(source_expression, datamodel_entity)
            if isinstance(table_info_list, List):  # noqa: UP006
                return table_info_list

            # parse redshift source
            table_info_list = self._parse_redshift_source(source_expression)
            if isinstance(table_info_list, List):  # noqa: UP006
                return table_info_list

            # parse bigquery source
            table_info_list = self._parse_bigquery_source(source_expression, datamodel_entity, table)
            if isinstance(table_info_list, List):  # noqa: UP006
                return table_info_list

            # parse databricks source
            table_info_list = self._parse_databricks_source(source_expression, datamodel_entity)
            if isinstance(table_info_list, List):  # noqa: UP006
                return table_info_list

            # parse Athena / ODBC-to-Athena source
            # `PowerBITableSource.expression`'s "before" validator already joins a
            # list of M lines into one string before storage, but its declared type
            # stays `str | List[str]`; normalize again so this narrows to `str`.
            athena_source_expression = (
                "\n".join(source_expression) if isinstance(source_expression, list) else source_expression
            )
            if isinstance(athena_source_expression, str):
                table_info_list = self._parse_athena_source(athena_source_expression)
                if isinstance(table_info_list, List):  # noqa: UP006
                    return table_info_list

            # parse generic Sql.Database source
            # (inline query, native query, catalog access)
            table_info_list = self._parse_sql_source(source_expression)
            if isinstance(table_info_list, List):  # noqa: UP006
                return table_info_list

            return None  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error to parse table source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _get_table_and_datamodel_lineage(
        self,
        db_service_prefix: Optional[str],  # noqa: UP045
        table: PowerBiTable,
        datamodel_entity: DashboardDataModel,
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        Method to create lineage between table and datamodels
        """
        (
            prefix_service_name,
            prefix_database_name,
            prefix_schema_name,
            prefix_table_name,
        ) = self.parse_db_service_prefix(db_service_prefix)

        try:
            table_info_list = self._parse_table_info_from_source_exp(table, datamodel_entity)
            if not table_info_list:
                # if tables are not found from source expression
                # try establishing lineage using powerbi's table name.
                # PowerBiTable.name is now Optional, so skip nameless tables here
                # to match _get_column_info and avoid build_es_fqn_search_string
                # raising on a None table_name (which surfaces as a noisy lineage
                # error rather than a quiet skip).
                if not table.name:
                    logger.debug(
                        "Skipping PowerBI table with empty name for lineage to datamodel [%s]",
                        datamodel_entity.name,
                    )
                    return
                table_info_list = [{"table": table.name}]
            if isinstance(table_info_list, List):  # noqa: UP006
                for table_info in table_info_list:
                    table_name = table_info.get("table") or table.name
                    schema_name = table_info.get("schema")
                    database_name = self.resolve_source_database(table_info)
                    if prefix_table_name and table_name and prefix_table_name.lower() != table_name.lower():
                        logger.debug(f"Table {table_name} does not match prefix {prefix_table_name}")
                        return

                    if prefix_schema_name and schema_name and prefix_schema_name.lower() != schema_name.lower():
                        logger.debug(f"Schema {table_info.get('schema')} does not match prefix {prefix_schema_name}")
                        return

                    if prefix_database_name and database_name and prefix_database_name.lower() != database_name.lower():
                        logger.debug(
                            f"Database {table_info.get('database')} does not match prefix {prefix_database_name}"
                        )
                        return

                    try:
                        fqn_search_string = build_es_fqn_search_string(
                            service_name=prefix_service_name or "*",
                            table_name=(prefix_table_name or table_name),
                            schema_name=(prefix_schema_name or schema_name),
                            database_name=(prefix_database_name or database_name),
                        )
                    except ValueError:
                        logger.debug(f"Skipping table '{table_name}' with invalid FQN characters")
                        continue
                    table_entity = self.metadata.search_in_any_service(
                        entity_type=Table,
                        fqn_search_string=fqn_search_string,
                    )
                    self._record_source_reference(datamodel_entity, fqn_search_string, resolved=bool(table_entity))
                    if table_entity and datamodel_entity:
                        logger.debug(
                            "Creating lineage between db table=%s and datamodel=%s",
                            table_entity.name.root,  # pyright: ignore[reportAttributeAccessIssue]
                            datamodel_entity.name.root,
                        )
                        columns_list = [column.name for column in (table.columns or []) if column.name]
                        column_lineage = self._get_column_lineage(table_entity, datamodel_entity, columns_list)
                        yield self._get_add_lineage_request(
                            to_entity=datamodel_entity,
                            from_entity=table_entity,
                            column_lineage=column_lineage,
                        )
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name="DataModel Lineage for pbit files",
                    error=(
                        "Error to yield datamodel lineage details using pbit files for"
                        f"datamodel [{datamodel_entity.name}]: {exc}"
                    ),
                    stackTrace=traceback.format_exc(),
                )
            )

    def create_table_datamodel_lineage_from_files(
        self,
        db_service_prefix: Optional[str],  # noqa: UP045
        datamodel_entity: Optional[DashboardDataModel],  # noqa: UP045
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        Method to create lineage between table and datamodels using pbit files
        """
        (prefix_service_name, *_) = self.parse_db_service_prefix(db_service_prefix)

        try:
            # check if the datamodel_file_mappings is populated or not
            # if not, then populate the datamodel_file_mappings and process the lineage
            if not self.datamodel_file_mappings:
                self.datamodel_file_mappings = self.client.file_client.get_data_model_schema_mappings()

            # search which file contains the datamodel and for the given datamodel_entity
            datamodel_file_list = []
            for datamodel_schema in self.datamodel_file_mappings or []:
                for connections in datamodel_schema.connectionFile.RemoteArtifacts or []:
                    if connections.DatasetId == model_str(datamodel_entity.name):
                        datamodel_file_list.append(datamodel_schema)  # noqa: PERF401

            for datamodel_schema_file in datamodel_file_list:
                for table in datamodel_schema_file.tables or []:
                    yield from self._get_table_and_datamodel_lineage(
                        db_service_prefix=db_service_prefix,
                        table=table,
                        datamodel_entity=datamodel_entity,
                    )
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name="DataModel Lineage",
                    error=(
                        f"Error to yield datamodel lineage details for DB service name [{prefix_service_name}]: {exc}"
                    ),
                    stackTrace=traceback.format_exc(),
                )
            )

    def _emit_om_target_lineage(
        self,
        *,
        to_entity: Union[DashboardDataModel, Dashboard],  # noqa: UP007
        target_ids: Iterable[Optional[str]],  # noqa: UP045
        target: LineageTargetSpec,
        error_name: str,
        column_lineage_builder: Optional[Callable[..., Optional[List[ColumnLineage]]]] = None,  # noqa: UP006, UP045
    ) -> Iterable[Either[AddLineageRequest]]:
        """Resolve target entities in OM and yield lineage from each into `to_entity`.

        Silent skip on falsy or missing target; failures surface as `Either.left`.
        """
        service_name = self.context.get().dashboard_service  # pyright: ignore[reportAttributeAccessIssue]
        for target_id in target_ids:
            if not target_id:
                logger.debug(
                    "Skipping %s with no target id (to=%s)",
                    error_name,
                    to_entity.name.root,
                )
                continue
            try:
                target_fqn = fqn.build(
                    self.metadata,
                    entity_type=target.entity_type,
                    service_name=service_name,
                    **{target.fqn_kwarg: target_id},
                )
                if not target_fqn:
                    logger.warning(
                        "Cannot build %s FQN for %s: target_id=%s to=%s",
                        target.entity_type.__name__,
                        error_name,
                        target_id,
                        to_entity.name.root,
                    )
                    continue
                target_entity = self.metadata.get_by_name(
                    entity=target.entity_type,
                    fqn=target_fqn,
                )
                if not target_entity:
                    logger.debug(
                        "No %s entity with id=%s found for [%s]",
                        target.entity_type.__name__,
                        target_id,
                        to_entity.name.root,
                    )
                    continue
                column_lineage = column_lineage_builder(to_entity, target_entity) if column_lineage_builder else None
                lineage_request = self._get_add_lineage_request(
                    from_entity=target_entity,
                    to_entity=to_entity,
                    column_lineage=column_lineage,  # pyright: ignore[reportArgumentType]
                )
                if lineage_request is None:
                    logger.debug(
                        "No lineage request built for %s: target=%s to=%s",
                        error_name,
                        target_entity.name.root,
                        to_entity.name.root,
                    )
                    continue
                if self.report_column_usage_enabled and column_lineage:
                    self._track_column_lineage_edge(
                        from_entity=target_entity,
                        to_entity=to_entity,
                        edge_kind=error_name,
                        emitted_count=len(column_lineage),
                    )
                yield lineage_request
            except Exception as exc:  # pylint: disable=broad-except
                yield Either(
                    left=StackTraceError(
                        name=error_name,
                        error=(f"Error to yield {error_name} between [{to_entity.name.root}, {target_id!s}]: {exc}"),
                        stackTrace=traceback.format_exc(),
                    ),
                    right=None,
                )

    def create_dataset_upstream_dataflow_lineage(
        self,
        datamodel: Dataset,
        datamodel_entity: DashboardDataModel,
    ) -> Iterable[Either[AddLineageRequest]]:
        """Create lineage between dataset and upstreamDataflow."""
        column_lineage_builder = None
        if self.report_column_usage_enabled:
            column_lineage_builder = partial(self._create_dataset_upstream_dataflow_column_lineage, datamodel)
        yield from self._emit_om_target_lineage(
            to_entity=datamodel_entity,
            target_ids=(u.targetDataflowId for u in datamodel.upstreamDataflows or []),
            target=DATAMODEL_TARGET,
            error_name="Dataset and UpstreamDataflow Lineage",
            column_lineage_builder=column_lineage_builder,
        )

    def _parse_dataflow_source_ref(self, m_expression: str) -> Optional[Any]:  # noqa: UP045
        """Deferred import of `dataflow_mapping.parse_partition_dataflow_source`."""
        try:
            from metadata.ingestion.source.dashboard.powerbi.dataflow_mapping import (
                parse_partition_dataflow_source,
            )
        except ImportError:
            logger.debug("dataflow_mapping.parse_partition_dataflow_source is not available yet")
            return None
        try:
            return parse_partition_dataflow_source(m_expression)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug(f"Error parsing partition dataflow source: {exc}")
            logger.debug(traceback.format_exc())
            return None

    def _map_columns_to_dataflow(
        self,
        columns: List[str],  # noqa: UP006
        source_ref: Any,
        entity_attribute_names: List[str],  # noqa: UP006
    ) -> Optional[Any]:  # noqa: UP045
        """Deferred import of `dataflow_mapping.map_columns_to_dataflow`."""
        try:
            from metadata.ingestion.source.dashboard.powerbi.dataflow_mapping import (
                map_columns_to_dataflow,
            )
        except ImportError:
            logger.debug("dataflow_mapping.map_columns_to_dataflow is not available yet")
            return None
        try:
            return map_columns_to_dataflow(columns, source_ref, entity_attribute_names)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Error mapping columns to dataflow: {exc}")
            logger.debug(traceback.format_exc())
            return None

    def _create_dataset_upstream_dataflow_column_lineage(
        self,
        datamodel: Dataset,
        to_entity: DashboardDataModel,
        target_entity: DashboardDataModel,
    ) -> List[ColumnLineage]:  # noqa: UP006
        """Dataflow column -> model column lineage for every TMDL table whose
        partition M expression navigates into `target_entity` (the upstream
        dataflow), matched by dataflow id. `to_entity` is the dataset's own
        `DashboardDataModel` (the `_emit_om_target_lineage` naming: the edge always
        points *to* it); named that way here only to match the shared
        `column_lineage_builder(to_entity, target_entity)` call signature.

        Reads the cached `SemanticModelDefinition` (not the adapted
        `dataset.tables`/`PowerBiTable`, which drops `sourceColumn`) because
        `map_columns_to_dataflow` reverse-walks `Table.RenameColumns` M steps
        against each column's *source* name - the query-level name before the model
        renamed it to its display name - and that only survives on the TMDL
        `TmdlColumn.source_column`, not on the OM `PowerBiColumns` shape.
        """
        datamodel_entity = to_entity
        dataflow_entity = target_entity
        dataflow_id = dataflow_entity.name.root
        model_definition = self.state.get_semantic_model_definition(datamodel.id)
        column_lineage: List[ColumnLineage] = []  # noqa: UP006
        for table in getattr(model_definition, "tables", None) or []:
            table_name = getattr(table, "name", None)
            if not table_name:
                continue
            for partition in getattr(table, "partitions", None) or []:
                source_expression = getattr(partition, "source", None)
                if not source_expression:
                    continue
                source_ref = self._parse_dataflow_source_ref(source_expression)
                if source_ref is None or source_ref.dataflow_id != dataflow_id:
                    continue
                entity_column = next(
                    (c for c in dataflow_entity.columns if c.name.root.lower() == source_ref.entity.lower()),
                    None,
                )
                if entity_column is None:
                    continue
                entity_attribute_names = [c.name.root for c in entity_column.children or []]
                # source (query-level) name -> model (display) name.
                source_to_model_column = {
                    (getattr(column, "source_column", None) or column.name): column.name
                    for column in getattr(table, "columns", None) or []
                    if getattr(column, "name", None)
                }
                mapping = self._map_columns_to_dataflow(
                    columns=list(source_to_model_column.keys()),
                    source_ref=source_ref,
                    entity_attribute_names=entity_attribute_names,
                )
                if mapping is None:
                    continue
                for source_column_name, attribute_name in (mapping.mapped or {}).items():
                    model_column_name = source_to_model_column.get(source_column_name, source_column_name)
                    attribute_column = next(
                        (c for c in entity_column.children or [] if c.name.root.lower() == attribute_name.lower()),
                        None,
                    )
                    to_column_fqn = self._get_downstream_data_model_column_fqn(
                        data_model_entity=datamodel_entity,
                        table_name=table_name,
                        column=model_column_name,
                    )
                    attribute_column_fqn = attribute_column.fullyQualifiedName if attribute_column else None
                    if attribute_column_fqn is None or not to_column_fqn:
                        self._metrics[self.METRIC_DATAFLOW_MODEL_COLUMNS_UNMAPPED] += 1
                        continue
                    column_lineage.append(
                        ColumnLineage(
                            fromColumns=[attribute_column_fqn],
                            toColumn=FullyQualifiedEntityName(to_column_fqn),
                        )
                    )
                    self._metrics[self.METRIC_DATAFLOW_MODEL_COLUMNS_MAPPED] += 1
                self._metrics[self.METRIC_DATAFLOW_MODEL_COLUMNS_UNMAPPED] += len(mapping.unmapped or [])
        return column_lineage

    def _get_downstream_data_model_column_fqn(
        self, data_model_entity: DashboardDataModel, table_name: str, column: str
    ) -> Optional[str]:  # noqa: UP045
        """
        Get the FQN of the column if it exists in the
        downstream data model entity's table and column.
        """
        try:
            if not data_model_entity:
                return None
            for table in data_model_entity.columns:
                if table.name.root != table_name:
                    continue
                for child_column in table.children or []:
                    if column.lower() == child_column.name.root.lower():
                        return child_column.fullyQualifiedName.root
        except Exception as exc:
            logger.error(
                f"Error to get downstream data_model_column_fqn for data_model_entity="
                f"{data_model_entity.name.root}, table_name={table_name}, column={column}: {exc}"
            )
            logger.debug(traceback.format_exc())
        return None

    def _create_dataset_upstream_dataset_column_lineage(
        self,
        datamodel_entity: DashboardDataModel,
        upstream_dataset_entity: DashboardDataModel,
    ) -> Optional[List[ColumnLineage]]:  # noqa: UP006, UP045
        """
        Create column lineage between powerbi dataset/datamodel and
        its upstream dataset/datamodel
        """
        try:
            target_tables = [table.name.root for table in datamodel_entity.columns]
            if not target_tables:
                return []
            column_lineage = []
            for table in upstream_dataset_entity.columns:
                if table.name.root not in target_tables:
                    continue
                for column in table.children or []:
                    source_column = column.fullyQualifiedName.root
                    target_column = self._get_downstream_data_model_column_fqn(
                        data_model_entity=datamodel_entity,
                        table_name=table.name.root,
                        column=column.name.root,
                    )
                    if source_column and target_column:
                        column_lineage.append(ColumnLineage(fromColumns=[source_column], toColumn=target_column))
            return column_lineage  # noqa: TRY300
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(
                "Error while creating column lineage between dataset = "
                f"{datamodel_entity.name.root} and upstream dataset = "
                f"{upstream_dataset_entity.name.root}: {exc}"
            )
        return []

    def create_dataset_upstream_dataset_lineage(
        self,
        datamodel: Dataset,
        datamodel_entity: DashboardDataModel,
    ) -> Iterable[Either[AddLineageRequest]]:
        """Create lineage between dataset and upstreamDataset (with column lineage)."""
        yield from self._emit_om_target_lineage(
            to_entity=datamodel_entity,
            target_ids=(u.targetDatasetId for u in datamodel.upstreamDatasets or []),
            target=DATAMODEL_TARGET,
            error_name="Dataset and UpstreamDataset Lineage",
            column_lineage_builder=self._create_dataset_upstream_dataset_column_lineage,
        )

    def _parse_dataflow_m_document(self, dataflow_export: DataflowExportResponse) -> List[dict]:  # noqa: UP006
        """
        Parse Power Query M expressions from the dataflow export document
        to extract table references for each entity/query in the dataflow.

        Returns a list of dicts: [{"entity_name": str, "tables": [{"database": str, "schema": str, "table": str}], "sql": str|None}]
        """
        results = []
        mashup = dataflow_export.mashup
        if not mashup or not mashup.document:
            return results

        document = mashup.document
        queries_metadata = mashup.queriesMetadata or {}

        # Split the document into individual shared expressions
        # Pattern: shared <name> = let ... in ... ;
        shared_blocks = re.split(r"(?:^|\r?\n)shared\s+", document)

        for block in shared_blocks:
            if not block.strip():
                continue

            # Extract the entity name (handles quoted names like #"Channel Categories")
            name_match = re.match(r'(?:#"([^"]+)"|(\S+))\s*=\s*let\b', block, re.DOTALL)
            if not name_match:
                continue
            entity_name = name_match.group(1) or name_match.group(2)

            # Detected independently of loadEnabled: "seen" means the block is
            # Athena/ODBC-shaped, whether or not it ends up dispatched below.
            is_athena_or_odbc_block = any(
                kw in block
                for kw in (ATHENA_DATABASES_EXPRESSION_KW, ODBC_QUERY_EXPRESSION_KW, ODBC_DATASOURCE_EXPRESSION_KW)
            )
            is_recognized_source_block = is_athena_or_odbc_block or SQL_DATABASE_EXPRESSION_KW in block

            # Only process entities that have loadEnabled=true in queriesMetadata
            query_meta = queries_metadata.get(entity_name, {})
            if isinstance(query_meta, dict) and not query_meta.get("loadEnabled", False):
                if is_athena_or_odbc_block:
                    self._metrics[self.METRIC_ATHENA_ODBC_QUERIES_SEEN] += 1
                # Only queries recognized as Athena/ODBC/Sql.Database sourced
                # would otherwise have reached the lineage parser below - a
                # disabled SharePoint/Web helper query was never going to be
                # parsed for lineage, so it doesn't belong in this counter.
                if is_recognized_source_block:
                    self._metrics[self.METRIC_QUERIES_SKIPPED_LOAD_DISABLED] += 1
                continue

            table_info_list = self._parse_athena_source(block)
            if not isinstance(table_info_list, List):  # noqa: UP006
                table_info_list = self._parse_sql_source(block)
            if table_info_list:
                sql_query = None
                for table_info in table_info_list:
                    if table_info.get("sql"):
                        sql_query = table_info.pop("sql")
                results.append(
                    {
                        "entity_name": entity_name,
                        "tables": table_info_list,
                        "sql": sql_query,
                    }
                )
        return results

    def _parse_sql_source(self, m_expression: str) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """
        Parse a Power Query M expression block from a dataflow document to extract
        database table references. Handles:
        1. Sql.Database("server", "db", [Query="SQL"]) - inline SQL query
        2. Value.NativeQuery(Source, "SQL") - NativeQuery pattern
        3. Sql.Database("server", "db") then Source{[Schema="x", Item="y"]}[Data] - catalog access
        """
        try:
            if SQL_DATABASE_EXPRESSION_KW not in m_expression:
                return None

            # Pattern 1: Sql.Database with inline Query parameter
            # e.g. Sql.Database("dwsql", "dw_integration", [Query = "SELECT ..."])
            inline_query_match = re.search(
                r'Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*\[Query\s*=\s*"((?:[^"]|"")*)"',
                m_expression,
                re.DOTALL,
            )
            if inline_query_match:
                server = inline_query_match.group(1)
                database = inline_query_match.group(2)
                sql_query = inline_query_match.group(3)
                return self._extract_tables_from_sql(sql_query, database, server)

            # Pattern 2: Value.NativeQuery with Sql.Database Source
            # e.g. Value.NativeQuery(Source, "SELECT ... FROM schema.table")
            native_query_match = re.search(
                r'Value\.NativeQuery\(\s*\w+\s*,\s*"((?:[^"]|"")*)"',
                m_expression,
                re.DOTALL,
            )
            if native_query_match:
                sql_query = native_query_match.group(1)
                # Extract database info from Sql.Database call in the same block
                db_match = re.search(
                    r'Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"',
                    m_expression,
                )
                database = db_match.group(2) if db_match else None
                server = db_match.group(1) if db_match else None
                return self._extract_tables_from_sql(sql_query, database, server)

            # Pattern 3: Catalog access - Sql.Database("server", "db") then
            # Source{[Schema="dbo", Item="TableName"]}[Data]
            db_match = re.search(
                r'Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"',
                m_expression,
            )
            if db_match:
                server = db_match.group(1)
                database = db_match.group(2)

                schema_match = re.search(
                    r'\{?\[Schema\s*=\s*"([^"]+)"\s*,\s*Item\s*=\s*"([^"]+)"\]',
                    m_expression,
                )
                if schema_match:
                    schema = schema_match.group(1)
                    table = schema_match.group(2)
                    return [
                        {
                            "database": database,
                            "schema": schema,
                            "table": table,
                        }
                    ]

            return None  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error parsing dataflow SQL source: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def _extract_tables_from_sql(
        self,
        sql_query: str,
        database: Optional[str],  # noqa: UP045
        server: Optional[str],  # noqa: UP045
        dialect: Dialect = Dialect.TSQL,
    ) -> Optional[List[dict]]:  # noqa: UP006, UP045
        """
        Extract table references from a SQL query found in a dataflow M expression.

        Defaults to the TSQL dialect for the Power Query Sql.Database /
        Value.NativeQuery connector (SQL Server / Azure SQL), whose queries use
        bracket-quoted identifiers like [Column Name]. Callers sourcing Athena
        SQL (``Odbc.Query`` DSNs) pass ``dialect=Dialect.ATHENA`` so
        double-quoted identifiers like "schema"."table" parse correctly instead.
        """
        try:
            # Clean PowerBI special characters
            cleaned_sql = sql_query.replace("#(lf)", "\n")
            cleaned_sql = cleaned_sql.replace("#(tab)", "\t")
            cleaned_sql = cleaned_sql.replace('""', '"')
            cleaned_sql = re.sub(r"\s+", " ", cleaned_sql).strip()

            if not cleaned_sql:
                return None

            try:
                parser = LineageParser(
                    cleaned_sql,
                    dialect=dialect,
                    timeout_seconds=30,
                    parser_type=self.get_query_parser_type(),
                )
            except Exception as parser_exc:
                logger.debug(f"LineageParser failed for dataflow SQL: {parser_exc}")
                return None

            if not parser.source_tables:
                logger.debug("No source tables found in Power Query M SQL")
                return None

            lineage_tables = []
            for source_table in parser.source_tables:
                table_name = source_table.raw_name
                schema_name = None
                database_name = database

                if hasattr(source_table, "schema") and source_table.schema:
                    schema_str = (
                        source_table.schema.raw_name
                        if hasattr(source_table.schema, "raw_name")
                        else str(source_table.schema)
                    )
                    if "." in schema_str:
                        parts = schema_str.split(".")
                        if len(parts) == 2:
                            database_name = parts[0] if parts[0] else database
                            schema_name = parts[1]
                        else:
                            schema_name = schema_str
                    else:
                        schema_name = schema_str

                if table_name:
                    table_info = {
                        "database": database_name,
                        "schema": schema_name,
                        "table": table_name,
                        "sql": cleaned_sql,
                    }
                    if dialect == Dialect.ATHENA:
                        table_info["dsn"] = server
                    lineage_tables.append(table_info)
            return lineage_tables if lineage_tables else None  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error extracting tables from dataflow SQL: {exc}")
            logger.debug(traceback.format_exc())
        return None

    def create_dataflow_table_lineage(
        self,
        datamodel: Dataflow,
        datamodel_entity: DashboardDataModel,
        dataflow_export: DataflowExportResponse,
        db_service_prefix: Optional[str],  # noqa: UP045
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        Create lineage between dataflow entities and database tables
        by parsing the Power Query M document from the dataflow export.
        Also creates column-level lineage where column names match.
        """
        (
            prefix_service_name,
            prefix_database_name,
            prefix_schema_name,
            prefix_table_name,
        ) = self.parse_db_service_prefix(db_service_prefix)

        try:
            parsed_entities = self._parse_dataflow_m_document(dataflow_export)
            if not parsed_entities:
                logger.debug(f"No table references found in dataflow [{datamodel.name}] M document")
                return

            # Build a map of entity_name -> entity attributes for column lineage.
            # Skip nameless entities/attributes since both are now Optional and a
            # None entity name can never match a parsed M-document reference,
            # while None attribute names break the List[str] contract of
            # _get_dataflow_column_lineage and produce noisy failed lookups.
            entity_attributes_map = {}
            for entity in dataflow_export.entities or []:
                if not entity.name:
                    logger.debug(
                        "Skipping nameless dataflow entity while building attributes map for dataflow [%s]",
                        datamodel.name,
                    )
                    continue
                entity_attributes_map[entity.name] = [attr.name for attr in entity.attributes or [] if attr.name]

            for parsed_entity in parsed_entities:
                entity_name = parsed_entity["entity_name"]
                sql_query = parsed_entity.get("sql")

                for table_info in parsed_entity.get("tables", []):
                    table_name = table_info.get("table")
                    schema_name = table_info.get("schema")
                    database_name = self.resolve_source_database(table_info)

                    if not table_name:
                        continue

                    if prefix_table_name and table_name and prefix_table_name.lower() != table_name.lower():
                        continue

                    if prefix_schema_name and schema_name and prefix_schema_name.lower() != schema_name.lower():
                        continue

                    if prefix_database_name and database_name and prefix_database_name.lower() != database_name.lower():
                        continue
                    try:
                        fqn_search_string = build_es_fqn_search_string(
                            service_name=prefix_service_name or "*",
                            table_name=prefix_table_name or table_name,
                            schema_name=prefix_schema_name or schema_name,
                            # `or "*"` mirrors build_es_fqn_search_string's own
                            # internal `database_name or "*"` fallback, so this
                            # narrows the static type to `str` with no behavior
                            # change versus passing a possibly-`None` value through.
                            database_name=(prefix_database_name or database_name) or "*",
                        )
                    except ValueError:
                        logger.debug(f"Skipping table '{table_name}' with invalid FQN characters")
                        continue
                    table_entity = self.metadata.search_in_any_service(
                        entity_type=Table,
                        fqn_search_string=fqn_search_string,
                    )
                    self._record_source_reference(datamodel_entity, fqn_search_string, resolved=bool(table_entity))
                    if table_entity and datamodel_entity:
                        column_lineage = self._get_dataflow_column_lineage(
                            table_entity=table_entity,
                            datamodel_entity=datamodel_entity,
                            entity_name=entity_name,
                            entity_attributes=entity_attributes_map.get(entity_name, []),
                        )
                        yield self._get_add_lineage_request(
                            to_entity=datamodel_entity,
                            from_entity=table_entity,
                            column_lineage=column_lineage,
                            sql=sql_query,
                        )
        except Exception as exc:  # pylint: disable=broad-except
            yield Either(
                left=StackTraceError(
                    name="Dataflow Table Lineage",
                    error=(f"Error to yield dataflow table lineage for dataflow [{datamodel.name}]: {exc}"),
                    stackTrace=traceback.format_exc(),
                )
            )

    def _get_dataflow_column_lineage(
        self,
        table_entity: Table,
        datamodel_entity: DashboardDataModel,
        entity_name: str,
        entity_attributes: List[str],  # noqa: UP006
    ) -> List[ColumnLineage]:  # noqa: UP006
        """
        Get column-level lineage between a database table and a dataflow entity.
        Matches columns from the database table to the dataflow entity's attributes
        within the datamodel.
        """
        try:
            column_lineage = []
            for attr_name in entity_attributes:
                from_column = get_column_fqn(table_entity=table_entity, column=attr_name)
                to_column = self._get_downstream_data_model_column_fqn(
                    data_model_entity=datamodel_entity,
                    table_name=entity_name,
                    column=attr_name,
                )
                if from_column and to_column:
                    column_lineage.append(ColumnLineage(fromColumns=[from_column], toColumn=to_column))
            return column_lineage  # noqa: TRY300
        except Exception as exc:
            logger.debug(f"Error getting dataflow column lineage: {exc}")
            logger.debug(traceback.format_exc())
        return []

    def create_dataflow_upstream_dataflow_lineage(
        self,
        datamodel: Dataflow,
        datamodel_entity: DashboardDataModel,
    ) -> Iterable[Either[AddLineageRequest]]:
        """Create lineage between dataflow and upstreamDataflow."""
        yield from self._emit_om_target_lineage(
            to_entity=datamodel_entity,
            target_ids=(u.targetDataflowId for u in datamodel.upstreamDataflows or []),
            target=DATAMODEL_TARGET,
            error_name="Dataflow and UpstreamDataflow Lineage",
        )

    def create_datamart_upstream_datamart_lineage(
        self,
        datamodel: Datamart,
        datamodel_entity: DashboardDataModel,
    ) -> Iterable[Either[AddLineageRequest]]:
        """Create lineage between datamart and its upstream datamarts."""
        yield from self._emit_om_target_lineage(
            to_entity=datamodel_entity,
            target_ids=(
                u.targetDatamartId
                for u in datamodel.upstreamDatamarts or []
                if u.targetDatamartId and u.targetDatamartId != datamodel.id
            ),
            target=DATAMODEL_TARGET,
            error_name="Datamart and UpstreamDatamart Lineage",
        )

    def _reports_for_datamodel(self, dataset_id: str) -> List[PowerBIReport]:  # noqa: UP006
        """Reports in the current workspace whose resolved dataset id(s)
        (`_resolve_report_dataset_ids`) include `dataset_id`."""
        reports = []
        for dashboard in self.state.filtered_dashboards:
            details = self.get_dashboard_details(dashboard)
            if isinstance(details, PowerBIReport) and dataset_id in self._resolve_report_dataset_ids(details):
                reports.append(details)
        return reports

    def _compute_column_usage(self, model_definition: Any, reports: Mapping[str, Any]) -> Optional[Any]:  # noqa: UP045
        """Deferred import of `column_usage.resolve_column_usage`."""
        try:
            from metadata.ingestion.source.dashboard.powerbi.column_usage import (
                resolve_column_usage,
            )
        except ImportError:
            logger.debug("column_usage.resolve_column_usage is not available yet")
            return None
        try:
            return resolve_column_usage(model_definition, reports)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Error resolving column usage: {exc}")
            logger.debug(traceback.format_exc())
            return None

    def _ensure_column_usage_computed(self) -> None:
        """Compute and cache `ModelColumnUsage` for every dataset in the current
        workspace that has a cached semantic model definition (i.e. TMDL fetch +
        parse succeeded in `yield_datamodel`) - memoised so repeat calls within the
        same workspace (`yield_dashboard_lineage_details` runs once per
        `db_service_prefix`) are a no-op. Logs dangling refs at INFO and calls the
        `on_datamodel_column_usage` hook once per model, after the model entity
        exists in OM.
        """
        if self.state.column_usage_computed:
            return
        self.state.mark_column_usage_computed()
        for datamodel in self._filtered_datamodels():
            if not isinstance(datamodel, Dataset):
                continue
            model_definition = self.state.get_semantic_model_definition(datamodel.id)
            if model_definition is None:
                continue
            reports = {
                report.id: report_definition
                for report in self._reports_for_datamodel(datamodel.id)
                if (report_definition := self.state.get_report_definition(report.id)) is not None
            }
            usage = self._compute_column_usage(model_definition, reports)
            if usage is None:
                continue
            self.state.cache_column_usage(datamodel.id, usage)
            self._metrics[self.METRIC_MODEL_COLUMNS_USED] += len(usage.used or frozenset())
            self._metrics[self.METRIC_MODEL_COLUMNS_UNUSED] += len(usage.unused or frozenset())
            self._metrics[self.METRIC_DAX_UNRESOLVED] += len(usage.dax_unresolved or set())
            for report_id, dangling_refs in (usage.dangling or {}).items():
                for ref in dangling_refs or []:
                    self._metrics[self.METRIC_COLUMN_REFS_DANGLING] += 1
                    logger.info(
                        "Dangling column/measure reference in report [%s]: table=%s name=%s (kind=%s)",
                        report_id,
                        getattr(ref, "table", None),
                        getattr(ref, "name", None),
                        getattr(ref, "kind", None),
                    )
            datamodel_fqn = fqn.build(
                self.metadata,
                entity_type=DashboardDataModel,
                service_name=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                data_model_name=datamodel.id,
            )
            datamodel_entity = (
                self.metadata.get_by_name(entity=DashboardDataModel, fqn=datamodel_fqn) if datamodel_fqn else None
            )
            if datamodel_entity:
                self.on_datamodel_column_usage(datamodel_entity, usage)

    def yield_dashboard_lineage_details(
        self,
        dashboard_details: Group,
        db_service_prefix: Optional[str] = None,  # noqa: UP045
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        We will build the logic to build the logic as below
        tables - datamodel - report - dashboard
        """
        (prefix_service_name, *_) = self.parse_db_service_prefix(db_service_prefix)

        if self.report_column_usage_enabled:
            # Must run before the report loop below: `create_datamodel_report_lineage`
            # needs each dataset's `ModelColumnUsage` (cached here) to build its
            # model -> chart column lineage. Memoised per workspace
            # (`state.column_usage_computed`) since this method can run once per
            # `db_service_prefix`.
            self._ensure_column_usage_computed()

        for dashboard in self.state.filtered_dashboards:
            dashboard_details = self.get_dashboard_details(dashboard)
            try:
                if isinstance(dashboard_details, PowerBIReport):
                    yield from self.create_datamodel_report_lineage(
                        db_service_prefix=db_service_prefix,
                        dashboard_details=dashboard_details,
                    )
                if isinstance(dashboard_details, PowerBIDashboard):
                    yield from self.create_report_dashboard_lineage(dashboard_details=dashboard_details)
            except Exception as exc:  # pylint: disable=broad-except
                yield Either(
                    left=StackTraceError(
                        name="Dashboard Lineage",
                        error=f"Error to yield dashboard lineage details for DB service name [{str(prefix_service_name)}]: {exc}",  # noqa: RUF010
                        stackTrace=traceback.format_exc(),
                    )
                )
        """
        Iterate loop for filtered datamodels so datamodels which are not connected to
        any report but have tables would be eligible for a dataset-db_table lineage.
        Also create below lineages:
        1. dataset-db_table
        2. dataset-upstreamDataflow
        3. dataset-upstreamDataset
        4. dataset-db_table (from pbit files)
        5. dataflow-db_table (from M document parsing)
        6. dataflow-upstreamDataflow
        7. datamart-upstreamDatamart
        """
        for datamodel in self._filtered_datamodels():
            try:
                datamodel_fqn = fqn.build(
                    self.metadata,
                    entity_type=DashboardDataModel,
                    service_name=self.context.get().dashboard_service,  # pyright: ignore[reportAttributeAccessIssue]
                    data_model_name=datamodel.id,
                )
                datamodel_entity = self.metadata.get_by_name(
                    entity=DashboardDataModel,
                    fqn=datamodel_fqn,
                )
                if datamodel_entity:
                    if isinstance(datamodel, Dataset):
                        # 1. datamodel-db_table lineage
                        for table in datamodel.tables or []:
                            yield from self._get_table_and_datamodel_lineage(
                                db_service_prefix=db_service_prefix,
                                table=table,
                                datamodel_entity=datamodel_entity,
                            )
                        # 2. dataset-upstreamDataflow lineage
                        yield from self.create_dataset_upstream_dataflow_lineage(datamodel, datamodel_entity)
                        # 3. dataset-upstreamDataset lineage
                        yield from self.create_dataset_upstream_dataset_lineage(datamodel, datamodel_entity)
                        # create the lineage between table and datamodel using the pbit files
                        if self.client.file_client:
                            yield from self.create_table_datamodel_lineage_from_files(
                                db_service_prefix=db_service_prefix,
                                datamodel_entity=datamodel_entity,
                            )
                    elif isinstance(datamodel, Dataflow):
                        # 5. dataflow-db_table lineage via M document parsing
                        dataflow_export = self.state.get_dataflow_export(datamodel.id)
                        if dataflow_export:
                            yield from self.create_dataflow_table_lineage(
                                datamodel=datamodel,
                                datamodel_entity=datamodel_entity,
                                dataflow_export=dataflow_export,
                                db_service_prefix=db_service_prefix,
                            )
                        # 6. dataflow-upstreamDataflow lineage
                        yield from self.create_dataflow_upstream_dataflow_lineage(datamodel, datamodel_entity)
                    elif isinstance(datamodel, Datamart):
                        # 7. datamart-upstreamDatamart lineage
                        yield from self.create_datamart_upstream_datamart_lineage(datamodel, datamodel_entity)
                    else:
                        logger.warning(f"Unknown datamodel type: {type(datamodel)}, name: {datamodel.name}")
            except Exception as exc:  # pylint: disable=broad-except
                yield Either(
                    left=StackTraceError(
                        name="Datamodel Lineage",
                        error=f"Error to yield datamodel lineage details for DB service name [{str(prefix_service_name)}]: {exc}",  # noqa: RUF010
                        stackTrace=traceback.format_exc(),
                    )
                )

    def yield_dashboard_lineage(
        self,
        dashboard_details: Any,
    ) -> Iterable[Either]:
        """Flush the sink before lineage resolution so that target lookups in
        super().yield_dashboard_lineage see this workspace's just-flushed entities.
        """
        ws_id = self.context.get().workspace.id  # pyright: ignore[reportAttributeAccessIssue]
        yield Either(right=Barrier(reason=f"powerbi_ws:{ws_id}"))  # pyright: ignore[reportCallIssue]
        yield from super().yield_dashboard_lineage(dashboard_details)
        if self.report_column_usage_enabled and self._pending_column_lineage_edges:
            # Flush the column-lineage edges just yielded above before reading any
            # of them back - `get_lineage_edge` would otherwise see a pre-write state.
            yield Either(right=Barrier(reason=f"powerbi_column_lineage_verify:{ws_id}"))  # pyright: ignore[reportCallIssue]
            self._verify_column_lineage_edges()

    def yield_datamodel_dashboard_lineage(
        self,
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        Returns:
            Lineage request between Data Models and Dashboards
        """
        """
            We're implementing this differently inside `yield_dashboard_lineage_details`
            since we have report and dashboard both as dashboard.
        """

    def get_project_name(self, dashboard_details: Any) -> Optional[str]:  # noqa: UP045
        """
        Get the project / workspace / folder / collection name of the dashboard
        """
        try:
            return str(self.context.get().workspace.name)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error fetching project name for {dashboard_details.id}: {exc}")
        return None

    def get_owner_ref(self, dashboard_details: Any) -> Optional[EntityReferenceList]:  # noqa: UP045
        """
        Method to process the dashboard owners
        """
        try:
            if not self.source_config.includeOwners:
                logger.debug(f"Skipping owner processing for {dashboard_details.id} as includeOwners is False")
                return None
            if self.service_connection.useAdminApis:
                return self._get_owner_ref_admin(dashboard_details)
            return self._get_owner_ref_non_admin(dashboard_details)
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(f"Could not fetch owner data due to {err}")
        return None

    def _get_owner_ref_admin(  # pylint: disable=unused-argument, useless-return  # noqa: C901
        self, dashboard_details: Any
    ) -> Optional[EntityReferenceList]:  # noqa: UP045
        """
        Admin-mode owner resolution: reads the per-entity `users` array that the
        admin workspace scan embeds inline (`getArtifactUsers`). Non-admin GET
        endpoints never populate that array - see `_get_owner_ref_non_admin`.
        """
        owner_ref_list = []  # to assign multiple owners to entity if they exist
        for owner in dashboard_details.users or []:
            owner_ref = None
            # put filtering conditions
            access_right: Optional[str] = None  # noqa: UP045
            if isinstance(dashboard_details, Dataset):
                access_right = owner.datasetUserAccessRight
            elif isinstance(dashboard_details, Dataflow):
                access_right = owner.dataflowUserAccessRight
            elif isinstance(dashboard_details, Datamart):
                access_right = owner.datamartUserAccessRight
            elif isinstance(dashboard_details, PowerBIReport):
                access_right = owner.reportUserAccessRight
            elif isinstance(dashboard_details, PowerBIDashboard):
                access_right = owner.dashboardUserAccessRight

            if owner.userType != "Member":
                logger.debug(f"User is not a member of {dashboard_details.id}: ({owner.displayName}, {owner.email})")
                continue
            if access_right and any(keyword in access_right.lower() for keyword in OWNER_ACCESS_RIGHTS_KEYWORDS):
                if owner.email:
                    try:
                        owner_email = EmailStr._validate(owner.email)  # pyright: ignore[reportAttributeAccessIssue]
                    except PydanticCustomError:
                        logger.debug(f"Invalid email for owner: {owner.email}")
                        owner_email = None
                    if owner_email:
                        try:
                            owner_ref = self.metadata.get_reference_by_email(owner_email.lower())
                        except Exception as err:
                            logger.debug(
                                f"Could not process owner data with email"
                                f" {owner.email} in {dashboard_details.id}: {err}"
                            )
                elif owner.displayName:
                    try:
                        owner_ref = self.metadata.get_reference_by_name(name=owner.displayName)
                    except Exception as err:
                        logger.debug(
                            f"Could not process owner data with name"
                            f" {owner.displayName} in {dashboard_details.id}: {err}"
                        )
                if owner_ref:
                    owner_ref_list.append(owner_ref.root[0])
            else:
                logger.debug(
                    f"User does not have owner, admin or write access to"
                    f" {dashboard_details.id}: ({owner.displayName}, {owner.email})"
                )
        # check for last modified, configuredBy user
        current_active_user = None
        if isinstance(dashboard_details, Dataset):
            current_active_user = dashboard_details.configuredBy
        elif isinstance(dashboard_details, (Dataflow, PowerBIReport, Datamart)):
            current_active_user = dashboard_details.modifiedBy
        if current_active_user:
            try:
                owner_ref = self.metadata.get_reference_by_email(current_active_user.lower())
                if owner_ref and owner_ref.root[0] not in owner_ref_list:
                    owner_ref_list.append(owner_ref.root[0])
            except Exception as err:
                logger.debug(f"Could not fetch current active user due to {err}")
        if len(owner_ref_list) > 0:
            logger.debug(f"Successfully fetched owners data for {dashboard_details.id}")
            return EntityReferenceList(root=owner_ref_list)
        return None

    def resolve_owner_principal(self, principal: PowerBIPrincipal) -> Optional[EntityReference]:  # noqa: UP045
        """Resolve one Power BI principal to an OpenMetadata owner reference.

        Looks the principal up in OpenMetadata and returns None when it does
        not exist. Subclasses may override to provision missing principals -
        this default implementation never creates a user or team, so generic
        ingestion never writes one as a side effect of owner resolution.
        """
        if principal.principal_type == POWERBI_APP_PRINCIPAL_TYPE:
            return None
        try:
            if principal.email:
                owner_ref_list = self.metadata.get_reference_by_email(principal.email.lower())
            elif principal.display_name:
                # `is_owner=True` rejects a Team match whose type isn't Group -
                # `get_reference_by_name` can otherwise return the wrong kind
                # of Team for a Power BI security group.
                owner_ref_list = self.metadata.get_reference_by_name(
                    name=principal.display_name,
                    is_owner=(principal.principal_type == POWERBI_GROUP_PRINCIPAL_TYPE),
                )
            else:
                # A Group normalized from the dataset-ACL endpoint has neither
                # (that endpoint carries no email or display name at all - see
                # `PowerBIPrincipal.from_dataset_user`) - counted unresolved,
                # never guessed at from `identifier` (an opaque object id).
                return None
        except Exception as err:
            logger.debug(f"Could not resolve owner principal {principal.identifier}: {err}")
            return None
        if owner_ref_list and owner_ref_list.root:
            return owner_ref_list.root[0]
        return None

    def _resolve_write_principals(
        self,
        principals: Iterable[PowerBIPrincipal],
        is_write_right: Callable[[Optional[str]], bool],  # noqa: UP045
    ) -> List[EntityReference]:  # noqa: UP006
        """Resolve every distinct write-capable principal in `principals` to an
        owner reference via `resolve_owner_principal` (the seam subclasses use
        to provision missing users/teams). Viewer-level and unresolved
        principals are counted, never treated as owners; `App` principals are
        never resolved at all - a Power BI app is not a person or a team.

        Groups every row by `(principal_type, identifier)` first - the
        identifier lower-cased, since a Power BI UPN is case-insensitive and
        the two source endpoints are not guaranteed to agree on case (not
        observed to actually differ live; defensive) - and only then decides
        write-capability, across every row in the group. A principal reached
        from two sources (e.g. a workspace member who also has a row on a
        dataset's ACL) can hold a low-privilege row from one and a
        write-level row from the other (a workspace Viewer with
        `ReadWriteReshareExplore` on the dataset is an ordinary setup); it
        must be judged an owner by its best row, not by whichever row the
        dedup happened to see first - grouping before judging makes the
        outcome independent of input order.

        `is_write_right` decides write-capability from a row's own
        `access_right` (a workspace role or a dataset right, depending on
        where that row came from) - a predicate, not a fixed set, because a
        dataset right is not exact-matched (see `is_write_dataset_right`'s
        docstring).
        """
        groups: dict[tuple[str, str], List[PowerBIPrincipal]] = {}  # noqa: UP006
        for principal in principals:
            key = (principal.principal_type, principal.identifier.lower())
            groups.setdefault(key, []).append(principal)

        owner_refs: List[EntityReference] = []  # noqa: UP006
        for (principal_type, _), rows in groups.items():
            if principal_type == POWERBI_APP_PRINCIPAL_TYPE:
                self._metrics[self.METRIC_OWNER_PRINCIPALS_SKIPPED_APP] += 1
                continue
            if not any(is_write_right(row.access_right) for row in rows):
                self._metrics[self.METRIC_OWNER_PRINCIPALS_SKIPPED_VIEWER] += 1
                continue
            principal = self._merge_principal_rows(rows)
            try:
                owner_ref = self.resolve_owner_principal(principal)
            except Exception as err:
                logger.debug(f"Could not resolve owner principal {principal.identifier}: {err}")
                owner_ref = None
            if owner_ref is None:
                self._metrics[self.METRIC_OWNER_PRINCIPALS_UNRESOLVED] += 1
                continue
            owner_refs.append(owner_ref)
        return owner_refs

    @staticmethod
    def _merge_principal_rows(rows: List[PowerBIPrincipal]) -> PowerBIPrincipal:  # noqa: UP006
        """One principal can surface as more than one row - a workspace-member
        row and a dataset-ACL row for the same `(principal_type, identifier)`
        - and the two endpoints don't carry the same fields (the dataset-ACL
        endpoint has no email or display name at all for a `Group`; see
        `PowerBIPrincipal.from_dataset_user`). Merge every row's identifying
        info before resolving, so resolution has whatever any row provides,
        not just whichever row happened to be the write-capable one.
        """
        base = rows[0]
        return PowerBIPrincipal(
            principal_type=base.principal_type,
            identifier=base.identifier,
            email=next((row.email for row in rows if row.email), None),
            display_name=next((row.display_name for row in rows if row.display_name), None),
            access_right=base.access_right,
        )

    def _collect_non_admin_owners(
        self,
        configured_by: Optional[str],  # noqa: UP045
        principals: List[PowerBIPrincipal],  # noqa: UP006
        is_write_right: Callable[[Optional[str]], bool],  # noqa: UP045
    ) -> List[EntityReference]:  # noqa: UP006
        """`configured_by` (the non-admin equivalent of admin mode's
        `modifiedBy`) plus every write-capable principal, resolved to distinct
        owner references. Both paths go through `resolve_owner_principal` so a
        subclass override (e.g. provisioning missing users) applies uniformly.
        """
        owner_refs: List[EntityReference] = []  # noqa: UP006
        seen_ids: set = set()

        def _add(ref: Optional[EntityReference]) -> None:  # noqa: UP045
            if ref is None:
                return
            # `ref.id` is a `Uuid` RootModel, not hashable on its own - key on
            # its string form instead (see `model_str()`'s docstring).
            ref_id = model_str(ref.id)
            if ref_id not in seen_ids:
                seen_ids.add(ref_id)
                owner_refs.append(ref)

        if configured_by:
            try:
                _add(
                    self.resolve_owner_principal(
                        PowerBIPrincipal(
                            principal_type=POWERBI_USER_PRINCIPAL_TYPE,
                            identifier=configured_by,
                            email=configured_by,
                        )
                    )
                )
            except Exception as err:
                logger.debug(f"Could not resolve configuredBy owner {configured_by}: {err}")

        for owner_ref in self._resolve_write_principals(principals, is_write_right):
            _add(owner_ref)
        return owner_refs

    def _compute_dataflow_owner_refs(self, dataflow: Dataflow) -> List[EntityReference]:  # noqa: UP006
        """Dataflow owners = `configuredBy` + workspace members with a write-capable role.

        Memoised per dataflow id for the current workspace (`WorkspaceState`):
        a dataflow's owner set is looked up here at most once per run, however
        many times it is reached, since nothing currently re-derives a
        dataflow's owners from a dependent asset the way a dataset's are.
        Kept memoised anyway for the same reason as the dataset path below -
        cheap, and future-proof against a dependent being added later.
        """
        cached = self.state.get_dataflow_owner_refs(dataflow.id)
        if cached is not None:
            return list(cached)
        owner_refs = self._collect_non_admin_owners(
            configured_by=dataflow.configuredBy,
            principals=self.state.workspace_principals,
            is_write_right=lambda right: right in POWERBI_WRITE_WORKSPACE_ROLES,
        )
        self.state.cache_dataflow_owner_refs(dataflow.id, owner_refs)
        return owner_refs

    def _compute_datamodel_owner_refs(self, dataset: Dataset) -> List[EntityReference]:  # noqa: UP006
        """Semantic model (dataset) owners = `configuredBy` + workspace members with
        a write-capable role + dataset-ACL principals with a write-level right.

        A principal's `access_right` is checked against whichever domain it
        actually came from (workspace role vs dataset right) via a single
        combined predicate - the two domains' values never overlap, so this
        stays correct for a merged principals list without needing to track
        each principal's source separately.

        Memoised per dataset id for the current workspace: every report that
        points at this dataset (`_compute_report_owner_refs`), and every
        dashboard tile behind one of those reports
        (`_compute_dashboard_owner_refs`), reaches this same dataset - without
        memoisation each of those recomputes the full principal resolution,
        including the `resolve_owner_principal` OpenMetadata lookups, once per
        dependent instead of once per dataset.
        """
        cached = self.state.get_dataset_owner_refs(dataset.id)
        if cached is not None:
            return list(cached)
        principals = list(self.state.workspace_principals) + list(dataset.dataset_principals or [])
        owner_refs = self._collect_non_admin_owners(
            configured_by=dataset.configuredBy,
            principals=principals,
            is_write_right=lambda right: right in POWERBI_WRITE_WORKSPACE_ROLES or is_write_dataset_right(right),
        )
        self.state.cache_dataset_owner_refs(dataset.id, owner_refs)
        return owner_refs

    def _compute_report_owner_refs(self, report: PowerBIReport) -> List[EntityReference]:  # noqa: UP006
        """Reports have no owner endpoint in non-admin mode (404) - they inherit
        their semantic model's owners via `datasetId`.
        """
        if not report.datasetId:
            return []
        dataset = self.state.find_dataset(report.datasetId)
        if dataset is None:
            return []
        return self._compute_datamodel_owner_refs(dataset)

    def _compute_dashboard_owner_refs(self, dashboard: PowerBIDashboard) -> List[EntityReference]:  # noqa: UP006
        """Dashboards have no owner endpoint in non-admin mode either - they union
        the owners of every report behind their tiles.
        """
        owner_refs: List[EntityReference] = []  # noqa: UP006
        seen_ids: set = set()
        for tile in dashboard.tiles or []:
            if not tile.reportId:
                continue
            report = self.state.find_report(tile.reportId)
            if report is None:
                continue
            for ref in self._compute_report_owner_refs(report):
                ref_id = model_str(ref.id)
                if ref_id not in seen_ids:
                    seen_ids.add(ref_id)
                    owner_refs.append(ref)
        return owner_refs

    def _get_owner_ref_non_admin(self, dashboard_details: Any) -> Optional[EntityReferenceList]:  # noqa: UP045
        """
        Non-admin owner resolution.

        Non-admin GET endpoints never populate `users` on individual dashboard
        entities - that array is filled by the admin scan's `getArtifactUsers`
        (see `_get_owner_ref_admin`). Instead, ownership is derived from
        `configuredBy` plus workspace membership and dataset ACLs fetched
        separately (`fetch_group_users` / `fetch_dataset_users`, wired in
        `get_org_workspace_data`); reports and dashboards have no owner
        endpoint of their own in non-admin mode and inherit owners from their
        semantic model / reports respectively. See the `_compute_*_owner_refs`
        methods for the per-entity-type rules.
        """
        if isinstance(dashboard_details, PowerBIDashboard):
            owner_refs = self._compute_dashboard_owner_refs(dashboard_details)
            metric = self.METRIC_OWNERS_ASSIGNED_DASHBOARDS
        elif isinstance(dashboard_details, PowerBIReport):
            owner_refs = self._compute_report_owner_refs(dashboard_details)
            metric = self.METRIC_OWNERS_ASSIGNED_REPORTS
        elif isinstance(dashboard_details, Dataset):
            owner_refs = self._compute_datamodel_owner_refs(dashboard_details)
            metric = self.METRIC_OWNERS_ASSIGNED_DATAMODELS
        elif isinstance(dashboard_details, Dataflow):
            owner_refs = self._compute_dataflow_owner_refs(dashboard_details)
            metric = self.METRIC_OWNERS_ASSIGNED_DATAFLOWS
        else:
            # Datamart is admin-scan-only (see its docstring in models.py) -
            # never reached when useAdminApis is False.
            return None
        if owner_refs:
            self._metrics[metric] += 1
            logger.debug(f"Successfully resolved non-admin owners for {dashboard_details.id}")
            return EntityReferenceList(root=owner_refs)
        self._metrics[self.METRIC_ASSETS_WITHOUT_OWNER] += 1
        return None
