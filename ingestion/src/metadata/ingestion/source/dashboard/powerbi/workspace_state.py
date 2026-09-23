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
"""Workspace-scoped ingestion state for the PowerBI source.

Lifecycle contract:
    enter(workspace)   activates a workspace; raises if another is active
    exit()             releases per-workspace caches; idempotent
    enter + exit must be paired by the caller (typically via try / finally
    around the workspace iteration).
"""

from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.source.dashboard.powerbi.models import (
    Dataflow,
    DataflowExportResponse,
    Dataset,
    Group,
    PowerBIDashboard,
    PowerBIPrincipal,
    PowerBIReport,
)

# A workspace's "dashboards" list from the admin scan can contain either
# PowerBI Dashboards or Reports (both modelled as Dashboard in OM).
DashboardLike = PowerBIDashboard | PowerBIReport
# A workspace's "datamodels" set is the concatenation of datasets and
# dataflows; both become DashboardDataModel entities in OM.
DataModelLike = Dataset | Dataflow


class WorkspaceState:
    """State container for PowerBI workspace iteration.

    Per-workspace caches released on `exit`. Cross-workspace report-id
    registry persists for the whole run (tile-pinned lineage needs it
    to verify that a tile's referenced report exists somewhere in the
    tenant; only the id is required, not the report payload).
    """

    # Bounded per CLAUDE.md's cache rule, even though these are released on
    # every `exit()` - a pathological single workspace should still degrade
    # to recomputing rather than grow unbounded within one `enter()` scope.
    _MAX_CACHED_OWNER_REFS = 5_000

    def __init__(self) -> None:
        self._current: Group | None = None
        self._datasets_by_id: dict[str, Dataset] = {}
        self._reports_by_id: dict[str, PowerBIReport] = {}
        self._dataflow_exports: dict[str, DataflowExportResponse] = {}
        self._known_report_ids: set[str] = set()
        self._filtered_dashboards: list[DashboardLike] = []
        self._filtered_datamodels: list[DataModelLike] | None = None
        self._dashboard_charts: dict[str, list[str]] = {}
        self._workspace_principals: list[PowerBIPrincipal] = []
        self._dataset_owner_refs: dict[str, list[EntityReference]] = {}
        self._dataflow_owner_refs: dict[str, list[EntityReference]] = {}
        # Report/model definitions fetched from Fabric and the column usage computed
        # from them - only populated when `report_column_usage_enabled` is True.
        # Same per-workspace, reset-on-exit lifecycle as `_dataflow_exports`.
        self._report_definitions: dict[str, object] = {}
        self._semantic_model_definitions: dict[str, object] = {}
        self._column_usage: dict[str, object] = {}
        self._column_usage_computed: bool = False

    def enter(self, workspace: Group) -> None:
        """Activate `workspace` and build its per-workspace caches.

        Raises:
            RuntimeError: a workspace is already active; call `exit()` first.
        """
        if self._current is not None:
            raise RuntimeError(
                f"WorkspaceState.enter() called while workspace "
                f"'{self._current.name}' is still active. Call exit() first."
            )
        self._current = workspace
        self._datasets_by_id = {d.id: d for d in workspace.datasets or []}
        self._reports_by_id = {r.id: r for r in workspace.reports or []}
        self._filtered_dashboards = []
        self._filtered_datamodels = None
        self._dashboard_charts = {}
        # Non-admin only; empty for a workspace built from the admin scan
        # (owner resolution there reads each entity's own `users` instead).
        self._workspace_principals = workspace.workspace_principals or []
        self._dataset_owner_refs = {}
        self._dataflow_owner_refs = {}
        self._report_definitions = {}
        self._semantic_model_definitions = {}
        self._column_usage = {}
        self._column_usage_computed = False
        for report in workspace.reports or []:
            self._known_report_ids.add(report.id)

    def exit(self) -> None:
        """Release per-workspace caches. Idempotent. Cross-workspace report registry persists."""
        if self._current is None:
            return
        self._current = None
        self._datasets_by_id = {}
        self._reports_by_id = {}
        self._dataflow_exports = {}
        self._filtered_dashboards = []
        self._filtered_datamodels = None
        self._dashboard_charts = {}
        self._workspace_principals = []
        self._dataset_owner_refs = {}
        self._dataflow_owner_refs = {}
        self._report_definitions = {}
        self._semantic_model_definitions = {}
        self._column_usage = {}
        self._column_usage_computed = False

    @property
    def current(self) -> Group:
        """Return the active workspace, raising if none is set."""
        if self._current is None:
            raise RuntimeError("No active workspace scope.")
        return self._current

    def find_dataset(self, dataset_id: str) -> Dataset | None:
        """Look up a dataset by id in the current workspace."""
        return self._datasets_by_id.get(dataset_id)

    def is_known_report(self, report_id: str | None) -> bool:
        """Return True if `report_id` was seen in any workspace entered so far."""
        return report_id is not None and report_id in self._known_report_ids

    def find_report(self, report_id: str) -> PowerBIReport | None:
        """Look up a report by id in the current workspace.

        Used for non-admin dashboard owner resolution, which has no owner
        endpoint of its own and instead unions the owners of the reports
        behind the dashboard's tiles.
        """
        return self._reports_by_id.get(report_id)

    @property
    def workspace_principals(self) -> list[PowerBIPrincipal]:
        """This workspace's membership (non-admin only; empty under the admin scan)."""
        return self._workspace_principals

    def cache_dataflow_export(self, key: str, export: DataflowExportResponse) -> None:
        """Memoise a dataflow export for the current workspace's lineage stage."""
        self._dataflow_exports[key] = export

    def get_dataflow_export(self, key: str) -> DataflowExportResponse | None:
        """Fetch a previously cached dataflow export for the current workspace."""
        return self._dataflow_exports.get(key)

    # --- Non-admin owner refs: memoised per dataset/dataflow id -------------
    #
    # A dataset's (or dataflow's) owner set is recomputed by every dependent
    # asset that inherits it - a report via its `datasetId`, a dashboard via
    # every tile's report - so without this cache the same OpenMetadata
    # lookups (`resolve_owner_principal`) run once per dependent, and the
    # owner_principals_* counters count each principal once per dependent
    # instead of once per owning asset. Caching here (not in the source
    # itself) keeps the memoisation workspace-scoped, matching every other
    # per-workspace cache on this class.

    def cache_dataset_owner_refs(self, dataset_id: str, owner_refs: list[EntityReference]) -> None:
        """Memoise a dataset's computed owner refs for the current workspace."""
        if len(self._dataset_owner_refs) < self._MAX_CACHED_OWNER_REFS:
            self._dataset_owner_refs[dataset_id] = owner_refs

    def get_dataset_owner_refs(self, dataset_id: str) -> list[EntityReference] | None:
        """Fetch a previously cached dataset owner-refs list; `None` on a cache miss."""
        return self._dataset_owner_refs.get(dataset_id)

    def cache_dataflow_owner_refs(self, dataflow_id: str, owner_refs: list[EntityReference]) -> None:
        """Memoise a dataflow's computed owner refs for the current workspace."""
        if len(self._dataflow_owner_refs) < self._MAX_CACHED_OWNER_REFS:
            self._dataflow_owner_refs[dataflow_id] = owner_refs

    def get_dataflow_owner_refs(self, dataflow_id: str) -> list[EntityReference] | None:
        """Fetch a previously cached dataflow owner-refs list; `None` on a cache miss."""
        return self._dataflow_owner_refs.get(dataflow_id)

    # --- Filtered dashboards: write per-item, read by iteration -------------

    def add_filtered_dashboard(self, dashboard: DashboardLike) -> None:
        """Record a dashboard that passed the workspace's filter."""
        self._filtered_dashboards.append(dashboard)

    @property
    def filtered_dashboards(self) -> list[DashboardLike]:
        """Dashboards that passed the filter for the current workspace."""
        return self._filtered_dashboards

    # --- Filtered datamodels: write-once (lazy memo), read by iteration -----

    def set_filtered_datamodels(self, datamodels: list[DataModelLike]) -> None:
        """Populate the memoised filtered-datamodels cache for the current workspace."""
        self._filtered_datamodels = datamodels

    @property
    def filtered_datamodels(self) -> list[DataModelLike] | None:
        """Memoised filtered datamodels; `None` until populated via setter."""
        return self._filtered_datamodels

    # --- Dashboard charts: per-dashboard consume-on-read --------------------

    def add_dashboard_chart(self, dashboard_id: str, chart_id: str) -> None:
        """Record a chart id under a dashboard for the current workspace."""
        self._dashboard_charts.setdefault(dashboard_id, []).append(chart_id)

    def pop_dashboard_chart_ids(self, dashboard_id: str) -> list[str]:
        """Consume the chart ids for a dashboard; empty list if absent or already consumed."""
        return self._dashboard_charts.pop(dashboard_id, [])

    # --- Report/model definitions + column usage: report-column-usage feature only ---
    #
    # Populated only when `PowerbiSource.report_column_usage_enabled` is True. Report
    # and model definitions are cached as they're fetched (chart/datamodel stages) so
    # the later lineage stage, which runs in the same workspace scope, reuses them
    # instead of re-fetching from Fabric.

    def cache_report_definition(self, report_id: str, definition: object) -> None:
        """Memoise a parsed report definition for the current workspace."""
        self._report_definitions[report_id] = definition

    def get_report_definition(self, report_id: str) -> object | None:
        """Fetch a previously cached report definition; `None` on a cache miss."""
        return self._report_definitions.get(report_id)

    def cache_semantic_model_definition(self, dataset_id: str, definition: object) -> None:
        """Memoise a parsed semantic model (TMDL) definition for the current workspace."""
        self._semantic_model_definitions[dataset_id] = definition

    def get_semantic_model_definition(self, dataset_id: str) -> object | None:
        """Fetch a previously cached semantic model definition; `None` on a cache miss."""
        return self._semantic_model_definitions.get(dataset_id)

    def cache_column_usage(self, dataset_id: str, usage: object) -> None:
        """Memoise a dataset's computed `ModelColumnUsage` for the current workspace."""
        self._column_usage[dataset_id] = usage

    def get_column_usage(self, dataset_id: str) -> object | None:
        """Fetch a previously cached `ModelColumnUsage`; `None` on a cache miss."""
        return self._column_usage.get(dataset_id)

    @property
    def column_usage_computed(self) -> bool:
        """Whether `resolve_column_usage` has already run for every dataset in this workspace."""
        return self._column_usage_computed

    def mark_column_usage_computed(self) -> None:
        """Record that column usage has been computed for every dataset in the current workspace."""
        self._column_usage_computed = True
