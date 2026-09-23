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
Test the report -> semantic-model column usage feature: the `report_column_usage_enabled`
seam, chart creation per report visual, TMDL -> PowerBiTable adaptation, model -> chart
and dataflow -> model column lineage, the `on_datamodel_column_usage` hook, and metrics.

Wires against the real `report_definition`/`tmdl`/`column_usage`/`dataflow_mapping`
dataclasses (built in parallel with this connector) directly, rather than re-testing
their own byte-parsing logic - that's their own test suite's job. Two tests
(`TestReportDefinitionEndToEnd`, `TestTmdlEndToEnd`) additionally run real bytes through
the real parsers to prove the wiring, not just the shapes, line up.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from metadata.generated.schema.entity.data.chart import ChartType
from metadata.generated.schema.entity.data.dashboard import Dashboard
from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel
from metadata.generated.schema.entity.data.table import Column, DataType
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.generated.schema.type.entityLineage import ColumnLineage
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.powerbi.column_usage import (
    ColumnUse,
    ModelColumnUsage,
)
from metadata.ingestion.source.dashboard.powerbi.dataflow_mapping import (
    ColumnMapping,
    DataflowSourceRef,
)
from metadata.ingestion.source.dashboard.powerbi.fabric_client import (
    FabricDefinitionResult,
)
from metadata.ingestion.source.dashboard.powerbi.metadata import PowerbiSource
from metadata.ingestion.source.dashboard.powerbi.models import (
    Dataset,
    Group,
    PowerBIReport,
)
from metadata.ingestion.source.dashboard.powerbi.report_definition import (
    FieldRef,
    ReportDefinition,
    VisualDefinition,
    parse_report_definition,
)
from metadata.ingestion.source.dashboard.powerbi.tmdl import (
    SemanticModelDefinition,
    TmdlColumn,
    TmdlMeasure,
    TmdlPartition,
    TmdlTable,
    parse_tmdl,
)

MOCK_CONFIG = {
    "source": {
        "type": "powerbi",
        "serviceName": "mock_powerbi_column_usage",
        "serviceConnection": {
            "config": {
                "type": "PowerBI",
                "clientId": "client_id",
                "clientSecret": "secret",
                "tenantId": "tenant_id",
            },
        },
        "sourceConfig": {"config": {"type": "DashboardMetadata"}},
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "loggerLevel": "DEBUG",
        "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "openmetadata",
            "securityConfig": {"jwtToken": "mock-token"},
        },
    },
}


class _EnabledPowerbiSource(PowerbiSource):
    """Test-only subclass with the feature switched on."""

    report_column_usage_enabled = True


def _build_source(cls=PowerbiSource):
    with (
        patch("metadata.ingestion.source.dashboard.dashboard_service.DashboardServiceSource.test_connection"),
        patch("metadata.ingestion.source.dashboard.dashboard_service.create_connection") as create_connection,
        patch("metadata.ingestion.source.dashboard.powerbi.fabric_client.msal"),
    ):
        create_connection.return_value.client = False
        config = OpenMetadataWorkflowConfig.model_validate(MOCK_CONFIG)
        source = cls.create(
            MOCK_CONFIG["source"],
            OpenMetadata(config.workflowConfig.openMetadataServerConfig),
        )
    mock_context = MagicMock()
    mock_context.workspace = Group(id="ws-1", name="Test Workspace")
    mock_context.dashboard_service = "test_powerbi_service"
    source.context.get = MagicMock(return_value=mock_context)
    if source.fabric_client is not None:
        # Hermetic by default: `_get_report_definition`/`_get_semantic_model_definition`
        # always resolve `lastUpdatedTimeUtc` via these before fetching a definition,
        # and the real `FabricApiClient` would otherwise reach for a real (lazily
        # built, unmocked here) msal client - i.e. a real network call. Tests that
        # care about this listing override these mocks themselves.
        source.fabric_client.list_reports_last_updated = MagicMock(return_value={})
        source.fabric_client.list_semantic_models_last_updated = MagicMock(return_value={})
    return source


def _dm(id_: str, name: str, fqn_: str, columns=None) -> DashboardDataModel:
    """Build a `DashboardDataModel` via `.model_validate` rather than the strict
    constructor - the id/name/fqn args below are plain strings, and basedpyright
    checks a `model_validate(dict)` call far more leniently than field-by-field
    kwargs against `Uuid`/`EntityName`/`FullyQualifiedEntityName`, which is all this
    file needs from these fixtures (pydantic still fully validates them)."""
    return DashboardDataModel.model_validate(
        {
            "id": id_,
            "name": name,
            "fullyQualifiedName": fqn_,
            "dataModelType": "PowerBIDataModel",
            "service": {"id": "22222222-2222-2222-2222-222222222222", "type": "dashboardService"},
            "columns": columns or [],
        }
    )


def _col(name: str, fqn_: str, data_type=DataType.NUMBER, children=None) -> Column:
    """Build a `Column` via `.model_validate` - see `_dm`."""
    payload: dict = {"name": name, "dataType": data_type, "fullyQualifiedName": fqn_}
    if children is not None:
        payload["children"] = children
    return Column.model_validate(payload)


def _dashboard(id_: str, name: str, fqn_: str) -> Dashboard:
    """Build a `Dashboard` via `.model_validate` - see `_dm`."""
    return Dashboard.model_validate(
        {
            "id": id_,
            "name": name,
            "fullyQualifiedName": fqn_,
            "service": {"id": "22222222-2222-2222-2222-222222222222", "type": "dashboardService"},
        }
    )


@pytest.fixture
def source():
    return _build_source(PowerbiSource)


@pytest.fixture
def enabled_source():
    return _build_source(_EnabledPowerbiSource)


class TestSwitchOff:
    """With `report_column_usage_enabled` False, behaviour must be unchanged."""

    def test_no_fabric_client_constructed(self, source):
        assert source.fabric_client is None

    def test_report_dashboard_request_has_no_charts_field(self, source):
        report = PowerBIReport(id="rep-1", name="Report One", datasetId="ds-1")
        source.state.add_filtered_dashboard(report)
        source.get_owner_ref = MagicMock(return_value=None)

        requests = [e.right for e in source.yield_dashboard(Group(id="ws-1", name="Test Workspace")) if e.right]

        assert len(requests) == 1
        # Omitted (None), never an explicit empty list - see the comment at the call
        # site: `CreateDashboardRequest(charts=[])` serializes differently from a
        # dashboard request that never mentions `charts` at all.
        assert requests[0].charts is None

    def test_report_visuals_never_fetched_from_chart_stage(self, source):
        report = PowerBIReport(id="rep-1", name="Report One")
        source.state.add_filtered_dashboard(report)

        charts = list(source.yield_dashboard_chart(Group(id="ws-1", name="Test Workspace")))

        assert charts == []
        assert source.state.pop_dashboard_chart_ids("rep-1") == []

    def test_all_new_metrics_seeded_at_zero(self, source):
        values = source.metric_values()
        for key in (
            PowerbiSource.METRIC_REPORT_DEFINITIONS_FETCHED,
            PowerbiSource.METRIC_MODEL_DEFINITIONS_FETCHED,
            PowerbiSource.METRIC_REPORTS_FORMAT_LEGACY,
            PowerbiSource.METRIC_VISUALS_DATA,
            PowerbiSource.METRIC_MEASURES_RESOLVED_TRANSITIVELY,
            PowerbiSource.METRIC_REPORT_VISUAL_CHARTS_CREATED,
            PowerbiSource.METRIC_MODEL_COLUMNS_INGESTED,
            PowerbiSource.METRIC_AUTO_DATE_TABLES_SKIPPED,
            PowerbiSource.METRIC_COLUMN_LINEAGE_EMITTED,
            PowerbiSource.METRIC_COLUMN_LINEAGE_VERIFIED,
            PowerbiSource.METRIC_COLUMN_LINEAGE_DROPPED_BY_SERVER,
            PowerbiSource.METRIC_DATAFLOW_MODEL_COLUMNS_MAPPED,
            PowerbiSource.METRIC_DATAFLOW_MODEL_COLUMNS_UNMAPPED,
            PowerbiSource.METRIC_MODEL_COLUMNS_USED,
            PowerbiSource.METRIC_MODEL_COLUMNS_UNUSED,
        ):
            assert values[key] == 0

    def test_on_datamodel_column_usage_default_is_noop(self, source):
        assert source.on_datamodel_column_usage(MagicMock(), MagicMock()) is None


class TestChartCreationPerVisual:
    def test_data_visual_becomes_chart_non_data_visual_does_not(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="Report One")
        enabled_source.state.add_filtered_dashboard(report)
        report_definition = ReportDefinition(
            format="legacy",
            visuals=[
                VisualDefinition(
                    visual_id="v1",
                    page_id="page1",
                    page_display_name="Page One",
                    visual_type="barChart",
                    title="Revenue by Region",
                    refs=[FieldRef("column", "Sales", "Amount")],
                    is_data_visual=True,
                ),
                VisualDefinition(
                    visual_id="v2",
                    page_id="page1",
                    page_display_name="Page One",
                    visual_type="textbox",
                    title=None,
                    refs=[],
                    is_data_visual=False,
                ),
            ],
        )
        enabled_source._get_report_definition = MagicMock(return_value=report_definition)

        charts = [e.right for e in enabled_source.yield_dashboard_chart(Group(id="ws-1", name="x")) if e.right]

        assert len(charts) == 1
        chart = charts[0]
        assert chart.name.root == "rep-1_v1"
        assert chart.displayName == "Revenue by Region"
        assert chart.chartType == ChartType.Bar
        assert enabled_source.state.pop_dashboard_chart_ids("rep-1") == ["rep-1_v1"]
        assert enabled_source.metric_values()[PowerbiSource.METRIC_VISUALS_DATA] == 1
        assert enabled_source.metric_values()[PowerbiSource.METRIC_VISUALS_NON_DATA] == 1
        assert enabled_source.metric_values()[PowerbiSource.METRIC_REPORT_VISUAL_CHARTS_CREATED] == 1

    def test_display_name_falls_back_to_page_and_visual_type(self, enabled_source):
        report = PowerBIReport(id="rep-2", name="Report Two")
        enabled_source.state.add_filtered_dashboard(report)
        report_definition = ReportDefinition(
            format="pbir",
            visuals=[
                VisualDefinition(
                    visual_id="v9",
                    page_id="page9",
                    page_display_name="Overview",
                    visual_type="lineChart",
                    title=None,
                    refs=[FieldRef("column", "Sales", "Date")],
                    is_data_visual=True,
                )
            ],
        )
        enabled_source._get_report_definition = MagicMock(return_value=report_definition)

        charts = [e.right for e in enabled_source.yield_dashboard_chart(Group(id="ws-1", name="x")) if e.right]

        assert charts[0].displayName == "Overview / lineChart"
        assert charts[0].chartType == ChartType.Line

    def test_unknown_visual_type_falls_back_to_other(self, enabled_source):
        report = PowerBIReport(id="rep-3", name="Report Three")
        enabled_source.state.add_filtered_dashboard(report)
        report_definition = ReportDefinition(
            format="legacy",
            visuals=[
                VisualDefinition(
                    visual_id="v1",
                    page_id="p1",
                    page_display_name="P",
                    visual_type="someNewVisualType",
                    title="T",
                    refs=[FieldRef("column", "T", "C")],
                    is_data_visual=True,
                )
            ],
        )
        enabled_source._get_report_definition = MagicMock(return_value=report_definition)

        charts = [e.right for e in enabled_source.yield_dashboard_chart(Group(id="ws-1", name="x")) if e.right]

        assert charts[0].chartType == ChartType.Other

    def test_no_report_definition_yields_no_charts(self, enabled_source):
        report = PowerBIReport(id="rep-4", name="Report Four")
        enabled_source.state.add_filtered_dashboard(report)
        enabled_source._get_report_definition = MagicMock(return_value=None)

        charts = list(enabled_source.yield_dashboard_chart(Group(id="ws-1", name="x")))

        assert charts == []

    def test_report_dashboard_request_includes_chart_fqns(self, enabled_source):
        report = PowerBIReport(id="rep-5", name="Report Five")
        enabled_source.state.add_filtered_dashboard(report)
        enabled_source.get_owner_ref = MagicMock(return_value=None)
        enabled_source.state.add_dashboard_chart("rep-5", "rep-5_v1")

        with patch(
            "metadata.ingestion.source.dashboard.powerbi.metadata.fqn.build",
            side_effect=lambda *a, **kw: kw.get("chart_name"),
        ):
            requests = [e.right for e in enabled_source.yield_dashboard(Group(id="ws-1", name="x")) if e.right]

        assert requests[0].charts is not None
        assert [c.root for c in requests[0].charts] == ["rep-5_v1"]


class TestFabricFetchOrchestration:
    """`_get_report_definition` / `_get_semantic_model_definition`: cache, metrics, no client."""

    def test_no_fabric_client_returns_none(self, source):
        report = PowerBIReport(id="rep-1", name="R")
        assert source._get_report_definition(report, "ws-1") is None

    def test_fetch_failure_increments_failed_metric(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R")
        enabled_source.fabric_client.get_report_definition = MagicMock(return_value=None)

        result = enabled_source._get_report_definition(report, "ws-1")

        assert result is None
        assert enabled_source.metric_values()[PowerbiSource.METRIC_REPORT_DEFINITIONS_FAILED] == 1

    def test_second_call_served_from_workspace_state_cache(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R")
        report_definition = ReportDefinition(format="legacy")
        enabled_source.fabric_client.get_report_definition = MagicMock(
            return_value=FabricDefinitionResult(parts={"report.json": b"{}"}, from_cache=False)
        )
        enabled_source._parse_report_definition = MagicMock(return_value=report_definition)

        first = enabled_source._get_report_definition(report, "ws-1")
        second = enabled_source._get_report_definition(report, "ws-1")

        assert first is report_definition
        assert second is report_definition
        enabled_source.fabric_client.get_report_definition.assert_called_once()

    def test_from_cache_result_increments_cache_skipped_metric(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R")
        enabled_source.fabric_client.get_report_definition = MagicMock(
            return_value=FabricDefinitionResult(parts={"report.json": b"{}"}, from_cache=True)
        )
        enabled_source._parse_report_definition = MagicMock(return_value=ReportDefinition(format="legacy"))

        enabled_source._get_report_definition(report, "ws-1")

        assert enabled_source.metric_values()[PowerbiSource.METRIC_REPORT_DEFINITIONS_CACHE_SKIPPED] == 1
        assert enabled_source.metric_values()[PowerbiSource.METRIC_REPORT_DEFINITIONS_FETCHED] == 0


class TestLastUpdatedListing:
    """`_get_report_last_updated` / `_get_semantic_model_last_updated`: one Fabric
    listing call per workspace per run, threaded into the getDefinition cache key."""

    def test_last_updated_is_looked_up_and_passed_through(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R")
        enabled_source.fabric_client.list_reports_last_updated = MagicMock(
            return_value={"rep-1": "2026-01-01T00:00:00Z"}
        )
        enabled_source.fabric_client.get_report_definition = MagicMock(
            return_value=FabricDefinitionResult(parts={"report.json": b"{}"}, from_cache=False)
        )
        enabled_source._parse_report_definition = MagicMock(return_value=ReportDefinition(format="legacy"))

        enabled_source._get_report_definition(report, "ws-1")

        enabled_source.fabric_client.get_report_definition.assert_called_once_with(
            "ws-1", "rep-1", "2026-01-01T00:00:00Z"
        )

    def test_listing_called_once_per_workspace_not_per_report(self, enabled_source):
        report_a = PowerBIReport(id="rep-a", name="A")
        report_b = PowerBIReport(id="rep-b", name="B")
        enabled_source.fabric_client.list_reports_last_updated = MagicMock(return_value={})
        enabled_source.fabric_client.get_report_definition = MagicMock(return_value=None)

        enabled_source._get_report_definition(report_a, "ws-1")
        enabled_source._get_report_definition(report_b, "ws-1")

        enabled_source.fabric_client.list_reports_last_updated.assert_called_once_with("ws-1")

    def test_listing_failure_falls_back_to_none_and_counts_metric(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R")
        enabled_source.fabric_client.list_reports_last_updated = MagicMock(return_value=None)
        enabled_source.fabric_client.get_report_definition = MagicMock(return_value=None)

        enabled_source._get_report_definition(report, "ws-1")

        enabled_source.fabric_client.get_report_definition.assert_called_once_with("ws-1", "rep-1", None)
        assert enabled_source.metric_values()[PowerbiSource.METRIC_REPORT_LAST_UPDATED_LISTING_FAILED] == 1

    def test_item_missing_the_field_is_recorded_as_none_not_fabricated(self, enabled_source):
        enabled_source.fabric_client.list_reports_last_updated = MagicMock(return_value={"rep-1": None})

        mapping = enabled_source._get_report_last_updated("ws-1")

        assert mapping == {"rep-1": None}

    def test_model_last_updated_looked_up_and_passed_through(self, enabled_source):
        dataset = Dataset(id="ds-1", name="Sales Model")
        enabled_source.fabric_client.list_semantic_models_last_updated = MagicMock(
            return_value={"ds-1": "2026-02-01T00:00:00Z"}
        )
        enabled_source.fabric_client.get_semantic_model_definition = MagicMock(return_value=None)

        enabled_source._get_semantic_model_definition(dataset, "ws-1")

        enabled_source.fabric_client.get_semantic_model_definition.assert_called_once_with(
            "ws-1", "ds-1", "2026-02-01T00:00:00Z"
        )

    def test_model_listing_failure_falls_back_to_none_and_counts_metric(self, enabled_source):
        dataset = Dataset(id="ds-1", name="Sales Model")
        enabled_source.fabric_client.list_semantic_models_last_updated = MagicMock(return_value=None)
        enabled_source.fabric_client.get_semantic_model_definition = MagicMock(return_value=None)

        enabled_source._get_semantic_model_definition(dataset, "ws-1")

        assert enabled_source.metric_values()[PowerbiSource.METRIC_MODEL_LAST_UPDATED_LISTING_FAILED] == 1


class TestTmdlAdapter:
    """`_tmdl_table_to_powerbi_table` / `_replace_dataset_tables_with_tmdl`."""

    def test_adapts_columns_measures_and_partition_source(self, enabled_source):
        table = TmdlTable(
            name="Sales",
            columns=[TmdlColumn(name="Amount", data_type="int64", source_column="AMOUNT")],
            measures=[TmdlMeasure(name="Total", expression="SUM(Sales[Amount])")],
            partitions=[TmdlPartition(name="Sales", mode="import", source="let Source = 1 in Source")],
        )

        powerbi_table = enabled_source._tmdl_table_to_powerbi_table(table)

        assert powerbi_table.name == "Sales"
        assert [c.name for c in powerbi_table.columns] == ["Amount"]
        assert powerbi_table.columns[0].dataType == "int64"
        assert [m.name for m in powerbi_table.measures] == ["Total"]
        assert powerbi_table.measures[0].expression == "SUM(Sales[Amount])"
        # Preserved via `partitions[0].source`, not `source` directly, so
        # `PowerBiTable.extract_source_from_partitions` populates `.source` the same
        # way it does for the push-dataset-tables response.
        assert powerbi_table.source[0].expression == "let Source = 1 in Source"

    def test_replace_dataset_tables_skips_auto_date_tables(self, enabled_source):
        dataset = Dataset(id="ds-1", name="Sales Model", tables=[])
        model_definition = SemanticModelDefinition(
            tables=[
                TmdlTable(name="Sales", columns=[TmdlColumn(name="Amount")]),
                TmdlTable(name="LocalDateTable_abc123", is_auto_date=True, columns=[TmdlColumn(name="Date")]),
            ]
        )
        enabled_source._get_semantic_model_definition = MagicMock(return_value=model_definition)

        enabled_source._replace_dataset_tables_with_tmdl(dataset)

        assert [t.name for t in dataset.tables or []] == ["Sales"]
        assert enabled_source.metric_values()[PowerbiSource.METRIC_AUTO_DATE_TABLES_SKIPPED] == 1
        assert enabled_source.metric_values()[PowerbiSource.METRIC_MODEL_COLUMNS_INGESTED] == 1

    def test_no_model_definition_leaves_dataset_tables_untouched(self, enabled_source):
        original_tables = []
        dataset = Dataset(id="ds-1", name="Sales Model", tables=original_tables)
        enabled_source._get_semantic_model_definition = MagicMock(return_value=None)

        enabled_source._replace_dataset_tables_with_tmdl(dataset)

        assert dataset.tables == original_tables


class TestModelColumnUsageComputation:
    def _dataset_and_report(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R", datasetId="ds-1")
        dataset = Dataset(id="ds-1", name="Sales Model", tables=[])
        enabled_source.state.add_filtered_dashboard(report)
        enabled_source.state.set_filtered_datamodels([dataset])
        return dataset, report

    def test_memoised_per_workspace(self, enabled_source):
        self._dataset_and_report(enabled_source)
        enabled_source._compute_column_usage = MagicMock(return_value=None)

        enabled_source._ensure_column_usage_computed()
        enabled_source._ensure_column_usage_computed()

        # `_compute_column_usage` is never even reached without a cached model
        # definition, so memoisation is proven by `column_usage_computed` flipping
        # and a second call being a pure no-op.
        assert enabled_source.state.column_usage_computed is True

    def test_hook_called_once_per_model_with_computed_usage(self, enabled_source):
        dataset, report = self._dataset_and_report(enabled_source)
        enabled_source.state.cache_semantic_model_definition(dataset.id, SemanticModelDefinition())
        enabled_source.state.cache_report_definition(report.id, ReportDefinition(format="legacy"))
        usage = ModelColumnUsage(
            business_columns=frozenset({("Sales", "Amount")}),
            used=frozenset({("Sales", "Amount")}),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
        )
        enabled_source._compute_column_usage = MagicMock(return_value=usage)
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "test_powerbi_service.ds-1")
        enabled_source.metadata.get_by_name = MagicMock(return_value=datamodel_entity)
        enabled_source.on_datamodel_column_usage = MagicMock()

        enabled_source._ensure_column_usage_computed()

        enabled_source.on_datamodel_column_usage.assert_called_once_with(datamodel_entity, usage)
        assert enabled_source.state.get_column_usage("ds-1") is usage
        assert enabled_source.metric_values()[PowerbiSource.METRIC_MODEL_COLUMNS_USED] == 1

    def test_dangling_refs_logged_and_counted(self, enabled_source, caplog):
        dataset, report = self._dataset_and_report(enabled_source)
        enabled_source.state.cache_semantic_model_definition(dataset.id, SemanticModelDefinition())
        enabled_source.state.cache_report_definition(report.id, ReportDefinition(format="legacy"))
        dangling_ref = FieldRef("column", "Ghost", "NoSuchColumn")
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            dangling={"rep-1": [dangling_ref]},
        )
        enabled_source._compute_column_usage = MagicMock(return_value=usage)
        enabled_source.metadata.get_by_name = MagicMock(return_value=None)

        with caplog.at_level("INFO"):
            enabled_source._ensure_column_usage_computed()

        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_REFS_DANGLING] == 1
        assert any("rep-1" in message and "NoSuchColumn" in message for message in caplog.messages)

    def test_repeated_dangling_ref_counted_and_logged_once(self, enabled_source, caplog):
        """The same (kind, table, name) can appear more than once in one report's
        dangling list (once per context - filter/projection/sort/...); it must be
        counted and logged once, not once per raw occurrence."""
        dataset, report = self._dataset_and_report(enabled_source)
        enabled_source.state.cache_semantic_model_definition(dataset.id, SemanticModelDefinition())
        enabled_source.state.cache_report_definition(report.id, ReportDefinition(format="legacy"))
        repeated_ref_a = FieldRef("column", "Ghost", "NoSuchColumn", context="projection")
        repeated_ref_b = FieldRef("column", "Ghost", "NoSuchColumn", context="filter@visual")
        distinct_ref = FieldRef("measure", "Ghost", "NoSuchMeasure")
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            dangling={"rep-1": [repeated_ref_a, repeated_ref_b, distinct_ref]},
        )
        enabled_source._compute_column_usage = MagicMock(return_value=usage)
        enabled_source.metadata.get_by_name = MagicMock(return_value=None)

        with caplog.at_level("INFO"):
            enabled_source._ensure_column_usage_computed()

        # 2 distinct (kind, table, name) triples, not 3 raw occurrences.
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_REFS_DANGLING] == 2
        dangling_log_lines = [m for m in caplog.messages if "NoSuchColumn" in m or "NoSuchMeasure" in m]
        assert len(dangling_log_lines) == 2


class TestDatamodelReportColumnLineage:
    def test_direct_and_measure_refs_both_emitted_never_merged(self, enabled_source):
        datamodel_entity = _dm(
            "11111111-1111-1111-1111-111111111111",
            "ds-1",
            "svc.ds-1",
            columns=[
                _col(
                    "Sales",
                    "svc.ds-1.Sales",
                    data_type=DataType.TABLE,
                    children=[_col("Amount", "svc.ds-1.Sales.Amount")],
                )
            ],
        )
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            per_visual={
                ("rep-1", "v1"): [
                    ColumnUse(table="Sales", column="Amount", via_measure=None),
                    ColumnUse(table="Sales", column="Amount", via_measure="Total Sales"),
                    ColumnUse(table="Sales", column="Amount", via_measure="Avg Sales"),
                ]
            },
        )
        chart_entity = MagicMock()
        chart_entity.fullyQualifiedName.root = "svc.rep-1_v1"
        enabled_source.metadata.get_by_name = MagicMock(return_value=chart_entity)

        with patch(
            "metadata.ingestion.source.dashboard.powerbi.metadata.fqn.build",
            side_effect=lambda *a, **kw: kw.get("chart_name"),
        ):
            column_lineage = enabled_source._create_datamodel_report_column_lineage(
                datamodel_entity=datamodel_entity, report_id="rep-1", usage=usage
            )

        # Same toColumn, three separate entries - never merged, per
        # LineageRepository.validateLineageDetails (java, ~718-751) which filters
        # columnsLineage per-entry and never dedupes on toColumn.
        assert len(column_lineage) == 3
        assert all(entry.toColumn.root == "svc.rep-1_v1" for entry in column_lineage)
        functions = sorted((str(e.function.root) if e.function else "") for e in column_lineage)
        assert functions == ["", "measure:Avg Sales", "measure:Total Sales"]

    def test_repeated_ref_within_one_visual_is_deduped(self, enabled_source):
        """The raw report JSON can repeat the same field ref inside one visual
        (e.g. a slicer's cachedFilterDisplayItems re-references the column it
        projects) - column_usage.py's per_visual list can carry that duplicate
        straight through; this must collapse to one ColumnLineage entry."""
        datamodel_entity = _dm(
            "11111111-1111-1111-1111-111111111111",
            "ds-1",
            "svc.ds-1",
            columns=[
                _col(
                    "Sales",
                    "svc.ds-1.Sales",
                    data_type=DataType.TABLE,
                    children=[_col("Amount", "svc.ds-1.Sales.Amount")],
                )
            ],
        )
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            per_visual={
                ("rep-1", "v1"): [
                    ColumnUse(table="Sales", column="Amount", via_measure=None),
                    # Same (table, column, via_measure) triple again - e.g. the
                    # slicer's own projection plus its cachedFilterDisplayItems.
                    ColumnUse(table="Sales", column="Amount", via_measure=None),
                ]
            },
        )
        chart_entity = MagicMock()
        chart_entity.fullyQualifiedName.root = "svc.rep-1_v1"
        enabled_source.metadata.get_by_name = MagicMock(return_value=chart_entity)

        with patch(
            "metadata.ingestion.source.dashboard.powerbi.metadata.fqn.build",
            side_effect=lambda *a, **kw: kw.get("chart_name"),
        ):
            column_lineage = enabled_source._create_datamodel_report_column_lineage(
                datamodel_entity=datamodel_entity, report_id="rep-1", usage=usage
            )

        assert len(column_lineage) == 1
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_REFS_RESOLVED] == 1

    def test_visual_from_other_report_excluded(self, enabled_source):
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            per_visual={("rep-OTHER", "v1"): [ColumnUse(table="Sales", column="Amount")]},
        )

        column_lineage = enabled_source._create_datamodel_report_column_lineage(
            datamodel_entity=datamodel_entity, report_id="rep-1", usage=usage
        )

        assert column_lineage == []

    def test_unresolved_column_counted_dangling_not_emitted(self, enabled_source):
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            per_visual={("rep-1", "v1"): [ColumnUse(table="Sales", column="Missing")]},
        )
        chart_entity = MagicMock()
        chart_entity.fullyQualifiedName.root = "svc.rep-1_v1"
        enabled_source.metadata.get_by_name = MagicMock(return_value=chart_entity)

        with patch(
            "metadata.ingestion.source.dashboard.powerbi.metadata.fqn.build",
            side_effect=lambda *a, **kw: kw.get("chart_name"),
        ):
            column_lineage = enabled_source._create_datamodel_report_column_lineage(
                datamodel_entity=datamodel_entity, report_id="rep-1", usage=usage
            )

        assert column_lineage == []
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_REFS_DANGLING] == 1


class TestDataflowModelColumnLineage:
    def test_mapped_and_unmapped_columns_counted(self, enabled_source):
        dataset = Dataset(id="ds-1", name="Sales Model")
        model_definition = SemanticModelDefinition(
            tables=[
                TmdlTable(
                    name="Sales",
                    columns=[
                        TmdlColumn(name="Amount", source_column="AMOUNT"),
                        TmdlColumn(name="Unmapped Col", source_column="NOT_IN_DATAFLOW"),
                    ],
                    partitions=[
                        TmdlPartition(
                            name="Sales",
                            source='Source{[workspaceId="ws1", dataflowId="df-1", entity="SalesEntity"]}[Data]',
                        )
                    ],
                )
            ]
        )
        enabled_source.state.cache_semantic_model_definition("ds-1", model_definition)

        datamodel_entity = _dm(
            "11111111-1111-1111-1111-111111111111",
            "ds-1",
            "svc.ds-1",
            columns=[
                _col(
                    "Sales",
                    "svc.ds-1.Sales",
                    data_type=DataType.TABLE,
                    children=[_col("Amount", "svc.ds-1.Sales.Amount")],
                )
            ],
        )
        dataflow_entity = _dm(
            "33333333-3333-3333-3333-333333333333",
            "df-1",
            "svc.df-1",
            columns=[
                _col(
                    "SalesEntity",
                    "svc.df-1.SalesEntity",
                    data_type=DataType.TABLE,
                    children=[_col("AMOUNT", "svc.df-1.SalesEntity.AMOUNT")],
                )
            ],
        )

        column_lineage = enabled_source._create_dataset_upstream_dataflow_column_lineage(
            dataset, datamodel_entity, dataflow_entity
        )

        assert len(column_lineage) == 1
        assert column_lineage[0].fromColumns[0].root == "svc.df-1.SalesEntity.AMOUNT"
        assert enabled_source.metric_values()[PowerbiSource.METRIC_DATAFLOW_MODEL_COLUMNS_MAPPED] == 1
        # "Unmapped Col" (sourceColumn NOT_IN_DATAFLOW) never matches an attribute.
        assert enabled_source.metric_values()[PowerbiSource.METRIC_DATAFLOW_MODEL_COLUMNS_UNMAPPED] == 1

    def test_partition_pointing_elsewhere_is_ignored(self, enabled_source):
        dataset = Dataset(id="ds-1", name="Sales Model")
        model_definition = SemanticModelDefinition(
            tables=[
                TmdlTable(
                    name="Sales",
                    columns=[TmdlColumn(name="Amount", source_column="AMOUNT")],
                    partitions=[
                        TmdlPartition(
                            name="Sales",
                            source='Source{[workspaceId="ws1", dataflowId="df-OTHER", entity="X"]}[Data]',
                        )
                    ],
                )
            ]
        )
        enabled_source.state.cache_semantic_model_definition("ds-1", model_definition)
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        dataflow_entity = _dm("33333333-3333-3333-3333-333333333333", "df-1", "svc.df-1")

        column_lineage = enabled_source._create_dataset_upstream_dataflow_column_lineage(
            dataset, datamodel_entity, dataflow_entity
        )

        assert column_lineage == []


class TestColumnLineageEdgeDedup:
    """`yield_dashboard_lineage_details` re-processes every report x
    db-service-prefix pair, so `create_datamodel_report_lineage` /
    `_emit_om_target_lineage` get called many times for the same edge. A
    column-carrying edge must be written once per workspace; a later re-visit
    must be skipped entirely (never rewritten columnless - `addLineage`
    replaces `lineageDetails` wholesale, so a columnless rewrite would erase the
    columns the first write set)."""

    def test_model_report_edge_written_once_across_redundant_calls(self, enabled_source):
        report = PowerBIReport(id="rep-1", name="R", datasetId="ds-1")
        datamodel_entity = _dm(
            "11111111-1111-1111-1111-111111111111",
            "ds-1",
            "svc.ds-1",
            columns=[
                _col(
                    "Sales",
                    "svc.ds-1.Sales",
                    data_type=DataType.TABLE,
                    children=[_col("Amount", "svc.ds-1.Sales.Amount")],
                )
            ],
        )
        report_entity = _dashboard("22222222-2222-2222-2222-222222222222", "rep-1", "svc.rep-1")
        chart_entity = MagicMock()
        chart_entity.fullyQualifiedName.root = "svc.rep-1_v1"
        usage = ModelColumnUsage(
            business_columns=frozenset(),
            used=frozenset(),
            unused=frozenset(),
            wholly_unused_tables=frozenset(),
            per_visual={("rep-1", "v1"): [ColumnUse(table="Sales", column="Amount")]},
        )
        enabled_source.state.cache_column_usage("ds-1", usage)

        def get_by_name(entity, fqn):
            if entity is Dashboard:
                return report_entity
            if entity is DashboardDataModel:
                return datamodel_entity
            return chart_entity

        enabled_source.metadata.get_by_name = MagicMock(side_effect=get_by_name)

        with patch(
            "metadata.ingestion.source.dashboard.powerbi.metadata.fqn.build",
            side_effect=lambda *a, **kw: kw.get("chart_name") or "some-fqn",
        ):
            # Two calls, as the redundant per-(report, db-service-prefix) loop
            # would produce for one report with two configured prefixes.
            first_pass = list(enabled_source.create_datamodel_report_lineage(None, report))
            second_pass = list(enabled_source.create_datamodel_report_lineage("some_prefix", report))

        first_requests = [e.right for e in first_pass if e.right is not None]
        second_requests = [e.right for e in second_pass if e.right is not None]
        assert len(first_requests) == 1
        assert first_requests[0].edge.lineageDetails.columnsLineage
        # The redundant second pass writes nothing at all for this edge.
        assert second_requests == []
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_EDGES_WRITTEN_MODEL_REPORT] == 1
        assert len(enabled_source._pending_column_lineage_edges) == 1  # pylint: disable=protected-access

    def test_dataflow_dataset_edge_written_once_across_redundant_calls(self, enabled_source):
        dataset = Dataset.model_validate(
            {
                "id": "ds-1",
                "name": "Sales Model",
                "upstreamDataflows": [{"groupId": "ws-1", "targetDataflowId": "df-1"}],
            }
        )
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        dataflow_entity = _dm("33333333-3333-3333-3333-333333333333", "df-1", "svc.df-1")
        enabled_source.metadata.get_by_name = MagicMock(return_value=dataflow_entity)
        enabled_source._create_dataset_upstream_dataflow_column_lineage = MagicMock(
            return_value=[
                ColumnLineage.model_validate(
                    {"fromColumns": ["svc.df-1.Sales.Amount"], "toColumn": "svc.ds-1.Sales.Amount"}
                )
            ]
        )

        with patch(
            "metadata.ingestion.source.dashboard.powerbi.metadata.fqn.build",
            return_value="some-fqn",
        ):
            first_pass = list(enabled_source.create_dataset_upstream_dataflow_lineage(dataset, datamodel_entity))
            second_pass = list(enabled_source.create_dataset_upstream_dataflow_lineage(dataset, datamodel_entity))

        first_requests = [e.right for e in first_pass if e.right is not None]
        second_requests = [e.right for e in second_pass if e.right is not None]
        assert len(first_requests) == 1
        assert second_requests == []
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_EDGES_WRITTEN_DATAFLOW_DATASET] == 1


class TestColumnLineageReadBackVerification:
    def test_verify_bypasses_get_lineage_edge_cache(self, enabled_source):
        """Must read via `self.metadata.client.get(...)` directly, never through
        `OpenMetadata.get_lineage_edge` - that method's `search_cache` is never
        invalidated on write and can hand back a pre-write value."""
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        report_entity = _dashboard("22222222-2222-2222-2222-222222222222", "rep-1", "svc.rep-1")
        enabled_source._track_column_lineage_edge(
            from_entity=datamodel_entity,
            to_entity=report_entity,
            edge_kind=PowerbiSource.EDGE_KIND_MODEL_REPORT,
            emitted_count=2,
        )
        enabled_source.metadata.get_lineage_edge = MagicMock(
            side_effect=AssertionError("must not use the cached get_lineage_edge")
        )
        enabled_source.metadata.get_suffix = MagicMock(return_value="/lineage")
        enabled_source.metadata.client = MagicMock()
        enabled_source.metadata.client.get.return_value = {
            "lineageDetails": {"columnsLineage": [{"toColumn": "x"}, {"toColumn": "y"}]}
        }

        enabled_source._verify_column_lineage_edges()

        enabled_source.metadata.client.get.assert_called_once()
        called_path = enabled_source.metadata.client.get.call_args.args[0]
        assert "getLineageEdge" in called_path
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_VERIFIED] == 2
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_ENTRIES_VERIFIED_MODEL_REPORT] == 2

    def test_dropped_by_server_counted_per_kind(self, enabled_source):
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        dataflow_entity = _dm("33333333-3333-3333-3333-333333333333", "df-1", "svc.df-1")
        enabled_source._track_column_lineage_edge(
            from_entity=dataflow_entity,
            to_entity=datamodel_entity,
            edge_kind=PowerbiSource.EDGE_KIND_DATAFLOW_DATASET,
            emitted_count=3,
        )
        enabled_source.metadata.get_suffix = MagicMock(return_value="/lineage")
        enabled_source.metadata.client = MagicMock()
        # Server kept only 1 of the 3 emitted entries.
        enabled_source.metadata.client.get.return_value = {"lineageDetails": {"columnsLineage": [{"toColumn": "x"}]}}

        enabled_source._verify_column_lineage_edges()

        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_VERIFIED] == 1
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_DROPPED_BY_SERVER] == 2
        assert enabled_source.metric_values()[PowerbiSource.METRIC_COLUMN_LINEAGE_ENTRIES_DROPPED_DATAFLOW_DATASET] == 2

    def test_pending_edges_drained_after_verification(self, enabled_source):
        datamodel_entity = _dm("11111111-1111-1111-1111-111111111111", "ds-1", "svc.ds-1")
        report_entity = _dashboard("22222222-2222-2222-2222-222222222222", "rep-1", "svc.rep-1")
        enabled_source._track_column_lineage_edge(
            from_entity=datamodel_entity,
            to_entity=report_entity,
            edge_kind=PowerbiSource.EDGE_KIND_MODEL_REPORT,
            emitted_count=1,
        )
        enabled_source.metadata.get_suffix = MagicMock(return_value="/lineage")
        enabled_source.metadata.client = MagicMock()
        enabled_source.metadata.client.get.return_value = {"lineageDetails": {"columnsLineage": [{"toColumn": "x"}]}}

        enabled_source._verify_column_lineage_edges()

        assert enabled_source._pending_column_lineage_edges == []  # pylint: disable=protected-access


class TestDeferredImportsRealModulesPresent:
    """The real parser modules exist in this checkout - the deferred-import wrappers
    must resolve to them, not silently degrade to the not-available-yet path."""

    def test_parse_report_definition_wrapper_uses_real_module(self, enabled_source):
        parts = {"report.json": json.dumps({"sections": []}).encode("utf-8")}

        result = enabled_source._parse_report_definition(parts)

        assert result is not None
        assert result.format == "legacy"
        assert enabled_source.metric_values()[PowerbiSource.METRIC_REPORTS_FORMAT_LEGACY] == 1

    def test_parse_semantic_model_definition_wrapper_uses_real_module(self, enabled_source):
        parts = {"definition/tables/Sales.tmdl": b"table Sales\n"}

        result = enabled_source._parse_semantic_model_definition(parts)

        assert result is not None
        assert [t.name for t in result.tables] == ["Sales"]

    def test_map_columns_to_dataflow_wrapper_uses_real_module(self, enabled_source):
        source_ref = DataflowSourceRef(workspace_id="ws1", dataflow_id="df1", entity="Sales")

        mapping = enabled_source._map_columns_to_dataflow(["AMOUNT"], source_ref, ["AMOUNT"])

        assert mapping == ColumnMapping(mapped={"AMOUNT": "AMOUNT"}, unmapped=[])

    def test_parse_dataflow_source_ref_wrapper_uses_real_module(self, enabled_source):
        result = enabled_source._parse_dataflow_source_ref(
            'Source{[workspaceId="ws1", dataflowId="df1", entity="Sales"]}[Data]'
        )
        assert result == DataflowSourceRef(workspace_id="ws1", dataflow_id="df1", entity="Sales")


class TestReportDefinitionEndToEnd:
    """One genuine bytes-in test through the real `report_definition` parser."""

    def test_legacy_report_json_produces_one_data_visual(self, enabled_source):
        report_json = {
            "sections": [
                {
                    "name": "page1",
                    "displayName": "Page One",
                    "visualContainers": [
                        {
                            "config": {
                                "name": "v1",
                                "singleVisual": {
                                    "visualType": "barChart",
                                    "prototypeQuery": {
                                        "From": [{"Name": "s", "Entity": "Sales"}],
                                        "Select": [
                                            {
                                                "Column": {
                                                    "Expression": {"SourceRef": {"Source": "s"}},
                                                    "Property": "Amount",
                                                },
                                                "Name": "Sales.Amount",
                                            }
                                        ],
                                    },
                                },
                            }
                        }
                    ],
                }
            ]
        }
        parts = {"report.json": json.dumps(report_json).encode("utf-8")}

        report_definition = parse_report_definition(parts)

        assert report_definition is not None
        assert report_definition.format == "legacy"
        assert len(report_definition.visuals) == 1
        visual = report_definition.visuals[0]
        assert visual.is_data_visual is True
        assert visual.visual_id == "v1"
        assert visual.page_id == "page1"
        assert visual.refs[0].table == "Sales"
        assert visual.refs[0].name == "Amount"

        # And that this connects to chart creation end to end.
        report = PowerBIReport(id="rep-1", name="R")
        enabled_source.state.add_filtered_dashboard(report)
        enabled_source._get_report_definition = MagicMock(return_value=report_definition)
        charts = [e.right for e in enabled_source.yield_dashboard_chart(Group(id="ws-1", name="x")) if e.right]
        assert len(charts) == 1
        assert charts[0].name.root == "rep-1_v1"


class TestTmdlEndToEnd:
    """One genuine bytes-in test through the real `tmdl` parser."""

    def test_tmdl_text_produces_columns_measures_and_dataflow_partition(self, enabled_source):
        tmdl_text = (
            "table Sales\n"
            "\tcolumn Amount\n"
            "\t\tdataType: int64\n"
            "\t\tsourceColumn: AMOUNT\n"
            "\n"
            "\tmeasure Total = SUM(Sales[Amount])\n"
            "\n"
            "\tpartition Sales = m\n"
            "\t\tmode: import\n"
            '\t\tsource = Source{[workspaceId="ws1", dataflowId="df-1", entity="SalesEntity"]}[Data]\n'
        )
        parts = {"definition/tables/Sales.tmdl": tmdl_text.encode("utf-8")}

        model_definition = parse_tmdl(parts)

        assert len(model_definition.tables) == 1
        table = model_definition.tables[0]
        assert table.name == "Sales"
        assert table.columns[0].name == "Amount"
        assert table.columns[0].source_column == "AMOUNT"
        assert table.measures[0].expression == "SUM(Sales[Amount])"
        partition_source = table.partitions[0].source
        assert partition_source is not None
        assert 'dataflowId="df-1"' in partition_source

        # And that the adapter turns this into a working PowerBiTable.
        powerbi_table = enabled_source._tmdl_table_to_powerbi_table(table)
        assert powerbi_table is not None
        assert powerbi_table.name == "Sales"
        assert powerbi_table.source is not None
        assert powerbi_table.source[0].expression == partition_source
