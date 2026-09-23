#  Copyright 2023 Collate
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
Tests for metadata.ingestion.source.dashboard.powerbi.column_usage
"""

from metadata.ingestion.source.dashboard.powerbi.column_usage import resolve_column_usage
from metadata.ingestion.source.dashboard.powerbi.report_definition import (
    FieldRef,
    ReportDefinition,
    VisualDefinition,
)
from metadata.ingestion.source.dashboard.powerbi.tmdl import (
    SemanticModelDefinition,
    TmdlColumn,
    TmdlHierarchy,
    TmdlHierarchyLevel,
    TmdlMeasure,
    TmdlRelationship,
    TmdlTable,
    TmdlVariation,
)


def _model() -> SemanticModelDefinition:
    sales = TmdlTable(
        name="Sales",
        columns=[
            TmdlColumn(name="Amount"),
            TmdlColumn(name="CustomerId"),
            TmdlColumn(name="OrderDate"),
            TmdlColumn(name="Region"),
            TmdlColumn(name="RegionSort"),
            TmdlColumn(name="RunningTotal", expression="SUMX(Sales, Sales[Amount])"),
        ],
        measures=[
            TmdlMeasure(name="Total Sales", expression="SUM(Sales[Amount])"),
            TmdlMeasure(name="Sales Rank", expression="RANKX(ALL(Sales), [Total Sales])"),
            TmdlMeasure(name="Shared", expression="SUM(Sales[Amount])"),
            TmdlMeasure(name="A", expression="[Shared] + 1"),
            TmdlMeasure(name="B", expression="[Shared] + 2"),
        ],
        hierarchies=[
            TmdlHierarchy(
                name="Geo Hierarchy",
                levels=[TmdlHierarchyLevel(name="Region", column="Region")],
            )
        ],
    )
    sales.columns[3].sort_by_column = "RegionSort"  # Region -> RegionSort
    sales.columns[2].variations = [
        TmdlVariation(
            name="Variation",
            is_default=True,
            default_hierarchy=("LocalDateTable_x", "Date Hierarchy"),
        )
    ]
    customer = TmdlTable(
        name="Customer",
        columns=[TmdlColumn(name="CustomerId"), TmdlColumn(name="Name"), TmdlColumn(name="Country")],
    )
    unused_table = TmdlTable(name="Budget", columns=[TmdlColumn(name="Value")])
    auto_date = TmdlTable(
        name="LocalDateTable_x",
        is_auto_date=True,
        columns=[TmdlColumn(name="Date"), TmdlColumn(name="Year")],
        hierarchies=[
            TmdlHierarchy(
                name="Date Hierarchy",
                levels=[TmdlHierarchyLevel(name="Year", column="Year")],
            )
        ],
    )
    model = SemanticModelDefinition(tables=[sales, customer, unused_table, auto_date])
    model.relationships = [
        TmdlRelationship("Sales", "CustomerId", "Customer", "CustomerId"),
        TmdlRelationship("Sales", "OrderDate", "LocalDateTable_x", "Date"),
    ]
    return model


def _ref(table, name, kind="column", context="projection", hierarchy=None, variation_level=None) -> FieldRef:
    return FieldRef(
        kind=kind, table=table, name=name, hierarchy=hierarchy, context=context, variation_level=variation_level
    )


def _report(visuals=None, report_refs=None, page_refs=None, fmt="pbir") -> ReportDefinition:
    return ReportDefinition(
        format=fmt,
        visuals=visuals or [],
        report_refs=report_refs or [],
        page_refs=page_refs or {},
    )


def _visual(visual_id, refs, is_data_visual=True, page_id="Page1") -> VisualDefinition:
    return VisualDefinition(
        visual_id=visual_id,
        page_id=page_id,
        page_display_name=None,
        visual_type="columnChart",
        title=None,
        refs=refs,
        is_data_visual=is_data_visual,
    )


class TestBusinessColumnsAndUnused:
    def test_business_columns_exclude_auto_date_table(self):
        usage = resolve_column_usage(_model(), {})
        assert ("LocalDateTable_x", "Date") not in usage.business_columns
        assert ("LocalDateTable_x", "Year") not in usage.business_columns
        assert ("Sales", "Amount") in usage.business_columns

    def test_no_reports_means_everything_unused(self):
        usage = resolve_column_usage(_model(), {})
        assert usage.used == frozenset()
        assert usage.unused == usage.business_columns

    def test_wholly_unused_table_when_no_column_used(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert "Budget" in usage.wholly_unused_tables
        assert "Sales" not in usage.wholly_unused_tables

    def test_auto_date_table_never_counted_as_wholly_unused(self):
        usage = resolve_column_usage(_model(), {})
        assert "LocalDateTable_x" not in usage.wholly_unused_tables


class TestDirectUsage:
    def test_direct_column_projection_is_used(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "Amount") in usage.used

    def test_report_level_filter_is_used(self):
        report = _report(report_refs=[_ref("Customer", "Country", context="filter@report")])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Customer", "Country") in usage.used

    def test_page_level_filter_is_used(self):
        report = _report(page_refs={"Page1": [_ref("Customer", "Name", context="filter@page")]})
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Customer", "Name") in usage.used

    def test_visual_filter_is_used(self):
        report = _report(visuals=[_visual("v1", [_ref("Customer", "Name", context="filter@visual")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Customer", "Name") in usage.used

    def test_hierarchy_level_ref_resolves_to_its_column(self):
        ref = _ref("Sales", "Region", kind="hierarchy_level", hierarchy="Geo Hierarchy", context="projection")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "Region") in usage.used

    def test_dangling_hierarchy_level_ref_is_collected(self):
        ref = _ref("Sales", "NoSuchLevel", kind="hierarchy_level", hierarchy="Geo Hierarchy")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert usage.dangling["r1"] == [ref]

    def test_dangling_column_ref_is_collected_and_counted(self):
        ref = _ref("Sales", "DoesNotExist")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert usage.dangling["r1"] == [ref]
        assert usage.counters["dangling_refs"] == 1

    def test_dangling_table_ref_is_collected(self):
        ref = _ref("NoSuchTable", "Whatever")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert usage.dangling["r1"] == [ref]

    def test_ref_tagged_measure_that_is_actually_a_column_still_resolves(self):
        # Mirrors the reference implementation's cross-kind fallback: the report JSON's
        # own Column/Measure tag can be stale relative to the model.
        ref = _ref("Sales", "Amount", kind="measure")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "Amount") in usage.used

    def test_ref_tagged_column_that_is_actually_a_measure_still_resolves(self):
        ref = _ref("Sales", "Total Sales", kind="column")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "Amount") in usage.used  # via the Total Sales measure closure


class TestMeasureTransitiveUsage:
    def test_measure_ref_pulls_in_its_dax_columns(self):
        ref = _ref("Sales", "Total Sales", kind="measure")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "Amount") in usage.used

    def test_transitive_measure_chain(self):
        # Sales Rank -> RANKX(ALL(Sales), [Total Sales]) -> Total Sales -> Sales[Amount]
        ref = _ref("Sales", "Sales Rank", kind="measure")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "Amount") in usage.used
        assert usage.counters["measures_transitive"] == 2  # Sales Rank + Total Sales

    def test_measure_chain_with_a_cycle_terminates(self):
        model = _model()
        model.tables[0].measures.append(TmdlMeasure(name="A", expression="[B] + Sales[Amount]"))
        model.tables[0].measures.append(TmdlMeasure(name="B", expression="[A]"))
        ref = _ref("Sales", "A", kind="measure")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(model, {"r1": report})  # must not hang
        assert ("Sales", "Amount") in usage.used

    def test_via_measure_is_the_visual_facing_measure_not_a_deeper_hop(self):
        ref = _ref("Sales", "Sales Rank", kind="measure")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        uses = usage.per_visual[("r1", "v1")]
        amount_uses = [u for u in uses if u.table == "Sales" and u.column == "Amount"]
        assert amount_uses and amount_uses[0].via_measure == "Sales Rank"

    def test_same_column_via_two_measures_produces_two_entries(self):
        model = _model()
        model.tables[0].measures.append(TmdlMeasure(name="Amount Twice", expression="Sales[Amount] * 2"))
        refs = [_ref("Sales", "Total Sales", kind="measure"), _ref("Sales", "Amount Twice", kind="measure")]
        report = _report(visuals=[_visual("v1", refs)])
        usage = resolve_column_usage(model, {"r1": report})
        uses = usage.per_visual[("r1", "v1")]
        amount_uses = [u for u in uses if u.table == "Sales" and u.column == "Amount"]
        assert len(amount_uses) == 2
        assert {u.via_measure for u in amount_uses} == {"Total Sales", "Amount Twice"}


class TestCalculatedColumnTransitiveUsage:
    def test_calculated_column_pulls_in_its_dax_dependency(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "RunningTotal")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "RunningTotal") in usage.used
        assert ("Sales", "Amount") in usage.used
        assert usage.counters["calculated_columns_expanded"] >= 1


class TestSection5StructuralRules:
    def test_sortby_target_of_used_column_is_used(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Region")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "RegionSort") in usage.used

    def test_sortby_target_not_used_when_source_column_not_used(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "RegionSort") not in usage.used

    def test_relationship_traversed_when_both_sides_have_used_business_columns(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount"), _ref("Customer", "Name")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "CustomerId") in usage.used
        assert ("Customer", "CustomerId") in usage.used
        assert usage.counters["relationships_traversed"] == 1

    def test_relationship_not_traversed_when_only_one_side_used(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "CustomerId") not in usage.used
        assert ("Customer", "CustomerId") not in usage.used

    def test_relationship_to_auto_date_table_never_marks_key_used(self):
        # Sales<->LocalDateTable_x relationship: the date table has zero business
        # columns, so it can never satisfy "both sides have a used business column",
        # no matter what else on Sales is used.
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount"), _ref("Customer", "Name")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "OrderDate") not in usage.used

    def test_everything_else_among_business_columns_is_unused(self):
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Amount")])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Budget", "Value") in usage.unused

    def test_usage_is_the_union_across_two_reports(self):
        report_a = _report(visuals=[_visual("v1", [_ref("Sales", "Amount")])])
        report_b = _report(visuals=[_visual("v1", [_ref("Customer", "Name")])])
        usage = resolve_column_usage(_model(), {"a": report_a, "b": report_b})
        assert ("Sales", "Amount") in usage.used
        assert ("Customer", "Name") in usage.used
        # The union across both reports satisfies the relationship traversal too.
        assert ("Sales", "CustomerId") in usage.used


class TestVisualClassificationCounters:
    def test_data_and_non_data_visuals_are_counted_separately(self):
        report = _report(
            visuals=[
                _visual("v1", [_ref("Sales", "Amount")], is_data_visual=True),
                _visual("v2", [], is_data_visual=False),
            ]
        )
        usage = resolve_column_usage(_model(), {"r1": report})
        assert usage.counters["visuals_data"] == 1
        assert usage.counters["visuals_non_data"] == 1

    def test_all_declared_counters_are_seeded_even_when_unused(self):
        usage = resolve_column_usage(_model(), {})
        # A no-report run should never omit a counter key just because nothing hit it.
        assert usage.counters["dax_unresolved"] == 0
        assert usage.counters["relationships_traversed"] == 0
        assert usage.counters["measures_transitive"] == 0
        assert usage.counters["sortby_columns_added"] == 0


class TestDaxUnresolvedPropagation:
    def test_unresolved_dax_ref_in_a_used_measure_is_surfaced(self):
        model = _model()
        model.tables[0].measures.append(TmdlMeasure(name="Broken", expression="[GhostMeasure] + 1"))
        report = _report(visuals=[_visual("v1", [_ref("Sales", "Broken", kind="measure")])])
        usage = resolve_column_usage(model, {"r1": report})
        assert "[GhostMeasure]" in usage.dax_unresolved


class TestVariationHierarchyLevel:
    def test_resolves_to_business_column_for_used_5(self):
        ref = _ref("Sales", "OrderDate", kind="hierarchy_level", variation_level="Year")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        assert ("Sales", "OrderDate") in usage.used
        # The auto-date table itself is never a business column, used_5 or not.
        assert ("LocalDateTable_x", "Year") not in usage.used

    def test_also_touches_the_auto_date_table_column_for_chart_lineage(self):
        ref = _ref("Sales", "OrderDate", kind="hierarchy_level", variation_level="Year")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        uses = {(u.table, u.column) for u in usage.per_visual[("r1", "v1")]}
        assert ("Sales", "OrderDate") in uses
        assert ("LocalDateTable_x", "Year") in uses

    def test_unresolvable_variation_level_produces_no_auto_date_touch(self):
        # The column has no variation targeting a "Month" level in this fixture.
        ref = _ref("Sales", "OrderDate", kind="hierarchy_level", variation_level="Month")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        uses = {(u.table, u.column) for u in usage.per_visual[("r1", "v1")]}
        assert uses == {("Sales", "OrderDate")}

    def test_column_without_a_variation_at_all_is_unaffected(self):
        ref = _ref("Sales", "Amount", kind="hierarchy_level", variation_level="Year")
        report = _report(visuals=[_visual("v1", [ref])])
        usage = resolve_column_usage(_model(), {"r1": report})
        uses = {(u.table, u.column) for u in usage.per_visual[("r1", "v1")]}
        assert uses == {("Sales", "Amount")}


class TestPerVisualMeasureClosureIsolation:
    def test_two_visuals_reaching_a_shared_submeasure_each_get_the_full_closure(self):
        # Regression: the measure-closure cycle guard used to be a single set shared
        # across every ref in the report, so whichever visual explored the shared
        # "Shared" submeasure first silently starved every later visual's own
        # per-visual attribution of the columns reached through it -- even though the
        # report-wide used_5 total stayed correct (a union either way).
        report = _report(
            visuals=[
                _visual("v1", [_ref("Sales", "A", kind="measure")]),
                _visual("v2", [_ref("Sales", "B", kind="measure")]),
            ]
        )
        usage = resolve_column_usage(_model(), {"r1": report})
        v1_cols = {(u.table, u.column) for u in usage.per_visual[("r1", "v1")]}
        v2_cols = {(u.table, u.column) for u in usage.per_visual[("r1", "v2")]}
        assert ("Sales", "Amount") in v1_cols
        assert ("Sales", "Amount") in v2_cols

    def test_measures_transitive_counter_still_deduplicates_across_visuals(self):
        report = _report(
            visuals=[
                _visual("v1", [_ref("Sales", "A", kind="measure")]),
                _visual("v2", [_ref("Sales", "B", kind="measure")]),
            ]
        )
        usage = resolve_column_usage(_model(), {"r1": report})
        # A, B and Shared are each counted once even though Shared is reached twice.
        assert usage.counters["measures_transitive"] == 3
