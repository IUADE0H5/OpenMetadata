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
Resolves which physical semantic-model columns are actually used by a set of reports,
per the adopted "used" rule (project docs, section 5):

- Direct uses: visual projections/sorts/objects refs and filters at report, page and
  visual level, plus tooltip page bindings.
- Transitive uses: a used measure pulls in the columns (and further measures) its DAX
  touches; a used calculated column pulls in its own DAX dependencies the same way.
- A `sortByColumn` target of a used column is used.
- A relationship key is used only if the relationship is *traversed*: both endpoint
  tables already have at least one used *business* column. A table with zero business
  columns (every auto-date table, by construction) can never satisfy that on its own
  side, so a relationship into an auto-date table never marks the business-side key
  used by itself.
- Relationship traversal and `sortByColumn` are evaluated exactly once, against the
  base (direct + transitive) used set -- deliberately not a fixpoint. A column either
  rule adds never itself unlocks a further relationship or sortBy target. (Calculated
  columns still expand to their own fixpoint first, since a chain of calculated
  columns genuinely needs multiple hops to resolve; only the two structural rules are
  single-pass.)
- Everything else among the business columns (i.e. not belonging to an auto-date table)
  is unused. A model can back several reports; usage is the union across all of them.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

from metadata.ingestion.source.dashboard.powerbi.dax import DaxReferences, extract_dax_references
from metadata.ingestion.source.dashboard.powerbi.report_definition import FieldRef, ReportDefinition
from metadata.ingestion.source.dashboard.powerbi.tmdl import SemanticModelDefinition

ColumnKey = tuple[str, str]
VisualKey = tuple[str, str]

# Every counter this module reports, seeded at 0 so a diff against a prior run never
# has to guess whether a missing key means "zero" or "not measured yet".
_COUNTER_KEYS = (
    "reports",
    "visuals_data",
    "visuals_non_data",
    "direct_column_refs",
    "direct_measure_refs",
    "hierarchy_level_refs",
    "dangling_refs",
    "measures_transitive",
    "calculated_columns_expanded",
    "relationships_traversed",
    "sortby_columns_added",
    "dax_unresolved",
)

# Fixpoint safety cap: real models have at most a few hundred tables/relationships, so
# this is never reached in practice -- it exists to turn a modelling bug into a
# truncated-but-finite result instead of an infinite loop.
_MAX_FIXPOINT_ITERATIONS = 50


@dataclass(frozen=True)
class ColumnUse:
    table: str
    column: str
    # The visual-facing measure this column was reached through, if any. None for a
    # direct column/hierarchy-level use. Two different measures reaching the same
    # physical column produce two separate ColumnUse entries, never merged.
    via_measure: str | None = None


@dataclass
class ModelColumnUsage:
    business_columns: frozenset[ColumnKey]
    used: frozenset[ColumnKey]
    unused: frozenset[ColumnKey]
    wholly_unused_tables: frozenset[str]
    per_visual: dict[VisualKey, list[ColumnUse]] = field(default_factory=dict)
    dangling: dict[str, list[FieldRef]] = field(default_factory=dict)
    dax_unresolved: set[str] = field(default_factory=set)
    counters: dict[str, int] = field(default_factory=dict)


@dataclass
class _ModelIndex:
    """Precomputed, read-only lookups derived once from the model, shared by every
    step of resolution."""

    all_columns: set[ColumnKey]
    business_columns: frozenset[ColumnKey]
    measure_index: set[ColumnKey]
    hierarchies: dict[str, dict[str, dict[str, str]]]
    sort_by: dict[ColumnKey, str]
    measure_deps: dict[ColumnKey, DaxReferences]
    calc_column_deps: dict[ColumnKey, DaxReferences]
    # (business_table, business_column) -> {level_name: auto_date_table_column_key}.
    # Built from each column's TMDL `variation` block; used only to additionally
    # resolve chart lineage for the internal auto-date table a variation drills into
    # (report_definition.py has no model access to look that table up itself).
    variation_levels: dict[ColumnKey, dict[str, ColumnKey]]


def _build_index(model: SemanticModelDefinition) -> _ModelIndex:
    return _ModelIndex(
        all_columns={(table.name, column.name) for table in model.tables for column in table.columns},
        business_columns=frozenset(
            (table.name, column.name) for table in model.tables if not table.is_auto_date for column in table.columns
        ),
        measure_index={(table.name, measure.name) for table in model.tables for measure in table.measures},
        hierarchies={
            table.name: {
                hierarchy.name: {level.name: level.column for level in hierarchy.levels if level.column is not None}
                for hierarchy in table.hierarchies
            }
            for table in model.tables
        },
        sort_by={
            (table.name, column.name): column.sort_by_column
            for table in model.tables
            for column in table.columns
            if column.sort_by_column
        },
        measure_deps={
            (table.name, measure.name): extract_dax_references(measure.expression, model, table.name)
            for table in model.tables
            for measure in table.measures
        },
        calc_column_deps={
            (table.name, column.name): extract_dax_references(column.expression, model, table.name)
            for table in model.tables
            for column in table.columns
            if column.expression
        },
        variation_levels=_build_variation_levels(model),
    )


def _build_variation_levels(model: SemanticModelDefinition) -> dict[ColumnKey, dict[str, ColumnKey]]:
    tables_by_name = {table.name: table for table in model.tables}
    result: dict[ColumnKey, dict[str, ColumnKey]] = {}
    for table in model.tables:
        for column in table.columns:
            for variation in column.variations:
                if variation.default_hierarchy is None:
                    continue
                auto_date_table_name, hierarchy_name = variation.default_hierarchy
                auto_date_table = tables_by_name.get(auto_date_table_name)
                if auto_date_table is None:
                    continue
                hierarchy = next((h for h in auto_date_table.hierarchies if h.name == hierarchy_name), None)
                if hierarchy is None:
                    continue
                levels = {
                    level.name: (auto_date_table_name, level.column)
                    for level in hierarchy.levels
                    if level.column is not None
                }
                if levels:
                    result[(table.name, column.name)] = levels
    return result


def _resolve_ref(ref: FieldRef, index: _ModelIndex) -> tuple[str, ColumnKey] | None:
    """Resolves a FieldRef to ("column", key) or ("measure", key) against the model,
    applying the same kind/actual-model cross-check fallback as the reference
    implementation: a ref tagged Column that is actually a measure (or vice versa)
    still resolves, since the JSON's own tag can lag the model."""
    if ref.kind == "hierarchy_level":
        if ref.hierarchy is None:
            # A variation-based ref: report_definition.py already resolved it straight
            # to the physical (table, column) the auto-date hierarchy was built on --
            # just check that column genuinely exists.
            key = (ref.table, ref.name)
            return ("column", key) if key in index.all_columns else None
        column = index.hierarchies.get(ref.table, {}).get(ref.hierarchy, {}).get(ref.name)
        return ("column", (ref.table, column)) if column is not None else None
    key = (ref.table, ref.name)
    if key in index.measure_index:
        return ("measure", key)
    if key in index.all_columns:
        return ("column", key)
    return None


@dataclass
class _DirectUsageResult:
    used: set[ColumnKey]
    per_visual: dict[VisualKey, list[ColumnUse]]
    dangling: dict[str, list[FieldRef]]
    dax_unresolved: set[str]
    measures_seen: set[ColumnKey]


def _measure_closure(
    start: ColumnKey,
    measure_deps: dict[ColumnKey, DaxReferences],
    all_measures_seen: set[ColumnKey],
    dax_unresolved: set[str],
) -> set[ColumnKey]:
    """Returns the full transitive column closure reached from `start`. Cycle-guarded
    with a *local* seen set, deliberately not `all_measures_seen`: that set is shared
    across every ref in a report purely to count distinct measures reached overall
    (`measures_transitive`), and gating traversal on it here would make a call's
    returned columns depend on which other ref happened to explore the same measure
    subgraph first -- correct for the report-wide `used` set (a union anyway) but
    silently incomplete for any one visual's own per-visual attribution."""
    columns: set[ColumnKey] = set()
    local_seen: set[ColumnKey] = set()
    stack = [start]
    while stack:
        key = stack.pop()
        if key in local_seen:
            continue
        local_seen.add(key)
        all_measures_seen.add(key)
        deps = measure_deps.get(key)
        if deps is None:
            continue
        columns |= deps.columns
        dax_unresolved.update(deps.unresolved)
        stack.extend(deps.measures)
    return columns


def _collect_direct_usage(
    reports: Mapping[str, ReportDefinition], index: _ModelIndex, counters: dict[str, int]
) -> _DirectUsageResult:
    result = _DirectUsageResult(used=set(), per_visual={}, dangling={}, dax_unresolved=set(), measures_seen=set())

    def apply_ref(ref: FieldRef, report_id: str, visual_key: VisualKey | None) -> None:
        resolution = _resolve_ref(ref, index)
        if resolution is None:
            result.dangling.setdefault(report_id, []).append(ref)
            counters["dangling_refs"] += 1
            return
        kind, key = resolution
        if kind == "column":
            metric = "hierarchy_level_refs" if ref.kind == "hierarchy_level" else "direct_column_refs"
            counters[metric] += 1
            result.used.add(key)
            if visual_key is not None:
                result.per_visual.setdefault(visual_key, []).append(ColumnUse(key[0], key[1]))
            if ref.kind == "hierarchy_level" and ref.hierarchy is None and ref.variation_level is not None:
                # A variation ref resolves to its business column above (needed for
                # used_5); the internal auto-date table's own same-named column is a
                # second, separate physical touch for chart lineage only -- it's never
                # a business column, so it can't affect used_5/unused_5 either way.
                auto_date_key = index.variation_levels.get(key, {}).get(ref.variation_level)
                if auto_date_key is not None:
                    result.used.add(auto_date_key)
                    if visual_key is not None:
                        result.per_visual[visual_key].append(ColumnUse(auto_date_key[0], auto_date_key[1]))
        else:
            counters["direct_measure_refs"] += 1
            reached = _measure_closure(key, index.measure_deps, result.measures_seen, result.dax_unresolved)
            result.used.update(reached)
            if visual_key is not None:
                entries = result.per_visual.setdefault(visual_key, [])
                entries.extend(ColumnUse(col[0], col[1], via_measure=key[1]) for col in reached)

    for report_id, report in reports.items():
        counters["reports"] += 1
        for ref in report.report_refs:
            apply_ref(ref, report_id, None)
        for page_refs in report.page_refs.values():
            for ref in page_refs:
                apply_ref(ref, report_id, None)
        for visual in report.visuals:
            counters["visuals_data" if visual.is_data_visual else "visuals_non_data"] += 1
            visual_key = (report_id, visual.visual_id)
            for ref in visual.refs:
                apply_ref(ref, report_id, visual_key)

    return result


def _traversed_relationships(
    model: SemanticModelDefinition, used: set[ColumnKey], index: _ModelIndex
) -> tuple[set[ColumnKey], set[str]]:
    """A relationship is traversed only when both endpoint tables already have at
    least one used business column; an auto-date table has none, by construction, so
    it can never satisfy this on its own side."""
    added: set[ColumnKey] = set()
    traversed: set[str] = set()
    used_business_tables = {table for table, column in used if (table, column) in index.business_columns}
    for relationship in model.relationships:
        if relationship.from_table not in used_business_tables or relationship.to_table not in used_business_tables:
            continue
        from_key = (relationship.from_table, relationship.from_column)
        to_key = (relationship.to_table, relationship.to_column)
        added.update(key for key in (from_key, to_key) if key not in used)
        traversed.add(f"{from_key[0]}.{from_key[1]}->{to_key[0]}.{to_key[1]}")
    return added, traversed


def _sortby_additions(used: set[ColumnKey], index: _ModelIndex) -> set[ColumnKey]:
    added = set()
    for table, column in used:
        sort_target = index.sort_by.get((table, column))
        if sort_target and (table, sort_target) not in used:
            added.add((table, sort_target))
    return added


def _calculated_column_additions(
    used: set[ColumnKey], index: _ModelIndex, measures_seen: set[ColumnKey], dax_unresolved: set[str]
) -> tuple[set[ColumnKey], int]:
    added: set[ColumnKey] = set()
    expanded = 0
    for key in used:
        deps = index.calc_column_deps.get(key)
        if deps is None:
            continue
        expanded += 1
        added.update(column_key for column_key in deps.columns if column_key not in used)
        for measure_key in deps.measures:
            added.update(
                column_key
                for column_key in _measure_closure(measure_key, index.measure_deps, measures_seen, dax_unresolved)
                if column_key not in used
            )
        dax_unresolved.update(deps.unresolved)
    return added, expanded


def _expand_calculated_columns(
    used: set[ColumnKey],
    index: _ModelIndex,
    measures_seen: set[ColumnKey],
    dax_unresolved: set[str],
    counters: dict[str, int],
) -> None:
    """Fixpoint over calculated-column DAX deps only: a calculated column can depend on
    another calculated column, so this alone needs to iterate until stable. This is
    part of computing the "direct + transitive" base used set, strictly before the
    structural rules below ever run."""
    for _ in range(_MAX_FIXPOINT_ITERATIONS):
        added, expanded = _calculated_column_additions(used, index, measures_seen, dax_unresolved)
        counters["calculated_columns_expanded"] += expanded
        if not added:
            break
        used |= added


def _apply_structural_rules_once(
    model: SemanticModelDefinition,
    used: set[ColumnKey],
    index: _ModelIndex,
    counters: dict[str, int],
) -> set[ColumnKey]:
    """Relationship traversal and sortByColumn targets are evaluated exactly once
    against the base (direct + transitive) used set -- deliberately not a fixpoint. A
    column a relationship or sortBy adds here never unlocks a further relationship or
    sortBy target; that keeps unused_lenient <= unused_section5 <= unused_strict for
    every model, which a fixpoint here would not guarantee."""
    relationship_added, traversed = _traversed_relationships(model, used, index)
    counters["relationships_traversed"] = len(traversed)

    sortby_added = _sortby_additions(used, index)
    counters["sortby_columns_added"] = len(sortby_added)

    return relationship_added | sortby_added


def resolve_column_usage(model: SemanticModelDefinition, reports: Mapping[str, ReportDefinition]) -> ModelColumnUsage:
    counters: dict[str, int] = dict.fromkeys(_COUNTER_KEYS, 0)
    index = _build_index(model)

    direct = _collect_direct_usage(reports, index, counters)
    counters["measures_transitive"] = len(direct.measures_seen)

    used = direct.used
    _expand_calculated_columns(used, index, direct.measures_seen, direct.dax_unresolved, counters)
    used |= _apply_structural_rules_once(model, used, index, counters)
    counters["dax_unresolved"] = len(direct.dax_unresolved)

    final_used = index.business_columns & frozenset(used)
    unused = index.business_columns - final_used
    wholly_unused_tables = frozenset(
        table.name
        for table in model.tables
        if not table.is_auto_date
        and any((table.name, column.name) in index.business_columns for column in table.columns)
        and not any((table.name, column.name) in final_used for column in table.columns)
    )

    return ModelColumnUsage(
        business_columns=index.business_columns,
        used=final_used,
        unused=unused,
        wholly_unused_tables=wholly_unused_tables,
        per_visual=direct.per_visual,
        dangling=direct.dangling,
        dax_unresolved=direct.dax_unresolved,
        counters=counters,
    )
