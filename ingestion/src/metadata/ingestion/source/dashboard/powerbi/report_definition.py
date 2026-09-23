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
Parses a Power BI report's `getDefinition` payload (legacy `report.json` or the newer
PBIR `definition/pages/**` layout) into the field references each visual, page and the
report itself make: which model columns, measures and hierarchy levels are actually
touched. This is the pure-parsing half of column-usage resolution -- it never talks to
the Fabric API and never resolves against a semantic model; see `column_usage.py` for
that.
"""

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

FieldKind = Literal["column", "measure", "hierarchy_level"]

# Visual types that are never data-bound. This is a hint only -- the actual
# is_data_visual decision is made on whether any FieldRef was collected, so a
# slicer or a custom visual with fields still counts even if its type isn't listed.
NON_DATA_VISUAL_TYPES = {
    "shape",
    "textbox",
    "image",
    "actionButton",
    "bookmarkNavigator",
    "pageNavigator",
}

# Query-expression discriminator tags whose shape we actively resolve to a FieldRef.
_REF_PRODUCING_KINDS = {"Column", "Measure", "PropertyVariationSource", "HierarchyLevel"}
# Discriminator tags we recognize as filter/aggregation plumbing that never resolves to
# a physical field by itself (their operands, walked recursively, may still contain one).
_KNOWN_NON_REF_KINDS = {
    "Aggregation",
    "Hierarchy",
    "Literal",
    "Arithmetic",
    "ScopedEval",
    "Subquery",
    "DateSpan",
}
_KNOWN_EXPRESSION_KINDS = _REF_PRODUCING_KINDS | _KNOWN_NON_REF_KINDS
# Sibling keys on a Select/OrderBy/filter root that are metadata, not a second
# expression kind -- used to find the "real" discriminator key on that root.
_EXPRESSION_ROOT_METADATA_KEYS = {"Name", "NativeReferenceName", "Direction"}


@dataclass(frozen=True)
class FieldRef:
    """A single reference to a model column, measure or hierarchy level.

    `name` is the column or measure name depending on `kind`; `hierarchy` is only set
    when `kind == "hierarchy_level"`. `context` records where the reference came from:
    one of projection|sort|objects|filter@report|filter@page|filter@visual|tooltip.
    """

    kind: FieldKind
    table: str
    name: str
    hierarchy: str | None = None
    context: str = "projection"


@dataclass
class VisualDefinition:
    visual_id: str
    page_id: str
    page_display_name: str | None
    visual_type: str | None
    title: str | None
    refs: list[FieldRef] = field(default_factory=list)
    is_data_visual: bool = False


@dataclass
class ReportDefinition:
    format: Literal["legacy", "pbir"]
    visuals: list[VisualDefinition] = field(default_factory=list)
    report_refs: list[FieldRef] = field(default_factory=list)
    page_refs: dict[str, list[FieldRef]] = field(default_factory=dict)
    skipped: "Counter[str]" = field(default_factory=Counter)
    unknown_expression_kinds: "Counter[str]" = field(default_factory=Counter)


def detect_report_format(parts: Mapping[str, bytes]) -> Literal["legacy", "pbir"] | None:
    """Sniffs the report shape from the part paths themselves, never from the
    dataset/report metadata's own `format` field, which can be stale."""
    if any(path == "report.json" for path in parts):
        return "legacy"
    if any(path.startswith("definition/pages/") for path in parts):
        return "pbir"
    return None


def parse_report_definition(parts: Mapping[str, bytes]) -> ReportDefinition | None:
    report_format = detect_report_format(parts)
    if report_format == "legacy":
        return _parse_legacy_report(parts)
    if report_format == "pbir":
        return _parse_pbir_report(parts)
    return None


# ---------------------------------------------------------------------------
# Expression walking, shared by both formats
# ---------------------------------------------------------------------------


def _alias_entity(expr: dict[str, Any] | None, aliases: dict[str, str]) -> str | None:
    source_ref = (expr or {}).get("SourceRef") or {}
    if "Entity" in source_ref:
        return source_ref["Entity"]
    if "Source" in source_ref:
        # An alias with no matching From[] entry is a malformed/orphaned reference
        # rather than something to silently drop -- keep the raw alias as the table so
        # it still becomes a FieldRef and surfaces as dangling downstream.
        return aliases.get(source_ref["Source"], source_ref["Source"])
    return None


def _walk_expression(node: Any, aliases: dict[str, str], context: str, out: list[FieldRef]) -> None:
    """Recursively collects FieldRefs from a query-expression JSON tree. Continues
    recursing into every value regardless of whether it matched a known kind -- an
    Aggregation, for instance, is never special-cased; recursion reaches the Column it
    wraps naturally, exactly as the reference prototype behaves."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("Column", "PropertyVariationSource") and isinstance(value, dict) and "Property" in value:
                table = _alias_entity(value.get("Expression"), aliases)
                if table is not None:
                    out.append(FieldRef("column", table, value["Property"], context=context))
            elif key == "Measure" and isinstance(value, dict) and "Property" in value:
                table = _alias_entity(value.get("Expression"), aliases)
                if table is not None:
                    out.append(FieldRef("measure", table, value["Property"], context=context))
            elif key == "HierarchyLevel" and isinstance(value, dict):
                _handle_hierarchy_level(value, aliases, context, out)
            _walk_expression(value, aliases, context, out)
    elif isinstance(node, list):
        for item in node:
            _walk_expression(item, aliases, context, out)


def _handle_hierarchy_level(node: dict[str, Any], aliases: dict[str, str], context: str, out: list[FieldRef]) -> None:
    hierarchy = (node.get("Expression") or {}).get("Hierarchy") or {}
    # A hierarchy defined via an auto-date variation resolves differently (the
    # "Hierarchy" name is synthetic); skip rather than guess at a wrong table/level.
    if "PropertyVariationSource" in (hierarchy.get("Expression") or {}):
        return
    table = _alias_entity(hierarchy.get("Expression"), aliases)
    hierarchy_name = hierarchy.get("Hierarchy")
    level = node.get("Level")
    if table is not None and hierarchy_name is not None and level is not None:
        out.append(FieldRef("hierarchy_level", table, level, hierarchy=hierarchy_name, context=context))


def _expression_root_kind(node: Any) -> str | None:
    """The single query-expression discriminator key of a Select/OrderBy/filter root,
    ignoring the metadata siblings (Name, NativeReferenceName, Direction) that ride
    alongside it. Returns None when the shape doesn't look like a single-kind root."""
    if not isinstance(node, dict):
        return None
    candidates = [k for k in node if k not in _EXPRESSION_ROOT_METADATA_KEYS]
    return candidates[0] if len(candidates) == 1 else None


def _track_unknown_kind(node: Any, report: ReportDefinition) -> None:
    kind = _expression_root_kind(node)
    if kind is not None and kind not in _KNOWN_EXPRESSION_KINDS:
        report.unknown_expression_kinds[kind] += 1


def _aliases_of(query: dict[str, Any] | None) -> dict[str, str]:
    return {
        entry["Name"]: entry["Entity"]
        for entry in (query or {}).get("From", [])
        if "Name" in entry and "Entity" in entry
    }


# ---------------------------------------------------------------------------
# Legacy (report.json)
# ---------------------------------------------------------------------------


def _load_json_field(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _parse_legacy_report(parts: Mapping[str, bytes]) -> ReportDefinition:
    report = ReportDefinition(format="legacy")
    raw = json.loads(parts["report.json"].decode("utf-8"))

    report_filters = _load_json_field(raw.get("filters")) or []
    for filt in report_filters:
        _walk_legacy_filter(filt, "filter@report", report.report_refs, report)

    for section in raw.get("sections", []):
        page_id = section.get("name", "")
        page_refs = report.page_refs.setdefault(page_id, [])
        section_filters = _load_json_field(section.get("filters")) or []
        for filt in section_filters:
            _walk_legacy_filter(filt, "filter@page", page_refs, report)

        for vc in section.get("visualContainers", []):
            visual = _parse_legacy_visual(vc, page_id, section.get("displayName"), report)
            if visual is not None:
                report.visuals.append(visual)

    return report


def _walk_legacy_filter(filt: dict[str, Any], context: str, out: list[FieldRef], report: ReportDefinition) -> None:
    subquery = filt.get("filter")
    aliases = _aliases_of(subquery) if isinstance(subquery, dict) else {}
    expression = filt.get("expression")
    if expression is not None:
        _track_unknown_kind(expression, report)
        _walk_expression(expression, aliases, context, out)
    if subquery is not None:
        _walk_expression(subquery, aliases, context, out)


def _parse_legacy_visual(
    vc: dict[str, Any],
    page_id: str,
    page_display_name: str | None,
    report: ReportDefinition,
) -> VisualDefinition | None:
    config = _load_json_field(vc.get("config")) or {}
    if "singleVisualGroup" in config:
        report.skipped["visual_group"] += 1
        return None
    single_visual = config.get("singleVisual")
    if not single_visual:
        report.skipped["no_single_visual"] += 1
        return None

    visual_type = single_visual.get("visualType")
    visual_id = config.get("name", "")
    refs: list[FieldRef] = []

    prototype_query = single_visual.get("prototypeQuery")
    if prototype_query:
        aliases = _aliases_of(prototype_query)
        for select in prototype_query.get("Select", []):
            _track_unknown_kind(select, report)
            _walk_expression(select, aliases, "projection", refs)
        for order_by in prototype_query.get("OrderBy", []):
            expr = order_by.get("Expression")
            _track_unknown_kind(expr, report)
            _walk_expression(expr, aliases, "sort", refs)
    elif visual_type not in NON_DATA_VISUAL_TYPES:
        report.skipped["no_prototype_query"] += 1

    if vc.get("queryBinary"):
        report.skipped["query_binary"] += 1

    _walk_expression(single_visual.get("objects"), {}, "objects", refs)
    _walk_expression(single_visual.get("vcObjects"), {}, "objects", refs)

    vc_filters = _load_json_field(vc.get("filters")) or []
    for filt in vc_filters:
        _walk_legacy_filter(filt, "filter@visual", refs, report)

    title = _extract_legacy_title(single_visual)
    return VisualDefinition(
        visual_id=visual_id,
        page_id=page_id,
        page_display_name=page_display_name,
        visual_type=visual_type,
        title=title,
        refs=refs,
        is_data_visual=bool(refs),
    )


def _extract_legacy_title(single_visual: dict[str, Any]) -> str | None:
    objects = single_visual.get("objects") or {}
    title_props = objects.get("title") or []
    for entry in title_props:
        text = (entry.get("properties") or {}).get("text") or {}
        literal = ((text.get("expr") or {}).get("Literal") or {}).get("Value")
        if literal:
            return literal.strip("'")
    return None


# ---------------------------------------------------------------------------
# PBIR (definition/pages/**)
# ---------------------------------------------------------------------------


def _parse_pbir_report(parts: Mapping[str, bytes]) -> ReportDefinition:
    report = ReportDefinition(format="pbir")

    report_json_bytes = parts.get("definition/report.json")
    if report_json_bytes is not None:
        report_json = json.loads(report_json_bytes.decode("utf-8"))
        for filt in (report_json.get("filterConfig") or {}).get("filters", []):
            _walk_pbir_filter(filt, "filter@report", report.report_refs, report)

    pages = _group_pbir_pages(parts)
    for page_id, page_parts in pages.items():
        page_json = page_parts.get("page")
        page_display_name = None
        page_refs = report.page_refs.setdefault(page_id, [])
        if page_json is not None:
            page = json.loads(page_json.decode("utf-8"))
            page_display_name = page.get("displayName")
            for filt in (page.get("filterConfig") or {}).get("filters", []):
                _walk_pbir_filter(filt, "filter@page", page_refs, report)
            for param in (page.get("pageBinding") or {}).get("parameters", []):
                field_expr = param.get("fieldExpr")
                if field_expr is not None:
                    _walk_expression(field_expr, {}, "tooltip", page_refs)

        for visual_id, visual_bytes in page_parts.get("visuals", {}).items():
            visual_def = _parse_pbir_visual(visual_bytes, visual_id, page_id, page_display_name, report)
            if visual_def is not None:
                report.visuals.append(visual_def)

    return report


def _group_pbir_pages(parts: Mapping[str, bytes]) -> dict[str, dict[str, Any]]:
    """Groups `definition/pages/<page>/page.json` and
    `definition/pages/<page>/visuals/<visual>/visual.json` parts by page id."""
    pages: dict[str, dict[str, Any]] = {}
    for path, payload in parts.items():
        if not path.startswith("definition/pages/"):
            continue
        segments = path.split("/")
        # definition/pages/<page>/page.json
        if len(segments) == 4 and segments[3] == "page.json":
            pages.setdefault(segments[2], {})["page"] = payload
        # definition/pages/<page>/visuals/<visual>/visual.json
        elif len(segments) == 6 and segments[3] == "visuals" and segments[5] == "visual.json":
            page_id, visual_id = segments[2], segments[4]
            pages.setdefault(page_id, {}).setdefault("visuals", {})[visual_id] = payload
    return pages


def _walk_pbir_filter(filt: dict[str, Any], context: str, out: list[FieldRef], report: ReportDefinition) -> None:
    field_expr = filt.get("field")
    if field_expr is not None:
        _track_unknown_kind(field_expr, report)
        _walk_expression(field_expr, {}, context, out)
    # A TopN-style filter carries its own subquery, structurally identical to legacy's
    # "filter" key; walk it defensively even though it wasn't observed in captured data.
    subquery = filt.get("filter")
    if subquery is not None:
        _walk_expression(subquery, _aliases_of(subquery), context, out)


def _parse_pbir_visual(
    visual_bytes: bytes,
    visual_id: str,
    page_id: str,
    page_display_name: str | None,
    report: ReportDefinition,
) -> VisualDefinition | None:
    visual_json = json.loads(visual_bytes.decode("utf-8"))
    visual = visual_json.get("visual")
    if visual is None:
        report.skipped["visual_group"] += 1
        return None

    visual_type = visual.get("visualType")
    refs: list[FieldRef] = []
    query = visual.get("query") or {}
    query_state = query.get("queryState") or {}
    has_query_state = False
    for role_state in query_state.values():
        for projection in role_state.get("projections", []):
            field_expr = projection.get("field")
            if field_expr is not None:
                has_query_state = True
                _track_unknown_kind(field_expr, report)
                _walk_expression(field_expr, {}, "projection", refs)

    for sort_entry in (query.get("sortDefinition") or {}).get("sort", []):
        field_expr = sort_entry.get("field")
        if field_expr is not None:
            _track_unknown_kind(field_expr, report)
            _walk_expression(field_expr, {}, "sort", refs)

    if not has_query_state and visual_type not in NON_DATA_VISUAL_TYPES:
        report.skipped["no_prototype_query"] += 1

    _walk_expression(visual.get("objects"), {}, "objects", refs)
    _walk_expression(visual.get("visualContainerObjects"), {}, "objects", refs)

    for filt in (visual_json.get("filterConfig") or {}).get("filters", []):
        _walk_pbir_filter(filt, "filter@visual", refs, report)

    title = _extract_pbir_title(visual)
    return VisualDefinition(
        visual_id=visual_id,
        page_id=page_id,
        page_display_name=page_display_name,
        visual_type=visual_type,
        title=title,
        refs=refs,
        is_data_visual=bool(refs),
    )


def _extract_pbir_title(visual: dict[str, Any]) -> str | None:
    objects = visual.get("objects") or {}
    title_props = objects.get("title") or []
    for entry in title_props:
        text = (entry.get("properties") or {}).get("text") or {}
        literal = ((text.get("expr") or {}).get("Literal") or {}).get("Value")
        if literal:
            return literal.strip("'")
    return None
