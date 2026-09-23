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
Tests for metadata.ingestion.source.dashboard.powerbi.report_definition
"""

import json

from metadata.ingestion.source.dashboard.powerbi.report_definition import (
    detect_report_format,
    parse_report_definition,
)


def _json_str(value) -> str:
    return json.dumps(value)


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------


class TestDetectReportFormat:
    def test_legacy_detected_from_report_json(self):
        assert detect_report_format({"report.json": b"{}"}) == "legacy"

    def test_pbir_detected_from_definition_pages_path(self):
        assert detect_report_format({"definition/pages/Page1/page.json": b"{}"}) == "pbir"

    def test_unknown_when_neither_marker_present(self):
        assert detect_report_format({"definition.pbir": b"{}"}) is None
        assert detect_report_format({}) is None

    def test_parse_report_definition_returns_none_for_unrecognized_parts(self):
        assert parse_report_definition({"random.json": b"{}"}) is None


# ---------------------------------------------------------------------------
# Legacy (report.json)
# ---------------------------------------------------------------------------


def _column_select(alias: str, prop: str, name: str | None = None) -> dict:
    return {
        "Column": {"Expression": {"SourceRef": {"Source": alias}}, "Property": prop},
        "Name": name or f"{alias}.{prop}",
    }


def _legacy_visual_container(
    visual_type="tableEx",
    select=None,
    order_by=None,
    objects=None,
    vc_objects=None,
    filters=None,
    from_=None,
    query_binary=False,
    no_single_visual=False,
    visual_group=False,
    name="vc1",
):
    single_visual = None
    if not no_single_visual and not visual_group:
        single_visual = {"visualType": visual_type}
        if select is not None or order_by is not None or from_ is not None:
            single_visual["prototypeQuery"] = {
                "Version": 2,
                "From": from_ or [{"Name": "s", "Entity": "Sales", "Type": 0}],
                "Select": select or [],
                "OrderBy": order_by or [],
            }
        if objects is not None:
            single_visual["objects"] = objects
        if vc_objects is not None:
            single_visual["vcObjects"] = vc_objects

    config = {"name": name, "layouts": {}}
    if visual_group:
        config["singleVisualGroup"] = {"displayName": "Group"}
    else:
        config["singleVisual"] = single_visual

    vc = {"config": _json_str(config), "height": 100, "width": 100, "x": 0, "y": 0, "z": 0}
    if filters is not None:
        vc["filters"] = _json_str(filters)
    if query_binary:
        vc["queryBinary"] = "base64=="
    return vc


def _legacy_report(sections):
    return {
        "config": _json_str({"version": "1.0"}),
        "layoutOptimization": 0,
        "resourcePackages": [],
        "sections": sections,
    }


class TestParseLegacyReportDefinition:
    def test_projection_and_sort_refs_with_alias_resolution(self):
        select = [
            _column_select("s", "Amount"),
            {
                "Aggregation": {
                    "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Amount"}},
                    "Function": 0,
                },
                "Name": "Sum(Sales.Amount)",
            },
        ]
        order_by = [
            {
                "Direction": 2,
                "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Amount"}},
            }
        ]
        vc = _legacy_visual_container(select=select, order_by=order_by)
        report = {
            "report.json": json.dumps(
                _legacy_report([{"name": "Section1", "displayName": "Page 1", "visualContainers": [vc]}])
            ).encode("utf-8")
        }

        rd = parse_report_definition(report)
        assert rd.format == "legacy"
        assert len(rd.visuals) == 1
        visual = rd.visuals[0]
        assert visual.is_data_visual is True
        projection_refs = [r for r in visual.refs if r.context == "projection"]
        sort_refs = [r for r in visual.refs if r.context == "sort"]
        assert {(r.kind, r.table, r.name) for r in projection_refs} == {("column", "Sales", "Amount")}
        assert {(r.kind, r.table, r.name) for r in sort_refs} == {("column", "Sales", "Amount")}

    def test_direct_entity_ref_without_alias(self):
        select = [{"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}, "Name": "x"}]
        vc = _legacy_visual_container(select=select, from_=[])
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert {(r.table, r.name) for r in rd.visuals[0].refs} == {("Sales", "Amount")}

    def test_measure_ref_via_aggregation_wrapper(self):
        select = [
            {
                "Measure": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Sales"},
                "Name": "Sales.Total Sales",
            }
        ]
        vc = _legacy_visual_container(select=select)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        refs = rd.visuals[0].refs
        assert refs[0].kind == "measure"
        assert (refs[0].table, refs[0].name) == ("Sales", "Total Sales")

    def test_no_single_visual_is_skipped(self):
        vc = _legacy_visual_container(no_single_visual=True)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert rd.visuals == []
        assert rd.skipped["no_single_visual"] == 1

    def test_single_visual_group_is_skipped(self):
        vc = _legacy_visual_container(visual_group=True)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert rd.visuals == []
        assert rd.skipped["visual_group"] == 1

    def test_visual_without_prototype_query_is_data_visual_false(self):
        vc = _legacy_visual_container(visual_type="shape", select=None, order_by=None, from_=None)
        # No Select/OrderBy/From at all -> no prototypeQuery key gets built.
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert rd.visuals[0].is_data_visual is False
        # "shape" is a known non-data visual type, so it's not counted as unusual.
        assert rd.skipped["no_prototype_query"] == 0

    def test_visual_without_prototype_query_unusual_type_is_tracked(self):
        vc = _legacy_visual_container(visual_type="customVisual123", select=None, order_by=None, from_=None)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert rd.skipped["no_prototype_query"] == 1

    def test_query_binary_is_tracked_but_visual_still_parsed(self):
        select = [_column_select("s", "Amount")]
        vc = _legacy_visual_container(select=select, query_binary=True)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert rd.skipped["query_binary"] == 1
        assert len(rd.visuals) == 1

    def test_objects_and_vc_objects_refs_count_as_usage(self):
        # Conditional-formatting/title bindings in objects always carry a direct
        # SourceRef.Entity in captured data (never an alias), so aliases aren't
        # resolved for this walk -- match that shape here.
        objects = {
            "title": [
                {
                    "properties": {
                        "titleText": {
                            "expr": {
                                "Measure": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Total Sales"}
                            }
                        }
                    }
                }
            ]
        }
        vc_objects = {
            "dataPoint": [
                {
                    "properties": {
                        "fill": {
                            "solid": {
                                "color": {
                                    "expr": {
                                        "Column": {
                                            "Expression": {"SourceRef": {"Entity": "Sales"}},
                                            "Property": "Region",
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        vc = _legacy_visual_container(select=[_column_select("s", "Amount")], objects=objects, vc_objects=vc_objects)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        object_refs = {(r.kind, r.table, r.name) for r in rd.visuals[0].refs if r.context == "objects"}
        assert ("measure", "Sales", "Total Sales") in object_refs
        assert ("column", "Sales", "Region") in object_refs

    def test_plain_literal_in_objects_does_not_produce_a_ref(self):
        objects = {"general": [{"properties": {"fontSize": {"expr": {"Literal": {"Value": "12D"}}}}}]}
        vc = _legacy_visual_container(select=[_column_select("s", "Amount")], objects=objects)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        object_refs = [r for r in rd.visuals[0].refs if r.context == "objects"]
        assert object_refs == []

    def test_visual_level_filter_with_entity_ref(self):
        filters = [
            {
                "name": "Filter1",
                "expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Region"}},
                "type": "Categorical",
            }
        ]
        vc = _legacy_visual_container(select=[_column_select("s", "Amount")], filters=filters)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        filter_refs = {(r.table, r.name) for r in rd.visuals[0].refs if r.context == "filter@visual"}
        assert filter_refs == {("Sales", "Region")}

    def test_visual_level_topn_filter_with_subquery_aliases(self):
        filters = [
            {
                "name": "Filter1",
                "expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "CustomerId"}},
                "filter": {
                    "Version": 2,
                    "From": [
                        {
                            "Name": "subquery",
                            "Expression": {
                                "Subquery": {
                                    "Query": {
                                        "Version": 2,
                                        "From": [{"Name": "a", "Entity": "Sales", "Type": 0}],
                                        "Select": [_column_select("a", "CustomerId")],
                                        "OrderBy": [
                                            {
                                                "Direction": 2,
                                                "Expression": {
                                                    "Measure": {
                                                        "Expression": {"SourceRef": {"Source": "a"}},
                                                        "Property": "Total Sales",
                                                    }
                                                },
                                            }
                                        ],
                                        "Top": 10,
                                    }
                                }
                            },
                            "Type": 2,
                        },
                        {"Name": "a", "Entity": "Sales", "Type": 0},
                    ],
                    "Where": [],
                },
                "type": "TopN",
            }
        ]
        vc = _legacy_visual_container(select=[_column_select("s", "Amount")], filters=filters)
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        filter_refs = {(r.kind, r.table, r.name) for r in rd.visuals[0].refs if r.context == "filter@visual"}
        assert ("column", "Sales", "CustomerId") in filter_refs
        assert ("measure", "Sales", "Total Sales") in filter_refs

    def test_section_filters_land_in_page_refs_not_visual_refs(self):
        section_filters = [
            {
                "name": "F1",
                "expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Region"}},
                "type": "Categorical",
            }
        ]
        section = {
            "name": "Section1",
            "displayName": "Page 1",
            "filters": _json_str(section_filters),
            "visualContainers": [],
        }
        report = {"report.json": json.dumps(_legacy_report([section])).encode("utf-8")}
        rd = parse_report_definition(report)
        assert rd.page_refs["Section1"]
        assert {(r.table, r.name, r.context) for r in rd.page_refs["Section1"]} == {("Sales", "Region", "filter@page")}

    def test_report_level_filters_land_in_report_refs(self):
        report_filters = [
            {
                "name": "F1",
                "expression": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Region"}},
                "type": "Categorical",
            }
        ]
        raw = _legacy_report([{"name": "Section1", "visualContainers": []}])
        raw["filters"] = _json_str(report_filters)
        report = {"report.json": json.dumps(raw).encode("utf-8")}
        rd = parse_report_definition(report)
        assert {(r.table, r.name, r.context) for r in rd.report_refs} == {("Sales", "Region", "filter@report")}

    def test_unresolvable_alias_still_produces_a_field_ref(self):
        select = [_column_select("missing_alias", "Region")]
        vc = _legacy_visual_container(select=select, from_=[{"Name": "s", "Entity": "Sales", "Type": 0}])
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        refs = rd.visuals[0].refs
        assert len(refs) == 1
        # The unresolved alias itself becomes the table, so the ref survives and can
        # later be recognized as dangling instead of silently vanishing.
        assert refs[0].table == "missing_alias"
        assert refs[0].name == "Region"

    def test_is_data_visual_true_even_for_shape_type_when_it_has_refs(self):
        # A shape isn't normally data-bound, but the decision is made on refs alone.
        vc = _legacy_visual_container(visual_type="shape", select=[_column_select("s", "Amount")])
        report = {
            "report.json": json.dumps(_legacy_report([{"name": "Section1", "visualContainers": [vc]}])).encode("utf-8")
        }
        rd = parse_report_definition(report)
        assert rd.visuals[0].is_data_visual is True


# ---------------------------------------------------------------------------
# PBIR (definition/pages/**)
# ---------------------------------------------------------------------------


def _pbir_parts(pages: dict, report_filters=None) -> dict:
    parts = {}
    report_json = {"$schema": "x", "layoutOptimization": 0}
    if report_filters is not None:
        report_json["filterConfig"] = {"filters": report_filters}
    parts["definition/report.json"] = json.dumps(report_json).encode("utf-8")
    for page_id, page in pages.items():
        parts[f"definition/pages/{page_id}/page.json"] = json.dumps(page["page"]).encode("utf-8")
        for visual_id, visual in page.get("visuals", {}).items():
            parts[f"definition/pages/{page_id}/visuals/{visual_id}/visual.json"] = json.dumps(visual).encode("utf-8")
    return parts


def _pbir_visual(
    query_state=None,
    sort=None,
    objects=None,
    visual_container_objects=None,
    filters=None,
    visual_type="columnChart",
    no_visual=False,
):
    if no_visual:
        return {"name": "vg1", "position": {}, "visualGroup": {}}
    visual = {"visualType": visual_type}
    query = {}
    if query_state is not None:
        query["queryState"] = query_state
    if sort is not None:
        query["sortDefinition"] = {"sort": sort}
    if query:
        visual["query"] = query
    if objects is not None:
        visual["objects"] = objects
    if visual_container_objects is not None:
        visual["visualContainerObjects"] = visual_container_objects
    result = {"name": "v1", "position": {}, "visual": visual}
    if filters is not None:
        result["filterConfig"] = {"filters": filters}
    return result


class TestParsePbirReportDefinition:
    def test_projection_and_sort_refs_direct_entity(self):
        query_state = {
            "Y": {
                "projections": [
                    {
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}},
                        "queryRef": "x",
                    }
                ]
            }
        }
        sort = [
            {
                "field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}},
                "direction": "Ascending",
            }
        ]
        visual = _pbir_visual(query_state=query_state, sort=sort)
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1", "displayName": "Page 1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        assert rd.format == "pbir"
        assert len(rd.visuals) == 1
        v = rd.visuals[0]
        assert v.page_id == "Page1"
        assert v.page_display_name == "Page 1"
        assert v.is_data_visual is True
        assert {(r.kind, r.table, r.name, r.context) for r in v.refs} == {
            ("column", "Sales", "Amount", "projection"),
            ("column", "Sales", "Amount", "sort"),
        }

    def test_visual_group_has_no_visual_key_and_is_skipped(self):
        visual = _pbir_visual(no_visual=True)
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"vg1": visual}}})
        rd = parse_report_definition(parts)
        assert rd.visuals == []
        assert rd.skipped["visual_group"] == 1

    def test_aggregation_measure_projection(self):
        query_state = {
            "Data": {
                "projections": [
                    {
                        "field": {
                            "Aggregation": {
                                "Expression": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": "Sales"}},
                                        "Property": "Total Sales",
                                    }
                                },
                                "Function": 0,
                            }
                        },
                        "queryRef": "Sum(Sales.Total Sales)",
                    }
                ]
            }
        }
        visual = _pbir_visual(query_state=query_state)
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        refs = rd.visuals[0].refs
        assert (refs[0].kind, refs[0].table, refs[0].name) == ("measure", "Sales", "Total Sales")

    def test_hierarchy_level_projection(self):
        query_state = {
            "Category": {
                "projections": [
                    {
                        "field": {
                            "HierarchyLevel": {
                                "Expression": {
                                    "Hierarchy": {
                                        "Expression": {"SourceRef": {"Entity": "Date"}},
                                        "Hierarchy": "Date Hierarchy",
                                    }
                                },
                                "Level": "Year",
                            }
                        },
                        "queryRef": "Date.Date Hierarchy.Year",
                    }
                ]
            }
        }
        visual = _pbir_visual(query_state=query_state)
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        ref = rd.visuals[0].refs[0]
        assert ref.kind == "hierarchy_level"
        assert ref.table == "Date"
        assert ref.hierarchy == "Date Hierarchy"
        assert ref.name == "Year"

    def test_variation_hierarchy_level_resolves_to_business_column(self):
        # A drilled auto-date hierarchy (Year/Quarter/.../Day on a date column) is
        # wrapped in PropertyVariationSource, not a plain SourceRef -- the "Hierarchy"
        # name here belongs to the internal auto-date table, not the business one.
        query_state = {
            "Values": {
                "projections": [
                    {
                        "field": {
                            "HierarchyLevel": {
                                "Expression": {
                                    "Hierarchy": {
                                        "Expression": {
                                            "PropertyVariationSource": {
                                                "Expression": {"SourceRef": {"Entity": "Sales"}},
                                                "Name": "Variation",
                                                "Property": "OrderDate",
                                            }
                                        },
                                        "Hierarchy": "Date Hierarchy",
                                    }
                                },
                                "Level": "Year",
                            }
                        },
                        "queryRef": "Sales.OrderDate.Variation.Date Hierarchy.Year",
                    }
                ]
            }
        }
        visual = _pbir_visual(query_state=query_state)
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        ref = rd.visuals[0].refs[0]
        assert ref.kind == "hierarchy_level"
        assert ref.table == "Sales"
        assert ref.name == "OrderDate"
        # None (never a real hierarchy name) is the variation marker column_usage.py
        # keys off; the drilled level is kept separately for the auto-date lookup.
        assert ref.hierarchy is None
        assert ref.variation_level == "Year"

    def test_nested_from_inside_objects_resolves_aliases_locally(self):
        # A conditional-formatting/filter-chip binding under `objects` can carry its
        # own nested {"From": [...], "Where": [...]} sub-query with its own alias scope,
        # unrelated to the visual's own outer aliases.
        objects = {
            "general": [
                {
                    "properties": {
                        "filter": {
                            "filter": {
                                "Version": 2,
                                "From": [{"Name": "l", "Entity": "Date", "Type": 0}],
                                "Where": [
                                    {
                                        "Condition": {
                                            "In": {
                                                "Expressions": [
                                                    {
                                                        "Column": {
                                                            "Expression": {"SourceRef": {"Source": "l"}},
                                                            "Property": "Year",
                                                        }
                                                    }
                                                ],
                                                "Values": [[{"Literal": {"Value": "2026L"}}]],
                                            }
                                        }
                                    }
                                ],
                            }
                        }
                    }
                }
            ]
        }
        visual = _pbir_visual(
            query_state={
                "Y": {
                    "projections": [
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}}}
                    ]
                }
            },
            objects=objects,
        )
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        object_refs = {(r.table, r.name) for r in rd.visuals[0].refs if r.context == "objects"}
        assert ("Date", "Year") in object_refs

    def test_visual_page_and_report_level_filters(self):
        visual_filter = [
            {
                "name": "f1",
                "field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Region"}},
                "type": "Categorical",
            }
        ]
        visual = _pbir_visual(
            query_state={
                "Y": {
                    "projections": [
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}}}
                    ]
                }
            },
            filters=visual_filter,
        )
        page_filter = [
            {
                "name": "f2",
                "field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Year"}},
                "type": "Categorical",
            }
        ]
        report_filter = [
            {
                "name": "f3",
                "field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Country"}},
                "type": "Categorical",
            }
        ]
        parts = _pbir_parts(
            {"Page1": {"page": {"name": "Page1", "filterConfig": {"filters": page_filter}}, "visuals": {"v1": visual}}},
            report_filters=report_filter,
        )
        rd = parse_report_definition(parts)
        assert {(r.table, r.name) for r in rd.report_refs} == {("Sales", "Country")}
        assert {(r.table, r.name) for r in rd.page_refs["Page1"]} == {("Sales", "Year")}
        visual_filter_refs = {(r.table, r.name) for r in rd.visuals[0].refs if r.context == "filter@visual"}
        assert visual_filter_refs == {("Sales", "Region")}

    def test_tooltip_page_binding_field_expr(self):
        page = {
            "name": "TooltipPage",
            "pageBinding": {
                "type": "Tooltip",
                "parameters": [
                    {
                        "name": "p1",
                        "fieldExpr": {
                            "Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}
                        },
                    }
                ],
            },
        }
        parts = _pbir_parts({"TooltipPage": {"page": page, "visuals": {}}})
        rd = parse_report_definition(parts)
        tooltip_refs = {(r.table, r.name, r.context) for r in rd.page_refs["TooltipPage"]}
        assert tooltip_refs == {("Sales", "Amount", "tooltip")}

    def test_objects_and_visual_container_objects_count_as_usage(self):
        objects = {
            "title": [
                {
                    "properties": {
                        "text": {
                            "expr": {
                                "Measure": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Total Sales"}
                            }
                        }
                    }
                }
            ]
        }
        vco = {
            "background": [{"properties": {"fill": {"solid": {"color": {"expr": {"Literal": {"Value": "'#fff'"}}}}}}}]
        }
        visual = _pbir_visual(
            query_state={
                "Y": {
                    "projections": [
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}}}
                    ]
                }
            },
            objects=objects,
            visual_container_objects=vco,
        )
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        object_refs = {(r.kind, r.table, r.name) for r in rd.visuals[0].refs if r.context == "objects"}
        assert ("measure", "Sales", "Total Sales") in object_refs

    def test_no_query_state_non_data_visual_type_not_flagged_unusual(self):
        visual = _pbir_visual(visual_type="textbox")
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        assert rd.visuals[0].is_data_visual is False
        assert rd.skipped["no_prototype_query"] == 0

    def test_no_query_state_unusual_visual_type_is_tracked(self):
        visual = _pbir_visual(visual_type="someCustomVisual")
        parts = _pbir_parts({"Page1": {"page": {"name": "Page1"}, "visuals": {"v1": visual}}})
        rd = parse_report_definition(parts)
        assert rd.skipped["no_prototype_query"] == 1

    def test_page_display_name_defaults_to_none_without_page_json(self):
        # A page folder with only visuals (no page.json captured) shouldn't crash.
        visual = _pbir_visual(
            query_state={
                "Y": {
                    "projections": [
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "Sales"}}, "Property": "Amount"}}}
                    ]
                }
            }
        )
        parts = {
            "definition/report.json": b"{}",
            "definition/pages/Page1/visuals/v1/visual.json": json.dumps(visual).encode("utf-8"),
        }
        rd = parse_report_definition(parts)
        assert rd.visuals[0].page_display_name is None
