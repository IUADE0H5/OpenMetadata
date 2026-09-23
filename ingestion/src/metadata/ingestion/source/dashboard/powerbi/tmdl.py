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
A minimal, defensive TMDL (Tabular Model Definition Language) parser for the parts of a
semantic model's `getDefinition?format=TMDL` payload that matter for column-usage
resolution: tables, columns (incl. calculated columns), measures, hierarchies,
relationships and partitions. TMDL is indentation-significant; there is no official
grammar published for third-party parsers, so this reads the concrete syntax Power BI
Desktop / the Fabric service actually emit rather than a formal spec.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

# A table is flagged as an internal auto-date table by its generated name prefix, or by
# either of these two structural markers -- never a business table a report author added.
_AUTO_DATE_NAME_PREFIXES = ("LocalDateTable_", "DateTableTemplate_")

_MEMBER_HEADER_RE = re.compile(
    r"^\t(column|measure|hierarchy|partition)\s+"
    r"((?:'(?:[^']|'')*')|[^\s=]+)"
    r"(?:\s*=\s*(.*))?$"
)
_LEVEL_HEADER_RE = re.compile(r"^\t\tlevel\s+((?:'(?:[^']|'')*')|[^\s=]+)\s*$")
_RELATIONSHIP_HEADER_RE = re.compile(r"^relationship\s+")
_VARIATION_HEADER_RE = re.compile(r"^\t\tvariation\s+((?:'(?:[^']|'')*')|[^\s=]+)\s*$")


@dataclass
class TmdlVariation:
    """A column's auto-date variation: the link between a business date column and the
    internal auto-date table Power BI generates a default drill-down hierarchy from."""

    name: str
    is_default: bool = False
    relationship: str | None = None
    # (auto_date_table, hierarchy_name), parsed from `defaultHierarchy: T.'Hierarchy'`.
    default_hierarchy: tuple[str, str] | None = None


@dataclass
class TmdlColumn:
    name: str
    data_type: str | None = None
    source_column: str | None = None
    # DAX text for a calculated column; None for a regular (sourced) column.
    expression: str | None = None
    sort_by_column: str | None = None
    is_hidden: bool = False
    variations: list[TmdlVariation] = field(default_factory=list)


@dataclass
class TmdlMeasure:
    name: str
    expression: str = ""


@dataclass
class TmdlHierarchyLevel:
    name: str
    column: str | None = None


@dataclass
class TmdlHierarchy:
    name: str
    levels: list[TmdlHierarchyLevel] = field(default_factory=list)


@dataclass
class TmdlPartition:
    name: str
    mode: str | None = None
    # The M (Power Query) expression powering this partition, if any.
    source: str | None = None


@dataclass
class TmdlTable:
    name: str
    is_hidden: bool = False
    is_auto_date: bool = False
    columns: list[TmdlColumn] = field(default_factory=list)
    measures: list[TmdlMeasure] = field(default_factory=list)
    hierarchies: list[TmdlHierarchy] = field(default_factory=list)
    partitions: list[TmdlPartition] = field(default_factory=list)


@dataclass
class TmdlRelationship:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    is_active: bool = True


@dataclass
class SemanticModelDefinition:
    tables: list[TmdlTable] = field(default_factory=list)
    relationships: list[TmdlRelationship] = field(default_factory=list)

    def table(self, name: str) -> TmdlTable | None:
        return next((t for t in self.tables if t.name == name), None)


def parse_tmdl(parts: Mapping[str, bytes]) -> SemanticModelDefinition:
    model = SemanticModelDefinition()
    for path, payload in parts.items():
        if path.startswith("definition/tables/") and path.endswith(".tmdl"):
            model.tables.append(_parse_table(payload.decode("utf-8")))
    relationships_bytes = parts.get("definition/relationships.tmdl")
    if relationships_bytes is not None:
        model.relationships = _parse_relationships(relationships_bytes.decode("utf-8"))
    return model


def _unquote(name: str) -> str:
    name = name.strip()
    if name.startswith("'") and name.endswith("'") and len(name) >= 2:
        return name[1:-1].replace("''", "'")
    return name


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip("\t"))


def _is_auto_date(name: str, is_private: bool, show_as_variations_only: bool) -> bool:
    return name.startswith(_AUTO_DATE_NAME_PREFIXES) or is_private or show_as_variations_only


def _read_expression(lines: list[str], header_index: int, inline_rest: str | None) -> tuple[str | None, int]:
    """Reads the expression that follows a `name = ...` header, whether it's inline,
    fenced (```), or an unfenced multi-line block indented deeper than the header.
    Returns (expression_or_None, index_of_next_unconsumed_line)."""
    if inline_rest is None:
        return None, header_index + 1
    rest = inline_rest.strip()
    base_indent = _indent(lines[header_index])
    if rest == "```":
        buf: list[str] = []
        j = header_index + 1
        while j < len(lines) and lines[j].strip() != "```":
            buf.append(lines[j])
            j += 1
        return "\n".join(buf).strip("\n"), j + 1
    if rest != "":
        return rest, header_index + 1
    # No inline text after '=': an unfenced block, indented deeper than the header line,
    # possibly with blank separator lines inside it.
    buf = []
    j = header_index + 1
    while j < len(lines):
        if lines[j].strip() == "":
            if buf and (j + 1 >= len(lines) or _indent(lines[j + 1]) <= base_indent):
                break
            buf.append(lines[j])
        elif _indent(lines[j]) > base_indent:
            buf.append(lines[j])
        else:
            break
        j += 1
    return ("\n".join(buf).strip("\n") or None), j


def _parse_table(text: str) -> TmdlTable:
    lines = text.split("\n")
    table = TmdlTable(name="")
    is_private = False
    show_as_variations_only = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("table "):
            table.name = _unquote(line[len("table ") :])
            i += 1
            continue

        header = _MEMBER_HEADER_RE.match(line)
        if header:
            kind, raw_name, raw_rest = header.groups()
            name = _unquote(raw_name)
            if kind == "column":
                column = TmdlColumn(name=name)
                table.columns.append(column)
                column.expression, i = _read_expression(lines, i, raw_rest)
                i = _read_column_properties(lines, i, column)
                continue
            if kind == "measure":
                measure = TmdlMeasure(name=name)
                table.measures.append(measure)
                expression, i = _read_expression(lines, i, raw_rest if raw_rest is not None else "")
                measure.expression = expression or ""
                continue
            if kind == "hierarchy":
                hierarchy = TmdlHierarchy(name=name)
                table.hierarchies.append(hierarchy)
                i = _read_hierarchy_levels(lines, i + 1, hierarchy)
                continue
            if kind == "partition":
                # The "= m" here just marks an M-expression partition; the real M
                # expression lives under the nested `source = ...` property below.
                partition = TmdlPartition(name=name)
                table.partitions.append(partition)
                i = _read_partition_properties(lines, i + 1, partition)
                continue

        stripped = line.strip()
        if _indent(line) == 1:
            if stripped == "isHidden":
                table.is_hidden = True
            elif stripped == "isPrivate":
                is_private = True
            elif stripped == "showAsVariationsOnly":
                show_as_variations_only = True
        i += 1

    table.is_auto_date = _is_auto_date(table.name, is_private, show_as_variations_only)
    return table


def _read_column_properties(lines: list[str], i: int, column: TmdlColumn) -> int:
    while i < len(lines) and (_indent(lines[i]) >= 2 or lines[i].strip() == ""):
        if lines[i].strip() == "":
            i += 1
            continue
        variation_header = _VARIATION_HEADER_RE.match(lines[i])
        if variation_header:
            variation, i = _read_variation(lines, i, variation_header)
            column.variations.append(variation)
            continue
        stripped = lines[i].strip()
        if stripped == "isHidden":
            column.is_hidden = True
        elif stripped.startswith("dataType:"):
            column.data_type = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("sourceColumn:"):
            column.source_column = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("sortByColumn:"):
            column.sort_by_column = _unquote(stripped.split(":", 1)[1].strip())
        elif _MEMBER_HEADER_RE.match(lines[i]) or lines[i].startswith("table "):
            break
        i += 1
    return i


def _read_variation(lines: list[str], i: int, header: re.Match[str]) -> tuple[TmdlVariation, int]:
    variation = TmdlVariation(name=_unquote(header.group(1)))
    i += 1
    while i < len(lines):
        line = lines[i]
        if line.strip() == "":
            i += 1
            continue
        if _indent(line) < 3:
            break
        stripped = line.strip()
        if stripped == "isDefault":
            variation.is_default = True
        elif stripped.startswith("relationship:"):
            variation.relationship = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("defaultHierarchy:"):
            variation.default_hierarchy = _split_qualified_ref(stripped.split(":", 1)[1].strip())
        i += 1
    return variation, i


def _read_hierarchy_levels(lines: list[str], i: int, hierarchy: TmdlHierarchy) -> int:
    current_level: TmdlHierarchyLevel | None = None
    while i < len(lines):
        line = lines[i]
        if line.strip() == "":
            i += 1
            continue
        if _indent(line) < 2:
            break
        level_match = _LEVEL_HEADER_RE.match(line)
        if level_match:
            current_level = TmdlHierarchyLevel(name=_unquote(level_match.group(1)))
            hierarchy.levels.append(current_level)
            i += 1
            continue
        stripped = line.strip()
        if current_level is not None and stripped.startswith("column:"):
            current_level.column = _unquote(stripped.split(":", 1)[1].strip())
        i += 1
    return i


def _read_partition_properties(lines: list[str], i: int, partition: TmdlPartition) -> int:
    while i < len(lines):
        line = lines[i]
        if line.strip() == "":
            i += 1
            continue
        if _indent(line) < 2:
            break
        stripped = line.strip()
        if stripped.startswith("mode:"):
            partition.mode = stripped.split(":", 1)[1].strip()
            i += 1
        elif stripped.startswith("source"):
            rest = line.split("=", 1)[1] if "=" in line else ""
            partition.source, i = _read_expression(lines, i, rest)
        else:
            i += 1
    return i


def _parse_relationships(text: str) -> list[TmdlRelationship]:
    lines = text.split("\n")
    relationships: list[TmdlRelationship] = []
    current: dict[str, object] = {}

    def flush() -> None:
        if "from_column" in current and "to_column" in current:
            from_table, from_col = _split_qualified_ref(current["from_column"])  # type: ignore[arg-type]
            to_table, to_col = _split_qualified_ref(current["to_column"])  # type: ignore[arg-type]
            relationships.append(
                TmdlRelationship(
                    from_table=from_table,
                    from_column=from_col,
                    to_table=to_table,
                    to_column=to_col,
                    is_active=current.get("is_active", True),  # type: ignore[arg-type]
                )
            )

    for line in lines:
        if _RELATIONSHIP_HEADER_RE.match(line):
            flush()
            current = {}
            continue
        stripped = line.strip()
        if stripped.startswith("fromColumn:"):
            current["from_column"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("toColumn:"):
            current["to_column"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("isActive:"):
            current["is_active"] = stripped.split(":", 1)[1].strip().lower() != "false"
    flush()
    return relationships


def _split_qualified_ref(ref: str) -> tuple[str, str]:
    """Splits a TMDL `Table.column` / `'Table Name'.column` / `Table.'col name'`
    reference into (table, column), unquoting either side."""
    ref = ref.strip()
    if ref.startswith("'"):
        end = ref.index("'", 1)
        while end + 1 < len(ref) and ref[end + 1] == "'":
            end = ref.index("'", end + 2)
        table = ref[1:end].replace("''", "'")
        rest = ref[end + 2 :]  # skip closing quote and the separating '.'
    else:
        dot = ref.index(".")
        table = ref[:dot]
        rest = ref[dot + 1 :]
    column = _unquote(rest)
    return table, column
