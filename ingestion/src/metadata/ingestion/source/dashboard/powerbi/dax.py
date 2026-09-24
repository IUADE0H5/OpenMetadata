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
Extracts the physical table/column and measure references a single DAX expression
makes, resolved against a parsed semantic model. This is a regex-based reader, not a
DAX parser -- it is deliberately one-hop (a measure's own references only); transitive
closure across measure-to-measure and calculated-column chains lives in
`column_usage.py`, which can cycle-guard across many expressions at once.

Power BI/DAX identifiers (tables, columns, measures) are case-insensitive: `'T'[col]`
and `'T'[COL]` name the same column if the model declares either casing. Every
resolution here is done case-insensitively and always returns the model's own
canonical (TMDL-declared) spelling, never the DAX text's casing, so a downstream
consumer can always match on the exact string the model itself uses.
"""

import re
from dataclasses import dataclass, field

from metadata.ingestion.source.dashboard.powerbi.tmdl import SemanticModelDefinition

_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.S)
_COMMENT_LINE_RE = re.compile(r"//[^\n]*")
_STRING_LITERAL_RE = re.compile(r'"(?:[^"]|"")*"')

# 'Table Name'[Column] or Table[Column].
_QUALIFIED_REF_RE = re.compile(r"'((?:[^']|'')+)'\s*\[([^\]]+)\]|([A-Za-z_]\w*)\s*\[([^\]]+)\]")
# A bare [Column] / [Measure], not the tail of a qualified ref (no identifier or ']'
# immediately before it).
_BARE_REF_RE = re.compile(r"(?<![\]\w'])\[([^\]]+)\]")
# Any bare or quoted identifier token, used to spot table-valued arguments.
_BARE_TOKEN_RE = re.compile(r"'((?:[^']|'')+)'|([A-Za-z_]\w*)")
_VAR_NAME_RE = re.compile(r"\bVAR\s+([A-Za-z_]\w*)")


@dataclass
class DaxReferences:
    columns: set[tuple[str, str]] = field(default_factory=set)
    measures: set[tuple[str, str]] = field(default_factory=set)
    # Tables referenced as a table-valued argument (e.g. `SUMX(T, ...)`), never a column edge.
    tables: set[str] = field(default_factory=set)
    unresolved: set[str] = field(default_factory=set)


def _strip_dax(expression: str) -> str:
    """Removes comments and string-literal contents before any bracket scanning, so a
    literal like `"Cost of [requests]"` can never be mistaken for a column reference."""
    text = _COMMENT_BLOCK_RE.sub(" ", expression)
    text = _COMMENT_LINE_RE.sub("", text)
    return _STRING_LITERAL_RE.sub('""', text)


@dataclass
class _CasefoldIndex:
    """A per-model, case-insensitive lookup built fresh for each call -- bounded by
    the model's own table/column/measure counts, not an accumulating cache."""

    table_by_lower: dict[str, str]
    column_by_lower: dict[tuple[str, str], tuple[str, str]]
    # lower(measure name) -> every (canonical_table, canonical_measure) declaring it,
    # in table-declaration order, for the bare-[X]-prefers-host-table tie-break.
    measures_by_name_lower: dict[str, list[tuple[str, str]]]
    measure_by_lower: dict[tuple[str, str], tuple[str, str]]


def _build_casefold_index(model: SemanticModelDefinition) -> _CasefoldIndex:
    table_by_lower = {table.name.lower(): table.name for table in model.tables}
    column_by_lower = {
        (table.name.lower(), column.name.lower()): (table.name, column.name)
        for table in model.tables
        for column in table.columns
    }
    measures_by_name_lower: dict[str, list[tuple[str, str]]] = {}
    measure_by_lower: dict[tuple[str, str], tuple[str, str]] = {}
    for table in model.tables:
        for measure in table.measures:
            measures_by_name_lower.setdefault(measure.name.lower(), []).append((table.name, measure.name))
            measure_by_lower[(table.name.lower(), measure.name.lower())] = (table.name, measure.name)
    return _CasefoldIndex(table_by_lower, column_by_lower, measures_by_name_lower, measure_by_lower)


def extract_dax_references(expression: str, model: SemanticModelDefinition, host_table: str) -> DaxReferences:
    result = DaxReferences()
    if not expression:
        return result

    text = _strip_dax(expression)
    index = _build_casefold_index(model)
    var_names_lower = {name.lower() for name in _VAR_NAME_RE.findall(text)}
    host_table_lower = host_table.lower()

    consumed_spans: list[tuple[int, int]] = []
    for match in _QUALIFIED_REF_RE.finditer(text):
        quoted, quoted_col, bare, bare_col = match.groups()
        table_raw = quoted.replace("''", "'") if quoted is not None else bare
        column_raw = quoted_col if quoted is not None else bare_col
        consumed_spans.append(match.span())

        canonical_table = index.table_by_lower.get(table_raw.lower())
        if canonical_table is None:
            result.unresolved.add(f"{table_raw}[{column_raw}]")
            continue
        canonical_table_lower = canonical_table.lower()
        measure_key = index.measure_by_lower.get((canonical_table_lower, column_raw.lower()))
        if measure_key is not None:
            result.measures.add(measure_key)
            continue
        column_key = index.column_by_lower.get((canonical_table_lower, column_raw.lower()))
        if column_key is not None:
            result.columns.add(column_key)
        else:
            # The table is real but no column or measure by this name exists under any
            # casing -- a stale/typo'd reference, not a resolvable one.
            result.unresolved.add(f"{table_raw}[{column_raw}]")

    for match in _BARE_REF_RE.finditer(text):
        if _within(match.start(), consumed_spans):
            continue
        name = match.group(1)
        if name.lower() in var_names_lower:
            continue
        candidates = index.measures_by_name_lower.get(name.lower())
        if candidates:
            owner = next((c for c in candidates if c[0].lower() == host_table_lower), candidates[0])
            result.measures.add(owner)
            continue
        column_key = index.column_by_lower.get((host_table_lower, name.lower()))
        if column_key is not None:
            result.columns.add(column_key)
        else:
            result.unresolved.add(f"[{name}]")

    for match in _BARE_TOKEN_RE.finditer(text):
        quoted, bare = match.groups()
        name_raw = quoted.replace("''", "'") if quoted is not None else bare
        canonical_table = index.table_by_lower.get(name_raw.lower())
        if canonical_table is None:
            continue
        if _within(match.start(), consumed_spans) or text[match.end() : match.end() + 1] == "[":
            continue  # part of (or immediately followed by) a qualified ref, not table-valued
        result.tables.add(canonical_table)

    return result


def _within(pos: int, spans: list[tuple[int, int]]) -> bool:
    return any(start <= pos < end for start, end in spans)
