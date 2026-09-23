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


def extract_dax_references(expression: str, model: SemanticModelDefinition, host_table: str) -> DaxReferences:
    result = DaxReferences()
    if not expression:
        return result

    text = _strip_dax(expression)
    table_names = {table.name for table in model.tables}
    column_names = {(table.name, column.name) for table in model.tables for column in table.columns}
    measures_by_name: dict[str, list[str]] = {}
    for table in model.tables:
        for measure in table.measures:
            measures_by_name.setdefault(measure.name, []).append(table.name)
    measure_index = {(table.name, measure.name) for table in model.tables for measure in table.measures}
    var_names = set(_VAR_NAME_RE.findall(text))

    consumed_spans: list[tuple[int, int]] = []
    for match in _QUALIFIED_REF_RE.finditer(text):
        quoted, quoted_col, bare, bare_col = match.groups()
        table = quoted.replace("''", "'") if quoted is not None else bare
        column = quoted_col if quoted is not None else bare_col
        consumed_spans.append(match.span())
        if table not in table_names:
            result.unresolved.add(f"{table}[{column}]")
            continue
        if (table, column) in measure_index:
            result.measures.add((table, column))
        else:
            result.columns.add((table, column))

    for match in _BARE_REF_RE.finditer(text):
        if _within(match.start(), consumed_spans):
            continue
        name = match.group(1)
        if name in var_names:
            continue
        owners = measures_by_name.get(name)
        if owners:
            owner = host_table if host_table in owners else owners[0]
            result.measures.add((owner, name))
        elif (host_table, name) in column_names:
            result.columns.add((host_table, name))
        else:
            result.unresolved.add(f"[{name}]")

    for match in _BARE_TOKEN_RE.finditer(text):
        quoted, bare = match.groups()
        name = quoted.replace("''", "'") if quoted is not None else bare
        if name not in table_names:
            continue
        if _within(match.start(), consumed_spans) or text[match.end() : match.end() + 1] == "[":
            continue  # part of (or immediately followed by) a qualified ref, not table-valued
        result.tables.add(name)

    return result


def _within(pos: int, spans: list[tuple[int, int]]) -> bool:
    return any(start <= pos < end for start, end in spans)
