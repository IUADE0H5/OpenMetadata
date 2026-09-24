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
Maps a semantic-model table's columns back to the Power BI dataflow entity that feeds
it, by reading the table partition's M (Power Query) expression. Handles the
`PowerBI.Dataflows` / `PowerPlatform.Dataflows` navigation chain and the
`Table.RenameColumns` steps that commonly sit between the dataflow's own attribute
names and the model column's `sourceColumn`.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass, field

# A navigation step of the shape `<prev>{[key="value", ...]}[Data]`.
_NAV_STEP_RE = re.compile(r"\{\[\s*(.*?)\s*\]\}\[Data\]", re.S)
_NAV_KV_RE = re.compile(r"(\w+)\s*=\s*\"([^\"]*)\"")
# `Table.RenameColumns(<prev step>, {{"old","new"}, ...})`. Power Query never nests a
# second call inside the rename-pairs list, so a non-greedy match to the first `}}` is
# reliable here even though it wouldn't be for arbitrary M.
_RENAME_CALL_RE = re.compile(r"Table\.RenameColumns\(\s*[^,]+,\s*(\{\{.*?\}\})\s*\)", re.S)
_RENAME_PAIR_RE = re.compile(r'\{\s*"((?:[^"]|"")*)"\s*,\s*"((?:[^"]|"")*)"\s*\}')


@dataclass(frozen=True)
class DataflowSourceRef:
    workspace_id: str
    dataflow_id: str
    entity: str
    # Ordered old->new pairs, in the order the M expression applies them.
    renames: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class ColumnMapping:
    mapped: dict[str, str]
    unmapped: list[str]


def parse_partition_dataflow_source(m_expression: str) -> DataflowSourceRef | None:
    if not m_expression:
        return None

    found: dict[str, str] = {
        key: value
        for nav_step in _NAV_STEP_RE.finditer(m_expression)
        for key, value in _NAV_KV_RE.findall(nav_step.group(1))
    }

    workspace_id = found.get("workspaceId")
    dataflow_id = found.get("dataflowId")
    entity = found.get("entity")
    if not workspace_id or not dataflow_id or not entity:
        return None

    renames: list[tuple[str, str]] = []
    for rename_call in _RENAME_CALL_RE.finditer(m_expression):
        for old, new in _RENAME_PAIR_RE.findall(rename_call.group(1)):
            renames.append((old.replace('""', '"'), new.replace('""', '"')))

    return DataflowSourceRef(workspace_id=workspace_id, dataflow_id=dataflow_id, entity=entity, renames=renames)


def map_columns_to_dataflow(
    columns: Iterable[str],
    source_ref: DataflowSourceRef,
    entity_attribute_names: Iterable[str],
) -> ColumnMapping:
    attribute_names = set(entity_attribute_names)
    # A column's sourceColumn is the *post*-rename name; walk the rename chain
    # backwards to recover the dataflow entity's own attribute name.
    reverse_rename: dict[str, str] = {new: old for old, new in source_ref.renames}

    mapped: dict[str, str] = {}
    unmapped: list[str] = []
    for column in columns:
        original = column
        visited = set()
        while original in reverse_rename and original not in visited:
            visited.add(original)
            original = reverse_rename[original]
        if original in attribute_names:
            mapped[column] = original
        else:
            unmapped.append(column)
    return ColumnMapping(mapped=mapped, unmapped=unmapped)
