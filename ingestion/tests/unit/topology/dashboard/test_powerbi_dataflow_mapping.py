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
Tests for metadata.ingestion.source.dashboard.powerbi.dataflow_mapping
"""

from metadata.ingestion.source.dashboard.powerbi.dataflow_mapping import (
    DataflowSourceRef,
    map_columns_to_dataflow,
    parse_partition_dataflow_source,
)

POWER_BI_DATAFLOWS_SOURCE = """
let
    Source = PowerBI.Dataflows(null),
    ws = Source{[workspaceId="11111111-2222-3333-4444-555555555555"]}[Data],
    df = ws{[dataflowId="66666666-7777-8888-9999-aaaaaaaaaaaa"]}[Data],
    sales_ = df{[entity="sales"]}[Data],
    renamed = Table.RenameColumns(sales_,{{"cust_id", "CustomerId"}, {"amt", "Amount"}})
in
    renamed
"""

POWER_PLATFORM_DATAFLOWS_SOURCE = """
let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{[Id="Workspaces"]}[Data],
    ws = Workspaces{[workspaceId="11111111-2222-3333-4444-555555555555"]}[Data],
    df = ws{[dataflowId="66666666-7777-8888-9999-aaaaaaaaaaaa"]}[Data],
    entity_ = df{[entity="sales", version=""]}[Data]
in
    entity_
"""

CHAINED_RENAMES_SOURCE = """
let
    Source = PowerBI.Dataflows(null),
    ws = Source{[workspaceId="11111111-2222-3333-4444-555555555555"]}[Data],
    df = ws{[dataflowId="66666666-7777-8888-9999-aaaaaaaaaaaa"]}[Data],
    sales_ = df{[entity="sales"]}[Data],
    step1 = Table.RenameColumns(sales_,{{"amt", "Total"}}),
    step2 = Table.RenameColumns(step1,{{"Total", "Amount"}})
in
    step2
"""


class TestParsePartitionDataflowSource:
    def test_power_bi_dataflows_navigation(self):
        ref = parse_partition_dataflow_source(POWER_BI_DATAFLOWS_SOURCE)
        assert ref.workspace_id == "11111111-2222-3333-4444-555555555555"
        assert ref.dataflow_id == "66666666-7777-8888-9999-aaaaaaaaaaaa"
        assert ref.entity == "sales"

    def test_power_platform_dataflows_navigation_with_workspaces_hop(self):
        ref = parse_partition_dataflow_source(POWER_PLATFORM_DATAFLOWS_SOURCE)
        assert ref.workspace_id == "11111111-2222-3333-4444-555555555555"
        assert ref.dataflow_id == "66666666-7777-8888-9999-aaaaaaaaaaaa"
        assert ref.entity == "sales"

    def test_extracts_single_rename_call_pairs(self):
        ref = parse_partition_dataflow_source(POWER_BI_DATAFLOWS_SOURCE)
        assert ref.renames == [("cust_id", "CustomerId"), ("amt", "Amount")]

    def test_extracts_chained_rename_calls_in_order(self):
        ref = parse_partition_dataflow_source(CHAINED_RENAMES_SOURCE)
        assert ref.renames == [("amt", "Total"), ("Total", "Amount")]

    def test_no_rename_calls_gives_empty_list(self):
        ref = parse_partition_dataflow_source(POWER_PLATFORM_DATAFLOWS_SOURCE)
        assert ref.renames == []

    def test_returns_none_for_empty_expression(self):
        assert parse_partition_dataflow_source("") is None
        assert parse_partition_dataflow_source(None) is None

    def test_returns_none_when_not_a_dataflow_source(self):
        assert parse_partition_dataflow_source('let\n  Source = Csv.Document(File.Contents("x"))\nin\n  Source') is None

    def test_returns_none_when_navigation_incomplete(self):
        incomplete = 'let\n  Source = PowerBI.Dataflows(null),\n  ws = Source{[workspaceId="w"]}[Data]\nin\n  ws'
        assert parse_partition_dataflow_source(incomplete) is None


class TestMapColumnsToDataflow:
    def test_direct_match(self):
        ref = DataflowSourceRef("w", "d", "sales", renames=[])
        result = map_columns_to_dataflow(["amount", "customer_id"], ref, ["amount", "customer_id", "region"])
        assert result.mapped == {"amount": "amount", "customer_id": "customer_id"}
        assert result.unmapped == []

    def test_match_through_single_rename(self):
        ref = DataflowSourceRef("w", "d", "sales", renames=[("amt", "Amount")])
        result = map_columns_to_dataflow(["Amount"], ref, ["amt", "cust_id"])
        assert result.mapped == {"Amount": "amt"}

    def test_match_through_rename_chain(self):
        ref = DataflowSourceRef("w", "d", "sales", renames=[("amt", "Total"), ("Total", "Amount")])
        result = map_columns_to_dataflow(["Amount"], ref, ["amt"])
        assert result.mapped == {"Amount": "amt"}

    def test_unmapped_when_no_match(self):
        ref = DataflowSourceRef("w", "d", "sales", renames=[])
        result = map_columns_to_dataflow(["ghost_column"], ref, ["amount"])
        assert result.unmapped == ["ghost_column"]
        assert result.mapped == {}

    def test_cyclic_rename_chain_does_not_infinite_loop(self):
        # Defensive: a malformed/cyclic rename chain must terminate, not hang.
        ref = DataflowSourceRef("w", "d", "sales", renames=[("a", "b"), ("b", "a")])
        result = map_columns_to_dataflow(["a"], ref, ["nonexistent"])
        assert result.unmapped == ["a"]
