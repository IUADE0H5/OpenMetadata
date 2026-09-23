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
Tests for metadata.ingestion.source.dashboard.powerbi.dax
"""

from metadata.ingestion.source.dashboard.powerbi.dax import extract_dax_references
from metadata.ingestion.source.dashboard.powerbi.tmdl import (
    SemanticModelDefinition,
    TmdlColumn,
    TmdlMeasure,
    TmdlTable,
)


def _model() -> SemanticModelDefinition:
    sales = TmdlTable(
        name="Sales",
        columns=[
            TmdlColumn(name="Amount"),
            TmdlColumn(name="CustomerId"),
            TmdlColumn(name="OrderDate"),
        ],
        measures=[
            TmdlMeasure(name="Total Sales", expression="SUM(Sales[Amount])"),
            TmdlMeasure(
                name="Sales YoY",
                expression="[Total Sales] - CALCULATE([Total Sales], SAMEPERIODLASTYEAR('Date'[Date]))",
            ),
        ],
    )
    customer = TmdlTable(
        name="Customer",
        columns=[TmdlColumn(name="Name"), TmdlColumn(name="Region")],
    )
    date_table = TmdlTable(name="Date", columns=[TmdlColumn(name="Date"), TmdlColumn(name="Year")])
    return SemanticModelDefinition(tables=[sales, customer, date_table])


class TestExtractDaxReferences:
    def test_empty_expression_returns_empty_references(self):
        refs = extract_dax_references("", _model(), "Sales")
        assert refs.columns == set()
        assert refs.measures == set()
        assert refs.tables == set()
        assert refs.unresolved == set()

    def test_quoted_table_column_ref(self):
        refs = extract_dax_references("SUM('Sales'[Amount])", _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_bare_table_column_ref(self):
        refs = extract_dax_references("SUM(Sales[Amount])", _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_qualified_measure_ref(self):
        refs = extract_dax_references("Sales[Total Sales] + 1", _model(), "Sales")
        # "Total Sales" is a measure on Sales, not a column -- resolved as a measure ref.
        assert refs.measures == {("Sales", "Total Sales")}
        assert refs.columns == set()

    def test_bare_measure_ref_resolves_to_owning_table(self):
        refs = extract_dax_references("[Total Sales] * 2", _model(), "Customer")
        assert refs.measures == {("Sales", "Total Sales")}

    def test_bare_column_ref_resolves_to_host_table(self):
        refs = extract_dax_references("[Amount] * 1.1", _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_bare_ref_prefers_host_table_when_ambiguous(self):
        model = _model()
        model.tables[1].measures.append(TmdlMeasure(name="Total Sales", expression="1"))
        refs = extract_dax_references("[Total Sales]", model, "Customer")
        assert refs.measures == {("Customer", "Total Sales")}

    def test_unresolved_qualified_ref_to_unknown_table(self):
        refs = extract_dax_references("Unknown[Thing]", _model(), "Sales")
        assert refs.unresolved == {"Unknown[Thing]"}
        assert refs.columns == set() and refs.measures == set()

    def test_unresolved_bare_ref_to_unknown_name(self):
        refs = extract_dax_references("[NotARealField]", _model(), "Sales")
        assert refs.unresolved == {"[NotARealField]"}

    def test_var_names_are_not_treated_as_refs(self):
        expr = "VAR CurrentAmount = Sales[Amount] RETURN CurrentAmount"
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}
        # "CurrentAmount" is never bracket-referenced, so it can't leak into unresolved.
        assert refs.unresolved == set()

    def test_bare_bracket_matching_a_var_name_is_ignored(self):
        # Defensive: a bare [X] whose X coincides with a declared VAR name is dropped,
        # never counted as a column/measure/unresolved ref.
        expr = "VAR Threshold = 10 RETURN IF([Threshold] > Sales[Amount], 1, 0)"
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}
        assert refs.unresolved == set()

    def test_earlier_wrapper_does_not_block_inner_ref(self):
        expr = "FILTER(Sales, Sales[CustomerId] = EARLIER(Sales[CustomerId]))"
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "CustomerId")}
        assert refs.tables == {"Sales"}

    def test_table_valued_argument_produces_no_column_edge(self):
        refs = extract_dax_references("COUNTROWS(Customer)", _model(), "Sales")
        assert refs.tables == {"Customer"}
        assert refs.columns == set()

    def test_quoted_table_valued_argument(self):
        model = _model()
        model.tables[0].name = "Sales Facts"
        refs = extract_dax_references("COUNTROWS('Sales Facts')", model, "Sales Facts")
        assert refs.tables == {"Sales Facts"}

    def test_line_comment_is_stripped(self):
        expr = "SUM(Sales[Amount]) // Sales[CustomerId] should not count\n"
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_block_comment_is_stripped(self):
        expr = "SUM(Sales[Amount]) /* Sales[CustomerId] */ + 1"
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_string_literal_containing_bracket_is_not_a_ref(self):
        expr = 'FORMAT(Sales[Amount], "Total [Amount]")'
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_string_literal_with_escaped_quotes(self):
        expr = 'Sales[Amount] & "say ""hi"" [not a ref]"'
        refs = extract_dax_references(expr, _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_transitive_chain_is_one_hop_only(self):
        # extract_dax_references is single-hop: it reports the Sales-YoY measure's own
        # direct reference to [Total Sales], not [Total Sales]'s own SUM(Sales[Amount]).
        # Transitive closure across measures is column_usage.py's job.
        model = _model()
        yoy = model.tables[0].measures[1]
        refs = extract_dax_references(yoy.expression, model, "Sales")
        assert refs.measures == {("Sales", "Total Sales")}
        # The measure's own direct 'Date'[Date] ref is still one hop; [Total Sales]'s
        # own SUM(Sales[Amount]) is not pulled in -- that's column_usage.py's job.
        assert refs.columns == {("Date", "Date")}


class TestCaseInsensitiveResolution:
    """Power BI/DAX identifiers are case-insensitive: 'T'[COL] and 'T'[col] name the
    same column if the model declares either casing. Confirmed against real captured
    DAX referencing a column under different casing than the model's own declaration."""

    def test_qualified_ref_with_different_table_and_column_casing_resolves_to_canonical(self):
        refs = extract_dax_references("SUM('SALES'[amount])", _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_qualified_measure_ref_with_different_casing_resolves_to_canonical(self):
        refs = extract_dax_references("sales[total sales]", _model(), "Sales")
        assert refs.measures == {("Sales", "Total Sales")}

    def test_bare_measure_ref_with_different_casing_resolves_to_canonical(self):
        refs = extract_dax_references("[TOTAL SALES] * 2", _model(), "Customer")
        assert refs.measures == {("Sales", "Total Sales")}

    def test_bare_column_ref_with_different_casing_resolves_to_canonical(self):
        refs = extract_dax_references("[amount] * 1.1", _model(), "Sales")
        assert refs.columns == {("Sales", "Amount")}

    def test_table_valued_argument_with_different_casing_resolves_to_canonical(self):
        refs = extract_dax_references("COUNTROWS(customer)", _model(), "Sales")
        assert refs.tables == {"Customer"}

    def test_column_that_does_not_exist_under_any_casing_is_unresolved(self):
        # The table is real; no column or measure by this name exists under any case.
        refs = extract_dax_references("Sales[NotAColumnAtAll]", _model(), "Sales")
        assert refs.unresolved == {"Sales[NotAColumnAtAll]"}
        assert refs.columns == set()
        assert refs.measures == set()
