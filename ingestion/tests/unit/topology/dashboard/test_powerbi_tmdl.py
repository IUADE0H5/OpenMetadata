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
Tests for metadata.ingestion.source.dashboard.powerbi.tmdl
"""

from metadata.ingestion.source.dashboard.powerbi.tmdl import parse_tmdl

SALES_TABLE = """\
table Sales
\tlineageTag: 11111111-1111-1111-1111-111111111111

\tcolumn Amount
\t\tdataType: double
\t\tsummarizeBy: sum
\t\tsourceColumn: Amount

\t\tannotation SummarizationSetBy = Automatic

\tcolumn OrderDate
\t\tdataType: dateTime
\t\tsourceColumn: OrderDate

\t\tvariation Variation
\t\t\tisDefault
\t\t\trelationship: 77777777-7777-7777-7777-777777777777
\t\t\tdefaultHierarchy: LocalDateTable_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.'Date Hierarchy'

\tcolumn Year = YEAR([OrderDate])
\t\tdataType: int64
\t\tsummarizeBy: none

\tcolumn Quarter = "Q" & [QuarterNo]
\t\tdataType: string
\t\tsortByColumn: QuarterNo

\tmeasure 'Total Sales' = SUM(Sales[Amount])
\t\tformatString: 0

\tmeasure 'Multi Line Measure' = ```
\t\t\tVAR x = 1
\t\t\tRETURN x
\t\t\t```
\t\tformatString: 0

\thierarchy 'Order Hierarchy'
\t\tlineageTag: 22222222-2222-2222-2222-222222222222

\t\tlevel Year
\t\t\tlineageTag: 33333333-3333-3333-3333-333333333333
\t\t\tcolumn: Year

\t\tlevel Quarter
\t\t\tlineageTag: 44444444-4444-4444-4444-444444444444
\t\t\tcolumn: Quarter

\tpartition Sales = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = PowerBI.Dataflows(null),
\t\t\t\t    ws = Source{[workspaceId="11111111-2222-3333-4444-555555555555"]}[Data],
\t\t\t\t    df = ws{[dataflowId="66666666-7777-8888-9999-aaaaaaaaaaaa"]}[Data],
\t\t\t\t    Sales1 = df{[entity="sales_raw"]}[Data]
\t\t\t\tin
\t\t\t\t    Sales1

\tannotation PBI_ResultType = Table
"""

QUOTED_TABLE = """\
table 'Sales (2)'
\tisHidden

\tcolumn 'Order Count'
\t\tdataType: int64
"""

AUTO_DATE_TABLE = """\
table LocalDateTable_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
\tisHidden
\tisPrivate

\tcolumn Date
\t\tdataType: dateTime
\t\tisHidden
"""

RELATIONSHIPS = """\
relationship 55555555-5555-5555-5555-555555555555
\tfromColumn: Sales.OrderDate
\ttoColumn: LocalDateTable_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.Date

relationship 66666666-6666-6666-6666-666666666666
\tcrossFilteringBehavior: bothDirections
\ttoCardinality: many
\tisActive: false
\tfromColumn: 'Sales (2)'.'Order Count'
\ttoColumn: Sales.Amount
"""


def _parts(**extra: bytes):
    parts = {
        "definition/tables/Sales.tmdl": SALES_TABLE.encode("utf-8"),
        "definition/relationships.tmdl": RELATIONSHIPS.encode("utf-8"),
    }
    parts.update(extra)
    return parts


class TestParseTmdl:
    def test_parses_table_name_and_column_basics(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        assert table is not None
        amount = next(c for c in table.columns if c.name == "Amount")
        assert amount.data_type == "double"
        assert amount.source_column == "Amount"
        assert amount.expression is None
        assert amount.is_hidden is False

    def test_parses_calculated_column_inline_expression(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        year = next(c for c in table.columns if c.name == "Year")
        assert year.expression == "YEAR([OrderDate])"

    def test_calculated_column_string_concat_expression_with_equals_in_value(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        quarter = next(c for c in table.columns if c.name == "Quarter")
        assert quarter.expression == '"Q" & [QuarterNo]'
        assert quarter.sort_by_column == "QuarterNo"

    def test_parses_inline_measure(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        measure = next(m for m in table.measures if m.name == "Total Sales")
        assert measure.expression == "SUM(Sales[Amount])"

    def test_parses_fenced_multiline_measure(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        measure = next(m for m in table.measures if m.name == "Multi Line Measure")
        assert "VAR x = 1" in measure.expression
        assert "RETURN x" in measure.expression
        assert "```" not in measure.expression

    def test_parses_hierarchy_with_levels(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        hierarchy = table.hierarchies[0]
        assert hierarchy.name == "Order Hierarchy"
        assert [(level.name, level.column) for level in hierarchy.levels] == [
            ("Year", "Year"),
            ("Quarter", "Quarter"),
        ]

    def test_parses_column_variation_block(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        order_date = next(c for c in table.columns if c.name == "OrderDate")
        assert len(order_date.variations) == 1
        variation = order_date.variations[0]
        assert variation.name == "Variation"
        assert variation.is_default is True
        assert variation.relationship == "77777777-7777-7777-7777-777777777777"
        assert variation.default_hierarchy == (
            "LocalDateTable_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Date Hierarchy",
        )

    def test_column_without_variation_has_empty_list(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        amount = next(c for c in table.columns if c.name == "Amount")
        assert amount.variations == []

    def test_parses_unfenced_multiline_partition_source(self):
        model = parse_tmdl(_parts())
        table = model.table("Sales")
        partition = table.partitions[0]
        assert partition.mode == "import"
        assert "PowerBI.Dataflows(null)" in partition.source
        assert 'entity="sales_raw"' in partition.source
        # The trailing `annotation` line after the block must not leak into the source.
        assert "annotation" not in partition.source

    def test_quoted_table_and_column_names_with_spaces(self):
        model = parse_tmdl(_parts(**{"definition/tables/Sales2.tmdl": QUOTED_TABLE.encode("utf-8")}))
        table = model.table("Sales (2)")
        assert table is not None
        assert table.is_hidden is True
        assert table.columns[0].name == "Order Count"

    def test_auto_date_table_is_flagged(self):
        model = parse_tmdl(
            _parts(
                **{
                    "definition/tables/LocalDate.tmdl": AUTO_DATE_TABLE.encode("utf-8"),
                }
            )
        )
        auto_date = model.table("LocalDateTable_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        assert auto_date.is_auto_date is True
        sales = model.table("Sales")
        assert sales.is_auto_date is False

    def test_show_as_variations_only_flags_auto_date(self):
        text = "table Budget\n\tshowAsVariationsOnly\n\n\tcolumn Value\n\t\tdataType: double\n"
        model = parse_tmdl(_parts(**{"definition/tables/Budget.tmdl": text.encode("utf-8")}))
        assert model.table("Budget").is_auto_date is True

    def test_parses_relationships_with_quoted_and_bare_sides(self):
        model = parse_tmdl(_parts(**{"definition/tables/LocalDate.tmdl": AUTO_DATE_TABLE.encode("utf-8")}))
        rels = {(r.from_table, r.from_column, r.to_table, r.to_column, r.is_active) for r in model.relationships}
        assert (
            "Sales",
            "OrderDate",
            "LocalDateTable_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "Date",
            True,
        ) in rels
        assert ("Sales (2)", "Order Count", "Sales", "Amount", False) in rels

    def test_ignores_parts_outside_tables_and_relationships(self):
        model = parse_tmdl(_parts(**{"definition/model.tmdl": b"model Model\n\tculture: en-US\n"}))
        assert len(model.tables) == 1

    def test_table_lookup_returns_none_for_missing_table(self):
        model = parse_tmdl(_parts())
        assert model.table("DoesNotExist") is None
