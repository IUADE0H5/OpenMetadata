import uuid
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest

from metadata.generated.schema.entity.data.dashboardDataModel import (
    DashboardDataModel,
    DataModelType,
)
from metadata.generated.schema.entity.data.table import Column, DataType
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.generated.schema.type.entityLineage import ColumnLineage
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.generated.schema.type.filterPattern import FilterPattern
from metadata.ingestion.api.models import Either
from metadata.ingestion.models.barrier import Barrier
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.powerbi.constants import is_write_dataset_right
from metadata.ingestion.source.dashboard.powerbi.metadata import PowerbiSource
from metadata.ingestion.source.dashboard.powerbi.models import (
    Dataflow,
    DataflowEntity,
    DataflowEntityAttribute,
    DataflowExportResponse,
    DataflowMashup,
    Datamart,
    Dataset,
    DatasetUpstreamDataflowLink,
    DatasetUpstreamDataflowLinksResponse,
    Datasource,
    DatasourceConnectionDetails,
    Group,
    PowerBiColumns,
    PowerBIDashboard,
    PowerBIDatasetUser,
    PowerBIPrincipal,
    PowerBIReport,
    PowerBiTable,
    PowerBITableSource,
    PowerBIUser,
    PowerBIWorkspaceUser,
    ReportPage,
    Tile,
    UpstreaDataflow,
    UpstreamDatamart,
    Workspaces,
)
from metadata.ingestion.source.dashboard.powerbi.workspace_state import WorkspaceState
from metadata.utils import fqn

MOCK_REDSHIFT_EXP = """
let
Source = AmazonRedshift.Database("redsshift-cluster.redshift.amazonaws.com:5439","dev"),
demo_dbt_jaffle = Source{[Name="demo_dbt_jaffle"]}[Data],
customers_clean1 = demo_dbt_jaffle{[Name="customers_clean"]}[Data]
in
customers_clean1
"""

MOCK_REDSHIFT_EXP_INVALID = """
let
Source = Database("redsshift-cluster.redshift.amazonaws.com:5439","dev"),
demo_dbt_jaffle = Source{[Name="demo_dbt_jaffle"]}[Data],
customers_clean1 = demo_dbt_jaffle{[Name="customers_clean"]}[Data]
in
customers_clean1
"""

MOCK_REDSHIFT_EXP_INVALID_V2 = """
let
Source = AmazonRedshift.Database("redsshift-cluster.redshift.amazonaws.com:5439","dev"),
customers_clean1 = demo_dbt_jaffle{[Name="customers_clean"]}[Data]
in
customers_clean1
"""

EXPECTED_REDSHIFT_RESULT = [
    {
        "database": "dev",
        "schema": "demo_dbt_jaffle",
        "table": "customers_clean",
    }
]


MOCK_SNOWFLAKE_EXP = """let
    Source = Snowflake.Databases("abcd-123.snowflakecomputing.com","COMPUTE_WH"),
    DEMO_STAGE_Database = Source{[Name="DEMO_STAGE",Kind="Database"]}[Data],
    PUBLIC_Schema = DEMO_STAGE_Database{[Name="PUBLIC",Kind="Schema"]}[Data],
    STG_CUSTOMERS_View = PUBLIC_Schema{[Name="STG_CUSTOMERS",Kind="View"]}[Data]
in
    STG_CUSTOMERS_View"""

MOCK_SNOWFLAKE_EXP_INVALID = """let
    Source = Snowflake("abcd-123.snowflakecomputing.com","COMPUTE_WH"),
    DEMO_STAGE_Database = Source{[Name="DEMO_STAGE",Kind="Database"]}[Data],
in
    STG_CUSTOMERS_View"""

EXPECTED_SNOWFLAKE_RESULT = [
    {
        "database": "DEMO_STAGE",
        "schema": "PUBLIC",
        "table": "STG_CUSTOMERS",
    }
]

MOCK_DATABRICKS_EXP = """let
    Source = Databricks.Catalogs(Databricks_Server, Databricks_HTTP_Path, [Catalog = "", Database = ""]),
    test_database = Source{[Name="DEMO_STAGE",Kind="Database"]}[Data],
    test_schema = test_database{[Name="PUBLIC",Kind="Schema"]}[Data],
    test_table = test_schema{[Name="STG_CUSTOMERS",Kind="Table"]}[Data]
in 
    Source"""  # noqa: W291

MOCK_DATABRICKS_MULTICLOUD_EXP = """let
    Source = DatabricksMultiCloud.Catalogs(Databricks_Server, Databricks_HTTP_Path, [Catalog = "", Database = ""]),
    test_database = Source{[Name="DEMO_STAGE",Kind="Database"]}[Data],
    test_schema = test_database{[Name="PUBLIC",Kind="Schema"]}[Data],
    test_table = test_schema{[Name="STG_CUSTOMERS",Kind="Table"]}[Data]
in
    Source"""

MOCK_DATABRICKS_NATIVE_EXP = """let
    Source = Value.NativeQuery(Databricks.Catalogs(Databricks_Server, Databricks_HTTP_Path, [Catalog="DEMO_CATALOG", Database=null, EnableAutomaticProxyDiscovery=null]){[Name="DEMO_STAGE",Kind="Database"]}[Data], "PUBLIC.STG_CUSTOMERS", null, [EnableFolding=true])
in
    Source"""

MOCK_DATABRICKS_NATIVE_QUERY_EXP = """let 
    Source = Value.NativeQuery(Databricks.Catalogs(Databricks_Server, Databricks_HTTP_Path,  
        [Catalog="DEMO_CATALOG", Database=null, EnableAutomaticProxyDiscovery=null]) 
        {[Name="DEMO_STAGE",Kind="Database"]}[Data],  
            "select * from PUBLIC.STG_CUSTOMERS", null, [EnableFolding=true]) 
in 
    "Source" """  # noqa: W291

EXPECTED_DATABRICKS_RESULT = [{"database": "DEMO_STAGE", "schema": "PUBLIC", "table": "STG_CUSTOMERS"}]

MOCK_DATABRICKS_NATIVE_QUERY_EXP_WITH_EXPRESSION = """let
    Source = Value.NativeQuery(Databricks.Catalogs(Databricks_Server, Databricks_HTTP_Path, [   Catalog=   "DEMO_CATALOG", Database=null, EnableAutomaticProxyDiscovery=null]){[Name=DB, Kind=   "Database"]}[Data], "SELECT * FROM PUBLIC.STG_CUSTOMERS", null, [EnableFolding=true])
in
    Source"""
EXPECTED_DATABRICKS_RESULT_WITH_EXPRESSION = [{"database": "MY_DB", "schema": "PUBLIC", "table": "STG_CUSTOMERS"}]


MOCK_DATABRICKS_NATIVE_INVALID_QUERY_EXP = """let
    Source = Value.NativeQuery(Databricks.Catalogs(Databricks_Server, Databricks_HTTP_Path, [Catalog="DEMO_CATALOG", Database=null, EnableAutomaticProxyDiscovery=null]){[Name="DEMO_STAGE",Kind = "Database"]}[Data], "WITH test as (select) Select test", null, [EnableFolding=true])
in
    Source"""

MOCK_DATABRICKS_NATIVE_INVALID_EXP = """let
    Source = Value.NativeQuery(Databricks.Catalogs(Databricks_Server, Databricks_HTTP_Path, [Catalog="DEMO_CATALOG", Database=null, EnableAutomaticProxyDiscovery=null]){[Name="DEMO_STAGE",Kind=  "Database"]}[Data], null, [EnableFolding=true])
in
    Source"""

MOCK_BIGQUERY_DIRECT_EXP = """let
    Source = GoogleBigQuery.Database([BillingProject="my-gcp-project"]),
    project = Source{[Name="my-gcp-project"]}[Data],
    dataset = project{[Name="my_dataset",Kind="Schema"]}[Data],
    table = dataset{[Name="my_table",Kind="Table"]}[Data]
in
    table"""

EXPECTED_BIGQUERY_DIRECT_RESULT = [{"database": "my-gcp-project", "schema": "my_dataset", "table": "my_table"}]

MOCK_BIGQUERY_DIRECT_VIEW_EXP = """let
    Source = GoogleBigQuery.Database([BillingProject="my-gcp-project"]),
    project = Source{[Name="my-gcp-project"]}[Data],
    dataset = project{[Name="analytics",Kind="Schema"]}[Data],
    view = dataset{[Name="daily_stats",Kind="View"]}[Data]
in
    view"""

EXPECTED_BIGQUERY_DIRECT_VIEW_RESULT = [{"database": "my-gcp-project", "schema": "analytics", "table": "daily_stats"}]

MOCK_BIGQUERY_NATIVE_QUERY_EXP = (
    "let\n"
    "    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[BillingProject="my-gcp-project"])'
    '{[Name="my-gcp-project"]}[Data], '
    '"SELECT p.id, p.name#(lf)'
    "FROM `my_dataset.products` AS p#(lf)"
    'JOIN `my_dataset.categories` AS c ON p.category_id = c.id", '
    "null, [EnableFolding=true])\n"
    "in\n"
    "    Source"
)

EXPECTED_BIGQUERY_NATIVE_QUERY_RESULT = [
    {"database": "my-gcp-project", "schema": "my_dataset", "table": "categories"},
    {"database": "my-gcp-project", "schema": "my_dataset", "table": "products"},
]

MOCK_BIGQUERY_NATIVE_QUERY_WITH_COMMENTS_EXP = (
    "let\n"
    "    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[UseStorageApi=false, BillingProject="my-gcp-project"])'
    '{[Name="my-gcp-project"]}[Data], '
    '"WITH cte AS (#(lf)'
    "SELECT id, name#(lf)"
    "FROM `dataset_a.table_one` t1 -- main table#(lf)"
    "JOIN `dataset_b.table_two` t2 ON t1.id = t2.fk_id#(tab)#(lf)"
    ")#(lf)"
    'SELECT * FROM cte", '
    "null, [EnableFolding=true])\n"
    "in\n"
    "    Source"
)

EXPECTED_BIGQUERY_NATIVE_QUERY_WITH_COMMENTS_RESULT = [
    {"database": "my-gcp-project", "schema": "dataset_a", "table": "table_one"},
    {"database": "my-gcp-project", "schema": "dataset_b", "table": "table_two"},
]

MOCK_BIGQUERY_NATIVE_QUERY_MULTI_CTE_EXP = (
    "let\n"
    "    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[BillingProject="prod-project"])'
    '{[Name="prod-project"]}[Data], '
    '"WITH staging AS (#(lf)'
    "SELECT id FROM `raw_data.events`#(lf)"
    "),#(lf)"
    "agg AS (#(lf)"
    "SELECT id FROM staging#(lf)"
    "JOIN `analytics.dimensions` d ON staging.id = d.event_id#(lf)"
    ")#(lf)"
    "SELECT * FROM agg#(lf)"
    'JOIN `reporting.summary` s ON agg.id = s.id", '
    "null, [EnableFolding=true])\n"
    "in\n"
    "    Source"
)

EXPECTED_BIGQUERY_NATIVE_QUERY_MULTI_CTE_RESULT = [
    {"database": "prod-project", "schema": "analytics", "table": "dimensions"},
    {"database": "prod-project", "schema": "raw_data", "table": "events"},
    {"database": "prod-project", "schema": "reporting", "table": "summary"},
]

MOCK_BIGQUERY_INVALID_EXP = """let
    Source = SomeOther.Database("not-bigquery"),
    table = Source{[Name="test"]}[Data]
in
    table"""

MOCK_BIGQUERY_NATIVE_QUERY_WITH_TRANSFORMS_EXP = (
    "let\n"
    "    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[BillingProject="my-project"])'
    '{[Name="my-project"]}[Data], '
    '"SELECT id, name FROM `sales.orders`", '
    "null, [EnableFolding=true]),\n"
    '    #"Replaced Value" = Table.ReplaceValue(Source,"old","new",'
    'Replacer.ReplaceText,{"name"}),\n'
    '    #"Changed Type" = Table.TransformColumnTypes('
    '#"Replaced Value",{{"id", type text}})\n'
    "in\n"
    '    #"Changed Type"'
)

EXPECTED_BIGQUERY_NATIVE_QUERY_WITH_TRANSFORMS_RESULT = [
    {"database": "my-project", "schema": "sales", "table": "orders"}
]

MOCK_BIGQUERY_NATIVE_QUERY_BLOCK_COMMENTS_EXP = (
    "let\n"
    "/*Original source*/\n"
    "//    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[BillingProject="my-gcp-project"])'
    '{[Name="my-gcp-project"]}[Data], '
    '"SELECT * FROM DW_PowerBI.View_MRS_Business_Performance", '
    "null, [EnableFolding=true]),\n"
    "\n"
    "/*New source - with weeks*/\n"
    "    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[BillingProject="my-gcp-project", '
    "UseStorageApi=false])"
    '{[Name="my-gcp-project"]}[Data], '
    '"with migrated as #(lf)(#(lf)#(lf)'
    "  SELECT distinct  en.Master_Account_Holder_ID,#(lf)"
    "case when #(lf)"
    "          count( distinct case when Financial_institution_ID "
    "like '%STRIPE%' then 'STRIPE' else 'Payfac' end) > 1#(lf)"
    "          then 1 else 0 end as migrated_to_stripe,#(lf)"
    "max(case when Financial_institution_ID like '%STRIPE%' "
    "then 1 else 0 end ) ind_stripe#(lf)"
    "FROM DW_Main.View_Fact_MRS_Billing_Postings bp#(lf)"
    "left join `my-gcp-project."
    "DW_Main.View_Dim_Entities` en#(lf)"
    "on en.Entity_ID=bp.Merchant_ID#(lf)"
    "WHERE 1=1#(lf)"
    "AND bp.Posting_Type_Name='OPERATION' and "
    "bp.Posting_Sub_Type_Name='DEBIT'#(lf)"
    "and bp.Merchant_ID IS NOT NULL#(lf)"
    "-- and bp.Merchant_ID in (63501474, 65499418)#(lf)"
    "and bp.Billing_Amount_USD>1--------------new 24.04.2024#(lf)"
    '--and bp.Financial_institution_ID in (""STRIPEUSEP"",'
    '""STRIPEHKEP"")#(lf)'
    "--AND entity_id = 90728196#(lf)"
    "--and Master_Account_Holder_ID = '001Nv00000HXfp9IAD'#(lf)"
    "group by all#(lf)"
    "having count( distinct case when Financial_institution_ID "
    "like '%STRIPE%' then 'STRIPE' else 'Payfac' end) > 1#(lf)"
    "          #(lf)#(lf)"
    ")#(lf)#(lf)#(lf)"
    "select *#(lf)"
    ",case when a.Master_Account_ID=m.Master_Account_Holder_ID "
    "then 1 else 0 end as ind_migrated#(lf)"
    ",case when      DATE_TRUNC(DATE_SUB(CURRENT_DATE(), "
    "INTERVAL 1 MONTH), MONTH)=Month_first_date then  1 else 0 "
    "end as Last_month#(lf)"
    ",case when      DATE_TRUNC(CURRENT_DATE(), MONTH)<>"
    "Month_first_date and extract(quarter from  Month_first_date)"
    "=extract(quarter from  CURRENT_DATE()) and extract(year from "
    " Month_first_date)=extract(year from  CURRENT_DATE()) then "
    " 1 else 0 end as Last_FULL_Q#(lf)"
    ",case when     DATE_TRUNC(CURRENT_DATE(), MONTH)<>"
    "Month_first_date  and extract(year from  Month_first_date)"
    "=extract(year from  CURRENT_DATE()) then  1 else 0 end as "
    "Last_FULL_Y#(lf)from `DA_Business_Analytics_VIEWS_Manual."
    "View_NEW_MRS_check` a#(lf)"
    "left join migrated m #(lf)"
    'on a.Master_Account_ID=m.Master_Account_Holder_ID", '
    "null, [EnableFolding=true]),\n"
    '    #"Added Custom" = Table.AddColumn(Source, "Monthyear", '
    "each Date.StartOfMonth([DayDate])),\n"
    '    #"Changed Type1" = Table.TransformColumnTypes('
    '#"Added Custom",{{"Monthyear", type date}})\n'
    "in\n"
    '    #"Changed Type1"'
)

EXPECTED_BIGQUERY_NATIVE_QUERY_BLOCK_COMMENTS_RESULT = [
    {
        "database": "my-gcp-project",
        "schema": "DA_Business_Analytics_VIEWS_Manual",
        "table": "View_NEW_MRS_check",
    },
    {
        "database": "my-gcp-project",
        "schema": "DW_Main",
        "table": "View_Fact_MRS_Billing_Postings",
    },
    {
        "database": "my-gcp-project",
        "schema": "DW_Main",
        "table": "View_Dim_Entities",
    },
]

# =============================================================================
# Dataflow M Document Test Data
# =============================================================================

# Pattern 1: Sql.Database with inline Query parameter
MOCK_DATAFLOW_INLINE_QUERY_BLOCK = (
    "BookToBill_Unite = let\n"
    '  Source = Sql.Database("dwsql", "dw_integration", '
    '[Query = "  SELECT [AccountID]#(lf)      ,[ProductID]#(lf)'
    "  FROM [DW_Integration].[DataWarehouse].[v_FactUnitePurchases]#(lf)"
    '  where IsDeleted = 0", EnableCrossDatabaseFolding = true]),\n'
    '  #"Changed column type" = Table.TransformColumnTypes(Source, '
    '{{"CycleStartDate", type date}})\n'
    "in\n"
    '  #"Changed column type";\r\n'
)

# Pattern 2: Value.NativeQuery with Sql.Database source
MOCK_DATAFLOW_NATIVE_QUERY_BLOCK = (
    "AccountSalesForceProperties = let\n"
    '    Source = Sql.Database("dwsql", "operationaldatastore"),\n'
    "    AccountSalesForceProperties =\n"
    "        Value.NativeQuery(\n"
    "            Source,\n"
    '            "\n'
    "            SELECT\n"
    "                AccountID,\n"
    "                SalesForceBroadVertical\n"
    "            FROM Snowflake.AccountSalesForceProperties\n"
    '            "\n'
    "        )\n"
    "in\n"
    "    AccountSalesForceProperties;\r\n"
)

# Pattern 3: Sql.Database with Schema/Item catalog access
MOCK_DATAFLOW_CATALOG_ACCESS_BLOCK = (
    "Accounts = let\n"
    '  Source = Sql.Database("dwsql", "dw_datawarehouse"),\n'
    '  dbo_DimAccounts = Source{[Schema = "dbo", Item = "DimAccounts"]}[Data],\n'
    '  #"Removed Other Columns" = Table.SelectColumns(dbo_DimAccounts, '
    '{"AccountKey", "AccountID"})\n'
    "in\n"
    '  #"Removed Other Columns";\r\n'
)

# Non-SQL source (PowerPlatform.Dataflows) - should be skipped
MOCK_DATAFLOW_NON_SQL_BLOCK = (
    '#"Channel Group Mapping(Sharepoint)" = let\r\n'
    "  Source = PowerPlatform.Dataflows([]),\r\n"
    '  #"Navigation 1" = Source{[Id = "Workspaces"]}[Data]\r\n'
    "in\r\n"
    '  #"Navigation 1";\r\n'
)

# Computed query (no Sql.Database) - should be skipped
MOCK_DATAFLOW_COMPUTED_BLOCK = (
    '#"AOP by MRR" = let\n'
    '  Source = #"AOP by MRR(Sharepoint)",\n'
    '  #"Changed Type1" = Table.TransformColumnTypes(Source, '
    '{{"Month", type date}})\n'
    "in\n"
    '  #"Changed Type1";\r\n'
)

# No let block - should be skipped
MOCK_DATAFLOW_NO_LET_BLOCK = (
    "UTC_to_PST = let\n"
    "  Query = (datetimecolumn as datetime) =>\n"
    "let\n"
    "  date = DateTime.Date(datetimecolumn)\n"
    "in\n"
    "  date;\r\n"
)

# Full M document combining multiple patterns
MOCK_DATAFLOW_FULL_DOCUMENT = (
    "section Section1;\r\n"
    "shared "
    + MOCK_DATAFLOW_CATALOG_ACCESS_BLOCK
    + "shared "
    + MOCK_DATAFLOW_INLINE_QUERY_BLOCK
    + "shared "
    + MOCK_DATAFLOW_NON_SQL_BLOCK
    + "shared "
    + MOCK_DATAFLOW_COMPUTED_BLOCK
    + "shared "
    + MOCK_DATAFLOW_NATIVE_QUERY_BLOCK
)

MOCK_DATAFLOW_QUERIES_METADATA = {
    "Accounts": {"queryId": "q1", "queryName": "Accounts", "loadEnabled": True},
    "BookToBill_Unite": {
        "queryId": "q2",
        "queryName": "BookToBill_Unite",
        "loadEnabled": True,
    },
    "Channel Group Mapping(Sharepoint)": {
        "queryId": "q3",
        "queryName": "Channel Group Mapping(Sharepoint)",
    },
    "AOP by MRR": {"queryId": "q4", "queryName": "AOP by MRR", "loadEnabled": True},
    "AccountSalesForceProperties": {
        "queryId": "q5",
        "queryName": "AccountSalesForceProperties",
        "loadEnabled": True,
    },
}

MOCK_DATAFLOW_EXPORT = DataflowExportResponse(
    name="DimensionTables",
    description="Test dataflow",
    version="1.0",
    entities=[
        DataflowEntity(
            name="Accounts",
            description="",
            attributes=[
                DataflowEntityAttribute(name="AccountKey", dataType="int64"),
                DataflowEntityAttribute(name="AccountID", dataType="int64"),
            ],
        ),
        DataflowEntity(
            name="BookToBill_Unite",
            description="",
            attributes=[
                DataflowEntityAttribute(name="AccountID", dataType="int64"),
                DataflowEntityAttribute(name="ProductID", dataType="int64"),
            ],
        ),
        DataflowEntity(
            name="AccountSalesForceProperties",
            description="",
            attributes=[
                DataflowEntityAttribute(name="AccountID", dataType="int64"),
                DataflowEntityAttribute(name="SalesForceBroadVertical", dataType="string"),
            ],
        ),
    ],
    **{
        "pbi:mashup": DataflowMashup(
            document=MOCK_DATAFLOW_FULL_DOCUMENT,
            queriesMetadata=MOCK_DATAFLOW_QUERIES_METADATA,
        )
    },
)

# Dataflow export with no mashup
MOCK_DATAFLOW_EXPORT_NO_MASHUP = DataflowExportResponse(
    name="EmptyDataflow",
    entities=[],
)

# Dataflow export with empty document
MOCK_DATAFLOW_EXPORT_EMPTY_DOC = DataflowExportResponse(
    name="EmptyDocDataflow",
    entities=[],
    **{"pbi:mashup": DataflowMashup(document="", queriesMetadata={})},
)
MOCK_BIGQUERY_NATIVE_QUERY_FQN_BACKTICK_EXP = (
    "let\n"
    "    Source = Value.NativeQuery(GoogleBigQuery.Database("
    '[BillingProject="payoneer-prod-eu-svc-data-016f"])'
    '{[Name="payoneer-prod-eu-svc-data-016f"]}[Data], '
    '"SELECT e.Master_Account_Holder_ID, e.Entity_ID#(lf)'
    "FROM `payoneer-prod-eu-svc-data-016f.DW_Main.View_Dim_Entities` e#(lf)"
    "JOIN `payoneer-prod-eu-svc-data-016f.DW_Main.View_Dim_Master_Accounts` m "
    'ON e.Master_Account_Holder_ID = m.Master_Account_Holder_ID", '
    "null, [EnableFolding=true])\n"
    "in\n"
    "    Source"
)

EXPECTED_BIGQUERY_NATIVE_QUERY_FQN_BACKTICK_RESULT = [
    {
        "database": "payoneer-prod-eu-svc-data-016f",
        "schema": "DW_Main",
        "table": "View_Dim_Entities",
    },
    {
        "database": "payoneer-prod-eu-svc-data-016f",
        "schema": "DW_Main",
        "table": "View_Dim_Master_Accounts",
    },
]

mock_config = {
    "source": {
        "type": "powerbi",
        "serviceName": "mock_metabase",
        "serviceConnection": {
            "config": {
                "type": "PowerBI",
                "clientId": "client_id",
                "clientSecret": "secret",
                "tenantId": "tenant_id",
            },
        },
        "sourceConfig": {
            "config": {
                "type": "DashboardMetadata",
                "includeOwners": True,
            }
        },
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "loggerLevel": "DEBUG",
        "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "openmetadata",
            "enableVersionValidation": "false",
            "securityConfig": {
                "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGc"
                "iOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE"
                "2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXB"
                "iEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fN"
                "r3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3u"
                "d-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
            },
        },
    },
}

MOCK_DASHBOARD_WITH_OWNERS = {
    "id": "dashboard1",
    "displayName": "Test Dashboard",
    "webUrl": "https://test.com",
    "embedUrl": "https://test.com/embed",
    "tiles": [],
    "users": [
        {
            "displayName": "John Doe",
            "emailAddress": "john.doe@example.com",
            "dashboardUserAccessRight": "Owner",
            "userType": "Member",
        },
        {
            "displayName": "Jane Smith",
            "emailAddress": "jane.smith@example.com",
            "dashboardUserAccessRight": "Owner",
            "userType": "Member",
        },
    ],
}

MOCK_DATASET_WITH_OWNERS = {
    "id": "dataset1",
    "name": "Test Dataset",
    "tables": [],
    "description": "Test dataset description",
    "users": [
        {
            "displayName": "John Doe",
            "emailAddress": "john.doe@example.com",
            "datasetUserAccessRight": "Owner",
            "userType": "Member",
        }
    ],
}

MOCK_USER_1_ENITYTY_REF_LIST = EntityReferenceList(
    root=[EntityReference(id=uuid.uuid4(), name="John Doe", type="user")]
)
MOCK_USER_2_ENITYTY_REF_LIST = EntityReferenceList(
    root=[EntityReference(id=uuid.uuid4(), name="Jane Smith", type="user")]
)

MOCK_SNOWFLAKE_EXP_V2 = 'let\n    Source = Snowflake.Databases(Snowflake_URL,Warehouse,[Role=Role]),\n    Database = Source{[Name=DB,Kind="Database"]}[Data],\n    DB_Schema = Database{[Name=Schema,Kind="Schema"]}[Data],\n    Table = DB_Schema{[Name="CUSTOMER_TABLE",Kind="Table"]}[Data],\n    #"Andere entfernte Spalten" = Table.SelectColumns(Table,{"ID_BERICHTSMONAT", "ID_AKQUISE_VERMITTLER", "ID_AKQUISE_OE", "ID_SPARTE", "ID_RISIKOTRAEGER", "ID_KUNDE", "STUECK", "BBE"})\nin\n    #"Andere entfernte Spalten"'
MOCK_SNOWFLAKE_EXP_V3 = 'let\n    Source = Snowflake.Databases(Snowflake_URL,Warehouse,[Role=Role]),\n    Database = Source{[Name=P_Database_name,Kind="Database"]}[Data],\n    DB_Schema = Database{[Name=P_Schema_name,Kind="Schema"]}[Data],\n    Table = DB_Schema{[Name="CUSTOMER_TABLE",Kind="Table"]}[Data],\n    #"Andere entfernte Spalten" = Table.SelectColumns(Table,{"ID_BERICHTSMONAT", "ID_AKQUISE_VERMITTLER", "ID_AKQUISE_OE", "ID_SPARTE", "ID_RISIKOTRAEGER", "ID_KUNDE", "STUECK", "BBE"})\nin\n    #"Andere entfernte Spalten"'
EXPECTED_SNOWFLAKE_RESULT_V2 = [
    {
        "database": "MY_DB",
        "schema": "MY_SCHEMA",
        "table": "CUSTOMER_TABLE",
    }
]
MOCK_DATASET_FROM_WORKSPACE = Dataset(
    id="testdataset",
    name="Test Dataset",
    tables=[],
    expressions=[
        {
            "name": "DB",
            "expression": '"MY_DB" meta [IsParameterQuery=true, List={"MY_DB_DEV", "MY_DB", "MY_DB_PROD"}, DefaultValue="MY_DB", Type="Text", IsParameterQueryRequired=true]',
        },
        {
            "name": "Schema",
            "expression": '"MY_SCHEMA" meta [IsParameterQuery=true, List={"MY_SCHEMA", "MY_SCHEMA_PROD"}, DefaultValue="MY_SCHEMA", Type="Text", IsParameterQueryRequired=true]',
        },
    ],
)
MOCK_DATASET_FROM_WORKSPACE_V2 = Dataset(
    id="testdataset",
    name="Test Dataset",
    tables=[],
    expressions=[
        {
            "name": "DB",
        },
        {
            "name": "Schema",
        },
    ],
)
MOCK_DATASET_FROM_WORKSPACE_V3 = Dataset(
    id="testdataset",
    name="Test Dataset",
    tables=[],
    expressions=[
        {
            "name": "P_Database_name",
            "description": "The parameter contains the name of the database",
            "expression": '"MANUFACTURING_BUSINESS_DATA_PRODUCTS" meta [IsParameterQuery=true, List={"DEVELOPMENT_BUSINESS_DATA_PRODUCTS", "MANUFACTURING_BUSINESS_DATA_PRODUCTS"}, DefaultValue="DEVELOPMENT_BUSINESS_DATA_PRODUCTS", Type="Text", IsParameterQueryRequired=true]',
        },
        {
            "name": "P_Schema_name",
            "description": "The parameter contains the schema name",
            "expression": '"INVENTORY_BY_PURPOSE" meta [IsParameterQuery=true, List={"MVANGENE_INVENTORY_BY_PURPOSE", "INVENTORY_BY_PURPOSE", "ANORRBRI_INVENTORY_BY_PURPOSE"}, DefaultValue="MVANGENE_INVENTORY_BY_PURPOSE", Type="Text", IsParameterQueryRequired=true]',
        },
    ],
)
MOCK_DASHBOARD_DATA_MODEL = DashboardDataModel(
    name="dummy_datamodel",
    id=uuid.uuid4(),
    columns=[],
    dataModelType=DataModelType.PowerBIDataModel.value,
)
MOCK_DATAMODEL_ENTITY = DashboardDataModel(
    name="dummy_dataflow_id_a",
    id=uuid.uuid4(),
    dataModelType=DataModelType.PowerBIDataFlow.value,
    columns=[],
)
MOCK_DATAMART = Datamart(
    id="datamart_b",
    name="sales_datamart",
    description="Sales datamart",
    modifiedBy="jane.doe@example.com",
    users=[
        PowerBIUser(
            displayName="Jane Doe",
            emailAddress="jane.doe@example.com",
            datamartUserAccessRight="ReadWriteReshare",
            userType="Member",
        )
    ],
    upstreamDatamarts=[
        UpstreamDatamart(
            groupId="ws-1",
            targetDatamartId="datamart_a",
        ),
        UpstreamDatamart(
            groupId="ws-1",
            targetDatamartId="datamart_b",
        ),
    ],
)

# --- Athena / ODBC-to-Athena PowerBI connector fixtures (neutral names) ---

# Three-level AmazonAthena.Databases navigation: AwsDataCatalog placeholder ->
# Glue database -> table. `#"Navigation N"` step names, as the real connector emits.
MOCK_ATHENA_NAV_EXP = """let
    Source = AmazonAthena.Databases("warehouse-dsn", null, []),
    #"Navigation 1" = Source{[Name = "AwsDataCatalog", Kind = "Database"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Name = "sales_db", Kind = "Schema"]}[Data],
    #"Navigation 3" = #"Navigation 2"{[Name = "orders", Kind = "Table"]}[Data]
in
    #"Navigation 3"
"""

# Same shape, but the Database level is a real federated catalog, not the
# AwsDataCatalog placeholder - it should be returned as the database.
MOCK_ATHENA_NAV_FEDERATED_CATALOG_EXP = """let
    Source = AmazonAthena.Databases("analytics-dsn", null, []),
    Navigation1 = Source{[Name = "external_catalog", Kind = "Database"]}[Data],
    Navigation2 = Navigation1{[Name = "sales_db", Kind = "Schema"]}[Data],
    Navigation3 = Navigation2{[Name = "orders", Kind = "Table"]}[Data]
in
    Navigation3
"""

# Kind="View" instead of "Table", and unnumbered/unquoted step names.
MOCK_ATHENA_NAV_VIEW_EXP = """let
    Source = AmazonAthena.Databases("warehouse-dsn", null, []),
    Navigation = Source{[Name = "AwsDataCatalog", Kind = "Database"]}[Data],
    Navigation1 = Navigation{[Name = "sales_db", Kind = "Schema"]}[Data],
    Navigation2 = Navigation1{[Name = "orders_view", Kind = "View"]}[Data]
in
    Navigation2
"""

# The only native-SQL shape observed in live capture: Odbc.Query with a DSN
# and double-quoted schema/table identifiers.
MOCK_ODBC_QUERY_EXP = (
    "let\n"
    '    Source = Odbc.Query("dsn=warehouse-dsn", "select * from ""sales_db"".""orders"" limit 10;")\n'
    "in\n"
    "    Source\n"
)

MOCK_DATAFLOW_ATHENA_BLOCK = (
    "Orders = let\n"
    '  Source = AmazonAthena.Databases("warehouse-dsn", null, []),\n'
    '  #"Navigation 1" = Source{[Name = "AwsDataCatalog", Kind = "Database"]}[Data],\n'
    '  #"Navigation 2" = #"Navigation 1"{[Name = "sales_db", Kind = "Schema"]}[Data],\n'
    '  #"Navigation 3" = #"Navigation 2"{[Name = "orders", Kind = "Table"]}[Data]\n'
    "in\n"
    '  #"Navigation 3";\r\n'
)

MOCK_DATAFLOW_ATHENA_SECOND_DSN_BLOCK = (
    "Customers = let\n"
    '  Source = AmazonAthena.Databases("analytics-dsn", null, []),\n'
    '  #"Navigation 1" = Source{[Name = "AwsDataCatalog", Kind = "Database"]}[Data],\n'
    '  #"Navigation 2" = #"Navigation 1"{[Name = "crm_db", Kind = "Schema"]}[Data],\n'
    '  #"Navigation 3" = #"Navigation 2"{[Name = "customers", Kind = "Table"]}[Data]\n'
    "in\n"
    '  #"Navigation 3";\r\n'
)

MOCK_DATAFLOW_ODBC_QUERY_BLOCK = (
    "OrdersOdbc = let\n"
    '  Source = Odbc.Query("dsn=warehouse-dsn", "select * from ""sales_db"".""orders"" limit 10;")\n'
    "in\n"
    "  Source;\r\n"
)

MOCK_DATAFLOW_ATHENA_QUERIES_METADATA = {
    "Orders": {"queryId": "q1", "queryName": "Orders", "loadEnabled": True},
    "Customers": {"queryId": "q2", "queryName": "Customers", "loadEnabled": True},
    "OrdersOdbc": {"queryId": "q3", "queryName": "OrdersOdbc", "loadEnabled": True},
}

MOCK_DATAFLOW_ATHENA_DOCUMENT = (
    "section Section1;\r\n"
    "shared "
    + MOCK_DATAFLOW_ATHENA_BLOCK
    + "shared "
    + MOCK_DATAFLOW_ATHENA_SECOND_DSN_BLOCK
    + "shared "
    + MOCK_DATAFLOW_ODBC_QUERY_BLOCK
)

MOCK_DATAFLOW_EXPORT_ATHENA = DataflowExportResponse(
    name="AthenaDataflow",
    description="Test Athena dataflow",
    version="1.0",
    entities=[
        DataflowEntity(
            name="Orders",
            description="",
            attributes=[
                DataflowEntityAttribute(name="OrderId", dataType="int64"),
                DataflowEntityAttribute(name="Amount", dataType="double"),
            ],
        ),
    ],
    **{
        "pbi:mashup": DataflowMashup(
            document=MOCK_DATAFLOW_ATHENA_DOCUMENT,
            queriesMetadata=MOCK_DATAFLOW_ATHENA_QUERIES_METADATA,
        )
    },
)

# --- Counter-reconciliation fixtures (neutral names) ---

# One disabled Athena query (would have been lineage-relevant) plus one
# disabled non-SQL helper query (SharePoint - never lineage-relevant), to
# prove the skip counter only counts the former.
MOCK_DATAFLOW_LOAD_DISABLED_ATHENA_BLOCK = (
    "DisabledOrders = let\n"
    '  Source = AmazonAthena.Databases("warehouse-dsn", null, []),\n'
    '  #"Navigation 1" = Source{[Name = "AwsDataCatalog", Kind = "Database"]}[Data],\n'
    '  #"Navigation 2" = #"Navigation 1"{[Name = "sales_db", Kind = "Schema"]}[Data],\n'
    '  #"Navigation 3" = #"Navigation 2"{[Name = "orders_disabled", Kind = "Table"]}[Data]\n'
    "in\n"
    '  #"Navigation 3";\r\n'
)

MOCK_DATAFLOW_LOAD_DISABLED_HELPER_BLOCK = (
    "DisabledHelper = let\n"
    '  Source = SharePoint.Files("https://example.sharepoint.com/sites/test", [ApiVersion = 15]),\n'
    "  Filtered = Table.SelectRows(Source, each true)\n"
    "in\n"
    "  Filtered;\r\n"
)

MOCK_DATAFLOW_LOAD_DISABLED_DOCUMENT = (
    "section Section1;\r\n"
    "shared " + MOCK_DATAFLOW_LOAD_DISABLED_ATHENA_BLOCK + "shared " + MOCK_DATAFLOW_LOAD_DISABLED_HELPER_BLOCK
)

MOCK_DATAFLOW_LOAD_DISABLED_QUERIES_METADATA = {
    "DisabledOrders": {"queryId": "q1", "queryName": "DisabledOrders", "loadEnabled": None},
    "DisabledHelper": {"queryId": "q2", "queryName": "DisabledHelper"},
}

MOCK_DATAFLOW_EXPORT_LOAD_DISABLED = DataflowExportResponse(
    name="DisabledQueriesDataflow",
    entities=[],
    **{
        "pbi:mashup": DataflowMashup(
            document=MOCK_DATAFLOW_LOAD_DISABLED_DOCUMENT,
            queriesMetadata=MOCK_DATAFLOW_LOAD_DISABLED_QUERIES_METADATA,
        )
    },
)

# The same table (sales_db.user_activity) reached via two different M
# queries in one dataflow: an Athena navigation and an Odbc.Query - the
# "test query for api" pattern seen in the real capture.
MOCK_DATAFLOW_DUPLICATE_REF_NAV_BLOCK = (
    "ActivityNav = let\n"
    '  Source = AmazonAthena.Databases("warehouse-dsn", null, []),\n'
    '  #"Navigation 1" = Source{[Name = "AwsDataCatalog", Kind = "Database"]}[Data],\n'
    '  #"Navigation 2" = #"Navigation 1"{[Name = "sales_db", Kind = "Schema"]}[Data],\n'
    '  #"Navigation 3" = #"Navigation 2"{[Name = "user_activity", Kind = "Table"]}[Data]\n'
    "in\n"
    '  #"Navigation 3";\r\n'
)

MOCK_DATAFLOW_DUPLICATE_REF_ODBC_BLOCK = (
    "ActivityOdbc = let\n"
    '  Source = Odbc.Query("dsn=warehouse-dsn", "select * from ""sales_db"".""user_activity"" limit 10;")\n'
    "in\n"
    "  Source;\r\n"
)

MOCK_DATAFLOW_DUPLICATE_REF_DOCUMENT = (
    "section Section1;\r\n"
    "shared " + MOCK_DATAFLOW_DUPLICATE_REF_NAV_BLOCK + "shared " + MOCK_DATAFLOW_DUPLICATE_REF_ODBC_BLOCK
)

MOCK_DATAFLOW_DUPLICATE_REF_QUERIES_METADATA = {
    "ActivityNav": {"queryId": "q1", "queryName": "ActivityNav", "loadEnabled": True},
    "ActivityOdbc": {"queryId": "q2", "queryName": "ActivityOdbc", "loadEnabled": True},
}

MOCK_DATAFLOW_EXPORT_DUPLICATE_REF = DataflowExportResponse(
    name="DuplicateRefDataflow",
    entities=[
        DataflowEntity(
            name="ActivityNav",
            attributes=[DataflowEntityAttribute(name="UserId", dataType="int64")],
        ),
    ],
    **{
        "pbi:mashup": DataflowMashup(
            document=MOCK_DATAFLOW_DUPLICATE_REF_DOCUMENT,
            queriesMetadata=MOCK_DATAFLOW_DUPLICATE_REF_QUERIES_METADATA,
        )
    },
)


# --- Non-admin owner resolution fixtures (neutral names) ---

MOCK_WORKSPACE_USER_CONTRIBUTOR = PowerBIWorkspaceUser(
    identifier="ada@example.com",
    principalType="User",
    displayName="Ada",
    emailAddress="ada@example.com",
    groupUserAccessRight="Contributor",
)
MOCK_WORKSPACE_USER_VIEWER = PowerBIWorkspaceUser(
    identifier="viewer@example.com",
    principalType="User",
    displayName="Viewer",
    emailAddress="viewer@example.com",
    groupUserAccessRight="Viewer",
)
MOCK_WORKSPACE_GROUP = PowerBIWorkspaceUser(
    identifier="group-object-id",
    principalType="Group",
    displayName="Analytics Team",
    groupUserAccessRight="Member",
)
MOCK_WORKSPACE_APP = PowerBIWorkspaceUser(
    identifier="app-object-id",
    principalType="App",
    displayName="Some App",
    groupUserAccessRight="Admin",
)


class PowerBIUnitTest(TestCase):
    """
    Implements the necessary methods to extract
    powerbi Dashboard Unit Test
    """

    @patch("metadata.ingestion.source.dashboard.dashboard_service.DashboardServiceSource.test_connection")
    @patch("metadata.ingestion.source.dashboard.dashboard_service.create_connection")
    def __init__(self, methodName, create_connection, test_connection) -> None:  # noqa: N803
        super().__init__(methodName)
        create_connection.return_value.client = False
        test_connection.return_value = False
        self.config = OpenMetadataWorkflowConfig.model_validate(mock_config)
        self.powerbi: PowerbiSource = PowerbiSource.create(
            mock_config["source"],
            OpenMetadata(self.config.workflowConfig.openMetadataServerConfig),
        )

    @pytest.mark.order(1)
    @patch.object(
        WorkspaceState,
        "find_dataset",
        return_value=MOCK_DATASET_FROM_WORKSPACE,
    )
    def test_parse_database_source(self, *_):
        # Test with valid redshift source
        result = self.powerbi._parse_redshift_source(MOCK_REDSHIFT_EXP)
        self.assertEqual(result, EXPECTED_REDSHIFT_RESULT)

        # Test with invalid redshift source
        result = self.powerbi._parse_redshift_source(MOCK_REDSHIFT_EXP_INVALID)
        self.assertEqual(result, None)

        # Test with invalid redshift source
        result = self.powerbi._parse_redshift_source(MOCK_REDSHIFT_EXP_INVALID_V2)
        self.assertEqual(result, None)

        # Test with valid snowflake source
        result = self.powerbi._parse_snowflake_source(MOCK_SNOWFLAKE_EXP, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_SNOWFLAKE_RESULT)

        # Test with invalid snowflake source
        result = self.powerbi._parse_snowflake_source(MOCK_SNOWFLAKE_EXP_INVALID, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, None)

        result = self.powerbi._parse_snowflake_source(MOCK_SNOWFLAKE_EXP_V2, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_SNOWFLAKE_RESULT_V2)

        test_snowflaek_query_expression = 'let\n    Source = Value.NativeQuery(Snowflake.Databases("dummy_host",(Warehouse)){[Name=(Database)]}[Data], "select * from "& Database &".""STG"".""STATIC_AOPANDLE""", null, [EnableFolding=true]),\n    #"Renamed Columns" = Table.RenameColumns(Source,{{"AOP_IMPRESSIONS", "AOP Impressions"}, {"AOP_ORDERS", "AOP Orders"}, {"AOP_SPEND", "AOP Spend"}, {"AOP_TOTAL_REV", "AOP Total Revenue"}, {"AOP_UNITS", "AOP Units"}, {"AOP_VISITS", "AOP Visits"}, {"LE_IMPRESSIONS", "LE Impressions"}, {"LE_ORDERS", "LE Orders"}, {"LE_SPEND", "LE Spend"}, {"LE_TOTAL_REV", "LE Total Revenue"}, {"LE_UNITS", "LE Units"}, {"LE_VISITS", "LE Visits"}, {"SITEID", "SiteID"}, {"COUNTRY", "Country"}, {"REGION", "Region"}, {"CHANNEL", "Channel"}, {"DATE", "Date"}, {"AOP_CONV", "AOP_Conv"}, {"LE_CONV", "LE_Conv"}}),\n    #"Changed Type" = Table.TransformColumnTypes(#"Renamed Columns",{{"SiteID", type text}, {"AOP Impressions", type number}, {"AOP Visits", type number}, {"AOP Orders", type number}, {"AOP Units", type number}, {"AOP Total Revenue", type number}, {"AOP Spend", type number}, {"AOP_Conv", type number}, {"AOP_UPT", type number}, {"AOP_ASP", type number}, {"AOP_AOV", type number}, {"AOP_CTR", type number}, {"LE Impressions", type number}, {"LE Visits", type number}, {"LE Orders", type number}, {"LE Units", type number}, {"LE Total Revenue", type number}, {"LE Spend", type number}, {"LE_Conv", type number}, {"LE_UPT", type number}, {"LE_ASP", type number}, {"LE_AOV", type number}, {"LE_CTR", type number}}),\n    #"Duplicated Column" = Table.DuplicateColumn(#"Changed Type", "Date", "Date - Copy"),\n    #"Split Column by Delimiter" = Table.SplitColumn(#"Duplicated Column", "Date - Copy", Splitter.SplitTextByDelimiter("-", QuoteStyle.None), {"Date - Copy.1", "Date - Copy.2", "Date - Copy.3"}),\n    #"Changed Type1" = Table.TransformColumnTypes(#"Split Column by Delimiter",{{"Date - Copy.1", type text}, {"Date - Copy.2", type text}, {"Date - Copy.3", type text}}),\n    #"Inserted Merged Column" = Table.AddColumn(#"Changed Type1", "Merged", each Text.Combine({[#"Date - Copy.1"], [#"Date - Copy.2"], [#"Date - Copy.3"]}, ""), type text),\n    #"Renamed Columns1" = Table.RenameColumns(#"Inserted Merged Column",{{"Merged", "DateKey"}}),\n    #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns1",{"Date - Copy.1", "Date - Copy.2", "Date - Copy.3"}),\n    #"Added Custom" = Table.AddColumn(#"Removed Columns", "Brand", each "CROCS"),\n    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom",{{"Brand", type text}})\nin\n    #"Changed Type2"'
        result = self.powerbi._parse_snowflake_source(test_snowflaek_query_expression, MOCK_DASHBOARD_DATA_MODEL)
        # Test should parse the Snowflake query and extract table info
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        result_table = result[0]
        self.assertEqual(result_table.get("schema"), "STG")
        self.assertEqual(result_table.get("table"), "STATIC_AOPANDLE")

        # Test with valid databricks native source
        result = self.powerbi._parse_databricks_source(
            MOCK_DATABRICKS_NATIVE_QUERY_EXP_WITH_EXPRESSION, MOCK_DASHBOARD_DATA_MODEL
        )
        self.assertEqual(result, EXPECTED_DATABRICKS_RESULT_WITH_EXPRESSION)

        result = self.powerbi._parse_databricks_source(MOCK_DATABRICKS_NATIVE_EXP, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_DATABRICKS_RESULT)

        result = self.powerbi._parse_databricks_source(MOCK_DATABRICKS_NATIVE_QUERY_EXP, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_DATABRICKS_RESULT)

        result = self.powerbi._parse_databricks_source(MOCK_DATABRICKS_EXP, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_DATABRICKS_RESULT)

        result = self.powerbi._parse_databricks_source(MOCK_DATABRICKS_MULTICLOUD_EXP, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_DATABRICKS_RESULT)

        result = self.powerbi._parse_databricks_source(
            MOCK_DATABRICKS_NATIVE_INVALID_QUERY_EXP, MOCK_DASHBOARD_DATA_MODEL
        )
        # sqlglot parses this sql and returns empty source list vs sqlfluff raising the error, hence adjusting test
        self.assertEqual(result, [])

        result = self.powerbi._parse_databricks_source(MOCK_DATABRICKS_NATIVE_INVALID_EXP, MOCK_DASHBOARD_DATA_MODEL)
        self.assertIsNone(result)

        # Test with valid BigQuery direct navigation source
        table = PowerBiTable(name="test_table")
        result = self.powerbi._parse_bigquery_source(MOCK_BIGQUERY_DIRECT_EXP, MOCK_DASHBOARD_DATA_MODEL, table)
        self.assertEqual(result, EXPECTED_BIGQUERY_DIRECT_RESULT)

        # Test with BigQuery direct navigation source (View)
        result = self.powerbi._parse_bigquery_source(MOCK_BIGQUERY_DIRECT_VIEW_EXP, MOCK_DASHBOARD_DATA_MODEL, table)
        self.assertEqual(result, EXPECTED_BIGQUERY_DIRECT_VIEW_RESULT)

        # Test with BigQuery Value.NativeQuery source
        result = self.powerbi._parse_bigquery_source(MOCK_BIGQUERY_NATIVE_QUERY_EXP, MOCK_DASHBOARD_DATA_MODEL, table)
        self.assertEqual(result, EXPECTED_BIGQUERY_NATIVE_QUERY_RESULT)

        # Test with BigQuery NativeQuery containing SQL comments and #(tab)
        result = self.powerbi._parse_bigquery_source(
            MOCK_BIGQUERY_NATIVE_QUERY_WITH_COMMENTS_EXP,
            MOCK_DASHBOARD_DATA_MODEL,
            table,
        )
        self.assertEqual(result, EXPECTED_BIGQUERY_NATIVE_QUERY_WITH_COMMENTS_RESULT)

        # Test with BigQuery NativeQuery with multiple CTEs
        result = self.powerbi._parse_bigquery_source(
            MOCK_BIGQUERY_NATIVE_QUERY_MULTI_CTE_EXP,
            MOCK_DASHBOARD_DATA_MODEL,
            table,
        )
        self.assertEqual(result, EXPECTED_BIGQUERY_NATIVE_QUERY_MULTI_CTE_RESULT)

        # Test with non-BigQuery expression returns None
        result = self.powerbi._parse_bigquery_source(MOCK_BIGQUERY_INVALID_EXP, MOCK_DASHBOARD_DATA_MODEL, table)
        self.assertIsNone(result)

        # Test with BigQuery NativeQuery followed by Table transforms
        result = self.powerbi._parse_bigquery_source(
            MOCK_BIGQUERY_NATIVE_QUERY_WITH_TRANSFORMS_EXP,
            MOCK_DASHBOARD_DATA_MODEL,
            table,
        )
        self.assertEqual(result, EXPECTED_BIGQUERY_NATIVE_QUERY_WITH_TRANSFORMS_RESULT)

        # Test with BigQuery NativeQuery containing block comments and commented-out source
        result = self.powerbi._parse_bigquery_source(
            MOCK_BIGQUERY_NATIVE_QUERY_BLOCK_COMMENTS_EXP,
            MOCK_DASHBOARD_DATA_MODEL,
            table,
        )
        self.assertEqual(result, EXPECTED_BIGQUERY_NATIVE_QUERY_BLOCK_COMMENTS_RESULT)

        # Test with BigQuery NativeQuery using fully-qualified backtick-quoted tables
        # e.g. `project.dataset.table` — parser returns "project.dataset" as schema,
        # which must be split into database and schema
        result = self.powerbi._parse_bigquery_source(
            MOCK_BIGQUERY_NATIVE_QUERY_FQN_BACKTICK_EXP,
            MOCK_DASHBOARD_DATA_MODEL,
            table,
        )
        self.assertEqual(result, EXPECTED_BIGQUERY_NATIVE_QUERY_FQN_BACKTICK_RESULT)

    @pytest.mark.order(2)
    @patch("metadata.ingestion.ometa.ometa_api.OpenMetadata.get_reference_by_email")
    def test_owner_ingestion(self, get_reference_by_email):
        # Mock responses for dashboard owners
        self.powerbi.metadata.get_reference_by_email.side_effect = [
            MOCK_USER_1_ENITYTY_REF_LIST,
            MOCK_USER_2_ENITYTY_REF_LIST,
        ]
        # Test dashboard owner ingestion
        dashboard = PowerBIDashboard.model_validate(MOCK_DASHBOARD_WITH_OWNERS)
        owner_ref = self.powerbi.get_owner_ref(dashboard)
        self.assertIsNotNone(owner_ref)
        self.assertEqual(len(owner_ref.root), 2)
        self.assertEqual(owner_ref.root[0].name, "John Doe")
        self.assertEqual(owner_ref.root[1].name, "Jane Smith")

        # Verify get_reference_by_email was called with correct emails
        self.powerbi.metadata.get_reference_by_email.assert_any_call("john.doe@example.com")
        self.powerbi.metadata.get_reference_by_email.assert_any_call("jane.smith@example.com")

        # Reset mock for dataset test
        self.powerbi.metadata.get_reference_by_email.reset_mock()
        self.powerbi.metadata.get_reference_by_email.side_effect = [MOCK_USER_1_ENITYTY_REF_LIST]

        # Test dataset owner ingestion
        dataset = Dataset.model_validate(MOCK_DATASET_WITH_OWNERS)
        owner_ref = self.powerbi.get_owner_ref(dataset)
        self.assertIsNotNone(owner_ref.root)
        self.assertEqual(len(owner_ref.root), 1)
        self.assertEqual(owner_ref.root[0].name, "John Doe")

        # Verify get_reference_by_email was called with correct email
        self.powerbi.metadata.get_reference_by_email.assert_called_once_with("john.doe@example.com")

        # Reset mock for no owners test
        self.powerbi.metadata.get_reference_by_email.reset_mock()

        # Test with no owners
        dashboard_no_owners = PowerBIDashboard.model_validate(
            {
                "id": "dashboard2",
                "displayName": "Test Dashboard 2",
                "webUrl": "https://test.com",
                "embedUrl": "https://test.com/embed",
                "tiles": [],
                "users": [],
            }
        )
        owner_ref = self.powerbi.get_owner_ref(dashboard_no_owners)
        self.assertIsNone(owner_ref)

        # Verify get_reference_by_email was not called when there are no owners
        self.powerbi.metadata.get_reference_by_email.assert_not_called()

        # Reset mock for invalid owners test
        self.powerbi.metadata.get_reference_by_email.reset_mock()
        # Test with invalid owners
        dashboard_invalid_owners = PowerBIDashboard.model_validate(
            {
                "id": "dashboard3",
                "displayName": "Test Dashboard 3",
                "webUrl": "https://test.com",
                "embedUrl": "https://test.com/embed",
                "tiles": [],
                "users": [
                    {
                        "displayName": "Kane Williams",
                        "emailAddress": "kane.williams@example.com",
                        "dashboardUserAccessRight": "Read",
                        "userType": "Member",
                    },
                ],
            }
        )
        owner_ref = self.powerbi.get_owner_ref(dashboard_invalid_owners)
        self.assertIsNone(owner_ref)

        # Verify get_reference_by_email was not called when there are no owners
        self.powerbi.metadata.get_reference_by_email.assert_not_called()

    @pytest.mark.order(3)
    def test_parse_table_info_from_source_exp(self):
        table = PowerBiTable(
            name="test_table",
            source=[PowerBITableSource(expression=MOCK_REDSHIFT_EXP)],
        )
        result = self.powerbi._parse_table_info_from_source_exp(table, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, EXPECTED_REDSHIFT_RESULT)

        # no source expression
        table = PowerBiTable(
            name="test_table",
            source=[PowerBITableSource(expression=None)],
        )
        result = self.powerbi._parse_table_info_from_source_exp(table, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, None)

        # no source
        table = PowerBiTable(
            name="test_table",
            source=[],
        )
        result = self.powerbi._parse_table_info_from_source_exp(table, MOCK_DASHBOARD_DATA_MODEL)
        self.assertEqual(result, None)

    @pytest.mark.order(4)
    @patch.object(
        WorkspaceState,
        "find_dataset",
        return_value=MOCK_DATASET_FROM_WORKSPACE_V2,
    )
    def test_parse_dataset_expressions(self, *_):
        # test with valid snowflake source but no
        # dataset expression value
        result = self.powerbi._parse_snowflake_source(MOCK_SNOWFLAKE_EXP_V2, MOCK_DASHBOARD_DATA_MODEL)
        result = result[0]
        self.assertIsNone(result["database"])
        self.assertIsNone(result["schema"])
        self.assertEqual(result["table"], "CUSTOMER_TABLE")

    @pytest.mark.order(5)
    @patch.object(OpenMetadata, "get_by_name", return_value=MOCK_DATAMODEL_ENTITY)
    @patch.object(fqn, "build", return_value="powerbi.dataflow_a")
    def test_upstream_dataflow_lineage(self, *_):
        MOCK_DATAMODEL_ENTITY_2 = DashboardDataModel(  # noqa: N806
            name="dummy_dataflow_id_b",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[],
        )
        MOCK_DATAMODEL_2 = Dataflow(  # noqa: N806
            name="dataflow_b",
            objectId="dummy_dataflow_id_b",
            upstreamDataflows=[
                UpstreaDataflow(
                    targetDataflowId="dataflow_a",
                )
            ],
        )
        lineage_request = list(
            self.powerbi.create_dataflow_upstream_dataflow_lineage(MOCK_DATAMODEL_2, MOCK_DATAMODEL_ENTITY_2)
        )
        assert lineage_request[0].right is not None

    @pytest.mark.order(6)
    def test_include_owners_flag_enabled(self):
        """
        Test that when includeOwners is True, owner information is processed
        """
        # Mock the source config to have includeOwners = True
        self.powerbi.source_config.includeOwners = True

        # Test that owner information is processed when includeOwners is True
        self.assertTrue(self.powerbi.source_config.includeOwners)

        # Test with a dashboard that has owners
        dashboard_with_owners = PowerBIDashboard.model_validate(MOCK_DASHBOARD_WITH_OWNERS)

        # Mock the metadata.get_reference_by_email method to return different users for different emails
        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:

            def mock_get_ref_by_email(email):
                if email == "john.doe@example.com":
                    return EntityReferenceList(root=[EntityReference(id=uuid.uuid4(), name="John Doe", type="user")])
                elif email == "jane.smith@example.com":  # noqa: RET505
                    return EntityReferenceList(root=[EntityReference(id=uuid.uuid4(), name="Jane Smith", type="user")])
                return EntityReferenceList(root=[])

            mock_get_ref.side_effect = mock_get_ref_by_email

            # Test get_owner_ref with includeOwners = True
            result = self.powerbi.get_owner_ref(dashboard_with_owners)

            # Should return owner reference when includeOwners is True
            self.assertIsNotNone(result)
            self.assertEqual(len(result.root), 2)
            # Check that both owners are present
            owner_names = [owner.name for owner in result.root]
            self.assertIn("John Doe", owner_names)
            self.assertIn("Jane Smith", owner_names)

    @pytest.mark.order(7)
    def test_include_owners_flag_disabled(self):
        """
        Test that when includeOwners is False, owner information is not processed
        """
        # Mock the source config to have includeOwners = False
        self.powerbi.source_config.includeOwners = False

        # Test that owner information is not processed when includeOwners is False
        self.assertFalse(self.powerbi.source_config.includeOwners)

        # Test with a dashboard that has owners
        dashboard_with_owners = PowerBIDashboard.model_validate(MOCK_DASHBOARD_WITH_OWNERS)

        # Test get_owner_ref with includeOwners = False
        result = self.powerbi.get_owner_ref(dashboard_with_owners)

        # Should return None when includeOwners is False
        self.assertIsNone(result)

    @pytest.mark.order(8)
    def test_include_owners_flag_in_config(self):
        """
        Test that the includeOwners flag is properly set in the configuration
        """
        # Check that the mock configuration includes the includeOwners flag
        config = mock_config["source"]["sourceConfig"]["config"]
        self.assertIn("includeOwners", config)
        self.assertTrue(config["includeOwners"])

    @pytest.mark.order(9)
    def test_include_owners_flag_with_no_owners(self):
        """
        Test that when includeOwners is True but dashboard has no owners, returns None
        """
        # Mock the source config to have includeOwners = True
        self.powerbi.source_config.includeOwners = True

        # Create a dashboard with no owners
        dashboard_no_owners = PowerBIDashboard.model_validate(
            {
                "id": "dashboard_no_owners",
                "displayName": "Test Dashboard No Owners",
                "webUrl": "https://test.com",
                "embedUrl": "https://test.com/embed",
                "tiles": [],
                "users": [],  # No users/owners
            }
        )

        # Test get_owner_ref with no owners
        result = self.powerbi.get_owner_ref(dashboard_no_owners)

        # Should return None when there are no owners
        self.assertIsNone(result)

    @pytest.mark.order(10)
    def test_include_owners_flag_with_exception(self):
        """
        Test that when includeOwners is True but an exception occurs, it's handled gracefully
        """
        # Mock the source config to have includeOwners = True
        self.powerbi.source_config.includeOwners = True

        # Test with a dashboard that has owners
        dashboard_with_owners = PowerBIDashboard.model_validate(MOCK_DASHBOARD_WITH_OWNERS)

        # Mock the metadata.get_reference_by_email method to raise an exception
        with patch.object(
            self.powerbi.metadata,
            "get_reference_by_email",
            side_effect=Exception("API Error"),
        ):
            # Test get_owner_ref with exception
            result = self.powerbi.get_owner_ref(dashboard_with_owners)

            # Should return None when exception occurs
            self.assertIsNone(result)

    @pytest.mark.order(11)
    @patch.object(
        WorkspaceState,
        "find_dataset",
        return_value=MOCK_DATASET_FROM_WORKSPACE_V3,
    )
    def test_parse_dataset_expressions_v2(self, *_):
        # test with valid snowflake source but no
        # dataset expression value
        result = self.powerbi._parse_snowflake_source(MOCK_SNOWFLAKE_EXP_V3, MOCK_DASHBOARD_DATA_MODEL)
        result = result[0]
        self.assertEqual(result["database"], "MANUFACTURING_BUSINESS_DATA_PRODUCTS")
        self.assertEqual(result["schema"], "INVENTORY_BY_PURPOSE")
        self.assertEqual(result["table"], "CUSTOMER_TABLE")

    @pytest.mark.order(12)
    def test_create_dataset_upstream_dataset_column_lineage(self):
        """
        Test column lineage creation between dataset and upstream dataset
        """
        upstream_entity = DashboardDataModel(
            name="upstream_dataset",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataModel.value,
            columns=[
                Column(
                    name="orders",
                    dataType=DataType.STRUCT,
                    children=[
                        Column(
                            name="order_id",
                            dataType=DataType.INT,
                            fullyQualifiedName="service.upstream_dataset.orders.order_id",
                        ),
                        Column(
                            name="amount",
                            dataType=DataType.FLOAT,
                            fullyQualifiedName="service.upstream_dataset.orders.amount",
                        ),
                    ],
                ),
            ],
        )

        downstream_entity = DashboardDataModel(
            name="downstream_dataset",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataModel.value,
            columns=[
                Column(
                    name="orders",
                    dataType=DataType.STRUCT,
                    children=[
                        Column(
                            name="order_id",
                            dataType=DataType.INT,
                            fullyQualifiedName="service.downstream_dataset.orders.order_id",
                        ),
                        Column(
                            name="amount",
                            dataType=DataType.FLOAT,
                            fullyQualifiedName="service.downstream_dataset.orders.amount",
                        ),
                    ],
                ),
            ],
        )

        result = self.powerbi._create_dataset_upstream_dataset_column_lineage(
            datamodel_entity=downstream_entity,
            upstream_dataset_entity=upstream_entity,
        )

        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ColumnLineage)
        self.assertEqual(result[0].fromColumns[0].root, "service.upstream_dataset.orders.order_id")
        self.assertEqual(result[0].toColumn.root, "service.downstream_dataset.orders.order_id")

    @pytest.mark.order(13)
    def test_get_report_url(self):
        """
        Test report URL generation with different page scenarios
        """
        from unittest.mock import MagicMock

        workspace_id = "test-workspace-123"
        dashboard_id = "test-dashboard-456"

        # Create a mock client with api_client
        mock_api_client = MagicMock()
        self.powerbi.client = MagicMock()
        self.powerbi.client.api_client = mock_api_client

        # Create a PowerBIReport object as required by the method signature
        dashboard_details = PowerBIReport(id=dashboard_id, name="Test Report")

        # Test with multiple pages - should use first page name
        with patch("metadata.ingestion.source.dashboard.powerbi.metadata.clean_uri") as mock_clean_uri:
            mock_clean_uri.return_value = "https://app.powerbi.com"
            mock_api_client.fetch_report_pages.return_value = [
                ReportPage(name="page1", displayName="Page 1"),
                ReportPage(name="page2", displayName="Page 2"),
                ReportPage(name="page3", displayName="Page 3"),
            ]

            result = self.powerbi._get_report_url(workspace_id, dashboard_details)

            mock_api_client.fetch_report_pages.assert_called_once_with(workspace_id, dashboard_id)
            self.assertEqual(
                result,
                f"https://app.powerbi.com/groups/{workspace_id}/reports/{dashboard_id}/page1?experience=power-bi",
            )

        # Test with single page - should use that page name
        with patch("metadata.ingestion.source.dashboard.powerbi.metadata.clean_uri") as mock_clean_uri:
            mock_clean_uri.return_value = "https://app.powerbi.com"
            mock_api_client.fetch_report_pages.reset_mock()
            mock_api_client.fetch_report_pages.return_value = [
                ReportPage(name="single-page", displayName="Single Page")
            ]

            result = self.powerbi._get_report_url(workspace_id, dashboard_details)

            self.assertEqual(
                result,
                f"https://app.powerbi.com/groups/{workspace_id}/reports/{dashboard_id}/single-page?experience=power-bi",
            )

        # Test with no pages - should not add page_id
        with patch("metadata.ingestion.source.dashboard.powerbi.metadata.clean_uri") as mock_clean_uri:
            mock_clean_uri.return_value = "https://app.powerbi.com"
            mock_api_client.fetch_report_pages.reset_mock()
            mock_api_client.fetch_report_pages.return_value = []

            result = self.powerbi._get_report_url(workspace_id, dashboard_details)

            self.assertEqual(
                result,
                f"https://app.powerbi.com/groups/{workspace_id}/reports/{dashboard_id}?experience=power-bi",
            )

        # Test with exception during fetch_report_pages - should handle gracefully
        with patch("metadata.ingestion.source.dashboard.powerbi.metadata.clean_uri") as mock_clean_uri:
            mock_clean_uri.return_value = "https://app.powerbi.com"
            mock_api_client.fetch_report_pages.reset_mock()
            mock_api_client.fetch_report_pages.side_effect = Exception("API Error")

            result = self.powerbi._get_report_url(workspace_id, dashboard_details)

            # Should build URL without page_id when exception occurs
            self.assertEqual(
                result,
                f"https://app.powerbi.com/groups/{workspace_id}/reports/{dashboard_id}?experience=power-bi",
            )

    @pytest.mark.order(14)
    def test_powerbi_report_description_parsing(self):
        """
        Test that PowerBIReport model correctly parses the description field
        from API responses, which is used in yield_dashboard for reports
        """
        report_id = "test-report-456"

        # Test with description present
        mock_response_with_description = {
            "id": report_id,
            "name": "Test Report",
            "datasetId": "dataset-789",
            "description": "Test report description",
        }

        result = PowerBIReport(**mock_response_with_description)

        assert result is not None
        assert result.id == report_id
        assert result.name == "Test Report"
        assert result.datasetId == "dataset-789"
        assert result.description == "Test report description"

        # Test with None description
        mock_response_no_description = {
            "id": report_id,
            "name": "Test Report Without Description",
            "datasetId": "dataset-789",
        }

        result = PowerBIReport(**mock_response_no_description)

        assert result is not None
        assert result.id == report_id
        assert result.name == "Test Report Without Description"
        assert result.description is None

        # Test with empty string description
        mock_response_empty_description = {
            "id": report_id,
            "name": "Test Report Empty Description",
            "datasetId": "dataset-789",
            "description": "",
        }

        result = PowerBIReport(**mock_response_empty_description)

        assert result is not None
        assert result.description == ""

    @pytest.mark.order(15)
    def test_paginate_project_filter_pattern_none(self):
        """
        Test _paginate_project_filter_pattern when filter_pattern is None
        Should return default filter pattern that includes all workspaces
        """
        result = self.powerbi._paginate_project_filter_pattern(None)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].includes, [".*"])
        self.assertIsNone(result[0].excludes)

    @pytest.mark.order(16)
    def test_paginate_project_filter_pattern_only_excludes(self):
        """
        Test _paginate_project_filter_pattern with only exclude filters
        Should return the original filter pattern without pagination
        """
        filter_pattern = FilterPattern(excludes=["workspace1", "workspace2"])

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0].includes)
        self.assertEqual(result[0].excludes, ["workspace1", "workspace2"])

    @pytest.mark.order(17)
    def test_paginate_project_filter_pattern_includes_under_limit(self):
        """
        Test _paginate_project_filter_pattern with include filters = 15
        Should return two batches
        """
        includes = [f"workspace{i}" for i in range(15)]
        filter_pattern = FilterPattern(includes=includes)

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].includes, includes[:10])

    @pytest.mark.order(18)
    def test_paginate_project_filter_pattern_includes_at_limit(self):
        """
        Test _paginate_project_filter_pattern with exactly 20 include filters
        Should return two batches
        """
        includes = [f"workspace{i}" for i in range(20)]
        filter_pattern = FilterPattern(includes=includes)

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].includes), 10)
        self.assertEqual(len(result[1].includes), 10)

    @pytest.mark.order(19)
    def test_paginate_project_filter_pattern_includes_over_limit(self):
        """
        Test _paginate_project_filter_pattern with include filters over the limit = 45
        Should paginate into multiple batches
        """
        includes = [f"workspace{i}" for i in range(45)]
        filter_pattern = FilterPattern(includes=includes)

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 5)
        self.assertEqual(len(result[0].includes), 10)
        self.assertEqual(len(result[4].includes), 5)
        self.assertEqual(result[1].includes, includes[10:20])
        self.assertEqual(result[4].includes, includes[40:45])

    @pytest.mark.order(20)
    def test_paginate_project_filter_pattern_with_includes_and_excludes(self):
        """
        Test _paginate_project_filter_pattern with both includes and excludes
        Excludes should be preserved across all paginated batches
        """
        includes = [f"workspace{i}" for i in range(25)]
        excludes = ["excluded1", "excluded2"]
        filter_pattern = FilterPattern(includes=includes, excludes=excludes)

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 3)
        self.assertEqual(len(result[0].includes), 10)
        self.assertEqual(len(result[2].includes), 5)
        self.assertEqual(result[0].excludes, excludes)
        self.assertEqual(result[1].excludes, excludes)

    @pytest.mark.order(21)
    def test_paginate_project_filter_pattern_empty_includes(self):
        """
        Test _paginate_project_filter_pattern with empty includes list
        Should return the original filter pattern
        """
        filter_pattern = FilterPattern(includes=[], excludes=["excluded1"])

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].includes, [])
        self.assertEqual(result[0].excludes, ["excluded1"])

    @pytest.mark.order(22)
    def test_paginate_project_filter_pattern_large_batch(self):
        """
        Test _paginate_project_filter_pattern with a large number of includes
        Should correctly paginate into multiple batches
        """
        includes = [f"workspace{i}" for i in range(100)]
        filter_pattern = FilterPattern(includes=includes)

        result = self.powerbi._paginate_project_filter_pattern(filter_pattern)

        self.assertEqual(len(result), 10)
        for i in range(9):
            self.assertEqual(len(result[i].includes), 10)
        self.assertEqual(len(result[9].includes), 10)
        total_includes = sum(len(batch.includes) for batch in result)
        self.assertEqual(total_includes, 100)

    @pytest.mark.order(23)
    def test_table_name_fallback_when_source_expression_parsing_fails(self):
        """
        Test that when _parse_table_info_from_source_exp returns None,
        the _get_table_and_datamodel_lineage method falls back to using
        the PowerBI table name for lineage.
        """
        from unittest.mock import MagicMock

        table = PowerBiTable(
            name="my_powerbi_table",
            source=[],
            columns=[],
        )

        mock_table_entity = MagicMock()
        mock_table_entity.id = uuid.uuid4()
        mock_table_entity.fullyQualifiedName = "service.database.schema.my_powerbi_table"

        with (
            patch.object(self.powerbi, "_parse_table_info_from_source_exp", return_value=None),
            patch.object(
                self.powerbi.metadata,
                "search_in_any_service",
                return_value=mock_table_entity,
            ) as mock_search,
            patch.object(self.powerbi, "_get_column_lineage", return_value=[]),
            patch.object(self.powerbi, "_get_add_lineage_request") as mock_lineage_request,
        ):
            mock_lineage_request.return_value = MagicMock()

            list(
                self.powerbi._get_table_and_datamodel_lineage(
                    db_service_prefix=None,
                    table=table,
                    datamodel_entity=MOCK_DASHBOARD_DATA_MODEL,
                )
            )

            mock_search.assert_called_once()
            call_args = mock_search.call_args
            fqn_search_string = call_args.kwargs.get("fqn_search_string") or call_args[1].get("fqn_search_string")
            self.assertIn("my_powerbi_table", fqn_search_string)

    @pytest.mark.order(24)
    def test_table_name_fallback_with_valid_source_expression(self):
        """
        Test that when _parse_table_info_from_source_exp returns valid table info,
        the parsed table name is used instead of the PowerBI table name.
        """
        from unittest.mock import MagicMock

        table = PowerBiTable(
            name="powerbi_table_name",
            source=[PowerBITableSource(expression=MOCK_REDSHIFT_EXP)],
            columns=[],
        )

        mock_table_entity = MagicMock()
        mock_table_entity.id = uuid.uuid4()
        mock_table_entity.fullyQualifiedName = "service.dev.demo_dbt_jaffle.customers_clean"

        with (
            patch.object(
                self.powerbi.metadata,
                "search_in_any_service",
                return_value=mock_table_entity,
            ) as mock_search,
            patch.object(self.powerbi, "_get_column_lineage", return_value=[]),
            patch.object(self.powerbi, "_get_add_lineage_request") as mock_lineage_request,
        ):
            mock_lineage_request.return_value = MagicMock()

            list(
                self.powerbi._get_table_and_datamodel_lineage(
                    db_service_prefix=None,
                    table=table,
                    datamodel_entity=MOCK_DASHBOARD_DATA_MODEL,
                )
            )

            mock_search.assert_called_once()
            call_args = mock_search.call_args
            fqn_search_string = call_args.kwargs.get("fqn_search_string") or call_args[1].get("fqn_search_string")
            self.assertIn("customers_clean", fqn_search_string)
            self.assertNotIn("powerbi_table_name", fqn_search_string)

    @pytest.mark.order(25)
    def test_get_dataflow_column_info(self):
        """
        Test that _get_dataflow_column_info correctly extracts tables and columns
        from the dataflow export API response
        """
        dataflow_export = DataflowExportResponse(
            name="test_dataflow",
            description="Test dataflow description",
            version="1.0",
            entities=[
                DataflowEntity(
                    name="queryinsights exec_requests_history",
                    description="Query insights table",
                    attributes=[
                        DataflowEntityAttribute(
                            name="distributed_statement_id",
                            dataType="string",
                            description="Statement ID",
                        ),
                        DataflowEntityAttribute(
                            name="submit_time",
                            dataType="dateTime",
                        ),
                        DataflowEntityAttribute(
                            name="total_elapsed_time_ms",
                            dataType="int64",
                        ),
                    ],
                ),
                DataflowEntity(
                    name="Query",
                    description="",
                    attributes=[
                        DataflowEntityAttribute(
                            name="Column1",
                            dataType="string",
                        ),
                        DataflowEntityAttribute(
                            name="Column2",
                            dataType="string",
                        ),
                    ],
                ),
            ],
        )

        result = self.powerbi._get_dataflow_column_info(dataflow_export)

        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)

        first_table = result[0]
        self.assertEqual(first_table.name.root, "queryinsights exec_requests_history")
        self.assertEqual(first_table.dataType, DataType.TABLE)
        self.assertEqual(first_table.description.root, "Query insights table")
        self.assertEqual(len(first_table.children), 3)

        first_column = first_table.children[0]
        self.assertEqual(first_column.name.root, "distributed_statement_id")
        self.assertEqual(first_column.description.root, "Statement ID")

        second_table = result[1]
        self.assertEqual(second_table.name.root, "Query")
        self.assertEqual(len(second_table.children), 2)

    @pytest.mark.order(26)
    def test_get_dataflow_column_info_empty_entities(self):
        """
        Test that _get_dataflow_column_info handles empty entities list
        """
        dataflow_export = DataflowExportResponse(
            name="empty_dataflow",
            entities=[],
        )

        result = self.powerbi._get_dataflow_column_info(dataflow_export)

        self.assertIsNotNone(result)
        self.assertEqual(len(result), 0)

    @pytest.mark.order(27)
    def test_get_dataflow_column_info_entity_without_attributes(self):
        """
        Test that _get_dataflow_column_info handles entities without attributes
        """
        dataflow_export = DataflowExportResponse(
            name="dataflow_no_attrs",
            entities=[
                DataflowEntity(
                    name="EmptyTable",
                    description="Table with no columns",
                    attributes=[],
                ),
            ],
        )

        result = self.powerbi._get_dataflow_column_info(dataflow_export)

        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name.root, "EmptyTable")
        self.assertEqual(len(result[0].children), 0)

    @pytest.mark.order(28)
    def test_get_dataset_ids_from_report_datasources(self):
        """
        Test that _get_dataset_ids_from_report_datasources extracts dataset IDs
        from the report datasources API response by parsing the
        connectionDetails.database field with pattern sobe_wowvirtualserver-{DATASET_ID}
        """
        from unittest.mock import MagicMock, PropertyMock

        mock_api_client = MagicMock()
        self.powerbi.client = MagicMock()
        self.powerbi.client.api_client = mock_api_client

        mock_context = MagicMock()
        mock_context.workspace.id = "test-workspace-id"

        with patch.object(type(self.powerbi), "context", new_callable=PropertyMock) as mock_ctx:
            mock_ctx.return_value.get.return_value = mock_context

            mock_api_client.fetch_report_datasources.return_value = [
                Datasource(
                    name="TestDatasource",
                    datasourceType="AnalysisServices",
                    connectionDetails=DatasourceConnectionDetails(
                        server="pbiazure://api.powerbi.com/",
                        database="sobe_wowvirtualserver-45812303-926b-49b3-9eb2-8c8209acfaa2",
                    ),
                    datasourceId="3bb310b9-daee-4442-aa3a-f344038e17d8",
                    gatewayId="1ce5fe9c-93eb-410e-8cb8-05ec0b7f3ac6",
                ),
            ]

            result = self.powerbi._get_dataset_ids_from_report_datasources(report_id="test-report-id")

            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], "45812303-926b-49b3-9eb2-8c8209acfaa2")
            mock_api_client.fetch_report_datasources.assert_called_once_with(
                group_id="test-workspace-id", report_id="test-report-id"
            )

            mock_api_client.fetch_report_datasources.return_value = [
                Datasource(
                    name="NoDB",
                    datasourceType="Web",
                    connectionDetails=DatasourceConnectionDetails(
                        server="https://example.com",
                        database=None,
                    ),
                ),
            ]

            result = self.powerbi._get_dataset_ids_from_report_datasources(report_id="test-report-id")
            self.assertEqual(result, [])

            mock_api_client.fetch_report_datasources.return_value = None
            result = self.powerbi._get_dataset_ids_from_report_datasources(report_id="test-report-id")
            self.assertEqual(result, [])

    @pytest.mark.order(29)
    def test_column_name_truncated_when_exceeding_max_length(self):
        """
        Test that column names longer than 256 characters are truncated
        and the full name is preserved in displayName
        """
        long_column_name = "A" * 300
        long_table_name = "B" * 300
        dataset = Dataset(
            id="test-dataset-id",
            name="test-dataset",
            tables=[
                PowerBiTable(
                    name=long_table_name,
                    columns=[
                        PowerBiColumns(
                            name=long_column_name,
                            dataType="string",
                        ),
                    ],
                ),
            ],
        )

        result = self.powerbi._get_column_info(dataset)

        assert len(result) == 1
        table_column = result[0]
        assert len(table_column.name.root) == 256
        assert table_column.name.root == long_table_name[:256]
        assert table_column.displayName == long_table_name

        child_column = table_column.children[0]
        assert len(child_column.name.root) == 256
        assert child_column.name.root == long_column_name[:256]
        assert child_column.displayName == long_column_name

    @pytest.mark.order(30)
    def test_parse_sql_source_pattern1_inline_query(self):
        """
        Pattern 1: Sql.Database("server", "db", [Query = "SQL"])
        """
        result = self.powerbi._parse_sql_source(MOCK_DATAFLOW_INLINE_QUERY_BLOCK)
        assert result is not None
        assert len(result) >= 1
        table_info = result[0]
        assert table_info["database"] == "DW_Integration"
        assert table_info["schema"] == "DataWarehouse"
        assert table_info["table"] == "v_FactUnitePurchases"

    @pytest.mark.order(31)
    def test_parse_sql_source_pattern2_native_query(self):
        """
        Pattern 2: Value.NativeQuery(Source, "SQL") with Sql.Database source
        """
        result = self.powerbi._parse_sql_source(MOCK_DATAFLOW_NATIVE_QUERY_BLOCK)
        assert result is not None
        assert len(result) >= 1
        table_info = result[0]
        assert table_info["database"] == "operationaldatastore"
        assert table_info["table"] == "AccountSalesForceProperties"

    @pytest.mark.order(32)
    def test_parse_sql_source_pattern3_catalog_access(self):
        """
        Pattern 3: Sql.Database("server", "db") + Source{[Schema="x", Item="y"]}[Data]
        """
        result = self.powerbi._parse_sql_source(MOCK_DATAFLOW_CATALOG_ACCESS_BLOCK)
        assert result is not None
        assert len(result) == 1
        assert result[0] == {
            "database": "dw_datawarehouse",
            "schema": "dbo",
            "table": "DimAccounts",
        }

    @pytest.mark.order(33)
    def test_parse_sql_source_non_sql(self):
        """
        Non-SQL sources (PowerPlatform.Dataflows) should return None
        """
        result = self.powerbi._parse_sql_source(MOCK_DATAFLOW_NON_SQL_BLOCK)
        assert result is None

    @pytest.mark.order(34)
    def test_parse_sql_source_computed_query(self):
        """
        Computed queries without Sql.Database should return None
        """
        result = self.powerbi._parse_sql_source(MOCK_DATAFLOW_COMPUTED_BLOCK)
        assert result is None

    @pytest.mark.order(35)
    def test_parse_dataflow_m_document_full(self):
        """
        Test full M document parsing with loadEnabled filtering.
        Should extract: Accounts (pattern 3), BookToBill_Unite (pattern 1),
        AccountSalesForceProperties (pattern 2).
        Should skip: Channel Group Mapping(Sharepoint) (no loadEnabled),
        AOP by MRR (computed, no Sql.Database).
        """
        result = self.powerbi._parse_dataflow_m_document(MOCK_DATAFLOW_EXPORT)
        assert result is not None
        entity_names = [r["entity_name"] for r in result]

        assert "Accounts" in entity_names
        assert "BookToBill_Unite" in entity_names
        assert "AccountSalesForceProperties" in entity_names
        assert "Channel Group Mapping(Sharepoint)" not in entity_names
        assert "AOP by MRR" not in entity_names

        accounts_entry = next(r for r in result if r["entity_name"] == "Accounts")
        assert accounts_entry["tables"][0]["schema"] == "dbo"
        assert accounts_entry["tables"][0]["table"] == "DimAccounts"
        assert accounts_entry["sql"] is None

        booktobill_entry = next(r for r in result if r["entity_name"] == "BookToBill_Unite")
        assert booktobill_entry["tables"][0]["database"] == "DW_Integration"
        assert booktobill_entry["sql"] is not None

    @pytest.mark.order(36)
    def test_parse_dataflow_m_document_no_mashup(self):
        """
        Dataflow export with no mashup should return empty list
        """
        result = self.powerbi._parse_dataflow_m_document(MOCK_DATAFLOW_EXPORT_NO_MASHUP)
        assert result == []

    @pytest.mark.order(37)
    def test_parse_dataflow_m_document_empty_document(self):
        """
        Dataflow export with empty document should return empty list
        """
        result = self.powerbi._parse_dataflow_m_document(MOCK_DATAFLOW_EXPORT_EMPTY_DOC)
        assert result == []

    @pytest.mark.order(38)
    def test_parse_dataflow_m_document_load_enabled_filtering(self):
        """
        Test that entities without loadEnabled=true are filtered out
        """
        doc = "section Section1;\r\nshared " + MOCK_DATAFLOW_CATALOG_ACCESS_BLOCK
        queries_metadata_disabled = {
            "Accounts": {
                "queryId": "q1",
                "queryName": "Accounts",
                "loadEnabled": False,
            },
        }
        export = DataflowExportResponse(
            name="TestDataflow",
            entities=[],
            **{
                "pbi:mashup": DataflowMashup(
                    document=doc,
                    queriesMetadata=queries_metadata_disabled,
                )
            },
        )
        result = self.powerbi._parse_dataflow_m_document(export)
        assert result == []

    @pytest.mark.order(39)
    def test_extract_tables_from_sql_with_powerbi_special_chars(self):
        """
        Test SQL extraction with PowerBI special characters #(lf), #(tab), ""
        """
        result = self.powerbi._extract_tables_from_sql(
            "SELECT [AccountID]#(lf)FROM [DW_Integration].[DataWarehouse].[v_FactUnitePurchases]#(lf)where IsDeleted = 0",
            "dw_integration",
            "dwsql",
        )
        assert result is not None
        assert len(result) >= 1
        tables = [t["table"] for t in result]
        assert "v_FactUnitePurchases" in tables

    @pytest.mark.order(40)
    def test_extract_tables_from_sql_empty(self):
        """
        Test SQL extraction with empty SQL
        """
        result = self.powerbi._extract_tables_from_sql("", "db", "server")
        assert result is None

    @pytest.mark.order(41)
    def test_create_dataflow_table_lineage(self):
        """
        Test end-to-end dataflow table lineage creation
        """
        from unittest.mock import MagicMock

        mock_table_entity = MagicMock()
        mock_table_entity.id = uuid.uuid4()
        mock_table_entity.fullyQualifiedName = "sql_service.dw_datawarehouse.dbo.DimAccounts"
        mock_table_entity.columns = [
            Column(
                name="AccountKey",
                dataType=DataType.INT,
                fullyQualifiedName="sql_service.dw_datawarehouse.dbo.DimAccounts.AccountKey",
            ),
        ]

        mock_datamodel_entity = DashboardDataModel(
            name="test_dataflow_id",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[
                Column(
                    name="Accounts",
                    dataType=DataType.TABLE,
                    children=[
                        Column(
                            name="AccountKey",
                            dataType=DataType.INT,
                            fullyQualifiedName="service.test_dataflow_id.Accounts.AccountKey",
                        ),
                        Column(
                            name="AccountID",
                            dataType=DataType.INT,
                            fullyQualifiedName="service.test_dataflow_id.Accounts.AccountID",
                        ),
                    ],
                ),
            ],
        )

        mock_datamodel = Dataflow(
            name="DimensionTables",
            objectId="test_dataflow_id",
        )

        with (
            patch.object(
                self.powerbi.metadata,
                "search_in_any_service",
                return_value=mock_table_entity,
            ) as mock_search,
            patch.object(self.powerbi, "_get_add_lineage_request") as mock_lineage_request,
        ):
            mock_lineage_request.return_value = MagicMock()

            results = list(
                self.powerbi.create_dataflow_table_lineage(
                    datamodel=mock_datamodel,
                    datamodel_entity=mock_datamodel_entity,
                    dataflow_export=MOCK_DATAFLOW_EXPORT,
                    db_service_prefix=None,
                )
            )

            assert len(results) > 0
            assert mock_search.call_count >= 1
            assert mock_lineage_request.call_count >= 1

    @pytest.mark.order(42)
    def test_create_dataflow_table_lineage_no_parsed_entities(self):
        """
        Test that lineage creation returns empty when no entities are parsed
        """
        mock_datamodel = Dataflow(
            name="EmptyDataflow",
            objectId="empty_id",
        )
        mock_datamodel_entity = DashboardDataModel(
            name="empty_id",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[],
        )

        results = list(
            self.powerbi.create_dataflow_table_lineage(
                datamodel=mock_datamodel,
                datamodel_entity=mock_datamodel_entity,
                dataflow_export=MOCK_DATAFLOW_EXPORT_NO_MASHUP,
                db_service_prefix=None,
            )
        )
        assert results == []

    @pytest.mark.order(43)
    def test_get_dataflow_column_lineage(self):
        """
        Test column lineage matching between database table and dataflow entity
        """
        from unittest.mock import MagicMock

        mock_table_entity = MagicMock()
        mock_table_entity.columns = [
            Column(
                name="AccountKey",
                dataType=DataType.INT,
                fullyQualifiedName="service.db.schema.DimAccounts.AccountKey",
            ),
            Column(
                name="AccountID",
                dataType=DataType.INT,
                fullyQualifiedName="service.db.schema.DimAccounts.AccountID",
            ),
            Column(
                name="UnmatchedCol",
                dataType=DataType.VARCHAR,
                fullyQualifiedName="service.db.schema.DimAccounts.UnmatchedCol",
            ),
        ]

        mock_datamodel_entity = DashboardDataModel(
            name="test_dataflow_id",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[
                Column(
                    name="Accounts",
                    dataType=DataType.TABLE,
                    children=[
                        Column(
                            name="AccountKey",
                            dataType=DataType.INT,
                            fullyQualifiedName="svc.test_dataflow_id.Accounts.AccountKey",
                        ),
                        Column(
                            name="AccountID",
                            dataType=DataType.INT,
                            fullyQualifiedName="svc.test_dataflow_id.Accounts.AccountID",
                        ),
                    ],
                ),
            ],
        )

        result = self.powerbi._get_dataflow_column_lineage(
            table_entity=mock_table_entity,
            datamodel_entity=mock_datamodel_entity,
            entity_name="Accounts",
            entity_attributes=["AccountKey", "AccountID"],
        )

        assert len(result) == 2
        assert all(isinstance(cl, ColumnLineage) for cl in result)
        from_cols = [cl.fromColumns[0].root for cl in result]
        assert "service.db.schema.DimAccounts.AccountKey" in from_cols
        assert "service.db.schema.DimAccounts.AccountID" in from_cols

    @pytest.mark.order(44)
    def test_get_dataflow_column_lineage_no_matches(self):
        """
        Test column lineage when no columns match
        """
        from unittest.mock import MagicMock

        mock_table_entity = MagicMock()
        mock_table_entity.columns = [
            Column(
                name="SomeOtherCol",
                dataType=DataType.INT,
                fullyQualifiedName="service.db.schema.Table.SomeOtherCol",
            ),
        ]

        mock_datamodel_entity = DashboardDataModel(
            name="test_dataflow_id",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[
                Column(
                    name="Accounts",
                    dataType=DataType.TABLE,
                    children=[
                        Column(
                            name="AccountKey",
                            dataType=DataType.INT,
                            fullyQualifiedName="svc.test_dataflow_id.Accounts.AccountKey",
                        ),
                    ],
                ),
            ],
        )

        result = self.powerbi._get_dataflow_column_lineage(
            table_entity=mock_table_entity,
            datamodel_entity=mock_datamodel_entity,
            entity_name="Accounts",
            entity_attributes=["AccountKey"],
        )

        assert result == []

    @pytest.mark.order(45)
    def test_parse_dataflow_m_document_quoted_entity_names(self):
        """
        Test that quoted entity names like #"Channel Categories" are parsed correctly
        """
        doc = (
            "section Section1;\r\n"
            'shared #"Channel Categories" = let\n'
            '  Source = Sql.Database("dwsql", "dw_datawarehouse"),\n'
            '  dbo_DimChannels = Source{[Schema = "dbo", Item = "DimChannels"]}[Data]\n'
            "in\n"
            "  dbo_DimChannels;\r\n"
        )
        queries_metadata = {
            "Channel Categories": {
                "queryId": "q1",
                "queryName": "Channel Categories",
                "loadEnabled": True,
            },
        }
        export = DataflowExportResponse(
            name="TestDataflow",
            entities=[],
            **{
                "pbi:mashup": DataflowMashup(
                    document=doc,
                    queriesMetadata=queries_metadata,
                )
            },
        )
        result = self.powerbi._parse_dataflow_m_document(export)
        assert len(result) == 1
        assert result[0]["entity_name"] == "Channel Categories"
        assert result[0]["tables"][0]["table"] == "DimChannels"
        assert result[0]["tables"][0]["schema"] == "dbo"

    @pytest.mark.order(46)
    def test_parse_sql_source_multiple_databases(self):
        """
        Test M expression with Sql.Database pointing to a different database
        than the inline query references
        """
        block = (
            "CustomerMapping = let\n"
            '  Source = Sql.Database("dwsql", "dw_datawarehouse", '
            '[Query = ";WITH A as (SELECT [CustomerID] '
            "FROM [DW_DataWarehouse].[dbo].[DimAccounts] "
            'GROUP BY [CustomerID]) SELECT CustomerID FROM A"'
            ", CreateNavigationProperties = false]),\n"
            '  #"Changed column type" = Table.TransformColumnTypes(Source, '
            '{{"CustomerCreateDate", type date}})\n'
            "in\n"
            '  #"Changed column type";\r\n'
        )
        result = self.powerbi._parse_sql_source(block)
        assert result is not None
        tables = [t["table"] for t in result]
        assert "DimAccounts" in tables

    @pytest.mark.order(47)
    def test_yield_dashboard_chart_populates_dashboard_charts_mapping(self):
        """
        Test that yield_dashboard_chart correctly populates the dashboard_charts
        mapping so each dashboard ID maps only to its own tile IDs.
        """
        from unittest.mock import MagicMock

        dashboard_1 = PowerBIDashboard(
            id="dash-1",
            displayName="Dashboard One",
            tiles=[
                Tile(id="tile-1a", title="Tile 1A"),
                Tile(id="tile-1b", title="Tile 1B"),
            ],
        )
        dashboard_2 = PowerBIDashboard(
            id="dash-2",
            displayName="Dashboard Two",
            tiles=[
                Tile(id="tile-2a", title="Tile 2A"),
            ],
        )
        dashboard_3 = PowerBIDashboard(
            id="dash-3",
            displayName="Dashboard Three",
            tiles=[
                Tile(id="tile-3a", title="Tile 3A"),
                Tile(id="tile-3b", title="Tile 3B"),
                Tile(id="tile-3c", title="Tile 3C"),
            ],
        )

        for d in (dashboard_1, dashboard_2, dashboard_3):
            self.powerbi.state.add_filtered_dashboard(d)

        mock_context = MagicMock()
        mock_context.workspace = Group(id="ws-1", name="Test Workspace")
        mock_context.dashboard_service = "test_powerbi_service"
        self.powerbi.context.get = MagicMock(return_value=mock_context)

        workspace = Group(id="ws-1", name="Test Workspace")
        charts = list(self.powerbi.yield_dashboard_chart(workspace))

        assert self.powerbi.state.pop_dashboard_chart_ids("dash-1") == ["tile-1a", "tile-1b"]
        assert self.powerbi.state.pop_dashboard_chart_ids("dash-2") == ["tile-2a"]
        assert self.powerbi.state.pop_dashboard_chart_ids("dash-3") == [
            "tile-3a",
            "tile-3b",
            "tile-3c",
        ]

        successful_charts = [c for c in charts if c.right is not None]
        assert len(successful_charts) == 6

    @pytest.mark.order(49)
    def test_yield_dashboard_chart_filtered_chart_not_in_mapping(self):
        """
        Test that charts excluded by chartFilterPattern are not added
        to the dashboard_charts mapping.
        """
        from unittest.mock import MagicMock

        dashboard = PowerBIDashboard(
            id="dash-filter",
            displayName="Filter Dashboard",
            tiles=[
                Tile(id="tile-keep", title="Keep Me"),
                Tile(id="tile-skip", title="Skip Me"),
            ],
        )
        self.powerbi.state.add_filtered_dashboard(dashboard)
        self.powerbi.source_config.chartFilterPattern = FilterPattern(excludes=["Skip Me"])

        mock_context = MagicMock()
        mock_context.workspace = Group(id="ws-1", name="Test Workspace")
        mock_context.dashboard_service = "test_powerbi_service"
        self.powerbi.context.get = MagicMock(return_value=mock_context)

        list(self.powerbi.yield_dashboard_chart(Group(id="ws-1", name="Test Workspace")))

        assert self.powerbi.state.pop_dashboard_chart_ids("dash-filter") == ["tile-keep"]

    @pytest.mark.order(50)
    @patch.object(fqn, "build", side_effect=lambda *args, **kwargs: kwargs.get("chart_name"))
    def test_yield_dashboard_uses_per_dashboard_charts(self, *_):
        """
        Test that yield_dashboard associates only the correct charts with each
        dashboard, not all charts from the workspace.
        """
        from unittest.mock import MagicMock

        dashboard_1 = PowerBIDashboard(
            id="dash-1",
            displayName="Dashboard One",
            tiles=[
                Tile(id="tile-1a", title="Tile 1A"),
            ],
        )
        dashboard_2 = PowerBIDashboard(
            id="dash-2",
            displayName="Dashboard Two",
            tiles=[
                Tile(id="tile-2a", title="Tile 2A"),
                Tile(id="tile-2b", title="Tile 2B"),
            ],
        )
        dashboard_3 = PowerBIDashboard(
            id="dash-3",
            displayName="Dashboard Three",
            tiles=[],
        )

        for d in (dashboard_1, dashboard_2, dashboard_3):
            self.powerbi.state.add_filtered_dashboard(d)
        self.powerbi.state.add_dashboard_chart("dash-1", "tile-1a")
        self.powerbi.state.add_dashboard_chart("dash-2", "tile-2a")
        self.powerbi.state.add_dashboard_chart("dash-2", "tile-2b")
        # dash-3 intentionally has no charts

        mock_context = MagicMock()
        mock_context.workspace = Group(id="ws-1", name="Test Workspace")
        mock_context.dashboard_service = "test_powerbi_service"
        self.powerbi.context.get = MagicMock(return_value=mock_context)

        workspace = Group(id="ws-1", name="Test Workspace")
        results = list(self.powerbi.yield_dashboard(workspace))

        dashboards = [r.right for r in results if r.right is not None]
        assert len(dashboards) == 3

        dash_1_result = next(d for d in dashboards if d.name.root == "dash-1")
        assert len(dash_1_result.charts) == 1
        assert dash_1_result.charts[0].root == "tile-1a"

        dash_2_result = next(d for d in dashboards if d.name.root == "dash-2")
        assert len(dash_2_result.charts) == 2

        dash_3_result = next(d for d in dashboards if d.name.root == "dash-3")
        assert len(dash_3_result.charts) == 0

    @pytest.mark.order(51)
    def test_tsql_dialect_required_for_bracket_quoted_identifiers(self):
        """
        Regression guard, outcome-based: the queries PowerBI dataflows produce
        through `Sql.Database` / `Value.NativeQuery` use T-SQL bracket-quoted
        identifiers (e.g. [Column Name], [db].[schema].[table]). Parsing
        these under ANSI fails at the sqlglot/sqlfluff layer; parsing under
        TSQL succeeds. `_extract_tables_from_sql` must therefore use TSQL.

        We cannot observe this difference through `_parse_sql_source`'s return
        value alone: LineageParser falls back to the permissive SqlParse
        analyzer, which recovers source tables even when the higher-fidelity
        parsers fail. Instead we run LineageParser directly on the
        representative queries and assert that the connector's chosen dialect
        is the one for which the real parsers succeed.
        """
        from metadata.ingestion.lineage.models import Dialect
        from metadata.ingestion.lineage.parser import LineageParser

        bracket_queries = [
            "SELECT CE_UNIQUE_ID, [FUNCT_ACCOUNT ALT_L2] FROM cub.v_md_FunctAccount_with_CostElement",
            "SELECT ORGANIZATION_ID, [Level], [ORGANIZATION ID AND DESCRIPTION] FROM cub.v_md_Organization_FLAT",
            "SELECT * FROM [NBS_GENIE].[QS].[Company_v2]",
            "SELECT SORT_ORDER, SOURCE FROM cub.[v_md_SourceSystem (FAC)]",
            "SELECT TOP 100 IBI_DETAILS_ID, [YEAR] FROM cub.v_fact_IBI_vs_BPC_Delta WHERE [YEAR] = 2024",
        ]

        for sql in bracket_queries:
            ansi_parser = LineageParser(sql, dialect=Dialect.ANSI, timeout_seconds=10)
            tsql_parser = LineageParser(sql, dialect=Dialect.TSQL, timeout_seconds=10)

            assert ansi_parser.query_parsing_success is False, (
                f"Expected ANSI to fail on bracket-quoted T-SQL: {sql!r}. "
                "If ANSI now parses this, the dialect choice in "
                "_extract_tables_from_sql may no longer matter and this "
                "test should be re-evaluated."
            )
            assert tsql_parser.query_parsing_success is True, (
                f"Expected TSQL to parse bracket-quoted T-SQL: {sql!r}. "
                "If this fails, the underlying parsers (sqlglot/sqlfluff) "
                "have regressed and PowerBI dataflow lineage will be lost."
            )

    @pytest.mark.order(52)
    def test_extract_tables_from_sql_tsql_bracket_queries(self):
        """
        End-to-end smoke test: 5 representative T-SQL queries from the
        production PowerBI ingestion log that previously failed parsing under
        the ANSI dialect. With the TSQL dialect, each must successfully parse
        and yield the expected source table.

        Cases:
          1. Bracket-quoted column with embedded space  ([FUNCT_ACCOUNT ALT_L2])
          2. Multi-word bracket-quoted column           ([ORGANIZATION ID AND DESCRIPTION])
          3. Three-part fully-bracketed table reference ([db].[schema].[table])
          4. Bracket-quoted table with space + parens   (cub.[v_md_SourceSystem (FAC)])
          5. T-SQL TOP clause with bracket-quoted col   (SELECT TOP n ... [YEAR])
        """
        cases = [
            (
                "bracket-quoted column with space",
                "SELECT CE_UNIQUE_ID, [FUNCT_ACCOUNT ALT_L2] FROM cub.v_md_FunctAccount_with_CostElement",
                "v_md_FunctAccount_with_CostElement",
            ),
            (
                "multi-word bracket-quoted column",
                "SELECT ORGANIZATION_ID, [Level], [ORGANIZATION ID AND DESCRIPTION] FROM cub.v_md_Organization_FLAT",
                "v_md_Organization_FLAT",
            ),
            (
                "three-part bracketed table reference",
                "SELECT * FROM [NBS_GENIE].[QS].[Company_v2]",
                "Company_v2",
            ),
            (
                "bracketed table name with space and parens",
                "SELECT SORT_ORDER, SOURCE FROM cub.[v_md_SourceSystem (FAC)]",
                "v_md_SourceSystem (FAC)",
            ),
            (
                "T-SQL TOP clause with bracket-quoted reserved word",
                "SELECT TOP 100 IBI_DETAILS_ID, [YEAR] FROM cub.v_fact_IBI_vs_BPC_Delta WHERE [YEAR] = 2024",
                "v_fact_IBI_vs_BPC_Delta",
            ),
        ]

        for label, sql, expected_table in cases:
            m_expression = (
                f'TestEntity = let\n  Source = Sql.Database("server", "db", [Query = "{sql}"])\nin\n  Source;\r\n'
            )
            result = self.powerbi._parse_sql_source(m_expression)
            assert result is not None, f"[{label}] _parse_sql_source returned None"
            assert any(expected_table.lower() in t["table"].lower() for t in result), (
                f"[{label}] expected table '{expected_table}' not found in result {[t['table'] for t in result]}"
            )

    def test_yield_dashboard_lineage_yields_barrier_first(self):
        """The override must emit a ``Barrier`` as its first record so the sink
        flushes before ``super().yield_dashboard_lineage`` runs its
        ``get_by_name`` lookups. Subsequent yields come from ``super``.
        """
        ws_id = "test-workspace-id"
        mock_workspace = MagicMock()
        mock_workspace.id = ws_id
        mock_ctx = MagicMock()
        mock_ctx.get.return_value = MagicMock(workspace=mock_workspace)

        sentinel_super_records = [
            Either(right=MagicMock(name="lineage-1")),
            Either(right=MagicMock(name="lineage-2")),
        ]

        with (
            patch.object(self.powerbi, "context", mock_ctx),
            patch(
                "metadata.ingestion.source.dashboard.dashboard_service.DashboardServiceSource.yield_dashboard_lineage",
                return_value=iter(sentinel_super_records),
            ),
        ):
            emitted = list(self.powerbi.yield_dashboard_lineage(MagicMock()))

        # First record is a Barrier carrying the workspace id in its reason.
        assert len(emitted) == 1 + len(sentinel_super_records)
        first = emitted[0]
        assert isinstance(first.right, Barrier)
        assert ws_id in (first.right.reason or "")

        # Subsequent records are exactly what super yielded, in order.
        for actual, expected in zip(emitted[1:], sentinel_super_records, strict=True):
            assert actual is expected

    @pytest.mark.order(53)
    @patch.object(OpenMetadata, "get_by_name", return_value=MOCK_DATAMODEL_ENTITY)
    @patch.object(fqn, "build", return_value="powerbi.datamart_a")
    def test_upstream_datamart_lineage(self, *_):
        """`create_datamart_upstream_datamart_lineage` should emit one lineage
        request per non-self upstream datamart reference. The self-reference in
        MOCK_DATAMART (targetDatamartId == datamart_b == datamart.id) must be
        filtered out.
        """
        MOCK_DATAMART_ENTITY = DashboardDataModel(  # noqa: N806
            name="datamart_b",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDatamart.value,
            columns=[],
        )

        lineage_requests = list(
            self.powerbi.create_datamart_upstream_datamart_lineage(MOCK_DATAMART, MOCK_DATAMART_ENTITY)
        )

        successful = [r for r in lineage_requests if r.right is not None]
        assert len(successful) == 1

    @pytest.mark.order(55)
    @patch.object(fqn, "build", side_effect=lambda *args, **kwargs: kwargs.get("chart_name"))
    def test_yield_dashboard_advances_workspace_progress(self, *_):
        self.powerbi.state = WorkspaceState()
        self.powerbi.__dict__.pop("_progress_tracking", None)

        for dash in (
            PowerBIDashboard(id="dash-1", displayName="One", tiles=[]),
            PowerBIDashboard(id="dash-2", displayName="Two", tiles=[]),
        ):
            self.powerbi.state.add_filtered_dashboard(dash)

        mock_context = MagicMock()
        mock_context.workspace = Group(id="ws-1", name="Sales")
        mock_context.dashboard_service = "test_powerbi_service"
        self.powerbi.context.get = MagicMock(return_value=mock_context)

        self.powerbi._open_group_progress(
            "Sales",
            {"Dashboard": None, "Chart": None, "DashboardDataModel": None},
        )
        list(self.powerbi.yield_dashboard(Group(id="ws-1", name="Sales")))

        assert self.powerbi.progress_tracking.registry.assets_ingested() == 2
        out = self.powerbi.progress_tracking.registry.render_cli()
        assert "Sales.Dashboard" in out
        assert "Dashboard 2" in out

    @pytest.mark.order(56)
    def test_get_dashboard_tracks_workspace_group(self):
        self.powerbi.state = WorkspaceState()
        self.powerbi.__dict__.pop("_progress_tracking", None)

        ws_a = Group(id="wa", name="Alpha")
        ws_b = Group(id="wb", name="Beta")
        self.powerbi._prepare_workspace_data = MagicMock(return_value=iter([ws_a, ws_b]))
        self.powerbi.get_dashboards_list = MagicMock(return_value=[])
        self.powerbi.context.get = MagicMock(return_value=MagicMock())

        produced = list(self.powerbi.get_dashboard())

        assert [w.id for w in produced] == ["wa", "wb"]
        assert self.powerbi.progress_tracking.registry.global_counters() == [("Workspaces", 2, None)]
        assert self.powerbi.progress_tracking.registry.snapshot() is None

    @pytest.mark.order(56)
    def test_get_dashboard_does_not_count_workspace_failing_before_open(self):
        self.powerbi.state = WorkspaceState()
        self.powerbi.__dict__.pop("_progress_tracking", None)

        ws_ok = Group(id="ok", name="Ok")
        ws_bad = Group(id="bad", name="Bad")
        self.powerbi._prepare_workspace_data = MagicMock(return_value=iter([ws_bad, ws_ok]))
        self.powerbi.context.get = MagicMock(return_value=MagicMock())
        self.powerbi.get_dashboards_list = MagicMock(side_effect=[RuntimeError("boom"), []])

        produced = list(self.powerbi.get_dashboard())

        assert [w.id for w in produced] == ["ok"]
        assert self.powerbi.progress_tracking.registry.global_counters() == [("Workspaces", 1, None)]

    @pytest.mark.order(57)
    def test_admin_workspace_progress_total_reconciles_to_active_scan_results(self):
        self.powerbi.__dict__.pop("_progress_tracking", None)
        self.powerbi.pagination_entity_per_page = 100

        candidate_workspaces = [Group(id=f"ws-{index}", name=f"Workspace {index}") for index in range(46)]
        active_workspaces = [Group(id=f"ws-{index}", name=f"Workspace {index}", state="Active") for index in range(40)]
        skipped_workspaces = [
            Group(id=f"ws-{index}", name=f"Workspace {index}", state="Deleted") for index in range(40, 44)
        ]
        self.powerbi.client = MagicMock()
        self.powerbi.client.api_client = MagicMock()
        self.powerbi.client.api_client.fetch_all_workspaces = MagicMock(return_value=candidate_workspaces)
        self.powerbi.client.api_client.initiate_workspace_scan = MagicMock(return_value=MagicMock(id="scan-1"))
        self.powerbi.client.api_client.wait_for_scan_complete = MagicMock(return_value=True)
        self.powerbi.client.api_client.fetch_workspace_scan_result = MagicMock(
            return_value=Workspaces(workspaces=active_workspaces + skipped_workspaces)
        )

        produced = list(self.powerbi.get_admin_workspace_data())

        assert [workspace.id for workspace in produced] == [f"ws-{index}" for index in range(40)]
        assert self.powerbi.progress_tracking.registry.global_counters() == [("Workspaces", 0, 40)]

    @pytest.mark.order(58)
    @patch.object(fqn, "build", side_effect=lambda *args, **kwargs: kwargs.get("chart_name"))
    def test_unnamed_workspace_keys_progress_on_id(self, *_):
        self.powerbi.__dict__.pop("_progress_tracking", None)

        self.powerbi.state.add_filtered_dashboard(PowerBIDashboard(id="dash-1", displayName="One", tiles=[]))

        mock_context = MagicMock()
        mock_context.workspace = Group(id="ws-x", name=None)
        mock_context.dashboard_service = "test_powerbi_service"
        self.powerbi.context.get = MagicMock(return_value=mock_context)

        assert self.powerbi._progress_group_name() == "ws-x"

        self.powerbi._open_group_progress("ws-x", {"Dashboard": None})
        list(self.powerbi.yield_dashboard(Group(id="ws-x", name=None)))

        out = self.powerbi.progress_tracking.registry.render_cli()
        assert "ws-x.Dashboard" in out
        assert "None.Dashboard" not in out

    @pytest.mark.order(54)
    def test_yield_datamodel_for_datamart(self):
        """`yield_datamodel` should emit a CreateDashboardDataModelRequest with
        dataModelType=PowerBIDatamart, no columns, and the datamart-specific
        sourceUrl when the workspace contains a Datamart.
        """
        mock_context = MagicMock()
        mock_context.workspace = Group(
            id="ws-1",
            name="Test Workspace",
            datasets=[],
            dataflows=[],
            datamarts=[MOCK_DATAMART],
        )
        mock_context.dashboard_service = "test_powerbi_service"
        self.powerbi.context.get = MagicMock(return_value=mock_context)
        self.powerbi.source_config.includeDataModels = True
        self.powerbi.source_config.includeOwners = False
        self.powerbi.state.set_filtered_datamodels(None)

        results = list(self.powerbi.yield_datamodel(mock_context.workspace))
        successful = [r.right for r in results if r.right is not None]

        assert len(successful) == 1
        request = successful[0]
        assert request.dataModelType == DataModelType.PowerBIDatamart
        assert request.columns == []
        assert request.sourceUrl.root.endswith("/groups/ws-1/datamarts/datamart_b?experience=power-bi")

    @pytest.mark.order(60)
    def test_parse_athena_source_navigation_default_catalog(self):
        """
        AmazonAthena.Databases navigation whose Kind="Database" level is the
        literal AwsDataCatalog placeholder: the catalog is not an OM database.
        """
        result = self.powerbi._parse_athena_source(MOCK_ATHENA_NAV_EXP)
        assert result is not None
        assert len(result) == 1
        table_info = result[0]
        assert table_info["database"] is None
        assert table_info["schema"] == "sales_db"
        assert table_info["table"] == "orders"
        assert table_info["dsn"] == "warehouse-dsn"

    @pytest.mark.order(61)
    def test_parse_athena_source_navigation_federated_catalog(self):
        """
        A Kind="Database" level that is not the AwsDataCatalog placeholder is a
        real federated catalog, and is returned as the database.
        """
        result = self.powerbi._parse_athena_source(MOCK_ATHENA_NAV_FEDERATED_CATALOG_EXP)
        assert result is not None
        table_info = result[0]
        assert table_info["database"] == "external_catalog"
        assert table_info["schema"] == "sales_db"
        assert table_info["table"] == "orders"
        assert table_info["dsn"] == "analytics-dsn"

    @pytest.mark.order(62)
    def test_parse_athena_source_navigation_view_kind_and_step_name_variation(self):
        """
        Kind="View" is accepted like "Table", and unquoted, unsuffixed step
        names (Navigation instead of #"Navigation 3") are tolerated because
        parsing matches the {[Name=..., Kind=...]} record, never step names.
        """
        result = self.powerbi._parse_athena_source(MOCK_ATHENA_NAV_VIEW_EXP)
        assert result is not None
        table_info = result[0]
        assert table_info["database"] is None
        assert table_info["schema"] == "sales_db"
        assert table_info["table"] == "orders_view"

    @pytest.mark.order(63)
    def test_parse_athena_source_odbc_query_uses_athena_dialect(self):
        """
        Odbc.Query SQL is extracted and parsed with Dialect.ATHENA so
        double-quoted identifiers resolve to schema + table.
        """
        from metadata.ingestion.lineage.models import Dialect

        with patch.object(
            self.powerbi,
            "_extract_tables_from_sql",
            wraps=self.powerbi._extract_tables_from_sql,
        ) as mock_extract:
            result = self.powerbi._parse_athena_source(MOCK_ODBC_QUERY_EXP)

        mock_extract.assert_called_once()
        assert mock_extract.call_args.kwargs.get("dialect") == Dialect.ATHENA

        assert result is not None
        table_info = result[0]
        assert table_info["schema"] == "sales_db"
        assert table_info["table"] == "orders"
        assert table_info["dsn"] == "warehouse-dsn"

    @pytest.mark.order(64)
    def test_parse_athena_source_non_athena_returns_none(self):
        """Non-Athena/ODBC sources are left to the other parsers."""
        assert self.powerbi._parse_athena_source(MOCK_REDSHIFT_EXP) is None

    @pytest.mark.order(65)
    def test_resolve_source_database_default_and_override(self):
        """
        Default resolve_source_database returns the parsed "database"; a
        subclass override can map the DSN onto a different database.
        """
        table_info = {
            "database": None,
            "schema": "sales_db",
            "table": "orders",
            "dsn": "warehouse-dsn",
        }
        assert self.powerbi.resolve_source_database(table_info) is None
        assert self.powerbi.resolve_source_database({"database": "raw_db"}) == "raw_db"

        class DsnAwarePowerbiSource(PowerbiSource):
            def resolve_source_database(self, table_info):
                dsn_to_database = {"warehouse-dsn": "warehouse_catalog"}
                return dsn_to_database.get(table_info.get("dsn")) or super().resolve_source_database(table_info)

        override_source = DsnAwarePowerbiSource.__new__(DsnAwarePowerbiSource)
        assert override_source.resolve_source_database(table_info) == "warehouse_catalog"
        assert override_source.resolve_source_database({"database": "raw_db"}) == "raw_db"

    @pytest.mark.order(66)
    def test_resolve_source_database_override_changes_fqn_search_string(self):
        """
        A resolve_source_database override changes the database used to build
        the FQN search string that search_in_any_service is queried with.
        """
        table = PowerBiTable(
            name="orders",
            source=[PowerBITableSource(expression=MOCK_ATHENA_NAV_EXP)],
        )

        self.powerbi.resolve_source_database = MagicMock(return_value="warehouse_catalog")
        try:
            with patch.object(self.powerbi.metadata, "search_in_any_service", return_value=None) as mock_search:
                list(
                    self.powerbi._get_table_and_datamodel_lineage(
                        db_service_prefix=None,
                        table=table,
                        datamodel_entity=MOCK_DASHBOARD_DATA_MODEL,
                    )
                )
        finally:
            del self.powerbi.resolve_source_database

        assert mock_search.call_count == 1
        fqn_search_string = mock_search.call_args.kwargs["fqn_search_string"]
        assert "warehouse_catalog" in fqn_search_string

    @pytest.mark.order(67)
    def test_parse_dataflow_m_document_multi_query_multi_dsn(self):
        """
        A dataflow document with two Athena-navigation queries against
        different DSNs, plus one Odbc.Query-sourced entity, all parse
        independently and keep their own dsn.
        """
        export = DataflowExportResponse(
            name="AthenaDataflow",
            entities=[],
            **{
                "pbi:mashup": DataflowMashup(
                    document=MOCK_DATAFLOW_ATHENA_DOCUMENT,
                    queriesMetadata=MOCK_DATAFLOW_ATHENA_QUERIES_METADATA,
                )
            },
        )
        result = self.powerbi._parse_dataflow_m_document(export)
        entity_names = [r["entity_name"] for r in result]
        assert "Orders" in entity_names
        assert "Customers" in entity_names
        assert "OrdersOdbc" in entity_names

        orders_entry = next(r for r in result if r["entity_name"] == "Orders")
        assert orders_entry["tables"][0]["dsn"] == "warehouse-dsn"
        assert orders_entry["tables"][0]["schema"] == "sales_db"
        assert orders_entry["tables"][0]["table"] == "orders"

        customers_entry = next(r for r in result if r["entity_name"] == "Customers")
        assert customers_entry["tables"][0]["dsn"] == "analytics-dsn"
        assert customers_entry["tables"][0]["schema"] == "crm_db"
        assert customers_entry["tables"][0]["table"] == "customers"

        odbc_entry = next(r for r in result if r["entity_name"] == "OrdersOdbc")
        assert odbc_entry["tables"][0]["dsn"] == "warehouse-dsn"
        assert odbc_entry["tables"][0]["schema"] == "sales_db"
        assert odbc_entry["tables"][0]["table"] == "orders"
        assert odbc_entry["sql"] is not None

    @pytest.mark.order(68)
    def test_create_dataflow_table_lineage_athena_source(self):
        """
        End to end: _parse_dataflow_m_document over an Athena-sourced dataflow
        document feeds create_dataflow_table_lineage, which resolves the table
        and emits lineage into the dataflow entity.
        """
        mock_table_entity = MagicMock()
        mock_table_entity.id = uuid.uuid4()
        mock_table_entity.fullyQualifiedName = "athena_service.sales_db.orders"
        mock_table_entity.columns = [
            Column(
                name="OrderId",
                dataType=DataType.INT,
                fullyQualifiedName="athena_service.sales_db.orders.OrderId",
            ),
        ]

        mock_datamodel_entity = DashboardDataModel(
            name="athena_dataflow_id",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[
                Column(
                    name="Orders",
                    dataType=DataType.TABLE,
                    children=[
                        Column(
                            name="OrderId",
                            dataType=DataType.INT,
                            fullyQualifiedName="service.athena_dataflow_id.Orders.OrderId",
                        ),
                    ],
                ),
            ],
        )

        mock_datamodel = Dataflow(name="AthenaDataflow", objectId="athena_dataflow_id")

        with (
            patch.object(
                self.powerbi.metadata,
                "search_in_any_service",
                return_value=mock_table_entity,
            ) as mock_search,
            patch.object(self.powerbi, "_get_add_lineage_request") as mock_lineage_request,
        ):
            mock_lineage_request.return_value = MagicMock()

            results = list(
                self.powerbi.create_dataflow_table_lineage(
                    datamodel=mock_datamodel,
                    datamodel_entity=mock_datamodel_entity,
                    dataflow_export=MOCK_DATAFLOW_EXPORT_ATHENA,
                    db_service_prefix=None,
                )
            )

        assert len(results) > 0
        assert mock_search.call_count >= 1
        assert any("sales_db" in c.kwargs["fqn_search_string"] for c in mock_search.call_args_list)
        assert mock_lineage_request.call_count >= 1

    @pytest.mark.order(69)
    def test_dataset_upstream_dataflow_link_parses_row_shape(self):
        """
        The non-admin workspace-wide dataset->dataflow link row shape
        ({datasetObjectId, dataflowObjectId, workspaceObjectId}) parses
        directly, including a cross-workspace row (the dataflow lives in a
        different workspace than the dataset it links from).
        """
        same_workspace_row = DatasetUpstreamDataflowLink(
            datasetObjectId="dataset-1",
            dataflowObjectId="dataflow-1",
            workspaceObjectId="ws-1",
        )
        assert same_workspace_row.datasetObjectId == "dataset-1"
        assert same_workspace_row.dataflowObjectId == "dataflow-1"
        assert same_workspace_row.workspaceObjectId == "ws-1"

        cross_workspace_row = DatasetUpstreamDataflowLink(
            datasetObjectId="dataset-2",
            dataflowObjectId="dataflow-in-other-workspace",
            workspaceObjectId="ws-2",
        )
        assert cross_workspace_row.dataflowObjectId == "dataflow-in-other-workspace"
        assert cross_workspace_row.workspaceObjectId == "ws-2"

        response = DatasetUpstreamDataflowLinksResponse(
            **{
                "@odata.context": "http://example.com/$metadata",
                "value": [
                    {
                        "datasetObjectId": "dataset-1",
                        "dataflowObjectId": "dataflow-1",
                        "workspaceObjectId": "ws-1",
                    },
                    {
                        "datasetObjectId": "dataset-2",
                        "dataflowObjectId": "dataflow-in-other-workspace",
                        "workspaceObjectId": "ws-2",
                    },
                ],
            }
        )
        assert len(response.value) == 2
        assert response.value[1].workspaceObjectId == "ws-2"

    @pytest.mark.order(70)
    def test_get_org_workspace_data_wires_dataflows_and_dataset_links(self):
        """
        get_org_workspace_data (the non-admin path) populates workspace
        dataflows, each dataflow's upstreamDataflows, and fills
        Dataset.upstreamDataflows from the workspace-wide dataset->dataflow
        links call.
        """
        workspace = Group(
            id="ws-1",
            name="Sales Workspace",
            datasets=[Dataset(id="dataset-1", name="Sales Dataset")],
        )

        mock_api_client = MagicMock()
        mock_api_client.fetch_all_workspaces.return_value = [workspace]
        mock_api_client.fetch_all_org_dashboards.return_value = []
        mock_api_client.fetch_all_org_tiles.return_value = []
        mock_api_client.fetch_all_org_reports.return_value = []
        mock_api_client.fetch_all_org_datasets.return_value = []
        mock_api_client.fetch_dataset_tables.return_value = []
        mock_api_client.fetch_all_org_dataflows.return_value = [Dataflow(name="SalesFlow", objectId="dataflow-1")]
        mock_api_client.fetch_dataflow_upstream.return_value = [UpstreaDataflow(targetDataflowId="dataflow-0")]
        mock_api_client.fetch_dataset_to_dataflow_links.return_value = [
            DatasetUpstreamDataflowLink(
                datasetObjectId="dataset-1",
                dataflowObjectId="dataflow-1",
                workspaceObjectId="ws-1",
            )
        ]

        self.powerbi.client = MagicMock()
        self.powerbi.client.api_client = mock_api_client

        result_workspaces = list(self.powerbi.get_org_workspace_data())

        assert len(result_workspaces) == 1
        result_workspace = result_workspaces[0]
        assert [d.id for d in result_workspace.dataflows] == ["dataflow-1"]
        assert result_workspace.dataflows[0].upstreamDataflows[0].targetDataflowId == "dataflow-0"
        assert result_workspace.datasets[0].upstreamDataflows[0].targetDataflowId == "dataflow-1"

    @pytest.mark.order(71)
    def test_metric_values_counts_dataflows_and_source_references(self):
        """
        metric_values() exposes plain counters for a metrics reporter: an
        Athena source reference that resolves increments both the "parsed"
        and "resolved" counters.
        """
        table = PowerBiTable(
            name="orders",
            source=[PowerBITableSource(expression=MOCK_ATHENA_NAV_EXP)],
        )
        mock_table_entity = MagicMock()
        mock_table_entity.name.root = "orders"

        with patch.object(self.powerbi.metadata, "search_in_any_service", return_value=mock_table_entity):
            list(
                self.powerbi._get_table_and_datamodel_lineage(
                    db_service_prefix=None,
                    table=table,
                    datamodel_entity=MOCK_DASHBOARD_DATA_MODEL,
                )
            )

        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_ATHENA_ODBC_QUERIES_SEEN] >= 1
        assert metrics[PowerbiSource.METRIC_SOURCE_REFERENCES_PARSED] >= 1
        assert metrics[PowerbiSource.METRIC_SOURCE_REFERENCES_RESOLVED] >= 1

    @pytest.mark.order(72)
    def test_queries_skipped_load_disabled_counts_only_recognized_sources(self):
        """
        queries_skipped_load_disabled only counts disabled queries that would
        otherwise have reached the lineage parser (Athena/ODBC/Sql.Database
        sourced). A disabled non-SQL helper query (SharePoint here) was never
        going to be parsed for lineage and must not inflate the counter.
        """
        self.powerbi._metrics.clear()
        result = self.powerbi._parse_dataflow_m_document(MOCK_DATAFLOW_EXPORT_LOAD_DISABLED)

        assert result == []
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_QUERIES_SKIPPED_LOAD_DISABLED] == 1

    @pytest.mark.order(73)
    def test_athena_odbc_queries_seen_counts_disabled_blocks_too(self):
        """
        athena_odbc_queries_seen counts every recognized Athena/ODBC M block,
        including ones skipped for loadEnabled - "seen" means detected, not
        "successfully dispatched for parsing".
        """
        self.powerbi._metrics.clear()
        self.powerbi._parse_dataflow_m_document(MOCK_DATAFLOW_EXPORT_LOAD_DISABLED)

        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_ATHENA_ODBC_QUERIES_SEEN] == 1

    @pytest.mark.order(74)
    def test_source_references_dedup_counts_distinct_pairs_once(self):
        """
        The same table reached through two different M queries in one
        dataflow (Athena navigation + Odbc.Query, resolving to the same
        schema.table) is one distinct (data model, table reference) pair:
        parsed/resolved must not double-count it, and resolved + unresolved
        must equal parsed by construction.
        """
        mock_table_entity = MagicMock()
        mock_table_entity.id = uuid.uuid4()
        mock_table_entity.fullyQualifiedName = "athena_service.sales_db.user_activity"
        mock_table_entity.columns = []

        mock_datamodel_entity = DashboardDataModel(
            name="duplicate_ref_dataflow_id",
            id=uuid.uuid4(),
            dataModelType=DataModelType.PowerBIDataFlow.value,
            columns=[],
        )
        mock_datamodel = Dataflow(name="DuplicateRefDataflow", objectId="duplicate_ref_dataflow_id")

        self.powerbi._metrics.clear()
        self.powerbi._counted_source_references.clear()

        with (
            patch.object(self.powerbi.metadata, "search_in_any_service", return_value=mock_table_entity),
            patch.object(self.powerbi, "_get_add_lineage_request", return_value=MagicMock()),
        ):
            results = list(
                self.powerbi.create_dataflow_table_lineage(
                    datamodel=mock_datamodel,
                    datamodel_entity=mock_datamodel_entity,
                    dataflow_export=MOCK_DATAFLOW_EXPORT_DUPLICATE_REF,
                    db_service_prefix=None,
                )
            )

        # Two M queries resolve to the same table, so two lineage edges may
        # still be yielded (one per source query) - but the reconciliation
        # counters must treat it as a single distinct reference.
        assert len(results) == 2
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_SOURCE_REFERENCES_PARSED] == 1
        assert metrics[PowerbiSource.METRIC_SOURCE_REFERENCES_RESOLVED] == 1
        assert metrics.get(PowerbiSource.METRIC_SOURCE_REFERENCES_UNRESOLVED, 0) == 0
        assert metrics[PowerbiSource.METRIC_SOURCE_REFERENCES_PARSED] == metrics[
            PowerbiSource.METRIC_SOURCE_REFERENCES_RESOLVED
        ] + metrics.get(PowerbiSource.METRIC_SOURCE_REFERENCES_UNRESOLVED, 0)

    @pytest.mark.order(75)
    def test_unresolved_source_reference_logged_and_bounded(self):
        """
        An unresolved reference is logged once at INFO with the data model
        and the FQN search string, and logging stops once
        _MAX_UNRESOLVED_LOGGED is reached even though the metric itself keeps
        counting every distinct unresolved reference.
        """
        from metadata.ingestion.source.dashboard.powerbi import metadata as powerbi_metadata_module

        table_a = PowerBiTable(
            name="orders",
            source=[PowerBITableSource(expression=MOCK_ATHENA_NAV_EXP)],
        )
        table_b = PowerBiTable(
            name="orders2",
            source=[PowerBITableSource(expression=MOCK_ATHENA_NAV_FEDERATED_CATALOG_EXP)],
        )

        self.powerbi._metrics.clear()
        self.powerbi._counted_source_references.clear()
        self.powerbi._unresolved_logged_count = 0
        self.powerbi._MAX_UNRESOLVED_LOGGED = 1
        try:
            with (
                patch.object(self.powerbi.metadata, "search_in_any_service", return_value=None),
                patch.object(powerbi_metadata_module.logger, "info") as mock_info,
            ):
                list(
                    self.powerbi._get_table_and_datamodel_lineage(
                        db_service_prefix=None,
                        table=table_a,
                        datamodel_entity=MOCK_DASHBOARD_DATA_MODEL,
                    )
                )
                list(
                    self.powerbi._get_table_and_datamodel_lineage(
                        db_service_prefix=None,
                        table=table_b,
                        datamodel_entity=MOCK_DASHBOARD_DATA_MODEL,
                    )
                )
                unresolved_calls = [
                    call
                    for call in mock_info.call_args_list
                    if call.args and "Unresolved PowerBI source reference" in call.args[0]
                ]
        finally:
            del self.powerbi._MAX_UNRESOLVED_LOGGED

        # Capped at 1 log line despite 2 distinct unresolved references.
        assert len(unresolved_calls) == 1
        assert "dummy_datamodel" in unresolved_calls[0].args[1]

        metrics = self.powerbi.metric_values()
        # The metric itself is not capped, only the log volume.
        assert metrics[PowerbiSource.METRIC_SOURCE_REFERENCES_UNRESOLVED] == 2

    @pytest.mark.order(76)
    def test_get_org_workspace_data_counts_outside_scope_links(self):
        """
        A dataset->dataflow link whose workspaceObjectId is not one of the
        workspaces this run processed is counted in
        upstream_links_outside_scope and never appended to
        Dataset.upstreamDataflows - it can never resolve, so it isn't tried.
        """
        workspace = Group(
            id="ws-1",
            name="Sales Workspace",
            datasets=[Dataset(id="dataset-1", name="Sales Dataset")],
        )

        mock_api_client = MagicMock()
        mock_api_client.fetch_all_workspaces.return_value = [workspace]
        mock_api_client.fetch_all_org_dashboards.return_value = []
        mock_api_client.fetch_all_org_tiles.return_value = []
        mock_api_client.fetch_all_org_reports.return_value = []
        mock_api_client.fetch_all_org_datasets.return_value = []
        mock_api_client.fetch_dataset_tables.return_value = []
        mock_api_client.fetch_all_org_dataflows.return_value = []
        mock_api_client.fetch_dataflow_upstream.return_value = []
        mock_api_client.fetch_dataset_to_dataflow_links.return_value = [
            DatasetUpstreamDataflowLink(
                datasetObjectId="dataset-1",
                dataflowObjectId="foreign-dataflow-1",
                workspaceObjectId="ws-outside-scope",
            )
        ]

        self.powerbi.client = MagicMock()
        self.powerbi.client.api_client = mock_api_client
        self.powerbi._metrics.clear()

        result_workspaces = list(self.powerbi.get_org_workspace_data())

        assert len(result_workspaces) == 1
        assert result_workspaces[0].datasets[0].upstreamDataflows == []
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_UPSTREAM_LINKS_OUTSIDE_SCOPE] == 1

    @pytest.mark.order(77)
    def test_get_org_workspace_data_in_scope_link_resolves_when_both_workspaces_ingested(self):
        """
        "Outside scope" means outside the set of workspaces processed in
        this run: when the dataflow's own workspace is also being ingested
        this run, the counter stays 0 and the link is appended normally.
        """
        workspace_a = Group(
            id="ws-a",
            name="Source Workspace",
            datasets=[Dataset(id="dataset-1", name="Sales Dataset")],
        )
        workspace_b = Group(id="ws-b", name="Target Workspace")

        mock_api_client = MagicMock()
        mock_api_client.fetch_all_workspaces.return_value = [workspace_a, workspace_b]
        mock_api_client.fetch_all_org_dashboards.return_value = []
        mock_api_client.fetch_all_org_tiles.return_value = []
        mock_api_client.fetch_all_org_reports.return_value = []
        mock_api_client.fetch_all_org_datasets.return_value = []
        mock_api_client.fetch_dataset_tables.return_value = []
        mock_api_client.fetch_all_org_dataflows.return_value = []
        mock_api_client.fetch_dataflow_upstream.return_value = []

        def links_for_group(group_id):
            if group_id == "ws-a":
                return [
                    DatasetUpstreamDataflowLink(
                        datasetObjectId="dataset-1",
                        dataflowObjectId="cross-workspace-dataflow",
                        workspaceObjectId="ws-b",
                    )
                ]
            return []

        mock_api_client.fetch_dataset_to_dataflow_links.side_effect = links_for_group

        self.powerbi.client = MagicMock()
        self.powerbi.client.api_client = mock_api_client
        self.powerbi._metrics.clear()

        result_workspaces = list(self.powerbi.get_org_workspace_data())

        resolved_workspace = next(w for w in result_workspaces if w.id == "ws-a")
        assert resolved_workspace.datasets[0].upstreamDataflows[0].targetDataflowId == "cross-workspace-dataflow"
        metrics = self.powerbi.metric_values()
        assert metrics.get(PowerbiSource.METRIC_UPSTREAM_LINKS_OUTSIDE_SCOPE, 0) == 0

    # -- Non-admin owner resolution -------------------------------------

    @pytest.mark.order(78)
    def test_principal_normalization_parses_user_group_and_app(self):
        """`PowerBIPrincipal.from_workspace_user`/`from_dataset_user` normalize
        both non-admin endpoints' rows, including Group and App principal
        types the admin-scan-shaped `PowerBIUser` has no equivalent for.
        """
        user_principal = PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_CONTRIBUTOR)
        assert user_principal == PowerBIPrincipal(
            principal_type="User",
            identifier="ada@example.com",
            email="ada@example.com",
            display_name="Ada",
            access_right="Contributor",
        )

        group_principal = PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_GROUP)
        assert group_principal.principal_type == "Group"
        assert group_principal.display_name == "Analytics Team"
        assert group_principal.email is None

        app_principal = PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_APP)
        assert app_principal.principal_type == "App"

        # Dataset ACL rows carry no email/displayName - a User's `identifier`
        # (documented as its UPN) is used as the email fallback, a Group's is not.
        dataset_user = PowerBIPrincipal.from_dataset_user(
            PowerBIDatasetUser(identifier="ada@example.com", principalType="User", datasetUserAccessRight="ReadWrite")
        )
        assert dataset_user.email == "ada@example.com"
        dataset_group = PowerBIPrincipal.from_dataset_user(
            PowerBIDatasetUser(identifier="group-object-id", principalType="Group", datasetUserAccessRight="ReadWrite")
        )
        assert dataset_group.email is None

    @pytest.mark.order(79)
    def test_non_admin_dataflow_owner_from_configured_by(self):
        """Dataflow owners include `configuredBy` (the non-admin field the
        `Dataflow` model previously never read - see `models.py`).
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow", configuredBy="ada@example.com")

        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:
            mock_get_ref.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Ada", type="user")]
            )
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is not None
        assert len(result.root) == 1
        assert result.root[0].name == "Ada"
        mock_get_ref.assert_called_once_with("ada@example.com")
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNERS_ASSIGNED_DATAFLOWS] == 1

    @pytest.mark.order(80)
    def test_non_admin_dataflow_excludes_viewer_includes_contributor(self):
        """Contributor is the lowest workspace role that can write; Viewer
        never becomes an owner (see the constants' generic-vs-policy note).
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        self.powerbi._metrics.clear()
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            workspace_principals=[
                PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_CONTRIBUTOR),
                PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_VIEWER),
            ],
        )
        self.powerbi.state.enter(workspace)
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow")

        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:
            mock_get_ref.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Ada", type="user")]
            )
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is not None
        assert len(result.root) == 1
        assert result.root[0].name == "Ada"
        mock_get_ref.assert_called_once_with("ada@example.com")
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNER_PRINCIPALS_SKIPPED_VIEWER] == 1

    @pytest.mark.order(81)
    def test_non_admin_datamodel_owners_from_dataset_acl_write_right_only(self):
        """Dataset-ACL principals with a write-level right become owners;
        read-only ACL principals are excluded.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        dataset = Dataset(
            id="dataset-1",
            name="Sales Semantic Model",
            dataset_principals=[
                PowerBIPrincipal.from_dataset_user(
                    PowerBIDatasetUser(
                        identifier="ada@example.com",
                        principalType="User",
                        datasetUserAccessRight="ReadWrite",
                    )
                ),
                PowerBIPrincipal.from_dataset_user(
                    PowerBIDatasetUser(
                        identifier="reader@example.com",
                        principalType="User",
                        datasetUserAccessRight="Read",
                    )
                ),
            ],
        )

        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:
            mock_get_ref.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Ada", type="user")]
            )
            result = self.powerbi.get_owner_ref(dataset)

        assert result is not None
        assert len(result.root) == 1
        mock_get_ref.assert_called_once_with("ada@example.com")

    @pytest.mark.order(82)
    def test_non_admin_report_owners_inherit_from_dataset(self):
        """Reports have no owner endpoint in non-admin mode (404) - they
        inherit their semantic model's owners via `datasetId`.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        dataset = Dataset(id="dataset-1", name="Sales Semantic Model", configuredBy="ada@example.com")
        workspace = Group(id="ws-a", name="Analytics Workspace", datasets=[dataset])
        self.powerbi.state.enter(workspace)
        report = PowerBIReport(id="report-1", name="Sales Report", datasetId="dataset-1")

        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:
            mock_get_ref.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Ada", type="user")]
            )
            result = self.powerbi.get_owner_ref(report)

        assert result is not None
        assert len(result.root) == 1
        assert result.root[0].name == "Ada"
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNERS_ASSIGNED_REPORTS] == 1

    @pytest.mark.order(83)
    def test_non_admin_dashboard_owners_union_reports(self):
        """Dashboards have no owner endpoint in non-admin mode either - they
        union the owners of every report behind their tiles.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        dataset_a = Dataset(id="dataset-a", name="Model A", configuredBy="ada@example.com")
        dataset_b = Dataset(id="dataset-b", name="Model B", configuredBy="grace@example.com")
        report_a = PowerBIReport(id="report-a", name="Report A", datasetId="dataset-a")
        report_b = PowerBIReport(id="report-b", name="Report B", datasetId="dataset-b")
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            datasets=[dataset_a, dataset_b],
            reports=[report_a, report_b],
        )
        self.powerbi.state.enter(workspace)
        dashboard = PowerBIDashboard(
            id="dashboard-1",
            displayName="Executive Dashboard",
            tiles=[
                Tile(id="tile-1", reportId="report-a"),
                Tile(id="tile-2", reportId="report-b"),
            ],
        )

        def fake_get_reference_by_email(email):
            name = {"ada@example.com": "Ada", "grace@example.com": "Grace"}[email]
            return EntityReferenceList(root=[EntityReference(id=uuid.uuid4(), name=name, type="user")])

        with patch.object(
            self.powerbi.metadata,
            "get_reference_by_email",
            side_effect=fake_get_reference_by_email,
        ):
            result = self.powerbi.get_owner_ref(dashboard)

        assert result is not None
        assert {ref.name for ref in result.root} == {"Ada", "Grace"}
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNERS_ASSIGNED_DASHBOARDS] == 1

    @pytest.mark.order(84)
    def test_non_admin_app_principal_never_resolved(self):
        """`App` principals are never owners, regardless of their access right."""
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        self.powerbi._metrics.clear()
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            workspace_principals=[PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_APP)],
        )
        self.powerbi.state.enter(workspace)
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow")

        with (
            patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref_email,
            patch.object(self.powerbi.metadata, "get_reference_by_name") as mock_get_ref_name,
        ):
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is None
        mock_get_ref_email.assert_not_called()
        mock_get_ref_name.assert_not_called()
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNER_PRINCIPALS_SKIPPED_APP] == 1
        assert metrics[PowerbiSource.METRIC_ASSETS_WITHOUT_OWNER] == 1

    @pytest.mark.order(85)
    def test_non_admin_unresolved_principal_leaves_asset_ownerless_and_counted(self):
        """`resolve_owner_principal` returning None leaves the asset owner-less
        and is counted, never treated as an error.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        self.powerbi._metrics.clear()
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            workspace_principals=[PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_CONTRIBUTOR)],
        )
        self.powerbi.state.enter(workspace)
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow")

        with patch.object(self.powerbi.metadata, "get_reference_by_email", return_value=None):
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is None
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNER_PRINCIPALS_UNRESOLVED] == 1
        assert metrics[PowerbiSource.METRIC_ASSETS_WITHOUT_OWNER] == 1

    @pytest.mark.order(86)
    def test_non_admin_group_principal_resolved_via_is_owner_team_lookup(self):
        """A `Group` principal is resolved as a Team by name with `is_owner=True`
        (rejects a same-named Team of the wrong type - see `get_reference_by_name`).
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            workspace_principals=[PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_GROUP)],
        )
        self.powerbi.state.enter(workspace)
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow")

        with patch.object(self.powerbi.metadata, "get_reference_by_name") as mock_get_ref_name:
            mock_get_ref_name.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Analytics Team", type="team")]
            )
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is not None
        assert result.root[0].name == "Analytics Team"
        mock_get_ref_name.assert_called_once_with(name="Analytics Team", is_owner=True)

    @pytest.mark.order(87)
    def test_resolve_owner_principal_override_is_honoured(self):
        """`resolve_owner_principal` is the seam a subclass overrides to
        provision missing principals; generic ingestion must call it (not
        create users/teams directly), so an override changes the outcome.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            workspace_principals=[PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_CONTRIBUTOR)],
        )
        self.powerbi.state.enter(workspace)
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow")
        provisioned_ref = EntityReference(id=uuid.uuid4(), name="Provisioned Ada", type="user")

        with (
            patch.object(self.powerbi, "resolve_owner_principal", return_value=provisioned_ref) as mock_override,
            patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref,
        ):
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is not None
        assert result.root[0] == provisioned_ref
        assert mock_override.called
        # Generic ingestion never calls OpenMetadata directly once the seam is overridden.
        mock_get_ref.assert_not_called()

    @pytest.mark.order(88)
    @patch("metadata.ingestion.ometa.ometa_api.OpenMetadata.get_reference_by_email")
    def test_admin_mode_owner_resolution_is_unaffected(self, get_reference_by_email):
        """`useAdminApis=True` (the mock config's default) still dispatches to
        the admin-mode owner path, byte-for-byte the pre-existing behaviour.
        """
        assert self.powerbi.service_connection.useAdminApis is True
        self.powerbi.metadata.get_reference_by_email.side_effect = [
            MOCK_USER_1_ENITYTY_REF_LIST,
            MOCK_USER_2_ENITYTY_REF_LIST,
        ]
        dashboard = PowerBIDashboard.model_validate(MOCK_DASHBOARD_WITH_OWNERS)

        with patch.object(self.powerbi, "_get_owner_ref_non_admin") as mock_non_admin:
            result = self.powerbi.get_owner_ref(dashboard)

        assert result is not None
        assert len(result.root) == 2
        mock_non_admin.assert_not_called()

    @pytest.mark.order(89)
    def test_is_write_dataset_right_is_a_prefix_predicate_not_an_exact_set(self):
        """Microsoft's documented dataset-right enum (Read, ReadWrite,
        ReadReshare, ReadWriteReshare, Owner) is incomplete - a live probe of
        a production workspace's dataset ACLs returned `Explore`-suffixed
        variants beyond it. `is_write_dataset_right` must classify those by
        capability (does it start with `ReadWrite`, or equal `Owner`?), not
        by exact membership in the documented set.
        """
        # Observed live, 231/291 ACL rows in the probed workspace: write-level.
        assert is_write_dataset_right("ReadWriteReshareExplore") is True
        # Documented values.
        assert is_write_dataset_right("ReadWrite") is True
        assert is_write_dataset_right("ReadWriteReshare") is True
        assert is_write_dataset_right("Owner") is True
        # Read-only, including the observed `Explore` suffix on a read right
        # (1/291 rows) - must NOT be treated as write just because it isn't
        # in a fixed "read" list.
        assert is_write_dataset_right("Read") is False
        assert is_write_dataset_right("ReadExplore") is False
        assert is_write_dataset_right("ReadReshare") is False
        # Absent/unknown right: never a false positive.
        assert is_write_dataset_right(None) is False
        assert is_write_dataset_right("") is False
        assert is_write_dataset_right("SomethingUnexpected") is False

    @pytest.mark.order(90)
    def test_non_admin_datamodel_owner_from_live_observed_explore_suffix_right(self):
        """End-to-end (not just the predicate unit test above): a dataset-ACL
        principal with the live-observed `ReadWriteReshareExplore` right - not
        in Microsoft's documented enum - must still become an owner; a
        principal with the also-observed `ReadExplore` (read-only) must not.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        dataset = Dataset(
            id="dataset-1",
            name="Sales Semantic Model",
            dataset_principals=[
                PowerBIPrincipal.from_dataset_user(
                    PowerBIDatasetUser(
                        identifier="ada@example.com",
                        principalType="User",
                        datasetUserAccessRight="ReadWriteReshareExplore",
                    )
                ),
                PowerBIPrincipal.from_dataset_user(
                    PowerBIDatasetUser(
                        identifier="reader@example.com",
                        principalType="User",
                        datasetUserAccessRight="ReadExplore",
                    )
                ),
            ],
        )

        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:
            mock_get_ref.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Ada", type="user")]
            )
            result = self.powerbi.get_owner_ref(dataset)

        assert result is not None
        assert len(result.root) == 1
        mock_get_ref.assert_called_once_with("ada@example.com")

    @pytest.mark.order(91)
    def test_non_admin_dataset_acl_group_with_no_usable_name_is_unresolved_not_crashed(self):
        """A `Group` principal normalized from the dataset-ACL endpoint (which
        carries no email or display name at all, only `identifier` and the
        right - see `PowerBIPrincipal.from_dataset_user`) must resolve to
        `None` and be counted unresolved, never crash and never be guessed at
        from its opaque `identifier` object id.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        self.powerbi._metrics.clear()
        dataset_acl_group = PowerBIPrincipal.from_dataset_user(
            PowerBIDatasetUser(
                identifier="group-object-id",
                principalType="Group",
                datasetUserAccessRight="ReadWriteReshareExplore",
            )
        )
        assert dataset_acl_group.email is None
        assert dataset_acl_group.display_name is None
        dataset = Dataset(
            id="dataset-1",
            name="Sales Semantic Model",
            dataset_principals=[dataset_acl_group],
        )

        with (
            patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref_email,
            patch.object(self.powerbi.metadata, "get_reference_by_name") as mock_get_ref_name,
        ):
            result = self.powerbi.get_owner_ref(dataset)

        assert result is None
        mock_get_ref_email.assert_not_called()
        mock_get_ref_name.assert_not_called()
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNER_PRINCIPALS_UNRESOLVED] == 1
        assert metrics[PowerbiSource.METRIC_ASSETS_WITHOUT_OWNER] == 1

    @pytest.mark.order(92)
    def test_non_admin_workspace_role_write_check_is_exact_match(self):
        """Unlike the dataset right, the workspace-role write check is a plain
        exact-match set (`POWERBI_WRITE_WORKSPACE_ROLES`) - verified live: a
        production workspace's `GET .../users` rows returned only `Admin`,
        `Member` and `Viewer`, all handled correctly by exact match.
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        workspace = Group(
            id="ws-a",
            name="Analytics Workspace",
            workspace_principals=[
                PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_CONTRIBUTOR),  # Contributor
                PowerBIPrincipal.from_workspace_user(MOCK_WORKSPACE_USER_VIEWER),  # Viewer
            ],
        )
        self.powerbi.state.enter(workspace)
        dataflow = Dataflow(objectId="dataflow-1", name="Orders Dataflow")

        with patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref:
            mock_get_ref.return_value = EntityReferenceList(
                root=[EntityReference(id=uuid.uuid4(), name="Ada", type="user")]
            )
            result = self.powerbi.get_owner_ref(dataflow)

        assert result is not None
        assert len(result.root) == 1

    @pytest.mark.order(93)
    def test_non_admin_dataset_acl_app_excluded_by_app_rule_not_by_right(self):
        """An `App` principal on a dataset's ACL holding a write-level right
        (live-observed: `App|ReadWriteReshareExplore`, 21 rows in the probed
        workspace) must still never become an owner - excluded by the `App`
        principal-type rule specifically, not because its right happens to
        fail the write-capability check. Paired with a `Group` holding the
        observed read-only `ReadExplore` right, also excluded (by right).
        """
        self.powerbi.service_connection.useAdminApis = False
        self.powerbi.source_config.includeOwners = True
        self.powerbi._metrics.clear()
        dataset = Dataset(
            id="dataset-1",
            name="Sales Semantic Model",
            dataset_principals=[
                PowerBIPrincipal.from_dataset_user(
                    PowerBIDatasetUser(
                        identifier="app-object-id",
                        principalType="App",
                        datasetUserAccessRight="ReadWriteReshareExplore",
                    )
                ),
                PowerBIPrincipal.from_dataset_user(
                    PowerBIDatasetUser(
                        identifier="group-object-id",
                        principalType="Group",
                        datasetUserAccessRight="ReadExplore",
                    )
                ),
            ],
        )

        with (
            patch.object(self.powerbi.metadata, "get_reference_by_email") as mock_get_ref_email,
            patch.object(self.powerbi.metadata, "get_reference_by_name") as mock_get_ref_name,
        ):
            result = self.powerbi.get_owner_ref(dataset)

        assert result is None
        mock_get_ref_email.assert_not_called()
        mock_get_ref_name.assert_not_called()
        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNER_PRINCIPALS_SKIPPED_APP] == 1
        assert metrics[PowerbiSource.METRIC_OWNER_PRINCIPALS_SKIPPED_VIEWER] == 1
        assert metrics[PowerbiSource.METRIC_ASSETS_WITHOUT_OWNER] == 1

    @pytest.mark.order(94)
    def test_metric_values_always_has_every_declared_key_at_zero(self):
        """A freshly constructed source's `metric_values()` must contain every
        declared `METRIC_*` key at 0, not merely the keys that happened to be
        incremented. A bare `Counter()` only emits a key once it's first
        incremented, making "this metric is legitimately 0" indistinguishable
        from "this code path never ran" on the far side of a Prometheus
        scrape - the failure mode that hid a real owners bug for a full run.
        """
        metrics = self.powerbi.metric_values()
        expected_keys = self.powerbi._all_metric_keys()
        # Guard against the guard: if this list is empty, the assertion below
        # would pass vacuously.
        assert len(expected_keys) > 10
        for key in expected_keys:
            assert key in metrics, f"metric_values() is missing declared key {key!r}"
            assert metrics[key] == 0

    @pytest.mark.order(95)
    def test_metric_values_incrementing_one_key_leaves_others_at_zero(self):
        """Incrementing one counter must not cause the others to vanish from
        `metric_values()` - they stay present at 0, not absent.
        """
        self.powerbi._metrics.clear()
        # clear() on a Counter drops every key back to unset; reseed exactly
        # as __init__ does, so this test doesn't depend on __init__'s
        # internals beyond the documented `_all_metric_keys()` seam.
        self.powerbi._metrics.update(dict.fromkeys(self.powerbi._all_metric_keys(), 0))

        self.powerbi._metrics[PowerbiSource.METRIC_OWNERS_ASSIGNED_DATAFLOWS] += 1

        metrics = self.powerbi.metric_values()
        assert metrics[PowerbiSource.METRIC_OWNERS_ASSIGNED_DATAFLOWS] == 1
        untouched_keys = [
            k for k in self.powerbi._all_metric_keys() if k != PowerbiSource.METRIC_OWNERS_ASSIGNED_DATAFLOWS
        ]
        assert len(untouched_keys) > 5
        for key in untouched_keys:
            assert metrics[key] == 0
