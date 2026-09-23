#  Copyright 2025 Collate
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
Constants used by PowerBI metadata ingestion
"""

OWNER_ACCESS_RIGHTS_KEYWORDS = ["owner", "write", "admin"]

# Power BI principal type strings, as returned verbatim by the non-admin
# workspace-users and dataset-users endpoints (`principalType`).
POWERBI_USER_PRINCIPAL_TYPE = "User"
POWERBI_GROUP_PRINCIPAL_TYPE = "Group"
POWERBI_APP_PRINCIPAL_TYPE = "App"

# Workspace roles that can publish or modify workspace content. Viewer is the
# only Power BI workspace role that cannot; Contributor is the lowest role
# that can - that is a property of the Power BI permission model itself, not
# of one deployment's policy, so this mapping is generic rather than a
# per-tenant choice.
# https://learn.microsoft.com/en-us/power-bi/collaborate-share/service-roles-new-workspaces
POWERBI_WRITE_WORKSPACE_ROLES = frozenset({"Admin", "Member", "Contributor"})

# Dataset-level access rights (from the non-admin dataset-users endpoint)
# that grant write access to the dataset, as opposed to the read-only
# `Read` right. Same generic reasoning as POWERBI_WRITE_WORKSPACE_ROLES.
POWERBI_WRITE_DATASET_RIGHTS = frozenset({"ReadWrite", "ReadWriteReshare", "Owner"})

SNOWFLAKE_QUERY_EXPRESSION_KW = "Value.NativeQuery(Snowflake.Databases("
DATABRICKS_QUERY_EXPRESSION_KW = "Value.NativeQuery(Databricks.Catalogs("
BIGQUERY_QUERY_EXPRESSION_KW = "Value.NativeQuery(GoogleBigQuery.Database("
SQL_DATABASE_EXPRESSION_KW = "Sql.Database("
ATHENA_DATABASES_EXPRESSION_KW = "AmazonAthena.Databases("
ODBC_QUERY_EXPRESSION_KW = "Odbc.Query("
ODBC_DATASOURCE_EXPRESSION_KW = "Odbc.DataSource("

# Athena's default-catalog placeholder: the M navigation's `Kind="Database"`
# level is always this literal unless a real federated catalog is configured,
# so it is never an OM database on its own.
ATHENA_DEFAULT_CATALOG = "AwsDataCatalog"

DEFAULT_REPORTS_PREFIX = "reports"
RDL_REPORT_FORMAT = "RDL"
RDL_REPORTS_PREFIX = "rdlreports"

# =============================================================================
# Power BI Admin API - OData Filter Node Limit
# =============================================================================
# The 'Groups - Get Groups As Admin' API enforces a MaxNodeCount limit of 100
# for OData $filter expressions. Each filter clause consumes a certain number
# of AST (Abstract Syntax Tree) nodes, and each 'or' operator adds 1 node.
#
# Node cost per filter type:
#   - trim(name) eq '{value}'        : ~6 nodes per clause (most expensive)
#   - startswith(name, '{value}')     : ~3 nodes per clause
#   - endswith(name, '{value}')       : ~3 nodes per clause
#   - contains(name, '{value}')       : ~3 nodes per clause
#
# Formula: (nodes_per_clause × N) + (N - 1) ≤ 100  # noqa: RUF003
#
# Worst case at N=10 (all trim eq): 6×10 + 9 = 69 nodes (within limit)  # noqa: RUF003
# Best case at N=10 (all contains):  3×10 + 9 = 39 nodes (within limit)  # noqa: RUF003
#
# Batch size is set to 10 to safely accommodate any mix of filter types
# while staying well under the 100-node limit.
# =============================================================================

MAX_PROJECT_FILTER_SIZE = 10

SQL_LINE_COMMENT_PATTERN = r"//[^\n]*"
