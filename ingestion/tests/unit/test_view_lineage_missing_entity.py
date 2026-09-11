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
get_view_lineage must degrade gracefully when the view's own Table entity cannot be resolved
(metadata.get_by_name returns None). It used to dereference table_entity.serviceType directly,
raising AttributeError that the broad except then reported as a misleading "Could not parse query".
Query-based lineage does not need that entity, so it must still run.
"""

from unittest.mock import MagicMock, patch

from metadata.generated.schema.metadataIngestion.parserconfig.queryParserConfig import (
    QueryParserType,
)
from metadata.ingestion.source.models import TableView
from metadata.utils import db_utils

VIEW_DDL = 'CREATE VIEW "svc_db"."schema"."the_view" AS SELECT id FROM "svc_db"."schema"."src_table"'


def _view() -> TableView:
    return TableView(
        table_name="the_view",
        schema_name="schema",
        db_name="svc_db",
        view_definition=VIEW_DDL,
    )


def test_missing_view_entity_does_not_crash_and_still_runs_query_lineage():
    metadata = MagicMock()
    metadata.get_by_name.return_value = None  # the view entity cannot be resolved

    with patch.object(db_utils, "get_lineage_by_query", return_value=[]) as mock_query_lineage:
        result = list(
            db_utils.get_view_lineage(
                view=_view(),
                metadata=metadata,
                service_names=["svc"],
                connection_type="Athena",
                timeout_seconds=10,
                parser_type=QueryParserType.Auto,
            )
        )

    # No AttributeError swallowed as a parse failure: the query-lineage branch was reached.
    assert result == []
    assert mock_query_lineage.called


def test_missing_view_entity_skips_table_entity_branch_without_crash():
    """When the parser yields no source/target and the entity is missing, skip cleanly rather than
    passing None into the table-entity lineage path."""
    metadata = MagicMock()
    metadata.get_by_name.return_value = None

    view = TableView(
        table_name="t",
        schema_name="s",
        db_name="d",
        # DDL the parser cannot turn into source+target, forcing the else branch
        view_definition="CREATE VIEW s.t AS SELECT 1",
    )

    with (
        patch.object(db_utils, "get_lineage_by_query", return_value=[]),
        patch.object(db_utils, "get_lineage_via_table_entity", return_value=[]) as mock_entity_lineage,
    ):
        result = list(
            db_utils.get_view_lineage(
                view=view,
                metadata=metadata,
                service_names=["svc"],
                connection_type="Athena",
                timeout_seconds=10,
                parser_type=QueryParserType.Auto,
            )
        )

    assert result == []
    # With no entity, the table-entity branch must not be invoked with None.
    assert not mock_entity_lineage.called
