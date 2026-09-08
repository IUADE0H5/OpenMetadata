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
The server aggregates table usage up to the schema and database, but a percentile rank only exists
for the entity types the sink asks it to compute. Schemas were never asked for, so they showed as
0th percentile with non-zero counts.
"""

from unittest.mock import MagicMock

from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.bulksink.metadata_usage import MetadataUsageBulkSink


def test_close_computes_percentiles_for_tables_schemas_and_databases(tmp_path):
    sink = MetadataUsageBulkSink.__new__(MetadataUsageBulkSink)
    sink.config = MagicMock()
    sink.config.filename = str(tmp_path / "missing-stage-dir")
    sink.metadata = MagicMock()
    sink.today = "2026-09-02"

    sink.close()

    requested = {call.args[0] for call in sink.metadata.compute_percentile.call_args_list}
    assert requested == {Table, DatabaseSchema, Database}
    assert all(call.args[1] == "2026-09-02" for call in sink.metadata.compute_percentile.call_args_list)
