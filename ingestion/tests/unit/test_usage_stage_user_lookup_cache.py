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
The usage stage resolves the query author against OpenMetadata once per (query, table) record.
Query logs repeat the same handful of principals thousands of times, so the lookup must be cached —
hits and misses alike — or the stage spends most of the run on GET /users/name/{fqn}.
"""

from unittest.mock import MagicMock

from metadata.generated.schema.entity.teams.user import User
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName, Uuid
from metadata.ingestion.stage.table_usage import TableStageConfig, TableUsageStage


def _stage(tmp_path, known_users):
    metadata = MagicMock()

    def get_by_name(entity, fqn):
        if entity is User and fqn in known_users:
            return User(
                id=Uuid("11111111-1111-1111-1111-111111111111"),
                name=EntityName(fqn),
                fullyQualifiedName=FullyQualifiedEntityName(fqn),
                email=f"{fqn}@example.com",
            )
        return None

    metadata.get_by_name = MagicMock(side_effect=get_by_name)
    return TableUsageStage(TableStageConfig(filename=str(tmp_path / "stage")), metadata), metadata


def test_resolved_user_yields_users_and_used_by(tmp_path):
    stage, _ = _stage(tmp_path, {"alice"})

    assert stage._get_user_entity("alice") == (["alice"], ["alice"])


def test_unresolved_principal_yields_only_used_by(tmp_path):
    stage, _ = _stage(tmp_path, set())

    assert stage._get_user_entity("etl-service-role") == (None, ["etl-service-role"])
    assert stage._get_user_entity("") == (None, None)
    assert stage._get_user_entity(None) == (None, None)


def test_repeated_lookups_hit_the_server_once_per_principal(tmp_path):
    stage, metadata = _stage(tmp_path, {"alice"})

    for _ in range(500):
        stage._get_user_entity("alice")
        stage._get_user_entity("etl-service-role")

    assert metadata.get_by_name.call_count == 2
    assert stage._get_user_entity("alice") == (["alice"], ["alice"])
    assert stage._get_user_entity("etl-service-role") == (None, ["etl-service-role"])


def test_user_lookup_cache_is_bounded(tmp_path):
    stage, metadata = _stage(tmp_path, set())

    for i in range(1500):
        stage._get_user_entity(f"principal-{i}")

    assert len(stage._user_lookup_cache) <= 1000
    assert metadata.get_by_name.call_count == 1500
