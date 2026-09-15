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
Debezium envelopes as the Avro parser really produces them: no ``before`` (dropped by a
ReplaceField transform) and the row state wrapped in a nullable union record.
"""

import json
from unittest.mock import Mock
from uuid import uuid4

from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.type.basic import FullyQualifiedEntityName
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.schema import FieldModel, SchemaType
from metadata.generated.schema.type.schema import Topic as TopicSchema
from metadata.ingestion.lineage.topic_lineage import (
    get_topic_field_fqn,
    is_cdc_envelope,
    unwrap_record,
)
from metadata.ingestion.source.pipeline.kafkaconnect.metadata import KafkaconnectSource
from metadata.parsers.avro_parser import parse_avro_schema

ENVELOPE = json.dumps(
    {
        "type": "record",
        "name": "Envelope",
        "namespace": "dev.platform.cards-dbz-01.cards_db.rep.cards",
        "fields": [
            {
                "name": "after",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Value",
                        "fields": [{"name": "cardid", "type": "int"}, {"name": "cardname", "type": "string"}],
                    },
                ],
                "default": None,
            },
            {"name": "op", "type": "string"},
            {"name": "ts_ms", "type": ["null", "long"], "default": None},
        ],
    }
)


def _with_fqns(fields, prefix):
    for field in fields:
        field.fullyQualifiedName = FullyQualifiedEntityName(f"{prefix}.{field.name.root}")
        if field.children:
            _with_fqns(field.children, field.fullyQualifiedName.root)
    return fields


def _topic():
    fields = _with_fqns(parse_avro_schema(ENVELOPE), 'kafka."cards"')
    return Topic(
        id=uuid4(),
        name="cards",
        partitions=1,
        service=EntityReference(id=uuid4(), type="messagingService"),
        messageSchema=TopicSchema(schemaType=SchemaType.Avro, schemaFields=fields),
    )


def test_envelope_without_before_is_still_cdc():
    assert is_cdc_envelope({"after", "op", "ts_ms"})
    assert is_cdc_envelope(["before", "op"])
    assert not is_cdc_envelope({"after", "ts_ms"})
    assert not is_cdc_envelope({"id", "name"})


def test_unwrap_looks_through_a_single_record_child_only():
    plain = FieldModel(name="x", dataType="INT")
    wrapped = FieldModel(
        name="after", dataType="UNION", children=[FieldModel(name="Value", dataType="RECORD", children=[plain])]
    )
    assert unwrap_record(wrapped).name.root == "Value"
    assert unwrap_record(plain) is plain
    two = FieldModel(name="row", dataType="RECORD", children=[plain, FieldModel(name="y", dataType="INT")])
    assert unwrap_record(two) is two


def test_columns_and_field_fqns_come_from_the_wrapped_after_record():
    topic = _topic()
    source = Mock(spec=KafkaconnectSource)
    extract = KafkaconnectSource._extract_columns_from_entity.__get__(source, KafkaconnectSource)

    assert extract(topic) == ["cardid", "cardname"]
    assert get_topic_field_fqn(topic, "cardname") == 'kafka."cards".Envelope.after.Value.cardname'
