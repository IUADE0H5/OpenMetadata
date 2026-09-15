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
Avro schemas whose namespace breaks the Avro name grammar still yield their fields.
"""

import json

from metadata.parsers.avro_parser import parse_avro_schema

# Debezium builds the namespace from the topic name; a hyphen in the connector or server name
# lands in the namespace and the strict avro parser rejects the whole schema as InvalidName.
DEBEZIUM_ENVELOPE = json.dumps(
    {
        "type": "record",
        "name": "Envelope",
        "namespace": "dev.data-platform.cards-dbz-01.cards_db.rep.cards",
        "fields": [
            {
                "name": "after",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Value",
                        "fields": [
                            {"name": "cardid", "type": "int"},
                            {"name": "cardname", "type": ["null", "string"], "default": None},
                        ],
                        "connect.name": "dev.data-platform.cards-dbz-01.cards_db.rep.cards.Value",
                    },
                ],
                "default": None,
            },
            {"name": "op", "type": "string"},
        ],
    }
)


def test_hyphenated_namespace_is_parsed():
    parsed = parse_avro_schema(DEBEZIUM_ENVELOPE)

    assert parsed is not None
    envelope = parsed[0]
    assert envelope.name.root == "Envelope"
    assert [field.name.root for field in envelope.children] == ["after", "op"]
    value = envelope.children[0].children[0]
    assert value.name.root == "Value"
    assert [column.name.root for column in value.children] == ["cardid", "cardname"]
