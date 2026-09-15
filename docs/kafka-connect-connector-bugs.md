# Kafka Connect connector: bugs found against a Debezium cluster

Defects in the stock `kafkaconnect` pipeline connector and the code it depends on, found on
2026-09-15 by running it against a Strimzi-managed Kafka Connect 3.9 cluster running a Debezium
Postgres connector with Avro values in a Confluent-compatible registry. Each entry says what was
observed, the cause in code, and the fix on `fork-generic`.

| # | Symptom | Cause | Fix | Status |
|---|---|---|---|---|
| 1 | Every topic of the cluster has `schemaType: Avro`, the full `schemaText`, and zero `schemaFields`; the Kafka connector logs `Unable to parse the avro schema: InvalidName` per topic | Debezium builds the Avro record namespace from `topic.prefix`, which carries hyphens (`dev.platform.cards-dbz-01.db`). `parse_avro_schema` calls `avro.schema.parse` in strict mode, which rejects the name and the whole schema is dropped | parse with `validate_names=False`; the schema is unambiguous | fixed, `e1e88e4958`, test `test_avro_parser_lenient_names.py` |
| 2 | Table → topic edges are created but carry no column lineage even when fields are present | `_extract_columns_from_entity` treats a schema as a Debezium envelope only when `after`, `before` and `op` are all present. `ReplaceField$Value` transforms that drop `before` and `source` are the documented way to shrink Debezium payloads, and then detection fails and the envelope's own fields (`after`, `op`, `ts_ms`) are matched against table columns | `is_cdc_envelope`: `op` plus either row state | fixed, `5f3a8af5a7`, test `test_topic_lineage_cdc_envelope.py` |
| 3 | Same symptom as 2 once detection passes | The Avro `["null", {record Value}]` union parses into an `after` field whose only child is the `Value` record; the columns are that record's children. Both column extraction and `get_topic_field_fqn` looked one level too high and found `Value` | `unwrap_record`: look through a field whose single child is a record | fixed, same commit |
| 4 | Warning `Topic __debezium-heartbeat.<prefix> not found in OpenMetadata` on every run | The connector lists `/connectors/{name}/topics`, which includes the heartbeat topic, while the messaging ingestion excludes `^__.*` by default | none; harmless. Worth skipping `__debezium-heartbeat.*` in `_parse_and_resolve_topics` to keep the log clean | open |
| 5 | Connector `sourceUrl` is the raw `hostPort` | Fine for a UI host; for an in-cluster service URL it is a link nobody can open | none needed | note |

Things that worked as documented and are worth knowing:

- Service resolution by hostname needs the database service's `hostPort` to equal
  `database.hostname` of the connector. Services imported from another catalogue have no
  `hostPort`; `lineageInformation.dbServiceNames` is the fallback, and the table is then found
  by schema and name search, so `unistatement.default.rep.cards` resolves although Debezium
  reports database `unistatement_utf8`.
- `messagingServiceName` on the connection is honoured over broker matching; broker matching
  would also have worked here because the Connect config carries
  `database.history.kafka.bootstrap.servers`.
- Pipeline status comes from the Connect connector and task states (`RUNNING` → Successful,
  `FAILED`, `PAUSED`/`UNASSIGNED` → Pending). There is no run history, only the current state.
- The REST client is `kafka-connect-py`; the `kafkaconnect` extra must be installed.

Not observed here but visible in code: `_parse_cdc_schema_columns` (the fallback when a topic has
no parsed fields) only understands JSON Schema (`properties`, `oneOf`); an Avro `schemaText` is
never parsed there, so bug 1 could not be worked around from the Connect side.
