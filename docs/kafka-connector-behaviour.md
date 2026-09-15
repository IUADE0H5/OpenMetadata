# Kafka connector: what it actually collects

The official connector page lists connection fields and stops there. This is what
`ingestion/src/metadata/ingestion/source/messaging/kafka/` and the shared
`common_broker_source.py` do, call by call, read from the code (2026-09-15). Redpanda uses the
same source.

## Calls made

| Step | Call | Purpose |
|---|---|---|
| test connection `GetTopics` | AdminClient `list_topics(timeout=10)` | credentials, protocol |
| test connection `CheckSchemaRegistry` | Schema Registry `GET /subjects` | registry reachable; fails the test when no registry URL is configured |
| topic listing | AdminClient `list_topics()` (Kafka Metadata request) | every topic the principal may describe, then `topicFilterPattern` |
| per topic | Schema Registry `GET /subjects/<topic><suffix>/versions/latest` (suffix default `-value`) | message schema; a miss is a warning on the topic, not a failure |
| per topic | AdminClient `describe_configs([topic])` (DescribeConfigs) | topic configuration |
| per topic, only with `generateSampleData` | Consumer `subscribe`, `poll` | up to 10 messages |
| end of run, with `markDeletedTopics` | OpenMetadata topic listing for the service | soft-delete topics no longer seen |

Nothing else: no consumer groups, no offsets or lag, no throughput, no ACLs, no per-partition
leaders or ISR, no producer or consumer identity, no lineage. The Kafka `yield_topic_lineage`
hook is an empty default.

## What lands on the Topic entity

From the Metadata response:

- `partitions`: number of partitions.
- `replicationFactor`: replica count of partition 0.

From DescribeConfigs, when the config key is present:

- `maximumMessageSize` from `max.message.bytes`
- `minimumInSyncReplicas` from `min.insync.replicas`
- `retentionTime` from `retention.ms`
- `retentionSize` from `retention.bytes`
- `cleanupPolicies` from `cleanup.policy`, split on comma
- `topicConfig`: the complete DescribeConfigs map, every key with its effective value, including
  broker defaults and static defaults. A DescribeConfigs failure only logs a warning; the topic is
  still created without these fields.

From the registry, when the subject exists:

- `messageSchema.schemaText`: the registered schema string as-is
- `messageSchema.schemaType`: `Avro`, `JSON`, `Protobuf`, or `Other` when the registry type is not
  one of those
- `messageSchema.schemaFields`: the schema parsed into a field tree (name, data type, children)
  by the parser registered for that type. Protobuf references are fetched from the registry and
  merged into one text before parsing. When the subject is missing the topic gets an empty
  `Other` schema.

Always: `name`, `service`. Owners, tags, description and display name are never read from Kafka;
`overrideMetadata` only decides whether the run may blank what a user typed in the UI.

## Sample data

Off by default and also switched off globally by the profiler setting `storeSampleData: false`.
When on, the consumer joins group `openmetadata-consumer` (override with `group.id` in
`consumerConfig`), seeks each partition to its high watermark minus 50, polls up to 10 messages
for at most 10 seconds, commits nothing, and unsubscribes. Messages are decoded with the topic's
registry schema for Avro, as UTF-8 text otherwise after stripping the Confluent wire header;
Protobuf payloads are stored as empty strings. The result is written to the topic's
`sampleData.messages` and is visible to anyone who can view the topic.

## Configuration that matters

- `bootstrapServers`, `securityProtocol`, `saslMechanism`, `saslUsername`, `saslPassword` are
  copied into the librdkafka config. `consumerConfig` is passed through unchanged and wins over
  them for `security.protocol`. `group.id`, `enable.auto.commit`, `auto.offset.reset` are removed
  from the AdminClient copy.
- `schemaRegistryURL`, `basicAuthUserInfo` (`user:password`), `schemaRegistryConfig` go to the
  Confluent `SchemaRegistryClient`. Without a URL the test connection fails on the registry step
  even though ingestion itself would run.
- `schemaRegistryTopicSuffixName` (default `-value`) is appended to the topic name to find the
  subject.
- `consumerConfigSSL` and `schemaRegistrySSL` write CA, cert and key to temp files and set the
  matching `ssl.*` keys.
- `topicFilterPattern` exists both on the connection (default excludes `^__.*`, `^_schemas$`,
  `^_confluent.*`) and in the pipeline `sourceConfig`; the pipeline one is what the run uses.

## Amazon MSK with IAM authentication

MSK IAM is SASL/OAUTHBEARER with a SigV4-signed token. librdkafka can only get such a token from
a Python `oauth_cb` callback, and the stock connector has no way to install one: `consumerConfig`
is JSON. The stock connector therefore cannot connect to an IAM-only MSK cluster. A connection
class that adds the callback is a small extension (the `aws-msk-iam-sasl-signer-python` package
produces the token); it also has to poll the AdminClient's main queue before each admin call,
because the callback runs from that queue and AdminClient results are resolved on a different one.

## Permissions needed

The same rights expressed for both authorization models: IAM actions for MSK IAM, Kafka ACLs for
clusters where the client is a certificate (mTLS) or a SASL user. On MSK the mapping is the one
AWS documents for `kafka-cluster:`; on other Kafka distributions the ACL column is what to grant.

| Connector step | MSK IAM action (resource) | Kafka ACL (resource) |
|---|---|---|
| open a connection | `Connect` (cluster) | none, authentication is enough |
| `list_topics` | `DescribeCluster` (cluster), `DescribeTopic` (topic/*) | `Describe` on Cluster, `Describe` on Topic `*` |
| `describe_configs` | `DescribeTopicDynamicConfiguration` (topic/*) | `DescribeConfigs` on Topic `*` |
| sample data: join the group | `DescribeGroup`, `AlterGroup` (group/openmetadata-consumer) | `Describe` and `Read` on Group `openmetadata-consumer` |
| sample data: read messages | `ReadData` (topic/*) | `Read` on Topic `*` |

For a certificate principal the ACLs, without sample data, are:

```bash
kafka-acls.sh --bootstrap-server <broker:9094> --command-config <admin.properties> --add \
  --allow-principal "User:CN=openmetadata-ingestion,OU=...,O=..." \
  --operation Describe --cluster
kafka-acls.sh --bootstrap-server <broker:9094> --command-config <admin.properties> --add \
  --allow-principal "User:CN=openmetadata-ingestion,OU=...,O=..." \
  --operation Describe --operation DescribeConfigs --topic '*'
```

Sample data adds `--operation Read --topic '*'` and `--operation Describe --operation Read --group openmetadata-consumer`.
The principal string is the certificate's full distinguished name unless the cluster maps it with
`ssl.principal.mapping.rules`; on MSK mTLS clusters with no ACLs at all, `allow.everyone.if.no.acl.found`
is true and every authenticated certificate can already describe and read. The service connection
for that case is `securityProtocol: SSL` with the client certificate and key in `consumerConfigSSL`.

The registry needs read access to `/subjects`, which basic auth or the client certificate grants
on the registry side; there is no Kafka ACL involved.
