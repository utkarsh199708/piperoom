# Optional ClickHouse dev warehouse

The default dev stack does not start ClickHouse. After starting the normal
pipeline and registering its Debezium connector, create the ClickHouse
warehouse objects with these commands, replacing <tag> with your stack owner:

    go run ./cmd/devctl lock --owner <tag>
    go run ./cmd/devctl up --owner <tag>
    go run ./cmd/devctl clickhouse init --owner <tag>
    go run ./cmd/devctl unlock --owner <tag>

The command starts the clickhouse Compose profile and creates
analytics.orders, the analytics.orders_kafka Kafka Engine table, and the
analytics.orders_mv materialized view. The Kafka table reads Confluent Avro
from pg.public.orders through the local Schema Registry.

The reader is piperoom_reader, with the piperoom_reader settings profile
(readonly=2) and SELECT access only on analytics.* and system.query_log. Host
clients use the native port localhost:9001; the port is bound to loopback.
These credentials are fixed local test credentials in dev/docker-compose.yml
and must not be reused outside this dev stack.

## What the sink stores

`amount` is an `INTEGER` in Postgres, so Debezium writes it as an Avro `int`.
ClickHouse stores an Avro `int` read into a `Decimal` column without scaling
(4969 would land as 49.69), so `analytics.orders_kafka` reads `amount` as
`Int64` and the view converts it with `toDecimal64(amount, 2)`: 4969 lands as
4969.00, the same value Postgres holds.

The Kafka table sets no offset-reset option: ClickHouse has no such table
setting (`kafka_auto_offset_reset` fails with `UNKNOWN_SETTING`), and its Kafka
consumer already sets librdkafka's `auto.offset.reset` to `earliest`
(`KafkaConfigLoader::getConsumerConfiguration` in 25.8). A new consumer group
therefore reads the snapshot rows that are already in the topic when
`devctl clickhouse init` runs.

Deletes are kept. A delete event carries the row only in `before`
(the table has `REPLICA IDENTITY FULL`), so the view reads `before` for
`op = 'd'` and sets `is_deleted = 1`. `analytics.orders` is a
`ReplacingMergeTree(version, is_deleted)`: query it with `FINAL` to see the
current rows without the deleted ones. Debezium's tombstones (empty values)
are skipped by the Kafka engine.

## The schema-change scenario breaks this sink on purpose

`devctl break schema-change` runs `ALTER TABLE orders ALTER COLUMN amount TYPE
numeric(12,2)` (tasks/A1-devstack.md). Connect then fails on the Schema
Registry check, so no new records reach the topic. After `devctl fix
schema-change` (compatibility `NONE`, task restarted), new records carry
`amount` as an Avro `decimal` (bytes), and the `Int64` read in
`analytics.orders_kafka` fails: the Kafka engine stops consuming and logs the
parse error, and `analytics.orders` stops updating. That is the intended
"warehouse sink breaks on the schema change" symptom. The table does not set
`kafka_handle_error_mode`, so bad records are never skipped silently.

## Notes

- `devctl clickhouse init` retries the init SQL every 2 s for up to 60 s while
  `clickhouse-client` cannot connect: the image's entrypoint first runs a
  temporary server on 127.0.0.1 for the init scripts, which also answers the
  healthcheck, so `up --wait` can return before the real server listens.
- The tables are created with `IF NOT EXISTS`. A container created by an
  earlier version of this command keeps its old tables; recreate it
  (`docker compose -f dev/docker-compose.yml --profile clickhouse down -v`)
  to pick up a schema change here.
