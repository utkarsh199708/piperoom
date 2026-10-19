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