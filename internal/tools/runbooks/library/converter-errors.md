---
id: converter-errors
title: Records the converter cannot read (no schema, wrong format, missing headers); the connector stops
keywords: converter, unknown magic byte, magic byte, schemas.enable, schema and payload, headless, headers, no schema, plain json, wrong format, mixed formats, tombstone, null value
---
## Symptoms
- A task is FAILED with a converter error, often on a sink connector or a consumer reading the CDC topic.
- Typical traces: `Unknown magic byte!` (a Schema Registry converter reading data that was not written with it), `JsonConverter with schemas.enable requires "schema" and "payload" fields` (plain JSON read with `schemas.enable=true`), or an error on a record whose value is null.
- One producer writes records in another format to the same topic (a test tool, a manual write, a second connector), or the key and value converters do not match the writer's.
- Tombstones (records with a null value after a delete) reach a sink that cannot take them.

## Find it with Piperoom tools
- `connect_status` — the FAILED task and the converter exception
- `connect_config` — `key.converter`, `value.converter`, `*.converter.schemas.enable`, `errors.tolerance` and the DLQ settings of both connectors
- `kafka_find_by_key` — the record at the failing offset, its headers and value
- `dlq_peek` — what already landed in the dead letter queue, with the error headers
- `schema_registry_versions` — whether the subject the converter expects exists

## Safe checks
- Compare the writer's converter (the source connector) with the reader's (the sink or the consumer): both must use the same format and the same `schemas.enable`.
- Find who else writes to the topic: a record without the Schema Registry header byte did not come from the Avro converter.
- Check whether the failing record is a tombstone (null value) and whether the sink is set to accept or drop them.

## Fixes
- Action `connect_update_connector_config` — align the sink's converters with the writer's, or set `errors.tolerance=all` with a dead letter queue so one bad record does not stop the sink; the preview shows the change.
- Action `connect_restart_task` — after the config is fixed.
- Manual — stop the extra writer, or move it to its own topic.
- Manual — for tombstones: Debezium's `ExtractNewRecordState` with `delete.tombstone.handling.mode`, or the sink's own delete settings.
- Manual — replay the records parked in the dead letter queue once the cause is fixed.

## Risks
- `errors.tolerance=all` without a dead letter queue skips bad records silently.
- A dead letter queue exists for sink connectors only; a source connector's converter errors still stop it.
- Changing a sink's converter for a topic that holds both formats only moves the failure to the other records.

## Sources
- https://kafka.apache.org/documentation/#connect_errorreporting — `errors.tolerance`, dead letter queue for sinks
- https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html — the Schema Registry wire format and magic byte
- https://debezium.io/documentation/reference/stable/transformations/event-flattening.html — `ExtractNewRecordState`, tombstones
- https://kafka.apache.org/documentation/#connect_configuring — converter settings
