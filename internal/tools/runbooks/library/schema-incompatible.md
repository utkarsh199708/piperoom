---
id: schema-incompatible
title: Schema Registry rejects a new schema (incompatible change)
keywords: schema, incompatible, compatibility, backward, 409, subject, registry, avro, decimal, decimal.handling.mode, missing_union_branch, schema-change
---
## Symptoms
- After a source DDL the Debezium task FAILED; its trace says the schema being registered is incompatible with an earlier schema, with `error code: 409`.
- The subject keeps its old versions; the slot goes inactive and the warehouse stops updating while the consumer looks healthy.
- Seen on the Piperoom dev stack (`schema-change`): the registry reports `MISSING_UNION_BRANCH` on the `before`/`after` fields, not "amount changed type". The field-level cause only shows by diffing schema versions (`amount: int` → `bytes` with `decimal(12,2)`).

## Find it with Piperoom tools
- `connect_status` — FAILED task, `RestClientException … incompatible … error code: 409`, and the rejected-schema line
- `schema_registry_subjects` — the value subject of the topic
- `schema_registry_versions` — versions and the compatibility level (subject or global)
- `schema_registry_diff` — field-level changes between versions, or a raw schema against the latest
- `pg_recent_ddl` — the `ALTER TABLE` that changed the column

## Safe checks
- Compatibility types: `BACKWARD` (the default), `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE`, `NONE`.
- `GET /config/{subject}` shows a subject's level.
- `POST /compatibility/subjects/{subject}/versions/latest?verbose=true` tests a schema without registering it; `verbose=true` returns the reasons.
- Debezium maps `numeric` with `decimal.handling.mode`: `precise` (default, Avro `bytes` with the `decimal` logical type), `double` or `string`.

## Fixes
- Action `schema_set_compatibility` — set the subject to `NONE` (or a looser level) only for the change, then propose restoring the old level once the new version is registered.
- Action `connect_restart_task` — after the new schema is allowed, so the task registers it and streams the backlog.
- Action `schema_register` — only to register an agreed schema by hand; Debezium registers its own schema on restart.
- Manual — before letting the schema in, make sure every consumer can read the new field type.
- Manual — or revert the DDL in the source database.

## Risks
- Leaving compatibility at `NONE` lets the next breaking change through unnoticed.
- Consumers that cannot read the new type fail next, downstream.
- Changing `decimal.handling.mode` changes the type of every decimal column for all consumers.

## Sources
- https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html — compatibility types, `BACKWARD` default
- https://docs.confluent.io/platform/current/schema-registry/develop/api.html — `/config`, `/compatibility`, `verbose`
- https://debezium.io/documentation/reference/stable/configuration/avro.html — Avro converter and Schema Registry with Debezium
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-decimal-handling-mode — `precise`, `double`, `string`
