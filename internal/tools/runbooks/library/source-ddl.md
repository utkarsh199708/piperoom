---
id: source-ddl
title: Source table DDL (dropped, renamed or retyped column)
keywords: ddl, alter table, dropped, drop column, column, renamed, rename, type change, set data type, replica identity, pg_recent_ddl, migration
---
## Symptoms
- A migration changed a captured table: a column was dropped, renamed or given a new type.
- The Debezium PostgreSQL connector emits no DDL events: logical decoding does not report DDL, so the change only shows as a new value schema on the next record.
- If the registry rejects that schema, the task fails (see the schema-incompatible runbook).
- If the registry accepts it, Connect stays healthy and the consumer or the warehouse write may fail on the new shape.
- Seen on the Piperoom dev stack (`schema-change`): `ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)` gave a new Avro value schema that the registry rejected.

## Find it with Piperoom tools
- `pg_recent_ddl` — the `ALTER TABLE` and who ran it; it reads the DDL audit table `piperoom_audit.ddl_log`, so it is empty without the audit trigger
- `schema_registry_versions` — a new version registered (or not)
- `schema_registry_diff` — removed, added and retyped fields between versions
- `connect_status` — FAILED on a rejected schema, or RUNNING when it was accepted
- `pg_row` — the source row of a key
- `warehouse_row` — whether the warehouse has the latest version of that row
- `trace_record` — where a changed row stopped

## Safe checks
- `ALTER TABLE … ALTER COLUMN … SET DATA TYPE`, `DROP COLUMN` and `RENAME COLUMN` change what Debezium sends.
- Removing a field or adding one with a default is `BACKWARD` compatible, so the registry accepts it; old consumers are not protected from it.
- A rename is a removed field plus an added field.
- REPLICA IDENTITY decides which old values an update or delete carries: `DEFAULT` (primary key), `FULL` (all columns), `USING INDEX`, `NOTHING`.

## Fixes
- Action `schema_set_compatibility` / `connect_restart_task` — only when the registry rejected a type change and the change is agreed; restore the old level afterwards.
- Action `schema_register` — only to register an agreed schema by hand.
- Manual — migrate the consumer or the warehouse table to the new column set.
- Manual — or revert the DDL in the source database.

## Risks
- Skipping records written after the DDL loses every change since then.
- `REPLICA IDENTITY FULL` makes every update and delete write the whole old row to WAL.

## Sources
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-replica-identity — no DDL events from logical decoding, replica identity
- https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-SET-DATA-TYPE — `SET DATA TYPE`
- https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-DROP-COLUMN — `DROP COLUMN`
- https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-RENAME — `RENAME COLUMN`
- https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY — `DEFAULT`, `FULL`, `USING INDEX`, `NOTHING`
- https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html — what `BACKWARD` allows
