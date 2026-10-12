---
id: slot-wal
title: Replication slot inactive or WAL retention growing
keywords: replication slot, slot, inactive, wal, growing, retained, restart_lsn, wal_status, lost, long transaction, max_slot_wal_keep_size, idle_replication_slot_timeout, heartbeat, resume, stopped, paused
---
## Symptoms
- Disk use on the source Postgres grows; `pg_wal` takes more and more space.
- The Debezium slot shows `active = false`, or it stays active while `restart_lsn` stops moving.
- `wal_status` moves from `reserved` to `extended`, then `unreserved`; `lost` means the slot can no longer be used.
- Usual causes: the connector task FAILED, the connector was stopped or deleted, or a long-open transaction pins the slot.
- Seen on the Piperoom dev stack: a STOPPED connector leaves the slot `active=false` and retained WAL grows with every commit; a PAUSED connector keeps its connection, so the slot stays `active=true`.
- Seen on the Piperoom dev stack: a session left `idle in transaction` kept `restart_lsn` stuck while retained WAL rose; data still reached the warehouse.

## Find it with Piperoom tools
- `pg_replication_slots` — `active=false`, `retained` growing between calls, `wal_status`
- `pg_long_transactions` — a session open for minutes or hours (state `idle in transaction`)
- `connect_status` — task FAILED, or connector PAUSED / STOPPED
- `connect_list_connectors` — the connector is missing or STOPPED
- `pipeline_map` — the source node shows `slot_active=false` and a large `slot_retained`

## Safe checks
Read-only SQL on the source database:
- `SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status, safe_wal_size FROM pg_replication_slots;`
- PostgreSQL 17 and later also show `inactive_since` and `invalidation_reason` in `pg_replication_slots`.
- `SELECT pid, usename, state, xact_start, backend_xmin, query FROM pg_stat_activity WHERE xact_start < now() - interval '5 minutes';`
- Kafka Connect: `GET /connectors/{name}/status` shows the connector and task state.

## Fixes
- Action `connect_restart_task` / `connect_restart_connector` — only when a task is FAILED and its cause is fixed; `connect_restart_connector` restarts only failed tasks (`onlyFailed=true`).
- Action `connect_resume_connector` — resume a STOPPED or PAUSED connector (`PUT /connectors/{name}/resume`) once the reason for the stop is known and safe to lift; the slot then replays the WAL it kept. Proposed through the approval flow: the kafka-connect owner approves before it runs. Rollback: pause it again with `PUT /connectors/{name}/pause`.
- Manual — end a blocking transaction with `SELECT pg_terminate_backend(pid);` (DBA, after checking whose session it is).
- Manual — prevent idle transactions with `idle_in_transaction_session_timeout`.
- Manual — for databases where the captured tables are quiet, set Debezium `heartbeat.interval.ms` and `heartbeat.action.query` so the connector can confirm a newer LSN.
- Manual — cap retained WAL with `max_slot_wal_keep_size` (default -1, no limit) or `idle_replication_slot_timeout` (default 0, off).
- Manual (last resort) — drop the slot with `SELECT pg_drop_replication_slot('<slot>');`; the connector then needs a new snapshot.

## Risks
- Dropping a slot loses Debezium's position: every change since then must come from a new snapshot.
- `pg_terminate_backend` rolls back that session's open transaction.
- `max_slot_wal_keep_size` and `idle_replication_slot_timeout` protect the disk by invalidating the slot (`wal_status` `lost`), which also needs a new snapshot.
- Restarting a task whose cause is not fixed fails again in the same way.
- Resuming a connector that someone stopped on purpose (for example for maintenance) starts work they meant to hold. Check with the owner first; the approval step is where that check happens.
- A resume replays the whole backlog the slot kept. A large backlog adds catch-up load on the source and the consumers.
- Rollback is not a clean undo: pausing keeps the slot `active=true` and WAL keeps being retained until the connector runs again.
- If `wal_status` is `lost`, resuming cannot bring the changes back; the slot needs a new snapshot.

## Sources
- https://www.postgresql.org/docs/current/view-pg-replication-slots.html — slot columns, `wal_status` values, `inactive_since`, `invalidation_reason`
- https://www.postgresql.org/docs/current/runtime-config-replication.html — `max_slot_wal_keep_size`, `idle_replication_slot_timeout`
- https://www.postgresql.org/docs/current/functions-admin.html — `pg_terminate_backend`, `pg_drop_replication_slot`
- https://www.postgresql.org/docs/current/monitoring-stats.html — `pg_stat_activity` columns
- https://www.postgresql.org/docs/current/runtime-config-client.html — `idle_in_transaction_session_timeout`
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-wal-disk-space — WAL growth and heartbeats
- https://kafka.apache.org/41/kafka-connect/user-guide/ — connector status, pause, resume and stop REST endpoints
