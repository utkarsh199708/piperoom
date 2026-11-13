---
id: source-restart
title: Source Postgres restarted, upgraded or failed over; the connector lost its connection
keywords: postgres restart, restart, reboot, maintenance, failover, upgrade, pg_upgrade, connection refused, i/o error, terminating connection, administrator command, shutting down, starting up, connection lost, retriable, errors.max.retries, auto restart, catch up
---
## Symptoms
- The source Postgres was restarted, upgraded, patched or failed over; shortly after, a Debezium task is FAILED.
- Typical traces: `PSQLException: An I/O error occurred while sending to the backend`, `Connection refused`, `FATAL: terminating connection due to administrator command`, `the database system is shutting down` or `is starting up`.
- The connector itself may still read RUNNING while its task is FAILED; no new records reach the topic and the warehouse goes stale.
- After the database is back, nothing recovers by itself: Kafka Connect does not restart a FAILED task on its own.
- After a restart the connector replays the WAL the slot kept while it was down, so lag drops gradually instead of at once. That is recovery, not a new failure.
- After a major-version upgrade or a failover to a standby, the replication slot may be missing; the task then fails on the slot, not the connection (see the restore runbook).

## Find it with Piperoom tools
- `connect_status` — the FAILED task and the first lines of its trace (connection error, not a data error)
- `connect_list_connectors` — every connector on the same database with a failed task
- `pg_replication_slots` — the slot still exists; `active=false` while the task is down, `active=true` and `restart_lsn` moving after the restart
- `pg_streaming_liveness` — whether changes flow again after the restart
- `warehouse_freshness` — new rows arriving again confirms the recovery end to end

## Safe checks
- Confirm the database accepts connections again before any restart: `SELECT pg_is_in_recovery(), now() - pg_postmaster_start_time();` (a recent start time confirms the restart).
- Confirm the slot survived: `SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status FROM pg_replication_slots;`
- Kafka Connect: `GET /connectors/{name}/status` for the trace of each task.
- Debezium retries connection errors itself for `errors.max.retries` attempts, waiting `retriable.restart.connector.wait.ms` between them; check both in the connector config and the docs of your Debezium version.

## Fixes
- Action `connect_restart_connector` — once the database accepts connections; it restarts only the failed tasks (`onlyFailed=true`). The task resumes from its stored offset and the slot replays the kept WAL.
- Action `connect_restart_task` — the same for one task.
- Manual — to restart the connector automatically once the database is back, list `connect_restart_task` and `connect_restart_connector` under the pipeline's `auto_approve:` in owners.yaml: the room still proposes, shows a countdown that the driver or an owner can cancel, and runs only with write mode on.
- Manual — set `errors.max.retries` (and `retriable.restart.connector.wait.ms`) on the connector so short restarts are absorbed by Debezium itself, without a FAILED task.
- Manual — after a major-version upgrade or failover where the slot is gone, follow the restore runbook instead of restarting.

## Risks
- Restarting before the database accepts connections fails again in the same way.
- If the slot is missing, a restart either fails or, with a snapshot mode that creates a new slot, starts from the new slot's position: changes written while no slot existed are lost unless a snapshot runs.
- During catch-up the source, Kafka and every sink carry the backlog at once; expect lag to fall over minutes, not seconds.

## Sources
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-errors-max-retries — `errors.max.retries`, `retriable.restart.connector.wait.ms`
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-when-things-go-wrong — behaviour when the database stops or restarts
- https://kafka.apache.org/documentation/#connect_rest — task restart, `onlyFailed`, `includeTasks`
- https://www.postgresql.org/docs/current/functions-info.html — `pg_postmaster_start_time`
- https://www.postgresql.org/docs/current/view-pg-replication-slots.html — slot columns
