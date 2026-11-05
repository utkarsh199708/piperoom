---
id: restore-offset-mismatch
title: Database restored, cloned or upgraded; the connector's stored offsets do not match the new WAL or slot
keywords: restore, restored, clone, refresh, lower environment, staging, test environment, pg_upgrade, upgrade, new database, offsets, stored offset, lsn, wal, slot does not exist, replication slot, mismatch, skipped snapshot, reset offsets, snapshot.mode
---
## Symptoms
- A lower environment was refreshed from a production backup, a database was cloned or rebuilt, or Postgres had a major-version upgrade; the connector then fails or silently skips data.
- Typical traces: `replication slot "<name>" does not exist`, or an error that the requested or saved LSN is not available on the server.
- Quieter variant: the connector starts, finds stored offsets in Kafka Connect, skips its snapshot and streams only new changes, so rows that exist in the restored database never reach the topic.
- Kafka Connect keeps source offsets under the connector name; a connector that keeps its name after the database it reads changed reuses offsets that belong to the old database.
- A major-version upgrade with `pg_upgrade` does not keep logical replication slots unless the old cluster is PostgreSQL 17 or later.

## Find it with Piperoom tools
- `connect_status` — the failed task and the slot or LSN error in its trace
- `connect_config` — `database.hostname`, `database.dbname`, `slot.name`, `snapshot.mode`, `topic.prefix`
- `pg_replication_slots` — whether the slot named in the config exists on the new database, and its `confirmed_flush_lsn`
- `pg_publication_tables` — whether the publication exists on the restored database
- `reconcile` — row counts on the source against the warehouse show what was skipped

## Safe checks
- On the restored database: `SELECT slot_name, plugin, active, confirmed_flush_lsn FROM pg_replication_slots;` and `SELECT pg_current_wal_lsn();`
- Read the connector's stored source offsets (Kafka Connect 3.6+: `GET /connectors/{name}/offsets`) and compare their LSN with the new server's.
- Decide with the data owner whether the restored environment needs a full snapshot or only changes from now on.

## Fixes
- Action `connect_stop_connector` — stop the connector first; nothing is lost, the offsets stay stored.
- Action `connect_reset_source_offsets` — clear the stored offsets (or move them to a valid position) on the STOPPED connector; the preview says whether a snapshot follows from `snapshot.mode`. With `resume_after` it resumes the connector afterwards.
- Manual — create the publication and let the connector create its slot, or have the DBA create them, before resuming.
- Manual — for a refreshed lower environment, give its connector its own name and `topic.prefix` so it never inherits another environment's offsets or topics.
- Manual — after a major upgrade, plan the slot: recreate it and snapshot, or upgrade to 17+ on both sides so `pg_upgrade` keeps it.

## Risks
- A reset with a snapshot re-emits every row: sinks that do not upsert by key get duplicates.
- A reset without a snapshot starts from the new slot's position: changes before it are not streamed.
- Resetting the offsets of the wrong connector replays or skips another environment's data; check the name and database first.

## Sources
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-snapshot-mode — snapshot modes
- https://kafka.apache.org/documentation/#connect_rest — `GET`, `PATCH` and `DELETE /connectors/{name}/offsets` (KIP-875, Kafka 3.6+)
- https://www.postgresql.org/docs/current/pgupgrade.html — logical replication slots in `pg_upgrade`
- https://www.postgresql.org/docs/current/view-pg-replication-slots.html — slot columns
