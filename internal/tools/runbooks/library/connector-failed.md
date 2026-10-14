---
id: connector-failed
title: Debezium connector task failed (permissions, authentication, publication)
keywords: connector, task, failed, permission, publication, authentication, auth, replication, login, pg_hba.conf, create, publication.autocreate.mode, dbz_publication, ownership
---
## Symptoms
- `connect_status` shows a task FAILED with a trace; the connector itself may still say RUNNING.
- The slot is inactive, the topic gets no new records, the consumer is idle and the warehouse goes stale.
- Typical causes: the connector user cannot log in (password or `pg_hba.conf`), the role lacks the REPLICATION or LOGIN attribute, the user may not create or change the publication, or a captured table is missing from the publication.
- Changes of a table that is not in the publication are not streamed at all.

## Find it with Piperoom tools
- `connect_list_connectors` — the connector with a failed task count above 0
- `connect_status` — the FAILED task with its exception and `Caused by:` lines
- `connect_config` — `database.user`, `database.hostname`, `table.include.list` (publication settings are not shown)
- `pg_replication_slots` — `active=false` while the task is down

## Safe checks
- Role attributes: `SELECT rolname, rolreplication, rolcanlogin FROM pg_roles WHERE rolname = '<connector user>';`
- Publication contents: `SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';` (Debezium's default publication name).
- Connect: `GET /connectors/{name}/status` for the full trace of each task.
- Debezium's `publication.autocreate.mode`: `all_tables` (default) creates a publication for all tables; `filtered` creates or updates one for the captured tables only; `disabled` expects an existing publication and fails without it.

## Fixes
- Manual — give the connector role the REPLICATION and LOGIN attributes, and add a `pg_hba.conf` entry that lets the Connect host connect (DBA).
- Manual — creating a publication needs the CREATE privilege on the database; a publication `FOR ALL TABLES` needs a superuser; adding a table to a publication needs ownership of that table.
- Manual — add a missing table as its owner: `ALTER PUBLICATION dbz_publication ADD TABLE <schema.table>;`
- Manual — or have the DBA create the publication and set `publication.autocreate.mode=disabled`, so the connector never changes it.
- Action `connect_restart_task` / `connect_restart_connector` — only after the cause is fixed; `connect_restart_connector` restarts only failed tasks (`onlyFailed=true`).

## Risks
- A restart before the cause is fixed fails again in the same way.
- Superuser for the connector role gives it far more than replication needs.
- While the task is down the slot keeps WAL; see the slot runbook if the disk fills.

## Sources
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-permissions — role attributes, `pg_hba.conf`, publication privileges
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-publication-autocreate-mode — `all_tables`, `filtered`, `disabled`
- https://www.postgresql.org/docs/current/logical-replication-security.html — REPLICATION attribute, publication privileges
- https://www.postgresql.org/docs/current/sql-createpublication.html — CREATE privilege, superuser for `FOR ALL TABLES`
- https://www.postgresql.org/docs/current/sql-alterpublication.html — `ADD TABLE`, ownership needed to add a table
- https://www.postgresql.org/docs/current/view-pg-publication-tables.html — `pg_publication_tables`
- https://www.postgresql.org/docs/current/view-pg-roles.html — `rolreplication`, `rolcanlogin`
- https://docs.confluent.io/platform/current/connect/references/restapi.html — status and restart endpoints, `onlyFailed`
