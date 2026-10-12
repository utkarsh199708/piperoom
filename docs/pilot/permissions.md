# Least-privilege permissions

Piperoom's agent is **read-only by design**. Reads are used for investigation; the only
writes are four approved fix actions, executed after every required layer owner approves.
Grant exactly what is below and nothing more. Create **dedicated** service accounts for
Piperoom, never reuse a human or application credential, so you can audit and revoke it.

The names below match the code: tools in `internal/tools/{dbtools,kafkatools}`, actions in
`internal/actions`, clients in `internal/clients/*`. Replace placeholders (`<...>`).

## Summary

| System | Used by | Read access needed | Write access needed |
|---|---|---|---|
| Source Postgres | `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`, `pg_row`, `trace_record` | pipeline tables (`SELECT`), `piperoom_audit.ddl_log`, stat views | **none** |
| Warehouse (Postgres or Snowflake) | `warehouse_freshness`, `warehouse_row`, `trace_record` | `SELECT` on the pipeline's sink tables | **none** |
| Kafka | `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag`, `kafka_find_by_key`, `trace_record` | describe topics/groups, read pipeline topics | **none** |
| Kafka Connect | `connect_list_connectors`, `connect_status`, `connect_config` | REST `GET` | restart connector/task (`connect_restart_*`) |
| Schema Registry | `schema_registry_subjects`, `_versions`, `_diff` | REST `GET` | set compatibility, register schema |
| Room database | room service | n/a | full access to its **own** database only (it creates its tables itself) |
| Anthropic API | agent | API key (env var) | n/a |

Everything in the "write" column other than the room database is touched **only** by
`internal/actions`, and only after approval. If you do not want Piperoom to ever change
Connect or the Registry, give it read-only credentials there (see below): proposals will
still be created, but executing them will fail. That is a valid, safe pilot mode.

## Source Postgres

Create a read-only role limited to the pipeline's tables. Piperoom also forces
`default_transaction_read_only = on` on its connections as a second layer, but the role must
not be able to write in the first place.

```sql
CREATE ROLE piperoom_ro LOGIN PASSWORD '<generated, from your secret store>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  CONNECTION LIMIT 5;
ALTER ROLE piperoom_ro SET default_transaction_read_only = on;
ALTER ROLE piperoom_ro SET statement_timeout = '10s';

GRANT CONNECT ON DATABASE <db> TO piperoom_ro;

-- Only the tables listed under source_tables in owners.yaml.
GRANT USAGE ON SCHEMA public TO piperoom_ro;
GRANT SELECT ON public.orders TO piperoom_ro;     -- repeat per pipeline table

-- DDL audit log (see "DDL audit" below). Optional: without it pg_recent_ddl reports "not installed".
GRANT USAGE ON SCHEMA piperoom_audit TO piperoom_ro;
GRANT SELECT ON piperoom_audit.ddl_log TO piperoom_ro;
```

Notes:

- **Do not** grant `SELECT` on all tables or on schemas (`GRANT ... ON ALL TABLES`).
  `pg_row` is limited to the pipeline's `source_tables` in code, but the credential is the
  real boundary: a bug or a changed `owners.yaml` must not widen what the role can read.
- `pg_replication_slots` is readable by any role. No `REPLICATION` attribute is needed or wanted.
- `pg_long_transactions` reads `pg_stat_activity`. Without `pg_read_all_stats` the role sees
  other sessions' rows but not their `query` text. That is the **preferred** least-privilege
  state. Do not grant `pg_read_all_stats` or `pg_monitor` unless you accept the agent
  seeing live query text (which can contain literal values).
- Primary-key discovery uses `information_schema`, which only lists tables the role can
  access, so the grants above are enough.
- Never use the Debezium replication user or the application user here.

### DDL audit (optional, recommended)

`pg_recent_ddl` reads `piperoom_audit.ddl_log`, filled by an event trigger. The reference
implementation is `dev/postgres-init/02-ddl-audit.sql`. Installing an event trigger
requires a superuser (or equivalent) **once**, run by your DBA, not by Piperoom. The trigger
function is `SECURITY DEFINER`, so Piperoom's role only needs `SELECT` on the log table.
The log stores the full DDL statement text (`query`), so review that it does not contain
secrets (for example `CREATE ROLE ... PASSWORD`) before enabling it in production.

## Warehouse

Same principle: a dedicated `SELECT`-only identity on the sink tables from `sink_tables`
in `owners.yaml`.

**Postgres-compatible warehouse**: as above, a role with `CONNECT`, `USAGE` on the schema
and `SELECT` on the sink tables.

**Snowflake** (`internal/clients/snowflake`): the client does not enforce read-only; the role
in the DSN does. Create a role with only:

```sql
CREATE ROLE PIPEROOM_RO;
GRANT USAGE ON WAREHOUSE <wh> TO ROLE PIPEROOM_RO;           -- ideally a small, dedicated warehouse
GRANT USAGE ON DATABASE <db> TO ROLE PIPEROOM_RO;
GRANT USAGE ON SCHEMA <db>.<schema> TO ROLE PIPEROOM_RO;
GRANT SELECT ON TABLE <db>.<schema>.<sink_table> TO ROLE PIPEROOM_RO;  -- per sink table
CREATE USER PIPEROOM_SVC DEFAULT_ROLE = PIPEROOM_RO ...;
GRANT ROLE PIPEROOM_RO TO USER PIPEROOM_SVC;
```

Do not grant `INSERT/UPDATE/DELETE/CREATE/OWNERSHIP`, and do not give the user other roles.
Set a resource monitor / `STATEMENT_TIMEOUT_IN_SECONDS` on the dedicated warehouse to cap cost.

## Kafka

Piperoom uses the Kafka protocol for metadata, offsets, consumer-group lag, and reading
records from a topic by key or time (it assigns partitions directly; it never joins a
consumer group, never commits offsets, never produces, never creates/deletes topics).

Minimum ACLs for a principal such as `User:piperoom`:

| Resource | Operation | Why |
|---|---|---|
| Topic `<prefix>*` (prefixed, only the pipeline's topics) | `Describe` | list topics, start/end offsets |
| Topic `<prefix>*` | `Read` | `kafka_find_by_key`, `trace_record` |
| Group `<consumer group>` (literal, one per consumer in `owners.yaml`) | `Describe` | `kafka_consumer_lag` |
| Cluster | `Describe` only if your brokers require it for group listing | `kafka_list_groups` |

Do **not** grant `Write`, `Create`, `Delete`, `Alter`, `AlterConfigs`, `ClusterAction`, or `Read`
on groups (that would allow committing offsets). Do not grant `Read` on topics outside the
pipeline: record values may contain customer data (see [data-handling.md](data-handling.md)).

Use TLS and SASL on the listener if your cluster has them. The settings exist
(`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `KAFKA_TLS_*`, and the same for Schema Registry
and Connect; see [setup.md](setup.md#1-configure-the-environment)), and `internal/wiring`
builds the Kafka, Schema Registry and Connect clients from them (`netsec`). Basic auth is
refused over plain `http://`, and Kafka PLAIN needs TLS. This was checked in unit tests with
fake servers only, not against a real secured cluster, so try it on staging first.

## Kafka Connect

Connect's REST API usually has no per-endpoint authorization, so scope it with the network
and, where available, a proxy or the Connect REST extension that your platform provides.

- **Read-only mode (recommended default):** allow only `GET /connectors`,
  `GET /connectors/{name}/status`, `GET /connectors/{name}/config`. Piperoom reads connector config
  and **redacts** keys containing `password`, `secret`, `token`, or `.key` before showing
  them, but still prefer connectors whose secrets use config providers
  (`${file:...}` / `${vault:...}`) so no secret is in the REST output at all.
- **Write mode (only to allow approved fixes):** additionally allow
  `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true` and
  `POST /connectors/{name}/tasks/{id}/restart`.
  Piperoom's actions use nothing else: no create, delete, pause, or config `PUT`.

Also restrict the allowed `{name}` values to the pipeline's connectors if your proxy can.
Piperoom already scopes every write action to the room's pipeline (`actions.Scoped.CheckScope`);
this is defence in depth.

## Schema Registry

- **Read-only:** `GET /subjects`, `GET /subjects/{subject}/versions[/{version}]`,
  `GET /schemas/ids/{id}` (decoding Avro records for `kafka_find_by_key` / `trace_record`),
  `GET /config/{subject}` and `GET /config` (compatibility).
- **Write mode:** additionally `PUT /config/{subject}` (set compatibility) and
  `POST /subjects/{subject}/versions` (register schema).

Use Registry ACLs / RBAC where available and restrict writes to the pipeline's subjects
(the topic-name prefix). A relaxed compatibility level or a new schema version affects all
producers and consumers of that subject immediately, so keep write mode off until you trust
the approval process in your team.

## Room database

The room service uses a separate Postgres database (`PIPEROOM_DB_URL`) that Piperoom owns.
At start-up `cmd/server` applies the embedded goose migrations itself (`cmd/server/main.go:92`,
`:210-231`, `migrations/migrate.go:20`), so the role in `PIPEROOM_DB_URL` needs rights to
create tables and indexes in that database (migration `00001_init.sql` creates `rooms`,
`room_events`, `proposals`, `approvals`, `suggestions`, `handoffs`; `00002` adds
`proposal_steps`; goose adds its own `goose_db_version` table). It also needs `LISTEN`/`NOTIFY`,
which any role has. Give it its own database and role. That role must not have access to any
pipeline data. It holds the permanent room history, so treat it with the same care as the data
in it (see [data-handling.md](data-handling.md)).

## Anthropic API key

Set `ANTHROPIC_API_KEY` in the process environment from your secret manager. It is never
written to config files, events, or logs by Piperoom. Use a dedicated key for the pilot
with a spend limit, so it can be revoked or capped on its own.

## Review checklist

- [ ] Every credential is dedicated to Piperoom and stored in a secret manager, not in the repo or in `owners.yaml`.
- [ ] Postgres and warehouse roles cannot write (`INSERT` as that role fails), and see only pipeline tables.
- [ ] Kafka principal has no write/group-read/alter ACLs, only pipeline-prefixed topics.
- [ ] Connect and Registry are in read-only mode unless you deliberately enabled write mode.
- [ ] Network: Piperoom can reach only the five systems above, the Anthropic API, and its own DB.
- [ ] The room service's HTTP port (`PIPEROOM_HTTP_ADDR`) is not reachable from outside the pilot team's network.
