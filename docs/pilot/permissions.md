# Least-privilege permissions

Piperoom's agent is **read-only by design**. Reads are used for investigation. The only writes
are **six** fix actions, listed under [Kafka Connect](#kafka-connect) and
[Schema Registry](#schema-registry). Each runs only after every required layer owner approves.
Grant exactly what is below and nothing more. Create **dedicated** service accounts for
Piperoom, never reuse a human or application credential, so you can audit and revoke it.

The names below match the code: tools in `internal/tools/{dbtools,kafkatools}`, actions in
`internal/actions`, clients in `internal/clients/*`. Replace placeholders (`<...>`).

## Summary

| System | Used by | Read access needed | Write access needed |
|---|---|---|---|
| Source Postgres | `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`, `pg_row`, `trace_record` | pipeline tables (`SELECT`), `piperoom_audit.ddl_log`, stat views | **none** |
| Warehouse (**Postgres only today**) | `warehouse_freshness`, `warehouse_row`, `trace_record` | `SELECT` on the pipeline's sink tables | **none** |
| Kafka | `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag`, `kafka_find_by_key`, `trace_record` | describe topics/groups, read pipeline topics | **none** |
| Kafka Connect | `connect_list_connectors`, `connect_status`, `connect_config` | REST `GET` | 4 actions: `connect_restart_task`, `connect_restart_connector`, `connect_resume_connector`, `connect_update_connector_config` |
| Schema Registry | `schema_registry_subjects`, `_versions`, `_diff` | REST `GET` | 2 actions: `schema_set_compatibility`, `schema_register` |
| Room database | room service | n/a | full access to its **own** database only (it creates its tables itself) |
| Anthropic API | agent | API key (env var) | n/a |

Every write action is registered in `internal/actions/kafka_actions.go:38-52`. The room service
runs them only after approval, and checks each one against the room's pipeline (see
`internal/room/service_executor.go:211`). If you do not want Piperoom to ever change Connect or
the Registry, give it read-only credentials and proxy rules there (see below). Proposals will
still be created, but executing them will fail. That is a valid, safe pilot mode.

## Source Postgres

Create a read-only role limited to the pipeline's tables. Piperoom also forces
`default_transaction_read_only = on` on its connections as a second layer
(`internal/clients/postgres/postgres.go:31`), but the role must not be able to write in the
first place.

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
  `pg_row` is limited to the pipeline's `source_tables` in code
  (`internal/tools/dbtools/pg.go:177`), but the credential is the real boundary: a bug or a
  changed `owners.yaml` must not widen what the role can read.
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

**Today the warehouse is Postgres only.** The service always opens `WAREHOUSE_DB_URL` with the
Postgres client (`internal/wiring/wiring.go:166`, `internal/clients/warehouse/warehouse.go`).
`internal/clients/snowflake` contains a Snowflake client, but nothing outside its own package
imports it and no setting selects it. A PR to make it selectable is in progress. No date is
promised. Until it lands, the Snowflake grants below are for planning only.

Same principle for either engine: a dedicated `SELECT`-only identity on the sink tables from
`sink_tables` in `owners.yaml`.

**Postgres-compatible warehouse (what works today)**: a role with `CONNECT`, `USAGE` on the
schema and `SELECT` on the sink tables, as in the source Postgres section.

**Snowflake (not selectable yet)**: the client does not enforce read-only; the role in the DSN
does (`internal/clients/snowflake/snowflake.go:1-16`). When the PR lands, create a role with only:

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
records from a topic by key or time. It assigns partitions directly (`internal/clients/kafka/kafka.go`).
It never joins a consumer group, never commits offsets, never produces, never creates or deletes
topics.

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
builds the Kafka, Schema Registry and Connect clients from them (`internal/clients/netsec`).
Basic auth is refused over plain `http://` (`netsec/http.go:53`), and Kafka SASL PLAIN is refused
without TLS (`netsec/kafka.go:105`). This was checked in unit tests with fake servers only, not
against a real secured cluster, so try it on staging first.

## Kafka Connect

Connect's REST API usually has no per-endpoint authorization, so scope it with the network
and, where available, a proxy or the Connect REST extension that your platform provides.
The Piperoom client in `internal/clients/connect/connect.go` makes exactly these calls.

**Read calls (needed in every mode):**

| Method and path | Used by | Code |
|---|---|---|
| `GET /connectors` | `connect_list_connectors`; actions check that a connector exists | `connect.go:50` |
| `GET /connectors/{name}/status` | `connect_status`; actions read state and poll it | `connect.go:72` |
| `GET /connectors/{name}/config` | `connect_config`; the config action reads it before and after its write | `connect.go:93`, `internal/actions/connector_config.go:179,194` |

**Write calls (only for approved fixes, one per action):**

| Action (layer `kafka-connect`) | HTTP call | Code |
|---|---|---|
| `connect_restart_task` | `POST /connectors/{name}/tasks/{id}/restart` | `connect.go:102`; `kafka_actions.go:201` (`restartTask`) |
| `connect_restart_connector` | `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true` | `connect.go:108`; `kafka_actions.go:273` (`restartConnector`) |
| `connect_resume_connector` | `PUT /connectors/{name}/resume`. Validate refuses any state other than PAUSED or STOPPED | `connect.go:112-115`; `kafka_actions.go:372` (`resumeConnector`) |
| `connect_update_connector_config` | `PATCH /connectors/{name}/config` with **one** key and value | `connect.go:119-140`; `connector_config.go:74` (`updateConnectorConfig`) |

Notes on the table:

- Piperoom sends no other write. No create, no delete, no pause, and no `PUT` on config.
- `connect_update_connector_config` may change only these keys and values
  (`connector_config.go:22-28`): `decimal.handling.mode` = `precise|double|string`;
  `time.precision.mode` = `adaptive|adaptive_time_microseconds|connect`;
  `tombstones.on.delete` = `true|false`. `errors.tolerance` is deliberately not allowed
  (`connector_config.go:14-21`). Keys that look like credentials are refused
  (`connector_config.go:47-62`).
- A config `PATCH` needs Kafka Connect 3.5 or later. An older Connect answers 405, which is
  reported as an error. There is no `PUT` fallback (`connect.go:119-140`).
- Connect restarts the connector's tasks when its config changes, so the update is not free.

**Read-only mode (recommended default):** allow only the three `GET` paths above and
`GET /connectors`. Proposals that need a write will then fail, by design.

**Write mode (only to allow approved fixes):** additionally allow exactly these four calls:

- `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true`
- `POST /connectors/{name}/tasks/{id}/restart`
- `PUT /connectors/{name}/resume`
- `PATCH /connectors/{name}/config`

Also restrict the allowed `{name}` values to the pipeline's connectors if your proxy can.
Piperoom already checks every write action against the room's pipeline (the `Scoped` check in
`internal/actions/kafka_actions.go:55-61`, run at proposal time and before execution); this is
defence in depth.

Connector config is also read by the agent. `connect_config` shows only a fixed list of keys, and
credential-like keys as `***` (`internal/tools/kafkatools/connect_tools.go:155-170`). Still prefer
connectors whose secrets come from config providers (`${file:...}` / `${vault:...}`), so no secret
is in the REST output at all.

## Schema Registry

**Read-only:** `GET /subjects`, `GET /subjects/{subject}/versions[/{version}]`,
`GET /schemas/ids/{id}` (decoding Avro records for `kafka_find_by_key` / `trace_record`),
`GET /config/{subject}` and `GET /config` (compatibility). See `internal/clients/schemareg/schemareg.go`.

**Write mode:** additionally two calls, one per action:

- `PUT /config/{subject}`: `schema_set_compatibility` (`schemareg.go:134-135`;
  `kafka_actions.go:466`). It reads the old level first and reports it.
- `POST /subjects/{subject}/versions`: `schema_register` (`schemareg.go:147-148`;
  `kafka_actions.go:542`). It checks that the schema parses as Avro and that the subject exists.

Use Registry ACLs / RBAC where available and restrict writes to the pipeline's subjects
(the topic-name prefix). A relaxed compatibility level or a new schema version affects all
producers and consumers of that subject immediately, so keep write mode off until you trust
the approval process in your team.

## Room database

The room service uses a separate Postgres database (`PIPEROOM_DB_URL`) that Piperoom owns.
At start-up `cmd/server` applies the embedded goose migrations itself (`cmd/server/main.go:108`,
`migrations/migrate.go:20`), so the role in `PIPEROOM_DB_URL` needs rights to
create tables and indexes in that database. The migrations create `rooms`, `room_events`,
`proposals`, `approvals`, `suggestions`, `handoffs` (`00001_init.sql`), `proposal_steps`
(`00002_handoff_and_steps.sql`) and `slack_threads` (`00003_slack_threads.sql`). Goose adds its own
`goose_db_version` table. The role also needs `LISTEN`/`NOTIFY`, which any role has. Give it its
own database and role. That role must not have access to any pipeline data. It holds the
permanent room history, so treat it with the same care as the data in it (see
[data-handling.md](data-handling.md)).

## Anthropic API key

Set `ANTHROPIC_API_KEY` in the process environment from your secret manager. It is never
written to config files, events, or logs by Piperoom. Use a dedicated key for the pilot
with a spend limit, so it can be revoked or capped on its own. If the key is a user key rather
than a workspace key, also set `ANTHROPIC_WORKSPACE_ID` (`internal/agent/model.go:42`).

## Review checklist

- [ ] Every credential is dedicated to Piperoom and stored in a secret manager, not in the repo or in `owners.yaml`.
- [ ] Postgres and warehouse roles cannot write (`INSERT` as that role fails), and see only pipeline tables.
- [ ] Kafka principal has no write/group-read/alter ACLs, only pipeline-prefixed topics.
- [ ] Connect and Registry are in read-only mode unless you deliberately enabled write mode. In write mode the proxy allows only the four Connect calls and two Registry calls listed above.
- [ ] Network: Piperoom can reach only the five systems above, the Anthropic API, its own DB, and, only if Slack is configured, `https://slack.com/api` (`internal/slackbridge/bridge.go:36`).
- [ ] The room service's HTTP port (`PIPEROOM_HTTP_ADDR`) is not reachable from outside the pilot team's network.
