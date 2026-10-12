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
| Source Postgres | `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`, `pg_row`, `pg_publication_tables`, `trace_record` | pipeline tables (`SELECT`), `piperoom_audit.ddl_log`, stat views, catalog views | **none** |
| Warehouse (Postgres, or Snowflake with `WAREHOUSE_KIND=snowflake`) | `warehouse_freshness`, `warehouse_row`, `trace_record`, `pipeline_map` (sink health) | `SELECT` on the pipeline's sink tables | **none** |
| Kafka | `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag`, `kafka_find_by_key`, `kafka_topic_config`, `trace_record` | describe topics/groups, describe topic configs, read pipeline topics | **none** |
| Other tools | `pipeline_map` reads the clients above and no new grant; `runbook_lookup` reads only the runbook text embedded in the binary (`internal/tools/runbooks/runbooks.go:42-43`) | none | none |
| Kafka Connect | `connect_list_connectors`, `connect_status`, `connect_config` | REST `GET` | 4 actions: `connect_restart_task`, `connect_restart_connector`, `connect_resume_connector`, `connect_update_connector_config` |
| Schema Registry | `schema_registry_subjects`, `_versions`, `_diff` | REST `GET` | 2 actions: `schema_set_compatibility`, `schema_register` |
| Room database | room service | n/a | full access to its **own** database only (it creates its tables itself) |
| Model provider | agent | `anthropic`: API key (env var). `bedrock`: AWS identity with invoke rights in `AWS_REGION`. `vertex`: Google identity with Vertex AI access in `VERTEX_PROJECT_ID` | n/a |

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
  (`internal/tools/dbtools/pg.go:243`), but the credential is the real boundary: a bug or a
  changed `owners.yaml` must not widen what the role can read.
- `pg_replication_slots` is readable by any role. No `REPLICATION` attribute is needed or wanted.
- `pg_long_transactions` reads `pg_stat_activity`. The tool never returns query text: the
  client selects only the statement verb (first keyword). Without `pg_read_all_stats` the role
  also does not see other sessions' `query` at all, which is still the **preferred**
  least-privilege state. Do not grant `pg_read_all_stats` or `pg_monitor` without need.
- Primary-key discovery uses `information_schema`, which only lists tables the role can
  access, so the grants above are enough.
- `pg_publication_tables` reads catalog views only (`pg_publication_tables`, `pg_class`,
  `pg_namespace`, and `has_table_privilege` for the connector role) (`internal/clients/postgres/publication.go:85-89,138-153`).
  It adds no table grant. It reports the publication name and the connector's database role
  name, taken from the Connect config (`internal/tools/dbtools/pg_publication.go:30-35`). It also
  reports, per source table, whether that table is in the publication and whether the role can
  SELECT it. Its `Data` lists only the publication members that are the pipeline's source tables
  (a `FOR ALL TABLES` publication has every table as a member). It exposes no row data.
- Never use the Debezium replication user or the application user here.

### DDL audit (optional, recommended)

`pg_recent_ddl` reads `piperoom_audit.ddl_log`, filled by an event trigger. The reference
implementation is `dev/postgres-init/02-ddl-audit.sql`. Installing an event trigger
requires a superuser (or equivalent) **once**, run by your DBA, not by Piperoom. The trigger
function is `SECURITY DEFINER`, so Piperoom's role only needs `SELECT` on the log table.
The log stores the full DDL statement text (`query`), so review that it does not contain
secrets (for example `CREATE ROLE ... PASSWORD`) before enabling it in production.

## Warehouse

**The warehouse is Postgres by default, or Snowflake with `WAREHOUSE_KIND=snowflake`.**
`WAREHOUSE_KIND` accepts `postgres` (the default) or `snowflake`. Any other value stops the start
(`internal/wiring/warehouse.go:50-60`). The choice is made in `internal/wiring/warehouse.go:65-83`,
called from `internal/wiring/wiring.go:174`. With Postgres, `WAREHOUSE_DB_URL` is a Postgres URL and
the client is `internal/clients/warehouse/warehouse.go`. With Snowflake, it is a gosnowflake DSN
(see the Snowflake section below).

Same principle for either engine: a dedicated `SELECT`-only identity on the sink tables from
`sink_tables` in `owners.yaml`.

**Postgres warehouse**: a role with `CONNECT`, `USAGE` on the schema and `SELECT` on the sink
tables, as in the source Postgres section.

**Snowflake (`WAREHOUSE_KIND=snowflake`)**: read-only access comes only from the role in the DSN.
The client does not enforce read-only, and the driver has no session-level read-only switch
(`internal/clients/snowflake/snowflake.go:6-10`). Give that role only the grants below.

- `WAREHOUSE_DB_URL` is a gosnowflake DSN, `user:password@account/database/schema?warehouse=...&role=...`.
  **URL-encode the password** (for example with `url.QueryEscape`). A raw `%`, `+`, `?`, `@`, `/` or
  `:` breaks it (`internal/clients/snowflake/README.md`).
- Startup errors are redacted of the DSN and its password (`internal/wiring/warehouse.go:74`;
  `internal/clients/snowflake/redact.go`). The DSN is never logged.
- The client pings the account when it opens (`internal/clients/snowflake/snowflake.go:45`). A
  wrong DSN stops the start.
- `sink_tables` may be `schema.table` in the DSN's database, or `db.schema.table` for another
  database. Each part must match `^[A-Za-z_][A-Za-z0-9_$]*$` (`internal/clients/snowflake/identifier.go`).
  Names are upper-cased and double-quoted, so `analytics.orders` runs as `"ANALYTICS"."ORDERS"`.
  A table or column created with quotes and a lower or mixed case cannot be reached.
- Column names come back lower-cased (`snowflake.go:148`), so the timestamp rule and the
  `pg_row` / `warehouse_row` output match Postgres ([data-handling.md](data-handling.md#2-what-each-tool-exposes)).
- **Network:** the host running Piperoom needs egress to the Snowflake account named in the DSN, on
  Snowflake's own host name (for example `<account>.snowflakecomputing.com`). Add it to your network
  allow-list and to the checklist below.

Create a role with only:

```sql
CREATE ROLE PIPEROOM_RO;
GRANT USAGE ON WAREHOUSE <wh> TO ROLE PIPEROOM_RO;           -- ideally a small, dedicated warehouse
GRANT USAGE ON DATABASE <db> TO ROLE PIPEROOM_RO;
GRANT USAGE ON SCHEMA <db>.<schema> TO ROLE PIPEROOM_RO;
GRANT USAGE ON DATABASE <other_db> TO ROLE PIPEROOM_RO;          -- only for db.schema.table sinks outside the DSN's database
GRANT USAGE ON SCHEMA <other_db>.<schema> TO ROLE PIPEROOM_RO;   -- same
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
| Topic `<prefix>*` | `DescribeConfigs` | `kafka_topic_config` (retention, cleanup policy, min in-sync replicas, partitions, replication factor). Calls `DescribeTopicConfigs` (`internal/clients/kafka/kafka.go:164-170`; `internal/tools/kafkatools/kafka_tools.go:297-328`) |
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
| `GET /connectors/{name}/config` | `connect_config`; the config action reads it before and after its write | `connect.go:93`, `internal/actions/connector_config.go:182,197` |

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

**Read-only mode (recommended default):** allow only these three `GET` calls:
`GET /connectors`, `GET /connectors/{name}/status` and `GET /connectors/{name}/config`.
Proposals that need a write will then fail, by design.

**Write mode (only to allow approved fixes):** additionally allow exactly these four calls:

- `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true`
- `POST /connectors/{name}/tasks/{id}/restart`
- `PUT /connectors/{name}/resume`
- `PATCH /connectors/{name}/config`

Also restrict the allowed `{name}` values to the pipeline's connectors if your proxy can.
Piperoom already checks every write action against the room's pipeline (the `Scoped` check in
`internal/actions/kafka_actions.go:55-61`, run at proposal time and before execution); this is
defence in depth.

Connector config is also read by the agent, and only for the room pipeline's own connectors.
`connect_config` shows a fixed list of key names, the `transforms` name list and each transform's
`type`, `field`, `fields` and `field.name`, and shows secret-looking keys and every other
`transforms*` setting as `***`, so the presence of a secret key is shown, never its value
(`internal/tools/kafkatools/connect_tools.go`). Still put secrets in connector config providers
(`${file:...}` / `${vault:...}`), not in `transforms*` settings, so no secret is in the REST output
at all.

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
At start-up `cmd/server` applies the embedded goose migrations itself (`cmd/server/main.go:137`,
`migrations/migrate.go:20`), so the role in `PIPEROOM_DB_URL` needs rights to
create tables and indexes in that database. The migrations create `rooms`, `room_events`,
`proposals`, `approvals`, `suggestions`, `handoffs` (`00001_init.sql`), `proposal_steps`
(`00002_handoff_and_steps.sql`) and `slack_threads` (`00003_slack_threads.sql`). Goose adds its own
`goose_db_version` table. The role also needs `LISTEN`/`NOTIFY`, which any role has. Give it its
own database and role. That role must not have access to any pipeline data. It holds the
permanent room history, so treat it with the same care as the data in it (see
[data-handling.md](data-handling.md)).

## Anthropic API key

Set `ANTHROPIC_API_KEY` in the process environment from your secret manager. Piperoom does not
write it to config files, events or logs. The Anthropic SDK reads it, and if it is unset the SDK
falls back to `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_PROFILE` and other sources
(`anthropic-sdk-go@v1.80.0/client.go:30-82`). Use a dedicated key for the pilot
with a spend limit, so it can be revoked or capped on its own. If the key is a user key rather
than a workspace key, also set `ANTHROPIC_WORKSPACE_ID` (`internal/agent/model.go:42`).

**Where the prompts go.** The SDK sends every request to `ANTHROPIC_BASE_URL` when that variable is
set (`anthropic-sdk-go@v1.80.0/client.go:52-58`). Prompts include the alert, human comments, proposal results and the
hand-off digest (see [data-handling.md](data-handling.md#what-reaches-the-model)). Leave
`ANTHROPIC_BASE_URL` unset, or pin it to the Anthropic host you approved. Leave
`ANTHROPIC_AUTH_TOKEN` and `ANTHROPIC_PROFILE` unset on the Piperoom host.

## Model provider credentials (`PIPEROOM_MODEL_PROVIDER`)

Pick one provider. The default is `anthropic` (the section above).

- **`bedrock`**: set `AWS_REGION`. Give the process an AWS identity that may invoke the model in
  that region (Bedrock invoke permissions on the model or inference profile). Piperoom uses the
  standard AWS chain: environment variables, shared config, or an instance or task role. An
  `AWS_BEARER_TOKEN_BEDROCK` key may be used instead. Credentials are checked once at start-up
  (`internal/agent/endpoint.go:138-165`). Requests go to `bedrock-runtime.<AWS_REGION>.amazonaws.com`.
- **`vertex`**: set `VERTEX_PROJECT_ID` and `VERTEX_REGION`. Give the process a Google identity
  (application default credentials, for example a service account) with access to Vertex AI in that
  project. Requests go to the Vertex AI endpoint for `VERTEX_REGION` (`anthropic-sdk-go@v1.80.0/vertex/vertex.go:96-106`).
- **Either way**, the provider's key or identity is the credential to protect. The Anthropic key
  section above does not apply, and `ANTHROPIC_BASE_URL` and `ANTHROPIC_API_KEY` do not reach these
  clients (`anthropic-sdk-go@v1.80.0/bedrock/bedrock.go:252`, `anthropic-sdk-go@v1.80.0/vertex/vertex.go:108`).

## Review checklist

- [ ] Every credential is dedicated to Piperoom and stored in a secret manager, not in the repo or in `owners.yaml`.
- [ ] Postgres and warehouse roles cannot write (`INSERT` as that role fails), and see only pipeline tables.
- [ ] Kafka principal has no write/group-read/alter ACLs, only pipeline-prefixed topics.
- [ ] Connect and Registry are in read-only mode unless you deliberately enabled write mode. In write mode the proxy allows only the four Connect calls and two Registry calls listed above.
- [ ] Network: Piperoom can reach only the five systems above, the Snowflake account host (only if `WAREHOUSE_KIND=snowflake`), the model provider's endpoint (the Anthropic API host or the pinned `ANTHROPIC_BASE_URL` host for `anthropic`; the Bedrock runtime in `AWS_REGION` for `bedrock`; the Vertex AI endpoint in `VERTEX_REGION` for `vertex`), its own DB, and, only if Slack is configured, `https://slack.com/api` (`internal/slackbridge/bridge.go:36`).
- [ ] If `WAREHOUSE_KIND=snowflake`: the DSN password is URL-encoded, the role in the DSN has only `USAGE` and `SELECT` as listed above, and the resource monitor is set.
- [ ] `ANTHROPIC_BASE_URL` is unset or pinned, and `ANTHROPIC_AUTH_TOKEN` and `ANTHROPIC_PROFILE` are unset on the host (`anthropic-sdk-go@v1.80.0/client.go:52-82`).
- [ ] The room service's HTTP port (`PIPEROOM_HTTP_ADDR`) is not reachable from outside the pilot team's network.
