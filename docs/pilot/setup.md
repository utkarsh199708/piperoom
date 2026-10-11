# Setup

Goal: Piperoom (today: the `cmd/investigate` command; the room service is not on `main` yet) running against your pipeline, investigating a real or staged alert, in under
a day. Do this first against a **staging** or replica pipeline.

## 0. Prerequisites

- Go (see `go.mod`) and Node (for the web UI), a Postgres database for Piperoom's own state.
- Network access from the Piperoom host to: source DB, warehouse, Kafka brokers, Connect,
  Schema Registry, `api.anthropic.com`, and (once the room service exists) the room DB.
- The credentials from [permissions.md](permissions.md).
- Read [README.md](README.md#read-this-first-pilot-limits) for the current limits.

## 1. Configure the environment

All settings are environment variables read by `internal/config/config.go`. Defaults point
at the local dev stack, so **set every one** you rely on.

| Variable | Meaning |
|---|---|
| `PIPEROOM_HTTP_ADDR` | listen address of the room service (not on `main` yet). Default `127.0.0.1:8090` (loopback). Keep it loopback; see below |
| `PIPEROOM_DB_URL` | Piperoom's own database for the room service (not on `main` yet): room events, proposals |
| `SOURCE_DB_URL` | source Postgres, the **read-only** role |
| `WAREHOUSE_DB_URL` | warehouse, the **read-only** role |
| `KAFKA_BROKERS` | comma-separated brokers |
| `CONNECT_URL` | Kafka Connect REST base URL |
| `SCHEMA_REGISTRY_URL` | Schema Registry base URL |
| `OWNERS_FILE` | pipelines and layer owners (step 2) |
| `USERS_FILE` | users and the layers each owns (step 2) |
| `PIPEROOM_MODEL` | model id, default `claude-opus-5-5` |
| `PIPEROOM_EFFORT` | reasoning effort, default `high` |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME` | Kafka TLS/SASL (empty = plaintext) |
| `KAFKA_SASL_PASSWORD` | **secret** |
| `KAFKA_TLS_CA_FILE`, `KAFKA_TLS_CERT_FILE`, `KAFKA_TLS_KEY_FILE`, `KAFKA_TLS_SERVER_NAME` | Kafka TLS files (the key file is a secret; mount read-only) |
| `SCHEMA_REGISTRY_USERNAME`, `SCHEMA_REGISTRY_TLS_CA_FILE`, `SCHEMA_REGISTRY_TLS_CERT_FILE` | Registry basic auth / TLS |
| `SCHEMA_REGISTRY_PASSWORD`, `SCHEMA_REGISTRY_TLS_KEY_FILE` | **secrets** |
| `CONNECT_USERNAME`, `CONNECT_TLS_CA_FILE`, `CONNECT_TLS_CERT_FILE` | Connect basic auth / TLS |
| `CONNECT_PASSWORD`, `CONNECT_TLS_KEY_FILE` | **secrets** |
| `ANTHROPIC_API_KEY` | read by the SDK from the environment; never stored in config |

The TLS/SASL/auth variables are read by `config.Load()` and applied by `internal/wiring`. A
wrong combination (for example a password with an `http://` URL) fails at startup with an
error that names the setting, never its value. Unit-tested with fakes; not yet tried against a
real secured cluster.

Put secrets in your secret manager and inject them into the process environment. Do not
commit them, and do not put DSNs with passwords in shell history or tickets. Use
`sslmode=require` (or stricter) for remote Postgres, not the dev default `sslmode=disable`.

## 2. Describe your pipeline: `owners.yaml` and `users.yaml`

Start from `dev/owners.yaml` and `dev/users.yaml`.

```yaml
# owners.yaml
pipelines:
  - id: orders                       # used as pipeline_id in rooms/alerts
    title: Orders CDC
    source_tables: [public.orders]   # the ONLY tables pg_row/trace may read at the source
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]  # the ONLY tables warehouse_row may read
    sink_ts_column: updated_at       # used for freshness
layers:                              # the default owner of each layer
  postgres: <user id>
  kafka-connect: <user id>
  schema-registry: <user id>
  kafka: <user id>
  consumer: <user id>
  warehouse: <user id>
```

```yaml
# users.yaml
users:
  - id: <user id>
    name: <display name>
    layers: [kafka-connect, kafka, schema-registry]   # layers this person may approve
```

Rules of thumb:

- Every layer a fix can touch must have an owner who is actually on call and reachable.
  A fix needs **all** of its required layers' owners to approve.
- `source_tables` and `sink_tables` are a security boundary for the row tools. List only
  tables the agent legitimately needs, and keep `SELECT` grants identical to these lists.
- Put the real people in `users.yaml`; remove the dev users.

## 3. Prepare the source database

1. Create the read-only role and grants ([permissions.md](permissions.md#source-postgres)).
2. (Optional) have your DBA install the DDL audit trigger (`dev/postgres-init/02-ddl-audit.sql`)
   so the agent can see recent schema changes.
3. The Piperoom database for its own state is only needed by the room service (not on `main`
   yet, A5 in review). Its schema and migration command will be documented when it lands.

## 4. Start the service

> **Room service: not on `main` yet (A5, in review).** There is no `cmd/server`, HTTP API,
> event store or migrations on `main`, so there is nothing to start in this step today. Use
> step 5.1 (`cmd/investigate`) to check your credentials and grants. Server startup, flags and
> the database migration will be documented here once A5 merges.

### Network exposure (applies once the room service exists)

There is no real authentication yet (finding F1; `docs/api.md` §Auth identifies users by the
`X-User` header). Keep `PIPEROOM_HTTP_ADDR` on its loopback default; do not bind it to
`0.0.0.0` or a public interface. Reach it through an SSH tunnel, VPN, or an authenticating
reverse proxy that sets `X-User` itself and strips the header from client requests.

## 5. Verify with a dry run (no real alert needed)

1. Check the agent can read every system without opening a room:
   `go run ./cmd/investigate` with a test alert for your `pipeline_id`
   (see `tasks/A4-agent-loop.md`). Each of `connect_list_connectors`, `kafka_consumer_lag`,
   `pg_replication_slots`, `warehouse_freshness` should return data. A permission error
   here means a missing grant, so fix the grant, not the code.
2. *Needs the room service (not on `main` yet):* open a room, open the web UI, and watch the
   agent work.
3. Confirm **read-only**: as the Piperoom DB role run `INSERT` on a pipeline table. It must
   fail. Same for Kafka produce with the Piperoom principal.
4. *Needs the room service:* confirm the approval gate: have the agent propose a fix and confirm it does not run
   until every required owner has approved. Reject it, and confirm nothing changed.
5. *Needs the room service:* practise a hand-off between two engineers.

## 6. Wire up alerts

> **Room service: not on `main` yet (A5, in review).** `internal/alertin` on `main` verifies
> and parses alert webhooks (Alertmanager, Grafana, PagerDuty, Datadog, generic), but it has no
> HTTP server: the endpoint that receives them belongs to the room service. The URL path, the
> environment variables that hold each sender's secret, and the loopback rule are therefore not
> final and will be documented when A5 merges.

What you can do now: read `internal/alertin/README.md` for how each sender must sign or
authenticate its requests, and plan to keep the secrets in your secret manager. The whole
alert body is stored in the room event log (`docs/api.md`), so do not put secrets in alert
payloads.

## 7. Run the pilot

- Start in **read-only mode** for Connect and Registry (no write grants). Let proposals show
  what the agent would do; owners execute fixes themselves.
- After a few incidents, enable write mode for one layer at a time.
- Keep a list of every wrong or unhelpful agent answer and send it to the Piperoom team.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `pg_recent_ddl` says the audit log is not installed | trigger not installed, or role lacks `USAGE`/`SELECT` on `piperoom_audit` |
| `kafka_consumer_lag` permission error | missing `Describe` on that consumer group |
| `pg_row` / `warehouse_row` "table is not in pipeline" | table is missing from `source_tables` / `sink_tables` |
| `connect_config` shows `***` | expected: secret-looking keys are redacted |
| Proposal cannot execute (room service, not on `main` yet) | write mode not granted on Connect/Registry (by design) |
| No agent response | `ANTHROPIC_API_KEY` missing, or egress to the API is blocked |
