# Setup

Goal: Piperoom running against your pipeline, investigating a real or staged alert, in under
a day. Do this first against a **staging** or replica pipeline.

## 0. Prerequisites

- Go (see `go.mod`) and Node (for the web UI), a Postgres database for Piperoom's own state.
- Network access from the Piperoom host to: source DB, warehouse, Kafka brokers, Connect,
  Schema Registry, the room DB, and `api.anthropic.com`.
- The credentials from [permissions.md](permissions.md).
- Read [README.md](README.md#read-this-first-pilot-limits) for the current limits.

## 1. Configure the environment

All settings are environment variables read by `internal/config/config.go`. Defaults point
at the local dev stack, so **set every one** you rely on.

| Variable | Meaning |
|---|---|
| `PIPEROOM_HTTP_ADDR` | listen address. Default `127.0.0.1:8090` (loopback). Keep it loopback; see below |
| `PIPEROOM_DB_URL` | Piperoom's own database (room events, proposals) |
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
3. Create the Piperoom database and role for its own state (once the room service lands, its schema comes from `migrations/`).

## 4. Start the service

> **Status:** the room service (`cmd/server`, `migrations/`, HTTP API, WebSocket, executor) is
> in a separate PR ([#15](https://github.com/utkarsh199708/piperoom/pull/15)) and was not on
> `main` when this was written. Check `git log -- cmd/server` before relying on this section.
> Until it merges, use step 5.1 (`cmd/investigate`) only. It has been tested with the scripted
> `--demo` agent and fakes, not against a real stack or a real Claude run.

Apply the schema first: `goose -dir migrations postgres "$PIPEROOM_DB_URL" up`.

```sh
go build ./...
go run ./cmd/server --demo   # try it first: scripted agent, in-memory log, no stack or API key
go run ./cmd/server          # real mode: serves the HTTP API and WebSocket on PIPEROOM_HTTP_ADDR
cd web && npm ci && npm run build
```

Verify startup with `curl -H 'X-User: <user id>' http://127.0.0.1:8090/api/me`
(see `docs/api.md`).

### Network exposure

There is no real authentication yet (finding F1). Do not bind `PIPEROOM_HTTP_ADDR` to
`0.0.0.0` or a public interface. Reach it through an SSH tunnel, VPN, or an authenticating
reverse proxy that sets `X-User` itself and strips the header from client requests.

## 5. Verify with a dry run (no real alert needed)

1. Check the agent can read every system without opening a room:
   `go run ./cmd/investigate` with a test alert for your `pipeline_id`
   (see `tasks/A4-agent-loop.md`). Each of `connect_list_connectors`, `kafka_consumer_lag`,
   `pg_replication_slots`, `warehouse_freshness` should return data. A permission error
   here means a missing grant, so fix the grant, not the code.
2. Open a room: `POST /api/rooms` with `{title, severity, pipeline_id}`, open the web UI,
   and watch the agent work.
3. Confirm **read-only**: as the Piperoom DB role run `INSERT` on a pipeline table. It must
   fail. Same for Kafka produce with the Piperoom principal.
4. Confirm the approval gate: have the agent propose a fix and confirm it does not run
   until every required owner has approved. Reject it, and confirm nothing changed.
5. Practise a hand-off between two engineers.

## 6. Wire up alerts

Point your alerting system at `POST /api/alerts/{source}` where source is `alertmanager`,
`grafana`, `pagerduty`, `datadog` or `generic` (`POST /api/alerts/webhook` is the generic form).
Each source is **disabled until its secret is set** in the server's environment:
`PIPEROOM_ALERTMANAGER_TOKEN`, `PIPEROOM_GRAFANA_SECRET`, `PIPEROOM_PAGERDUTY_SECRET`,
`PIPEROOM_DATADOG_TOKEN`, `PIPEROOM_GENERIC_SECRET` (all secrets), and optionally
`PIPEROOM_DEFAULT_PIPELINE`. The signature rules per sender are in `internal/alertin/README.md`.
The same incident sent again opens no second room.
The body is mapped to `title` / `severity` / `pipeline_id`, and the whole body is stored in
the room event log, so do not include secrets in alert payloads. See `docs/design/webhook-auth.md`
(if present; it is the webhook authentication design, C9) and, until it lands, keep this endpoint unreachable from
anywhere but your alerting system.

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
| Proposal cannot execute | write mode not granted on Connect/Registry (by design) |
| No agent response | `ANTHROPIC_API_KEY` missing, or egress to the API is blocked |
