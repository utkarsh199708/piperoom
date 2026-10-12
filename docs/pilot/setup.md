# Setup

Goal: Piperoom running against your pipeline, investigating a real or staged alert, in under
a day. Do this first against a **staging** or replica pipeline.

## 0. Prerequisites

- Go (see `go.mod`) and Node (for the web UI), a Postgres database for Piperoom's own state.
- Network access from the Piperoom host to: source DB, warehouse, Kafka brokers, Connect,
  Schema Registry, `api.anthropic.com`, and the room DB.
- The credentials from [permissions.md](permissions.md).
- Read [README.md](README.md#read-this-first-pilot-limits) for the current limits.

## 1. Configure the environment

All settings are environment variables read by `internal/config/config.go`. Defaults point
at the local dev stack, so **set every one** you rely on.

| Variable | Meaning |
|---|---|
| `PIPEROOM_HTTP_ADDR` | listen address of the room service. Default `127.0.0.1:8090` (loopback). `cmd/server` refuses a non-loopback address unless started with `--allow-remote`, see below |
| `PIPEROOM_DB_URL` | Piperoom's own database for the room service: room events, proposals. Migrated automatically at start-up |
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
3. Create an empty Postgres database and a role for Piperoom's own state, and put its URL in
   `PIPEROOM_DB_URL`. You do not run migrations by hand: `cmd/server` applies them at start-up
   (see step 4 and [permissions.md](permissions.md#room-database)).

## 4. Start the service

`cmd/server` is the room service: HTTP API, WebSocket, agent supervisor and the executor for
approved fixes. It takes two flags, `--demo` and `--allow-remote` (see above), and reads everything
else from the environment above.

**Real mode** (no flag) does this, in order (`cmd/server/main.go`):
1. Loads `USERS_FILE` (refuses to start if it is missing or unreadable, `:64-68`) and
   `OWNERS_FILE` (`:76-79`).
2. Cross-checks the two (`room.ValidateUsers`, `internal/room/users.go:70-110`) and refuses to
   start on any disagreement, in **both** directions (`:80-82`): every layer in `owners.yaml`
   must name a user who exists in `users.yaml` and lists that layer, and every layer a user
   lists must be assigned to that same user in `owners.yaml`. A layer name that is not one of
   the six known layers also stops the start. Fix the files; the error names the user and layer.
3. Applies the goose migrations to `PIPEROOM_DB_URL` (`:92`, `migrations/migrate.go:20`;
   30 s timeout, `:220`), then opens the store (30 s timeout, `:97`). A database that cannot
   be reached stops the start.
4. Opens the data stack (`wiring.Open`, `:126`: source Postgres, warehouse, Kafka, Connect,
   Schema Registry with the TLS/SASL/auth settings below) and registers the real tools and
   write actions (`:132`). This needs the credentials from [permissions.md](permissions.md)
   and `ANTHROPIC_API_KEY` (and `ANTHROPIC_WORKSPACE_ID` if your key is a user key, see
   `internal/agent/model.go:42`).
5. Reloads every room from the log (`:156`; a failure stops the start) and listens on
   `PIPEROOM_HTTP_ADDR` (`:172`).

```sh
export PIPEROOM_DB_URL=... SOURCE_DB_URL=... WAREHOUSE_DB_URL=... KAFKA_BROKERS=... \
       CONNECT_URL=... SCHEMA_REGISTRY_URL=... OWNERS_FILE=... USERS_FILE=... ANTHROPIC_API_KEY=...
go run ./cmd/server
```

**`--demo`** (`go run ./cmd/server --demo`) needs no database, stack or API key: in-memory
store, a scripted agent and a fake topology, but the **real** write actions over fake clients,
so the propose / approve / execute path is the real one (`cmd/server/main.go:1-8,114-124`).
It still loads `OWNERS_FILE` and cross-checks it against the users (falling back to built-in
demo users if `USERS_FILE` is missing). Use it to try the UI and the approval flow safely.
Nothing in it touches your systems, but do not use it for a real incident.

**The web UI** is not served by `cmd/server`. Run it separately: `cd web && npm ci && npm run dev`
serves it on `localhost:5173` (`web/package.json:7`) and it calls `http://localhost:8090` by
default (`web/src/api/client.ts:12`, override with `VITE_API_BASE`). The server only allows
CORS and WebSocket origins `http://localhost:5173` / `localhost:5173` / `127.0.0.1:5173`
(`internal/httpapi/server.go:152`, `internal/ws/gateway.go:180`), so a UI served from any other
origin is blocked until a reverse proxy puts both under one origin.

Verify startup: `curl -H 'X-User: <user id>' http://127.0.0.1:8090/api/me` returns that user,
and the same call with an unknown id returns `401` (`internal/httpapi/server.go:131-137`).

### Routes you need

All under `/api`, all JSON, and all (except the WebSocket) require `X-User`
(`internal/httpapi/server.go:63-96`; the full contract is `docs/api.md`):

| Purpose | Route |
|---|---|
| who am I | `GET /api/me` |
| open a room by hand | `POST /api/rooms` `{title, severity, pipeline_id}` |
| open a room from an alert | `POST /api/alerts/webhook` (see step 6) |
| list / read rooms | `GET /api/rooms`, `GET /api/rooms/{id}`, `GET /api/rooms/{id}/events?after=&limit=` |
| live view | `GET /api/rooms/{id}/ws?user=<id>&after=<seq>` (WebSocket) |
| talk to the room | `POST /api/rooms/{id}/comments`, `/steer`, `/suggestions/{sid}/send`, `/control/request`, `/control/grant`, `/close`, `/trace` |
| fixes | `POST /api/proposals/{pid}/approve`, `/reject` |
| shift change | `POST /api/rooms/{id}/handoffs`, `POST /api/handoffs/{hid}/accept` |
| pipeline map | `GET /api/pipelines/{pid}/topology` |

### What happens on a restart

On start-up the service rebuilds every room from the event log and **starts a new agent for
each room that is still open** (`internal/room/service.go:164-194`). That agent starts again
from the room's alert, not from where the old one stopped (`:190`, `:603`). If an agent run
fails it is run again, up to 3 runs in total with a growing pause, and each failure posts an
`agent.status` `error` event (`:588-589,619-634`).

A fix that was fully approved but had not finished when the server stopped is **not re-run**.
It is marked failed: the service writes one `action.executed` event with `ok=false` and the
text "proposal was interrupted by a service restart before it finished; create a new proposal
to retry" and closes the proposal (`internal/room/service.go:196-229`). Check the target
system yourself to see how far the interrupted step got before asking for a new proposal.

### Network exposure

There is no real authentication yet (finding F1; `docs/api.md` §Auth identifies users by the
`X-User` header). Keep `PIPEROOM_HTTP_ADDR` on its loopback default: `cmd/server` refuses to start on a
non-loopback address unless you pass `--allow-remote` (it then logs a warning). Reach it
through an SSH tunnel, VPN, or an authenticating reverse proxy that sets `X-User` itself and
strips the header from client requests.

## 5. Verify with a dry run (no real alert needed)

1. Check the agent can read every system without opening a room:
   `go run ./cmd/investigate` with a test alert for your `pipeline_id`
   (see `tasks/A4-agent-loop.md`). Each of `connect_list_connectors`, `kafka_consumer_lag`,
   `pg_replication_slots`, `warehouse_freshness` should return data. A permission error
   here means a missing grant, so fix the grant, not the code.
2. Start `cmd/server --demo` first to see the flow with no risk, then real mode. Open a room
   (`POST /api/rooms`), open the web UI, and watch the agent work.
3. Confirm **read-only**: as the Piperoom DB role run `INSERT` on a pipeline table. It must
   fail. Same for Kafka produce with the Piperoom principal.
4. Confirm the approval gate: have the agent propose a fix and confirm it does not run
   until every required owner has approved. Reject it, and confirm nothing changed.
5. Practise a hand-off between two engineers.

## 6. Wire up alerts

Point your alerting system at `POST /api/alerts/webhook` (`internal/httpapi/server.go:81`,
handler `internal/httpapi/handlers.go:44-71`). What it does today:

- It needs an `X-User: <user id>` header of a user in `users.yaml`, like every other route.
  **It does not verify the sender**: there is no signature or token check, and the
  `internal/alertin` package that implements those checks (Alertmanager, Grafana, PagerDuty,
  Datadog, generic; `internal/alertin/README.md`) is not used by the server. Treat the
  endpoint as open to anyone who can reach it and set that header, and keep it reachable only
  from your alerting system. The real design is still C9.
- The body must be valid JSON up to 1 MB. It reads `title`, `severity` and `pipeline_id` if the
  body is an object; anything else maps nothing, and the whole body is stored unchanged in the
  room's `room.created` event (`handlers.go:48-65`, `internal/room/service_room.go:116-118`),
  so do not put secrets in alert payloads.
- Defaults: no title becomes "New alert", an unknown severity becomes `sev3`, and a missing
  `pipeline_id` becomes `unknown` (`service_room.go:89-97`). A `pipeline_id` with characters
  other than letters, digits, `_`, `.`, `-` is rejected with 400 (`:98-100`).
- **A missing or unknown pipeline still opens a room, with proposals disabled.** The room gets
  a system message "pipeline ... is unknown; proposals are disabled for this room"
  (`service_room.go:126-132`) and any proposal the agent tries is refused
  (`internal/room/service_proposals.go:51-54`). The agent is still started
  (`service_room.go:135`), but its tools are scoped to a pipeline, so expect it to find little.
- The response is `201` with the room snapshot. The room has **no driver** until someone takes
  control (`POST /api/rooms/{id}/control/grant`).
- **No de-duplication:** the same alert sent twice opens two rooms (`service_room.go:102`).
  Alertmanager's `repeat_interval` will therefore open a new room each time; set it long, or
  group alerts so one firing sends one request.

## 6b. Not wired yet

- **Slack:** `internal/slackbridge` exists but `cmd/server` does not use it, so the room is not
  mirrored to Slack.
- **Real login:** see the README. Until C9 is built, everyone who can reach the API can act as
  any user.

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
| Server exits at start with "users/owners config" | `users.yaml` and `owners.yaml` disagree about a layer owner (step 4). The message names the user and layer |
| Server exits at start with "ping for migrations" | `PIPEROOM_DB_URL` cannot be reached, or the role lacks rights to create tables |
| Web UI shows CORS or WebSocket errors | the UI is not on `localhost:5173` (step 4) |
| A fix shows "interrupted by a service restart" | the server restarted mid-fix; check the system, then re-propose |
| No agent response | `ANTHROPIC_API_KEY` missing, or egress to the API is blocked |
