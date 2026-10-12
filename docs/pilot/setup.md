# Setup

Goal: Piperoom running against a **staging** or replica pipeline, investigating a real or staged
alert. Do this first against staging, not production.

Nothing in this repo measures setup time. The slow part is usually the credential and network
requests in [permissions.md](permissions.md), and naming a reachable owner for each layer. Plan
for those before you plan the install.

## 0. Prerequisites

- **Go 1.27** to build `cmd/server` (`go.mod:3`: `go 1.27.0`).
- **Node.js** to build and serve the web UI in `web/` (`npm ci`). `web/package.json` pins no
  version. The dev machine used Node 24 (`AGENTS.md` §3). You can skip Node if you use only the API.
- **A Postgres database for Piperoom's own state**, separate from the source and the warehouse.
  Its URL is `PIPEROOM_DB_URL`. Use a role that owns only this database. The service creates its own
  tables at start-up (`cmd/server/main.go:108`), so the role needs rights to create tables in it.
- **Network access** from the Piperoom host to: source Postgres, warehouse Postgres, Kafka brokers,
  Kafka Connect REST, Schema Registry, the room database, the Anthropic API host (`api.anthropic.com`
  unless you pin `ANTHROPIC_BASE_URL`, section 1), and `slack.com` only if you enable Slack.
- **Settings for real mode:** `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`, `PIPEROOM_DB_URL`,
  `KAFKA_BROKERS`, `CONNECT_URL`, `SCHEMA_REGISTRY_URL`, `OWNERS_FILE`, `USERS_FILE` and
  `ANTHROPIC_API_KEY` (section 1). Unset, each one falls back to the localhost dev stack
  (`internal/config/config.go:57-67`), so set every one you rely on.
- **Start-up checks are partial.** The source and warehouse Postgres are pinged when opened
  (`internal/clients/postgres/postgres.go:43`, `internal/clients/warehouse/warehouse.go:44`). Connect
  and Schema Registry are not contacted at start-up, so a wrong URL shows up on the first tool or
  action that uses it.
- **The credentials** from [permissions.md](permissions.md). One credential set per process (F7).
- Read [README.md](README.md#read-this-first-pilot-limits) for the current limits.

## 1. Configure the environment

All settings are environment variables read by `internal/config/config.go` (`config.Load()`,
`config.go:9-52` for the fields and `:57-92` for the defaults).

| Variable | Meaning |
|---|---|
| `PIPEROOM_HTTP_ADDR` | listen address of the room service. Default `127.0.0.1:8090`. Loopback is all of `127.0.0.0/8`, `::1`, and `localhost` in any letter case. Any other host, including an empty host or `0.0.0.0`, is refused unless you pass `--allow-remote` (`cmd/server/listen.go:19-41`) |
| `PIPEROOM_DB_URL` | Piperoom's own database: room events, proposals, Slack thread state. Use a role that owns only this database. Migrated automatically at start-up |
| `SOURCE_DB_URL` | source Postgres, the **read-only** role |
| `WAREHOUSE_DB_URL` | warehouse, the **read-only** role. Postgres only today ([permissions.md](permissions.md#warehouse)) |
| `KAFKA_BROKERS` | comma-separated brokers |
| `CONNECT_URL` | Kafka Connect REST base URL |
| `SCHEMA_REGISTRY_URL` | Schema Registry base URL |
| `OWNERS_FILE` | pipelines and layer owners (step 2) |
| `USERS_FILE` | users and the layers each owns (step 2) |
| `PIPEROOM_MODEL` | model id, default `claude-opus-5-5` (`config.go:66`) |
| `PIPEROOM_EFFORT` | reasoning effort, default `high` (`config.go:67`) |
| `SLACK_BOT_TOKEN` | optional, **secret**. Bot token with `chat:write`. Slack is on only when this and `SLACK_CHANNEL_ID` are both set. One without the other stops the start (`cmd/server/main.go:128-132`; `internal/slackbridge/bridge.go:63-69`) |
| `SLACK_CHANNEL_ID` | optional. A channel **ID** such as `C0123ABC`, never `#name` (`bridge.go:70-71`). Use a private channel |
| `PIPEROOM_PUBLIC_URL` | optional. Base URL of the web UI, used for room links in Slack. Must be an absolute `http(s)` URL (`bridge.go:73-79`). Empty means no links (`config.go:51`) |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME` | Kafka TLS/SASL (empty = plaintext) |
| `KAFKA_SASL_PASSWORD` | **secret** |
| `KAFKA_TLS_CA_FILE`, `KAFKA_TLS_CERT_FILE`, `KAFKA_TLS_KEY_FILE`, `KAFKA_TLS_SERVER_NAME` | Kafka TLS files (the key file is a secret; mount read-only) |
| `SCHEMA_REGISTRY_USERNAME`, `SCHEMA_REGISTRY_TLS_CA_FILE`, `SCHEMA_REGISTRY_TLS_CERT_FILE` | Registry basic auth / TLS |
| `SCHEMA_REGISTRY_PASSWORD`, `SCHEMA_REGISTRY_TLS_KEY_FILE` | **secrets** |
| `CONNECT_USERNAME`, `CONNECT_TLS_CA_FILE`, `CONNECT_TLS_CERT_FILE` | Connect basic auth / TLS |
| `CONNECT_PASSWORD`, `CONNECT_TLS_KEY_FILE` | **secrets** |
| `ANTHROPIC_API_KEY` | read by the Anthropic SDK, not by Piperoom. Piperoom does not write it to config files, events or logs. If it is unset, the SDK tries `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_PROFILE` and other sources (`anthropic-sdk-go@v1.80.0/client.go:30-82`) |
| `ANTHROPIC_BASE_URL` | **read by the SDK** (`client.go:58`). Every prompt goes to this host. Leave it unset, or pin it to the Anthropic host you approved |
| `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_PROFILE` | read by the SDK when `ANTHROPIC_API_KEY` is not set. Leave them unset on the Piperoom host, so the key used is the one you chose |
| `ANTHROPIC_WORKSPACE_ID` | only if the key is a user key (`internal/agent/model.go:42`) |

The client code refuses unsafe combinations at start-up: basic auth over `http://` and TLS files
with an `http://` URL (`internal/clients/netsec/http.go:53-56`), and SASL PLAIN without TLS
(`internal/clients/netsec/kafka.go:105`). The TLS, SASL and auth paths are checked with unit tests
and fake servers only. They have not been tried against a real secured cluster, so test them on
staging first.

Put secrets in your secret manager and inject them into the process environment. Do not
commit them, and do not put DSNs with passwords in shell history or tickets. Use
`sslmode=require` (or stricter) for remote Postgres, not the dev default `sslmode=disable`.

## 2. Describe your pipeline: `owners.yaml` and `users.yaml`

Start from `dev/owners.yaml` and `dev/users.yaml`. The loader refuses a pipeline that has no
`topic_prefix`, no `source_tables` or no `connectors`, and refuses `sink_tables` without
`sink_ts_column` (`internal/owners/owners.go:84-93`).

```yaml
# owners.yaml  (shape copied from dev/owners.yaml)
pipelines:
  - id: orders                       # used as pipeline_id in rooms and alerts
    title: Orders CDC
    topic_prefix: pg                 # required: Debezium topic.prefix of the connector
    source_tables: [public.orders]   # the ONLY tables pg_row and trace may read at the source
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]  # the ONLY tables warehouse_row may read
    sink_ts_column: updated_at       # required when sink_tables is set
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
  A fix needs **all** of its required layers' owners to approve. The six layers are
  `postgres`, `kafka-connect`, `schema-registry`, `kafka`, `consumer` and `warehouse`
  (`internal/events/events.go:268`).
- `source_tables` and `sink_tables` bound the row tools, but only for the pipeline the agent names.
  The room's pipeline is not checked, so any pipeline in this file can be read. Keep the file to
  the pipelines the room may see, and keep `SELECT` grants identical to these lists
  ([data-handling.md](data-handling.md#3-prompt-injection-and-untrusted-text), F4).
- Put the real people in `users.yaml`; remove the dev users.

## 3. Prepare the source database

1. Create the read-only role and grants ([permissions.md](permissions.md#source-postgres)).
2. (Optional) have your DBA install the DDL audit trigger (`dev/postgres-init/02-ddl-audit.sql`)
   so the agent can see recent schema changes.
3. Create an empty Postgres database and a role for Piperoom's own state, and put its URL in
   `PIPEROOM_DB_URL`. You do not run migrations by hand: `cmd/server` applies them at start-up
   (see step 4 and [permissions.md](permissions.md#room-database)).

## 4. Start the service

`cmd/server` is the room service: HTTP API, WebSocket, agent runner, the Slack mirror (if
configured) and the executor for approved fixes. It takes two flags, `--demo` and
`--allow-remote` (see [Network exposure](#network-exposure)), and reads everything else from the
environment above.

**Real mode** (no flag) does this, in order (`cmd/server/main.go`):

1. Loads `USERS_FILE` and `OWNERS_FILE` (`:76-91`). A missing or unreadable file stops the start.
2. Cross-checks the two (`room.ValidateUsers`, `internal/room/users.go:70`, called at `:92`).
   Any disagreement stops the start, in **both** directions: every layer in `owners.yaml` must name
   a user who exists in `users.yaml` and lists that layer, and every layer a user lists must be
   assigned to that same user in `owners.yaml`. A layer name that is not one of the six known
   layers also stops the start. The error names the user and the layer.
3. Applies the goose migrations to `PIPEROOM_DB_URL` (`:108`, `migrations/migrate.go`), then opens
   the room store (`:114`). A database that cannot be reached stops the start.
4. Starts the Slack mirror if `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` are set (`:128`). A wrong
   Slack setting stops the start. Mirrors then run for every open room (`:198`).
5. Opens the data stack (`wiring.Open`, `:153`): source Postgres, warehouse, Kafka, Connect and
   Schema Registry, with the TLS/SASL/auth settings above. Source and warehouse are pinged here.
   It then registers the read-only tools and the six write actions (`wiring.RegisterTools`, `:159`;
   `internal/wiring/wiring.go:190-210`). This needs the credentials from
   [permissions.md](permissions.md) and `ANTHROPIC_API_KEY`.
6. Reloads every room from the log (`:183`). A failure stops the start in real mode, but one room
   that cannot be rebuilt is only logged and skipped (`internal/room/service.go:201-204`). Then it
   listens on `PIPEROOM_HTTP_ADDR` (`:229`).

```sh
export PIPEROOM_DB_URL=... SOURCE_DB_URL=... WAREHOUSE_DB_URL=... KAFKA_BROKERS=... \
       CONNECT_URL=... SCHEMA_REGISTRY_URL=... OWNERS_FILE=... USERS_FILE=... ANTHROPIC_API_KEY=...
go run ./cmd/server
```

**`--demo`** (`go run ./cmd/server --demo`) needs no database, stack or API key. It uses an
in-memory store, a scripted agent and a fake topology. It runs the **real** write actions over
fake clients, so the propose / approve / execute path is the real one (`cmd/server/main.go:1-8,105-151`).
It still loads `OWNERS_FILE` and cross-checks it against the users (falling back to built-in
demo users if `USERS_FILE` is missing). Use it to try the UI and the approval flow safely.
Nothing in it touches your systems. **But demo mode does post to the real Slack channel if the
`SLACK_*` variables are set**, because the Slack mirror starts in both modes (`main.go:125-132`).
Unset them for a demo, or use a throwaway channel.

**The web UI** is not served by `cmd/server`. Run it separately: `cd web && npm ci && npm run dev`
serves it on `localhost:5173` (`web/package.json:7`). It calls `http://localhost:8090` by default
(`web/src/api/client.ts:12`, override with `VITE_API_BASE`). CORS allows only the origin
`http://localhost:5173` (`internal/httpapi/server.go:171`). WebSocket origins allowed are
`localhost:5173` and `127.0.0.1:5173` (`internal/ws/gateway.go:180`). These origin checks stop
browsers only. A non-browser client is not blocked by them. A UI served from any other origin is
blocked until a reverse proxy puts both under one origin.

Verify startup: `curl -H 'X-User: <user id>' http://127.0.0.1:8090/api/me` returns that user.
The same call with an unknown id returns `401` (`internal/httpapi/server.go:145-160`).

### Routes you need

All under `/api`, all JSON, and all (except the WebSocket) require `X-User`
(`internal/httpapi/server.go:82-116`; the full contract is `docs/api.md`):

| Purpose | Route |
|---|---|
| who am I | `GET /api/me` |
| open a room by hand | `POST /api/rooms` `{title, severity, pipeline_id}` |
| open a room from an alert | `POST /api/alerts/webhook` (see step 6) |
| list / read rooms | `GET /api/rooms`, `GET /api/rooms/{id}`, `GET /api/rooms/{id}/events?after=&limit=` |
| live view | `GET /api/rooms/{id}/ws?user=<id>&after=<seq>` (WebSocket; `?user=` is the identity, `internal/ws/gateway.go:156-160`) |
| talk to the room | `POST /api/rooms/{id}/comments`, `/steer`, `/suggestions/{sid}/send`, `/control/request`, `/control/grant`, `/close`, `/trace` |
| fixes | `POST /api/proposals/{pid}/approve`, `/reject` |
| shift change | `POST /api/rooms/{id}/handoffs`, `POST /api/handoffs/{hid}/accept` |
| pipeline map | `GET /api/pipelines/{pid}/topology` |
| probes (no `X-User`, outside `/api`) | `GET /healthz`, `GET /readyz`; `/readyz` checks the room store with a timeout (`server.go:79-80`; `internal/httpapi/health.go:85-90`) |

### What happens on a restart

On start-up the service reloads every room from the event log (`internal/room/service.go:183-221`).
An open room comes back with its agent **idle**. The service writes an `agent.status` `idle` event
with the detail "server restarted", and a system message (`service.go:223-240`). The agent does not
run again until someone resumes it: a driver's steer, a comment that mentions `@agent`, or a sent
suggestion (`internal/room/service_steer.go:44,68,137`; `service.go:243-251`). The new run starts
from the room's original alert, not from where the old run stopped (`service.go:247-251`).

A failing agent run is retried, up to 3 runs in total, with a pause that starts at 1 second and
doubles. Each failure posts an `agent.status` `error` event (`service.go:663-723`).

A fix that was fully approved but had not finished when the server stopped is **not re-run**.
It is marked failed. The service writes one `action.executed` event with `ok=false` and the text
"proposal was interrupted by a service restart before it finished; create a new proposal to retry",
and sets the proposal to failed (`service.go:260-290`). Check the target system yourself to see how
far the interrupted step got, before you ask for a new proposal.

### Network exposure

There is no real login yet (F1). `docs/api.md` §Auth identifies users by the `X-User` header.
`cmd/server` refuses to listen on any address other than loopback unless you pass `--allow-remote`.
Loopback is all of `127.0.0.0/8`, `::1`, and `localhost` in any letter case. With the flag it starts
and logs a warning (`cmd/server/main.go:59-67`; `cmd/server/listen.go:19-41`). Keep
`PIPEROOM_HTTP_ADDR` on loopback. Reach it through an SSH tunnel, a VPN, or an authenticating
reverse proxy that sets `X-User` itself and strips the header from client requests. The WebSocket
takes the same unverified `?user=` value. `--allow-remote` adds no authentication.

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
   Note that `X-User` is not proof of identity (F1), so the approval check is only as good as
   your network control.
5. Practise a hand-off between two engineers.
6. If you enabled Slack: check that the room's parent message appears once in the channel, and
   that replies go into its thread.

## 6. Wire up alerts

Point your alerting system at `POST /api/alerts/webhook` (`internal/httpapi/server.go:100`,
handler `internal/httpapi/handlers.go:50-92`). What it does on `main`:

- **Auth.** It needs `X-User: <user id>` of a user in `users.yaml`, like every other route
  (`server.go:145-160`). **It does not verify the sender.** Nothing checks a signature or a token.
  `internal/alertin` has the verification code for Alertmanager, Grafana, PagerDuty, Datadog and
  a generic form, but the server never calls it (`internal/alertin/verify.go:109-115`). Treat the
  endpoint as open to anyone who can reach it and send that header. Keep it reachable only from
  your alerting system (F9).
- **Body.** Valid JSON, up to 1 MiB (`internal/httpapi/server.go:24`, `handlers.go:52-64`). Piperoom
  reads `title`, `severity` and `pipeline_id` if the body is an object. The whole body is stored
  unchanged in the room's `room.created` event (`internal/room/service_room.go:136-138`), so do not
  put secrets in alert payloads.
- **Defaults.** No title becomes "New alert". An unknown severity becomes `sev3`. A missing
  `pipeline_id` becomes `unknown` (`service_room.go:93-102`). A `pipeline_id` with characters other
  than letters, digits, `_`, `.`, `-` is rejected with `400` (`service_room.go:103-105`).
- **Unknown pipeline.** A missing or unknown pipeline still opens a room. The room gets a system
  message that proposals are disabled (`service_room.go:150-157`). Any proposal the agent tries is
  refused (`internal/room/service_proposals.go:83-86`). The agent still runs, but its tools are
  scoped to a pipeline, so expect it to find little.
- **De-duplication.** A repeat has the same `dedup_key`, or the same bytes when the body has no
  `dedup_key`, for the same `pipeline_id` (`service_room.go:106-108,172-190`;
  `internal/alertin/parse.go:335-340,355-359`). If a room is still open for that key, the repeat
  joins it. A system message says the alert fired again, and the response is `200` with that room.
  No second room and no second agent start (`service_room.go:116-121,214-223`;
  `handlers.go:82-88`). A new room returns `201`. Closing the room frees the key, so the next repeat
  opens a new room (`service_room.go:336-338`). The key is rebuilt after a restart
  (`service.go:209,229-239`). If several open rooms hold the same key, the first one keeps it
  (`service_room.go:229-239`).
  - **Watch out:** the body alone decides whether a repeat is a repeat. A body with no `dedup_key`
    is a repeat only if it is byte-identical to the first one. If the body has a timestamp or any
    other changing field, every send opens a new room. Put a stable `dedup_key` in the body.
    Your alerting system's own grouping and repeat settings still decide how many requests it
    sends.
- **Resolved alerts.** A body with `"status": "resolved"` that matches an open room is noted in
  that room. The room stays open, because only the driver closes rooms (`service_room.go:219-222`).
  A resolved alert with no open match returns `202` with `{"ignored": true, "reason": "resolved alert with no open room"}`.
  It stores nothing and starts no agent (`service_room.go:122-125`; `handlers.go:68-78`). A resolved
  alert **without** a `dedup_key` always takes this path, unless its bytes are identical to an open
  room's first alert. A resolved body is normally not identical, because its `status` differs.
  The generic key is a hash of the body (`internal/alertin/parse.go:335-340`).
- **Driver.** The room has no driver until someone takes control (`POST /api/rooms/{id}/control/grant`).

## 6b. Slack (optional, outbound only)

- Turn it on by setting `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` (section 1), then restart. The log
  says "slack bridge enabled" (`cmd/server/slack.go:59-79`). With neither set, it logs "slack bridge
  disabled" and nothing else changes.
- Use one **private** channel and add the bot to it. A bot that is not in the channel gets
  `not_in_channel`, and that room's mirror stops (`internal/slackbridge/bridge.go:105-109`). A bad
  token, a missing scope or an archived channel also stop the mirror (`bridge.go:105-109`).
- Slack only receives events. Nothing in Slack reaches a room: no approvals, no steers
  (`internal/slackbridge/bridge.go:3-4`). Approvals happen only in Piperoom.
- Each room gets one thread. After a restart the bridge reads its saved thread state and
  continues from the last event it posted (`bridge.go:131-139`; `internal/store/slackthreads.go`).
  A crash between a Slack post and the save can repeat one message (`bridge.go:6-7`).
- What it posts, including approver names, the close resolution and the byte limits, is in
  [data-handling.md](data-handling.md#1-where-data-goes).

## 6c. Not built yet

- **Real login** (F1; the design doc named in `docs/api.md` is not on `main`). Until it exists,
  anyone who can reach the API or the WebSocket can act as any user.
- **Snowflake warehouse.** Not selectable yet. See [permissions.md](permissions.md#warehouse).

## 7. Run the pilot

- Start in **read-only mode** for Connect and Registry (no write grants). Let proposals show
  what the agent would do, and have the owners make the change by hand.
- After a few incidents, enable write mode for one layer at a time. The exact calls to allow are
  in [permissions.md](permissions.md#kafka-connect) and [permissions.md](permissions.md#schema-registry).
- Keep a list of every wrong or unhelpful agent answer and send it to the Piperoom team.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `pg_recent_ddl` says the audit log is not installed | trigger not installed, or role lacks `USAGE`/`SELECT` on `piperoom_audit` |
| `kafka_consumer_lag` permission error | missing `Describe` on that consumer group |
| `kafka_topic_config` permission error | missing `DescribeConfigs` on the pipeline's topics (see [permissions.md](permissions.md#kafka)) |
| `pg_row` / `warehouse_row` "table is not in pipeline" | table is missing from `source_tables` / `sink_tables`, or the `pipeline_id` the agent used is not the one you expect |
| `connect_config` shows `***` | expected: secret-looking keys are redacted; their values are never shown (`connect_tools.go:155-170`) |
| Proposal cannot execute | write mode not granted on Connect/Registry (by design) |
| `connect_update_connector_config` fails with a 405 | Connect older than 3.5 does not support `PATCH /connectors/{name}/config` (`connect.go:119-140`) |
| Server exits at start with "users/owners config" | `users.yaml` and `owners.yaml` disagree about a layer owner (step 4). The message names the user and layer |
| Server exits at start with "topic_prefix is required", "no source tables", "no connectors" or "sink_ts_column is required" | fix the named pipeline in `owners.yaml` (`internal/owners/owners.go:84-93`) |
| Server exits at start with "ping for migrations" | `PIPEROOM_DB_URL` cannot be reached, or the role lacks rights to create tables |
| Server exits at start with "slack bridge" | only one of `SLACK_BOT_TOKEN` / `SLACK_CHANNEL_ID` is set, or the channel is a `#name` not an ID |
| Slack thread never appears | bot not in the channel (`not_in_channel` in the log), or the room was opened before Slack was enabled and the server was not restarted |
| A repeat alert opened a second room | the body differs between sends (no `dedup_key`, or a changing field), the `pipeline_id` differs, or the first room was already closed |
| Web UI shows CORS errors | the UI is not on `http://localhost:5173`. CORS allows only that origin (`server.go:171`) |
| Web UI shows WebSocket errors | the UI is not on `localhost:5173` or `127.0.0.1:5173` (`ws/gateway.go:180`). These checks are browser-only |
| A fix shows "interrupted by a service restart" | the server restarted mid-fix; check the system, then re-propose |
| No agent response after a restart | the agent is idle until a driver steers or someone mentions `@agent` (step 4) |
| No agent response | `ANTHROPIC_API_KEY` missing, `ANTHROPIC_BASE_URL` pointing somewhere unexpected, or egress to the API is blocked |
