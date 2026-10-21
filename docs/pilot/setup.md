# Setup

Goal: Piperoom running against a **staging** or replica pipeline, investigating a real or staged
alert. Do this first against staging, not production.

There are two ways to run it. The **Docker path** (one image with the server and the web UI, and a
compose file with its own Postgres) is in [deploy/README.md](../../deploy/README.md). This page
covers the same settings for a host you run yourself.

Nothing in this repo measures setup time. The slow part is usually the credential and network
requests in [permissions.md](permissions.md), and naming a reachable owner for each layer. Plan
for those before you plan the install.

New here? Start with [quickstart.md](quickstart.md): two commands, read-only, no room database.

## Which variables matter first

These ten settings are what a first install sets. Every other setting is under [Reference](#reference).
All are read by `config.Load()` in `internal/config/config.go`, except `ANTHROPIC_API_KEY`, which the SDK reads.

| Variable | Default | What it does | Required? |
|---|---|---|---|
| `PIPEROOM_HTTP_ADDR` | `127.0.0.1:8090` | Listen address. Dev login off loopback is refused unless both dev variables are set ([section 1](#1-configure-the-environment)); `--quickstart` always refuses it (`cmd/server/quickstart.go`). | No |
| `PIPEROOM_AUTH_MODE` | `dev` | `dev` (pick a user, no password) or `oidc` (single sign-on; see [deploy/README.md](../../deploy/README.md#2-settings-in-deployenv)). `--quickstart` forces `dev`. | Yes on any shared network: `oidc` |
| `OWNERS_FILE`, `USERS_FILE` | `dev/owners.yaml`, `dev/users.yaml` | Who owns each layer, and who may log in, drive and approve. The server refuses to start when the two disagree ([section 2](#2-describe-your-pipeline-ownersyaml-and-usersyaml)). `discover -write-owners` writes both; `--quickstart` takes `--owners` and writes its own users file when `USERS_FILE` is unset. | Yes |
| `CONNECT_URL` | `http://localhost:8083` | Kafka Connect REST. Not contacted at start-up, so a wrong URL shows on the first tool call. | Yes |
| `KAFKA_BROKERS` | `localhost:29092` | Kafka bootstrap brokers, comma separated. | Yes |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry. `""` or `off` means you have none. | Yes, or `off` |
| `SOURCE_DB_URL` | the dev stack's `orders` database | The CDC source Postgres, read-only role. Pinged at start-up (`internal/clients/postgres/postgres.go`). | Yes |
| `WAREHOUSE_DB_URL` | the dev stack's `warehouse` database | The warehouse (Postgres; Snowflake or ClickHouse with `WAREHOUSE_KIND`), read-only role. Pinged at start-up. | Yes |
| `PIPEROOM_WRITE_MODE` | `off` | `off`: approved fixes are recorded but never run. `on`: they run. Any other value stops the start (`config.ParseWriteMode`). One switch per process ([section 7](#7-run-the-pilot)). | No |
| `ANTHROPIC_API_KEY` | none | The model key for the default `anthropic` provider. Other providers: [section 1](#1-configure-the-environment). | Yes (`anthropic` provider) |

Each URL and file setting falls back to the local dev stack when unset, so set every one you rely on.
The full server (without `--quickstart`) also needs `PIPEROOM_DB_URL`, its own room database ([section 0](#0-prerequisites)).
To serve the web UI, set `PIPEROOM_WEB_DIR` to the built `web/dist` ([section 4](#4-start-the-service)).

## Reference

### 0. Prerequisites

- **Go 1.27** to build `cmd/server` (`go.mod:3`: `go 1.27.2`), or **Docker with Compose v2** for the
  image in [deploy/README.md](../../deploy/README.md).
- **Node.js** to build and serve the web UI in `web/` (`npm ci`), if you run the UI from the
  source tree. `web/package.json` pins no version. The dev machine used Node 24 (`AGENTS.md` §3).
  The Docker image builds the UI itself, so Node is not needed for that path.
- **A Postgres database for Piperoom's own state**, separate from the source and the warehouse.
  Its URL is `PIPEROOM_DB_URL`. Use a role that owns only this database. The service creates its own
  tables at start-up (`cmd/server/main.go`), so the role needs rights to create tables in it.
- **Network access** from the Piperoom host to: the source Postgres, the warehouse (Postgres, or
  the Snowflake account / ClickHouse host set by `WAREHOUSE_KIND`), Kafka brokers, Kafka Connect REST, Schema
  Registry, the room database, and the model provider you choose (section 1): `api.anthropic.com`
  (unless you pin `ANTHROPIC_BASE_URL`), the AWS Bedrock runtime in `AWS_REGION`, or the Vertex AI
  endpoint in `VERTEX_REGION`. Add `slack.com` only if you enable Slack. For Snowflake, add the
  account host in the DSN, for example `<account>.snowflakecomputing.com`. For ClickHouse, add
  the host in the DSN ([clickhouse.md](clickhouse.md)).
- **Settings for real mode:** `SOURCE_DB_URL`, `WAREHOUSE_DB_URL`, `WAREHOUSE_KIND` (default
  `postgres`), `PIPEROOM_DB_URL`, `KAFKA_BROKERS`, `CONNECT_URL`, `SCHEMA_REGISTRY_URL`,
  `OWNERS_FILE`, `USERS_FILE`, and the model provider's settings (section 1). Unset, each of the URL
  and file settings falls back to the localhost dev stack (`internal/config/config.go`), so
  set every one you rely on. **`ANTHROPIC_API_KEY` has no localhost fallback**: unset, the SDK falls
  back only to its other credential sources (`anthropic-sdk-go@v1.80.0/client.go:30-82`), and model
  calls fail if none is set. It is needed only for the `anthropic` provider.
- **Start-up checks are partial.** The source and warehouse Postgres are pinged when opened
  (`internal/clients/postgres/postgres.go`, `internal/clients/warehouse/warehouse.go`). A
  Snowflake warehouse is pinged when opened too (`internal/clients/snowflake/snowflake.go`).
  Connect and Schema Registry are not contacted at start-up, so a wrong URL shows up on the first
  tool or action that uses it. For Bedrock, the AWS credentials are fetched once at start-up, and
  for Vertex the Google default credentials are looked up (section 1).
- **The credentials** from [permissions.md](permissions.md). One credential set per process, or one
  per pipeline with `connections:` in `owners.yaml` (section 2a, F7).
- Read [README.md](README.md#read-this-first-pilot-limits) for the current limits, and
  [Supported scale](#supported-scale) for what one replica is tested to carry.

### 1. Configure the environment

All settings are environment variables read by `internal/config/config.go` (`config.Load()`).
`config.go` holds the fields and `config.go` the defaults. The settings are grouped below.
The Docker path reads `deploy/.env` (which starts from `deploy/pilot.env.example`), but the compose
file passes only the variables it lists in its `environment:` block (`deploy/docker-compose.pilot.yml`).
That list carries the model-provider, triage, token-budget, `AWS_*`, `VERTEX_*`,
`GOOGLE_APPLICATION_CREDENTIALS`, retention and proxy variables, so Bedrock, Vertex, the triage model,
the token budget and retention work in the container once you set them in `deploy/.env`. Compose also
never passes `ANTHROPIC_BASE_URL`, so a value on the host cannot leak into the container. A variable
exported in the shell that runs compose overrides `deploy/.env`, so run compose from a clean shell
(see [deploy/README.md](../../deploy/README.md)).

**Core and database**

| Variable | Meaning |
|---|---|
| `PIPEROOM_HTTP_ADDR` | listen address of the room service. Default `127.0.0.1:8090`. Loopback is all of `127.0.0.0/8`, `::1`, and `localhost` in any letter case. Any other host, including an empty host or `0.0.0.0`, is refused in dev auth mode unless `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` **and** `PIPEROOM_DEV_ACK=dev-login-has-no-password` are both set (`cmd/server/listen.go`, `cmd/server/authsetup.go`; the deprecated `--allow-remote` flag is an alias of the first). The Docker image listens on `0.0.0.0` in `oidc` mode and sets neither dev variable (`Dockerfile`); `oidc` mode ignores both |
| `PIPEROOM_DB_URL` | Piperoom's own database: room events, proposals, Slack thread state. Use a role that owns only this database. Migrated automatically at start-up |
| `SOURCE_DB_URL` | source Postgres, the **read-only** role |
| `WAREHOUSE_KIND` | `postgres` (default), `snowflake` or `clickhouse`. A value the wiring does not accept stops the start (`internal/wiring/warehouse.go`). Read from `config.go` |
| `WAREHOUSE_DB_URL` | the warehouse, read-only role. With `postgres`: a Postgres URL. With `snowflake`: a gosnowflake DSN, `user:password@account/database/schema?warehouse=...&role=...`, with the password **URL-encoded** (a raw `%`, `+`, `?`, `@`, `/` or `:` breaks it, `internal/clients/snowflake/README.md`). With `clickhouse`: a clickhouse-go/v2 DSN, `clickhouse://<user>:<password>@<host>:<port>/<db>?secure=true` (see [clickhouse.md](clickhouse.md)). Startup errors are redacted of the DSN (`internal/wiring/warehouse.go`). Read-only access comes only from the DSN's role (for Snowflake, `internal/clients/snowflake/snowflake.go`; for ClickHouse, a `readonly=2` profile, `internal/clients/clickhouse/client.go`). See [permissions.md](permissions.md#warehouse) |
| `KAFKA_BROKERS` | comma-separated brokers |
| `CONNECT_URL` | Kafka Connect REST base URL |
| `SCHEMA_REGISTRY_URL` | Schema Registry base URL, or `off` when the pipelines use no Schema Registry (the pipeline map then has no schema-subject nodes and the schema tools answer "Schema Registry not configured"). **Docker Compose: required** (`deploy/docker-compose.pilot.yml` refuses to start without it; a blank value is an error, so a forgotten registry is never read as none). **Helm:** the chart skips empty values, so an empty value is unset and the server falls back to the dev default `http://localhost:8081`, which a pod cannot reach: the registry then shows as unknown in the map. Set the URL or `off`. In the server itself (`internal/config/config.go`) an explicitly empty value also means none |
| `OWNERS_FILE` | pipelines and layer owners (step 2) |
| `USERS_FILE` | users and the layers each owns (step 2) |
| `PIPEROOM_PUBLIC_URL` | optional. The externally visible base URL, used for room links in Slack. It is a WebSocket origin only when `PIPEROOM_WEB_DIR` is set; without a web dir the WebSocket ignores it (section 4). Its host is also the public host in the served UI's content-security policy. Must be a plain `http(s)` host, with no wildcard. An invalid value stops the start (`cmd/server/main.go`; `internal/httpapi/static.go`; `internal/httpapi/server.go`; `internal/ws/gateway.go`; `internal/httpapi/static.go`). Empty means no links. **Required in `oidc` mode, and it must be `https` there** (it sets the OIDC redirect URI) |
| Login: `PIPEROOM_AUTH_MODE`, `PIPEROOM_OIDC_*`, `PIPEROOM_AUTH_VIEWER_GROUPS`, `PIPEROOM_SESSION_IDLE`, `PIPEROOM_SESSION_MAX`, `PIPEROOM_TRUSTED_PROXIES`, `PIPEROOM_ALLOWED_HOSTS`, `PIPEROOM_SOLO_USER` | `PIPEROOM_AUTH_MODE` is `dev` (default) or `oidc`. Every one of these is described in [deploy/README.md](../../deploy/README.md#2-settings-in-deployenv), with the identity provider setup. They are read in `internal/config/config.go` and checked in `internal/auth/settings.go` and `cmd/server/authsetup.go` |
| `PIPEROOM_WEB_DIR` | **Live.** When set, the server also serves the built web UI (`web/dist`) at `/`, and refuses to start if the directory has no `index.html` (`checkWebDir` in `cmd/server/main.go`; `internal/httpapi/static.go`). The Docker image sets it to `/web` (`Dockerfile`). Unset, the server serves the API and the probes only |
| `PIPEROOM_RETENTION_DAYS` | **Live.** Deletes whole closed rooms (events, proposals, approvals, hand-offs) closed more than this many days ago, checked every 6 hours (`cmd/server/retention.go`; `internal/store/retention.go`). Open rooms are never deleted. Empty or `0` keeps everything; a value must be a whole number from 0 to 36500, or the start stops. Deletion cannot be undone: export a room first with `GET /api/rooms/{id}/export` (SHA-256 over the JCS form, `internal/room/service_export.go`) |

**Model provider, triage and token budget**

| Variable | Meaning |
|---|---|
| `PIPEROOM_MODEL_PROVIDER` | `anthropic` (default), `bedrock` or `vertex`. Any other value stops the start (`internal/agent/endpoint.go`). Field at `config.go`, default at `config.go` |
| `PIPEROOM_MODEL` | model id, default `claude-opus-5-5` (`config.go`). With `bedrock`, use a Bedrock model or inference-profile id: the default first-party name is rejected by Bedrock on the first request, and a warning is logged when it is left in place (`internal/wiring/model.go`) |
| `PIPEROOM_EFFORT` | reasoning effort, default `high` (`config.go`) |
| `PIPEROOM_TRIAGE_MODEL` | optional. A second model id for the first turns of an investigation (reading tools, forming a hypothesis). Default: none, so the main model runs every turn. The rules are in section 1a |
| `PIPEROOM_ROOM_TOKEN_BUDGET` | per-room cap on model tokens. Empty = `2000000` (`internal/agent/budget.go`). `0` = no cap. Any other value must be a whole number of at least 1, or the start stops (`budget.go`). The count is kept in memory and restarts from zero when the server restarts (`budget.go`) |
| `AWS_REGION` | required with `bedrock`. The Bedrock runtime endpoint is `bedrock-runtime.<AWS_REGION>.amazonaws.com` (`internal/agent/endpoint.go`; SDK `anthropic-sdk-go@v1.80.0/bedrock/bedrock.go:257`) |
| `AWS_BEARER_TOKEN_BEDROCK` | optional, `bedrock` only. A Bedrock API key the SDK reads itself (`endpoint.go`). When it is set, AWS credentials are not looked up |
| AWS credentials | `bedrock` without the bearer token: the standard AWS chain (environment, shared config, or an instance or task role). Checked once at start-up, with a 5-second bound (`endpoint.go`) |
| `VERTEX_PROJECT_ID`, `VERTEX_REGION` | required with `vertex`. The endpoint is `https://<VERTEX_REGION>-aiplatform.googleapis.com/`, with `global`, `us` and `eu` as special values (SDK `anthropic-sdk-go@v1.80.0/vertex/vertex.go:96-106`) |
| Google credentials | `vertex`: Google application default credentials, looked up once at start-up with the `cloud-platform` scope (`endpoint.go`) |

**Model access and the Anthropic SDK (provider `anthropic`)**

| Variable | Meaning |
|---|---|
| `ANTHROPIC_API_KEY` | read by the Anthropic SDK, not by Piperoom. **No localhost fallback.** Piperoom does not write it to config files, events or logs. If it is unset, the SDK falls back to `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_PROFILE` and other sources (`anthropic-sdk-go@v1.80.0/client.go:30-82`) |
| `ANTHROPIC_BASE_URL` | **read by the SDK, `anthropic` provider only** (`anthropic-sdk-go@v1.80.0/client.go:58`). The Bedrock and Vertex clients turn off the SDK's environment defaults, so this variable does not reach them. For `anthropic`, every prompt goes to this host. Leave it unset, or pin it to the Anthropic host you approved |
| `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_PROFILE` | read by the SDK when `ANTHROPIC_API_KEY` is not set (`anthropic-sdk-go@v1.80.0/client.go:30-82`). Leave them unset on the Piperoom host, so the key used is the one you chose |
| `ANTHROPIC_WORKSPACE_ID` | only if the key is a user key, and only for `anthropic` (`internal/agent/model.go`) |

**Agent and alerts**

| Variable | Meaning |
|---|---|
| `PIPEROOM_MAX_AGENTS` | caps concurrent agent runs across all rooms **of one replica**; the others queue. Empty or `0` means the default, 3 (`internal/room/agentslots.go`; `cmd/server/maxagents.go`). The start-up log prints the value (`server: agent cap` in `cmd/server/main.go`). It is a cost and fairness knob, not a server-capacity knob (see [Supported scale](#supported-scale)). `PIPEROOM_MAX_AGENTS_GLOBAL` caps working agents across all replicas (`internal/config/config.go`) |
| `ALERTMANAGER_TOKEN`, `GRAFANA_ALERT_SECRET`, `PAGERDUTY_ALERT_SECRET`, `DATADOG_ALERT_TOKEN`, `GENERIC_ALERT_SECRET` | Secrets for the signed alert routes `POST /api/alerts/{source}` (section 6). A source is enabled only when its secret is set (`internal/alertin/verify.go`); unset, its route answers 401 (`internal/httpapi/alerts.go`). Read in `cmd/server/alerts.go` (`config.go`) |
| `PAGERDUTY_SERVICES`, `DEFAULT_ALERT_PIPELINE` | Used by the signed routes. `PAGERDUTY_SERVICES` maps service ids to pipelines, and a bad entry stops the start. `DEFAULT_ALERT_PIPELINE` is used when an alert names no pipeline, and a bad value stops the start (`cmd/server/alerts.go`; `config.go`) |

**Built-in watcher**

The watcher polls Kafka Connect and the source database and tells you when a pipeline is in trouble.
The design is in [docs/design/watcher.md](../design/watcher.md).
The server reads these variables at start (`config.ParseWatch`), and a bad value stops the start with
an error that names the variable.

| Variable | Meaning |
|---|---|
| `PIPEROOM_WATCH_MODE` | `off`, `log` or `rooms`. Default `log` (see the modes below) |
| `PIPEROOM_WATCH_INTERVAL` | how often the watcher checks. Go duration, default `60s`, minimum `15s` |
| `PIPEROOM_WATCH_CHECK_TIMEOUT` | deadline for one check, default `10s`, at most `10s` (the source role's statement timeout) |
| `PIPEROOM_WATCH_COOLDOWN` | quiet time after a watcher room is closed, default `60m` |

Modes:

- `off`: no watcher is started. `--demo` always runs with the watcher off, whatever this says.
- `log` (default): the watcher runs and writes one log line for each change of state
  (`watch: ...` with `pipeline`, `signal`, `value`, `since`). It opens no room and posts nothing. Use
  it first: read the logs for a week and check that they match what really happened. This is **not**
  [shadow rooms](shadow-rooms.md): `log` mode runs no agent at all, shadow rooms run the agent blind.
- `rooms`: the same checks, and a firing signal opens a room (or joins the room that is already open
  for the pipeline). The agent investigates from the evidence. When the signal clears, the room gets a
  "resolved" note and stays open until the driver closes it. A room the driver closed is not opened
  again while the same signal still fires. One exception: if the signal rises to sev1, the watcher
  opens a room once more, and that one skips the cooldown.

To switch from `log` to `rooms`: set `PIPEROOM_WATCH_MODE=rooms` and restart the server. At start it
logs one line, `server: watcher starting`, with the mode, the interval and the number of pipelines
watched. Rooms the watcher opens count against `PIPEROOM_MAX_AGENTS`, and each room is an agent run
with the usual token budget.

What the source database needs:

- `SOURCE_DB_URL` must point at the **primary**. A replica cannot answer `pg_current_wal_lsn()`, so
  the replication-slot check fails there.
- PostgreSQL 13 or newer. From 13, `pg_replication_slots` has `safe_wal_size`, which the watcher uses
  for "slot invalidated in N hours". On older versions that estimate is missing. It is also missing
  when `max_slot_wal_keep_size` is `-1`, which is the default (and the RDS default). Then the watcher
  shows the growth rate, and an estimate only if you set `watch.wal_headroom_bytes` for the pipeline.
- No new grants: the watcher reads `pg_replication_slots` and calls the Connect status and config
  endpoints, the same reads the agent's tools already make ([permissions.md](permissions.md)).

**Slack, Kafka, Connect and Registry**

| Variable | Meaning |
|---|---|
| `SLACK_BOT_TOKEN` | optional, **secret**. Bot token with `chat:write`. Slack is on only when this and `SLACK_CHANNEL_ID` are both set. One without the other stops the start (`cmd/server/main.go`; `internal/slackbridge/bridge.go`) |
| `SLACK_CHANNEL_ID` | optional. A channel **ID** such as `C0123ABC`, never `#name` (`bridge.go`). Use a private channel |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME` | Kafka TLS/SASL (empty = plaintext) |
| `KAFKA_SASL_PASSWORD` | **secret** |
| `KAFKA_TLS_CA_FILE`, `KAFKA_TLS_CERT_FILE`, `KAFKA_TLS_KEY_FILE`, `KAFKA_TLS_SERVER_NAME` | Kafka TLS files (the key file is a secret; mount read-only) |
| `SCHEMA_REGISTRY_USERNAME`, `SCHEMA_REGISTRY_TLS_CA_FILE`, `SCHEMA_REGISTRY_TLS_CERT_FILE` | Registry basic auth / TLS |
| `SCHEMA_REGISTRY_PASSWORD`, `SCHEMA_REGISTRY_TLS_KEY_FILE` | **secrets** |
| `CONNECT_USERNAME`, `CONNECT_TLS_CA_FILE`, `CONNECT_TLS_CERT_FILE` | Connect basic auth / TLS |
| `CONNECT_PASSWORD`, `CONNECT_TLS_KEY_FILE` | **secrets** |

The client code refuses unsafe combinations at start-up: basic auth over `http://` and TLS files
with an `http://` URL (`internal/clients/netsec/http.go`), and SASL PLAIN without TLS
(`internal/clients/netsec/kafka.go`). The TLS, SASL and auth paths are checked with unit tests
and fake servers only. They have not been tried against a real secured cluster, so test them on
staging first.

Put secrets in your secret manager and inject them into the process environment. Do not
commit them, and do not put DSNs with passwords in shell history or tickets. Use
`sslmode=require` (or stricter) for remote Postgres, not the dev default `sslmode=disable`.

#### 1a. Triage model and the token budget

**Triage model** (`PIPEROOM_TRIAGE_MODEL`, optional). The first turns of an investigation run on the
triage model. The main model (`PIPEROOM_MODEL`) takes over for good when any of these happens
(`internal/agent/triage.go`):

- a turn would call `propose_fix`: that triage answer is dropped and the same request runs on the
  main model, so a fix is never written by the cheaper model (`triage.go`);
- a `[verification]` verdict has gone to the model;
- six turns have run on the triage model (`internal/agent/endpoint.go`).

A triage request that the provider rejects (a 4xx other than 401, 408, 409 or 429) is redone on the
main model for that run. Triage is turned off for every room only when the main model accepted the
same request and the error is about the model or the request shape, so a wrong triage model id
costs one failed request, not one per room (`triage.go`; the test that decides it is
`modelRejection`, `triage.go`). Prompt caches are per model, so a
short triage phase saves less than the token prices suggest.

**Token budget** (`PIPEROOM_ROOM_TOKEN_BUDGET`). Each room may use `2000000` model tokens by default
(input, cache creation, cache reads, output and compaction, counted at face value). The check runs
before each call, so one call can go past the cap by its own output (`internal/agent/budget.go`).
When a room is over budget, its investigation loop stops with a `BudgetError` and the room says so
(`budget.go`; `internal/agent/run.go`). Each room's count is kept on the Endpoint for the
life of the process (`internal/agent/endpoint.go`). Once the count reaches the limit, every
investigation call is refused at once (`budget.go`), so a new run for that room is refused
too. Only a server restart resets the count, and changing `PIPEROOM_ROOM_TOKEN_BUDGET` also needs a
restart. Hand-offs are counted but never refused, so a person taking over a stopped room still gets a
summary (`budget.go`; `internal/agent/endpoint.go`). Closing a room drops
its count: the server calls `Endpoint.Forget` (`internal/agent/endpoint.go`) from the room-closed
hook (`cmd/server/watch.go`), so the counts of closed rooms do not pile up in memory. Set `0` only if you accept an uncapped cost per room.

### 2. Describe your pipeline: `owners.yaml` and `users.yaml`

Start from `dev/owners.yaml` and `dev/users.yaml`. The loader refuses a pipeline that has no
`topic_prefix`, no `source_tables` or no `connectors`, and refuses `sink_tables` without
`sink_ts_column` (`internal/owners/owners.go`).

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
  (`internal/events/events.go`).
- `source_tables` and `sink_tables` bound the row tools, and the read tools are scoped to the
  room's pipeline (its connectors, topics, consumer groups, subjects and tables). Keep `SELECT`
  grants identical to these lists
  ([data-handling.md](data-handling.md#3-prompt-injection-and-untrusted-text), F4, F11).
- Put the real people in `users.yaml`; remove the dev users.
- For OIDC login the same two files also say **who may log in and what they may do**: optional
  `email:` and `sub:` on a user, `group:` and `user:` principals, per-pipeline `owners:`
  (approvers and backups) and `auditors:` in `owners.yaml`. Short examples and the rules are in
  [deploy/README.md](../../deploy/README.md#identity-and-rights).
- A pipeline may also have an optional `watch:` block (`internal/owners/watch.go`). It turns the
  watcher on or off for the pipeline and sets, per signal, `enabled`, `action` (`log` or `room`) and
  `for`; for `slot_wal` also `fire`, `clear` and `eta_sev1`; and the pipeline's `slot` name and
  `wal_headroom_bytes`. `watch.slot_wal.fire` also sets the slot-WAL warning threshold of the pipeline
  map (`internal/topology/builder.go`). Without the block, the defaults apply. See
  [docs/design/watcher.md](../design/watcher.md).
- A pipeline may also have an optional `shadow:` block (`enabled: true`) that makes every alert room
  of the pipeline a **blind agent room**: the agent investigates but proposes nothing and shows
  nothing until the room is closed and labelled. This is not the watcher's `log` mode. See
  [shadow-rooms.md](shadow-rooms.md).

#### 2a. Tenancy

Design and every rule: [docs/design/tenancy.md](../design/tenancy.md).

- **Separate customers or legal entities: one deployment each.** This is the only hard boundary.
  Install the Helm chart once per tenant (own namespace, Secret, values, ingress host, OIDC client),
  or run one Terraform apply or one compose project per tenant, each with an `owners.yaml` that lists
  only that tenant's pipelines.
- **Teams within one organisation: one deployment, in-process tenants.** In `owners.yaml`:
  - `tenants: {<name>: {members: [group:..., user:...]}}` turns the room filter on: a person sees only
    the rooms of pipelines in their tenants. A person is in a tenant when listed in its `members:` or
    when they may approve any layer of one of its pipelines (so a top-level `layers:` owner is in every
    tenant whose pipelines do not override that layer: give each pipeline its own `owners:` block).
    Everything else answers `404`.
  - `tenant: <name>` on a pipeline (default `default`).
  - `connections:` on a pipeline names the **environment variables or files** that hold its DSNs, URLs
    and logins, for example `source_db_url: {env: PAYMENTS_SOURCE_DB_URL}` or
    `kafka_sasl_password: {file: /run/secrets/payments/kafka-password}`. Never put a secret in
    `owners.yaml`. A pipeline with `connections:` gets its own clients, tools, actions and watcher
    reads, and takes nothing from the process-wide `SOURCE_DB_URL`, `KAFKA_*` and so on; a pipeline
    without it uses those, as before.
  - The start fails when two tenants use the same variable or file (pipelines without `connections:`
    count as using the process-wide variables, and a TLS client key or certificate counts by its real
    path however it was named) unless every pipeline involved sets `shared_connections: true`. A CA
    bundle may be shared.
  - With any `connections:` block, unset the libpq variables (`PGPASSWORD`, `PGUSER`, `PGHOST`,
    `PGDATABASE`, `PGPASSFILE`, `PGSERVICE`, `PGSERVICEFILE`, `PGSSLCERT`, `PGSSLKEY`, `PGSSLPASSWORD`)
    and remove `~/.pgpass`, `~/.pg_service.conf` and `~/.postgresql/postgresql.key` on the Piperoom
    host: pgx would add them to a DSN that leaves something out, so the start refuses them.
  - A `tenants:`, `connections:` or `tenant:` key with no value (or with every entry commented out) is
    refused; remove the key instead.
  - Set `PIPEROOM_TENANCY_MODE=teams` so the server refuses to start if the `tenants:` block is ever
    lost. The start-up log prints the mode and which pipelines use their own connections.
  - On Helm, mount per-team secret files with `extraVolumes`/`extraVolumeMounts`, or add per-team
    variables with `secrets.refs`.
- Not per tenant in one deployment: the model key and its cost, `PIPEROOM_MAX_AGENTS`, the room
  database, the Slack channel and the people list.

### 3. Prepare the source database

1. Create the read-only role and grants ([permissions.md](permissions.md#source-postgres)).
2. (Optional) have your DBA install the DDL audit trigger (`dev/postgres-init/02-ddl-audit.sql`)
   so the agent can see recent schema changes.
3. Create an empty Postgres database and a role for Piperoom's own state, and put its URL in
   `PIPEROOM_DB_URL`. You do not run migrations by hand: `cmd/server` applies them at start-up
   (see step 4 and [permissions.md](permissions.md#room-database)).

### 4. Start the service

`cmd/server` is the room service: HTTP API, WebSocket, agent runner, the Slack mirror (if
configured), the model endpoint, the web UI (if `PIPEROOM_WEB_DIR` is set) and the executor for
approved fixes. It takes the flags `--demo` and `--healthcheck` (and the deprecated `--allow-remote`, an alias of
`PIPEROOM_DEV_ALLOW_NONLOOPBACK=1`; see [Network exposure](#network-exposure)). `--healthcheck` probes `GET /healthz`
on `PIPEROOM_HTTP_ADDR` and exits 0 when it is healthy (`cmd/server/main.go`,
`cmd/server/healthcheck.go`). The rest comes from the environment above.

**Real mode** (no flag) does this, in order (`cmd/server/main.go`):

1. Loads `USERS_FILE` and `OWNERS_FILE`. A missing or unreadable file stops the start.
2. Cross-checks the two (`room.ValidateUsers`, `internal/room/users.go`, called).
   Any disagreement stops the start, in **both** directions: every layer in `owners.yaml` must name
   a user who exists in `users.yaml` and lists that layer, and every layer a user lists must be
   assigned to that same user in `owners.yaml`. A layer name that is not one of the six known
   layers also stops the start. The error names the user and the layer. The check covers only a
   layer whose owners are all `user:` principals for the owners-to-users direction. The reverse
   users-to-owners check always applies: every layer a user lists must include that user as
   `user:<id>` in `owners.yaml`, even if the layer also lists groups. Skipped owners-to-users layers
   are logged once. It also refuses a `users.yaml` id that starts with `u_` and an `email:` that is
   not ASCII.
3. Applies the goose migrations to `PIPEROOM_DB_URL` (`migrations/migrate.go`), then opens
   the room store. A database that cannot be reached stops the start.
4. Starts the Slack mirror if `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` are set. A wrong
   Slack setting stops the start. Mirrors then run for every open room.
5. Builds the model endpoint for `PIPEROOM_MODEL_PROVIDER` (`internal/wiring/model.go`). A bad
   provider, a missing region or project, a bad token budget, or missing cloud credentials stop the
   start here, before any alert arrives.
6. Opens the data stack (`wiring.Open`; `internal/wiring/wiring.go`): source
   Postgres, the warehouse (`WAREHOUSE_KIND`), Kafka, Connect and Schema Registry, with the
   TLS/SASL/auth settings above. Source and warehouse are pinged here. It then registers the
   read-only tools and the six write actions (`wiring.RegisterTools`;
   `internal/wiring/wiring.go`). This needs the credentials from
   [permissions.md](permissions.md) and the provider's key or credentials.
7. Reloads every room from the log. A failure stops the start in real mode. One room that
   cannot be rebuilt is only logged and skipped (`internal/room/service.go`), and a room row
   with no events is skipped at start-up (`service.go`). If `PIPEROOM_WEB_DIR` is set, the
   UI is served from it. Then the server listens on `PIPEROOM_HTTP_ADDR`.

```sh
export PIPEROOM_DB_URL=... SOURCE_DB_URL=... WAREHOUSE_DB_URL=... KAFKA_BROKERS=... \
       CONNECT_URL=... SCHEMA_REGISTRY_URL=... OWNERS_FILE=... USERS_FILE=... ANTHROPIC_API_KEY=...
go run ./cmd/server
```

**The Docker path.** [deploy/README.md](../../deploy/README.md) builds one image with the server and
the web UI, and starts it with a compose file that includes its own Postgres. It has its own
10-minute path: copy `deploy/pilot.env.example` to `deploy/.env`, copy the two example YAML files
under `deploy/config/`, then start the compose file. The image runs `/piperoom` with `PIPEROOM_AUTH_MODE=oidc` set in its environment and carries no dev auth (`Dockerfile`); it stops at start-up, naming the variable, until the OIDC settings are given. Its health check calls `--healthcheck`. The compose file
publishes the port on `PIPEROOM_BIND`, which defaults to `127.0.0.1` (`ports:` in `deploy/docker-compose.pilot.yml`).
Compose passes only the variables it lists; the list covers every setting in section 1 (see the compose
note there).

**`--demo`** (`go run ./cmd/server --demo`) needs no database, stack or API key. It uses an
in-memory store, a scripted agent and a fake topology. It runs the **real** write actions over
fake clients, so the propose / approve / execute path is the real one (`cmd/server/main.go`).
It still loads `OWNERS_FILE` and cross-checks it against the users (falling back to built-in
demo users if `USERS_FILE` is missing). Use it to try the UI and the approval flow safely.
Nothing in it touches your systems. **But demo mode does post to the real Slack channel if the
`SLACK_*` variables are set**, because the Slack mirror starts in both modes (`cmd/server/main.go`).
Unset them for a demo, or use a throwaway channel.

**The web UI.** Run it from the source tree with `cd web && npm ci && npm run dev`. That serves it on
`localhost:5173` (`web/package.json:7`). The Vite dev server proxies `/api` (with the WebSocket)
and `/auth` to the Go server at `http://127.0.0.1:8090`, so the browser sees one origin
(`web/vite.config.ts`; override the target with `PIPEROOM_API`). Or set `PIPEROOM_WEB_DIR` to a
built `web/dist` and the server serves the UI at `/` itself (step 7, `cmd/server/main.go`).
There is no CORS: the server sends no `Access-Control-*` headers, and a cross-origin browser `POST`
gets `403`. The WebSocket always accepts its own host, the `PIPEROOM_PUBLIC_URL` host and the
`PIPEROOM_ALLOWED_HOSTS` entries. It adds `localhost:5173` and `127.0.0.1:5173` only when no web
dir is set and the mode is not oidc (`wsOriginPatterns` in `internal/httpapi/server.go`). These
origin checks stop browsers only. A non-browser client is not blocked by them. The
server also checks the `Host` header (`421 bad_host`): loopback names are always allowed; set
`PIPEROOM_ALLOWED_HOSTS` for a tunnel or proxy that rewrites `Host`.

In `oidc` mode you sign in at your identity provider instead
([deploy/README.md](../../deploy/README.md#single-sign-on-oidc)). In dev mode, log in by opening the
UI and picking a user from `users.yaml` (dev login: no password, a session
cookie; there are no `?user=` links). To check startup from the shell in dev mode:
`curl -H 'X-User: <user id>' http://127.0.0.1:8090/api/me` returns that user. This `X-User` form
works only in dev mode and only when the request carries no session cookie. The same call with an
unknown id returns `401`. Every `POST` from curl needs `-H 'Content-Type: application/json'`
(else `400`), except the signed `POST /api/alerts/{source}` routes, which take the sender's own
content type. In dev mode more than one `X-User` value (several header lines, or a comma in one
line) answers `403 proxy_identity`.

#### Routes you need

All under `/api`, all JSON, and all need a logged-in user: the session cookie, or in dev mode `X-User`
when curl sends no cookie. The signed alert routes need neither. (The full contract is `docs/api.md`.)

| Purpose | Route |
|---|---|
| who am I | `GET /api/me` |
| open a room by hand | `POST /api/rooms` `{title, severity, pipeline_id}` |
| open a room from a signed alert | `POST /api/alerts/{source}`, `source` = `alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic` (section 6) |
| open a room from an unsigned dev alert | `POST /api/alerts/webhook` (section 6; no sender check, F9) |
| list / read rooms | `GET /api/rooms`, `GET /api/rooms/{id}`, `GET /api/rooms/{id}/events?after=&limit=` |
| live view | `GET /api/rooms/{id}/ws?after=<seq>` (WebSocket; the identity is the session cookie, or in dev mode `X-User` when there is no cookie; neither gives `401`) |
| talk to the room | `POST /api/rooms/{id}/comments`, `/steer`, `/suggestions/{sid}/send`, `/control/request`, `/control/grant`, `/close`, `/trace` |
| fixes | `POST /api/proposals/{pid}/approve`, `/reject` |
| shift change | `POST /api/rooms/{id}/handoffs`, `POST /api/handoffs/{hid}/accept` |
| pipeline map | `GET /api/pipelines/{pid}/topology` |
| probes (no `X-User`, outside `/api`) | `GET /healthz`, `GET /readyz`; `/readyz` checks the room store with a timeout (`server.go`; `internal/httpapi/health.go`) |

#### What happens on a restart

On start-up the service reloads every room from the event log (`Load` in `internal/room/service.go`).
**It starts no agent by itself**, because a restart must not spend model calls on rooms nobody is
watching. What an open room shows depends on what its agent was doing:

- **Nearly every open room gets the notice** (shadow rooms excepted: their run is marked `interrupted`
  and they get no notice, see [shadow-rooms.md](shadow-rooms.md#restarts-and-failures)). An agent that has finished its answer does not end its
  run: it goes idle and waits for input, so its room still holds an agent lease. A stop expires the
  lease at once (a crash lets it run out after 30 s), and the take-over scan, which runs every 10 s,
  adopts the room. Unless the next bullet applies, it writes an `agent.status` `idle` event with the
  detail "server restarted" and a system message ("the agent is paused; the driver can steer, or anyone
  can mention @agent, to resume"). That covers idle, done, working and queued agents alike
  (`Close` in `internal/room/service.go`; `adoptRoom` in `internal/room/replicas.go`;
  `docs/design/replicas.md` §4.3).
- The rooms that get **no** notice are those whose agent holds no lease: its supervisor gave up
  earlier, or an earlier restart already marked it "server restarted" and nobody has steered since.
  Their agent is simply not running.
- `--demo` uses an in-memory store that starts empty, so no demo room survives a restart.
- The agent runs again when a driver steers, a comment mentions `@agent`, a suggestion is sent, or any
  member confirms or disputes its hypothesis (`internal/room/service_steer.go`,
  `service_hypothesis.go`, `ensureAgentLocked`). The new run starts from the room's original
  alert, not from where the old run stopped.
- **`PIPEROOM_RESUME_ON_TAKEOVER=1` changes this, even with one server.** Then a room whose agent was
  working (or that has unanswered input) resumes by itself after the restart: a system message, a fresh
  run with the room's open proposals listed so it does not propose them again, at most 3 times per room
  per hour. The default is off. This is the only setting that makes a restart resume an agent
  (`adoptRoom`, `resumeAllowedLocked` in `internal/room/replicas.go`; `cmd/server/replicas.go`).

With **several replicas** it works the same way: a room whose server stopped is taken over by another
replica, at the next scan after a graceful stop or when its lease (30 s) runs out after a crash, and the
new server pauses or resumes the agent by the rules above. Failover was not tested under load
(`reviews/capacity-2026-10.md` §9.4). Shadow rooms are never resumed: a run that was in progress is
marked `interrupted` ([shadow-rooms.md](shadow-rooms.md#restarts-and-failures)).

A failing agent run is retried, up to 3 runs in total, with a pause that starts at 1 second and
doubles (`service.go`). Each failure posts an `agent.status` `error` event, and runs wait in
the agent queue when the cap is reached (`service.go`; `PIPEROOM_MAX_AGENTS`, section 1).

A fix that was fully approved but had not finished when the server stopped is **not re-run**. It is
marked failed, but not immediately: a sweep fails a fix once it has been stale long enough (`sweepStaleProposals`,
`recordInterruption` in `internal/room/replicas.go`, two cases below). The service writes one `action.executed` event with
`ok=false` and sets the proposal to failed:

- A fix whose step was already claimed is failed after the claim goes stale: the step timeout (30 s)
  plus 2 minutes. The text is "outcome unknown: the server running this proposal stopped before it
  recorded the result of step N; check the target before you retry with a new proposal". Check the
  target system yourself to see how far the step got, before you ask for a new proposal.
- A fix that was approved but never started is failed after 2 minutes, with "the proposal was approved,
  but the server that should have run it stopped before it started; create a new proposal to retry".
- Only the in-memory `--demo` store uses the older text "proposal was interrupted by a service restart
  before it finished; create a new proposal to retry".

#### Network exposure

The server has two login modes (F1; `docs/api.md` §Login; `docs/design/auth.md`).

- **`oidc`** (`PIPEROOM_AUTH_MODE=oidc`): real single sign-on through your identity provider. Only
  people named in `users.yaml` or `owners.yaml`, or in a viewer group, can log in. This is the mode
  for a TLS reverse proxy and for any wider network. Set it up with
  [deploy/README.md](../../deploy/README.md#single-sign-on-oidc). The server needs no
  `PIPEROOM_DEV_ALLOW_NONLOOPBACK` there (it ignores it and warns).
- **`dev`** (the server's default when `PIPEROOM_AUTH_MODE` is unset; the image and the compose file
  start in `oidc` instead): a person picks a user from `users.yaml` in the UI, with no password. The
  rest of this section is about dev mode.

In dev mode `cmd/server` refuses to listen on any address other than loopback unless
`PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` is set **and** `PIPEROOM_DEV_ACK=dev-login-has-no-password`
states that you know dev login has no password (the old `--allow-remote` flag is a deprecated alias of
the first; `--demo` skips only the acknowledgement and still needs the switch off loopback). Loopback is all of `127.0.0.0/8`, `::1`, and `localhost` in any
letter case. With both set it starts and logs a loud warning (`cmd/server/listen.go`,
`cmd/server/authsetup.go`). Keep `PIPEROOM_HTTP_ADDR` on loopback,
and reach it through an SSH tunnel or a VPN. An authenticating reverse proxy needs
`PIPEROOM_AUTH_MODE=oidc`: dev mode cannot trust a proxy's identity header, and it answers
`403 proxy_identity` to a request that sends `X-User` to the dev login. A pilot that has moved
behind a proxy with `oidc` mode no longer needs the git tag `pre-auth`; only a proxy pilot that
cannot use OIDC yet stays on that tag
([deploy/README.md](../../deploy/README.md#read-this-first-network-exposure)). The switch adds no
authentication. The Docker image does not set it: it starts in `oidc` mode, which needs neither
switch. To run the image in dev mode on purpose, use `deploy/docker-compose.dev-auth.yml` and put the
acknowledgement in `deploy/.env` ([deploy/README.md](../../deploy/README.md)).

#### Supported scale

What one Piperoom replica is proposed to carry, from what was tested. These are floors measured on a shared 4-vCPU VM with
Postgres on the same machine, not limits and not a target. The run on managed Postgres over the network,
which the review calls the most important one, a multi-hour soak and a replica failover under load have
not been done (`reviews/capacity-2026-10.md` §7, §8, §9.4).

> One replica on 4 vCPUs with the default database pool was **tested with up to 60 open rooms** (not a
> limit: more were not tried). The review proposes it **supports** up to **500 connected viewers** in
> total, up to **25 room events per second** across all rooms, `PIPEROOM_MAX_AGENTS` up to **10**, and
> rooms up to **10,000 events**. Each of those figures except the room count is set at about half of
> where something broke first in the review's runs, so they are floors, not limits. Beyond them,
> comments and agent events slow down for every room before anything is dropped.
> (`reviews/capacity-2026-10.md` §7; `docs/pilot/decision.md` §2.3)

A first pilot of one or two pipelines and a handful of engineers is far inside these figures.

- **Replicas do not raise the write rate.** For 2 or 3 replicas the review proposes up to **1,000
  viewers per replica** and the same event write rate as one replica. In its runs, three replicas gave
  179 events/s in total against 151 for one, and each append took 2.5 times longer. Nobody killed a
  replica under load, so failover is unmeasured (§9.2, §9.4). **For the first pilot run one replica**:
  more replicas add an untested failover path and no write capacity.
- **`PIPEROOM_MAX_AGENTS` is a cost and fairness knob, not a server-capacity knob** (§5). The server
  handled 30 concurrent agents in the review's runs; what limits it is the event write path. Runs beyond
  the cap **queue**: the room shows the agent as `queued` ("waiting for an agent slot (N ahead)",
  `docs/api.md`) and nothing is dropped. The cap is per replica. The queue is round-robin across
  pipelines and first-in first-out inside one pipeline (`internal/room/agentslots.go`), so a burst
  from one pipeline does not starve another. That is newer than the review's measurement, which found
  the old first-in first-out queue made other pipelines wait 15 to 17 run lengths (§4.1, §6.5); inside
  one pipeline, the wait for a room in a burst is still about (rooms ahead ÷ cap) × the length of one
  investigation (§5). Shadow runs and watcher-opened rooms take slots from the same cap.
- **The database pool is fixed.** Piperoom's own database pool is pgx's default, `max(4, NumCPU)` per
  process, and it **cannot be set** through `PIPEROOM_DB_URL`: a `pool_max_conns` in the URL stops the
  server at start with `unrecognized configuration parameter` (review §6.3; `migrateRoomDB` in
  `cmd/server/main.go` and the store's listener in `internal/store/hub.go` still call `pgx.ParseConfig`
  on the URL, so it is still open on `main`). Do not put `pool_*` parameters in that URL. With several
  replicas each pod uses about 5 more connections than its pool, so give Postgres `max_connections` for
  replicas × (pool + 5) (`deploy/helm/README.md`).
- **One slow database write slows every room.** The service holds one lock across each event write
  (review §6.1), so a slow disk or a remote database delays all rooms, and opening a room while writes
  queue also waits. Put the room database close to the server.
- **A join storm on a big room can be cut.** Many people opening the same 10,000-event room at once
  hit a 30-second replay limit; reconnecting finishes the replay (review §6.4). The tested case is 20
  people at once.

### 5. Verify with a dry run (no real alert needed)

1. Check the agent can read every system without opening a room:
   `go run ./cmd/investigate` with a test alert for your `pipeline_id`
   (see `tasks/A4-agent-loop.md`). Each of `connect_list_connectors`, `kafka_consumer_lag`,
   `pg_replication_slots`, `warehouse_freshness` should return data. A permission error
   here means a missing grant, so fix the grant, not the code.
2. Start `cmd/server --demo` first to see the flow with no risk, then real mode. Open a room
   (`POST /api/rooms`), open the web UI, and watch the agent work.
3. Confirm **read-only**: as the **source** read-only role (`SOURCE_DB_URL`) run `INSERT` on a
   pipeline table, and as the **warehouse** read-only role (`WAREHOUSE_DB_URL`) run an `INSERT` on a
   sink table. Both must fail. Do not test with the room-database role (`PIPEROOM_DB_URL`); that
   role is meant to write. Same for Kafka produce with the Piperoom principal.
4. Confirm the approval gate: have the agent propose a fix and confirm it does not run
   until every required owner has approved. Reject it, and confirm nothing changed. With write mode
   off (the default) an approved fix never runs at all (section 7).
   In dev mode, dev login is not proof of identity (F1), so the approval check is only as good as
   your network control. In `oidc` mode the approver is the signed-in person.
5. Practise a hand-off between two engineers.
6. If you enabled Slack: check that the room's parent message appears once in the channel, and
   that replies go into its thread.
7. If you run the container: `docker compose` health reports `healthy` once `/healthz` answers
   (`Dockerfile`), and the UI loads from the published port.

### 6. Wire up alerts

Use the signed route when you can: `POST /api/alerts/{source}`, where `{source}` is one of
`alertmanager`, `grafana`, `pagerduty`, `datadog` or `generic` (`internal/alertin/alertin.go`,
`Sources`). The route is registered at `internal/httpapi/server.go`. Each source checks its own
token or HMAC signature, and only a source whose secret is set is accepted (section 1). A failed
check answers 401, and an oversized body 413 (`internal/httpapi/alerts.go`). Repeats and
resolved alerts are handled the same way as the webhook below. The unsigned dev route,
`POST /api/alerts/webhook`, is described next.

Point your alerting system at `POST /api/alerts/webhook` only if you cannot use the signed route
(`internal/httpapi/server.go`, handler `internal/httpapi/handlers.go`). What it does on
`main`:

- **Auth.** In dev mode it needs `X-User: <user id>` of a user in `users.yaml` (or a dev session),
  like every other route outside the signed ones. In oidc mode it is not mounted. **It does not verify the sender.** Nothing checks a signature or a token.
  The signed routes call `internal/alertin` (`internal/alertin/verify.go`); this route does not. Treat the
  endpoint as open to anyone who can reach it and send that header. Keep it reachable only from
  your alerting system, or do not expose it (F9).
- **Body.** Valid JSON, up to 1 MiB (`internal/httpapi/server.go`, `handlers.go`). Piperoom
  reads `title`, `severity` and `pipeline_id` if the body is an object. The whole body is stored
  unchanged in the room's `room.created` event (`internal/room/service_room.go`), so do not
  put secrets in alert payloads. A body that Postgres `jsonb` cannot store gets `400`
  (`internal/room/service_room.go`; `internal/room/jsonb_guard.go`). A room whose creation
  fails is discarded rather than left behind (`service_room.go`).
- **Defaults.** No title becomes "New alert". An unknown severity becomes `sev3`. A missing
  `pipeline_id` becomes `unknown` (`service_room.go`). A `pipeline_id` with characters other
  than letters, digits, `_`, `.`, `-` is rejected with `400` (`service_room.go`).
- **Unknown pipeline.** A missing or unknown pipeline still opens a room. The room gets a system
  message that proposals are disabled (`service_room.go`). Any proposal the agent tries is
  refused (`internal/room/service_proposals.go`). The agent still runs, but its tools are
  scoped to a pipeline, so expect it to find little.
- **De-duplication.** A repeat has the same `dedup_key`, or the same bytes when the body has no
  `dedup_key`, for the same `pipeline_id` (`service_room.go`; signed sources build their own keys, and
the webhook's keys carry a `webhook:` prefix, so a webhook alert never matches a signed one even with
an identical body (`service_room.go`; index key);
  `internal/alertin/parse.go`). If a room is still open for that key, the repeat
  joins it. A system message says the alert fired again, and the response is `200` with that room.
  No second room and no second agent start (`service_room.go`;
  `internal/httpapi/alerts.go`). A new room returns `201`. Closing the room frees the key, so
  the next repeat opens a new room (`service_room.go`). The key is rebuilt after a
  restart (`service.go`; `service_room.go`). If several open rooms hold the same key,
  the first one keeps it (`service_room.go`).
  - **Watch out:** the body alone decides whether a repeat is a repeat. A body with no `dedup_key`
    is a repeat only if it is byte-identical to the first one. If the body has a timestamp or any
    other changing field, every send opens a new room. Put a stable `dedup_key` in the body.
    Your alerting system's own grouping and repeat settings still decide how many requests it
    sends.
- **Resolved alerts.** A body with `"status": "resolved"` that matches an open room is noted in
  that room. The room stays open, because only the driver closes rooms (`service_room.go`).
  A resolved alert with no open match returns `202` with `{"ignored": true, "reason": "resolved alert with no open room"}`.
  It stores nothing and starts no agent (`service_room.go`; `internal/httpapi/alerts.go`).
  A resolved alert **without** a `dedup_key` always takes this path, unless its bytes are identical
  to an open room's first alert. A resolved body is normally not identical, because its `status`
  differs. The generic key is a hash of the body (`internal/alertin/parse.go`).
- **Driver.** The room has no driver until someone takes control (`POST /api/rooms/{id}/control/grant`).

### 6b. Slack (optional, outbound only)

- Turn it on by setting `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` (section 1), then restart. The log
  says "slack bridge enabled" (`cmd/server/slack.go`). With neither set, it logs "slack bridge
  disabled" and nothing else changes.
- Use one **private** channel and add the bot to it. A bot that is not in the channel gets
  `not_in_channel`, and that room's mirror stops (`internal/slackbridge/bridge.go`). A bad
  token, a missing scope or an archived channel also stop the mirror (`bridge.go`).
- Slack only receives events. Nothing in Slack reaches a room: no approvals, no steers
  (`internal/slackbridge/bridge.go`). Approvals happen only in Piperoom.
- Each room gets one thread. After a restart the bridge reads its saved thread state and
  continues from the last event it posted (`bridge.go`; `internal/store/slackthreads.go`).
  A crash between a Slack post and the save can repeat one message (`bridge.go`).
- What it posts, including approver names, the close resolution and the byte limits, is in
  [data-handling.md](data-handling.md#1-where-data-goes).

### 6c. Status of the login design and other features

- **The login design is built** (`docs/design/auth.md`), including auth PR 4 (merged at `8283a58`):
  an approve must send the `params_hash` of the proposal the approver saw (a missing one is `400`,
  a different one `409 params_mismatch`, `internal/room/service_proposals.go`; a reject checks the
  hash the same way); the approval
  event carries the approver's `identity`; and only an approver, a backup or an auditor can export
  (`CheckExportAccess`, `internal/room/access.go`). In dev mode anyone who can reach the API or the
  WebSocket can still pick any user.
- **Room retention and export** are live (section 1). With `PIPEROOM_RETENTION_DAYS` unset or `0`,
  nothing deletes closed rooms by age.
- **Snowflake** is selectable with `WAREHOUSE_KIND=snowflake` (section 1 and
  [permissions.md](permissions.md#warehouse)).
- **ClickHouse** is selectable with `WAREHOUSE_KIND=clickhouse` (section 1 and
  [clickhouse.md](clickhouse.md)).

### 7. Run the pilot

- Start **read-only**: live rooms, with `PIPEROOM_WRITE_MODE` unset or `off` (the default). The agent still
  proposes and people still approve, but a fully approved proposal never runs: the room shows
  `open_proposals[].blocked: "write_mode_off"` and `GET /healthz` reports `"write_mode": "off"`
  (`cmd/server/writemode.go`: `writeModeSetup`; `internal/room/writemode.go`; `internal/httpapi/health.go`).
  Owners make the change by hand. Grant read-only credentials too: the switch stops the executor, it does
  not take write rights away from a role that has them ([decision.md §4](decision.md#4-recommendation-read-only-by-default-shadow-as-a-toggle-then-write-mode)).
- **Shadow is an opt-in toggle per pipeline, not a phase.** With `shadow: {enabled: true}` on a pipeline in
  `owners.yaml`, the agent runs blind on that pipeline's alerts, proposes nothing, and its output is shown
  only after the room closes ([shadow-rooms.md](shadow-rooms.md)). Other pipelines stay live and read-only.
- After the approvers have used read-only rooms, set `PIPEROOM_WRITE_MODE=on` and restart. It is **one
  switch per process**; every replica that shares the room database must use the same value
  (`cmd/server/writemode.go`). Write mode per layer is not built (out of scope in
  `tasks/P1-quickstart-readonly.md`). To limit the first layer, limit the credentials: grant the Kafka
  Connect restart calls first. The exact calls are in [permissions.md](permissions.md#kafka-connect) and
  [permissions.md](permissions.md#schema-registry). `--quickstart` never runs with write mode on
  (`cmd/server/quickstart.go`).
- Start with the cheapest model you trust for triage (section 1a), and keep the token budget on.
  Watch the per-room token use until you know what an investigation costs.
- Keep a list of every wrong or unhelpful agent answer and send it to the Piperoom team.

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `pg_recent_ddl` says the audit log is not installed | trigger not installed, or role lacks `USAGE`/`SELECT` on `piperoom_audit` |
| `kafka_consumer_lag` permission error | missing `Describe` on that consumer group |
| `kafka_topic_config` permission error | missing `DescribeConfigs` on the pipeline's topics (see [permissions.md](permissions.md#kafka)) |
| `pg_row` / `warehouse_row` "table is not in pipeline" | table is missing from `source_tables` / `sink_tables`, or the `pipeline_id` the agent used is not the room's pipeline |
| `connect_config` shows `***` | expected: secret-looking keys are redacted; their values are never shown (`internal/tools/kafkatools/connect_tools.go`) |
| Proposal approved by every layer but nothing runs; the room shows "waiting for write mode" (`blocked: "write_mode_off"`) | `PIPEROOM_WRITE_MODE` is unset or `off`, the default, by design. Set `on` and restart the full server (section 7). It is one switch per process, not per layer (`cmd/server/writemode.go`) |
| Write mode is on, but a fix step fails with a permission error | the Connect or Registry role has no write grant for that call ([permissions.md](permissions.md#kafka-connect)) |
| Server exits at start with "PIPEROOM_WRITE_MODE=... want off or on" | the value is not `off` or `on`; `true` and `1` are refused too (`internal/config/config.go`: `ParseWriteMode`) |
| `--quickstart` exits with "PIPEROOM_WRITE_MODE=on cannot be used with it" | the quickstart is read-only; unset the variable, or run the full server (`cmd/server/quickstart.go`) |
| `--quickstart` exits with "listens on loopback only" | `PIPEROOM_HTTP_ADDR` is not a loopback address; use `127.0.0.1:<port>` (`cmd/server/quickstart.go`: `checkQuickstartAddr`) |
| `--quickstart` exits with "writes a users file only when the owners file names one user" | the owners file names several owners or a group; set `USERS_FILE` to a users file that lists them (`cmd/server/quickstart.go`: `quickstartUsers`) |
| `discover -write-owners` wrote `owners.yaml.draft` and exited 1 | the draft failed `owners.Load`; the error is at the top of the `.draft` file (`cmd/discover/write.go`: `installOwners`) |
| `connect_update_connector_config` fails with a 405 | Connect older than 3.5 does not support `PATCH /connectors/{name}/config` (`connect.go`) |
| Server exits at start with "users/owners config" | `users.yaml` and `owners.yaml` disagree about a layer owner (step 4). The message names the user and layer |
| Server exits at start with "topic_prefix is required", "no source tables", "no connectors" or "sink_ts_column is required" | fix the named pipeline in `owners.yaml` (`internal/owners/owners.go`) |
| Server exits at start with "ping for migrations" | `PIPEROOM_DB_URL` cannot be reached, or the role lacks rights to create tables |
| Server exits at start with "unknown WAREHOUSE_KIND" | `WAREHOUSE_KIND` is not `postgres`, `snowflake` or `clickhouse` (`internal/wiring/warehouse.go`) |
| Server exits at start with "warehouse (snowflake)" | the DSN is wrong, the password is not URL-encoded, or the account host is unreachable. The error is redacted, so check the DSN by hand (`internal/wiring/warehouse.go`) |
| Snowflake sink table reports not found | a missing table, or one the role cannot see, maps to `clients.ErrNotFound`. Names are upper-cased and double-quoted, so a table created with quotes and a mixed case cannot be reached (`internal/clients/snowflake/README.md`, `identifier.go`) |
| Server exits at start with "PIPEROOM_MODEL_PROVIDER" | an unknown provider, or `bedrock` without `AWS_REGION`, or `vertex` without `VERTEX_PROJECT_ID` or `VERTEX_REGION` (`internal/agent/endpoint.go`) |
| Server exits at start with "bedrock needs AWS credentials" or "find Google application default credentials" | no AWS credentials or bearer token, or no Google default credentials on this host (`internal/agent/endpoint.go`) |
| Server exits at start with "PIPEROOM_ROOM_TOKEN_BUDGET" | the value is neither empty, `0`, nor a whole number of at least 1 (`internal/agent/budget.go`) |
| Bedrock rejects the first request with a model error | `PIPEROOM_MODEL` still holds the first-party name; use a Bedrock model or inference-profile id (`internal/wiring/model.go`) |
| A room stops with "token budget" | the room used its `PIPEROOM_ROOM_TOKEN_BUDGET`; raise it or open a new room (`internal/agent/budget.go`) |
| Server exits at start with "slack bridge" | only one of `SLACK_BOT_TOKEN` / `SLACK_CHANNEL_ID` is set, or the channel is a `#name` not an ID |
| Slack thread never appears | bot not in the channel (`not_in_channel` in the log), or the room was opened before Slack was enabled and the server was not restarted |
| A repeat alert opened a second room | the body differs between sends (no `dedup_key`, or a changing field), the `pipeline_id` differs, or the first room was already closed |
| Web UI cannot reach the API (network errors, `421`, `403`) | the UI must be same-origin with the API: use `npm run dev` (Vite proxies `/api` and `/auth`) or the served `PIPEROOM_WEB_DIR`. There is no CORS. A tunnel or proxy that rewrites `Host` needs `PIPEROOM_ALLOWED_HOSTS` |
| Login problems in `oidc` mode (`login_expired`, `not_provisioned`, `idp_unavailable`, `rate_limited`, `403 viewer`, `groups_missing`) | the table in [deploy/README.md](../../deploy/README.md#troubleshooting) has one row for each |
| Web UI shows WebSocket errors | the page's origin host is not the server's own host, the `PIPEROOM_PUBLIC_URL` host, a `PIPEROOM_ALLOWED_HOSTS` entry, or (no web dir, not oidc) `localhost:5173` / `127.0.0.1:5173`. Add the name to `PIPEROOM_ALLOWED_HOSTS`. These checks are browser-only |
| Server exits at start with "index.html" | `PIPEROOM_WEB_DIR` does not point at a built UI (`cmd/server/main.go`) |
| A fix shows "outcome unknown: the server running this proposal stopped..." or "the server that should have run it stopped before it started" | the server stopped mid-fix (the first text can appear about 2.5 minutes later). Check the target system, then re-propose |
| No agent response after a restart | the agent is idle until a driver steers, someone mentions `@agent`, a suggestion is sent, or a member confirms or disputes its hypothesis (step 4) |
| No agent response | the provider's key or credentials are missing, `ANTHROPIC_BASE_URL` points somewhere unexpected (`anthropic` only), or egress to the provider is blocked |
