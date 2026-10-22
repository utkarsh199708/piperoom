# Piperoom pilot onboarding

For the platform/data engineer at a pilot team who will connect Piperoom to a real pipeline.

These docs describe the code on `main` at `a91d2bd`, except the login, identity and role text,
which was checked against `main` at `41a2eea` (auth PR 3). The pilot gate G5 pass (restart behaviour,
login defaults, auth PR 4, shadow rooms, scale, Kafka key decoding) was re-checked against `main` at
`b5bbbc2`. Each claim names the file it comes from. If a file changes, update the claim and its citation
in the same PR.

| Doc | Read it to |
|---|---|
| [quickstart.md](quickstart.md) | get a read-only room on your own pipelines in two commands, before reading anything else |
| [permissions.md](permissions.md) | create **least-privilege** credentials for each system Piperoom touches |
| [clickhouse.md](clickhouse.md) | point the warehouse at ClickHouse: DSN, least-privilege `readonly=2` reader, what works, limits |
| [write-roles.md](write-roles.md) | create the **write** credentials for the P3 fix actions, only if you turn write mode on |
| [setup.md](setup.md) | the ten variables a first install sets, then the reference: prerequisites, configure Piperoom against your pipeline, start the room service and verify it |
| [data-handling.md](data-handling.md) | know exactly what data Piperoom reads, what reaches the model, where it goes (including Slack), and who sees it |
| [shadow-rooms.md](shadow-rooms.md) | run the agent blind on a pipeline's real alerts, then label and grade it after the incident, before anyone depends on it |
| [decision.md](decision.md) | see how the first pilot is proposed to run, and the go-live gates |

## Read this first: pilot limits

Piperoom is pre-1.0. The list below is the status from the code, not from older notes.

### Known gaps (F-list)

`docs/security/findings.md` is out of date with `main`. It still lists F2, F3, F4, F5, F8 and F10
as findings with no fix recorded. It is owned by another task and was not edited here. The status
below is from the code.

| ID | Status | What the code does now | Where |
|---|---|---|---|
| F1 | **fixed with OIDC; open in dev mode** | Two modes. **`oidc`** (`PIPEROOM_AUTH_MODE=oidc`): people sign in at your identity provider (authorization code with PKCE and a nonce); the server keeps its own session; only people named in `users.yaml` or `owners.yaml`, or in a viewer group, can log in; `X-User` is ignored; a TLS reverse proxy is supported. **`dev`** (the server's own default when `PIPEROOM_AUTH_MODE` is unset, `internal/config/config.go`; the Docker image and the compose file start in `oidc` instead, `Dockerfile`, `deploy/docker-compose.pilot.yml`): a person picks a user from `users.yaml` in the web app, and the server sets a session cookie (`POST /auth/dev-login`). There is no password. `curl` may still send `X-User` when it sends no cookie. In dev mode any client that can reach the API can act as any user, including a layer owner. The Host check, the cross-origin check and the WebSocket origin check stop browsers and DNS rebinding only; a non-browser client is not blocked by them. Setup is in [deploy/README.md](../../deploy/README.md#single-sign-on-oidc). | `internal/httpapi/auth.go`; `internal/httpapi/auth_oidc.go`; `internal/auth/`; `internal/ws/gateway.go`; design `docs/design/auth.md` |
| F2 | **fixed in the UI** | The approval view shows each step's `params` as key/value rows, one row per field. Raw JSON is shown only when `params` is not an object. Read the params before you approve. | `web/src/components/ProposalsPanel.tsx`; proposal step updates in `internal/store/store.go` change only status and output |
| F3 | **fixed** for `pg_row` and `warehouse_row` | `pg_row` keeps key and timestamp values; other columns are names with `null` values. `warehouse_row` keeps only timestamp-named columns and drops the rest, **including the key**. A column counts as a timestamp by name only: `at` or any name ending in `_at`, not by type. Only these two tools were checked. | `internal/tools/dbtools/pg.go`; `wh.go`; `dbtools.go`; the same column rule applies on Snowflake, where names come back lower-cased (`internal/clients/snowflake/snowflake.go`) |
| F4 | **fixed** (with F11) | Row tools accept only tables listed in the `source_tables` / `sink_tables` of the room's pipeline. The `pipeline_id` in a tool input must be the room's own pipeline, or the tool refuses. | `internal/tools/dbtools/pg.go`, `wh.go`, `dbtools.go` (`resolve`, `scopedTable`); `internal/tools/scope.go` |
| F5 | **fixed** | Integration tests print only host and port, not the DSN. Test code only. | `internal/clients/postgres/integration_test.go`; `internal/clients/warehouse/integration_test.go` |
| F6 | **closed** | Retention and export are live: `PIPEROOM_RETENTION_DAYS` deletes whole closed rooms older than N days, checked every 6 hours; an empty value or `0` keeps everything; `GET /api/rooms/{id}/export` saves a room first. A room's log is capped by `PIPEROOM_ROOM_MAX_EVENTS` (default 20000) and `PIPEROOM_ROOM_MAX_BYTES` (default 256 MiB), and with `PIPEROOM_ARCHIVE_DIR` set retention, and an admin delete asked to (`archive=true`, or `devctl rooms delete --archive`), write the room's export first. Details are in [data-handling.md](data-handling.md#4-retention-and-deletion). | `cmd/server/retention.go`; `internal/store/retention.go`; `internal/room/service_export.go`; the orphan-room delete is `internal/store/store.go` |
| F7 | **fixed by `connections:`** | A pipeline in `owners.yaml` may have its own `connections:` block. It names the environment variable or file that holds each DSN, URL and login (a plain secret value is refused). That pipeline gets its own clients, tools and actions and takes nothing from the process-wide `SOURCE_DB_URL`, `KAFKA_*` and so on. A pipeline without the block uses the process-wide settings, as before. The start fails when two tenants use the same variable or file, unless every pipeline involved sets `shared_connections: true`. One process still holds the secrets of every pipeline it serves, so separate customers still need one deployment each. Setup: [setup.md](setup.md#2a-tenancy). | `internal/owners/tenancy.go` (`Connections`, `ConnRef.UnmarshalYAML`, `validateConnections`, `Pipeline.HasConnections`, `Owners.CheckSharedRefs`); `internal/wiring/connections.go` (`pipelineConfig`); `internal/wiring/stacks.go` (`openStacks`); design `docs/design/tenancy.md` |
| F8 | **fixed** | The dev terminal sink removes control characters (including DEL and C1) and caps printed size. Dev CLI only, not the room service. | `cmd/investigate/sink.go` |
| F9 | **partly fixed** | The signed routes `POST /api/alerts/{source}` check the sender with a token or HMAC, and a source with no secret answers 401. The unsigned dev route `POST /api/alerts/webhook` still has no sender check: in dev mode anyone who can reach it and sends the `X-User` of a user in `users.yaml` (or has a dev session) can open rooms through it. In oidc mode it is not mounted (`404`); use the signed `/api/alerts/generic`. | `internal/httpapi/alerts.go`; `internal/alertin/verify.go`; `internal/httpapi/handlers.go`; `internal/httpapi/server.go` |
| F10 | **fixed** for write actions | Every write action is scope-checked against the room's pipeline when the proposal is created and again right before it runs. | `internal/room/service_proposals.go`; `service_executor.go`; `internal/actions/scope_test.go` |
| F11 | **fixed** | Every read tool is scoped to the room's pipeline. The runner takes the pipeline from the alert, never from the model, and a tool refuses a `pipeline_id`, connector, topic, consumer group, subject or table outside it before any client call. List tools return only the pipeline's own items. A room with no pipeline gets no reads (only `runbook_lookup` works). `connect_config` shows only `type`/`field` settings of transforms; `pg_long_transactions` returns no query text. Since T2, `pg_replication_slots` and `pg_long_transactions` are scoped too, with other objects only counted (`docs/security/tool-scope.md`). | `internal/tools/scope.go`; `internal/agent/run.go` (`toolScope`); `internal/tools/kafkatools/*`; `internal/tools/dbtools/*`; `cmd/investigate/scope_guard_test.go` |

### Other limits

- **Real login is OIDC.** The design is `docs/design/auth.md`. The Docker image and the compose file
  start in `oidc` mode and stop at start-up, naming the variable, until the OIDC settings are given
  (`Dockerfile`; [deploy/README.md](../../deploy/README.md#single-sign-on-oidc)). The image has no dev
  auth baked in. A server started without `PIPEROOM_AUTH_MODE` runs in `dev` mode: a person picks a
  user in the UI and there is no password. In dev mode keep the HTTP and WebSocket ports on loopback,
  a VPN or an SSH tunnel; off loopback it also needs `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` and
  `PIPEROOM_DEV_ACK=dev-login-has-no-password` (`cmd/server/authsetup.go`). A reverse proxy that does the
  authenticating needs `oidc` mode; once a proxy pilot has switched to it, it no longer needs the
  git tag `pre-auth`. In `oidc` mode who may log in and what they may do comes from `users.yaml`,
  `owners.yaml` and `PIPEROOM_AUTH_VIEWER_GROUPS`
  ([Identity and rights](../../deploy/README.md#identity-and-rights)). See
  [setup.md](setup.md#network-exposure).
- **Slack is wired, outbound only.** Room events are mirrored to one thread per room in one
  Slack channel. Nothing typed in Slack reaches a room, and approvals happen only in Piperoom.
  It is on only when both `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` are set. Slack can repeat one
  message after a crash. See [data-handling.md](data-handling.md#1-where-data-goes).
- **The alert webhook de-duplicates while a room is open.** The same alert (same `dedup_key`, or a
  byte-identical body) for the same pipeline joins the open room and does not start a second
  agent. A resolved alert with no open room is accepted with `202` and opens nothing. See
  [setup.md](setup.md#6-wire-up-alerts).
- **Write actions: six.** Four of them write to Kafka Connect and two to the Schema Registry. Each
  one is listed with its exact HTTP call in [permissions.md](permissions.md#kafka-connect).
- **Warehouse is Postgres by default; `WAREHOUSE_KIND` selects another.** The wiring accepts
  `postgres` (the default), `snowflake`, `bigquery`, `databricks` or `clickhouse`
  (`internal/wiring/warehouse.go`). The Snowflake client is selectable on `main`. `WAREHOUSE_DB_URL`
  is then a gosnowflake DSN with a URL-encoded password. Startup errors are redacted (`warehouse.go`).
  Read-only access comes only from the DSN role (`internal/clients/snowflake/snowflake.go`). The
  ClickHouse client is also selectable (`WAREHOUSE_KIND=clickhouse`); see
  [clickhouse.md](clickhouse.md). See [permissions.md](permissions.md#warehouse).
- **Retention is live.** `PIPEROOM_RETENTION_DAYS` deletes closed rooms older than N days, checked
  every 6 hours; an empty value or `0` keeps everything. Export a room first with
  `GET /api/rooms/{id}/export`. Also on `main`: the Docker image and the served web UI, the signed alert routes,
  the agent cap, the model providers, the triage model and the per-room token budget. The built-in
  watcher (`PIPEROOM_WATCH_*`, design in `docs/design/watcher.md`) is live; the default mode `log` only writes log lines.
  That is not [shadow rooms](shadow-rooms.md), which are a separate, per-pipeline feature. See
  [setup.md](setup.md#1-configure-the-environment).
- **A restart does not resume the agent, by default.** A room whose agent was working when the server
  stopped comes back idle with a "server restarted" message, and so does a room whose agent had finished
  (with Postgres, any open room that still holds an agent lease; shadow rooms are excepted, their run is marked `interrupted`; the other exceptions are in setup.md). A
  driver's steer, an `@agent` mention, a sent suggestion or a hypothesis verdict starts the agent again. Only
  `PIPEROOM_RESUME_ON_TAKEOVER=1` makes a working agent resume by itself, and it does so after a plain
  restart of one server too (`Load`, `adoptRoom` in `internal/room`). A fix that was approved but not
  finished is marked failed, not re-run. See [setup.md](setup.md#what-happens-on-a-restart).
- **Scale.** One replica was tested with up to 60 open rooms and is proposed to support 500 viewers and
  25 events per second (floors, about half of where something broke). More replicas do not raise the
  write rate. See [setup.md](setup.md#supported-scale).
- **Kafka keys are decoded with an Avro library that has open advisories.** See
  [permissions.md](permissions.md#kafka) and [data-handling.md](data-handling.md#3-prompt-injection-and-untrusted-text).
- **Listen address is checked.** In dev auth mode the server refuses any non-loopback
  `PIPEROOM_HTTP_ADDR` unless `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` is set (the old `--allow-remote`
  flag is a deprecated alias). Loopback means all of `127.0.0.0/8`, `::1`, or `localhost` in
  any letter case. See [setup.md](setup.md#network-exposure).
- **Redaction is partial.** `pg_row` keeps key and timestamp values; `warehouse_row` keeps only
  timestamp values and drops the key (F3). A timestamp column is one named `at` or ending in `_at`,
  whatever its type (`dbtools.go`). Other tools return their values as described in
  [data-handling.md](data-handling.md#2-what-each-tool-exposes). Table, topic and column names,
  and timestamps, still appear in the room.
- **Write mode is off by default.** With `PIPEROOM_WRITE_MODE` unset or `off`, the agent still
  proposes fixes and people still approve them, but a fully approved fix never runs and no
  `action.executed` is written. The room view marks it `blocked: "write_mode_off"` and `/healthz`
  reports `"write_mode": "off"`. Any value other than `off` or `on` stops the start. `--demo` forces
  `on` (its actions use fake clients). The switch is one per process. Grant read-only credentials
  too: the switch stops the executor, not the credentials. Code: `internal/config/config.go`
  (`ParseWriteMode`); `cmd/server/writemode.go` (`writeModeSetup`); `internal/room/service_executor.go`
  (`enqueueExecutionLocked`, `executeProposal`); `internal/room/writemode.go` (`RoomFor`);
  `internal/httpapi/health.go` (`handleHealthz`).
- **Credentials per pipeline (F7).** Give each team's pipelines their own `connections:` block
  (`internal/owners/tenancy.go`). Pipelines without it share the process-wide settings. One process
  still holds every secret it is given: run one deployment per customer.

If any of these is a blocker for your environment, start with a non-production replica or a
staging pipeline. Check the code on your release before you trust a row in the table above.
