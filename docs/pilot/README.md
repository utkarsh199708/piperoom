# Piperoom pilot onboarding

For the platform/data engineer at a pilot team who will connect Piperoom to a real pipeline.

These docs describe the code on `main` at `a91d2bd`, except the login, identity and role text,
which was checked against `main` at `41a2eea` (auth PR 3). Each claim names the file it comes from.
If a file changes, update the claim and its citation in the same PR.

| Doc | Read it to |
|---|---|
| [permissions.md](permissions.md) | create **least-privilege** credentials for each system Piperoom touches |
| [setup.md](setup.md) | prerequisites, configure Piperoom against your pipeline, start the room service and verify it |
| [data-handling.md](data-handling.md) | know exactly what data Piperoom reads, what reaches the model, where it goes (including Slack), and who sees it |

## Read this first: pilot limits

Piperoom is pre-1.0. The list below is the status from the code, not from older notes.

### Known gaps (F-list)

`docs/security/findings.md` is out of date with `main`. It still lists F2, F3, F4, F5, F8 and F10
as findings with no fix recorded. It is owned by another task and was not edited here. The status
below is from the code.

| ID | Status | What the code does now | Where |
|---|---|---|---|
| F1 | **fixed with OIDC; open in dev mode** | Two modes. **`oidc`** (`PIPEROOM_AUTH_MODE=oidc`): people sign in at your identity provider (authorization code with PKCE and a nonce); the server keeps its own session; only people named in `users.yaml` or `owners.yaml`, or in a viewer group, can log in; `X-User` is ignored; a TLS reverse proxy is supported. **`dev`** (the default, and what the image starts in): a person picks a user from `users.yaml` in the web app, and the server sets a session cookie (`POST /auth/dev-login`). There is no password. `curl` may still send `X-User` when it sends no cookie. In dev mode any client that can reach the API can act as any user, including a layer owner. The Host check, the cross-origin check and the WebSocket origin check stop browsers and DNS rebinding only; a non-browser client is not blocked by them. Setup is in [deploy/README.md](../../deploy/README.md#single-sign-on-oidc). | `internal/httpapi/auth.go`; `internal/httpapi/auth_oidc.go`; `internal/auth/`; `internal/ws/gateway.go`; design `docs/design/auth.md` |
| F2 | **fixed in the UI** | The approval view shows each step's `params` as key/value rows, one row per field. Raw JSON is shown only when `params` is not an object. Read the params before you approve. | `web/src/components/ProposalsPanel.tsx:22-54,146`; proposal step updates in `internal/store/store.go:544,561` change only status and output |
| F3 | **fixed** for `pg_row` and `warehouse_row` | `pg_row` keeps key and timestamp values; other columns are names with `null` values. `warehouse_row` keeps only timestamp-named columns and drops the rest, **including the key**. A column counts as a timestamp by name only: `at` or any name ending in `_at`, not by type. Only these two tools were checked. | `internal/tools/dbtools/pg.go:264,272-282`; `wh.go:124,132-140`; `dbtools.go:242-244`; the same column rule applies on Snowflake, where names come back lower-cased (`internal/clients/snowflake/snowflake.go:148`) |
| F4 | **fixed** (with F11) | Row tools accept only tables listed in the `source_tables` / `sink_tables` of the room's pipeline. The `pipeline_id` in a tool input must be the room's own pipeline, or the tool refuses. | `internal/tools/dbtools/pg.go`, `wh.go`, `dbtools.go` (`resolve`, `scopedTable`); `internal/tools/scope.go` |
| F5 | **fixed** | Integration tests print only host and port, not the DSN. Test code only. | `internal/clients/postgres/integration_test.go:21-24,34,46`; `internal/clients/warehouse/integration_test.go:20,33,45` |
| F6 | **partly closed** | Retention and export are live: `PIPEROOM_RETENTION_DAYS` deletes whole closed rooms older than N days, checked every 6 hours; an empty value or `0` keeps everything; `GET /api/rooms/{id}/export` saves a room first. Still open: there is no event cap or size cap on a room's event log, and nothing archives a room except that export. | `cmd/server/retention.go`; `internal/store/retention.go`; `internal/room/service_export.go`; the orphan-room delete is `internal/store/store.go:335` |
| F7 | **open** | One set of connection settings (databases, Kafka, Connect, Registry) per process, shared by every pipeline in `owners.yaml`. | `internal/config/config.go:14-104`; `internal/wiring/wiring.go:135-193` |
| F8 | **fixed** | The dev terminal sink removes control characters (including DEL and C1) and caps printed size. Dev CLI only, not the room service. | `cmd/investigate/sink.go:69-98` |
| F9 | **partly fixed** | The signed routes `POST /api/alerts/{source}` check the sender with a token or HMAC, and a source with no secret answers 401. The unsigned dev route `POST /api/alerts/webhook` still has no sender check: in dev mode anyone who can reach it and sends the `X-User` of a user in `users.yaml` (or has a dev session) can open rooms through it. In oidc mode it is not mounted (`404`); use the signed `/api/alerts/generic`. | `internal/httpapi/alerts.go:31-41`; `internal/alertin/verify.go:90-102,109-115`; `internal/httpapi/handlers.go:48-68`; `internal/httpapi/server.go:133` |
| F10 | **fixed** for write actions | Every write action is scope-checked against the room's pipeline when the proposal is created and again right before it runs. | `internal/room/service_proposals.go:94`; `service_executor.go:211,315-330`; `internal/actions/scope_test.go` |
| F11 | **fixed** | Every read tool is scoped to the room's pipeline. The runner takes the pipeline from the alert, never from the model, and a tool refuses a `pipeline_id`, connector, topic, consumer group, subject or table outside it before any client call. List tools return only the pipeline's own items. A room with no pipeline gets no reads (only `runbook_lookup` works). `connect_config` shows only `type`/`field` settings of transforms; `pg_long_transactions` returns no query text. Since T2, `pg_replication_slots` and `pg_long_transactions` are scoped too, with other objects only counted (`docs/security/tool-scope.md`). | `internal/tools/scope.go`; `internal/agent/run.go` (`toolScope`); `internal/tools/kafkatools/*`; `internal/tools/dbtools/*`; `cmd/investigate/scope_guard_test.go` |

### Other limits

- **Real login is OIDC, and it is opt-in.** The design is `docs/design/auth.md`. The image starts
  in dev auth mode: a person picks a user in the UI and there is no password. In dev mode keep
  the HTTP and WebSocket ports on loopback, a VPN or an SSH tunnel. For anything more, set
  `PIPEROOM_AUTH_MODE=oidc` and connect your identity provider
  ([deploy/README.md](../../deploy/README.md#single-sign-on-oidc)). A reverse proxy that does the
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
- **Warehouse is Postgres by default, or Snowflake with `WAREHOUSE_KIND=snowflake`.** The Snowflake
  client is selectable on `main` (`internal/wiring/warehouse.go:50-83`). `WAREHOUSE_DB_URL` is then a
  gosnowflake DSN with a URL-encoded password. Startup errors are redacted (`warehouse.go:74`).
  Read-only access comes only from the DSN role (`internal/clients/snowflake/snowflake.go:6-10`).
  See [permissions.md](permissions.md#warehouse).
- **Retention is live.** `PIPEROOM_RETENTION_DAYS` deletes closed rooms older than N days, checked
  every 6 hours; an empty value or `0` keeps everything. Export a room first with
  `GET /api/rooms/{id}/export`. Also on `main`: the Docker image and the served web UI, the signed alert routes,
  the agent cap, the model providers, the triage model and the per-room token budget. The built-in
  watcher (`PIPEROOM_WATCH_*`, design in `docs/design/watcher.md`) is live; the default mode `log` only writes log lines. See
  [setup.md](setup.md#1-configure-the-environment).
- **A restart does not resume the agent.** Open rooms come back with the agent idle. A driver's
  steer, an `@agent` mention or a sent suggestion starts it again. A fix that was approved but not
  finished is marked failed, not re-run. See [setup.md](setup.md#what-happens-on-a-restart).
- **Listen address is checked.** In dev auth mode the server refuses any non-loopback
  `PIPEROOM_HTTP_ADDR` unless `PIPEROOM_DEV_ALLOW_NONLOOPBACK=1` is set (the old `--allow-remote`
  flag is a deprecated alias). Loopback means all of `127.0.0.0/8`, `::1`, or `localhost` in
  any letter case. See [setup.md](setup.md#network-exposure).
- **Redaction is partial.** `pg_row` keeps key and timestamp values; `warehouse_row` keeps only
  timestamp values and drops the key (F3). A timestamp column is one named `at` or ending in `_at`,
  whatever its type (`dbtools.go:242-244`). Other tools return their values as described in
  [data-handling.md](data-handling.md#2-what-each-tool-exposes). Table, topic and column names,
  and timestamps, still appear in the room.
- **One credential set per process (F7).** Run one Piperoom process per team or pipeline group.
  Do not share one process between teams with different data access.

If any of these is a blocker for your environment, start with a non-production replica or a
staging pipeline. Check the code on your release before you trust a row in the table above.
