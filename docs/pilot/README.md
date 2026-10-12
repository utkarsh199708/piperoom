# Piperoom pilot onboarding

For the platform/data engineer at a pilot team who will connect Piperoom to a real pipeline.

These docs describe the code on `main` at `2a7bc34`. Each claim names the file it comes from.
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
| F1 | **open** | Identity is the `X-User` header on HTTP, checked only against `users.yaml`. The WebSocket takes the same thing as `?user=` with no other check, so the same weakness applies there. Any client that can reach the API can act as any user, including a layer owner. The web app still renders a user switcher. The CORS and WebSocket origin checks stop browsers only; a non-browser client is not blocked by them. | `internal/httpapi/server.go:145-160,171`; `internal/ws/gateway.go:156-160,180`; `web/src/components/RoomList.tsx:70`, `RoomScreen.tsx:89,136` |
| F2 | **fixed in the UI** | The approval view shows each step's `params` as key/value rows, one row per field. Raw JSON is shown only when `params` is not an object. Read the params before you approve. | `web/src/components/ProposalsPanel.tsx:22-60,146`; proposal step updates in `internal/store/store.go:517,534` change only status and output |
| F3 | **fixed** for `pg_row` and `warehouse_row` | `pg_row` keeps key and timestamp values; other columns are names with `null` values. `warehouse_row` keeps only timestamp-named columns and drops the rest, **including the key**. A column counts as a timestamp by name only: `at` or any name ending in `_at`, not by type. Only these two tools were checked. | `internal/tools/dbtools/pg.go:191,199`; `wh.go:111,119-127`; `dbtools.go:177-179` |
| F4 | **partly fixed** | Row tools accept only tables listed in the `source_tables` / `sink_tables` of the pipeline the agent names. That pipeline is **not** checked against the room's pipeline, so any pipeline in `owners.yaml` can be read from any room. Other reads have the same gap: `trace_record`, `pipeline_map`, `pg_publication_tables` and `kafka_topic_config` take an agent-chosen `pipeline_id` that nothing checks against the room. The `kafka_*`, `connect_status`, `connect_config` and `schema_registry_*` tools take free names (topic, group, connector, subject) with no scoping in code. Only your credentials and ACLs limit these reads. | `internal/tools/dbtools/pg.go:177`; `wh.go:97`; `dbtools.go:55-60`; `map.go:19,49`; `pg_publication.go:46-50`; `internal/tools/kafkatools/kafka_tools.go:25,79,140,184,245-261`; `connect_tools.go:74,135`; `registry_tools.go:20,55,102` |
| F5 | **fixed** | Integration tests print only host and port, not the DSN. Test code only. | `internal/clients/postgres/integration_test.go:21-24,34,46`; `internal/clients/warehouse/integration_test.go:20,33,45` |
| F6 | **open** | No retention, no event cap, no archive. No code path deletes `room_events` or `rooms`. | `internal/store/store.go` (the only DELETEs are on proposals, approvals, suggestions and handoffs: lines 431, 485, 560, 615) |
| F7 | **open** | One set of connection settings (databases, Kafka, Connect, Registry) per process, shared by every pipeline in `owners.yaml`. | `internal/config/config.go:9-52`; `internal/wiring/wiring.go:133-180` |
| F8 | **fixed** | The dev terminal sink removes control characters (including DEL and C1) and caps printed size. Dev CLI only, not the room service. | `cmd/investigate/sink.go:69-98` |
| F9 | **open** | The alert webhook has no sender check: no signature, no token. Anyone who sends the `X-User` of a user in `users.yaml` can open rooms. `alertin.Verify` is never called. | `internal/httpapi/handlers.go:50-92`; `internal/alertin/verify.go:109-115` |
| F10 | **fixed** for write actions | Every write action is scope-checked against the room's pipeline when the proposal is created and again right before it runs. | `internal/room/service_proposals.go:94`; `service_executor.go:211,315-330`; `internal/actions/scope_test.go` |

### Other limits

- **Real login is not built.** The design docs named in `docs/api.md` (`docs/design/auth.md`,
  `docs/design/webhook-auth.md`) are not in the repo on `main`. Until login exists, keep the HTTP
  and WebSocket ports on loopback or behind a proxy that does real authentication. See
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
- **Warehouse is Postgres only today.** A Snowflake client exists in `internal/clients/snowflake`,
  but the service cannot select it yet. A PR is in progress. No date is promised. See
  [permissions.md](permissions.md#warehouse).
- **A restart does not resume the agent.** Open rooms come back with the agent idle. A driver's
  steer, an `@agent` mention or a sent suggestion starts it again. A fix that was approved but not
  finished is marked failed, not re-run. See [setup.md](setup.md#what-happens-on-a-restart).
- **Listen address is checked.** The server refuses any non-loopback `PIPEROOM_HTTP_ADDR` unless it
  is started with `--allow-remote`. Loopback means all of `127.0.0.0/8`, `::1`, or `localhost` in
  any letter case. See [setup.md](setup.md#network-exposure).
- **Redaction is partial.** `pg_row` keeps key and timestamp values; `warehouse_row` keeps only
  timestamp values and drops the key (F3). A timestamp column is one named `at` or ending in `_at`,
  whatever its type (`dbtools.go:177-179`). Other tools return their values as described in
  [data-handling.md](data-handling.md#2-what-each-tool-exposes). Table, topic and column names,
  and timestamps, still appear in the room.
- **One credential set per process (F7).** Run one Piperoom process per team or pipeline group.
  Do not share one process between teams with different data access.

If any of these is a blocker for your environment, start with a non-production replica or a
staging pipeline. Check the code on your release before you trust a row in the table above.
