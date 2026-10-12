# Data handling

What Piperoom reads from your systems, where it goes, who can see it, and how to limit it.
Based on the code on `main` at `d63b72c`. Under `cmd/investigate` events are printed to the
terminal; under `cmd/server` they go to the room event log and, if Slack is configured, to a
Slack thread.

## 1. Where data goes

```
your systems ──read-only tools──► Summary (text) ──► Claude API   (agent's view)
                    │
                    ├───────────► Data (structured) ──► room event log ──► every room member
                    │                                   (permanent, append-only)
                    │
                    └───────────► selected room events ──► Slack thread   (only if configured)
```

- **Claude API (Anthropic).** The agent sees each tool's `Summary` text, the alert, and
  human messages in the room. Anything in those is sent to the Anthropic API. Review
  Anthropic's current commercial terms and data-retention settings for your organisation
  before the pilot; Piperoom does not change them.
- **Room event log (`room_events`, `migrations/00001_init.sql:16`).** Every tool call, result `Data`,
  comment, steer, proposal and approval is stored **permanently** in the Piperoom database and shown
  to everyone who can open the room. There is no per-event redaction after the fact. Redaction
  happens before the event is written: `pg_row` and `warehouse_row` return redacted `Data`
  (`internal/tools/dbtools/pg.go:191`, `wh.go:111`).
- **Slack (optional, outbound only).** Off unless **both** `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID`
  are set. A half setting stops the server at start (`cmd/server/main.go:128-132`;
  `internal/slackbridge/bridge.go:63-69`). It posts into one thread per room in one channel
  (`internal/slackbridge/bridge.go:1-8`). Nothing typed in Slack reaches a room.
  - **Posted:** the room title, severity, pipeline id and room id, plus a room link if
    `PIPEROOM_PUBLIC_URL` is set (`internal/slackbridge/render.go:78-92`). Hypotheses (text up to
    1500 characters). Each proposal's title, risk and rollback, and up to 10 steps with action name,
    layer, description and `params` (params up to 600 characters) (`render.go:131-165`). Approvals and
    rejections with their reason. Action results (output up to 500 characters). Hand-off summaries.
    The close resolution. Only these event types are posted (`render.go:24-32`).
  - **Not posted:** the raw alert body (`render.go:85`), tool results and their `Data`, comments and
    steers.
  - Slack keeps its copies under its own retention, not Piperoom's. Thread state is saved in the
    room database table `slack_threads` (`migrations/00003_slack_threads.sql`).
  - Everyone in the Slack channel can see every room mirrored into it. Use a **private** channel
    (`bridge.go:4-5`).
- **Logs.** Piperoom logs with `log/slog`; it is designed not to log secrets (the Anthropic
  key, DSN passwords) or full record values. Report any place that does.

## 2. What each tool exposes

| Tool | Exposes to the agent and the room | Does **not** expose |
|---|---|---|
| `pg_replication_slots` | slot name, plugin, active flag, WAL retained | data |
| `pg_recent_ddl` | DDL command, object, **full statement text**, user, time | row data |
| `pg_long_transactions` | pid, user, state, age, first 200 chars of query *if the role may see it* | |
| `pg_row` | row **column names**; values only for the key and timestamp columns (others are `null`) (`pg.go:191,199`) | other column values |
| `warehouse_freshness` | max timestamp of the sink table, lag | row data |
| `warehouse_row` | timestamp column values only; other columns are names with `null` (`wh.go:111,119`) | all other column values |
| `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag` | topic/group names, offsets, lag | record payloads |
| `kafka_find_by_key` | topic, partition, offset, timestamp and **key** of matching records | record **values** |
| `trace_record` | per-stage status and facts for one `(table, key)` | row values |
| `connect_*` | connector state, task errors (**stack traces can contain data**), and a fixed list of config keys (`connect_tools.go:155-170`); keys containing `password`/`secret`/`token`/`.key` are shown as `***` | secrets stored in those config keys |
| `schema_registry_*` | subjects, versions, schema definitions and diffs | data |

Things to know:

- **Keys are shown.** If your primary key or Kafka key is personal data (an email, a phone
  number), it will appear in the room. Prefer pipelines keyed by surrogate ids for the pilot.
- **Statement text and error traces are free text.** DDL statements and Connect task errors
  can contain literal values. Check yours (for example, `CREATE ROLE ... PASSWORD` in the
  DDL audit log, or a deserialization error that echoes a record).
- **Human input goes to the model.** Anything engineers type in comments and steers is sent to
  the Anthropic API and stored. Tell the team not to paste secrets or customer data.
- **Alert payloads are stored verbatim** in `room.created.alert` (`internal/room/service_room.go:137`).
  The agent's first message uses only the mapped title/severity/pipeline, and Slack never renders
  the raw alert (`render.go:85`), but the whole body is saved and shown in the room.

## 3. Prompt injection and untrusted text

Record keys, error traces, log lines and comments are attacker-influenced text. Piperoom
wraps tool output and non-driver human input in `untrusted` markers with escaping, and
tells the model to treat them as data. That reduces risk but is not a guarantee. The
controls that do not depend on the model are:

1. **Read-only credentials** ([permissions.md](permissions.md)): even a fooled agent cannot write.
2. **Human approval:** the agent can only *propose* a fix. It runs only after every required
   layer owner approves. Read the exact `params` of a proposal, not just its description (F2).
   Every write action is also checked against the room's pipeline before it runs (F10,
   `internal/room/service_executor.go:211`).
3. **Table scoping:** `pg_row` and `warehouse_row` read only tables that are listed for the
   `pipeline_id` the agent passes in (`pg.go:177`, `wh.go:97`). That pipeline is **not** checked
   against the room's pipeline, so a room can read the listed tables of any pipeline in
   `owners.yaml` (F4, still partly open). Keep `owners.yaml` to the pipelines the room may see.

## 4. Retention and deletion

- The event log has **no retention policy or size cap** (F6). No code deletes `room_events` or
  `rooms` (`internal/store/store.go`). Decide how long you keep closed rooms and delete them at the
  database level; there is no UI for deletion.
- Deleting the Piperoom database deletes the room history, the Slack thread state and everything
  else in it. Data sent to the Anthropic API, and messages already posted to Slack, are governed by
  your agreements with those providers, not by Piperoom.
- Back up and encrypt the room database like any other store that holds incident detail.

## 5. Controls you should apply

- Run on staging or replicas first; use pipelines without regulated data (card numbers,
  health data, government ids) for the pilot unless your security team signs off.
- Keep `source_tables` / `sink_tables` as small as possible and grant `SELECT` only on them.
- Pilot with a small, named group in `users.yaml`: everyone in a room sees everything in it.
- Turn Slack on only with a **private** channel whose members may see every room mirrored into it.
  Leave `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` unset if they may not.
- Keep the HTTP port on loopback or behind your VPN ([setup.md](setup.md#network-exposure)).
- Use a dedicated Anthropic key with a spend limit; rotate it at the end of the pilot.
- At the end of the pilot, revoke all Piperoom credentials, remove the Slack bot, and delete the
  room database.

## 6. Questions to answer before go-live

- [ ] Is it allowed to send the fields in sections 1 and 2 (keys, statement text, error traces, proposal params, hypotheses) to the Anthropic API?
- [ ] Is it allowed to post proposals, params and hypotheses to a Slack channel, and who is in that channel?
- [ ] Who may view incident history, and for how long is it kept?
- [ ] Who are the approvers per layer, and are they reachable out of hours?
- [ ] How do we revoke Piperoom's access quickly (one place per system, and the Slack bot)?
