# Data handling

What Piperoom reads from your systems, what reaches the model, where it goes, who can see it, and
how to limit it. Based on the code on `main` at `d372943`. Under `cmd/investigate` events are
printed to the terminal. Under `cmd/server` they go to the room event log and, if Slack is
configured, to a Slack thread.

## 1. Where data goes

```
your systems ──read-only tools──► Summary (text) ──► Claude API   (agent's view)
                    │
                    ├───────────► Data (structured) ──► room event log ──► every room member
                    │                                   (permanent, append-only)
                    │
                    └───────────► selected room events ──► Slack thread   (only if configured)
```

- **Claude API (Anthropic).** Everything in the next section goes to the Anthropic API. Review
  Anthropic's current commercial terms and data-retention settings for your organisation before
  the pilot. Piperoom does not change them.
  - **Where the requests go.** The Anthropic SDK reads `ANTHROPIC_BASE_URL` from the environment
    and sends every request there (`anthropic-sdk-go@v1.80.0/client.go:52-58`). It reads
    `ANTHROPIC_API_KEY` first, then `ANTHROPIC_AUTH_TOKEN`, then `ANTHROPIC_PROFILE` and other
    sources (`client.go:30-82`). Piperoom reads none of these itself. **Leave `ANTHROPIC_BASE_URL`
    unset, or pin it to the Anthropic host you approved. Unset `ANTHROPIC_AUTH_TOKEN` and
    `ANTHROPIC_PROFILE` on the host.** Otherwise the prompts and the key go wherever those
    variables point.
- **Room event log (`room_events`, `migrations/00001_init.sql:16`).** Every tool call, result `Data`,
  comment, steer, proposal and approval is stored **permanently** in the Piperoom database and shown
  to everyone who can open the room. There is no per-event redaction after the fact. Redaction
  happens before the event is written: `pg_row` and `warehouse_row` return redacted `Data`
  (`internal/tools/dbtools/pg.go:191`, `wh.go:111`).
- **Warehouse (Snowflake, only with `WAREHOUSE_KIND=snowflake`).** Warehouse queries go to the
  Snowflake account named in `WAREHOUSE_DB_URL`. The DSN holds the password, and startup errors
  are redacted of it (`internal/wiring/warehouse.go:74`; `internal/clients/snowflake/redact.go`).
  Warehouse queries run with the role in that DSN, so the account's network path is part of
  your egress review (section 5).
- **Slack (optional, outbound only).** Off unless **both** `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID`
  are set. A half setting stops the server at start (`cmd/server/main.go:128-132`;
  `internal/slackbridge/bridge.go:63-69`). It posts into one thread per room in one channel
  (`internal/slackbridge/bridge.go:1-8`). Nothing typed in Slack reaches a room. The mirror runs in
  `--demo` mode too, if the variables are set (`main.go:125-132` runs before the demo branch).
  - **Posted:** the room title, severity, pipeline id and room id, plus a room link if
    `PIPEROOM_PUBLIC_URL` is set (`internal/slackbridge/render.go:78-92`). Hypotheses (text up to
    1500 bytes). Each proposal's title, risk and rollback, and up to 10 steps with action name,
    layer, description and `params` (up to 600 bytes) (`render.go:131-165`). Approvals and
    rejections with the approver's name and the reason. Action results (output up to 500 bytes).
    Hand-off summaries. The close resolution (up to 1000 bytes, `render.go:224`) and the name of the
    person who closed the room; the close message is also broadcast to the channel, not only the
    thread (`ReplyBroadcast`, `render.go:225`). Only these event types are posted (`render.go:24-32`).
  - **Limits are bytes**, counted after escaping (`internal/slackbridge/escape.go:30-36`).
  - **Not posted:** the raw alert body (`render.go:85`), tool results and their `Data`, comments and
    steers.
  - Slack keeps its copies under its own retention, not Piperoom's. Thread state is saved in the
    room database table `slack_threads` (`migrations/00003_slack_threads.sql`). A crash between a
    Slack post and the save can repeat one message after a restart (`bridge.go:6-7`).
  - Everyone in the Slack channel can see every room mirrored into it. Use a **private** channel
    (`bridge.go:4-5`).
- **Logs.** Piperoom logs with `log/slog`. It is designed not to log secrets (the Anthropic key,
  DSN passwords) or full record values. Report any place that does.

## What reaches the model

These are the sources of text the model reads. Everything here goes to the Anthropic API.

| Source | What the model gets | Code |
|---|---|---|
| First message | the alert **title**, the **pipeline id**, the **room id** and the **pipeline map** (text). Severity is not in this message | `internal/agent/prompt.go:91-113` |
| Human input, live | a driver's steer, including a sent suggestion, in full, with the author's name and **without** the untrusted wrapper. A comment reaches the agent live only when it mentions `@agent`; comments are wrapped. | `internal/room/service.go:593-598`; `internal/agent/prompt.go:238-239` (steer), `:251-253` (comment) |
| Approvals and rejections | the layer and proposal id; on rejection, the reason | `internal/room/service.go:600-620` |
| Action results | the output of each step, in full, with the ok flag | `internal/room/service.go:625-635` (no cap on this path) |
| Tool results | each tool's `Summary` text (never `Data`) | the tool's `Summary`, capped by `tools.MaxSummaryBytes` |
| Hand-off | a digest of the room log (see below) | `internal/agent/handoff.go:19-25`; `prompt.go:298-385` |

**Hand-off digest.** The hand-off request sends the room log, rendered as text. It keeps every
hypothesis, proposal, approval, rejection and action, plus the newest 30 other events. Each free
text field is capped at 1024 bytes. Tool and action output lines are capped at 300 bytes. The whole
digest is held under 32 KiB, and the oldest kept events are folded into an "omitted" marker first
(`prompt.go:265-297`). The digest includes comments, suggestions, steers, reasons, outputs, tool
summaries and display names (`prompt.go:429-520`).

Two more things follow from this list:

- A tool's `Summary` can contain the key values and timestamps that the tool returns (section 2).
- Anything an engineer types is sent to the model in some form. A comment reaches the agent live
  only when it mentions `@agent`. Comments also reach the model through the hand-off digest, which
  keeps the newest 30 non-critical events, so an older comment can be folded out of it
  (`prompt.go:478`, `:265-297`). Steers are sent verbatim, unwrapped. Tell the team not to paste
  secrets or customer data.

## 2. What each tool exposes

| Tool | Exposes to the agent and the room | Does **not** expose |
|---|---|---|
| `pg_replication_slots` | slot name, plugin, active flag, WAL retained | data |
| `pg_recent_ddl` | DDL command, object, **full statement text**, user, time | row data |
| `pg_long_transactions` | pid, user, application name (client-set free text, shown as-is), state, transaction age, wait event, and at most the statement verb (first keyword, e.g. `UPDATE`). Database-wide: it lists sessions of every pipeline, because it cannot be tied to one pipeline. Each row has `has_xid` (summary `writer=yes\|no`): only writers hold the WAL horizon, but long read-only sessions are listed too since they block DDL and VACUUM | query text (never selected, never returned) |
| `pg_row` | row **column names**; values only for the key and timestamp columns, others `null` (`pg.go:191,199`). Key and timestamp columns are chosen **by name**: the key is the column you pass in, and timestamp columns are `at` or names ending in `_at` (`dbtools.go:220-222`) | other column values |
| `pg_publication_tables` | the publication name, the connector role, and only the pipeline's own source tables | other tables of a `FOR ALL TABLES` publication |
| `warehouse_freshness` | max timestamp of the sink table, lag | row data |
| `warehouse_row` | **timestamp-named columns only** (`at` or `*_at`), by name, not by type. The key and every other column are dropped, from both Summary and Data (`wh.go:111,119-127`; `dbtools.go:220-222`). The same on Snowflake, where column names come back lower-cased (`internal/clients/snowflake/snowflake.go:148`) | key value, all other columns |
| `pipeline_map` | the pipeline's tables, connectors, topics, consumer groups and sink tables, with owners and health (`map.go:17-26`) | row data |
| `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag` | topic/group names, offsets, lag, only for the room pipeline's own topics and consumer groups (`kafka_consumer_lag` shows only partitions of the pipeline's topics) | record payloads |
| `kafka_topic_config` | for a pipeline's own topic: retention, cleanup policy, min in-sync replicas, partition count and replication factor (`kafka_tools.go:236-270`) | record payloads |
| `kafka_find_by_key` | topic, partition, offset, timestamp and **key** of matching records | record **values** |
| `trace_record` | per-stage status and facts for one `(table, key)` (`map.go:46`) | row values |
| `connect_*` | connector state and task errors (**stack traces can contain data**). Config: a fixed list of key names (`connect_tools.go:155-166`), the `transforms` name list and each transform's `type`, `field`, `fields` and `field.name` (every other `transforms*` value is `***`), and secret-looking keys (`password`, `secret`, `token`, `.key`) as `***`. A secret key's presence is shown, not its value (`connect_tools.go:163-170`) | secret values |
| `schema_registry_*` | subjects, versions, schema definitions and diffs | data |
| `runbook_lookup` | sections of the runbook library embedded in the binary (`internal/tools/runbooks/runbooks.go:42-43`) | pipeline data, network |

Things to know:

- **Keys are shown.** If your primary key or Kafka key is personal data (an email, a phone
  number), it will appear in the room. Prefer pipelines keyed by surrogate ids for the pilot.
- **Statement text and error traces are free text.** DDL statements and Connect task errors
  can contain literal values. Check yours (for example, `CREATE ROLE ... PASSWORD` in the
  DDL audit log, or a deserialization error that echoes a record).
- **Only some `transforms*` settings are shown.** The `transforms` name list and each
  transform's `type`, `field`, `fields` and `field.name` appear; every other transform setting
  is shown as `***`. Still put secrets in config providers, not in transform settings.
- **`application_name` is free text.** The client sets it (any session can put anything in it),
  and `pg_long_transactions` shows it as-is.

## 3. Prompt injection and untrusted text

Record keys, error traces, log lines and comments are attacker-influenced text. Piperoom wraps
tool output, comments (whoever writes them), approval and action results, and every hand-off
digest line in `untrusted` markers with escaping, and tells the model to treat them as data
(`internal/agent/prompt.go:176-259`, `:429-520`). A driver's **steer**, including a sent
suggestion, reaches the live agent **without** the wrapper (`prompt.go:238-239`). The hand-off
digest does wrap steers and suggestions (`prompt.go:484,490`). That reduces risk but is not a
guarantee. The controls that do not depend on the model are:

1. **Read-only credentials** ([permissions.md](permissions.md)): even a fooled agent cannot write.
2. **Human approval:** the agent can only *propose* a fix. It runs only after every required
   layer owner approves. Read the exact `params` of a proposal, not just its description (F2).
   Every write action is also checked against the room's pipeline before it runs (F10,
   `internal/room/service_executor.go:211`).
3. **Read scoping (F11).** Every read tool is scoped to the room's pipeline, and the pipeline
   comes from the alert, never from the model. A tool refuses a `pipeline_id`, connector, topic,
   consumer group, subject or table that is not the room pipeline's own, before it calls any
   client. List tools return only the pipeline's own items. By default a read is refused: a room
   with no pipeline gets no reads (only `runbook_lookup` works), and only a CLI or dev caller can
   opt out in code. Two tools stay database-wide because the pipeline cannot be told from
   `owners.yaml`: `pg_replication_slots` (slot metadata for every slot, no row data) and
   `pg_long_transactions` (long sessions of every pipeline, no query text, since any long
   transaction holds the WAL horizon). Database grants, Kafka ACLs, Connect proxy rules and
   Registry ACLs remain the real boundary; keep them to the pipeline's own objects (see
   [permissions.md](permissions.md)).

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
- Keep `owners.yaml` to the pipelines your Piperoom may serve. A room can only read its own
  pipeline (F11), but one process holds the credentials for all of them (F7).
- Pilot with a small, named group in `users.yaml`: everyone in a room sees everything in it.
- Turn Slack on only with a **private** channel whose members may see every room mirrored into it.
  Leave `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` unset if they may not.
- Pin or unset `ANTHROPIC_BASE_URL`; unset `ANTHROPIC_AUTH_TOKEN` and `ANTHROPIC_PROFILE` on the host
  (section 1).
- Keep the HTTP port on loopback or behind your VPN ([setup.md](setup.md#network-exposure)).
- Use a dedicated Anthropic key with a spend limit; rotate it at the end of the pilot.
- At the end of the pilot, revoke all Piperoom credentials, remove the Slack bot, and delete the
  room database.

## 6. Questions to answer before go-live

- [ ] Is it allowed to send the fields in sections 1 and 2 (keys, statement text, error traces, proposal params, hypotheses, action output, the hand-off digest) to the Anthropic API?
- [ ] Is it allowed to post proposals, params, hypotheses, approver names and close resolutions to a Slack channel, and who is in that channel?
- [ ] Who may view incident history, and for how long is it kept?
- [ ] Who are the approvers per layer, and are they reachable out of hours?
- [ ] How do we revoke Piperoom's access quickly (one place per system, and the Slack bot)?
