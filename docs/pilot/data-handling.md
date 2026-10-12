# Data handling

What Piperoom reads from your systems, what reaches the model, where it goes, who can see it, and
how to limit it. Based on the code on `main` at `a91d2bd`. Under `cmd/investigate` events are
printed to the terminal. Under `cmd/server` they go to the room event log and, if Slack is
configured, to a Slack thread.

## 1. Where data goes

```
your systems ──read-only tools──► Summary (text) ──► model provider   (agent's view)
                    │
                    ├───────────► Data (structured) ──► room event log ──► every room member
                    │                                   (permanent, append-only)
                    │
                    └───────────► selected room events ──► Slack thread   (only if configured)
```

- **The model provider.** Everything in the next section goes to the model provider you choose
  with `PIPEROOM_MODEL_PROVIDER` (`internal/agent/endpoint.go:86-137`): `anthropic` (the default),
  `bedrock` or `vertex`. Review that provider's current commercial terms and data-retention
  settings for your organisation before the pilot. Piperoom does not change them.
  - **`bedrock` and `vertex` use your cloud account and identity.** The requests carry your AWS or
    Google credentials. The endpoint is in the region you set: the Bedrock runtime in `AWS_REGION`
    (`bedrock-runtime.<region>.amazonaws.com`, `anthropic-sdk-go@v1.80.0/bedrock/bedrock.go:257`), or
    the Vertex AI endpoint for `VERTEX_REGION`, in `VERTEX_PROJECT_ID`
    (`anthropic-sdk-go@v1.80.0/vertex/vertex.go:96-106`). The region does not by itself fix where
    processing happens. A Bedrock cross-region profile, such as an `in.` profile
    (`internal/config/config.go:89`), can route requests to other regions. The Vertex values `global`,
    `us` and `eu` are multi-region endpoints (`anthropic-sdk-go@v1.80.0/vertex/vertex.go:96-106`). Choose the profile or region
    that matches where your data may be processed. Usage is billed to your account.
  - **`anthropic`: where the requests go.** The Anthropic SDK reads `ANTHROPIC_BASE_URL` from the
    environment and sends every request there (`anthropic-sdk-go@v1.80.0/client.go:52-58`). It reads
    `ANTHROPIC_API_KEY` first, then `ANTHROPIC_AUTH_TOKEN`, then `ANTHROPIC_PROFILE` and other
    sources (`anthropic-sdk-go@v1.80.0/client.go:30-82`). Piperoom reads none of these itself. **Leave `ANTHROPIC_BASE_URL`
    unset, or pin it to the Anthropic host you approved. Unset `ANTHROPIC_AUTH_TOKEN` and
    `ANTHROPIC_PROFILE` on the host.** Otherwise the prompts and the key go wherever those
    variables point.
  - **The bedrock and vertex clients turn off the SDK's environment defaults**
    (`anthropic-sdk-go@v1.80.0/bedrock/bedrock.go:252`, `anthropic-sdk-go@v1.80.0/vertex/vertex.go:108`). `ANTHROPIC_BASE_URL` and `ANTHROPIC_API_KEY`
    therefore do not reach them.
- **Per-room token cap.** A room stops when it has used `PIPEROOM_ROOM_TOKEN_BUDGET` model tokens
  (default `2000000`). The count is in memory and starts again at zero after a restart. It counts
  input, cache creation, cache reads, output and compaction at face value, so the cap limits model
  work, not dollars (`internal/agent/budget.go:13-15,65-67`). Once a room reaches the limit, its calls
are refused until the server restarts, and a restart is also needed to change the limit
(`internal/agent/budget.go:88-98`). Hand-offs are counted but never refused.
Closing a room drops its count: the server calls `Endpoint.Forget` from the room-closed hook
(`cmd/server/watch.go`; `internal/agent/endpoint.go:236-250`), so the counts of closed rooms do not stay in memory.
- **Room event log (`room_events`, `migrations/00001_init.sql:16`).** Every tool call, result `Data`,
  comment, steer, proposal and approval is stored **permanently** in the Piperoom database and shown
  to everyone who can open the room. There is no per-event redaction after the fact. Redaction
  happens before the event is written: `pg_row` and `warehouse_row` return redacted `Data`
  (`internal/tools/dbtools/pg.go:264`, `wh.go:124`).
- **Warehouse (Snowflake, only with `WAREHOUSE_KIND=snowflake`).** Warehouse queries go to the
  Snowflake account named in `WAREHOUSE_DB_URL`. The DSN holds the password, and startup errors
  are redacted of it (`internal/wiring/warehouse.go:74`; `internal/clients/snowflake/redact.go`).
  Warehouse queries run with the role in that DSN, so the account's network path is part of
  your egress review (section 5).
- **Slack (optional, outbound only).** Off unless **both** `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID`
  are set. A half setting stops the server at start (`cmd/server/main.go:157-161`;
  `internal/slackbridge/bridge.go:63-69`). It posts into one thread per room in one channel
  (`internal/slackbridge/bridge.go:1-8`). Nothing typed in Slack reaches a room. The mirror runs in
  `--demo` mode too, if the variables are set (`cmd/server/main.go:157-161` runs before the demo branch).
  - **Posted:** the room title, severity, pipeline id and room id, plus a room link if
    `PIPEROOM_PUBLIC_URL` is set (`internal/slackbridge/render.go:78-92`). Hypotheses (text up to
    1500 bytes). Each proposal's title, risk and rollback, and up to 10 steps with action name,
    layer, description and `params` (up to 600 bytes) (`internal/slackbridge/render.go:131-165`). Approvals and
    rejections with the approver's name and the reason. Action results (output up to 500 bytes).
    Hand-off summaries. The close resolution (up to 1000 bytes, `internal/slackbridge/render.go:224`) and the name of the
    person who closed the room; the close message is also broadcast to the channel, not only the
    thread (`ReplyBroadcast`, `internal/slackbridge/render.go:225`). Only these event types are posted (`internal/slackbridge/render.go:24-32`).
  - **Limits are bytes**, counted after escaping (`internal/slackbridge/escape.go:30-36`).
  - **Not posted:** the raw alert body (`internal/slackbridge/render.go:85`), tool results and their `Data`, comments and
    steers.
  - Slack keeps its copies under its own retention, not Piperoom's. Thread state is saved in the
    room database table `slack_threads` (`migrations/00003_slack_threads.sql`). A crash between a
    Slack post and the save can repeat one message after a restart (`bridge.go:6-7`).
  - Everyone in the Slack channel can see every room mirrored into it. Use a **private** channel
    (`bridge.go:4-5`).
- **Logs.** Piperoom logs with `log/slog`. It is designed not to log secrets (the model key,
  DSN passwords) or full record values. Report any place that does.

## What reaches the model

These are the sources of text the model reads. Everything here goes to the model provider
(Anthropic, Bedrock or Vertex, per `PIPEROOM_MODEL_PROVIDER`).

| Source | What the model gets | Code |
|---|---|---|
| First message | the alert **title**, the **pipeline id**, the **room id** and the **pipeline map** (text). Severity is not in this message | `internal/agent/prompt.go:91-113` |
| Human input, live | a driver's steer, including a sent suggestion, in full, with the author's name and **without** the untrusted wrapper. A comment reaches the agent live only when it mentions `@agent`; comments are wrapped. | `internal/room/service.go:633-638`; `internal/agent/prompt.go:238-239` (steer), `:251-253` (comment) |
| Approvals and rejections | the layer and proposal id; on rejection, the reason | `internal/room/service.go:639-660` |
| Action results | the output of each step, in full, with the ok flag | `internal/room/service.go:661-674` (no cap on this path) |
| Tool results | each tool's `Summary` text (never `Data`) | the tool's `Summary`, capped by `tools.MaxSummaryBytes` |
| Hand-off | a digest of the room log (see below) | `internal/agent/handoff.go:19-25`; `prompt.go:298-392` |

**Hand-off digest.** The hand-off request sends the room log, rendered as text. It keeps every
hypothesis, proposal, approval, rejection and action, plus the newest 30 other events. Each free
text field is capped at 1024 bytes. Tool and action output lines are capped at 300 bytes. The whole
digest is held under 32 KiB, and the oldest kept events are folded into an "omitted" marker first
(`prompt.go:263-297`). The digest includes comments, suggestions, steers, reasons, outputs, tool
summaries and display names (`prompt.go:429-518`).

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
| `pg_row` | row **column names**; values only for the key and timestamp columns, others `null` (`pg.go:264,272-282`). Key and timestamp columns are chosen **by name**: the key is the column you pass in, and timestamp columns are `at` or names ending in `_at` (`dbtools.go:242-244`) | other column values |
| `pg_publication_tables` | the publication name, the connector role, and only the pipeline's own source tables | other tables of a `FOR ALL TABLES` publication |
| `warehouse_freshness` | max timestamp of the sink table, lag | row data |
| `warehouse_row` | **timestamp-named columns only** (`at` or `*_at`), by name, not by type. The key and every other column are dropped, from both Summary and Data (`wh.go:124,132-140`; `dbtools.go:242-244`). The same on Snowflake, where column names come back lower-cased (`internal/clients/snowflake/snowflake.go:148`) | key value, all other columns |
| `pipeline_map` | the pipeline's tables, connectors, topics, consumer groups and sink tables, with owners and health (`map.go:14-46`) | row data |
| `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag` | topic/group names, offsets, lag, only for the room pipeline's own topics and consumer groups (`kafka_consumer_lag` shows only partitions of the pipeline's topics) | record payloads |
| `kafka_topic_config` | for a pipeline's own topic: retention, cleanup policy, min in-sync replicas, partition count and replication factor (`kafka_tools.go:297-328`) | record payloads |
| `kafka_find_by_key` | topic, partition, offset, timestamp and **key** of matching records | record **values** |
| `trace_record` | per-stage status and facts for one `(table, key)` (`map.go:97`) | row values |
| `connect_*` | connector state and task errors (**stack traces can contain data**). Config: a fixed list of key names (`connect_tools.go:175-196`), the `transforms` name list and each transform's `type`, `field`, `fields` and `field.name` (every other `transforms*` value is `***`), and secret-looking keys (`password`, `secret`, `token`, `.key`) as `***`. A secret key's presence is shown, not its value (`connect_tools.go:183-194`) | secret values |
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
(`internal/agent/prompt.go:176-261`, `:429-520`). A driver's **steer**, including a sent
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

- Retention is live (F6, partly closed). `PIPEROOM_RETENTION_DAYS` deletes closed rooms older than N
  days, checked every 6 hours; an empty value or `0` keeps everything. Open rooms are never deleted.
  Export a room first with `GET /api/rooms/{id}/export` if you must keep it. The event log still has
  no size cap, and there is no UI for deletion.
- Deleting the Piperoom database deletes the room history, the Slack thread state and everything
  else in it. Data sent to the model provider (Anthropic, Bedrock or Vertex), and messages already posted to Slack, are governed by
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
- Use a dedicated key or cloud identity for the model provider (an Anthropic key, or the AWS or Google
  identity for Bedrock or Vertex), with a spend limit where the provider offers one. Rotate it at the
  end of the pilot.
- At the end of the pilot, revoke all Piperoom credentials, remove the Slack bot, and delete the
  room database.

## 6. Questions to answer before go-live

- [ ] Is it allowed to send the fields in sections 1 and 2 (keys, statement text, error traces, proposal params, hypotheses, action output, the hand-off digest) to the model provider (Anthropic, Bedrock or Vertex)?
- [ ] Is it allowed to post proposals, params, hypotheses, approver names and close resolutions to a Slack channel, and who is in that channel?
- [ ] Who may view incident history, and for how long is it kept?
- [ ] Who are the approvers per layer, and are they reachable out of hours?
- [ ] How do we revoke Piperoom's access quickly (one place per system, and the Slack bot)?
