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
  with `PIPEROOM_MODEL_PROVIDER` (`internal/agent/endpoint.go`): `anthropic` (the default),
  `bedrock` or `vertex`. Review that provider's current commercial terms and data-retention
  settings for your organisation before the pilot. Piperoom does not change them.
  - **`bedrock` and `vertex` use your cloud account and identity.** The requests carry your AWS or
    Google credentials. The endpoint is in the region you set: the Bedrock runtime in `AWS_REGION`
    (`bedrock-runtime.<region>.amazonaws.com`, `anthropic-sdk-go@v1.80.0/bedrock/bedrock.go:257`), or
    the Vertex AI endpoint for `VERTEX_REGION`, in `VERTEX_PROJECT_ID`
    (`anthropic-sdk-go@v1.80.0/vertex/vertex.go:96-106`). The region does not by itself fix where
    processing happens. A Bedrock cross-region profile, such as an `in.` profile
    (`internal/config/config.go`), can route requests to other regions. The Vertex values `global`,
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
  work, not dollars (`internal/agent/budget.go`). Once a room reaches the limit, its calls
are refused until the server restarts, and a restart is also needed to change the limit
(`internal/agent/budget.go`). Hand-offs are counted but never refused.
Closing a room drops its count: the server calls `Endpoint.Forget` from the room-closed hook
(`cmd/server/watch.go`; `internal/agent/endpoint.go`), so the counts of closed rooms do not stay in memory.
- **Room event log (`room_events`, `migrations/00001_init.sql:16`).** Every tool call, result `Data`,
  comment, steer, proposal and approval is stored in the Piperoom database until the room is deleted
  by retention or by an admin (section 4), and shown to everyone who can open the room. There is no
  per-event redaction after the fact: to remove one thing, delete the whole room. Redaction
  happens before the event is written: `pg_row` and `warehouse_row` return redacted `Data`
  (`internal/tools/dbtools/pg.go`, `wh.go`).
- **Warehouse (Snowflake, only with `WAREHOUSE_KIND=snowflake`).** Warehouse queries go to the
  Snowflake account named in `WAREHOUSE_DB_URL`. The DSN holds the password, and startup errors
  are redacted of it (`internal/wiring/warehouse.go`; `internal/clients/snowflake/redact.go`).
  Warehouse queries run with the role in that DSN, so the account's network path is part of
  your egress review (section 5).
- **Watch sample history (`watch_samples`, X3; contract in `internal/clients/clients.go`, table and
  writer land with X3a).** The watcher stores one number per pipeline and metric at most every
  15 minutes: retained slot WAL bytes, DLQ record count, total consumer lag, source database size
  (`pg_database_size`) and the age of the oldest unread record. No row value, record, config value or
  free text. Kept 7 days, then deleted. Shown to anyone who can see the pipeline
  (`GET /api/pipelines/{pid}/trends`); reading it makes no call to your systems.
- **Connector config drift (`config_drift`, X3b).** The watcher keeps, in memory only, each connector's
  approved config in its redacted form (`actions.RedactConfig`, the form section 4's config versions store),
  its hash (`actions.ConfigHash`, taken over the redacted form, so it cannot be used to guess a secret) and a
  fingerprint of each key's value keyed with a random per-process key (HMAC-SHA256, `actions.DigestConfig`),
  which never leaves the process; it never keeps or logs a raw config value. A drift room shows the two hashes and
  the changed key **names** only, and every changed key that looks like a credential (`actions.LooksSecret`) only
  as `<secret key changed>`.
- **External changes (X4: deploys, migrations, vendor incidents).** Rows of the same change log as the watcher's
  (`pipeline_changes`, section 4: `PIPEROOM_CHANGES_RETENTION_DAYS`, 5000 rows per pipeline). What they hold:
  - **The change webhook** (`POST /api/changes/webhook`, signed with `CHANGES_WEBHOOK_SECRET`) stores what your
    CI or deploy tool sends: title, service, actor, URL and a vendor status (the sender's own `id` only inside the
    dedup fingerprint, a hash). It is free text from the sender, capped (title 300 runes, the rest 128 bytes, URL
    1024 bytes and `http`/`https` only), shown as text, given to the model only inside the untrusted-data wrapper,
    and never logged.
  - **Migration history readers** (X4b) read, from the pipeline's own source database, the metadata columns of
    `flyway_schema_history` or `databasechangelog` (version, description, applied time, applied by), or dbt run
    results when configured; never a script's contents or checksum, never a row of another table. Read-only, 10 s.
  - **The vendor status poller** (X4a) fetches a pipeline's `owners.yaml` `vendor_status_url` (a vendor's
    public status feed) every 5 minutes from the server: an **outbound** HTTPS request to a host you name, part
    of your egress review (section 5). It sends nothing but the request.
  - **Paging a team** (`POST /api/rooms/{id}/page-team`, X4d, a driver's click, never automatic) posts one
    message to the team's own `slack_channel` from `owners.yaml` `teams:`: the hypothesis text, the cited
    change's title and URL, the cited step numbers and the room link. That channel is outside the room's
    Slack thread (see Slack, below); nothing is read back from it.
- **Slack (optional, outbound only).** Off unless **both** `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID`
  are set. A half setting stops the server at start (`cmd/server/main.go`;
  `internal/slackbridge/bridge.go`). It posts into one thread per room in one channel
  (`internal/slackbridge/bridge.go`). Nothing typed in Slack reaches a room. The mirror runs in
  `--demo` mode too, if the variables are set (`cmd/server/main.go` runs before the demo branch).
  - **Posted:** the room title, severity, pipeline id and room id, plus a room link if
    `PIPEROOM_PUBLIC_URL` is set (`internal/slackbridge/render.go`). Hypotheses (text up to
    1500 bytes). Each proposal's title, risk and rollback, and up to 10 steps with action name,
    layer, description and `params` (up to 600 bytes) (`internal/slackbridge/render.go`). Approvals and
    rejections with the approver's name and the reason. Action results (output up to 500 bytes).
    Hand-off summaries. The close resolution (up to 1000 bytes, `internal/slackbridge/render.go`) and the name of the
    person who closed the room; the close message is also broadcast to the channel, not only the
    thread (`ReplyBroadcast`, `internal/slackbridge/render.go`). Only these event types are posted (`internal/slackbridge/render.go`).
  - **Limits are bytes**, counted after escaping (`internal/slackbridge/escape.go`).
  - **Not posted:** the raw alert body (`internal/slackbridge/render.go`), tool results and their `Data`, comments and
    steers.
  - **Team pages (X4d).** A driver's "Page <team>" posts one message to that team's `slack_channel`
    (`owners.yaml` `teams:`), not to the room's channel: see External changes, above.
  - Slack keeps its copies under its own retention, not Piperoom's. Thread state is saved in the
    room database table `slack_threads` (`migrations/00003_slack_threads.sql`). A crash between a
    Slack post and the save can repeat one message after a restart (`bridge.go`).
  - Everyone in the Slack channel can see every room mirrored into it. Use a **private** channel
    (`bridge.go`).
- **Logs.** Piperoom logs with `log/slog`. It is designed not to log secrets (the model key,
  DSN passwords) or full record values. Report any place that does.

## What reaches the model

These are the sources of text the model reads. Everything here goes to the model provider
(Anthropic, Bedrock or Vertex, per `PIPEROOM_MODEL_PROVIDER`).

| Source | What the model gets | Code |
|---|---|---|
| First message | the alert **title**, the **pipeline id**, the **room id** and the **pipeline map** (text). Severity is not in this message | `internal/agent/prompt.go` |
| Human input, live | a driver's steer, including a sent suggestion, in full, with the author's name and **without** the untrusted wrapper. A comment reaches the agent live only when it mentions `@agent`; comments are wrapped. | `internal/room/service.go`; `internal/agent/prompt.go` (steer) (comment) |
| Approvals and rejections | the layer and proposal id; on rejection, the reason | `internal/room/service.go` |
| Action results | the output of each step, in full, with the ok flag | `internal/room/service.go` (no cap on this path) |
| Tool results | each tool's `Summary` text (never `Data`) | the tool's `Summary`, capped by `tools.MaxSummaryBytes` |
| `task_log_tail` summary | a window of the end of one Airflow task try's log, with secrets masked and fenced as untrusted data (the full masked tail is in `Data`) | `internal/tools/batchtools/logtail.go` |
| `dbt_model_sql` summary | a window of one dbt model's compiled SQL (raw Jinja when dbt never compiled it), secrets masked and fenced as untrusted data (the full masked SQL, at most 64 KiB, is in `Data`) | `internal/tools/dbttools/model_sql.go` |
| Hand-off | a digest of the room log (see below) | `internal/agent/handoff.go`; `prompt.go` |

**Hand-off digest.** The hand-off request sends the room log, rendered as text. It keeps every
hypothesis, proposal, approval, rejection and action, plus the newest 30 other events. Each free
text field is capped at 1024 bytes. Tool and action output lines are capped at 300 bytes. The whole
digest is held under 32 KiB, and the oldest kept events are folded into an "omitted" marker first
(`prompt.go`). The digest includes comments, suggestions, steers, reasons, outputs, tool
summaries and display names (`prompt.go`).

Two more things follow from this list:

- A tool's `Summary` can contain the key values and timestamps that the tool returns (section 2).
- A `task_log_tail` or `dbt_model_sql` `Summary` carries a window of free text (a task log, a
  model's SQL), so the model reads a masked, capped slice of text that dbt or a pipeline task
  wrote. The same applies to `dbt_run_results` messages and `dag_runs` run notes, which appear one
  line each in their summaries.
- Anything an engineer types is sent to the model in some form. A comment reaches the agent live
  only when it mentions `@agent`. Comments also reach the model through the hand-off digest, which
  keeps the newest 30 non-critical events, so an older comment can be folded out of it
  (`prompt.go`). Steers are sent verbatim, unwrapped. Tell the team not to paste
  secrets or customer data.

### Driver-authored proposals (P3c)

A driver can also author a proposal (`POST /api/rooms/{id}/proposals`); it enters the same path as an
agent proposal — the same `Validate`, scope check, `params_hash`, preview, per-layer approval, execution
record and agent verification — with `Origin: "driver"` (`internal/events/events.go`). It is never
auto-approved (`internal/room/autoapprove.go`).

- **What the params contain.** A step's `params` are the action's own typed fields only — connector,
  topic, group and table names, a timestamp, a boolean, a count — the same values the agent would
  propose (`schema_register` also takes a free-form Avro schema string). Do not put credentials in
  them: the action validation checks each field's shape and scope, not whether a value is a secret.
  Only `connect_update_connector_config` rejects a credential-looking key **or** value
  (`looksSecret`/`valueLooksSecret`, `internal/actions/connector_config.go`). Read the params, not just
  the description, before you approve (F2).
- **Where stored.** The params are stored in the proposal row (`proposals.steps`,
  `internal/store/store.go` `ProposalRow`) and, as part of the `proposal.created` event with
  `origin: "driver"`, in the room log (`internal/room/service_proposals.go`). They are therefore part of
  the permanent room history everyone in the room can see. `params_hash` (SHA-256 over the RFC 8785
  form) covers the params, so an approver's `params_hash` echo binds their approval to exactly the values
  on screen.
- **Where shown.** The proposal card renders each step's `params` as key/value rows (one row per field);
  raw JSON is shown only when `params` is not an object (`web/src/components/ProposalsPanel.tsx`). The
  params also reach the model — the agent hears about a driver proposal as `proposal_created` and
  verifies it after execution (`proposal_executed`; `internal/agent/contract.go`) — and, like any
  proposal, up to 600 bytes of each step's params are posted to Slack when it is configured (section 1).

## 2. What each tool exposes

| Tool | Exposes to the agent and the room | Does **not** expose |
|---|---|---|
| `pg_replication_slots` | the room pipeline's slots only (`owners.yaml` `watch.slot`, else `slot.name` from its connector config): slot name, plugin, active flag, WAL retained; other slots only as a count | data; other slots' names |
| `pg_recent_ddl` | DDL command, object, **full statement text**, user, time | row data |
| `pg_long_transactions` | pid, user, application name (client-set free text, shown as-is), state, transaction age, wait event, and at most the statement verb (first keyword, e.g. `UPDATE`). In a room it lists only sessions of the source database that hold or wait for a lock on the pipeline's source tables; every other long transaction is counted, and the oldest 5 other writers (which hold back every logical slot) are shown with pid, same-database flag, state, ages and wait event type only. Each row has `has_xid` (summary `writer=yes\|no`): only writers hold the WAL horizon, but long read-only sessions are listed too since they block DDL and VACUUM | query text (never selected, never returned) |
| `pg_row` | row **column names**; values only for the key and timestamp columns, others `null` (`pg.go`). Key and timestamp columns are chosen **by name**: the key is the column you pass in, and timestamp columns are `at` or names ending in `_at` (`dbtools.go`) | other column values |
| `pg_publication_tables` | the publication name, the connector role, and only the pipeline's own source tables; the publication's other tables only as a count | other tables of a `FOR ALL TABLES` publication |
| `warehouse_freshness` | max timestamp of the sink table, lag | row data |
| `warehouse_row` | **timestamp-named columns only** (`at` or `*_at`), by name, not by type. The key and every other column are dropped, from both Summary and Data (`wh.go`; `dbtools.go`). The same on Snowflake, BigQuery and Databricks, where column names come back lower-cased (`internal/clients/snowflake/snowflake.go`; `internal/clients/bigquery/bigquery.go`; `internal/clients/databricks/databricks.go`) | key value, all other columns |
| `clients.Reconciler` reader (the watcher's `count_drift` and `row_drift`, X1, and a recovery action's `Verify`, X2; not a tool) | row counts of a source or sink table over a time window, the primary-key values of at most 200 of the newest source rows, and the named timestamp columns of at most 1000 keys (`internal/clients/clients.go`). Only for the pipeline's own `source_tables`/`sink_tables`. The keys stay in the caller's memory: the room gets a `DriftSignal` (`internal/events/events.go`) or an `agent.recovery_check`/`agent.verification` with counts and a window only | every non-timestamp column value; key values in events, logs or the model |
| `pipeline_map` | the pipeline's tables, connectors, topics, consumer groups and sink tables, with owners and health (`map.go`) | row data |
| `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag` | topic/group names, offsets, lag, only for the room pipeline's own topics and consumer groups (`kafka_consumer_lag` shows only partitions of the pipeline's topics) | record payloads |
| `kafka_topic_config` | for a pipeline's own topic: retention, cleanup policy, min in-sync replicas, partition count and replication factor (`kafka_tools.go`) | record payloads |
| `kafka_find_by_key` | topic, partition, offset, timestamp and **key** of matching records | record **values** |
| `trace_record` | per-stage status and facts for one `(table, key)` (`map.go`) | row values |
| `connect_*` | connector state and task errors (**stack traces can contain data**). Config: a fixed list of key names (`connect_tools.go`), the `transforms` name list and each transform's `type`, `field`, `fields` and `field.name` (every other `transforms*` value is `***`), and secret-looking keys (`password`, `secret`, `token`, `.key`) as `***`. A secret key's presence is shown, not its value (`connect_tools.go`) | secret values |
| `schema_registry_*` | subjects, versions, schema definitions and diffs | data |
| `runbook_lookup` | sections of the runbook library embedded in the binary (`internal/tools/runbooks/runbooks.go`) | pipeline data, network |
| `dag_runs` | the last runs of one pipeline DAG (default 10, at most 100): run id, state, run type, logical date, start, duration and the run's `note` (free text, sanitized, redacted and scanned; the full note is in `Data`) (`internal/tools/batchtools/runs.go`) | row data |
| `task_instances` | the task instances of one run: task id, state, try, operator, start and duration; every name is cleaned and cut (`runs.go`) | row data |
| `task_log_tail` | the end of one task try's log, at most 32 KiB (default 16 KiB tail): invalid UTF-8 replaced, ANSI sequences dropped, secrets masked (`redact.Text`), and scanned for instruction-like text. The summary shows a fenced window of the end; `Data` holds the whole masked tail (`internal/tools/batchtools/logtail.go`) | the log before the tail window |
| `dag_schedule_gap` | the DAG's schedule text (DAG-author text, shown as one line), paused flag, last run and last success, next run, lateness and a verdict (`internal/tools/batchtools/schedule.go`) | row data |
| `dbt_run_results` | each node's status, rows and run time, and dbt's `message` (masked and scanned; one line in the summary, at most 2000 bytes in `Data`); failures first, scoped to the pipeline's models and their direct neighbours (`internal/tools/dbttools/run_results.go`) | results of models outside scope (counted) |
| `dbt_model_lineage` | one model's direct parents and children, as unique_ids, at most 100 each (`internal/tools/dbttools/lineage.go`) | other nodes |
| `dbt_model_sql` | one dbt model's compiled SQL (raw Jinja when dbt never compiled it), secrets masked, at most 64 KiB in `Data`, shown in windows in the summary; **never executed** (`internal/tools/dbttools/model_sql.go`) | nothing else |

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
- **Task logs, run notes, dbt messages and dbt SQL are free text.** `task_log_tail` returns the end
  of a task's log and `dbt_model_sql` a model's compiled SQL; both are masked of secrets and scanned
  for instruction-like text, but their content (an error message, a `SELECT`, a literal value in the
  log) is still shown as data. Review what your tasks and models print before the pilot.
- **The warehouse client's SQL is recorded as provenance, not sent to the model.** Every
  BigQuery/Databricks query is recorded as a provenance call (the fixed `SELECT` text and the
  rendered `@`/`:` parameters) on the room's tool result, masked of secrets
  (`internal/agent/provenance.go`); it goes to the room log only, never to the model.

## 3. Prompt injection and untrusted text

Record keys, error traces, log lines and comments are attacker-influenced text. Piperoom wraps
tool output, comments (whoever writes them), approval and action results, and every hand-off
digest line in `untrusted` markers with escaping, and tells the model to treat them as data
(`internal/agent/prompt.go`). A driver's **steer**, including a sent
suggestion, reaches the live agent **without** the wrapper (`prompt.go`). The hand-off
digest does wrap steers and suggestions (`prompt.go`). That reduces risk but is not a
guarantee. The controls that do not depend on the model are:

1. **Read-only credentials** ([permissions.md](permissions.md)): even a fooled agent cannot write.
2. **Human approval:** the agent can only *propose* a fix. It runs only after every required
   layer owner approves. Read the exact `params` of a proposal, not just its description (F2).
   Every write action is also checked against the room's pipeline before it runs (F10,
   `internal/room/service_executor.go`).
3. **Read scoping (F11).** Every read tool is scoped to the room's pipeline, and the pipeline
   comes from the alert, never from the model. A tool refuses a `pipeline_id`, connector, topic,
   consumer group, subject or table that is not the room pipeline's own, before it calls any
   client. List tools return only the pipeline's own items. By default a read is refused: a room
   with no pipeline gets no reads (only `runbook_lookup` works), and only a CLI or dev caller can
   opt out in code. Database-wide and cluster-wide metadata is scoped too (T2):
   `pg_replication_slots` shows only the pipeline's slots and `pg_long_transactions` only the
   sessions that lock the pipeline's tables; other objects appear only as a count, never by
   name ([tool-scope.md](../security/tool-scope.md)). Database grants, Kafka ACLs, Connect proxy rules and
   Registry ACLs remain the real boundary; keep them to the pipeline's own objects (see
   [permissions.md](permissions.md)).

> **Untrusted Kafka keys are decoded by the read tools.** `kafka_find_by_key` and `trace_record` decode
> record keys with an Avro library that has open advisories (GO-2026-5046, 5047, 5048). A crafted key
> can hold an agent slot until the server is restarted (three such keys block every room on a replica
> at the default `PIPEROOM_MAX_AGENTS`), fail a run, or run the server out of memory, which takes down
> every room on it. It cannot write anything. This is what `main`
> does today; it is tracked as pilot gate G2 (`docs/pilot/decision.md` §4). Details and the interim advice are in
> [permissions.md](permissions.md#kafka). Source: `reviews/vuln-report-2026-10.md` §avro.

## 4. Retention and deletion

Design: `docs/design/retention.md`. Every value below is read at start; a bad value stops the server.

- **Closed rooms.** `PIPEROOM_RETENTION_DAYS` deletes closed rooms older than N days, checked every 6
  hours; an empty value or `0` keeps everything. A pipeline's `retention_days:` in `owners.yaml` sets
  its own value; when both are set, the shorter wins. Rooms of a pipeline that is not in `owners.yaml`
  use `PIPEROOM_RETENTION_DAYS`.
- **Open rooms.** `PIPEROOM_MAX_OPEN_DAYS` (default 14, `0` = off) closes a room that has been open that
  long. The room gets a warning 24 hours before; the driver can extend it once by the same number of
  days. After the close, the closed-room retention above applies.
- **Rooms from before the upgrade.** The byte cap of a room created before the upgrade that added the caps
  applies once the server has counted its stored bytes, which it does in the background after the first start
  (see `docs/runbook.md`, "Upgrading to room limits"); its event cap applies at once.
- **Size of one room.** A room's log holds at most `PIPEROOM_ROOM_MAX_EVENTS` events (default 20000)
  and `PIPEROOM_ROOM_MAX_BYTES` of stored payloads (default 256 MiB). At 90% the room gets a warning; at
  100% the agent stops and its events are refused. People can still comment, approve, hand off and close,
  within a 2% reserve. A tool result's `Data` over `PIPEROOM_EVENT_MAX_DATA_BYTES` (default 64 KiB) is
  stored truncated (a preview and the original size), and so is a long action output.
- **Deleting one room.** A users.yaml user with `admin: true` can erase a room, open or closed, with
  `DELETE /api/rooms/{id}?reason=...` or `devctl rooms delete <id> --reason "..." --as <admin>`. It
  removes the room's events, proposals, approvals, suggestions, hand-offs, Slack thread state and shadow
  data from the Piperoom database, keeps no copy, and closes open WebSockets of the room. It does not
  remove messages already posted to Slack or data already sent to the model provider.
- **Audit of deletions.** Every deleted room, by an admin or by retention, leaves one row in
  `room_deletions`: the room id, pipeline, who (`system` for retention), when, the reason
  (`retention: closed N days` for retention) and the event count, and nothing else.
- **Archive (optional).** With `PIPEROOM_ARCHIVE_DIR` set, retention always, and an admin delete only when
  asked (`archive=true`, or `devctl rooms delete ... --archive`), first write the room's export (the same
  document as `GET /api/rooms/{id}/export`) to `<dir>/<pipeline>/<room>.json`, readable only by the
  server's user (mode `0600`). If the write fails, the room is not deleted. **The archive keeps the
  room's content**: such a purge or delete removes the room from Piperoom but not from that directory.
  Protect, back up and expire the directory under your own policy.
- Without an archive, export a room first with `GET /api/rooms/{id}/export` if you must keep it.
- **Fleet view: watcher status and connector config versions (X5-0; stored by X5a, X5c).** No new read of
  your systems: the watcher keeps, in the Piperoom database, one status row per pipeline (signal names,
  severities, topology node ids, times, forecast ETAs; no evidence values), replaced every tick, and one row per
  connector config it sees change. A config version stores the connector's config **redacted**: the value of
  every credential-looking key (`actions.LooksSecret`) reads `<redacted>`, every other value has passed the
  secret masker (`redact.Text`, so a password inside a JDBC URL is masked) and is cut at 4 KB. Its hash is
  taken over that redacted form, so it cannot be used to guess a secret. Versions older than
  `PIPEROOM_CHANGES_RETENTION_DAYS` are deleted, except the newest of each connector. Version lists (key
  names only) are visible to anyone who can see the pipeline; diffs (redacted values) only to its layer
  approvers and backups, the solo user and auditors.
- Deleting the Piperoom database deletes the room history, the Slack thread state and everything
  else in it. Data sent to the model provider (Anthropic, Bedrock or Vertex), and messages already posted to Slack, are governed by
  your agreements with those providers, not by Piperoom.
- Back up and encrypt the room database like any other store that holds incident detail.

## 5. Controls you should apply

- Run on staging or replicas first; use pipelines without regulated data (card numbers,
  health data, government ids) for the pilot unless your security team signs off.
- Keep `source_tables` / `sink_tables` as small as possible and grant `SELECT` only on them.
- Keep `owners.yaml` to the pipelines your Piperoom may serve. A room can only read its own
  pipeline (F11). A pipeline with its own `connections:` block gets its own clients (F7,
  `internal/owners/tenancy.go`), but one process still holds the credentials for all of them.
- Pilot with a small, named group in `users.yaml`: everyone in a room sees everything in it.
- Turn Slack on only with a **private** channel whose members may see every room mirrored into it.
  Leave `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` unset if they may not. A `teams:` entry's `slack_channel`
  (X4) receives the pages its team gets; give it the same care.
- Set a `vendor_status_url` (X4) only to a status feed host your egress policy allows (section 1).
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
