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
- **Watch sample history (`watch_samples`, X3; migration 00018, written by the watcher, X3a).** The
  watcher stores one number per pipeline and metric at most every 15 minutes: retained slot WAL bytes,
  DLQ record count, total consumer lag, source database size (`pg_database_size`) and the age of the
  oldest unread record. No row value, record, config value or free text. Kept 7 days, then deleted
  (the `samples-retention` loop). Shown to anyone who can see the pipeline
  (`GET /api/pipelines/{pid}/trends`); reading it makes no call to your systems. The first three come
  from reads the watcher already makes; database size and oldest unread age need optional client
  readers that no client implements yet, so they are not sampled today (section 2, watcher samples).
- **Watcher status (`watch_status`, X5; migration 00019, written by the watcher, X5a).** One row per
  watched pipeline, replaced on every tick (no history): when the watcher last checked it and, for each
  signal now firing or acked, its name, state, severity, start time, the topology node id it is about
  (`connect:<connector>`, `group:<group>`, `dlq:<topic>`, `pg:<table>`, `topic:<topic>`) and, for a
  forecast, its metric and ETA. No evidence value, row value, record, config value or free text. The
  fleet view (`GET /api/fleet`, any signed-in user, filtered by tenant) reads it with
  `pipeline_changes` counts, open rooms and the last closed room's verdict; `GET /api/fleet/changes`
  pages the same masked `pipeline_changes` rows the room shows. Neither route makes a call to your
  systems.
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
  - **Migration history readers** (X4b; the optional `clients.MigrationReader` on the source client) read, from
    the pipeline's own source database, the metadata columns of `flyway_schema_history` or `databasechangelog`
    (version or changeset id, description, applied time, applied by, success), found by a catalog lookup only in the
    schemas of the pipeline's own source tables, or dbt run results when configured (the run time, its
    `invocation_id` and a count of the pipeline's models per status, never dbt's messages); never a script's name,
    contents or checksum, never a row of another table. Read live by `what_changed`, not stored. Read-only, 10 s, at
    most 200 rows. A history table the login cannot read is reported as a fact
    ("grant SELECT on ..."), not read.
  - **The vendor status poller** (X4a; **not running on this commit**: `alertin.StatusPoller` is written and
    tested, but the server does not start it and the `owners.yaml` loader has no `vendor_status_url` key, so the
    file is refused). When it is wired, it fetches a pipeline's `vendor_status_url` (a vendor's public status
    feed) every 5 minutes from the server: an **outbound** HTTPS request to a host you name, part of your egress
    review (section 5). It sends nothing but the request. It connects directly (no HTTP proxy) and
    only to public addresses: a feed host, or a redirect target, that resolves to a loopback, private, link-local or
    cloud-metadata address is refused; redirects stay on `https` (at most 5); 10 s per fetch, body at most 4 MB.
  - **Paging a team** (`POST /api/rooms/{id}/page-team`, X4d, a driver's click, never automatic) posts one
    message to the team's own `slack_channel` from `owners.yaml` `teams:`: the driver's display name, the room
    and pipeline ids, the hypothesis's scope (fixed words), confidence and text (up to 1500 bytes), the cited
    change's kind, service, time, summary (up to 600 bytes) and URL (linked only when it is a plain absolute
    http(s) URL), the cited step numbers and the room link (`internal/slackbridge/teampage.go`). Untrusted text is
    escaped (no mentions, no links). No tool result, row or config value. That channel is outside the room's
    Slack thread (see Slack, below); nothing is read back from it. The room stores only `room.team_paged`
    (team, hypothesis seq, change id), never the text.
- **Lineage, PII tags and retention (X7; contract X7-0, design `docs/design/lineage.md`; what each proves:
  [governance.md](governance.md)).** The pipeline map gains column lineage on its edges and `pii` tags on its
  table nodes: column **names**, their PII class (from names and types, never values). The contract also names a
  `retention` tag (the declared duration), but no code sets it yet. Stored nowhere new: the map is built on
  request, like before. Shown to anyone who can see the pipeline (`GET /api/pipelines/{pid}/topology`) and to the
  agent in `pipeline_map`, which sends the column names and their classes to the model provider. The retention
  comparison is a separate reader (section 2, `clients.TopicConfigReader`) and reaches the room only as
  `config_risk` entries of durations and a policy word.
  - **OpenLineage export (X7b, `internal/discover/openlineage`; optional, off unless `OPENLINEAGE_URL` is set).** An
    **outbound** HTTP(S) POST from the server to the lineage server you name (Marquez, DataHub), part of your egress
    review (section 5). Off, no goroutine starts and nothing is sent. On, it runs on the elected leader replica only
    (election name `openlineage`) and sends one `RunEvent` for a pipeline's map at most every 15 minutes (only when the
    room service builds the map on the leader: an agent or shadow run starting, a fix preview, the map page
    `GET /api/pipelines/{pid}/topology`; the agent's own `pipeline_map` calls do not count), and one per executed fix
    step
    (`action.executed`: `COMPLETE`, `FAIL`, or `OTHER` when the outcome is unknown), through a dialer that refuses
    link-local and cloud-metadata addresses (`connprobe.Guard`; loopback and private ranges are allowed), with no
    proxy and no redirect; 10 s per POST, a network error, 429 or 5xx retried 3 times with backoff and then dropped
    with one warning; a body over 1 MiB is sent without its column facets or dropped; at most 64 events wait (the
    oldest is dropped). It sends dataset names (database, schema, table, topic; host and port of your Postgres,
    Kafka and warehouse, never user, password or DSN parameters), column names and their lineage, the PII class per
    column name, declared retention (when a node carries the `retention` tag; none does yet), layer owners' user
    ids, action names, pipeline and proposal ids and step indexes. Never a row or record value, a config value or a
    step's params beyond the topic and table names it matches against the map, node health or facts, room content
    or an action's output (the room clears `output` before the hook, `room.Deps.OnActionResult`).
    `OPENLINEAGE_API_KEY` goes only in the `Authorization: Bearer` header, only over https (or http to a loopback
    host; `config.ParseOpenLineage` refuses the rest at start), and is never logged or put in a body. Every
    pipeline of the server lands in one namespace (`OPENLINEAGE_NAMESPACE`): with several tenants, use one lineage
    server per tenant or leave it off. Left out: a pipeline with its own `connections:` block (its datasets are not
    the process-wide ones), MySQL sources, dbt models, and a sink table of a warehouse other than `WAREHOUSE_KIND`.
- **Staging (X8; optional, only with a pipeline's `owners.yaml` `staging:` block and write mode on; contract in
  `internal/actions/action.go`, `docs/api.md` "X8 staging").** When the driver clicks "Try in staging", the server
  writes the proposal's change to **your staging** Kafka Connect or Schema Registry with the staging credentials you
  reference in `staging.connections` (resolved like `connections:`, never logged), watches the connector for 60
  seconds, then puts the staging connector's previous whole config back (a registered staging schema version cannot
  be removed and is reported as left over). It never calls a production endpoint: a staging URL equal to a
  production one is a load error. The room gets `proposal.staged`: outcomes, object names and staging's own error
  text, redacted and capped (see What reaches the model, below); never a config value, schema body, staging URL or
  credential. The staging connector's config passes through the server's memory for the restore only.
- **Fix pull requests (Q7; designed, not built on this commit; design `docs/design/fix-pr.md`, setup
  [fix-pr-github.md](fix-pr-github.md)).** Off unless `FORGE_KIND=github` and a pipeline lists `repos:` (both
  proposed). Then the server reads, from the repositories you list and only under their path globs, file text at a
  commit, the base branch head, and a PR's state, checks and statuses, through one GitHub App installation, on
  outbound HTTPS to the API host only, with tokens narrowed to one repository per call (design §4.2, §9). **File
  text under the globs reaches the model provider and the room log**, masked of secrets (`redact.Text`), scanned
  for instruction-like text, wrapped as untrusted, at most 400 lines per read; a file above 64 KiB is refused
  (design §9.1). After an approval, **the PR title, body and diff leave Piperoom for the repository**, where everyone
  with read access to it sees them: title and body are model text the approvers saw in full, plus a footer naming
  the room, the proposal and the approvers' names (design §6.5). Added lines, title and body holding a secret are
  refused; links only to the forge host and `PIPEROOM_PUBLIC_URL`, no images or HTML, `@` mentions neutralised;
  a public repository is refused unless `public_ok: true` (design §6.2 rules 6 and 7, §14 D3). `pr_status` returns
  no PR body, comment or review text (design §9.1).
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
| Stage results (X8) | per staged step: the outcome (`healthy`, `failed`, `not_applied`), the staging object's name, and staging's own text (a failed task's trace, the registry's message), redacted and capped by `actions.CleanStageResult` (detail ≤ 2 KiB), fenced as untrusted data. Written by `Stage` through the pipeline's `staging:` clients only (Connect, Schema Registry; write mode on), which change staging and put it back; never a connector config value, schema body, staging URL or credential | `internal/actions` (`Stager`, X8b), `internal/room` (`proposal.staged`, X8c) |
| Tool results | each tool's `Summary` text (never `Data`) | the tool's `Summary`, capped by `tools.MaxSummaryBytes` |
| `task_log_tail` summary | a window of the end of one Airflow task try's log, with secrets masked and fenced as untrusted data (the full masked tail is in `Data`) | `internal/tools/batchtools/logtail.go` |
| `connect_worker_logs` summary (Q3c) | Kafka Connect worker log lines from Loki that name one of the room pipeline's connectors: per entry its time, level, worker label, first line and at most two `Caused by:` lines (each cut to 400 bytes), newest last, secrets masked and fenced as untrusted data (all returned lines, at most 200 and 64 KiB, are in `Data`) | `internal/tools/logtools/logtools.go` |
| `repo_list_files`, `repo_read_file`, `pr_status` summaries (Q7d) | for a repository the room pipeline lists in `owners.yaml` `repos:`: the paths under the repo's globs with layer and size and the base commit; a window of at most 400 numbered lines (within 8000 bytes) of one file under the globs, **its text as written in the repository** except secrets masked line by line (and every line of a PEM private key block), control characters blanked, lines cut at 1000 bytes, fenced as untrusted data; a pull request's state, merge time and commit, whether its head changed, its check state and the masked names of failed checks (only PRs from `piperoom/` branches). The window is also in `Data`, never the whole file | `internal/tools/forgetools/forgetools.go`, `internal/tools/forgetools/readfile.go` |
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
| `source_row` (Q8n; registered for a `source_kind: mysql` source only) | what `pg_row` exposes, from the MySQL source over the same code (`source_neutral.go`, `pg.go`); it reads only when the source client can name the table's single-column primary key and `pk_column` is that key, and refuses otherwise (the MySQL client of this commit cannot name it, so the tool refuses every call until it can) | other column values; a lookup by any column other than the primary key |
| `source_long_transactions` (Q8n; registered for a `source_kind: mysql` source only) | unscoped, what `pg_long_transactions` exposes, from the MySQL client's `LongTransactions` (pid is the processlist id). In a room, the MySQL client's pipeline-scoped blocking-session read: for the sessions whose InnoDB transaction is older than the threshold and that hold or wait for a metadata lock on the pipeline's tables, processlist id, user, `program_name`, command and state, transaction age and the statement verb; every other long transaction only as a count (no `has_xid`: the read cannot tell writers apart) | query text beyond its first keyword; the identity of a session outside the pipeline's tables |
| `pg_publication_tables` | the publication name, the connector role, and only the pipeline's own source tables; the publication's other tables only as a count | other tables of a `FOR ALL TABLES` publication |
| `pg_toast_risk` (X1d; the optional `clients.ToastColumnReader` on the source client and `clients.PlaceholderCounter` on the dev Postgres warehouse, both found by type assertion) | for the room pipeline's own `source_tables` only: replica identity, the size of the table's TOAST relation, and each toastable column's name, catalog type, storage mode and `pg_stats.avg_width` (`pg_class`, `pg_attribute`, `pg_stats`; `internal/clients/postgres/toast.go`); the source connector's `unavailable.value.placeholder` (or the Debezium default); for at most 10 columns, the **number** of sink rows of the last 24 h whose column holds that placeholder, as text or as the hex text or base64 a binary column takes for it (`count(*)` only, 2 s each; `internal/clients/warehouse/placeholder.go`; other warehouses: "not available"). Both clients check the pipeline scope themselves before any I/O | any row value, any other `pg_stats` column (`most_common_vals`, `histogram_bounds`), other pipelines' tables |
| `warehouse_freshness` | max timestamp of the sink table, lag | row data |
| `warehouse_row` | **timestamp-named columns only** (`at` or `*_at`), by name, not by type. The key and every other column are dropped, from both Summary and Data (`wh.go`; `dbtools.go`). The same on Snowflake, BigQuery and Databricks, where column names come back lower-cased (`internal/clients/snowflake/snowflake.go`; `internal/clients/bigquery/bigquery.go`; `internal/clients/databricks/databricks.go`) | key value, all other columns |
| `clients.Reconciler` reader (the watcher's `count_drift` and `row_drift`, X1, and a recovery action's `Verify`, X2; not a tool) | row counts of a source or sink table over a time window, the primary-key values of at most 200 of the newest source rows in a time window (the watcher samples rows old enough to have reached the sink, and never a table keyed by a column that looks like personal data), and the named timestamp columns of at most 1000 keys (`internal/clients/clients.go`). Only for the pipeline's own `source_tables`/`sink_tables`. The keys stay in the caller's memory: the room gets a `DriftSignal` (`internal/events/events.go`) or an `agent.recovery_check`/`agent.verification` with counts and a window only. Implemented for the source Postgres and the dev Postgres warehouse (`internal/clients/pgconv/reconcile.go`, X1a; other warehouses do not have it yet): `RowTimes` first reads the named columns' type names from `pg_catalog` and refuses a column that is not `date`, `timestamp` or `timestamptz` before any row is read; a provenance receipt of a read by key shows how many keys (`<n keys>`), never which; an error never quotes a key or a value | every non-timestamp column value; key values in events, logs, receipts or the model |
| The watcher's other `row_drift` reads (X1b-fix: the source's `Freshness`, `clients.ColumnReader`, `clients.ToastColumnReader` on the source and `clients.PlaceholderCounter` on the sink, each found by type assertion; not a tool) | for the pipeline's own `source_tables`/`sink_tables` only: the newest `sink_ts_column` of a source table (`max()`, to tell an idle sink from one that is behind); both tables' column names and catalog types, compared as name sets (the names both sides had are kept in the watcher's memory until a restart); the source's toastable column names and, for at most 4 columns per table and run (never `sink_ts_column` or a column whose name looks like personal data), the **number** of sink rows of a one-hour window holding Debezium's default `__debezium_unavailable_value`; the same key sample as the Reconciler row, read back on the sink for `sink_ts_column` only, to count rows that landed with it NULL. The room gets counts and column names in a `DriftSignal` | any row value (the placeholder is matched in the database, never returned); key values in events, logs or the model |
| `pg_resnapshot_rows` range reads (`RangeColumn`, `CountRange` on the source Postgres and the dev Postgres warehouse, X2a; the recovery action's `Validate`, `Preview` and `Verify`, not a tool) | one column's catalog type name and whether it alone is the table's primary key (`pg_catalog`), and the **number** of rows of a source or sink table whose key or timestamp column is in the step's range (`count(*)` only, 10 s; `internal/clients/pgconv/rangecount.go`). Only for the pipeline's own `source_tables`/`sink_tables`, checked by the client before any I/O. The preview carries the count (`count`, `unit: rows`) and names; `Verify` returns source and sink counts only (`agent.recovery_check`/`agent.verification`). The range's bounds are step params the approvers see on the proposal; the re-snapshot condition built from them (`clients.SnapshotConditionWriter`, `internal/clients/postgres/writer.go`; `clients.SnapshotConditionSignaler`, `internal/clients/kafka/signal.go`) goes only into the connector's signal row or signal record, never into an event, a log, a provenance receipt (`<bound>`, `<signal data with a row condition>`) or an error | every row value; the condition text outside the signal; other pipelines' tables |
| `clients.WorkerTaskReader` reader (the `connect_task_balance` tool, X6b) | for the whole Connect cluster, one `GET /connectors?expand=status`: worker id (host:port), connector name, type, task id and state (`internal/clients/clients.go`). Cluster metadata, not filtered by pipeline: the tool names only the room pipeline's connectors and counts every other connector's tasks per worker | task traces, connector configs; other pipelines' connector names, types and states |
| `connect_task_balance` (X6b; over `clients.WorkerTaskReader`, `Config` and `TopicOffsets`) | per worker seen (host:port): its task count, the room pipeline's tasks by connector name, task id and state, and every other connector's tasks as one count; the spread (max/min per worker, an imbalance finding), the unassigned task count; for each of the pipeline's sink connectors (at most 10) `tasks.max` and the partition count of its topics when they are all the pipeline's (at most 20 topics), and the idle tasks (`internal/tools/kafkatools/task_balance.go`). From the sink's config only `tasks.max`, `topics` and whether `topics.regex` is set are read | task traces; every other config value; other pipelines' connector names, types, states and topic names |
| `debezium_snapshot_progress` (X6b; the JMX exporter's snapshot metrics through `internal/clients/prom`, and the optional `EstimatedRows` on the source Postgres) | for one of the room pipeline's source connectors (its `topic.prefix` must be the pipeline's): whether a snapshot runs, is paused, completed or was aborted, its age and paused time, rows scanned in all and per pipeline source table, the scan rate, table counts, and an ETA from `pg_class.reltuples` estimates of the pipeline's own source tables (at most 50, catalog only); when the endpoint does not answer, that fact and the error naming the host (`internal/tools/debeziumtools/snapshot.go`, `internal/clients/postgres/estimate.go`) | any row; other connectors' metrics; table labels of tables outside the pipeline (counted in the rate only); the metrics URL's path and query |
| `connect_worker_metrics` (Q3b; Kafka Connect's own JMX metrics from the same `DEBEZIUM_METRICS_URL` endpoint through `internal/clients/prom`, names under `CONNECT_METRICS_PREFIX`, default `kafka_connect_`) | for one of the room pipeline's connectors (checked against `owners.yaml` before the scrape; no Connect call): per task id its record errors, failures, records skipped, retries, errors logged, DLQ produce requests and failures, and the age of its last error; the connector's total, running, failed, paused, unassigned and restarting task counts; the scraped worker's own counts for all its connectors (task and connector startup attempts and failures, connector and task counts, numbers only); the worker's rebalance state (rebalancing, completed rebalances, epoch, last, average and maximum rebalance time); the full names of required metric families the endpoint does not export (`internal/tools/debeziumtools/worker.go`) | any record or error text; other connectors' per-task or per-connector numbers; any label value other than a numeric task id (worker, leader or host names); the metrics URL's path and query |
| `connect_worker_logs` (Q3c; Grafana Loki `GET /loki/api/v1/query_range` through `internal/clients/workerlogs`, at `WORKER_LOGS_LOKI_URL` with the optional `WORKER_LOGS_TOKEN`) | for one of the room pipeline's connectors (checked against `owners.yaml` before any I/O): the newest worker log entries (a line with its joined stack trace) in the streams the operator's `WORKER_LOGS_SELECTOR` picks that contain the connector name (with a task: `<connector>-<n>` or `[<connector>\|task-<n>]`), at WARN or above by default, from at most 7 days back; at most 200 entries and 64 KiB of text, each entry cut to 16 KiB; per entry its time, level, logger, worker label (`pod`, `instance`, `host` or `container`) and text. The text is worker output and **can contain record values, SQL and hostnames**: it is sanitized, masked of secrets (`redact.Text`), scanned for instruction-like text and fenced in the summary (`internal/tools/logtools/logtools.go`). The model chooses only the connector, task, level, start and line count; the stream selector comes from configuration and the model's values are escaped as LogQL string literals. One request, 10 s, answer read to 4 MB; the token goes only to the configured host; the provenance receipt shows the host, the path and the LogQL query | lines that do not name the connector (other connectors' lines, worker-wide lines, Debezium lines that name only the topic prefix); other streams; the token |
| `forge.ForgeReader` and `forge.Writer` (Q7b; GitHub or GitHub Enterprise REST through `internal/clients/forge`, as one GitHub App installation; not a tool: the repo tools of Q7d and the PR action of Q7c read through it, and nothing is wired yet) | for one repository a pipeline lists in `owners.yaml` `repos:`: its visibility, archived flag and default branch (`GET /repos/{owner}/{repo}`), the commit its base branch points at, the regular files of the tree at a commit (path, blob id, size; at most 1000), one regular file's content at a commit (at most 64 KiB; directories, symlinks and submodules refused; the content is checked against its blob id and returned unmasked to the caller: the Q7d tools mask and fence it), and a pull request's state, head branch and commit, merge commit and time, and its head's check-run and commit-status names and results (at most 10 failed names, each cut to 100 bytes and masked of secrets). Every installation token is narrowed to that one repository and to `contents`, `pull_requests`, `checks`, `statuses` read (the writer: `contents` and `pull_requests` write); the App's private key, its JWT and the tokens are held as secrets, sent only to the configured API host, never logged; redirects are refused, 10 s per request, every answer read to 4 MB. **The writer sends data out**: with write mode on, the whole new content of each changed file (at most 10 files of 64 KiB), the commit message, the PR title and body go to the repository, on a new `piperoom/...` branch only; it never merges, pushes to another branch or deletes a branch it did not just create. The provenance receipt shows the API host and the request path with `?ref=<commit>`, never a token (`internal/clients/forge/client.go`) | PR bodies, comments and reviews; files outside the pipeline's repos (a token cannot reach another repository); CI and repository-governance paths (`.github/`, `CODEOWNERS`, ... are refused by `CheckRepoPath`); the key, the JWT and the tokens |
| `repo_list_files`, `repo_read_file`, `pr_status` (Q7d; over `forge.ForgeReader`; registered only when a forge is configured, not wired yet) | the room pipeline's repos only, each repo id and path checked against `owners.yaml` `repos:` and its globs (and the fixed CI and governance deny list) before any I/O; a call without a room pipeline is refused. `repo_list_files`: the files at the base branch head that match the globs (path, layer, size, blob id), paths scanned for instruction-like text and fenced. `repo_read_file`: one regular text file of at most 64 KiB at a commit (default the base head): header with layer, base commit, blob id, size and line counts; a window of lines masked of secrets, scanned for instruction-like text (`internal/injectscan`) and fenced; binary files, Git LFS pointers, symlinks, submodules and larger files are named, never shown. **Repository files can contain anything a committer wrote**, including hostnames, internal URLs and secrets the masks miss. `pr_status`: state, head and merge commit, merge time, whether the head moved since Piperoom wrote it (when the room supplies it), check state, total and failed names (masked, cut to 100 bytes, scanned, fenced), only for PRs whose head branch starts with `piperoom/`. 10 s per call (`internal/tools/forgetools/`) | files outside the globs; other repositories; the bytes of a file outside the window; PR bodies, comments, reviews and other people's pull requests; the key and tokens |
| `repo_open_pull_request` action (Q7c; Validate and Preview over `forge.ForgeReader`, Execute over `forge.ForgeWriter`; `internal/actions/fixpr`; not registered yet: wiring is Q7f) | **Reads**, for the one repo of the room pipeline's `repos:` the step names: its visibility and archived flag, each changed file at the step's `base_sha` (regular text files of at most 64 KiB; binary files, Git LFS pointers, symlinks and submodules refused), the base branch head and, for the "base branch check", each changed file at that head (blob ids compared). The preview holds the step's whole unified diff (3 lines of context), **every line masked of secrets** (`redact.Text`, and every line of a PEM private key block), the hosts of URLs in added lines and risk warnings (a new base image, a download, a pipe into a shell, a TLS check turned off, a widened ACL, instruction-like text) that quote no file text but a masked image name. **Writes, after approval and with write mode on**: the whole new content of each changed file (at most 10 files, a diff of at most 128 KiB), a commit message (the title plus the room and proposal ids), the PR title and body and a footer naming the room (its link with `PIPEROOM_PUBLIC_URL`), the proposal, the step and the approvers' display names, to the repository on a new `piperoom/<room>-<proposal>-<step>` branch. Added lines, title and body are refused when they hold a redaction hit (secrets, e-mail addresses), a redaction marker or a credential-like key in plain text (only `${...}` references pass); title and body also when they hold an image, HTML or a link to a host other than the forge and Piperoom; `@` mentions are neutralised; a public repository is refused unless `public_ok: true` (`internal/actions/fixpr/`) | files outside the globs or under the CI and governance deny list; deletes, renames and binary files; other repositories; merges, pushes to an existing branch, reviews; a secret already in a repository file, in the preview |
| `connect_source_offsets` (Q5a; Kafka Connect `GET /connectors/{name}/offsets`, Kafka Connect 3.5+, through `internal/clients/connect`) | for one connector of the room's pipeline, or each of them when none is named (checked against `owners.yaml` before any request): each stored `{partition, offset}` pair, for Debezium a source position such as an LSN and transaction id or a binlog file and position. Keys must be short and printable; string values are masked of secrets (`redact.Text`), folded to one line and cut to 256 bytes; numbers and booleans as they are; a key that looks like a credential is shown as `***`. At most 32 keys per map, 100 entries per connector, 500 in all. On a Connect without the endpoint, the version `GET /` reports (`internal/tools/kafkatools/connect_offsets.go`) | record data (the offsets API returns none); nested values, keys that are long or unprintable (counted); other pipelines' connectors |
| `connect_worker_info` (Q3a; the optional `kafka.ConnectClusterReader` on the Kafka client: `kadm.ListGroups`, `kadm.DescribeGroups`, `kadm.Metadata`, `kadm.DescribeTopicConfigs`; plus Connect's `GET /connector-plugins`, `GET /` and `GET /admin/loggers`) | the Kafka Connect worker groups (protocol type `connect`, at most 50) of the room's Connect cluster: a group whose member assignment names one of the room pipeline's connectors, a group id the pipeline declares, or the only Connect group in the cluster; per worker (at most 200) its member id, client host, advertised REST URL from the join metadata (kept only when it is an http(s) URL without credentials), leader flag and the pipeline's own connectors it runs, every other connector as a count; the Connect internal topics named by `CONNECT_INTERNAL_TOPICS` or, when unset, every topic whose name holds the word `connect` and looks like one (`connect-configs`, `my_connect_offsets`, `connect-cluster-status`; never a `__` topic; at most 50 read) and ties them to a shown group by Connect's naming (a name derived from the group id, e.g. `<group>-configs`; a generic name such as `connect-configs` only when exactly one Connect group has no topics named after it), with partitions, replication factor, effective `cleanup.policy` and `min.insync.replicas`, and the broker count; plugin classes, types and versions and the worker's Kafka version (as `connect_plugins`); logger names and levels of the worker at `CONNECT_URL`. 10 s per read (`internal/clients/kafka/connect_cluster.go`, `internal/clients/connect/loggers.go`, `internal/tools/kafkatools/worker_info.go`) | any record, offset or config value stored in the internal topics (they are never consumed); any other topic config value; other Connect groups (a count only) and other pipelines' connector names; in a scoped room, name-matched internal topics not tied to a shown group (another Connect cluster's, or no cluster's: a count only); the members' client ids and raw assignment bytes |
| `net_probe` (Q6a, registered in rooms by Q6b; DNS lookups, TCP connects and TLS handshakes from the room service's own network position through `internal/tools/nettools`, dials guarded by `connprobe.Guard`; Kafka `kadm.Metadata` for the advertised listeners; Kafka Connect `PUT /connector-plugins/{class}/config/validate` with the source connector's running config) | for one target label of the room's pipeline (`source`, `kafka`, `connect`, `schema-registry` or `warehouse`; the model names a label, never an address): the endpoints from the client set's own settings, the pipeline's literal `connections:` values and its connectors' `database.hostname`/`database.port`; per endpoint the resolved addresses, whether TCP connects, and for TLS endpoints the certificate's subject, SANs, issuer and expiry and whether the chain is trusted by the configured CAs; for `kafka` each broker's advertised `host:port` and whether it is reachable from here; for `source` how many errors Connect's validate reports and only its host and port messages, redacted; step times; at most 48 checks, each problem a clipped, redacted system error (`internal/tools/nettools/`). 3 s per step, 10 s in all | any credential (none is sent; no client certificate is presented); connector config values other than the host and port messages of the validate answer; other pipelines' or other client sets' endpoints; any address the model supplies |
| The recovery action `kafka_replay_range` (X2b; not a tool; reads through `kafka.ReplayReader`: whole records, start/end offsets and offsets by time, `clients.RateReader`) | its preview: per-partition offset ranges and counts, the topic and the pipeline's `reprocess_topic`, and the keys of the first 3 records, redacted as `dlq_replay`'s (text keys pass `redact.Text` and are cut at 60 characters; binary keys by size only); its result and `Verify`: counts, offsets and topic names. Execute holds whole records (key, value, headers) in server memory, 1 000 at a time, only to copy them into the `reprocess_topic`; it writes no other topic (`internal/actions/kafka_replay_range.go`, `internal/clients/kafka/writer.go`) | record values, headers, any key beyond the first 3 (redacted) |
| The watcher's sample readers (X3a, `internal/watch/forecast.go`; not tools) | numbers it already reads (a slot's retained WAL and `safe_wal_size`, DLQ start/end offsets, consumer-group lag totals); when a client implements them, the optional `clients.DBSizeReader` (`pg_database_size` of the source database) and `clients.UnreadAgeReader` (per pipeline topic, the timestamp of a group's oldest unconsumed record), and that topic's `retention.ms` through `clients.TopicConfigReader`. Only the pipeline's own slot, DLQ topics, groups and CDC topics; each call 10 s. Stored as one number per metric in `watch_samples`; a forecast's evidence names the slot or topic and carries numbers only | records, row values, other topics' names or ages, any other topic config value |
| `clients.ColumnReader` reader (column lineage X7a and PII tags X7c, through the map builder and the watcher's `row_drift` column choice; not a tool) | column name, catalog type name, position and nullability of the pipeline's own `source_tables`/`sink_tables`, at most 1600 per table, 10 s (`internal/clients/clients.go`). Implemented for the source Postgres and the dev Postgres warehouse from `pg_catalog` (`pg_class`, `pg_attribute`, `format_type` without the type modifier; `internal/clients/pgconv/columns.go`, X7a); the client checks the table name and the pipeline scope before any I/O and runs no query on the table itself. Other warehouses do not implement it yet: their sink hops have no column lineage and their sink tables no `pii` tag. The PII classifier (`internal/redact/columns.go`, X7c) sees only these column names and type names, corrected by the pipeline's `owners.yaml` `pii:` include/exclude; the watcher's `row_drift` never samples a column it or `pii.include` names. The map builder also reads each pipeline topic's latest `<topic>-value` schema from Schema Registry and keeps only its field names (`internal/topology/lineage_smt.go`); with the connectors' `transforms` class and field-list settings (`ExtractNewRecordState`, `ReplaceField$Value` `renames`/`exclude`/`include`), these give each edge's column map | any row or value, column defaults and comments, schema defaults and docs, any other connector config value; other pipelines' tables |
| `clients.TableRetentionReader` reader (the watcher's retention check, X7d; not a tool; no warehouse implements it yet) | whether a sink table has a configured retention, its duration and the name of the setting read | any row or value, the setting's expression |
| `clients.TopicConfigReader` (the watcher's retention check, X7d; the same read as `kafka_topic_config`) | for a pipeline's own topic: `retention.ms`, `retention.bytes`, `cleanup.policy`, `min.insync.replicas`, partitions, replication factor; the room sees only `config_risk` retention entries (`events.RetentionRisk`: durations and the policy word) | record payloads; any other topic config value |
| Migration history readers (X4b; the optional `clients.MigrationReader` on the source client, read by the tool `what_changed`; not a tool of their own) | for the schemas of the pipeline's own `source_tables`, from a table named `flyway_schema_history` or `databasechangelog` found by a catalog lookup: version or changeset id, description (cut at 300 characters), applied time, who applied it and a success flag, at most 200 rows, 10 s each (`internal/clients/postgres/migrations.go`); with a dbt reader, the latest run's time, `invocation_id` and counts of the pipeline's models per status. Read live, not stored; the description is untrusted text from your own migration tool, shown to the model inside the untrusted-data wrapper. A history table the login cannot read is a fact ("grant SELECT on ..."), not read. Only those two default table names are looked for | the script's name, body or checksum, any other column of the history table, any row of another table |
| PII classifier (X7c; `redact.ClassifyColumn`, `internal/redact/columns.go`; not a tool, no I/O of its own) | it reads no system: its input is the column names and type names the `clients.ColumnReader` row above returned, plus the pipeline's `owners.yaml` `pii:` include and exclude lists. Its output is the `pii` tag (`column:class` pairs, at most 2048 bytes) on the pipeline map's table nodes, and a "never read this column" decision in the watcher's `row_drift` (`redact.PersonalColumn`). Names only: it cannot see that a column named `notes` holds an address, and it never sees a value | any row, value, default or comment |
| OpenLineage emitter (X7b, `internal/discover/openlineage`; optional, off unless `OPENLINEAGE_URL` is set; not a tool, reads no system of its own) | its input is the pipeline map the room service builds and each `action.executed` step (action name, its topic and table params, proposal id, step index, ok or outcome unknown; never `output`). It POSTs to the lineage server named in `OPENLINEAGE_URL`, from the elected leader replica only: per pipeline at most one `RunEvent` every 15 minutes with dataset names (database, schema, table, topic; host and port of the process-wide `SOURCE_DB_URL`, `KAFKA_BROKERS` and warehouse, never user, password or DSN parameters), column names and their lineage, the PII class per column name, declared retention when a node carries it (none does yet) and layer owners' user ids; per executed step one `RunEvent` for the job `piperoom.<action>` with the pipeline and proposal ids, the step index and the datasets of that pipeline's map its params name. 10 s per POST, 3 retries on a network error, 429 or 5xx, then one warning and the event is dropped; bodies at most 1 MiB, responses read to 4 MB and discarded; dialled through `connprobe.Guard` (no link-local or cloud-metadata address), no proxy, no redirect | row or record values, config values, other step params, node health or facts, room content, an action's `output`, the API key outside the `Authorization` header (sent only over https or to a loopback host); pipelines with their own `connections:` block and dbt models (left out; a MySQL source table is named `{database}.{table}` under `mysql://host:port` since Q8h) |
| `pipeline_map` | the pipeline's tables, connectors, topics, consumer groups and sink tables, with owners and health (`map.go`); with X7, column names and their lineage per edge and `pii` tags per table node (the contract's `retention` tag is not set yet; `docs/design/lineage.md`); for a `source_kind: mysql` pipeline (Q8d) the source node's facts come from the MySQL client's binlog status instead of a replication slot: the oldest binlog file name (cut to 24 characters), the file count, `binlog_expire_logs_seconds` or the RDS binlog retention hours, and `gtid_mode` | row data; for MySQL, the GTID sets, server id and UUID and binlog contents (not put in the map) |
| `mysql_binlog_status` (Q8c, `internal/tools/mysqltools`; MySQL rooms only, registered by Q8g; reads `BinlogStatus` on the MySQL source client and, through `internal/clients/connect`, each in-scope connector's config, status and stored offsets) | the source server's binlog write position, `gtid_mode`, `gtid_executed` and `gtid_purged` (whole in Data, at most 200 bytes each in the summary), server id and UUID, the binlog file names and sizes (at most 500, oldest kept; a name that is not a binlog file name is dropped), `binlog_expire_logs_seconds` or the RDS binlog retention hours; for each of the room pipeline's Debezium MySQL connectors (or the one named): its state, `database.server.id`, and the stored offset's `file`, `pos`, `gtids`, `server_id`, `ts_sec` and `snapshot`; the verdicts computed from them | binlog event contents; any other connector config key (`database.password`, `database.user` and the rest are read but never shown); connectors or servers outside the room's pipeline; any host, file or GTID the model names (the tool takes none) |
| `kafka_schema_history` (Q8e, **not built on this commit**; design `docs/design/mysql-source.md` §7.3) | the schema history topic named by the in-scope connector's config: whether it exists, partitions, retention and cleanup policy, and the last at most 20 records' time, database name, position and **DDL text** (cut to 200 bytes, redacted, wrapped as untrusted) | any other topic; record values of the captured tables |
| `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag` | topic/group names, offsets, lag, only for the room pipeline's own topics and consumer groups (`kafka_consumer_lag` shows only partitions of the pipeline's topics) | record payloads |
| `kafka_topic_config` | for a pipeline's own topic: retention, cleanup policy, min in-sync replicas, partition count and replication factor (`kafka_tools.go`) | record payloads |
| `kafka_find_by_key` | topic, partition, offset, timestamp and **key** of matching records | record **values** |
| `kafka_dup_keys` (X1c; the optional reader `clients.DupScanner`, `DupScan` on the Kafka client, also the watcher's `dup_keys`/`order_skew` reader, which copies counts only into its signal) | for one of the room pipeline's CDC topics since a time: records scanned, duplicate `(key, source.lsn)` events and per-key `source.lsn` regressions as counts, per-partition offsets, and for at most 20 findings the partition, offsets, `source.lsn` positions and a key **fingerprint** (`sha256:` and 16 hex digits of the raw key bytes; a short key such as an integer id can be guessed back from it) (`internal/clients/kafka/dupscan.go`). The Kafka client reads whole records (at most 50 000, 10 s) to find `source.lsn` with its own bounded Avro/JSON walker, not the Avro library's decoder, and drops them | key values, record values, every field other than `source.lsn` |
| `kafka_throughput` (X6a; over `clients.RateReader`, `GroupLag` and the pipeline's `group_lag` watch samples, `clients.SampleReader`, when the room store has them) | for the room pipeline's own topics over a window: records written per partition and per second from offsets by record timestamp (no record is fetched), each declared consumer group's state, member count and lag on those topics with the partition holding the most lag, and, from two `group_lag` samples, the pipeline's consumed against produced records and the lag's change per minute (`throughput_tools.go`). A group's partitions on other pipelines' topics are left out (when the group reads any, no consumed rate is given) | record keys and values; other pipelines' topic names |
| The performance action `connect_tune_throughput` (X6c; not a tool; reads `Status`, `Config`, `TopicOffsets`, offsets by time over `clients.RateReader` and the pipeline's `group_lag` watch samples, `clients.SampleReader`) | from the connector's config only the allow-listed keys (`actions.TunableKeys`, integers), `topics`/`topics.regex`, `connector.class` and `consumer.override.group.id`; the partition count of a sink's topics; record counts per partition between offsets by timestamp (no record is fetched) and `group_lag` samples. Its preview carries each changed key's value now and the proposed value (`tuning`, the rollback) and Connect's `validate` verdict; its result names keys and integer values; `Verify` returns throughput and lag slopes in records per minute only (`agent.verification`), its detail names the connector, topics and window times (`internal/actions/tune_throughput.go`). The validate dry run sends the whole config only when no key looks secret, as `connect_update_connector_config` does | every other config value; record keys and values; other pipelines' topics (a sink reading one is refused before any read) |
| `trace_record` | per-stage status and facts for one `(table, key)` (`map.go`) | row values |
| `connect_*` | connector state and task errors (**stack traces can contain data**). Config: a fixed list of key names (`connect_tools.go`), the `transforms` name list and each transform's `type`, `field`, `fields` and `field.name` (every other `transforms*` value is `***`), and secret-looking keys (`password`, `secret`, `token`, `.key`) as `***`. A secret key's presence is shown, not its value (`connect_tools.go`) | secret values |
| `schema_registry_*` | subjects, versions, schema definitions and diffs | data |
| `runbook_lookup` | sections of the runbook library embedded in the binary (`internal/tools/runbooks/runbooks.go`); since Q1d the runner adds the running versions it read once per room with `connect_plugins` (the worker's Kafka version and each Debezium connector plugin's version, digits and one release word only, `internal/agent/versions.go`) to the call's input | pipeline data, network |
| `connect_plugins` (Q1c; Connect `GET /connector-plugins?connectorsOnly=false` and `GET /`) | worker-wide, not per pipeline: each installed plugin's class, type and version (connectors, converters, transforms, predicates; at most 200 rows in Data), counts by type, and the Kafka version and commit the worker reports, each version string checked for control characters and capped (`internal/tools/kafkatools/connect_plugins.go`) | connector configs and status; any record |
| `dag_runs` | the last runs of one pipeline DAG (default 10, at most 100): run id, state, run type, logical date, start, duration and the run's `note` (free text, sanitized, redacted and scanned; the full note is in `Data`) (`internal/tools/batchtools/runs.go`) | row data |
| `task_instances` | the task instances of one run: task id, state, try, operator, start and duration; every name is cleaned and cut (`runs.go`) | row data |
| `task_log_tail` | the end of one task try's log, at most 32 KiB (default 16 KiB tail): invalid UTF-8 replaced, ANSI sequences dropped, secrets masked (`redact.Text`), and scanned for instruction-like text. The summary shows a fenced window of the end; `Data` holds the whole masked tail (`internal/tools/batchtools/logtail.go`) | the log before the tail window |
| `dag_schedule_gap` | the DAG's schedule text (DAG-author text, shown as one line), paused flag, last run and last success, next run, lateness and a verdict (`internal/tools/batchtools/schedule.go`) | row data |
| `dbt_run_results` | each node's status, rows and run time, and dbt's `message` (masked and scanned; one line in the summary, at most 2000 bytes in `Data`); failures first, scoped to the pipeline's models and their direct neighbours (`internal/tools/dbttools/run_results.go`) | results of models outside scope (counted) |
| `dbt_model_lineage` | one model's direct parents and children, as unique_ids, at most 100 each (`internal/tools/dbttools/lineage.go`) | other nodes |
| `dbt_model_sql` | one dbt model's compiled SQL (raw Jinja when dbt never compiled it), secrets masked, at most 64 KiB in `Data`, shown in windows in the summary; **never executed** (`internal/tools/dbttools/model_sql.go`) | nothing else |
| MySQL source client (Q8b, `internal/clients/mysql`, a `mysql://` `SOURCE_DB_URL`; not a tool of its own: the `source_*` tools, `mysql_binlog_status` and the source actions read through it once Q8c, Q8g and Q8n land) | through the read user only, on sessions with `transaction_read_only = 1` and `max_execution_time = 10000`, 10 s per call: the CDC settings of `docs/design/mysql-source.md` §6.2 with their persisted values (`performance_schema.global_variables`, `persisted_variables`; on RDS/Aurora the binlog retention hours from `mysql.rds_show_configuration`); for the tables the call names only, whether each exists, has a primary key, and its storage engine (`information_schema`); for one user name, its accounts (`user@host`), locked and expired flags and its SELECT, `REPLICATION SLAVE`/`CLIENT`, `RELOAD`, `LOCK TABLES` and `SHOW DATABASES` privileges from named columns of `mysql.user` and from `mysql.db`, `mysql.tables_priv`, `mysql.role_edges` and `mysql.default_roles`; open InnoDB transactions and metadata-lock holders: processlist id, transaction start, user, `program_name` connection attribute, command and state, and the statement (`PROCESSLIST_INFO`, untrusted: redacted (N5) and cut at 200 bytes; for `LongTransactions` only its first keyword); the binlog write position, GTID mode and sets, server id and UUID, `binlog_expire_logs_seconds`, and the binlog file names and sizes (at most 500, oldest kept); one row by primary key (`SELECT *`, values untrusted: bytes that are not UTF-8 become `<binary N bytes>`, and the tool redacts and wraps them as `pg_row` does) (`internal/clients/mysql/`) | `authentication_string` or any password hash (never selected; the read grant of design §10.2 is column-level); `SHOW GRANTS`; binlog event contents (no replication connection is opened); in the pipeline-scoped read, the user, program and statement of a session holding or waiting for no lock on the pipeline's tables (blanked in SQL); any URL parameter other than `tls`, `tls_ca` and `tls_server_name` (refused at start-up) |
| MySQL source admin writer (Q8f, `internal/clients/mysql/writer.go`, the `SOURCE_DB_WRITE_URL` pool, built only with write mode on; not a tool: the `source_*` actions call it after approval) | before a grant, the hosts of the one user name being granted (`mysql.user` `User`, `Host`; with more than one, the refusal names every `user@host`); before ending a session, that one processlist id's user, host, command, thread type, `program_name` and transaction start, and the writer's own `CURRENT_USER()`; on RDS/Aurora after a denied `KILL`, `@@basedir` | any other account, password hash or grant; the statement text of the session it ends; row data |

Things to know:

- **Keys are shown.** If your primary key or Kafka key is personal data (an email, a phone
  number), it will appear in the room. Prefer pipelines keyed by surrogate ids for the pilot.
- **Statement text and error traces are free text.** DDL statements and Connect task errors
  can contain literal values. Check yours (for example, `CREATE ROLE ... PASSWORD` in the
  DDL audit log, or a deserialization error that echoes a record).
- **Only some `transforms*` settings are shown.** The `transforms` name list and each
  transform's `type`, `field`, `fields` and `field.name` appear; every other transform setting
  is shown as `***`. Still put secrets in config providers, not in transform settings.
- **MySQL query text and DDL are free text** (when the MySQL source is built). A blocking session's
  `PROCESSLIST_INFO` and the schema history topic's DDL can hold literal values; both are masked of secrets,
  cut to 200 bytes and wrapped as untrusted, but the rest is shown as data. `program_name` is client-set
  free text, like `application_name` ([mysql-users.md](mysql-users.md); design `docs/design/mysql-source.md` §6.5,
  §7.3, §11).
- **`application_name` is free text.** The client sets it (any session can put anything in it),
  and `pg_long_transactions` shows it as-is.
- **Worker log lines are free text.** `connect_worker_logs` returns what the Connect worker logged about a connector; a
  converter or sink error can echo a record value or a SQL statement. The lines are masked of secrets and scanned, but
  the rest is shown as data. Point `WORKER_LOGS_SELECTOR` at the Connect workers' streams only, and give Loki a read-only token. The token scope, the
  proposed settings and the order of the protections are in [worker-visibility.md](worker-visibility.md).
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
- When the status-page poller is wired (section 1: not yet), set a `vendor_status_url` (X4) only to a status feed
  host your egress policy allows.
- When the pull-request fix is built (section 1: not yet), list in `repos:` only repositories whose files under
  the globs may go to the model provider, keep the globs to the files a fix should change, and keep `public_ok`
  off ([fix-pr-github.md](fix-pr-github.md)).
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
- [ ] If the pull-request fix will be used: may the files under each `repos:` glob go to the model provider, and may the PR title, body and approvers' names be published in those repositories?
- [ ] Who may view incident history, and for how long is it kept?
- [ ] Who are the approvers per layer, and are they reachable out of hours?
- [ ] How do we revoke Piperoom's access quickly (one place per system, and the Slack bot)?
