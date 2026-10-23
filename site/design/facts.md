# Piperoom: product fact sheet for the landing page

Source of truth: `origin/main` at `2d4895c0` (fetched 2026-10-22). Every claim names the file that proves it.
Paths are repo-relative. If the page says something that is not in this file, check the repo first.

---

## 1. Positioning and claims

**One sentence:** Piperoom is a self-hosted incident room for CDC/Kafka data pipelines: when a pipeline breaks, a room
opens, Claude investigates with read-only tools while your team watches and steers, and a fix runs only after every
layer owner it touches approves it.

(Pipeline in scope: Postgres → Debezium → Kafka → consumers → warehouse. Proof: `README.md`, `AGENTS.md` §1,
`internal/agent/prompt.go` `promptCDCIntro`.)

### Claims the page may make (each with proof)

| # | Claim (page wording) | Proof |
|---|---|---|
| 1 | **The room opens itself.** A built-in watcher spots a failed connector, an inactive replication slot or growing WAL and opens a room. Alerts from Alertmanager, Grafana, PagerDuty, Datadog or any signed webhook open one too. | `internal/watch/signals.go`, `internal/owners/watch.go` (signal ids), `docs/design/watcher.md` §3; `docs/api.md` `POST /api/alerts/{source}`, `internal/alertin/` |
| 2 | **The agent can read, but it cannot write.** Every investigation tool is registered in a read-only registry that refuses any non-read tool. Source Postgres reads run in a READ ONLY transaction that is rolled back. The agent cites each piece of evidence by step. | `internal/tools/tool.go` (`NewReadOnlyRegistry`, `ErrNotReadOnly`), `internal/clients/postgres` package doc, `internal/agent/prompt.go` (“You use read-only tools; you only propose fixes”, “Cite evidence by step id”) |
| 3 | **Everyone can watch, one person drives.** One driver steers the agent. Anyone else's message becomes a suggestion that the driver can send on or decline. The driver's lease lasts 30 minutes and renews whenever they act. | `docs/api.md` (`/steer`, `/suggestions/{sid}/send`, `/control/*`), `internal/room/service_steer.go`, `internal/room/service.go` (`DriverLease = 30 * time.Minute`) |
| 4 | **Nothing runs without its owners.** The agent only proposes. A fix runs only when every required layer owner has approved that exact proposal. Each approval carries a SHA-256 fingerprint of the steps, so an edited proposal voids it. Afterwards the agent checks that the pipeline recovered. | `internal/room/service_proposals.go` (`ApproveProposal`: `params_hash` required; a mismatch returns `409 params_mismatch`), `internal/room/params_hash.go`, `internal/agent/verify.go` |
| 5 | **Hand-offs and audit trail are built in.** At shift change the agent writes a hand-off summary (what broke, why, done, ruled out, still open, risks) and the driver role moves when the next person accepts. Every room is an append-only event log that exports as JSON with a SHA-256 integrity hash, plus a post-mortem draft. | `internal/room/service_handoff.go`, `internal/agent/prompt.go` `handoffPrompt`, `internal/events/events.go` `HandoffSummary`; `internal/room/service_export.go`, `docs/api.md` `GET /api/rooms/{id}/export`, `GET /api/rooms/{id}/postmortem.md` |

Safe supporting lines:
- “Every room replays live in the browser, then streams new events” (`docs/api.md` WebSocket `?after=<seq>`).
- “Untrusted text stays data”: alerts, logs and record values reach the model only inside an untrusted wrapper (`internal/agent/prompt.go` `wrapUntrusted`, `promptRulesTools`).
- “Try it silently first”: shadow mode (§3).

---

## 2. Explainer storyboard: the `schema-change` incident

Sources: `dev/scenarios.md` §1 (what breaks, symptoms, fix), `docs/demo/schema-change.md` (beats, roles),
`internal/room/demo/demo.go` (scripted steps, hypothesis, proposal, verification and hand-off text that `--demo`
plays), `web/src/fixtures/schema-change.json` (comment, suggestion and executor outputs),
`evals/scenarios/schema-change.yaml` (real tool output format, expected fix), `internal/agent/verify.go`
(verify and follow-up). `dev/owners.yaml` and `dev/users.yaml` were checked: **schema-registry → Priya,
kafka-connect → Arjun**, kafka → Arjun, postgres → Priya, consumer → Kabir, warehouse → Meera.

> **All clock times below are illustrative.** The repo has no measured end-to-end timings for a real agent run.
> Production watcher defaults: `connector_down` fires after the state has lasted 3 min, with a 60 s poll. The demo
> profile uses 30 s and 15 s, so its room opens about 1 min after the break (`docs/design/watcher.md` §4,
> `dev/owners.yaml`, `docs/demo/schema-change.md`).

Cast: **Arjun** (driver; owns kafka-connect and kafka), **Priya** (owns postgres and schema-registry), **Kabir**
(owns the consumer; watcher), **Meera** (owns the warehouse; takes over at shift change).

| Time (illustrative) | Beat | What happens (real names and strings) |
|---|---|---|
| 09:41:07 | **The break** | An app migration runs `ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)` and 2 orders are inserted. Debezium builds a new Avro value schema (`amount: int → bytes(decimal 12,2)`). The subject `pg.public.orders-value` has compatibility `BACKWARD`, so Schema Registry **rejects it with HTTP 409** (`MISSING_UNION_BRANCH`). **Connector task 0 goes FAILED** while the connector itself still reads RUNNING. |
| 09:41–09:44 | **Silent damage** | `pg-orders-connector: RUNNING, tasks 0=FAILED`. Slot `debezium_orders` goes `active=false` and retained WAL grows. Consumer `order-sync` is Stable with lag 0 (healthy: the problem is upstream). The warehouse table `analytics.orders` goes stale. No error reaches the app team. |
| ~09:44:30 | **The room opens** | The watcher's `connector_down` signal (sev2, because the task is FAILED) opens a room titled like `orders: connector pg-orders-connector task 0 FAILED 3m`. A `slot_inactive` signal later joins the same room. The header chip reads **alert · watcher** with a verified badge (`alert_verified_by: watcher`). *Alternative:* an Alertmanager, Grafana, PagerDuty or Datadog rule posts to `POST /api/alerts/{source}` (signed); the eval's alert title is `orders stale`. |
| +20 s | **First look** | A cheap triage model makes at most 2 reads and writes a verdict labelled *triage, unverified*. It can never propose a fix (`internal/agent/triage_pass.go`). UI card: **First look**. |
| +0:30 → +2:30 | **Investigation** (step stream; scripted `--demo` order and titles) | **s_1** “Check the Debezium connector” → `connect_status`: `Task 0: FAILED … incompatible with an earlier schema for subject "pg.public.orders-value" … error code: 409`, `Rejected schema vs pg.public.orders-value v1: amount: int → bytes(decimal 12,2)`<br>**s_2** “Inspect the Schema Registry subject” → `schema_registry_versions`: `versions [1] … compatibility BACKWARD`<br>**s_3** “Diff the attempted schema against v1” → `schema_registry_diff`: `amount: int → bytes(decimal 12,2)`<br>**s_4** “Check consumer lag” → `kafka_consumer_lag`: `order-sync: Stable, 1 members, total lag 0` (rules out the consumer)<br>**s_5** “Check the DDL log” → `pg_recent_ddl`: `ALTER TABLE public.orders: ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)`<br>**s_6** “Check what changed before the alert” → `what_changed`: the DDL 90 s before the alert, plus a connector config change and a schema version. These feed the **What changed** lane. |
| (during) | **Many eyes, one driver** | Kabir comments on s_1: “Wrong service? I'd expect Connect to be down, not the schema.” The agent replies in the thread: “Connect is up; task 0 is failed on a Schema Registry compatibility error.” Kabir suggests “Check the connector config first, not the topic.” He cannot steer, so it becomes a suggestion. Arjun (driver) clicks **Send to agent**. Agent: “Connector config is unchanged; the failure is the value schema subject.” |
| ~09:47 | **Hypothesis** (high confidence; cites s_1, s_3, s_5, s_6) | “A schema change to orders.amount broke the pg-orders-connector on a Schema Registry incompatibility; no new rows reach the warehouse.” Per the prompt, the first failing component is **kafka-connect** and the root-cause component is **postgres** (the ALTER). Agent message: “Root cause found: the schema change on orders.amount is incompatible under BACKWARD compatibility for pg.public.orders-value. I've written a fix proposal.” |
| ~09:47:30 | **Proposal** | Title: **“Set subject compatibility to NONE and restart the connector task”**<br>Step 1 `schema_set_compatibility` · layer `schema-registry` · `{subject: pg.public.orders-value, level: NONE}`: “Allow the new decimal schema to register under the same subject.”<br>Step 2 `connect_restart_task` · layer `kafka-connect` · `{connector: pg-orders-connector, task: 0}`: “Restart Debezium task 0 so it picks up the new schema.”<br>Rollback: “set compatibility back to BACKWARD”. Required layers: schema-registry, kafka-connect. A **fingerprint** (params_hash) shows on the card. Agent: “The fix proposal is waiting for the schema-registry and kafka-connect owners.” |
| ~09:49 | **Approvals** | Priya sees **Needs your approval · schema-registry** and approves. The header pill reads **Agent waiting · 1 of 2 approvals**. Arjun sees **Needs your approval · kafka-connect** and approves. Both approvals carry the same params_hash. |
| ~09:50 | **Executor** (needs write mode on) | Step 0 output: `pg.public.orders-value compatibility BACKWARD → NONE`. Step 1 output: `restarted pg-orders-connector task 0 (RUNNING)`. Status: **Executing fix · 1 of 2**, then **Fix ran · verifying**. With write mode off (the default) the fix is approved but does not run. |
| ~09:51 | **Verify-after-fix** | The agent re-reads the touched layers (`connect_status`; for the schema step, `schema_registry_versions` checks the compatibility level) and runs up to 3 `warehouse_freshness` reads, which must show new rows. Verdict (internal): “recovered: every layer check passed and data is flowing again (new rows arrived after the fix). This does not show that the sink has caught up.” Room message: **“Verification passed: pg-orders-connector task 0 is RUNNING and new rows reached analytics.orders. pg.public.orders-value compatibility is still NONE; restore BACKWARD with a separate fix.”** Status: **Fix ran · recovered**. |
| ~09:52 | **Follow-up proposal** | The agent opens a separate one-step fix, `schema_set_compatibility` `{subject: pg.public.orders-value, level: BACKWARD}`, layer schema-registry, which needs Priya's approval. A config-only verdict then reads “every layer check passed. The proposal only changed schema configuration, so data flow was not part of this verdict.” The page may name the title loosely (for example “Restore BACKWARD compatibility”); the repo has no fixed title for it. |
| ~10:00 | **Shift change: hand-off** | Arjun clicks **Hand off** (key H) and picks Meera. Status shows “Writing hand-off…” (the server allows up to 3 min). Summary (from `demo.go` `WriteHandoff`):<br>**What broke:** Debezium task 0 failed after orders.amount was altered to numeric(12,2).<br>**Why:** New schema is incompatible under BACKWARD compatibility for subject pg.public.orders-value.<br>**Done:** Set compatibility to NONE · Restarted task 0 · Warehouse caught up<br>**Ruled out:** Kafka brokers · Consumer group config<br>**Still open:** Consider pinning decimal.handling.mode (and the restore proposal, waiting for schema-registry)<br>**Risks:** BACKWARD compatibility is off until re-enabled<br>Meera clicks **Accept** and becomes the driver. Arjun can stay as a watcher. |
| ~10:03 | **Restore approved** | Priya approves the follow-up. Compatibility is back to `BACKWARD` and verified. |
| ~10:05 | **Close** | Meera (driver) closes the room with a one-line resolution, for example “ALTER on orders.amount rejected by Schema Registry (BACKWARD); set NONE, restarted task 0, data flowing, BACKWARD restored.” The room shows **Resolved · verified**. Post-mortem draft (Markdown) and **Export JSON** (with `X-Piperoom-Export-SHA256`) are available. Per `dev/scenarios.md`, the consumer reads both schema versions and the missed rows reach the warehouse, with no data loss in this scenario. |

Other reads the real agent may add, all real tools: `pipeline_map`, `connect_config`, `pg_replication_slots` (the
prompt requires confirming the slot still exists before a restart), `warehouse_freshness`, `trace_record`,
`similar_incidents`. The eval expects the fix `schema_set_compatibility` + `connect_restart_task` (or
`connect_restart_connector`), forbids `schema_register`, and caps the run at 15 tool calls
(`evals/scenarios/schema-change.yaml`).

Note on risk: the scripted demo labels this proposal `risk: "low"`, but the system prompt calls compatibility NONE
“high-risk; propose … only as a last resort”. On the page, show the rollback text rather than a risk level.

---

## 3. Trust facts

| Fact | Precise wording | Proof |
|---|---|---|
| Read-only by construction | Tools implement `Tool` with a `Kind`. The agent's registry (`NewReadOnlyRegistry`) refuses any non-read tool. Client interfaces are split into `*Reader` and `*Writer` (e.g. `ConnectReader` / `ConnectWriter`, `SchemaRegistryReader` / `SchemaRegistryWriter`, `SourceReader` / `SourceWriter`). Writes exist only in `internal/actions`. Source Postgres reads run in READ ONLY transactions that are rolled back. | `internal/tools/tool.go`, `internal/clients/clients.go`, `internal/clients/postgres` doc, `internal/tools/readonly_test.go` |
| Scoped to the pipeline | Every read tool and write action is scope-checked against the room's pipeline (connectors, topics, groups, subjects, tables from `owners.yaml`). Actions are checked at proposal time and again before they run. | `internal/tools/scope.go`, `docs/pilot/README.md` F4/F10/F11 |
| Approval per layer, bound to the exact params | Each step names a layer, and the proposal lists `required_layers`. The approver must own that layer (`users.yaml`). `params_hash` = SHA-256 of the RFC 8785 (JCS) form of {room, proposal, title, risk, rollback, required_layers, steps}. Approve without it → `400`; with a different one → `409 params_mismatch`. The UI shows it as a **Fingerprint**. | `internal/room/params_hash.go`, `internal/room/service_proposals.go`, `web/src/components/ProposalsPanel.tsx` |
| Fixes previewed | Each write action has a Preview (dry run where the API allows, e.g. Connect's validate endpoint) and a rollback text. | `internal/actions/preview.go` |
| Write mode is off by default | `PIPEROOM_WRITE_MODE` defaults to `off`: proposals and approvals work, but nothing runs (UI: “Approve (will not run)”, “Approved · waiting for write mode”). `--demo` forces it on. | `docs/pilot/README.md`, `internal/config/config.go`, `cmd/server/writemode.go` |
| Verify after fix | Recovery needs the layer checks to pass and new rows in every sink table. A NONE compatibility left behind triggers a follow-up. | `internal/agent/verify.go` |
| Secret redaction | `internal/redact` masks secrets in tool summaries, error traces and record values before they reach the prompt, the room log, Slack or an export (e.g. `postgres://app:[redacted 16]@db:5432/orders`). Redaction is **partial**: names and timestamps still appear. `pg_row` and `warehouse_row` drop non-key and non-timestamp values. | `internal/redact/redact.go`, `docs/pilot/README.md` F3 |
| Prompt-injection hygiene | Alert text and tool output are wrapped as `<untrusted>` / `<tool_output untrusted="true">` and escaped. Invisible Unicode is made visible. Suspected instructions are flagged and reported in the hypothesis, never followed. | `internal/agent/prompt.go`, `internal/injectscan/` |
| Grounded claims | Terms in the hypothesis that no cited step shows are marked (“Terms the cited steps do not show”). Hand-off summaries are checked against the room log, and violations are shown before Accept. | `internal/agent/grounding.go`, `internal/agent/handoff_check.go`, `web/src/components/room/HypothesisCard.tsx` |
| Append-only log | Every room is a sequence of events (`room_events`, `(room_id, seq)`). The store appends and reads; events are never edited. Whole rooms can be removed by retention (`PIPEROOM_RETENTION_DAYS`, off by default) or an admin delete, which is logged in `room_deletions`. | `internal/events/events.go`, `internal/store/store.go`, `migrations/00001_init.sql`, `docs/api.md` DELETE room |
| Hashed export | `GET /api/rooms/{id}/export`: header `X-Piperoom-Export-SHA256` = SHA-256 of the JCS form. Restricted to approvers or auditors. | `internal/room/service_export.go`, `docs/api.md` |
| Self-hosted | Docker image (`Dockerfile`), `deploy/docker-compose.pilot.yml`, Helm chart (`deploy/helm/piperoom`), Terraform for GCP (`deploy/terraform/gcp`). Per-pipeline credentials (`connections:` block, env/file references only). | `deploy/`, `internal/owners/tenancy.go` |
| SSO | OIDC (`PIPEROOM_AUTH_MODE=oidc`): authorization code with PKCE and nonce; server-side sessions; roles member, viewer and auditor. The image starts in oidc mode. Dev mode (pick a user, no password) is for local use. | `docs/pilot/README.md` F1, `internal/httpapi/auth_oidc.go`, `docs/design/auth.md` |
| Model providers | Claude via `PIPEROOM_MODEL_PROVIDER` = `anthropic` (default) \| `bedrock` (AWS) \| `vertex` (Google). Separate triage model. Per-room and per-pipeline token and cost budgets. | `internal/config/config.go`, `internal/agent/endpoint.go`, `docs/design/cost.md` |
| Shadow mode | Per pipeline (`shadow: {enabled: true}`): the agent runs once with read tools only, proposes nothing and writes to a separate shadow log. After close, a person labels the room blind, then reads and grades the agent's answer (`GET /api/shadow/metrics`). | `docs/api.md` Shadow mode, `docs/design/shadow.md`, `dev/shadow.md` |
| Slack | Outbound only: one thread per room. Nothing typed in Slack reaches a room, and approvals happen only in Piperoom. | `internal/slackbridge`, `docs/pilot/README.md` |
| Optional auto-approve (do not lead with it) | A pipeline may list actions in `auto_approve:` (empty unless configured). A matching proposal gets a visible countdown that the driver or an owner can cancel, then a `grant: "policy"` approval. It still needs write mode on. | `internal/room/autoapprove.go` |

---

## 4. Integrations on `main`

**Core CDC path (wired, used in rooms)**
- PostgreSQL source (logical replication slot, WAL, DDL audit, publications): `internal/clients/postgres`
- Debezium on Kafka Connect (REST): `internal/clients/connect`; Debezium JMX metrics via Prometheus text: `internal/clients/prom`
- Kafka (franz-go): `internal/clients/kafka`
- Confluent Schema Registry: `internal/clients/schemareg`
- Warehouse, Postgres-backed (default): `internal/clients/warehouse`
- TLS/SASL to brokers, Registry and Connect: `internal/clients/netsec`

**Also reads (wired)**
- Warehouses via `WAREHOUSE_KIND`: **Snowflake** (`internal/clients/snowflake`), **BigQuery** (`bigquery`),
  **Databricks SQL** (`databricks`), **ClickHouse** (`clickhouse`). Selection is in `internal/wiring/warehouse.go`.
- Batch pipelines: **Apache Airflow 2.x** REST (`internal/clients/airflow`) and **dbt** artifacts (`internal/clients/dbt`),
  for `kind: batch` rooms (`internal/wiring/kinds.go`).
- Alert sources: Alertmanager, Grafana, PagerDuty, Datadog, generic (signed) (`internal/alertin`). Change feed
  webhook `POST /api/changes/webhook`. Slack (outbound).

**Present but NOT wired; do not list as supported:** `internal/clients/mongodb`, `internal/clients/oracle` and
`internal/clients/sqlserver` exist as client packages, but nothing outside their own folders imports them. There
is no MySQL client.

### Real read-only tool names (43)
- Connect: `connect_list_connectors`, `connect_status`, `connect_config`, `debezium_metrics`
- Schema Registry: `schema_registry_subjects`, `schema_registry_versions`, `schema_registry_diff`
- Kafka: `kafka_topic_offsets`, `kafka_list_groups`, `kafka_consumer_lag`, `kafka_find_by_key`, `kafka_topic_config`,
  `kafka_cluster_health`, `kafka_acls`, `kafka_throughput`, `kafka_partition_rates`, `kafka_dup_keys`, `dlq_peek`, `tls_cert_expiry`
- Postgres source: `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`, `pg_row`, `pg_publication_tables`,
  `pg_connector_scope_gap`, `pg_streaming_liveness`, `pg_toast_risk`
- Warehouse: `warehouse_freshness`, `warehouse_row`, `warehouse_load_errors`
- Cross-pipeline: `pipeline_map`, `trace_record`, `reconcile`, `what_changed`, `similar_incidents`, `runbook_lookup`
- Batch: `dag_runs`, `task_instances`, `task_log_tail`, `dag_schedule_gap`, `dbt_run_results`, `dbt_model_lineage`, `dbt_model_sql`
- Agent room tools (not reads): `update_hypothesis`, `propose_fix`, `post_message`
- **There is no `schema_get` tool.** Use `schema_registry_versions` / `schema_registry_diff`.

### Real write actions (15; they run only via approved proposals) and their layer
- kafka-connect: `connect_restart_task`, `connect_restart_connector`, `connect_resume_connector`, `connect_pause_connector`,
  `connect_update_connector_config`, `connect_snapshot_table`
- schema-registry: `schema_set_compatibility`, `schema_register`
- kafka: `dlq_replay`, `kafka_replay_range`
- consumer: `kafka_reset_consumer_offsets`
- postgres: `pg_drop_replication_slot`, `pg_add_publication_table`, `pg_snapshot_signal`, `pg_resnapshot_rows`
- (`docs/pilot/README.md` still says “six”; that is stale. The registry above comes from
  `internal/actions/kafka_actions.go` `Register` and `internal/wiring/actions_p3.go`.)

### Watcher signals
`connector_down`, `slot_inactive`, `slot_wal` (with an invalidation ETA from `safe_wal_size`), `consumer_lag`,
`group_unstable`, `offset_below_log_start`, `dlq_growth`, `config_risk`, plus config drift and forecast signals
(`internal/owners/watch.go`, `internal/watch/`). **Default `PIPEROOM_WATCH_MODE=log`** (log lines only); `rooms`
must be turned on. One open watcher room per pipeline.

---

## 5. UI tokens and real strings for mock screens

**Type:** Geist (sans) and Geist Mono. Sizes: xs 12/16, sm 13/18, base 14/20, lg 16/22, xl 20/26. Radius 8 / 6 / 4.
The look is “Linear/Vercel”, dark first, shadcn/ui + Tailwind + Radix + Lucide icons (`docs/design/ui-v2/README.md`).

**Dark (default) tokens** (`web/src/index.css`):
background `#0a0a0b` · foreground `#ececf0` · card `#121214` · secondary/muted `#19191d` · accent (hover) `#1f1f25` ·
border `#222228` · muted-fg `#9d9da8` · subtle-fg `#86868f` · sidebar `#0e0e10` ·
primary `#516edf` (ring `#5b7cfa`, primary-fg text `#8ea3ff`, subtle `rgb(91 124 250/.14)`) ·
success `#2ea36f` / solid `#26865b` (Approve button) / fg `#4fd48f` · warning `#d9a21b` / fg `#f0c04a` ·
destructive `#d34247` / fg `#ff7074` · info `#3b9eff` / fg `#7cc0ff` · **agent `#8b5cf6`** / fg `#b7a0ff` / subtle `rgb(139 92 246/.14)`.
Shadows: md `0 1px 2px rgb(0 0 0/.4), 0 8px 24px rgb(0 0 0/.4)`.

**Layer colours (dark / light):** postgres `#7cb8ff`/`#185eb7` · kafka-connect `#f0a35c`/`#964c08` ·
schema-registry `#5ad3c0`/`#0c6c5e` · kafka `#f28fb4`/`#a93060` · consumer `#b9d96b`/`#4c690b` ·
warehouse `#9fb3ff`/`#4356bd` · orchestrator `#4dd6e0` · transform `#d98bd4`.

**Light tokens:** background `#ffffff` · foreground `#18181b` · muted `#f4f4f5` · border `#e4e4e7` · primary `#4d6beb` ·
agent `#7c4ddc` · success `#1f9d5f`.

**Brand mark** (`web/src/components/shell/BrandMark.tsx`): a 20 px primary-blue rounded square (radius 5) with a white
pulse line, SVG path `M4 12h5l2-5 3 10 2-5h4`, viewBox 24, stroke 1.75, round caps.

**Animations available:** `heal` (success tint fades out, 600 ms), `cite` (agent tint fades out, 600 ms), `check` (pop in,
200 ms), `shimmer`, `indeterminate`.

**Room screen layout** (`web/src/components/RoomScreen.tsx`, `room/RoomHeader.tsx`):
1. **Header:** title (e.g. “Warehouse orders stale”) · severity badge `SEV2` · chip `alert · watcher` + verified badge ·
   status badge with dot · agent pill · buttons **Hand off** (H), **Close room** (C), Invite (copy link); a mono facts
   line (pipeline · cost · clock · opened); avatars with the driver ringed (“Kabir · driving”).
2. **Decision bar** (sticky): “Needs your approval · kafka-connect”, “Running step 1 of 2”, “Fix ran · 2 of 2 steps”.
3. **Pipeline lane** (“Pipeline, source to sink”): nodes Source → Connector → Schema subject → Topic → Consumer → Sink,
   each `healthy` / `degraded` / `failed` / `unknown`, coloured by layer, with the owner.
4. Pinned hand-offs (“Shift 1 summary”).
5. **Hypothesis card:** “Hypothesis” · “high confidence” · confidence path (“low 09:45 → high 09:47”) · text · “based on”
   step chips · **Confirm** / **Dispute…**.
6. **Proposals card** (“Proposals”): steps (“Step 1”), params as key/value rows, **Fingerprint** (e.g. `9f2c 1e7a … 0a1b`),
   “Approving exactly this”, **Approve and run** / **Approve** / **Reject** (“Rejection reason”), “Approved”.
7. **Investigation:** **First look** card, **What changed** lane (Before/After), step stream (step title → tool call →
   result), **Seen before** / “Similar incident” card.
8. **Composer** (bottom): textarea “Steer the agent…”, button **Steer**; a non-driver gets **Suggest** / “Suggest a first
   step…”; “You are driving”, “Lease ends in 4m — any action renews”; a closed room shows “This room is closed. Nothing
   more to steer.”
9. **Right rail (300 px):** Similar · proposals summary · People · suggestions (“Your suggestions”, **Send to agent**) ·
   Activity feed. On a phone these become tabs: Investigation · Fix · People · Activity.

**Status pill texts** (`web/src/lib/roomStatus.ts`, `lib/agentPill.ts`):
`Investigating · step 3` · `Needs approval · 4m` · `No driver · 2m` · `Executing fix · 1 of 2` · `Fix ran · verifying` ·
`Fix ran · recovered` · `Fix ran · not recovered` · `Fix failed · step 2` · `Resolved · verified` · `Resolved` · `Closed` ·
`Idle · waiting for a steer` · `Queued · waiting for an agent slot`.
Agent pill: `Agent working` · `Agent waiting · 1 of 2 approvals` · `Agent waiting for approval` · `Executing fix` ·
`Proposal rejected · re-planning` · `Agent idle · waiting for a driver` · `Shadow · agent works silently`.

**Buttons and labels:** “Take control”, “Take control and steer”, “Request control”, “Hand off”, “Hand off the room”,
“Hand off to someone else”, “Writing hand-off…”, “Read summary”, “Accept”, “Not yet”, “Approve”, “Approve and run”,
“Approve (will not run)”, “Reject”, “Send to agent”, “Close the room”, “Close room”, “Export JSON”,
“Post-mortem draft”, “Pipeline recovered.”, “Verified · recovered”, “Waiting for kafka-connect · 1 of 2 approved”,
“Needs your approval · schema-registry · kafka-connect”, “No one is driving — take control to steer the agent.”,
“View only”.
Hand-off card headings: **What broke · Why · Done · Ruled out · Still open · Risks**.
Home: “Rooms”, tabs “Needs you / Open / Resolved”, “Resolved today”, “Resolved · 6 min”, empty state “Alerts land here. A
room opens the moment a rule fires.”

---

## 6. Do not claim

1. **No customers, logos, testimonials or “trusted by”.** Piperoom is pre-1.0. The first customer pilot is only
   *proposed* (`docs/pilot/README.md`, `docs/pilot/decision.md`).
2. **No benchmark or outcome numbers**: no MTTR cut, accuracy %, “finds root cause in N seconds”, or cost per incident.
   An eval harness with 34 scenarios exists (`evals/`), but no results are published. The only measured scale is one
   replica tested with 60 open rooms (`docs/pilot/README.md`).
3. **No autonomous fixing, “self-healing” or “auto-remediation”.** The agent proposes; owners approve; write mode is off by
   default. Do not advertise the opt-in `auto_approve` countdown as autonomy.
4. **Nothing that is not on `main`**: no MongoDB, Oracle, SQL Server or MySQL source support; no two-way Slack or Slack
   approvals; no Teams; no hosted SaaS or “cloud” plan; no automatic hand-off at shift change (the driver starts it and
   the next person accepts); no `schema_get` tool.
5. **Not open source.** `LICENSE`: “All rights reserved … published for viewing only”.
6. **No compliance badges** (SOC 2, ISO, HIPAA). Do not say “PII never leaves” or “zero data exposure”: redaction is
   partial, and tool summaries do reach the model provider.
7. **Models:** Claude only (Anthropic API, Bedrock, Vertex). No GPT or other LLMs.
8. **Watcher:** do not say it opens rooms out of the box. The default is `log`; `rooms` is an explicit step.
9. **“No data loss”** only for the `schema-change` scenario on the dev stack (`dev/scenarios.md`), never as a general promise.
10. Timings in the storyboard are illustrative; label them so on the page.
