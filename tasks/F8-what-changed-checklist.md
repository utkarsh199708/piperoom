# F8 — "What changed" lane, similar-incidents card, runbook checklist

Owners: F8a lead (contracts) · F8b A3 (`internal/tools/changes/**` new, `internal/tools/similarincidents/**`) ·
F8c A7 (`internal/watch/**` change feed) · F8d A5 (`migrations/**`, `internal/store/**`, `internal/room/**`,
`internal/httpapi/**`, `cmd/server/**`) · F8e A6 (`web/**`) · F8f C11 (`internal/tools/runbooks/**`, items in hit
data) · F8g A4 (`internal/agent/prompt.go`, one method line).
Model: **Opus 5.5** for F8b and F8c (diffing, masking, dedup rules that must never store a secret or a record
value); **Sonnet 5.5** for F8d, F8e, F8f, F8g. Base: `main` after F0, the R wave and F6a (events/contract shape).

## Goal
After "what broke" every engineer asks "what changed". The room answers it as a timeline lane: DDL on the source,
connector config and state changes, schema versions registered or deleted, with diffs and who did it, before the
alert. Beside it: *"orders had this on 2026-09-30, fixed by `connect_resume_connector`, 12 min"* with a link, and
the matching runbook's steps as a checklist people tick, with "ask the agent to do step 3" one click away.

## Evidence
- Change history exists only as point-in-time reads: `pg_recent_ddl` reads the audit table (`internal/tools/dbtools/pg.go:111`),
  the topology keeps subject versions in process memory (`internal/topology/schema.go:266-300`, lost on restart),
  Connect has no config history at all. Platform review §3 #4 and §2 row 4 ("who registered v3").
- `similarincidents.Hit` carries free-text `Resolution` only (`internal/tools/similarincidents/tool.go:204-214`);
  labels exist only for shadow rooms (`shadow.labelled`, `internal/room/shadow.go:522`); the executed fix of a past
  room is in its log but never surfaced (gap 18); past-room text from unverified closers steers future rooms (gap 10).
- `runbook_lookup` returns a section as one text blob (`internal/tools/runbooks/tool.go:143-150`); the UI shows it
  as a summary line; nothing records that a human did a step (oncall "what I would build next", items 142-144).
- UX critique: "What changed view", "Runbook steps as a checklist", "Similar incidents card" are the three asks the
  on-call listed after the walkthrough (`ux-critique.md:142-144`).

## Scope — what to build
1. **F8a contract**: `clients.PipelineChange` + `ChangeReader`/`ChangeWriter`; events `room.labelled`,
   `checklist.step_done`; `HumanInput` unchanged; `docs/api.md` routes `POST /api/rooms/{id}/label`,
   `POST /api/rooms/{id}/checklist`; config `PIPEROOM_CHANGES_RETENTION_DAYS`.
2. **F8b tool** (A3): `what_changed` (KindRead) over the change table plus a live DDL read, scoped to the room's
   pipeline; `similar_incidents` indexes labels and executed fixes and returns `fix_that_worked`, `duration_s`,
   `closed_by` (verified only).
3. **F8c feed** (A7): the watcher records `connector_config` diffs (masked), `connector_state` transitions,
   `schema_version` ups/downs and `ddl` rows into the table every tick, deduplicated.
4. **F8d server** (A5): migration + store (PG and memstore) + retention; the label route (generalising the shadow
   blind label); the checklist route; snapshot fields; wiring.
5. **F8e web** (A6): the "What changed" lane with diffs, the similar-incident card, the runbook checklist, the
   label form on close.
6. **F8f** (C11): `items` in `runbook_lookup` hit data. **F8g** (A4): the method tells the agent to call
   `what_changed` as its second read.

## Out of scope
`propose_checklist` room tool with live preconditions (platform §3 #11: needs its own design), deploy webhooks
(`POST /api/changes/{source}`), topic-config and ACL change kinds (after F12), `changed_recently` topology facts,
cross-pipeline change correlation, `evalrun -from-room`, post-mortem (F2), Slack rendering of changes.

## Contract (F8a — exact diffs)
`internal/clients/clients.go` (new section, Piperoom's own data, not a customer system):
```go
// ---- Pipeline change log (Piperoom's own table, fed by the watcher) ----

// Change kinds of PipelineChange.Kind.
const (
	ChangeDDL             = "ddl"              // a row of the source DDL audit table
	ChangeConnectorConfig = "connector_config" // connector config keys changed (values masked)
	ChangeConnectorState  = "connector_state"  // connector or task state transition
	ChangeSchemaVersion   = "schema_version"   // a subject's latest version went up (registered) or down (deleted)
)

// PipelineChange is one recorded change on a pipeline. Before and After hold
// only the changed keys (config), the two states (state), the version pair
// (schema) or nothing (DDL: the statement is in Summary, cut at 300 runes).
// A credential value is never stored: masked keys read "***". Actor is the
// DDL username or ""; Source is "watcher" or "ddl_audit".
type PipelineChange struct {
	ID          int64           `json:"id"`
	PipelineID  string          `json:"pipeline_id"`
	Kind        string          `json:"kind"`
	Object      string          `json:"object"` // "public.orders", "pg-orders-connector", "pg-orders-connector/0", "pg.public.orders-value"
	At          time.Time       `json:"at"`
	Actor       string          `json:"actor,omitempty"`
	Source      string          `json:"source"`
	Fingerprint string          `json:"fingerprint"` // dedup key: sha256 of kind|object|at|before|after
	Before      json.RawMessage `json:"before,omitempty"`
	After       json.RawMessage `json:"after,omitempty"`
	Summary     string          `json:"summary"` // one line, ≤ 300 runes, control characters removed
}

// ChangeReader lists changes of one pipeline, newest first, at most limit.
// kinds nil = every kind. Implemented by the room store (store.PG, memstore).
type ChangeReader interface {
	ListChanges(ctx context.Context, pipelineID string, since, until time.Time, kinds []string, limit int) ([]PipelineChange, error)
}

// ChangeWriter records a change. A row with the same (pipeline, kind, object,
// fingerprint) is not written again; inserted reports whether it was.
type ChangeWriter interface {
	RecordChange(ctx context.Context, c PipelineChange) (inserted bool, err error)
}
```
`internal/events/events.go`:
```go
	// RoomLabelled is the human label of a closed room (POST /api/rooms/{id}/label):
	// what the cause was and what fixed it, for similar_incidents and evals.
	// Any room, shadow or not; re-labelling appends another and the last wins.
	RoomLabelled Type = "room.labelled"
	// ChecklistStepDone records that a person did (or undid) one item of a
	// runbook section shown in the room (POST /api/rooms/{id}/checklist).
	ChecklistStepDone Type = "checklist.step_done"

// RoomLabelledPayload generalises ShadowLabelledPayload to every closed room.
type RoomLabelledPayload struct {
	ActualLayer    string    `json:"actual_layer,omitempty"` // one of Layers, "" = unknown
	Cause          string    `json:"cause"`                  // 1..500 chars, typed text
	FixThatWorked  string    `json:"fix_that_worked,omitempty"` // ≤ 500 chars; "" = none / not fixed by a Piperoom action
	FixProposalID  string    `json:"fix_proposal_id,omitempty"` // the executed proposal, when the fix was one
	AgentHelped    *bool     `json:"agent_helped,omitempty"`
	Identity       *Identity `json:"identity,omitempty"`
}

// ChecklistStepDonePayload ticks or unticks item Item (0-based) of the runbook
// section that tool result ToolUseID (an agent.tool_result of runbook_lookup)
// returned as hits[Hit].items. By is the event actor; Note is optional typed text ≤ 300 chars.
type ChecklistStepDonePayload struct {
	StepID    string `json:"step_id"`
	ToolUseID string `json:"tool_use_id"`
	Hit       int    `json:"hit"`
	Item      int    `json:"item"`
	Done      bool   `json:"done"`
	Note      string `json:"note,omitempty"`
}
```
`internal/config/config.go`: `ChangesRetentionDays string // PIPEROOM_CHANGES_RETENTION_DAYS: default 30, "0" = keep;
rows per pipeline are capped at 5000 regardless`.
`docs/api.md` rows:
| `POST /api/rooms/{id}/label` | `{actual_layer?, cause, fix_that_worked?, fix_proposal_id?, agent_helped?}` → `Event (room.labelled)` | closed rooms only (`409 room_open`); caller = member of the room's pipeline (the `shadow/blind` rule), else `403 not_allowed`; viewers `403 viewer`; `cause` 1–500 chars typed-text rule; `fix_proposal_id` must be a proposal of this room with an executed step (`400 validation`); re-label allowed, last wins. A shadow room still needs `shadow/blind` first (`409 not_labelled`) so the blind label stays blind |
| `POST /api/rooms/{id}/checklist` | `{step_id, tool_use_id, hit, item, done, note?}` → `Event (checklist.step_done)` | open rooms only; any member (viewers `403 viewer`); `tool_use_id` must be a `runbook_lookup` result of this room and `hit`/`item` within its data (`400 validation`); idempotent (the same state again still appends) |
Snapshot: `"labelled": true|false`, `"checklist_done": {"<tool_use_id>:<hit>:<item>": {"by": Person, "at": "…"}}` (open rooms).

## Ownership and files
| File | Owner | New/mod |
|---|---|---|
| `internal/clients/clients.go`, `internal/events/events.go`, `internal/config/config.go`, `docs/api.md`, `AGENTS.md` §2 row `internal/tools/changes/** → A3` | lead (F8a) | mod |
| `internal/tools/changes/{changes.go,tool.go,render.go,changes_test.go}`; `internal/tools/similarincidents/{room.go,tool.go,…_test.go}`; `internal/wiring/wiring.go` registration (A4 owns wiring: one-line, A3 asks in the PR) | A3 (F8b) | new/mod |
| `internal/watch/{changes.go,changes_test.go,observe.go,watch.go}` (`Deps.Changes clients.ChangeWriter`, `Deps.Registry clients.SchemaRegistryReader`, `Deps.DDL watch.DDLReader`) | A7 (F8c) | new/mod |
| `migrations/00015_pipeline_changes.sql`; `internal/store/changes.go` (+ memstore); `internal/store/retention.go`; `internal/room/{service_label.go,service_checklist.go,snapshot.go,shadow.go}`; `internal/httpapi/{label.go,checklist.go,server.go}`; `cmd/server/{main.go,watch.go,retention.go}` | A5 (F8d) | new/mod |
| `web/src/components/room/{ChangesLane.tsx,SimilarCard.tsx,Checklist.tsx,LabelForm.tsx}`, `CloseRoomDialog.tsx`, `Timeline.tsx`, `RoomScreen.tsx`, `state/{types,reducer,describe}.ts`, `api/{client,types}.ts`, tests | A6 (F8e) | new/mod |
| `internal/tools/runbooks/tool.go` (`hit.Items []string` from `listItems(s.Text)`), test | C11 (F8f) | mod |
| `internal/agent/prompt.go` method step, `prompt_test.go` | A4 (F8g) | mod |

## Dependencies and merge order
F8a → F8d store+migration (the table) → F8c (writes it) and F8b (reads it) in parallel → F8d routes → F8f, F8g →
F8e. F8b's `similar_incidents` changes need `room.labelled` (F8a) only. F8e's similar card and checklist render
from tool-result `data`, so they can ship on fixtures before F8b/F8f land. F8 needs F6b's `PostEvidence` change
only if both touch `watch.Deps` in the same week: rebase, no coupling.

## Design details
### 1. Table and retention (F8d)
```sql
-- +goose Up
CREATE TABLE pipeline_changes (
    id          bigserial PRIMARY KEY,
    pipeline_id text        NOT NULL,
    kind        text        NOT NULL,   -- ddl | connector_config | connector_state | schema_version
    object      text        NOT NULL,
    at          timestamptz NOT NULL,
    actor       text        NOT NULL DEFAULT '',
    source      text        NOT NULL,
    fingerprint text        NOT NULL,
    before      jsonb,
    after       jsonb,
    summary     text        NOT NULL DEFAULT '',
    UNIQUE (pipeline_id, kind, object, fingerprint)
);
CREATE INDEX pipeline_changes_pipeline_at ON pipeline_changes (pipeline_id, at DESC);
-- +goose Down
DROP TABLE IF EXISTS pipeline_changes;
```
`RecordChange` = `INSERT … ON CONFLICT DO NOTHING RETURNING id` (inserted = a row came back); the same statement
then deletes rows of that pipeline beyond the newest 5000 (`DELETE … WHERE id IN (SELECT id … OFFSET 5000)`), so a
flapping task cannot grow the table. Retention: the existing retention loop (`cmd/server/retention.go`) also runs
`DELETE FROM pipeline_changes WHERE at < now() - interval` per `PIPEROOM_CHANGES_RETENTION_DAYS`; memstore mirrors
both rules. Values stored in `before`/`after` are ≤ 4 KiB per row (larger → `{"truncated": true}`), never a record.
### 2. Change feed (F8c)
Per tick the watcher already holds every connector's status and config. New in `observeSet`: one
`Registry.Versions(subject)` per pipeline subject (`<topic_prefix>.<table>-key|-value`, through `Scope`) and one
`DDL.RecentDDL(ctx, lastTickAt-2*interval, 200)` per client set (`watch.DDLReader` = `RecentDDL` of
`clients.SourceReader`; the audit table may be absent: one warn line, kind off). Diffing against `pipeState.last`:
- `connector_config`: keys whose value changed, were added or removed, after masking (same rule as
  `kafkatools.sensitiveKey`: key contains `password`, `secret`, `token` or `.key` → `"***"` on both sides;
  the diff still records *that* such a key changed). `before`/`after` = `{key: value}` of changed keys only;
  summary `config of pg-orders-connector: 2 keys changed (heartbeat.interval.ms, publication.name)`; `at` = tick time
  (Connect has no change time; said in the doc comment); fingerprint over the sorted key→(before,after) pairs.
- `connector_state`: `(connector|task) state` transitions `RUNNING→FAILED` etc.; `before`/`after` = `{"state": …,
  "trace": first line, ≤ 300 runes, control characters removed}`; object `pg-orders-connector/0` for tasks.
- `schema_version`: latest version up → `registered`, down → `deleted`; `before`/`after` = `{"version": n}`; actor "".
- `ddl`: one row per audit row newer than the last recorded `at` for that table; actor = `Username`; summary =
  `ALTER TABLE public.orders | <query ≤ 300 runes>`; fingerprint = sha256 of `(at, object_identity, query)`.
The first tick after a start records nothing but the baseline (no "changed" from nil), except DDL rows, which carry
their own time. Writes go through `call()` with `CheckTimeout`; a write error is logged once per episode and the
diff is retried next tick (the baseline is not advanced until the write succeeded).
### 3. `what_changed` tool (F8b, `internal/tools/changes`)
Input `{window?: "6h" (default; max "7d"), kinds?: [..], object?: "schema.table|connector|subject"}`; scoped with
`tools.ScopeFor`; refuses an object outside the pipeline (`ErrBadInput`). Reads `ChangeReader.ListChanges` (limit
200) and, when `Deps.Src` is set, `RecentDDL(since)` filtered to the pipeline's tables; merges by fingerprint
(the live read wins on text), sorts newest first. Summary, one line per change, ≤ 40 lines:
`2026-10-14T03:04:11Z ddl public.orders by app_migrator: ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric`
`2026-10-14T03:05:02Z connector_state pg-orders-connector/0: RUNNING → FAILED (MISSING_UNION_BRANCH: …)`
then `N more changes not shown` and, when nothing: `no recorded change on orders in the last 6h (recorded kinds: …)`.
`Data` = `{window, changes: []PipelineChange, omitted}` (before/after included; they are already masked). Summaries
are built from stored text, which came from customer systems: the runner wraps them like every tool output.
`similar_incidents`: `pastRoom` gains `Cause`, `FixThatWorked`, `FixProposal` (title + step actions of the proposal
whose every `action.executed` is `ok`), `DurationS`, `ClosedBy` (actor name only when `Verified`); index weights
`wLabelTerms` apply to `Cause`; `Resolution` is indexed only when the `room.closed` actor is verified (gap 10).
`Hit` gains `FixThatWorked string`, `FixActions []string`, `DurationS int64`, `ClosedBy string`. The summary line
becomes `[1] 2026-09-30 "<title>" (12 min, closed by Kabir) fixed by: connect_resume_connector — <resolution ≤ 200>`.
### 4. Routes (F8d)
`Label`: checks as the api row; identity as `ShadowLabel` does; `fix_proposal_id` must match a proposal with an
executed ok step. The close dialog posts `/close` then `/label` (two calls; a failed label shows a toast and the
form stays available on the closed room). `Checklist`: looks the `tool_use_id` up in the room log
(`agent.tool_result` with `tool == "runbook_lookup"`), decodes `data.hits[hit].items[item]` exists; appends with the
caller as actor; folds `checklist_done` in `Apply` (last event per key wins; `done: false` deletes the key).
### 5. Web (F8e)
- **Changes lane** (`ChangesLane.tsx`): shown above the step stream when any `what_changed` result exists; a vertical
  mini-timeline, newest at the top, each row `kind` badge (layer colour: ddl → postgres, connector_* → kafka-connect,
  schema_version → schema-registry), object in mono, relative time with UTC on hover, actor, and a disclosure
  showing `before → after` as a two-column key list (plain text, `whitespace-pre-wrap`, ≤ 4 KiB). The alert time is
  a horizontal rule "alert fired" so changes before it read as suspects. "Changes" lane collapses to a one-line
  count on < 768 px. Row click filters the stream to that layer (hook into F7's layer filter when it exists).
- **Similar card** (`SimilarCard.tsx`) in the rail: the first `similar_incidents` result's top hit:
  `orders had this on 30 Sep · fixed by connect_resume_connector · 12 min · closed by Kabir` with `Open room ↗`
  (`/rooms/{room_id}`) and the resolution clamped to two lines; a hit without a verified closer shows no name.
- **Checklist** (`Checklist.tsx`) under a `runbook_lookup` step: each hit section's `items` as checkboxes (`<input
  type=checkbox>` labelled by the item text, 44 px rows); ticked state from `state.checklist_done`, showing `by name ·
  2m ago`; viewers see disabled boxes; each item has `Ask the agent` (driver: prefills the composer with `Do step 3
  of "<section>": <item>`; non-driver: the same text as a suggestion). Items are plain text from an embedded runbook
  (trusted source, still rendered as text).
- **Label form** (`LabelForm.tsx`) inside `CloseRoomDialog` after the resolution: layer select (pre-filled from the
  final hypothesis' layer words when one matches), `cause` (pre-filled with the hypothesis text, editable), `fix that
  worked` (pre-filled with the executed proposal title, disabled when none ran), "Did the agent help? yes/no/skip".
  Two required fields at most; `Skip` closes without a label. On a closed room without a label the banner offers
  `Add label`.
- Reducer: `room.labelled` → `state.labelled = payload`; `checklist.step_done` → map update; `describe.ts` lines:
  `Kabir ticked "Resume the connector" (step 3 of Failed connector)`, `Priya labelled the room: cause … · fix …`.
### Failure modes
Audit trigger missing → `ddl` kind off with one log line; the tool says `ddl: audit trigger not installed`.
Registry unreachable → no `schema_version` rows that tick, no error row. Table write failing → tool answers from the
live DDL read and says `change log unavailable`. Label on a deleted room → `404`.

## Done when
- [ ] `ALTER TABLE public.orders ADD COLUMN x int` on the dev stack → a `ddl` row within one tick → `what_changed`
      lists it with `actor piperoom` (integration test `TestWhatChangedSeesDDL`).
- [ ] `devctl break connector-down` → a `connector_state RUNNING → FAILED` row; `devctl fix` → `FAILED → RUNNING`; the
      same tick twice writes nothing (`inserted == false`).
- [ ] A masked key change (`database.password`) is recorded as `"***" → "***"` and the value never appears in the DB
      (`TestConfigDiffMasks`).
- [ ] `POST /api/rooms/{id}/label` on a closed room appends `room.labelled`; `similar_incidents` on the next room
      returns the hit with `fix_that_worked` (memstore test `TestSimilarIndexesLabelAndExecutedFix`).
- [ ] Ticking item 3 appends `checklist.step_done` and a second tab shows the tick with the name within 1 s.
- [ ] The changes lane renders diffs as text (a `<b>` in a config value appears literally), a11y: lane is a `region`
      named "What changed", checkboxes labelled.
- [ ] `go test ./...`, `go vet`, `gofmt`, `npm run lint && npm run build`, `evalrun -validate` green.

## Tests
Go: `TestRecordChangeDedups`, `TestRecordChangeCapsPerPipeline`, `TestChangesRetention` (store, PG + memstore);
`TestConfigDiffMasks`, `TestStateTransitionRows`, `TestSchemaVersionUpAndDown`, `TestDDLRowsCarryAuditTime`,
`TestFirstTickRecordsNoDiff`, `TestWriteErrorKeepsBaseline` (watch, fake clock); `TestWhatChangedScope`,
`TestWhatChangedMergesLiveDDL`, `TestWhatChangedWindowBounds`, `TestWhatChangedSummaryCaps` (tools/changes);
`TestSimilarIndexesLabelAndExecutedFix`, `TestSimilarSkipsUnverifiedResolution`, `TestHitFixThatWorked`
(similarincidents); `TestLabelRequiresClosedRoom`, `TestLabelMember`, `TestLabelFixProposalMustHaveRun`,
`TestChecklistValidatesToolUse`, `TestApplyChecklistAndLabel` (room); `TestRunbookHitItems` (runbooks).
Vitest: `ChangesLane.test.tsx` (ordering, diff text-only, alert rule position), `SimilarCard.test.tsx` (verified
name only), `Checklist.test.tsx` (tick, undo, viewer disabled, ask-the-agent prefill), `LabelForm.test.tsx`
(prefill, skip, 500-char cap), `reducer.changes.test.ts`.
Eval: `schema-change.yaml` gains a `what_changed` recording and `evidence_tools: [what_changed]` once F8g lands (C6);
`recurring-connector-paused.yaml` recording regenerated with the new hit line (`PRINT_SIMILAR=1`).

## Effort
F8a S · F8b **M** (A3; two PRs: tool / similar) · F8c M (A7) · F8d **M** (A5; two PRs: store+migration / routes+snapshot)
· F8e **L** (A6; three PRs: lane / card+label / checklist) · F8f S · F8g S.
