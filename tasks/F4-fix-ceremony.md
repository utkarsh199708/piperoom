# F4 — The fix ceremony: blast radius, dry-run preview, visible execution, verification card, rollback

Owners: F4a lead (contract, in F0) · F4b A2 · F4c A5 · F4d A4 · F4e + F4f A6.
Model: **Sonnet 5.5** for F4b and F4c (REST calls, event folding, well-specified); **Opus 5.5** for F4d (changes
the verification path that decides "recovered") and F4e/F4f (the product's signature interaction, a11y + motion).
Rank 4 in `docs/product/vision-2026-10.md` §4; principle 4; defect D4; UX critique §8, §9, §4.B.

## Goal
Before approving, the kafka-connect owner reads what the step will touch, what the system says now and what is
expected after, what sits downstream, whether it can be undone, and the Registry's or Connect's own dry-run
verdict. When the last approval lands the card turns into "Running · step 1 of 2" and draws each check as the
executor reports it. Then a verification card shows every layer check and the data-flow evidence, the healed
lane node flashes, and the room nudges the driver to close. If the fix was wrong, "Roll back" drafts the undo.

## Evidence
- On-call walkthrough steps 10–11 (`09e-executed-full-dark.png`, `A1-lane-scrolled-right-dark.png`): after
  execution "two green lines and silence"; lane still `failed`; pill still "Agent waiting for approval"; no
  prompt to close. Step 15 (`D2…`): closed room offers nothing. Walkthrough gaps 3, 4.
- Platform review §3 (blast radius + dry-run), §4 gap 6 ("Recovered" is thin), gap 8 (restart loops are
  prompt-only): proposals are flat step lists with no preview; `verify.go:241` emits the verdict as plain
  `agent.message` text (`grep verification web/src` → nothing).
- UX critique §8 (`lib/blastRadius.ts`, contract change request `ProposalStep.verify`), §9 (`ExecutionCard`,
  `outcome_unknown` amber row, "Roll back" needs a route or a steer), §4.B (the Approval Ceremony).

## Scope — what to build
1. **F4a (lead, F0): contract.** `actions.Preview`, `events.PreviewResult`/`DryRunResult`, `proposal.previewed`,
   `agent.verification`, `ProposalStep.Verify`, snapshot and `docs/api.md`. Exact text in **Contract**.
2. **F4b (A2): `Preview` on the six actions** (`internal/actions/kafka_actions.go`, `connector_config.go`).
   Every `Preview` is read-only, takes ≤ 10 s (`timeout`, `kafka_actions.go:16`), decodes params with the
   action's own `parseParams`/struct, and returns an error (wrapped, `"actions: preview %s: %w"`) when the
   target cannot be read; it never calls a `Writer` method. Per action:
   | Action | Target | Current (names its read) | Expected | Reversible | DryRun |
   |---|---|---|---|---|---|
   | `connect_restart_task` | `connector <name> task <id>` | `taskState(st, id)` + worker from `Connect.Status` (`kafka_actions.go:177`): `"FAILED on 172.18.0.6:8083 (GET /connectors/<name>/status)"` | `"RUNNING within ~20 s; the task fails again if its cause is still there"` | false | nil |
   | `connect_restart_connector` | `connector <name>` | `connectorAndTasks(st)` (`:343`) | `"connector and its failed tasks RUNNING"` | false | nil |
   | `connect_resume_connector` | `connector <name>` | connector state | `"RUNNING (tasks assigned)"` | false (no pause action) | nil |
   | `connect_update_connector_config` | `connector <name> key <key>` | `configText(cfg, key)` (`connector_config.go:198`; secret keys never leave it) | `"<key> = <value>; Connect restarts the tasks"` | true (`Rollback: "connect_update_connector_config {connector, key, value: <current>}"`) | `PUT /connector-plugins/{class}/config/validate` when `cfg` holds no `looksSecret` key (`:55`), else `Skipped: "the connector config holds secret keys; not sent to validate"` |
   | `schema_set_compatibility` | `subject <subject>` | `levelText(level, subjectLevel)` (`:519`) from `Registry.Compatibility` | `p.Level` | true (`Rollback: "schema_set_compatibility {subject, level: <current>}"`; `"(global)"` current → `"delete the subject level (no action exists; needs a lead)"` and Reversible false) | nil |
   | `schema_register` | `subject <subject>` | `"latest version <v> (id <id>)"` from `Registry.Schema(subject, -1)` | `"a new version <v+1>"` | false (soft-delete is not an action) | `POST /compatibility/subjects/{subject}/versions/latest?verbose=true` |
   Dry-run HTTP calls (neither client has them; both are reads although one is a POST — Confluent documents
   the compatibility check as side-effect free, and Connect's validate only validates):
   - `schemareg.Client.CheckCompatibility(ctx, subject, schema string) (ok bool, messages []string, err error)`:
     `POST {base}/compatibility/subjects/{url.PathEscape(subject)}/versions/latest?verbose=true`, body
     `{"schema": "<schema>"}` with `Content-Type: application/vnd.schemaregistry.v1+json`; response
     `{"is_compatible": bool, "messages": [string]}` (`messages` absent on Registry < 6.1: then `messages` is
     nil and `Text` reads `"compatible: false (no detail from this Registry version)"`). A 404 for the subject
     (error_code 40401) wraps `clients.ErrNotFound`; 422 (42201 invalid schema) is `*schemareg.Error`.
     Decode through `io.LimitReader(…, 4<<20)` like `do` (`schemareg.go:156`).
   - `connect.Client.ValidateConfig(ctx, class string, cfg map[string]string) (ValidateResult, error)`:
     `PUT {base}/connector-plugins/{url.PathEscape(class)}/config/validate`, JSON body = the connector's
     current config with the one key replaced (`class` = `cfg["connector.class"]`); response
     `{"name", "error_count": int, "groups": [], "configs": [{"definition": {...}, "value": {"name", "value",
     "recommended_values", "errors": [string], "visible"}}]}`. `ValidateResult{ErrorCount int; Errors
     map[string][]string}` keeps only `value.name → value.errors` for entries with errors; **never** the
     values (they can be secrets). `DryRun.Text` = `"0 errors"` or `"2 errors: <key>: <msg>; <key2>: <msg>"`,
     ≤ 2 KiB, and `OK = ErrorCount == 0`.
   Both are **optional adapter interfaces in package actions** (`configValidator`, `compatibilityChecker`),
   asserted at use time like `connectorResumer` (`kafka_actions.go:368`): a `Deps.Connect`/`Deps.Registry`
   without them (fakes, `demo.FakeConnect`, evals `replayConnect`) gets `DryRun = &DryRunResult{Skipped:
   "client has no dry-run"}`. They are not added to the contract Reader interfaces: that would break every
   fake in #128, demo and evals; a contract change request is welcome once all fakes carry them.
   Every `PreviewResult` sets `Source` on its dry-run (`"registry compatibility check"` / `"connect config
   validate"`) and leaves `Downstream` nil (F4c fills it). Compile-time checks `var _ Preview = (*restartTask)(nil)`
   for all six.
3. **F4c (A5): run Preview at creation, store it as an event, fold it into the snapshot.** After
   `createProposal` (`service_proposals.go:72`) emits `proposal.created` and releases `s.mu`, start
   `go s.previewProposal(s.ctx, roomID, pipelineID, p)` (service lifetime context, like the executor). It:
   - for each step, `a, _ := acts.Get(step.Action)`; `pv, ok := a.(actions.Preview)`; if `!ok` → `StepPreview{
     StepIndex: i, Error: "no preview for this action"}`; else `ctx10 := tools.WithPipeline(ctx, pipeline)` with a
     10 s timeout, `res, err := pv.Preview(ctx10, step.Params)`; an error → `Error: truncateDetail(
     sanitizeAgentText(err.Error()))` (never the raw error with a connection string: run `redact.Text` first);
   - fills `Downstream` from `s.topo.Build(ctx, pipelineID)` (`service.go:1154` shows the 10 s pattern): the
     target node is the node whose `Name` equals the step's target param (`connector` → kind `connector`;
     `subject` → the topic `subjectTopic(subject)` of kind `topic`; `group` → `consumer_group`), then every node
     reachable over `Edge.Kind == EdgeData` edges from it, in BFS order, as `Node.ID`s, cap 12; no match or
     no topology → nil (the UI says "downstream unknown");
   - emits **one** `proposal.previewed` with `systemActor` (`service.go:863`) via `executorEmit`-style append
     (`service.go:766`), only if the proposal is still open (`st.snap.OpenProposals`), else drops it; a store
     error is logged once (`room: preview append failed`), never retried; total wall time ≤ 30 s (6 steps × 10 s
     is impossible by design: the goroutine uses one 30 s parent context).
   - `snapshot.go` folds `proposal.previewed` into `Proposal.Preview *events.ProposalPreviewedPayload` (new
     field, `json:"preview,omitempty"`), cloned in `cloneProposals` (`snapshot.go:169`); `Apply` table test.
   - `sanitizeProposal` (`service_proposals.go:46`) also runs `st.Verify = truncateDetail(sanitizeAgentText(
     st.Verify))`; `checkPlanText` is **not** applied to `Verify` (optional field).
   - Replicas: the replica that created the proposal previews it; a restart mid-preview loses it (the UI shows
     "Preview unavailable" after 20 s and the approval still works). No refresh route in this task (see Out of
     scope). Demo mode (`internal/room/demo`): `FakeConnect.Status` and `FakeRegistry.Compatibility` already
     serve the real actions' `Preview`; add deterministic values so the scripted room shows a full card.
4. **F4d (A4): typed verification event; `verify` on steps.**
   - `verify.go verifyActionResult` (`verify.go:236-242`): keep the model note (`out.notes`, unchanged text) and
     the `agent.status` transitions; replace the `agent.message` emission of the verdict with
     `agent.verification` built from the same run: `verifyCheck` gains `layer string` (set in `checkForStep`
     `:560` and `compatCheck` `:464` → `"schema-registry"`); `verifyProposal` returns the per-check results
     (`[]checkResult{layer, name, ok, detail}`) and the `flowResult` (which gains `sinks []sinkState` so each
     sink's `table`, `checkAt`, `proposalAt`, `passedBy` reach the event). `Verdict` = the head line without its
     trailing newline (`recoveredHead`/`recoveredConfigHead`/the two `NOT recovered` heads). The "already
     verified" repeat path (`:224`) emits `agent.verification{Repeat: true, Recovered: rec.recovered}` with no
     checks. `failedStepText`/`unknownOutcomeText` stay `agent.message` (they are not verdicts).
     Tests in `verify_test.go` that read the message text move to the event (`TestVerify*`).
   - `propose_fix` (`tools_room.go:266`): each step accepts optional `verify` (string ≤ 200 chars, "what the
     agent will check after this step runs, e.g. task 0 RUNNING within 60 s"); copied to
     `events.ProposalStep.Verify`. The tool description gains one sentence; `TestSystemPromptIsStable` is
     unaffected (the system prompt does not list tool descriptions) — if it is, update the golden once.
   - No prompt change for rollback: a steer `roll back proposal …` is an ordinary driver steer the model already
     follows (`prompt.go` steer handling); the recorded `rollback` text is in the room log it reads.
5. **F4e (A6): blast radius + execution card.**
   - `api/types.ts`: `PreviewResult`, `DryRunResult`, `StepPreview`, `ProposalPreviewedPayload`,
     `AgentVerificationPayload` (+ `VerificationCheck`, `DataFlowCheck`, `SinkCheck`), `ProposalStep.verify?`,
     `EventType` + `'proposal.previewed' | 'agent.verification'`. `state/types.ts`: `ProposalState.preview?:
     ProposalPreviewedPayload`, `ProposalState.verification?: AgentVerificationPayload`; `reducer.ts` folds both
     (`proposal.previewed` → by `proposal_id`, keep the latest; `agent.verification` → by `proposal_id`, a
     `repeat` never overwrites a full one); `describe.ts`: `"Previewed what the fix will do"`,
     `"Verification: recovered"` / `"Verification: not recovered"`.
   - `lib/execution.ts: executionPhase(p: ProposalState) → {phase: 'pending'|'running'|'done'|'failed'|'unknown',
     step: number, of: number}`: `running` when `status === 'approved'` and `executed.length < steps.length` and
     no failed step; `failed` on any `ok: false` without `outcome_unknown`; `unknown` on `outcome_unknown`;
     `done` when every step has an `ok: true`. F1's `roomStatus()` reads this for "Executing · 1 of 2".
   - `components/room/BlastRadius.tsx` under the steps (`ProposalsPanel.tsx` `Proposal`, before the layer
     rows): kicker `This touches`; one block per step: `target` (mono 13/18/500), `current → expected`
     (`Current:` / `Expected:` labels 12/16/500 subtle, values 13/18/400), `Downstream:` the node names resolved
     from `useTopology` by id (`topology.nodes.find(n => n.id === id)?.name ?? id`) joined with ` → `, or
     `downstream unknown`; `Reversible` as a neutral badge `reversible` / `not reversible` with the `rollback`
     text as `title`; dry-run as a row with `StatusDot` ok/fail and `Source` + `Text` (`<pre>` when
     multi-line, `max-h-32`), or `Skipped: <reason>` muted, or nothing when nil. A step with `preview.error`
     shows `Preview failed: <error>` in warning tone. `verify` on a step renders as `After it runs — <verify>`
     under the step description. While `preview` is absent: a one-line skeleton `Previewing what this will do…`
     for 20 s from the proposal's `at`, then `Preview unavailable` muted (never an error). Preview text never
     changes the fingerprint: it is informational and labelled `Previewed 10:04` (`lib/time.ts`).
   - `components/room/ExecutionCard.tsx` replaces `ExecutedList` (`ProposalsPanel.tsx:615`), keeping the
     `exec-hash-N` test ids and the "hash differs from proposal" badge: header line `Running · step 1 of 2 ·
     schema_set_compatibility` (agent tone, 2 px indeterminate `Progress` from `ui/progress.tsx`) in an
     `aria-live="polite"` `<p>`; one row per step: pending `○` muted, running spinner, `ok` → `CircleCheck`
     drawn with `motion-safe:animate-check` (200 ms scale 0.6→1), failed `CircleX`, `outcome_unknown` →
     amber `TriangleAlert` + `Result unknown — check the target before retrying`; `output` inline (text,
     `break-words`); `step N` is 1-based. Header after completion: `Ran 2 of 2 · verifying…` until the
     verification arrives (F4f), `Stopped at step 2 of 2` on failure.
6. **F4f (A6): verification card, close nudge, roll back, lane flash.**
   - `components/room/VerificationCard.tsx` (not `lib/verification.ts`, which #135 uses for the IdP badge):
     rendered under the `ExecutionCard` of the verified proposal; `role="status"`; header `Verified · recovered`
     (success) or `Verified · not recovered` (destructive) with `verdict` as the subtitle (14/20/400,
     `max-w-[72ch]`); one row per check: layer badge (`Badge variant` by layer as in `Timeline` `ToolBadge`),
     `name`, ✓/✗ (`CircleCheck`/`CircleX` 16 px with `aria-label`), `detail` mono 12 px clamped to 2 lines
     with a `Show more` toggle; `data_flow`: `checked: false` → `Data flow not verified: <detail>` muted; else
     one line per sink `analytics.orders · newest 10:12:04 · after baseline 10:04:00 · rows since proposal ✓`
     (`tabular-nums`), `fresh` decides the tone. `repeat: true` → one muted line `Already verified · no checks
     re-run`.
   - **Close nudge.** When `recovered` and the room is open: a row under the card `Pipeline recovered.` +
     driver: `Button` `Close room` (`kbd C`, opens the existing `CloseRoomDialog` with `initialText` =
     `Fixed by "<proposal title>"` — add an optional `initialText` prop to `CloseRoomDialog.tsx:26`, default
     `''`); non-driver: `<driver name> can close the room` muted; no driver: `Take control to close`. F1's
     decision bar shows the same nudge if present; this card does not duplicate a toast.
   - **Roll back.** On an `ExecutionCard` whose phase is `done`, `failed` or `unknown`, a `ghost-danger`
     `RotateCcw` button `Roll back` (driver) / `Suggest roll back` (member, not viewer). Click opens a small
     confirm dialog: the recorded `rollback` text (plain), and the sentence `This asks the agent to propose
     the undo; nothing runs without approvals.` Confirm sends `actions.steer(text)` (`useRoom.ts:414`, driver
     → `steer.sent`, else `steer.suggested`) with the exact text
     `` Roll back proposal "<title>" (<proposal_id>): propose a fix that undoes it. Recorded rollback: <rollback> ``
     (`title` and `rollback` are agent text already shown on the card; the steer is wrapped by the agent as
     untrusted like every steer). There is no rollback route in this task: the steer path needs no contract
     and keeps approvals in the loop; say so in the PR and in the dialog copy.
   - **Lane flash.** `PipelineMap.tsx` keeps `prevHealth` per node id in a `useRef`; on a render where a node
     goes from `fail`/`warn` to `ok`, add `motion-safe:animate-heal` (600 ms `bg-success-subtle` → transparent)
     to that node and the edge into it loses `EDGE_STALLED` immediately. `RoomScreen.tsx:45` bumps
     `refreshKey` on `agent.verification` too (count of verifications + executed), so the map re-polls at once.
   - F1 interplay: `lib/agentPill.ts` maps `done` → `Resolved · verified`; F1's `roomStatus()`, when merged,
     reads `executionPhase` and `verification?.recovered` from here.

## Out of scope
A preview refresh route (previews carry their time; the executor re-validates every step before it runs,
`service_executor.go:397 recheckStep`). A rollback route or automatic rollback. Hashing the preview into
`params_hash`. Persisting the agent's proposal tracker across restarts (platform gap 6). Consumer/kafka/warehouse
layer checks (verification v2). An "I read the params" gate (F1's set-completing confirm covers it). Adding the
dry-run methods to the contract Reader interfaces (see F4b).

## Contract (F4a — lead, in F0)
`internal/events/events.go` — after `ProposalStep`, new types and event:
```go
// ProposalStep gains (after Description):
	// Verify says what the agent will check after this step runs, in one
	// sentence ("task 0 RUNNING within 60 s"). Optional; agent text, ≤ 200
	// chars after the room's sanitizing. Part of the hashed proposal.
	Verify string `json:"verify,omitempty"`

	// ProposalPreviewed is written by the system actor once per proposal,
	// after proposal.created, with what each step would do (actions.Preview).
	// Informational: it is not part of params_hash and can be absent (no
	// preview support, a restart mid-preview).
	ProposalPreviewed Type = "proposal.previewed"
	// AgentVerification is the agent's verdict after the last step of a
	// proposal ran: the layer checks and the data-flow check it made. It
	// replaces the plain-text "[verification]" agent.message.
	AgentVerification Type = "agent.verification"

// PreviewResult is what one write action would do, read before approval.
// Current and Expected are short human sentences; Current names the read it
// came from. Downstream is filled by the room service from the topology (node
// ids, data edges only, ≤ 12); actions leave it nil.
type PreviewResult struct {
	Target     string        `json:"target"`
	Current    string        `json:"current"`
	Expected   string        `json:"expected"`
	Downstream []string      `json:"downstream,omitempty"`
	Reversible bool          `json:"reversible"`
	Rollback   string        `json:"rollback,omitempty"` // how to undo, as an action and params, when Reversible
	DryRun     *DryRunResult `json:"dry_run,omitempty"`  // nil when the target system has no dry-run
}

// DryRunResult is the target system's own answer to "would this be accepted"
// (Registry compatibility check, Connect config validate). Skipped is set,
// with OK false, when the check exists but was not run (the reason says why).
type DryRunResult struct {
	OK      bool   `json:"ok"`
	Source  string `json:"source"`            // "registry compatibility check" | "connect config validate"
	Text    string `json:"text"`              // the system's messages, redacted, ≤ 2 KiB
	Skipped string `json:"skipped,omitempty"` // why the check was not run
}

// StepPreview is one step's preview or the reason there is none.
type StepPreview struct {
	StepIndex int            `json:"step_index"`
	Preview   *PreviewResult `json:"preview,omitempty"`
	Error     string         `json:"error,omitempty"` // redacted, ≤ 500 bytes
}

// ProposalPreviewedPayload carries every step's preview, in step order.
type ProposalPreviewedPayload struct {
	ProposalID string        `json:"proposal_id"`
	Steps      []StepPreview `json:"steps"`
	At         time.Time     `json:"at"`
}

// VerificationCheck is one read-tool check of a layer after a fix.
type VerificationCheck struct {
	Layer  string `json:"layer"`  // one of Layers
	Name   string `json:"name"`   // the read tool, e.g. "connect_status"
	OK     bool   `json:"ok"`
	Detail string `json:"detail"` // the tool summary line, clipped (agent.clipLine)
}

// SinkCheck is one warehouse table in the data-flow check.
type SinkCheck struct {
	Table    string    `json:"table"`
	Newest   time.Time `json:"newest,omitzero"`   // newest row seen; zero when unread
	Baseline time.Time `json:"baseline,omitzero"` // proposal-time newest row; zero when unknown
	Passed   bool      `json:"passed"`
	PassedBy string    `json:"passed_by,omitempty"` // "rows since proposal" | "rows during check"
}

// DataFlowCheck says whether new rows reached every sink after the fix.
// Checked is false when the check could not run (Detail says why).
type DataFlowCheck struct {
	Checked bool        `json:"checked"`
	Fresh   bool        `json:"fresh"`
	Sinks   []SinkCheck `json:"sinks,omitempty"`
	Detail  string      `json:"detail,omitempty"`
}

// AgentVerificationPayload is the verdict on one proposal's fix. Repeat is
// true for a repeated last-step result: no check ran again, Checks is empty
// and Recovered repeats the first verdict.
type AgentVerificationPayload struct {
	ProposalID string              `json:"proposal_id"`
	Checks     []VerificationCheck `json:"checks"`
	DataFlow   *DataFlowCheck      `json:"data_flow,omitempty"`
	Recovered  bool                `json:"recovered"`
	Verdict    string              `json:"verdict"` // one line, e.g. "recovered: every layer check passed and data is flowing again"
	Repeat     bool                `json:"repeat,omitempty"`
}
```
`internal/actions/action.go` — after `Scoped`:
```go
// Preview is implemented by an action that can say, without writing, what
// Execute would do to the target: its current state, the expected state after,
// whether it can be undone, and the target system's own dry-run verdict where
// one exists. The room service calls it once per step after proposal.created
// (10 s, read-only, the pipeline scope in ctx) and stores the result on
// proposal.previewed; a Preview error is stored as the step's error and never
// blocks the proposal. Preview must call only *Reader client methods plus the
// documented dry-run endpoints; it never changes a system.
type Preview interface {
	Preview(ctx context.Context, params json.RawMessage) (PreviewResult, error)
}

// PreviewResult and DryRunResult are the event types, so one definition serves
// the action, the room log and the API.
type (
	PreviewResult = events.PreviewResult
	DryRunResult  = events.DryRunResult
)
```
(`actions` gains the import `piperoom/internal/events`; `events` is a leaf, no cycle.)
`internal/room/snapshot.go` (A5, F4c) `Proposal.Preview *events.ProposalPreviewedPayload json:"preview,omitempty"`.
`docs/api.md`: event list + "`proposal.previewed {proposal_id, steps: [{step_index, preview?: {target, current,
expected, downstream?, reversible, rollback?, dry_run?: {ok, source, text, skipped?}}, error?}], at}` (system
actor, once, informational, not hashed)"; "`agent.verification {proposal_id, checks: [{layer, name, ok, detail}],
data_flow?: {checked, fresh, sinks: [{table, newest, baseline, passed, passed_by}], detail}, recovered, verdict,
repeat?}` replaces the `[verification]` agent.message"; `ProposalStep` gains `verify?`; snapshot
`open_proposals[].preview`.

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `internal/events/events.go`, `internal/actions/action.go`, `docs/api.md` | lead (F4a) | modified |
| `internal/actions/{kafka_actions,connector_config,preview}.go`, `internal/clients/connect/connect.go` (`ValidateConfig`), `internal/clients/schemareg/schemareg.go` (`CheckCompatibility`) + tests | A2 (F4b) | modified / new |
| `internal/room/{service_proposals,service_preview,snapshot}.go`, `internal/room/demo/fakes.go` + tests | A5 (F4c) | modified / new |
| `internal/agent/{verify,tools_room}.go` + `verify_test.go` | A4 (F4d) | modified |
| `web/src/api/types.ts`, `state/{types,reducer,describe}.ts`, new `lib/execution.ts`, `components/ProposalsPanel.tsx`, new `components/room/{BlastRadius,ExecutionCard}.tsx`, `tailwind.config.ts` | A6 (F4e) | modified / new |
| new `components/room/VerificationCard.tsx`, `components/{PipelineMap,RoomScreen,CloseRoomDialog}.tsx`, `lib/agentPill.ts`, `hooks/useRoom.ts` (no change expected) | A6 (F4f) | modified / new |

## Dependencies and merge order
F0 (F4a) → F4b (fakes from #128 `fakes_test.go`; no other dependency) and F4d in parallel → F4c (needs F4b's
`Preview` on real actions for the integration test; unit tests use a fake action implementing `Preview`) →
F4e (needs F4a types; F1's `roomStatus()` if merged, else the `agentPill` fallback) → F4f (needs F4e's card and
F4d's event in fixtures). Rebases: F4b on #128; F4c on #147 and #141 (`snapshot.go`, `service.go`); F4d on #136
(`verify.go:928`) and #140; F4e/F4f on #146, #135, #125 (`RoomScreen.tsx`, `reducer.ts`). F4 never touches the
executor's claim/step logic (`service_executor.go`), `params_hash`, or `prompt.go`.

## Design details
- **Sequence.** `propose_fix` → `proposal.created` (hash computed) → `proposal.previewed` (system, ≤ 30 s later)
  → approvals → executor `action.executed` × N → agent `agent.verification` (+ `agent.status done`) →
  UI: BlastRadius · ExecutionCard running → done · VerificationCard · Close nudge · lane heal flash.
- **Preview is informational, not approved.** It is not in `params_hash` (`ComputeParamsHash` unchanged), so a
  wrong preview cannot change what runs; `Current` names its read, a `DryRun` is the target's own words. The
  approval card keeps every existing element (fingerprint, void sentence, per-layer identity).
- **Read-only guarantee.** `Preview` lives in `internal/actions` (the only package allowed to hold a Writer) but
  calls Reader methods and the two documented validate endpoints only; the F4b tests assert via the #128 fakes
  that `RestartTask`, `SetCompatibility`, `Register`, `Resume`, `UpdateConfig` are never called during
  `Preview` (`TestPreviewNeverWrites`). The room service calls `Preview` outside `s.mu` with the pipeline scope.
- **Untrusted text.** `Current`, `Expected`, `DryRun.Text`, `Error`, `Detail`, `verdict` and `output` are
  system/agent text: text nodes only, `redact.Text` on the server before storage (A5 for preview errors, A4
  for check details — `clipLine` already applies). Rollback steer text is built from fields already in the log.
- **Event folding.** `proposal.previewed` before its `proposal.created` (replay over a snapshot) waits in a
  `pendingPreviews` map; `agent.verification` for an unknown proposal is dropped; a second `proposal.previewed`
  for the same id replaces the first (future refresh).
- **Failure modes.** No `Preview` on an action → "no preview for this action", card still approvable. Topology
  404 → `downstream unknown`. Room closed mid-preview → event dropped. Executor failure → `Stopped at step N`,
  no verification event (the failed-step message stays), Roll back offered. `outcome_unknown` → amber row, Roll
  back dialog adds `The last step's outcome is unknown; check the target before rolling back.` Not recovered →
  destructive tone, no Close nudge, the agent keeps working.
- **Light/dark, motion, a11y, mobile.** Tones from tokens only (`--agent`, `--success-subtle`,
  `--warning-subtle`, `--destructive`); `animate-check` 200 ms, `animate-heal` 600 ms, both `motion-safe:` with
  `cubic-bezier(.2,.8,.2,1)`; one amber per screen fact. `ExecutionCard` header is the card's single live
  region; check icons carry `aria-label`s; the Roll back dialog restores focus (`useRestoreFocus`); `kbd C`
  hidden on coarse pointers. BlastRadius blocks stack, the downstream chain wraps on `→`, tables scroll inside
  the card, the Close nudge is full-width and 44 px tall below 480 px.

## Done when
- [ ] F4b: all six actions implement `Preview`; `TestPreviewNeverWrites` passes with the #128 fakes; the two
      dry-run client methods are exercised with `httptest` (compatible / incompatible with messages / 404 /
      Registry without `messages`; validate 0 errors / 2 errors / 405); `go test ./internal/actions/...
      ./internal/clients/...` without Docker; `-tags integration`: on the dev stack a `schema_register`
      preview of the incompatible `amount: bytes` schema returns `dry_run.ok == false` with the Registry message.
- [ ] F4c: every proposal gets exactly one `proposal.previewed` (system actor) within 30 s; `downstream` for
      `connect_restart_task pg-orders-connector` is `[pg.public.orders topic, order-sync, analytics.orders]`
      node ids on the demo topology; snapshot `open_proposals[].preview` set; `verify` sanitized;
      `go test ./internal/room/...` passes; `--demo` shows a previewed proposal.
- [ ] F4d: after the last step's `action_result` the room log has `agent.verification` (with `checks`,
      `data_flow`, `recovered`, `verdict`) and no `[verification]` `agent.message`; the model's note text is
      byte-identical to before; `repeat` on a repeated result; `verify` round-trips from `propose_fix` to
      `ProposalStep.Verify`; `go test ./internal/agent/...`; `ledger-producer-timeout-recovery` and the default
      eval set unchanged.
- [ ] F4e: the approval card shows per-step target / current → expected / downstream names / reversible /
      dry-run verdict, `Previewing…` then `Preview unavailable` after 20 s with no event; the card flips to
      `Running · step 1 of 2` on the set-completing approval and draws each check; `outcome_unknown` renders the
      amber row; `exec-hash-N` test ids kept; `npm run build && npm run lint && npx vitest run`.
- [ ] F4f: the verification card renders per-layer ✓/✗ and per-sink lines; the Close nudge opens the close
      dialog prefilled for the driver only; Roll back sends the exact steer text (`steer.sent` for the driver,
      `steer.suggested` for a member, absent for a viewer); a node that heals flashes once; fixtures mode shows
      the whole ceremony from `schema-change.json` (add `proposal.previewed`, two `action.executed`, one
      `agent.verification` and a healed topology frame to the fixture player).
- [ ] Only owned files changed; contract files untouched; each PR < 600 lines.

## Tests
- Go, F4b (`internal/actions/preview_test.go`): `TestPreviewRestartTaskReadsStatus`, `TestPreviewRestartTaskTargetGone`
  (404 → wrapped `clients.ErrNotFound`), `TestPreviewSetCompatibilityReversible`, `TestPreviewSetCompatibilityGlobalLevel`,
  `TestPreviewRegisterSchemaDryRun` (compatible/incompatible/skipped without checker), `TestPreviewUpdateConfigDryRun`,
  `TestPreviewUpdateConfigSkipsSecrets`, `TestPreviewNeverWrites`, `TestPreviewHonoursDeadline` (fake blocks; 10 s
  fake clock → error). Clients: `TestCheckCompatibilityVerbose`, `TestCheckCompatibilityNoMessages`,
  `TestValidateConfigErrors`, `TestValidateConfigNeverLogsValues` (slog buffer), `TestValidateConfig405`.
- Go, F4c (`internal/room/service_preview_test.go`): `TestPreviewEmittedOncePerProposal`, `TestPreviewSkipsActionWithoutPreview`,
  `TestPreviewErrorIsRedactedAndStored`, `TestPreviewDownstreamFromTopology` (fake builder with a 4-node graph,
  one `depends_on` edge excluded), `TestPreviewDroppedWhenProposalClosed`, `TestPreviewWallTime` (fake clock, 30 s),
  `TestApplyProposalPreviewed` (table), `TestSanitizeProposalVerify`, integration `TestPreviewOnDevStack`.
- Go, F4d: `TestVerificationEventRecovered`, `TestVerificationEventLayerFailed` (data flow `checked: false`,
  detail "not checked because a layer check did not pass"), `TestVerificationEventDataFlowSinks`,
  `TestVerificationRepeat`, `TestVerificationNoteUnchanged` (golden of `out.notes`), `TestProposeFixVerifyField`,
  `TestProposeFixVerifyTooLong` (201 chars → `ErrBadInput`).
- Vitest, F4e: `lib/execution.test.ts` (phase table incl. `outcome_unknown`), `state/reducer.preview.test.ts`
  (previewed-before-created, replace, verification for unknown proposal dropped),
  `components/room/BlastRadius.test.tsx` (full preview, step error, skipped dry-run, downstream names, `Preview
  unavailable` after 20 s with fake timers), `components/room/ExecutionCard.test.tsx` (running header text,
  1-based steps, unknown row, hash badge kept, `aria-live` once).
- Vitest, F4f: `components/room/VerificationCard.test.tsx` (recovered/not, sinks, repeat, nudge per role),
  `components/CloseRoomDialog.test.tsx` (+ `initialText`), `components/ProposalsPanel.rollback.test.tsx` (exact
  steer text, driver vs member vs viewer), `components/PipelineMap.heal.test.tsx` (fail→ok adds the class once,
  not on first render), `lib/agentPill.test.ts` (+ `Resolved · verified`).

## Effort
F4a S (lead) · F4b M (A2, ~550 with tests; the two client methods may ship first as a ~200-line PR) · F4c M
(A5, ~450) · F4d M (A4, ~400) · F4e M–L (A6, ~550) · F4f M (A6, ~500). ≈ 2.5 k lines over 6–7 PRs, none over 600.
