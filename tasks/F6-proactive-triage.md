# F6 — Proactive triage: the watcher opens the room with a one-line diagnosis; the list is pushed

Owners: F6a lead (contracts) · F6b A7 (`internal/watch/**`) · F6c A4 (`internal/agent/**`) · F6d A5
(`internal/room/**`, `internal/ws/**`, `internal/httpapi/**`, `cmd/server/**`, `internal/store/**`) · F6e A6 (`web/**`).
Model: **Opus 5.5** for F6b and F6c (state machines with fake clocks; a new model call path that must stay inside
the budget, redaction and untrusted-text rules). **Sonnet 5.5** for F6d and F6e (plumbing against a fixed frame).
Base: `main` after F0 and the R-wave PRs (#125, #126, #135, #140, #141, #142–#147). Read `AGENTS.md`, `docs/collab.md`,
`docs/design/watcher.md`, `docs/api.md` (WebSocket, Watcher alerts, Spend limits) and `docs/product/vision-2026-10.md` §2.

## Goal
At 03:12 the pager fires and the room list already reads *"orders: slot `debezium_orders` inactive 6m; task 0 FAILED
on MISSING_UNION_BRANCH after DDL on public.orders — suspected kafka-connect, medium"*. The on-call knows the shape
before the laptop is open, the row appeared the second the watcher opened it (no 15 s poll), and the full
investigation starts from the same numbers the watcher saw. Time-to-first-insight is the metric a pilot remembers.

## Evidence
- `PIPEROOM_WATCH_MODE` defaults to `log` (`internal/config/config.go:685`), so nothing opens a room by default; the
  v2 names `consumer_lag`, `freshness`, `long_tx` are parsed and dropped (`internal/owners/watch.go:60,224`); a
  crash-looping task reported RUNNING is invisible (`internal/watch/watch.go:21-24`). Platform review §4 gap 12.
- Storm cap = agent cap: `cmd/server/main.go:434` wires `MaxOpenRooms: maxAgents`; a 5-pipeline Connect outage leaves
  two rooms "waiting for an agent slot" with no diagnosis; the map is built only after the slot (`service.go:1151`). Gap 13.
- Re-fires and watcher joins are system `agent.message`s that `inputFor` (`internal/room/service.go:1002`) never
  feeds to the inbox; the agent never sees the watcher's rate/ETA/`wal_status` numbers (watcher.md I5; gap 4).
  #140 adds description/labels of generic and Alertmanager bodies to the first message, not the watcher shape.
- The rooms list is a 15 s poll (`web/src/components/RoomList.tsx:58,110`; oncall 03: the row came 14 092 ms after
  the webhook, screen `03-rooms-list-with-alert-dark.png`); vision D7; ux-critique #11.
- The room header shows the title and the agent pill only (`web/src/components/room/RoomHeader.tsx:63-82`); the first
  words a human reads are the alert title, never a diagnosis (platform §3 #2).

## Scope — what to build
1. **F6a contract** (lead): `agent.triage` event + payload; `UsageRoleFirstLook`; `HumanInput.Kind "alert_update"`;
   `Alert.Triage`; `Triager`; config vars; `docs/api.md` snapshot field, list WebSocket, alert-update rule. §Contract.
2. **F6b watcher v2** (A7): signals `consumer_lag`, `group_unstable`, `offset_below_log_start` and a task crash-loop
   counter inside `connector_down`; structured evidence on every join (`PostEvidence` gains the evidence JSON);
   the open-room cap moves to `PIPEROOM_WATCH_MAX_OPEN_ROOMS`. Default mode `rooms` is a config change (F6a).
3. **F6c triage pass** (A4): `Endpoint.Triage`: one bounded first look outside the agent pool on
   `PIPEROOM_TRIAGE_PASS_MODEL` (default `claude-haiku-5-5`), at most 2 read tools, ≤ 40 s, emitting
   `agent.triage`; the watcher evidence and the triage result in the main run's first message; `alert_update` inputs.
4. **F6d room/server** (A5): run the triage pass before the slot gate; `triage` in `RoomSnapshot` and `GET /api/rooms`
   rows; `alert_update` from re-fires and watcher joins; `GET /api/rooms/ws` pushing the list; wiring.
5. **F6e web** (A6): `TriageCard` in the room header until the first hypothesis, the headline as the row's second
   line, the list fed by the push with a poll fallback.

## Out of scope
Slack/PagerDuty triage lines (C12 follow-up), `freshness`/`long_tx`/`schema_changed`/`cert_expiry` signals (F12 adds
the reads they need), a Prometheus `/metrics` for the watcher, cross-pipeline correlation, browser notifications
(F1), first-look reads inside the main run (the triage pass replaces platform §3 #7), `logs_search`.

## Contract (F6a — exact diffs)
`internal/events/events.go`:
```go
	// AgentTriage is the bounded first look written by the triage pass before
	// the full investigation (docs/design: tasks/F6-proactive-triage.md). One
	// per run of the pass; a room may hold several (a re-fire re-runs it).
	AgentTriage Type = "agent.triage"

	UsageRoleFirstLook = "first_look" // a model call of the triage pass (agent.triage)

// AgentTriagePayload is the triage pass's one-line verdict. It is labelled
// "triage, unverified" wherever it is shown and never carries a proposal.
// Status "ok" has a Headline; "failed" has none and Reason says why (fixed
// codes: "timeout", "model_error", "budget", "refused", "no_tools", "off").
type AgentTriagePayload struct {
	Status         string   `json:"status"`                    // "ok" | "failed"
	Reason         string   `json:"reason,omitempty"`          // failed only
	Headline       string   `json:"headline,omitempty"`        // one sentence, ≤ 200 chars, plain text
	SuspectedLayer string   `json:"suspected_layer,omitempty"` // one of Layers, or "" when unsure
	Confidence     string   `json:"confidence,omitempty"`      // "low" | "medium" | "high"
	EvidenceRefs   []string `json:"evidence_refs"`             // step ids of the pass's reads; never null
	NextCheck      string   `json:"next_check,omitempty"`      // the one read the full run should do first, ≤ 120 chars
	Model          string   `json:"model"`
	TookMS         int64    `json:"took_ms"`
}
```
`internal/agent/contract.go`:
```go
	Kind      string       `json:"kind"` // "steer" | "comment" | "approval_result" | "action_result" | "alert_update"
	// Text of an "alert_update" is the server's rendering of a re-fired or
	// joined alert's evidence (AlertEvidenceText). It is untrusted data, it
	// never wakes an idle agent and never resets the turn guard.

// Alert gains:
	// Triage is the triage pass's result when it ran before this run (nil
	// otherwise). Its steps are already in the room log; the runner lists
	// them as known step ids so a hypothesis may cite them.
	Triage *TriageResult `json:"triage,omitempty"`

// TriageResult is what the triage pass returns to its caller.
type TriageResult struct {
	Payload events.AgentTriagePayload `json:"payload"`
	Steps   []TriageStep              `json:"steps"`
}

// TriageStep is one read the pass made, for the main run's first message.
type TriageStep struct {
	StepID  string `json:"step_id"`
	Tool    string `json:"tool"`
	Summary string `json:"summary"` // the tool summary, redacted; ≤ tools.MaxSummaryBytes
	IsError bool   `json:"is_error"`
}

// Triager is implemented by A4 (Endpoint.Triage): one bounded first look on a
// cheap model, at most 2 read tools, outside the agent pool. It emits its own
// steps and one agent.triage through deps.Sink and returns the same payload.
// It returns an error only when nothing was emitted (ctx ended first).
type Triager func(ctx context.Context, alert Alert, deps Deps, opts Options) (TriageResult, error)
```
`internal/config/config.go` (fields, defaults, `ParseTriagePass`):
```go
	TriagePass        string // PIPEROOM_TRIAGE_PASS: on | off, default on
	TriagePassModel   string // PIPEROOM_TRIAGE_PASS_MODEL: default "claude-haiku-5-5"
	TriagePassTimeout string // PIPEROOM_TRIAGE_PASS_TIMEOUT: Go duration, default 40s, max 90s
	WatchMaxOpenRooms string // PIPEROOM_WATCH_MAX_OPEN_ROOMS: rooms the watcher keeps open at once; default 6, min 1
	DefaultWatchMode = "rooms" // was "log"; --demo still forces off; setup.md documents "log" for the shadow week
```
`docs/api.md`: `RoomSnapshot` gains `"triage": {status, headline, suspected_layer, confidence, evidence_refs, at} | null`
(the newest `agent.triage`); `GET /api/rooms` rows carry it. New route `GET /api/rooms/ws` (§Design 4). Watcher alerts:
"a join carries the evidence JSON; the service feeds it to the agent as an `alert_update`". Errors: none new.

## Ownership and files
| File | Owner | New/mod |
|---|---|---|
| `internal/events/events.go`, `internal/agent/contract.go`, `internal/config/config.go`, `docs/api.md` | lead (F6a) | mod |
| `internal/owners/watch.go` (+`_test`): `consumer_lag`, `group_unstable`, `offset_below_log_start`, crash-loop keys | A3 (F6b-owners, S) | mod |
| `internal/watch/{watch,observe,signals,output,state}.go`, new `lag_signals.go`, `crashloop.go`, tests | A7 (F6b) | mod/new |
| `internal/agent/triage_pass.go`, `triage_pass_test.go`, `alerttext.go` (watcher shape), `prompt.go`, `run.go`, `endpoint.go` | A4 (F6c) | new/mod |
| `internal/room/{service,service_room,service_watch,snapshot}.go`, new `service_triage.go`, `listhub.go`; `internal/ws/listgateway.go`; `internal/httpapi/server.go`; `cmd/server/{main,watch}.go`; `internal/room/demo/demo.go` | A5 (F6d) | mod/new |
| `web/src/{hooks/useRoomList.ts,api/{client,types}.ts,state/{types,reducer,describe}.ts,lib/rooms.ts,components/{RoomList.tsx,room/RoomHeader.tsx,room/TriageCard.tsx,shell/AppShell.tsx}}` + tests | A6 (F6e) | mod/new |

## Dependencies and merge order
F6a first (one contract PR). F6b and F6c in parallel against F6a; F6d after F6c (it calls `Endpoint.Triage`) and
F6b (new `PostEvidence` signature); F6e after F6a (frame fixed) with fixtures, verified against F6d. F6c touches
`prompt.go` after #140 (`alertDetails`) and #141; F6e builds on #146's `useLoad`/`RoomHeader` and #125's
`useRoomAttention`. Nobody edits a contract file outside F6a. F1's `roomStatus()` reads `triage` from the snapshot;
F11's cost chip reads `first_look` usage rows.

## Design details
### 1. Watcher v2 signals (F6b)
`owners.yaml` keys (A3 adds the parsing; `v2Signals` keeps only `freshness`, `long_tx`, `compat_none`, `unreachable`):
```yaml
signals:
  consumer_lag:           {fire: 1000, clear: 500, for: 10m, members_expected: true}   # default enabled, action room
  group_unstable:         {for: 5m, flaps: 3, window: 15m}                             # default enabled, action room
  offset_below_log_start: {}                                                           # default enabled, action room, no `for`
  connector_down:         {for: 3m, crashloop_restarts: 3, crashloop_window: 15m}      # new keys on the v1 signal
```
Reads per tick, per client set, once: `GroupLag` for every group of the set's pipelines (new `watch.LagReader`
interface = `GroupLag(ctx, group) (clients.GroupLag, error)`, wired from the same `*kafka.Client`; nil → the three
signals stay unknown with one log line) and `DLQOffsets(cdcTopics...)` for the log-start offsets (`clients.DLQReader`
already fetches start/end without records; name kept, it is the cheap offsets call). Each call under `CheckTimeout`
through the existing `call()`. Verdicts (`reading` as today, time-based `for` through `sigState.step`):
- `consumer_lag`: value = `TotalLag`; `vBreach` ≥ `fire`, `vHold` between, `vClear` < `clear`. Rate = least-squares slope
  of the last 30 min of samples (`rate.go`, new per-group ring in `pipeState.lagSamples`). Severity sev3; sev2 when
  `members == 0` with `members_expected` (state `Empty`/`Dead`) or the slope stays > 0 for 2×`for`. Evidence:
  `{signal, group, state, members, total_lag, lag_rate_per_min, worst_partitions: [{topic, partition, lag}] (≤3)}`.
  Label `group order-sync lag 1471 (+310/min, 0 members)`.
- `group_unstable`: breach when `state ∉ {Stable, Empty}` (`PreparingRebalance`, `CompletingRebalance`, `Dead`
  with members) for `for`, **or** ≥ `flaps` state changes within `window` (ring of the last 12 `(at, state, members)`),
  which fires at once (`vBreachNow`). Severity sev3; sev2 with `consumer_lag` also firing. Evidence:
  `{signal, group, state, members, state_changes_15m, states: [{at, state, members}] (≤6)}`.
- `offset_below_log_start`: for every partition of the group's lag rows on a pipeline topic, `committed >= 0 &&
  committed < start` → `vBreachNow`, sev1 (records are already gone). Clear when no partition is below for `for`
  (use `connector_down.for`). Evidence: `{signal, group, topic, partition, committed, log_start, records_lost}` for the
  worst ≤ 3 partitions. Label `group order-sync committed below log start on pg.public.orders/2 (17.2M records gone)`.
- crash-loop counter: `pipeState.taskHist[connector/task]` ring of `(at, state)` per tick; a `FAILED→RUNNING` edge
  is a restart; ≥ `crashloop_restarts` restarts inside `crashloop_window` makes `classifyConnectors` report the task
  as bad with label `task 0 crash-looping (3 restarts in 15m)`, sev2, trace = the last FAILED first line, even when
  the current state is RUNNING. Honest limit in the doc comment: loops faster than `interval` are seen as RUNNING;
  F12's `debezium_metrics` is the detector for those.
Evidence of every signal is the same JSON the room gets (`compose`), now also passed on joins:
`RoomSink.PostEvidence(ctx, roomID, text string, evidence json.RawMessage) error` (A7 changes the interface and
the fakes; A5 the adapter). `signalIDs` order: the three new ids go after `config_risk`.
Cap: `Deps.MaxOpenRooms` is wired from `PIPEROOM_WATCH_MAX_OPEN_ROOMS` (default 6), not from `PIPEROOM_MAX_AGENTS`;
a room beyond `PIPEROOM_MAX_AGENTS` queues for the pool but still gets the triage pass (§2), so it is never silent.

### 2. Triage pass (F6c)
`func (e *Endpoint) Triage(ctx, alert, deps, opts) (TriageResult, error)` in `internal/agent/triage_pass.go`:
- Config via `EndpointConfig{TriagePass bool, TriagePassModel string, TriagePassTimeout time.Duration}` from
  `config.ParseTriagePass`. Off → emit `agent.triage {status: failed, reason: off}` and return (so the UI never waits).
- Budget: the call goes through `budgetModel` like every turn (room token budget, pipeline caps, `agent.usage` with
  role `first_look`); a `*BudgetError` → `status: failed, reason: budget`.
- Deadline: `min(TriagePassTimeout, 90 s)` on a context derived from the caller's; tools get `toolTimeout` (10 s).
- Step 1, tool choice. If `alert.Raw` is the watcher shape (`source == "watcher"`, `signals[0].signal` known), use the
  table, no model call: `connector_down → connect_status(first bad connector) + pg_recent_ddl(since now-6h)`;
  `slot_inactive → connect_status + pg_replication_slots`; `slot_wal → pg_long_transactions + pg_replication_slots`;
  `dlq_growth → dlq_peek + connect_status`; `consumer_lag`/`group_unstable` → `kafka_consumer_lag(group) +
  connect_status`; `offset_below_log_start → kafka_topic_offsets(topic) + kafka_topic_config(table)`; `config_risk →
  connect_config + connect_status`. Otherwise one model turn (`max_tokens` 1024, effort low, no thinking, strict
  tools = `deps.Tools` only, system prompt `triagePrompt()`: "pick at most 2 reads") and keep the first 2 `tool_use`
  blocks; the rest get a tool error `triage budget: 2 reads`. Inputs are validated by the tools as usual.
- Step 2: run the ≤ 2 reads in parallel with `runReadTool` (same step/tool_call/tool_result events, redaction,
  injection flag, `wrapToolOutput`). Step 3: one model call with `OutputConfig.Format` JSON schema
  `{headline, suspected_layer (enum Layers + ""), confidence, next_check, based_on_steps}` (same shape as
  `handoffSchema()`), `max_tokens` 512, effort low. Input = `firstUserMessage(alert)` + the two wrapped outputs.
  Headline: `capFreeText`, ≤ 200 chars, `escapeToolText` is **not** applied (it is stored, not prompted); the runner
  wraps it when it reads it back. `based_on_steps` filtered to the pass's own step ids → `EvidenceRefs`.
- Emit `agent.triage` once, always, from a `defer` (`status: failed` with the reason on timeout/model error/refusal).
- Main run: `Alert.Triage` set by A5. `firstUserMessage` adds, after the labels: "First look (triage, unverified;
  confidence X; steps s_1, s_2): <untrusted>headline</untrusted>" and each `TriageStep` as
  `wrapToolOutput(tool, step_id, summary)`; the runner registers the step ids as known (hypothesis citations, F3
  grounding). The prompt method gains one line: "A first look is a hint, verify it; do not repeat a read it already
  shows unless the numbers may have changed." `TestSystemPromptIsStable` updated.
- `alerttext.go`: `alertDetails` learns the watcher shape: `signals[]` rendered as `signal=slot_wal severity=sev2
  since=… firing_for=…s slot=… retained_wal_bytes=… rate_6h_bytes_per_hour=… eta_hours=…` from an **allow-list of
  keys** (numbers, booleans, RFC 3339 times, the fixed-vocabulary strings `state`, `wal_status`, `eta_basis`, risk
  codes); the only free text kept is a task `trace` first line, masked. ≤ 6 signals, ≤ 400 chars each. Exported
  `AlertEvidenceText(raw json.RawMessage) string` (same renderer) for the server's `alert_update`.
- `formatHumanInput`: `alert_update` → `[alert update] <untrusted>…</untrusted>`; `hasHumanInput` ignores it (no
  turn reset, no "idle → working" status by itself).

### 3. Room service (F6d)
- `startAgentLocked` (non-shadow): before `gate.acquire`, start `go s.runTriage(ctx, roomID, alert, deps)` under a
  process-wide semaphore of 4 (constant `maxTriagePasses`; a 5th room waits, it never fails). Its Sink and tools are
  the room's; its inbox is a closed one. The main supervisor waits for the triage result for at most
  `TriagePassTimeout + 5 s` *only if* it already holds a slot (otherwise it waits anyway); it then sets `alert.Triage`.
  Shadow rooms: no triage (nothing may enter the room log). `Load` after a restart: no triage for restored rooms.
- Re-fire of the same alert (`joinAlertRoomLocked`, now given `raw`) and a watcher join with evidence
  (`PostRoomNote(ctx, roomID, text, evidence)`): besides the system note, push `HumanInput{Kind: "alert_update",
  Actor: systemActor, Seq: e.Seq, Text: agent.AlertEvidenceText(raw)}` with `inbox.pushQuiet`: `Drain` returns it,
  `Wait` does not count it, and a pending quiet input is replaced (one per room at a time). A re-fire ≥ 15 min after
  the last `agent.triage` on a room whose agent is `idle` or `queued` re-runs the triage pass (new numbers, no pool).
- `Apply`: `events.AgentTriage` → `snap.Triage = &Triage{Status, Headline, SuspectedLayer, Confidence, EvidenceRefs,
  At}`; `agent.hypothesis` does **not** clear it (the UI decides what to show).
- `ListRooms` is unchanged; the push comes from a hub in `internal/room/listhub.go`: `SubscribeRooms(ctx) <-chan
  RoomSnapshot` fed from every fold (`emitLocked`, `fillGapLocked`, `syncRoom`, close, delete) after `s.mu` is
  released; per subscriber a buffer of 64, drop-and-resync on overflow (the gateway re-lists). With a shared store the
  gateway also re-lists every 10 s and diffs (`refreshAll` is what `ListRooms` does today).
### 4. List WebSocket (F6d, `internal/ws/listgateway.go`)
`GET /api/rooms/ws` (same auth as `/rooms/{id}/ws`, same origin rules, caps `MaxConnsPerUser` from #141). Frames:
```json
{ "type": "rooms", "rooms": [RoomSnapshot…] }                 // first frame: exactly GET /api/rooms for this user
{ "type": "room",  "room": RoomSnapshot }                      // one room changed (opened, updated, closed)
{ "type": "room_deleted", "room_id": "r_…" }
{ "type": "error", "error": { "code": "session_expired", "message": "…" } }   // then close, as the room socket
```
Client → server: `{ "type": "ping" }` every 20 s (read timeout 60 s as today). Rules: tenancy filter per connection
(`roomVisible` rule, a room outside the tenants is never sent); a `room` frame is sent only when `listDigest(snap)`
changed — a hash over `{status, title, severity, driver, agent_status.state, triage, hypothesis.{text,confidence},
open_proposals[].{proposal_id, approvals}, pending_suggestions count, closed_at, resolution}` — so step churn and
`last_seq` alone send nothing; coalesce to ≤ 1 frame per room per 500 ms; a full send buffer closes with policy
violation and the client reconnects (it gets a fresh `rooms` frame, so no state is lost). No replay cursor: the
list is small and the first frame is the truth.
### 5. Web (F6e)
- `useRoomList`: opens `/api/rooms/ws` (via `openListWs(after=none)`), folds `rooms`/`room`/`room_deleted` into the
  array (replace by id, sort with `sortRoomsForList`), reconnects with backoff 1 s → 30 s; after 3 failed opens in a
  row, or in fixtures mode, falls back to the 15 s poll (`POLL_MS` stays) and shows the existing "updated Ns ago"
  text; `reload()` stays for Retry. `RoomList.tsx` drops its own interval; `AppShell` keeps one list.
- Row second line (`RoomList.tsx`): the triage headline in `text-muted-foreground text-xs`, clamped to one line,
  prefixed by a `Badge variant="agent"` reading `first look` and the suspected layer badge (`Badge` layer variant
  from #135); `status: failed` → nothing; no triage yet on an open room < 2 min old → a 3-dot shimmer with
  `aria-label="First look in progress"`; older → nothing. The headline is plain text (never HTML).
- `TriageCard.tsx` in the room header under the title row: `First look · triage, unverified · medium` + headline +
  "based on steps 1, 2" chips that scroll to the step rows (same mechanism as F3's hypothesis chips; until F3 lands
  a plain `<a href="#step-…">`) + `Next: <next_check>`. Shown while `hypothesis === null`; once a hypothesis exists it
  collapses to one muted line `First look: …` above the hypothesis card; `status: failed` → `First look unavailable ·
  <reason>` in muted text for 60 s, then hidden. Dark/light via tokens only (`bg-card`, `text-muted-foreground`,
  `border-agent/30`); 16 px gutter; on a phone the card stacks under the badges. `aria-live="polite"` on the headline.
- Reducer: `agent.triage` → `state.triage` (last wins); `describe.ts`: `First look: <headline>` / `First look
  failed (<reason>)`; `agentPill` unchanged. `document.title` and notifications belong to F1.
### Failure modes
Triage model 4xx (`TriageModelWarning` already names incompatible ids) → `failed/model_error`, logged once per
process; no retry. Tool timeouts → the pass still answers from what it has (a headline at `low`). Watcher cap
reached → log line as today. List socket refused (`429 too_many_connections`) → poll fallback, no toast storm.

## Done when
- [ ] `devctl break consumer-lag` opens a room within 3 intervals with `consumer_lag` in the title; `devctl fix` resolves it.
- [ ] `devctl break connector-auth` fires `connector_down` with "crash-looping" within `crashloop_window` on the dev stack.
- [ ] A watcher room shows `agent.triage` with a non-empty headline within 40 s of `room.created` (server log line
      `agent: first look` with `took_ms`), with `PIPEROOM_MAX_AGENTS=1` and two rooms queued.
- [ ] `agent.usage` for the pass has `role: first_look`; `GET /api/rooms/{id}/usage` counts it; a spent budget gives `reason: budget`.
- [ ] The first user message of the main run contains the triage headline inside `<untrusted>` and the watcher numbers
      (`TestFirstUserMessageWatcherEvidence`), never a raw trace beyond its first masked line.
- [ ] A second `POST /api/alerts/generic` with the same `dedup_key` produces one `alert_update` in the next turn's
      history (`TestAlertUpdateReachesNextTurn`) and does not wake an idle agent.
- [ ] `GET /api/rooms/ws` sends `rooms` then `room` frames; a browser with the list open shows a new watcher room
      within 1 s of `room.created` without a poll (`RoomList.push.test.tsx`).
- [ ] `PIPEROOM_WATCH_MODE` unset → log line `server: watcher starting mode=rooms`; `--demo` → off.
- [ ] `gofmt`, `go vet`, `go test ./...`, `npm run lint && npm run build`, `go test -tags integration ./internal/watch/...` green.

## Tests
Go (fake clock `watch.Clock`, fakes in `fakes_test.go`): `TestConsumerLagFiresAfterForAndClearsWithHysteresis`,
`TestConsumerLagEmptyGroupIsSev2`, `TestGroupUnstableFlapsFireAtOnce`, `TestOffsetBelowLogStartIsSev1AtOnce`,
`TestCrashLoopCounterFiresWhileRunning`, `TestLagReaderMissingLeavesSignalsUnknown`,
`TestMaxOpenRoomsIsNotMaxAgents`, `TestPostEvidenceCarriesJSON` (A7);
`TestTriagePassUsesSignalTable`, `TestTriagePassModelPicksAtMostTwoTools`, `TestTriagePassTimeoutEmitsFailed`,
`TestTriagePassBudgetEmitsFailed`, `TestTriageHeadlineIsCappedAndPlain`, `TestFirstUserMessageTriageBlock`,
`TestFirstUserMessageWatcherEvidence` (allow-list, trace masked), `TestAlertUpdateDoesNotResetTurns`,
`TestSystemPromptIsStable` (A4); `TestTriageRunsBeforeSlot`, `TestTriageSkippedForShadowAndRestoredRooms`,
`TestQuietInputDoesNotWake`, `TestApplyAgentTriage`, `TestListHubCoalesces`, `TestListGatewayFirstFrameEqualsList`,
`TestListGatewayTenancyFilter`, `TestListGatewayDigestSkipsStepChurn` (A5).
Vitest: `useRoomList.push.test.tsx` (rooms/room/room_deleted folding, reconnect, poll fallback after 3 failures),
`RoomList.triage.test.tsx` (second line, shimmer < 2 min, failed hidden), `TriageCard.test.tsx` (unverified label,
collapse after hypothesis, a11y: one `region` with a name), `reducer.triage.test.ts`.
Eval: `evals/scenarios/watcher-connector-down.yaml` (C6): alert `Raw` = watcher evidence JSON; a pass must name
`kafka-connect`; `-validate` green.

## Effort
F6a S (lead) · F6b-owners S (A3) · F6b **L** (A7; split PRs: lag signals + offset signal / crash-loop + evidence +
cap) · F6c **M** (A4; split: pass + alert text / alert_update + first message) · F6d **M** (A5; split: triage wiring
+ snapshot / list hub + gateway) · F6e M (A6; split: list push / triage card).
