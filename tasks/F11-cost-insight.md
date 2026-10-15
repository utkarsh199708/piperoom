# F11 — Cost and insight metrics in the room

Owners: **F11a** A5 (`internal/room`, `internal/httpapi`) · **F11b** A6 (`web/**`) · C6 reconciles the evals
calibration report with the route (one line in `evals/README.md`, no code until F8's label lands).
Model: F11a Sonnet 5.5 (a fold over existing readers, a cached aggregate, one route; the shadow metrics route is
the template); F11b Sonnet 5.5.

Assumes merged: #146 (`usePipelines`, `useLoad`), #126 `r12/web-shadow-mode` (`state.shadow`, shadow metrics
page), #125 (toasts). F8's `POST /api/rooms/{id}/label` is designed by the F8 architect; F11 **references** its
event and reads it, it does not define it. F9c1 edits `HypothesisCard.tsx` first; F11b rebases on it.

## Goal
A pilot's first invoice must not be the first time they see what the agent costs, and "high confidence" must come
with the agent's own record on this pipeline. Every live room shows `$0.42 · 11 steps · 2m 10s` and "first
hypothesis in 48 s"; next to the confidence badge the room says `right 11 of 12 times at high on orders (90 d)`.

## Evidence
- Platform review §4 gap 14 ("Cost is invisible… the first invoice is the first time a pilot sees it"), gap 15
  (`time_to_first_hypothesis_s` only for shadow runs: `internal/room/shadow.go:390,847`), §3 #6 (track record;
  `evals/calibration.go:55 ReliabilityBucket`). `docs/design/cost.md` §8: the web reducer drops `agent.usage`
  (`web/src/state/reducer.ts:65`); a cost chip was left as "an optional follow-up".
- `agent.usage` today (`events.go AgentUsagePayload`): model, role (`triage|main|handoff|compaction|judge`), four
  token counts, `latency_ms`, `cost_usd` (omitted when unpriced). `GET /api/rooms/{id}/usage` exists
  (`internal/httpapi/usage.go`) but needs a second request and is not live.
- Oncall §5 "Incident clocks" and "Agent cost / budget in the room": both listed as missing against the best tool
  in the category.

## Scope — what to build

### F11a (A5)
1. **`RoomSnapshot.insight`** (`snapshot.go Apply`, pure fold, no migration): 
   `{"first_hypothesis_at": "…"|null, "time_to_first_hypothesis_s": 48|-1, "final_confidence": "high"|"",
   "steps": 11, "tool_calls": 14, "tool_errors": 1, "usage": {"calls": 9, "unpriced_calls": 0, "tokens": 812200,
   "cost_usd": 0.42|null, "latency_ms_total": 61200}}`. Fold rules: `agent.step.started` → `steps++`;
   `agent.tool_call` → `tool_calls++`; `agent.tool_result.is_error` → `tool_errors++`; first `agent.hypothesis` →
   `first_hypothesis_at = e.At`, `time_to_first_hypothesis_s = int(e.At − OpenedAt)` (floor, ≥ 0); every
   `agent.hypothesis` → `final_confidence`; `agent.usage` → `calls++`, `tokens += in + cache_create + cache_read +
   out`, `latency_ms_total += latency_ms`, `cost_usd += cost_usd` or `unpriced_calls++` when nil; `cost_usd` is
   `null` while `calls == unpriced_calls` (never 0 for unknown, cost.md 2.1), otherwise a lower bound (api.md
   `Usage` rule). Shadow rooms: zeros (their agent events are in the shadow log). The same numbers the web folds
   live (item 7); the snapshot carries them so the room list and `agent-stats` need no replay.
2. **`GET /api/pipelines/{pid}/agent-stats?days=90`** (`internal/httpapi/agentstats.go`,
   `internal/room/agentstats.go`). Visibility first: unknown pipeline or outside the caller's tenants →
   `404 not_found` (`CanSeePipeline`, as `GET /api/pipelines`); then any logged-in user, viewers included (numbers
   only, like `GET /api/shadow/metrics`). `days` 1–365, default 90, else `400 validation`. Answer:
   ```json
   { "pipeline_id": "orders", "window_days": 90, "generated_at": "…",
     "rooms": 23, "hypothesised": 21, "labelled": 17, "graded": 17,
     "layer_correct": 15, "root_cause": {"yes": 11, "partial": 4, "no": 2},
     "by_confidence": [ {"confidence": "high", "runs": 12, "correct": 11, "accuracy": 0.917},
                        {"confidence": "medium", "runs": 4, "correct": 2, "accuracy": 0.5},
                        {"confidence": "low", "runs": 1, "correct": 0, "accuracy": 0} ],
     "median_time_to_first_hypothesis_s": 48, "median_human_mttr_s": 720,
     "median_cost_usd": 0.42, "cost_available": true, "enough_data": true,
     "shadow": {"rooms": 6, "graded": 5, "layer_correct": 4} }
   ```
   `by_confidence` uses the field names of `evals.ReliabilityBucket` (`confidence, runs, correct, accuracy`;
   `probability` omitted) so the eval report and the room agree; `correct` = graded `root_cause: yes` (a `partial`
   is not correct; the UI wording says "right"). Medians `-1` when no room has the value; `cost_available` false
   when every room is unpriced; `enough_data` = `graded ≥ 10` (the UI hides percentages under it). **Sources:**
   closed rooms of the pipeline in the window via `store.ClosedRoomReader.ListClosedRooms(pid, 500)` +
   `EventsOfTypes(room, [agent.hypothesis, room.closed, shadow.labelled, shadow.graded, room.labelled], 50)`
   (the reader exists for `similar_incidents`: `internal/store/pastrooms.go:16`); shadow rooms count in `shadow` and
   in the totals when graded. A live room counts as `labelled`/`graded` when F8's label event (`room.labelled`, with
   its `actual_layer` and a human grade of the hypothesis — whatever F8 names them) is in its log; until F8 merges
   only shadow grades exist and `labelled` is 0 for live rooms, which the UI shows as "no labelled rooms yet".
   `time_to_first_hypothesis_s` per room from the first `agent.hypothesis` (live) or `shadow_runs.first_hypothesis_at`
   (shadow). Cost per room from the spend store (`SpendStore` room totals) when present, else from `agent.usage`
   events (the events read is then `agent.usage` too). Cache per (pipeline, days) for 2 minutes under a mutex (the
   `similar_incidents` index does the same); bounded work: 500 rooms × 50 events.
3. **Room list**: `RoomSnapshot.insight` is in every `GET /api/rooms` row (no extra field work); api.md documents it.

### F11b (A6)
4. **Reducer**: `agent.usage` is folded into `state.insight.usage` (same rules as item 1; still no activity entry,
   still no step), `agent.step.started/tool_call/tool_result/hypothesis` update `steps`, `tool_calls`,
   `tool_errors`, `first_hypothesis_at`. `fromSnapshot` seeds nothing (full replay). Fixtures: add two `agent.usage`
   events to `web/src/fixtures/schema-change.json` (priced and unpriced) so the chip shows in fixture mode.
5. **Cost chip** in `RoomHeader.tsx` facts line (mono 12, tabular nums, after the room id): `$0.42 · 11 steps ·
   2m 10s`; duration = now − opened_at while open (`useNow`), closed_at − opened_at when closed (`durationLabel`
   from F2a if merged, else a local `elapsedLabel`); `$0.42` becomes `812k tokens` when `cost_usd` is null and
   `≥ $0.42` when `unpriced_calls > 0`; nothing before the first usage event (`0 steps · 12s` only). Tooltip
   (`ui/tooltip.tsx`, also `aria-describedby`): `9 model calls · 812,200 tokens (41k cache reads) · claude-opus-5-5 ×7,
   claude-haiku-5-5 ×2 · first hypothesis in 48 s · 1 tool error`. The chip is a `<button>` that opens nothing; it is
   information, so no `aria-live`. Hidden in shadow rooms (their usage is revealed after close).
6. **First-insight line**: `HypothesisCard` kicker (12/16 subtle, right of the path line from F9c1):
   `first hypothesis in 48 s` (`insight.time_to_first_hypothesis_s`); on the room list row, the second meta line
   gains `· insight 48 s` for open rooms with a hypothesis (one span in `RoomList.tsx`, F6 owns the row rewrite —
   coordinate: F11b adds only the span).
7. **Track record** next to the confidence badge (`HypothesisCard.tsx`): `useAgentStats(pipelineId)`
   (`hooks/useAgentStats.ts`, one GET per room mount, `useLoad` from #146; 403/404/network → `null`, nothing shown).
   Copy by state: `enough_data` and a bucket for the current confidence with `runs > 0` →
   `right {correct} of {runs} times at {confidence} on {pipeline} (90 d)`; `enough_data` but no rooms at this
   confidence → `no record at {confidence} yet on {pipeline}`; not enough data → `{graded} labelled rooms on
   {pipeline} · not enough data yet` (never a percentage under 10). Rendered as a `Badge variant="outline"` with a
   `title` holding the full table (`high 11/12 · medium 2/4 · low 0/1 · median first insight 48 s`). When `graded`
   is 0: `no labelled rooms yet` only on hover of the confidence badge (no extra chip: keep the card quiet until
   there is data).
8. **Describe**: unchanged — `agent.usage` stays out of the activity rail (cost.md §8).

## Out of scope
Budget warnings in the room (`log_cap`-style strip at 80 % of `PIPEROOM_ROOM_TOKEN_BUDGET` — F10/F1 attention
system); a pipeline page with the reliability table (the "Pipelines" sidebar item is still dead; F8/F6 decide);
labels on live rooms (F8); `GET /api/usage` day views; confidence downgrade when a cited step errored (F3).

## Contract (F0 carries these)

`docs/api.md` — RoomSnapshot gains:
```json
  "insight": { "first_hypothesis_at": null, "time_to_first_hypothesis_s": -1, "final_confidence": "",
               "steps": 0, "tool_calls": 0, "tool_errors": 0,
               "usage": { "calls": 0, "unpriced_calls": 0, "tokens": 0, "cost_usd": null, "latency_ms_total": 0 } }
  // F11: folded from the room log (agent.step.started, agent.tool_call/result, agent.hypothesis, agent.usage).
  // time_to_first_hypothesis_s is seconds from opened_at to the first agent.hypothesis, -1 before one exists;
  // cost_usd is null while no call is priced and a lower bound when unpriced_calls > 0; zeros in a shadow room.
```
REST table row:
```md
| GET | `/api/pipelines/{pid}/agent-stats?days=90` | – | `AgentStats` | F11 (server F11a, web F11b). The agent's track record on one pipeline over the last `days` (1–365, default 90) of **closed** rooms: counts, graded hit rate, reliability by confidence (`by_confidence[]` with the field names of `evals.ReliabilityBucket`), medians of time to first hypothesis, human MTTR and cost, plus the shadow subset. Visibility first (`404 not_found` for an unknown pipeline or one outside the caller's tenants), then any logged-in user, viewers included. `enough_data` is false under 10 graded rooms: clients show counts, never a rate. Cached 2 minutes per pipeline. Sources: `shadow.labelled/graded` and F8's room label event; before F8 only shadow rooms are graded |
```
Objects section: `AgentStats` exactly as the JSON in item 2. No Go contract file changes (`events.go` untouched;
`AgentUsagePayload` is read as is). C6: `evals/README.md` notes that `-calibration` buckets and `agent-stats`
share names; the eval report may later ingest `agent-stats` for a pilot's live/eval comparison (no code in F11).

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `internal/room/snapshot.go` (`Insight` fold) + `snapshot_insight_test.go` | A5 | modified/new |
| `internal/room/agentstats.go` + `agentstats_test.go` (memstore, fake clock) | A5 | new |
| `internal/httpapi/agentstats.go` + `agentstats_test.go`, `server.go` (route under `short`, `pipelineVisible`) | A5 | new/modified |
| `web/src/state/{types,reducer}.ts` (+ `reducer.insight.test.ts`), `fixtures/schema-change.json` | A6 | modified |
| `web/src/components/room/RoomHeader.tsx` (chip), `room/CostChip.tsx` (+ `.test.tsx`), `lib/insight.ts` (+ test: formatting) | A6 | new/modified |
| `web/src/components/room/HypothesisCard.tsx` (+ test), `hooks/useAgentStats.ts`, `api/{client,types}.ts`, `components/RoomList.tsx` (one span) | A6 | modified/new |
| `evals/README.md` (one paragraph) | C6 | modified |

## Dependencies and merge order
F0 (api.md) → F11a (one PR ≈ 500 lines) → F11b (one PR ≈ 450 lines; fixture-mode testable before F11a, the
track record hidden until the route answers). F11b rebases on F9c1 (`HypothesisCard`) and F2a (`durationLabel`).
F8 later adds `room.labelled` to the sources list in `agentstats.go` (A5, one case). F6's room-list rewrite keeps
the `insight` span.

## Design details
- **Why fold, not fetch**: the room replays its whole log (`after=0`), so the chip is exact, live and free; the
  snapshot carries the same fold so lists and aggregates never replay. The two folds share the rules in item 1
  (table test on both sides with the same fixture log).
- **Numbers only**: `agent.usage` carries no text; the chip and tooltip render model ids (from the event) and
  counts; model ids are rendered as text nodes. `agent-stats` carries no room titles or hypothesis text.
- **Small-N honesty**: percentages only with ≥ 10 graded rooms; `partial` grades are not "right"; the tooltip shows
  the raw counts so nobody reads 1/1 as 100 %.
- **Cost truth**: `cost_usd` null → tokens shown instead; a lower bound shows `≥`; never 0 for unknown. Shadow
  rooms show no chip until reveal (their numbers are in `shadow.revealed`).
- **Performance**: `agent-stats` reads ≤ 500 rooms × ≤ 50 typed events per pipeline, cached 2 min; the web calls
  it once per room mount. `insight` adds ~120 bytes to each list row.
- **a11y / theme / mobile**: chip is plain text with a tooltip and `aria-describedby`; tabular nums; tokens only;
  below 640 px the chip drops the duration (the header "Open · 12m" badge has it).
- **Failure modes**: spend store down → `agent-stats` cost medians from events (slower, same answer); reader
  missing (`ClosedRoomReader` not implemented) → `404 not_found` from the handler like the usage routes without
  spend tables; the web hides the record on any error.

## Done when
- [ ] Opening the demo room: with no price table the chip reads `812k tokens · 11 steps ·
      2m 10s`; with `PIPEROOM_MODEL_PRICES` set it reads `$0.42 · …`; the tooltip lists calls, tokens, models, first
      hypothesis seconds and tool errors; a closed room's duration is frozen.
- [ ] `GET /api/rooms` rows carry `insight.time_to_first_hypothesis_s` ≥ 0 for every room with a hypothesis and
      `-1` otherwise; a shadow room has zeros and no chip.
- [ ] `GET /api/pipelines/orders/agent-stats` answers the documented shape; `enough_data` false with 3 graded rooms
      (the card says `3 labelled rooms on orders · not enough data yet`); with 12 graded fixtures it says
      `right 11 of 12 times at high on orders (90 d)`; `?days=0` → 400; unknown pipeline → 404; viewer → 200.
- [ ] The hypothesis card shows `first hypothesis in 48 s`; the room list row shows `insight 48 s`.
- [ ] `go test ./...`, `go vet`, `npm run build && npm run lint && npm test` pass; no new dependency; no
      `agent.usage` line in the activity rail.

## Tests
- Go: `snapshot_insight_test.go` (table: each event type's effect; null vs lower-bound cost; shadow zeros; floor of
  seconds; a second hypothesis keeps `first_hypothesis_at`), `agentstats_test.go` (fake clock: window edge, medians
  with 0/1/2/odd rooms, `partial` not correct, `enough_data` at 9 vs 10, shadow subset, unpriced → `cost_available`
  false, cache hit within 2 min and miss after, reader error → wrapped error), `httpapi/agentstats_test.go` (200
  member, 200 viewer, 404 unknown, 404 tenant, 400 days, JSON names match `evals.ReliabilityBucket` via a
  reflection test on the struct tags).
- Vitest: `reducer.insight.test.ts` (same table as Go, from the fixture log), `insight.test.ts` (`$0.42`, `≥ $0.42`,
  `812k tokens`, `2m 10s`, `1 h 02 min`), `CostChip.test.tsx` (tooltip text, hidden for shadow, duration frozen when
  closed), `HypothesisCard.record.test.tsx` (three copy states, hidden on 403/404), `useAgentStats.test.tsx`
  (one fetch per mount, error → null).
- Evals: none (behaviour of the agent unchanged); C6's README line only.

## Effort
F11a **M** (≈ 500 lines, the aggregate test is half of it) · F11b **S–M** (≈ 450 lines).
