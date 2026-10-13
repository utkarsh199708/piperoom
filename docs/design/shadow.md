# Shadow mode — design rev 3 (main @ 8283a58, 2026-10-13)

Rev 2 was written against `0c3c243`. Since then main merged: signed alerts (#42), the agent cap (#43),
`propose_fix` JSON-text params (#45), retention + JCS export (#46), read scoping (#48), model providers +
token budget (#49), the watcher (#54/#56/#57), the jsonb guard (#58) and auth PRs 1–4 (sessions, OIDC,
roles, `owners.yaml` principals, `params_hash`, approval identity, export access). Every file and function
named below was read on `8283a58`; rev 2 line numbers are gone.

**What changed in rev 3 (short list).**
1. A room is shadow because its pipeline says so in `owners.yaml` (`shadow:` block, next to `watch:`), not
   because of an env list. The env keeps only the numbers: grace, token budget, max turns.
2. The lead's decisions are baked in: grace after close = `0` by default (`PIPEROOM_SHADOW_GRACE`); shadow
   data in the export only with `?include=shadow` and only for closed rooms; grader and labeller = a member of
   the room's pipeline (viewers never), actor + identity recorded; denominator = every closed shadow room,
   errors included.
3. Roles are real now (auth PR 3): viewers are blocked by `viewerGuard` on every non-GET; the service adds the
   "member of this pipeline" check for label and grade.
4. The token budget exists (#49): shadow uses it through one new `Options.TokenBudget` field and reads the
   count back through one new `Deps.Usage` callback. No prompt or tool change.
5. One routing rule replaces rev 2's `statusSink`: in a shadow room every event whose actor is
   `events.AgentActor` goes to `shadow_events`, decided in `emitLocked`. That covers all four status paths.
6. The `shadow.revealed` event carries numbers only, never the hypothesis text, so the WebSocket cannot
   un-blind a labeller. Hypothesis and log come from `GET /shadow` after the blind label.
7. The "agent_status shadow" marker is dropped: `room.created.shadow` and `RoomSnapshot.shadow` already tell
   the UI. Fewer states.
8. No open questions remain. §8 lists the decisions and what was verified in code.

## Changes after review rev 3

The skeptical review (`shadow-review-rev3.md`) approved with four must-fixes and six nice-to-haves. All ten are
applied; none is rejected. Where each one landed:

| # | Item | What changed | Where |
|---|---|---|---|
| M1 | `demo.Run` calls `deps.Proposals.Create` before the inbox (`internal/room/demo/demo.go`), so `Proposals == nil` panics → 3 retries → `error` | `demo.Run` gets a nil guard: with `Proposals == nil` it skips the proposal and the `waiting_approval` status, emits `idle`, and goes to the wait loop (which ends at once on the closed inbox). Small, in PR 1b (A5). The `--demo` live check in PR 3 stays | 3.7, 7 |
| M2 | `cmd/investigate` exits 1 on any `ep.Run` error (`cmd/investigate/main.go`), so a spent budget would be a failed CLI run after PR 2 | PR 2 maps a wrapped `*BudgetError` to a printed line and exit 0 in `cmd/investigate`; every caller of `Endpoint.Run` is listed in 3.3 | 3.3, 7 |
| M3 | `canGradeShadow` rule (a) "in `users.yaml`" let every `users.yaml` user grade every pipeline, also in oidc mode (`RoleFor`, `access.go`) | Rule is now `authorizeLayer(pipelineID, user, layer, "", false)` over `events.Layers`, exactly `CheckExportAccess` minus the auditor shortcut. The 403 text and the api.md row now match the code | 3.5, 5.4 |
| M4 | With grace > 0, close calls `Forget`; a retried attempt gets a fresh budget at 0 (`endpoint.go` `budget`), so a shadow room could spend the budget twice and `Usage` under-reports | No retry once the room is closed: a run error after close → `interrupted`, one reveal | 3.3, 3.4, 6 |
| N1 | `Endpoint.budget(roomID)` reuses an existing entry; a hand-off written while the run is queued leaves the room at `e.limit`, not `Options.TokenBudget` | `Endpoint.Run` with `TokenBudget > 0` sets the limit on the room's entry under its mutex (new or existing); `used` is kept. Stated in 3.3; PR 2 scope | 3.3, 7 |
| N2 | `Deps.Usage` is not called on a panic (recover lives in `runGuarded`); tokens read 0 | `Endpoint.Run` calls `Usage` from a `defer`, so it runs on nil, error and panic alike; the panic still propagates | 3.3, 5.1, 6 |
| N3 | What if `CreateShadowRun` fails at open | `CreateShadowRun` is idempotent (`ON CONFLICT DO NOTHING`); the supervisor calls it again before `gate.acquire`, and so does the `Load` sweep; `CloseRoom` upserts an `error` row when none exists, so the room still reveals and still counts | 3.7 |
| N4 | Who may pass `?include=data` on `GET /shadow` | Any logged-in user, viewers included, after the label; the same rule as `tool_result.data` in a live room's `GET /events` today. Written down | 3.6, 5.4 |
| N5 | PR 1 too large; PR 3/4 need only PR 0 | PR 1 split into 1a (A3 owners, A5 store + migration + memstore) and 1b (room + httpapi + `cmd/server` wiring + demo guard); PR 3 and PR 4 start on fixtures after PR 0 and finish after 1b | 7 |
| N6 | `AgentStatusPayload` `queued` comment | Comment-only, kept as is (`queued` is already emitted by `emitQueued` and listed in api.md) | 5.2 |

## 1. Problem and who asked

Pilots need proof before they trust the agent. The GTM plan names shadow mode as the deck metric: the agent
investigates every real alert silently, the answer is revealed after the human closes, and we log
layer-correct rate, time-to-hypothesis vs MTTR and cost ("14 of 16 matched, median 4 min vs 52",
`reviews/gtm-plan-2026-10-13.md:24-25,35`). P1 and P3 allow only a sandbox or a design-partner track until
trust and audit exist; P2 wants a staging pilot with a cost guard (`reviews/product-feedback-2026-10-13.md:7-9`).
The moat is labelled real CDC incidents (`gtm-plan:20`); graded shadow runs are how we get them.

Auth PRs 1–4 removed the F1 blocker that rev 2 carried as risk 1: labels and grades are now tied to a session
identity (`events.Identity`, `docs/design/auth.md` §3.2), and viewers cannot write.

## 2. Goals / non-goals

Goals: (a) run the agent on real alerts with read tools only; (b) hide every agent event until the room is
closed; (c) take blind labels, then show hypothesis vs resolution and take a grade; (d) report "N of M
matched, median X vs Y min, tokens"; (e) turn graded runs into eval scenarios.

Non-goals: no proposals, no actions, no steering of a shadow run, no Slack mirror of shadow output, no change
to live rooms, no new login or role (the member/viewer split from auth PR 3 is enough), no per-pipeline budget
overrides (env only).

## 3. Design

### 3.1 How a room becomes shadow

**Config.** A pipeline opts in with a `shadow:` block in `owners.yaml`, shaped like `watch:`:

```yaml
pipelines:
  - id: orders
    # ...existing fields...
    watch: { ... }
    shadow:
      enabled: true          # absent block or enabled: false = live agent, as today
```

`owners.Load` validates it the way `WatchSpec.UnmarshalYAML` does (unknown key = error, `owners/watch.go`
`checkKeys`). `Pipeline.Shadow() bool` reads it. A pipeline that is not in `owners.yaml` cannot be shadow, so
rev 2's "unknown pipeline under `*`" case no longer exists: a shadow room always has a known pipeline, and
the denominator rule ("closed shadow rooms with a known pipeline") is true by construction.

Why YAML and not env: it sits next to `watch:`, it is per pipeline, it is validated at start, and a pilot
edits one file. Why no global env switch: one knob is enough; rollback is "remove the block, restart"
(open shadow rooms keep their flag from the log, see 3.7). The env holds only numbers (§5.3).

**Resolution.** `openAlertRoom` (`internal/room/service_room.go`) computes
`shadow := pipelineKnown && pipeline.Shadow()` once and freezes it in `room.created.shadow` (new payload
field). `Apply` folds it into `RoomSnapshot.Shadow`, so `rebuild` restores it after a restart without
re-reading `owners.yaml`. Every alert route goes through `openAlertRoom` (dev webhook, the five signed
sources, the watcher), so all of them make shadow rooms for a shadow pipeline. `POST /api/rooms`
(`CreateRoom`) never makes a shadow room: a person who opens a room wants the agent.

**Watcher.** Nothing in `internal/watch` changes. In mode `rooms` the watcher opens through
`OpenAlertRoom` with `Source: watcher`, so its rooms are shadow when the pipeline is. Joins, "still firing"
notes, the resolved note and the cooldown (`PostRoomNote`, `LastRoomForKey`, `OnRoomClosed`) behave as
today; none of them touches the agent. The watcher's `MaxOpenRooms` is `PIPEROOM_MAX_AGENTS`, and a shadow
run takes one of those agent slots while it runs, so the watcher cannot queue more shadow runs than live
runs. In mode `log` (the default) the watcher opens nothing, so shadow needs `PIPEROOM_WATCH_MODE=rooms`
or an external alert source. `room.created.alert_source` is kept in the metrics `per_room` rows so a pilot
can split watcher-opened from Alertmanager-opened rooms.

### 3.2 Where shadow output goes

`room_events` is read by the WS gateway, the Slack bridge, the export, `rebuild` and the web reducer.
Shadow output must reach none of them before the reveal, so it goes to two new tables (migration `00009`,
number confirmed by the lead at merge; `migrations/00008_...` is the last one today):

```sql
CREATE TABLE shadow_runs (
    room_id              text PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
    status               text        NOT NULL,          -- queued|running|hypothesised|no_hypothesis|error|budget|interrupted
    last_seq             bigint      NOT NULL DEFAULT 0,
    model                text        NOT NULL DEFAULT '',
    pipeline_map         text        NOT NULL DEFAULT '',
    started_at           timestamptz,                   -- slot acquired; NULL while queued
    first_hypothesis_at  timestamptz,
    final_hypothesis_at  timestamptz,
    finished_at          timestamptz,
    finished_after_close boolean     NOT NULL DEFAULT false,
    final_hypothesis     jsonb,                         -- events.AgentHypothesisPayload
    tool_calls           integer     NOT NULL DEFAULT 0,
    tokens               bigint      NOT NULL DEFAULT 0,
    revealed_at          timestamptz
);
CREATE TABLE shadow_events (
    room_id text        NOT NULL REFERENCES shadow_runs(room_id) ON DELETE CASCADE,
    seq     bigint      NOT NULL,
    type    text        NOT NULL,
    actor   jsonb       NOT NULL,
    payload jsonb       NOT NULL,
    at      timestamptz NOT NULL,
    PRIMARY KEY (room_id, seq)
);
```

`seq` comes from `UPDATE shadow_runs SET last_seq = last_seq + 1 WHERE room_id = $1 RETURNING last_seq`
in the same transaction as the insert, the same pattern as `PG.Append` (`internal/store/store.go`,
`UPDATE rooms SET last_seq = last_seq + 1`). The insert runs `store.CheckAppend` first, so the jsonb guard
(#58) covers shadow rows too. `memstore` implements the same interface in memory.

**The routing rule.** In a shadow room, every event whose actor is `events.AgentActor` is appended to
`shadow_events` instead of `room_events`; it is not folded into the snapshot and not fed to the inbox. The
decision is made in one place, `emitLocked` (`internal/room/service.go`):

```go
func (s *Service) emitLocked(ctx, st, roomID, typ, actor, payload) (events.Event, error) {
    if st.snap.Shadow && actor == events.AgentActor {
        return s.appendShadowLocked(ctx, st, roomID, typ, payload)   // returns an Event with RoomID/Seq/At; never applied
    }
    // ...today's body...
}
```

Everything that writes agent status goes through `emitLocked` with `events.AgentActor`: the agent Sink
(`agentEmit`), the retry `error` status in `startAgentLocked`, `emitQueued` and `emitSlotAcquired`
(`agentslots.go`). So no agent text, status or error reaches watchers. `noteRestartLocked` is skipped for
shadow rooms (its text says "steer to resume", which is wrong there). System-actor events (the "pipeline
unknown" note, watcher evidence, the restart note for live rooms) are unchanged. `sanitizeAgentPayload`
still runs in `agentEmit` before routing.

`appendShadowLocked` also keeps `shadow_runs` current: on `agent.hypothesis` it sets
`first_hypothesis_at` (once), `final_hypothesis_at` and `final_hypothesis`; `tool_calls` is counted from
`shadow_events` when the run finishes (one `SELECT count(*)`), so a crash loses nothing.

### 3.3 The shadow run

`startAgentLocked` gets one branch on `st.snap.Shadow`:

```go
deps := agent.Deps{Tools: s.tools, Actions: s.actions, Inbox: gatedInbox{...}, Sink: roomSink{...}, Proposals: proposalCreator{...}, Logger: s.log}
opts := s.opts
if st.snap.Shadow {
    deps.Actions, deps.Proposals = nil, nil           // propose_fix lists "(none)" and refuses
    deps.Usage = func(used int64) { s.noteShadowTokens(roomID, used) }
    opts = s.shadowOpts                               // MaxTurns 20, TokenBudget 500k by default
}
```

- **Tools.** The same registry, scoped by #48: `runner.toolScope` wraps every read tool call with
  `tools.WithPipelineID(ctx, alert.PipelineID)` (`internal/agent/run.go`), and the pipeline id comes from
  the alert, never from the model. Shadow does not touch this.
- **No writes.** `propose_fix` stays registered (the graded agent is the agent the pilot buys; the wiring
  tests in `tools_room.go` and `cmd/investigate/wire_test.go` are untouched). With `Actions == nil` its
  description lists `(none)` (`tools_room.go` `Description`), and `Run` fails with "proposals are not
  available" when `Proposals == nil`. No proposal means no approval and no executor; `st.exec` stays nil.
- **One shot, no steering.** `st.inbox` is created closed for a shadow room (`newClosedInbox()`): `Drain`
  returns nothing, `Wait` returns `agent.ErrInboxClosed`. `feedInbox` pushes nothing for a shadow room. The
  run therefore ends at the first `end_turn` (`pauseForHumanInput` → `waitForInput` → `Wait` →
  `ErrInboxClosed` → `stopErr` maps it to nil) or at `MaxTurns` (same path after the "max turns" status).
  `gatedInbox.Wait` still releases the slot first, so the slot is free the moment the run ends.
  `ensureAgentLocked` returns at once when `st.snap.Shadow`, so a steer, a suggestion send or an `@agent`
  comment never starts a run, before or after a restart. Those human events are still written to the room
  log (they are part of the human's incident record); the UI hides the steer box in a shadow room.
- **Budget (#49).** `Options.TokenBudget` (new, §5.1) is honoured by `Endpoint.Run`: with a value > 0 it
  sets that limit on the room's `tokenBudget` entry, creating the entry when it is missing and keeping
  `used` when it exists (`limit` moves under `b.mu`, so `admit` reads it locked). Why not "first creator
  wins": `WriteHandoff` also calls `e.budget(roomID)` (`endpoint.go`), so a hand-off written while the
  shadow run is still queued would otherwise pin the room to `e.limit` (`PIPEROOM_ROOM_TOKEN_BUDGET`).
  A hand-off call in flight at that moment keeps counting into the same entry, so nothing is lost. With
  `TokenBudget == 0` the entry uses `e.limit`, as today. Shadow default 500 000 against the live default
  2 000 000. The budget is checked before every loop call (`budgetModel.Turn`), exactly as for live
  rooms. The queue is the same `PIPEROOM_MAX_AGENTS` pool (`agentSlots`).
- **Tokens read back.** `Deps.Usage` (new, §5.1) is called by `Endpoint.Run` exactly once, from a
  `defer` placed right after the budget entry is taken, with `budget.Used()` for the room. The `defer`
  means it runs on a nil return, an error return and a panic alike (the panic still propagates to
  `runGuarded`, which turns it into an error), so a crashed run still reports its tokens. It is the
  run's own pointer, so the `Forget(roomID)` that the room-closed hook calls (`cmd/server/watch.go`
  `roomClosedHook.onClosed`) cannot race it. `agent.Run` (no endpoint, used by the evals) and `demo.Run`
  never call it; `tokens` stays 0 and the metrics say `tokens_available: false`.
- **Budget stop is typed.** Today `runner.run` calls `stopForBudget` (`triage.go`) and returns nil, so a
  supervisor cannot tell a budget stop from a normal end. Rev 3: `Run` returns the `*agent.BudgetError`
  (wrapped) after `stopForBudget`. The live supervisor treats it like nil (no retry, no extra `error`
  status: the runner already wrote the idle status and the message). The shadow supervisor maps it to
  status `budget`. The supervisor guard lands in PR 1b, before PR 2 changes the return, so the order of
  merges cannot produce three retries on a spent budget (§7). **Every caller of `Endpoint.Run` must
  handle the new return.** On main they are: the room service (`startAgentLocked`, guarded in PR 1b),
  `cmd/investigate` (`cmd/investigate/main.go` returns any `ep.Run` error as `investigate: …` and exits
  1), and three tests in `internal/agent`. PR 2 therefore also changes `cmd/investigate`: a wrapped
  `*BudgetError` prints its text (the runner already wrote the message to the terminal sink) and exits
  0, because a spent budget is a stop, not a failure. The evals use the plain `agent.Run`, which has no
  budget and never returns it.
- **Status.** `shadow_runs` is created at room open with `queued` (idempotent, see 3.7); `running` +
  `started_at` when `gate.acquire` returns; `model` and `pipeline_map` are written right after the
  topology render in the supervisor. When `runGuarded` returns: nil and ctx alive → `hypothesised` if
  `final_hypothesis` is set, else `no_hypothesis`; `*BudgetError` → `budget`, no retry; ctx cancelled →
  `interrupted`; any other error **while the room is still open** → the usual 3 attempts with backoff
  (the `error` statuses land in `shadow_events`), then `error`; any other error **after the room is
  closed** (possible only with grace > 0) → `interrupted`, no retry. The last rule exists because
  `CloseRoom` has already run the room-closed hook, so `Forget` dropped the room's budget entry; a
  retried attempt would get a fresh entry at 0 and could spend the budget a second time while `Usage`
  reports only the last attempt. `finished_at` is set once, with `tokens` and `tool_calls`.

### 3.4 Close, grace, reveal

The human works the room as today (comments, control, trace, hand-off; the hand-off summary is written
from the human log, since the room log holds no agent events) and closes it.

- **Resolution required.** `CloseRoom` on a shadow room refuses an empty or whitespace resolution with
  `400 validation` ("a shadow room needs a resolution to grade against"). Live rooms keep today's rule.
- **Grace.** `PIPEROOM_SHADOW_GRACE` (Go duration, default `0`). With `0`, `CloseRoom` cancels the run as
  it does today (`st.agentCancel`). With a positive value it does not cancel; it arms
  `time.AfterFunc(grace, cancel)` (dropped when the run ends first). The run still ends on its own at the
  first `end_turn`; grace only lets a run that is mid-investigation finish. It holds its agent slot during
  that time. `finished_after_close` is `finished_at > closed_at`. The room-closed hook runs at close as
  today (watcher ack, `Forget`), so the watcher's cooldown starts at the close, not at the run end. A run
  that fails during grace is not retried (3.3): it is `interrupted` and revealed as such.
- **Reveal, write-once.** One visible event `shadow.revealed` (system actor) is appended to the room log
  by whoever is second: the supervisor if the room is already closed when the run ends, else `CloseRoom`.
  `ShadowStore.MarkRevealed(roomID, at)` returns `first == false` when `revealed_at` is already set, and
  the loser writes nothing. The payload carries status, the three times, `finished_after_close`,
  `tool_calls` and `tokens` — **not** the hypothesis (see 3.5). `Load` sweeps closed-but-unrevealed runs
  (crash fallback).
- **After `room.closed`.** The log may now hold a few events after `room.closed`: `shadow.revealed`,
  `shadow.labelled`, `shadow.graded` (one per re-grade). Checked: `Apply`, `rebuild`, the export `fold`/
  `finish`, `LastRoomForKey` and retention do not assume `room.closed` is last. The Slack bridge stops
  its subscription at `room.closed` (`slackbridge/bridge.go`), so shadow results are never mirrored, which
  is the non-goal. The web reducer ignores unknown types (`reducer.ts` `default:`) until PR 3 adds them.

### 3.5 Blind labels, grade, roles

Order is enforced by the server: resolution (at close) → blind label → reveal visible → grade.

- `POST /api/rooms/{id}/shadow/blind {actual_layer, root_cause_terms}` → `shadow.labelled` (human
  actor; payload also carries `identity`). Allowed once; a second call → `409 already_decided`.
  `actual_layer` must be one of `events.Layers`; `root_cause_terms` 1–20 strings, each 1–200 characters,
  all through `rejectUnstorable`.
- `GET /api/rooms/{id}/shadow` answers `409 not_labelled` until the label exists. The `shadow.revealed`
  event has no hypothesis text, so neither the WebSocket nor `GET /events` can show the answer early.
- `POST /api/rooms/{id}/shadow/grade {layer_correct, root_cause, notes}` → `shadow.graded`
  (`root_cause` ∈ `yes|partial|no`; `notes` ≤ 2000 characters). Re-grade allowed; the last by `seq`
  wins; every event is kept.
- **Who may label and grade.** The viewer half is already done: `viewerGuard` (`httpapi/auth.go`) answers
  `403 viewer` to every non-GET for a viewer, and an `auditors:`-only principal is a viewer. The service
  adds the pipeline half, `canGradeShadow(pipelineID, user) (grant string, ok bool)`: it loops over
  `events.Layers` and returns the first `authorizeLayer(pipelineID, user, layer, "", false)` that
  succeeds (`internal/room/access.go`), which is exactly what `CheckExportAccess` does, minus the
  auditor shortcut. So a grader is: the solo user; an approver or backup for any layer of the room's
  pipeline in `owners.yaml` (user or group principal, pipeline `owners:` block or default layer owners);
  or, in dev mode, a `users.yaml` user whose `layers:` list holds a layer that the pipeline's `owners:`
  block does not replace. A `users.yaml` user with no layers, a group-only member of another pipeline
  and a viewer are refused with `403 not_allowed` ("labelling or grading a shadow room needs a member
  of its pipeline"). Being in `users.yaml` alone is **not** enough: `RoleFor` makes such a person a
  member of the server, not of the pipeline, and in oidc mode that would let anyone in the file grade
  every pipeline. The grant string that qualified them (`user:…`, `group:…`, `backup:…`, `solo`) goes
  into `identity.grant`, the same strings approvals use. The check runs on the pipeline id frozen in
  `room.created`, so it works after a restart without re-reading the room.
- **Identity.** The HTTP layer already fills `User.Identity` from the session (`httpapi/auth.go`); the
  service copies it into the payload with the grant, exactly as `Approve` does for `approval.given`. A
  dev-mode `X-User` request records `method: "dev"` and no session id, as approvals do.
- Any shadow route on a non-shadow room → `404 not_found`; on an open room → `409 room_open`.

### 3.6 Reading shadow data

- `GET /api/rooms/{id}/shadow` → `{run, events}`. `run` is the `shadow_runs` row (JSON names in §5.4);
  `events` are the shadow rows as `Event` objects plus `"shadow": true`, with their own `seq`, never fed to
  the room reducer. `?include=data` adds `tool_result.data` (F3, `docs/security/findings.md` §F3);
  default is `summary` only. `?include=room` adds `room: {created, labelled, graded}` for the eval
  builder (3.8). Values combine: `?include=data,room`. GET, so a viewer may read it after the label.
  `?include=data` has no extra access rule: any logged-in user may pass it, viewers included, because a
  viewer already sees `tool_result.data` in a live room's `GET /events` today, and a shadow run's tool
  output is no more sensitive than a live run's. The stricter rule (approvers and auditors) applies only
  to the export below, as it does for live rooms.
- `GET /api/rooms/{id}/export?include=shadow` (closed rooms only, else `409 room_open`; before the
  label `409 not_labelled`) adds one top-level member `"shadow": {run, events}` to the document, written
  after `"schema"` (keys stay sorted: `schema` < `shadow`), with full `data`. It is inside the hashed
  document, so the header hash changes with the flag. Without the flag the export is byte-identical to
  today, so `ExportSchema` stays `piperoom.room-export/v1`. Access is the export rule from auth PR 4
  (`CheckExportAccess`: approvers, backups, auditors).
- `GET /api/shadow/metrics?pipeline=&since=` → counts and medians over closed shadow rooms, joined with
  `rooms` on `closed_at` (#46's partial index). `since` is RFC 3339 and filters `closed_at >= since`.
  Fields are in §5.4. Medians are `-1` when no room has the value; arrays are `[]`.

**Clocks.** Every agent time counts from `rooms.opened_at`, the same clock as human MTTR
(`closed_at - opened_at`, including time with no driver); `started_at` is stored so queue time is visible.
Both `time_to_first_hypothesis_s` and `time_to_final_hypothesis_s` are reported; the grade judges the
final hypothesis.

**Denominator (lead decision).** `rooms` = every closed shadow room in the filter, whatever its status.
`layer_correct` counts graded rooms with `layer_correct: true`; an ungraded or `error`/`budget`/
`interrupted`/`no_hypothesis` room counts as not matched. "N of M matched" = `layer_correct` of `rooms`.
Honest, lower.

### 3.7 Restart, retention, failures

- **Restart.** `Load` starts no agents (today's rule). For every shadow room it loads: a run still
  `queued`/`running` becomes `interrupted` with `finished_at = now` and `tokens` kept as written (0 unless
  a hypothesis update stored one); a closed room whose run is finished but not revealed gets its reveal.
  Open shadow rooms keep `Shadow` from the log, so a pipeline whose `shadow:` block was removed still
  reveals.
- **Retention (#46).** `purgeRoom` (`internal/store/retention.go`) deletes `shadow_events` and
  `shadow_runs` explicitly and counts them (`PurgeResult.ShadowEvents`, `ShadowRuns`); the `ON DELETE
  CASCADE` keys are the backstop. `evictRooms` needs nothing new.
- **Store without shadow.** `ShadowStore` is a separate interface like `store.Purger`. `Load` returns an
  error when any pipeline is shadow and the store does not implement it (real mode refuses to start;
  both `*PG` and `memstore` implement it, so this only guards a future store).
- **`CreateShadowRun` fails at open.** The room row and `room.created` are already written, so the room
  exists, is shadow (the flag is in the snapshot) and has no run row; every `AppendShadow` would then
  fail on the foreign key. Rules: `CreateShadowRun` is idempotent (`INSERT … ON CONFLICT (room_id) DO
  NOTHING`; `memstore` the same), and it is called from three places: `openAlertRoom` (first try, a
  failure is logged at error and the open still succeeds, so the alert is not lost), the shadow
  supervisor before `gate.acquire` (second try; if it still fails the supervisor logs, writes nothing,
  and ends without a run, so no tokens are spent on a run nobody can store), and the `Load` sweep for
  every open shadow room without a row. `CloseRoom` on a shadow room with no run row upserts one with
  status `error` and `finished_at = closed_at` before it reveals, so such a room still gets its
  `shadow.revealed` and still counts in the metrics denominator as an error. Nothing leaks in any of
  these paths: routing is decided by `st.snap.Shadow`, not by the run row.
- **`AppendShadow` fails** → logged at warn, the run continues (same as a failed `Emit` today). **Store
  down at close** → the close succeeds (today's behaviour: `CloseRoom` only warns on a failed persist);
  the reveal is retried by the `Load` sweep. **Reveal append fails** → logged; `revealed_at` is set only
  after the append succeeded, so the sweep retries it.
- **Demo.** `--demo` loads `OWNERS_FILE` like real mode, so a copy of `dev/owners.yaml` with a `shadow:`
  block gives A6 shadow rooms on the in-memory store with the scripted `demo.Run`. Today `demo.Run`
  calls `deps.Proposals.Create` after its hypothesis and before it touches the inbox
  (`internal/room/demo/demo.go`); with `Proposals == nil` that is a nil-interface call, which
  `runGuarded` would turn into an error and retry three times, ending in status `error`. PR 1b adds a
  guard to `demo.Run`: when `deps.Proposals == nil` it skips the proposal and the `waiting_approval`
  status, emits `agent.status idle` ("no proposals in this room") and goes straight to the wait loop,
  where the closed inbox ends the run with nil. So a demo shadow run ends `hypothesised` after its five
  steps, and `tokens` is 0 there (`tokens_available: false`). Live demo rooms are unchanged: their
  `Proposals` is set.

### 3.8 Evals

`evalrun -from-shadow run.json -out scenario.yaml`, where `run.json` is `GET …/shadow?include=data,room`.
It pairs `agent.tool_call`/`agent.tool_result` into `recordings[]` (`why` stripped with
`toolparams.stripWhy`; `data` dropped), takes `pipeline` from `owners.yaml` by `room.created.pipeline_id`,
`resources.connectors/subjects` from that pipeline, `alert` from `room.created`, `expect.first_failing_layer`
from `actual_layer`, `root_cause_terms` as one OR group, `max_tool_calls` = observed + 5, and no `inputs`
(the shadow run had none). The scenario README says recordings cover only the path this run took.
`evalrun -check` must pass on the output.

## 4. Safety and security

- **No writes, by construction.** `Actions == nil` and `Proposals == nil` in the run's `Deps`; nothing
  else in the agent can reach `actions.Registry` or `CreateProposal`. The executor is never created for a
  shadow room. Read tools stay scoped (#48) and read-only (`tools.KindRead`, `permissions.md` role).
- **No leaks before the reveal.** Agent-actor events never enter `room_events` in a shadow room
  (routing rule, one choke point, leak test in §6). `shadow.revealed` holds numbers only. `GET /shadow`
  and `?include=shadow` answer `409 not_labelled` until the blind label. Default `GET /shadow` omits
  `data` (F3); the export includes it only under the export access rule.
- **Blind grading.** Resolution at close and the label are both written before anyone can read the run;
  the server enforces the order and refuses a second label.
- **Roles.** Viewers: GET only (`viewerGuard`). Label/grade: member of the room's pipeline (3.5). Export:
  auth PR 4 rule. Metrics: any logged-in user, GET.
- **Cost.** `PIPEROOM_SHADOW_TOKEN_BUDGET` (500 000) and `PIPEROOM_SHADOW_MAX_TURNS` (20) against live
  2 000 000 / 40. Same `PIPEROOM_MAX_AGENTS` pool. The run ends at the first `end_turn`, so a shadow run
  is one episode, never a parked room. Hand-off calls made during a shadow run count in `tokens`
  (same room budget); after the run they do not.
- **Rollback.** Remove the `shadow:` block and restart: new rooms are live; open shadow rooms keep their
  flag and still reveal. Migration down drops both tables.

## 5. Contract changes for the lead

Apply verbatim. Everything else in the design is implementation (A4, A5, A3, A6, C6) and is listed in
5.5 for review only.

### 5.1 `internal/agent/contract.go`

```diff
 // Deps are the agent's collaborators.
 type Deps struct {
 	Tools     *tools.Registry   // read-only tools from A2 and A3
 	Actions   *actions.Registry // write actions the agent may PROPOSE (never run)
 	Inbox     Inbox
 	Sink      Sink
 	Proposals Proposals
 	Logger    *slog.Logger
+	// Usage, when set, is called exactly once when Run ends, however it ends
+	// (nil, error or panic), with the model tokens the room has used in this
+	// process (input + output, cache included; the count behind
+	// PIPEROOM_ROOM_TOKEN_BUDGET). Only an Endpoint run counts tokens; the
+	// plain Run and the demo runner never call it. Shadow mode records the
+	// value. Nil = not wanted.
+	Usage func(used int64)
 }

 // Options tune the model call.
 type Options struct {
 	Model    string // default "claude-opus-5-5"
 	Effort   string // default "high"
 	MaxTurns int    // safety stop; default 40
+	// TokenBudget caps model tokens for this run's room. 0 = the Endpoint's
+	// default (PIPEROOM_ROOM_TOKEN_BUDGET). A value > 0 becomes the room's
+	// limit when Run starts, even if the room's count already exists (a
+	// hand-off may have created it); the count so far is kept. Shadow rooms
+	// set it lower.
+	TokenBudget int64
 }

 // Runner is implemented by A4 in run.go:
 //
 //	func Run(ctx context.Context, alert Alert, deps Deps, opts Options) error
 //
 // Run investigates until ctx ends or the room is closed. When the model
 // finishes a turn it emits AgentStatus "idle" and calls Inbox.Wait.
+// An Inbox whose Wait returns ErrInboxClosed ends the run cleanly (nil) after
+// the current turn. When the room's token budget stops the run, Run returns an
+// error wrapping *BudgetError after it has emitted the idle status and the
+// message; a caller must treat it as a stop, not a failure (no retry, no
+// non-zero exit).
 type Runner func(ctx context.Context, alert Alert, deps Deps, opts Options) error
```

### 5.2 `internal/events/events.go`

```diff
 	TracePinned Type = "trace.pinned"
+
+	// Shadow mode (docs/design/shadow.md). All three are written only to a
+	// closed shadow room, after room.closed.
+	ShadowRevealed Type = "shadow.revealed"
+	ShadowLabelled Type = "shadow.labelled"
+	ShadowGraded   Type = "shadow.graded"
 )
```

```diff
 	// DedupKey is the exact index key the room was opened under, so a restart
 	// rebuilds the same alert index instead of re-deriving it from Alert.
 	DedupKey string `json:"dedup_key,omitempty"`
+	// Shadow is true when the room's pipeline has shadow enabled in owners.yaml
+	// at open time: the agent runs silently, proposes nothing, and its output
+	// is revealed after the room closes. Frozen here so a restart keeps it.
+	// Never set by CreateRoom (POST /api/rooms).
+	Shadow bool `json:"shadow,omitempty"`
 }
```

```diff
 // AgentStatusPayload reports the agent's state.
 type AgentStatusPayload struct {
-	State  string `json:"state"` // "working" | "idle" | "waiting_approval" | "error" | "done"
+	State  string `json:"state"` // "working" | "idle" | "waiting_approval" | "error" | "done" | "queued"
 	Detail string `json:"detail,omitempty"`
 }
```

```diff
 // TracePinnedPayload pins a record trace to the room.
 type TracePinnedPayload struct {
 	Table  string       `json:"table"`
 	Key    string       `json:"key"`
 	Stages []TraceStage `json:"stages"`
 }
+
+// ---- Shadow mode ----
+
+// ShadowRevealedPayload says a shadow run's result exists. Written once per
+// room, by the system actor, when both the room is closed and the run has
+// ended. It carries numbers only: the hypothesis itself is read from
+// GET /api/rooms/{id}/shadow after the blind label, so this event cannot
+// un-blind a labeller.
+type ShadowRevealedPayload struct {
+	Status string `json:"status"` // "hypothesised" | "no_hypothesis" | "error" | "budget" | "interrupted"
+	// Seconds from rooms.opened_at; -1 when there was no hypothesis.
+	TimeToFirstHypothesisS int64 `json:"time_to_first_hypothesis_s"`
+	TimeToFinalHypothesisS int64 `json:"time_to_final_hypothesis_s"`
+	// HumanMTTRS is closed_at - opened_at in seconds.
+	HumanMTTRS         int64 `json:"human_mttr_s"`
+	FinishedAfterClose bool  `json:"finished_after_close"`
+	ToolCalls          int   `json:"tool_calls"`
+	Tokens             int64 `json:"tokens"` // 0 when the runner counts no tokens (demo, plain Run)
+}
+
+// ShadowLabelledPayload is the blind label a pipeline member gives after the
+// room is closed and before they may read the run. One per room.
+type ShadowLabelledPayload struct {
+	ActualLayer    string    `json:"actual_layer"`     // one of Layers
+	RootCauseTerms []string  `json:"root_cause_terms"` // 1..20, each 1..200 chars
+	Identity       *Identity `json:"identity,omitempty"`
+}
+
+// ShadowGradedPayload is a pipeline member's grade of the revealed run against
+// the resolution and the label. Re-grading appends another; the last wins.
+type ShadowGradedPayload struct {
+	LayerCorrect bool      `json:"layer_correct"`
+	RootCause    string    `json:"root_cause"` // "yes" | "partial" | "no"
+	Notes        string    `json:"notes,omitempty"`
+	Identity     *Identity `json:"identity,omitempty"`
+}
```

### 5.3 `internal/config/config.go`

```diff
 	// RetentionDays deletes whole closed rooms (events, proposals, approvals,
 	// hand-offs) whose closed_at is older than this many days. Open rooms are
 	// never deleted. Empty or "0" = keep forever.
 	RetentionDays string // PIPEROOM_RETENTION_DAYS
+
+	// Shadow mode (docs/design/shadow.md). Which pipelines are shadow is set
+	// per pipeline in owners.yaml (shadow: block); these are the numbers.
+	// Kept as strings like the watcher fields; ParseShadow types them and
+	// names the env var on error.
+	ShadowGrace       string // PIPEROOM_SHADOW_GRACE: Go duration the run may keep going after close; default 0 = cancel at close
+	ShadowTokenBudget string // PIPEROOM_SHADOW_TOKEN_BUDGET: tokens per shadow room; default 500000; "0" = use PIPEROOM_ROOM_TOKEN_BUDGET (there is no uncapped shadow run)
+	ShadowMaxTurns    string // PIPEROOM_SHADOW_MAX_TURNS: model turns per shadow run; default 20
```

```diff
 		RetentionDays: get("PIPEROOM_RETENTION_DAYS", ""),
+
+		ShadowGrace:       get("PIPEROOM_SHADOW_GRACE", DefaultShadowGrace),
+		ShadowTokenBudget: get("PIPEROOM_SHADOW_TOKEN_BUDGET", DefaultShadowTokenBudget),
+		ShadowMaxTurns:    get("PIPEROOM_SHADOW_MAX_TURNS", DefaultShadowMaxTurns),
```

```diff
 // Watcher defaults and limits.
 const (
 	DefaultWatchMode         = "log"
 	DefaultWatchInterval     = "60s"
 	DefaultWatchCheckTimeout = "10s"
 	DefaultWatchCooldown     = "60m"
 	...
 )
+
+// Shadow defaults.
+const (
+	DefaultShadowGrace       = "0"
+	DefaultShadowTokenBudget = "500000"
+	DefaultShadowMaxTurns    = "20"
+)
+
+// Shadow holds the typed shadow settings.
+type Shadow struct {
+	// Grace is how long a shadow run may keep going after the room closes.
+	// 0 = cancel at close, as a live run is.
+	Grace time.Duration
+	// TokenBudget caps model tokens per shadow room; 0 = no cap.
+	TokenBudget int64
+	// MaxTurns caps model turns per shadow run.
+	MaxTurns int
+}
+
+// ParseShadow validates the PIPEROOM_SHADOW_* fields. Empty fields mean the
+// default. Grace accepts "0" and any non-negative Go duration; TokenBudget
+// follows PIPEROOM_ROOM_TOKEN_BUDGET's rule (whole number >= 1, or "0" for no
+// cap); MaxTurns is a whole number >= 1.
+func ParseShadow(c Config) (Shadow, error) {
+	var sh Shadow
+	g := strings.TrimSpace(c.ShadowGrace)
+	if g == "" {
+		g = DefaultShadowGrace
+	}
+	if g != "0" {
+		d, err := time.ParseDuration(g)
+		if err != nil {
+			return Shadow{}, fmt.Errorf("config: PIPEROOM_SHADOW_GRACE=%q: not a duration like 0, 30s or 10m: %w", c.ShadowGrace, err)
+		}
+		if d < 0 {
+			return Shadow{}, fmt.Errorf("config: PIPEROOM_SHADOW_GRACE=%q: must not be negative", c.ShadowGrace)
+		}
+		sh.Grace = d
+	}
+	var err error
+	if sh.TokenBudget, err = parseWholeNumber("PIPEROOM_SHADOW_TOKEN_BUDGET", c.ShadowTokenBudget, DefaultShadowTokenBudget, true); err != nil {
+		return Shadow{}, err
+	}
+	n, err := parseWholeNumber("PIPEROOM_SHADOW_MAX_TURNS", c.ShadowMaxTurns, DefaultShadowMaxTurns, false)
+	if err != nil {
+		return Shadow{}, err
+	}
+	sh.MaxTurns = int(n)
+	return sh, nil
+}
+
+// parseWholeNumber parses a non-negative whole number; empty uses def. With
+// zeroOK, "0" is allowed (no cap); otherwise the value must be >= 1.
+func parseWholeNumber(name, v, def string, zeroOK bool) (int64, error) {
+	v = strings.TrimSpace(v)
+	if v == "" {
+		v = def
+	}
+	var n int64
+	for _, r := range v {
+		if r < '0' || r > '9' {
+			return 0, fmt.Errorf("config: %s=%q: want a whole number", name, v)
+		}
+		if n > (1<<62)/10 {
+			return 0, fmt.Errorf("config: %s=%q: too large", name, v)
+		}
+		n = n*10 + int64(r-'0')
+	}
+	if n == 0 && !zeroOK {
+		return 0, fmt.Errorf("config: %s=%q: must be at least 1", name, v)
+	}
+	return n, nil
+}
```

`cmd/server` wires `ParseShadow` into `room.Deps.ShadowOptions` (`agent.Options{Model: cfg.Model,
Effort: cfg.Effort, MaxTurns: sh.MaxTurns, TokenBudget: sh.TokenBudget}`) and `room.Deps.ShadowGrace`.
A `ParseShadow` error stops the start like a `ParseWatch` error.

### 5.4 `docs/api.md`

**Errors table**, 403 row: append `, not_allowed (also: label or grade a shadow room without being a member of
its pipeline)`. 409 row: append `; shadow mode: room_open (shadow route on an open room, or ?include=shadow
on an open room), not_labelled (shadow data read before the blind label)`.

**RoomSnapshot** example: add after `"resolution": ""`:
```
  "shadow": false,                 // true when the room's pipeline had shadow enabled at open (room.created.shadow): the agent ran silently, no proposals; see Shadow mode
```
and in the `agent_status` comment add `; in a shadow room the agent writes no status to the room log, so it
stays "idle"`.

**REST endpoints** table, new rows (after the `trace` row):

| Method | Path | Body | Result | Notes |
|---|---|---|---|---|
| POST | `/api/rooms/{id}/shadow/blind` | `{actual_layer, root_cause_terms}` | `Event` (`shadow.labelled`) | closed shadow rooms only; once per room (`409 already_decided`); caller must be a member of the room's pipeline: the solo user, or an approver or backup for any layer of that pipeline in `owners.yaml` (in dev mode also a `users.yaml` user whose `layers:` grant a layer the pipeline's `owners:` block does not replace), the same rule as the export minus auditors; else `403 not_allowed`; viewers `403 viewer`; `actual_layer` one of the six layers, `root_cause_terms` 1–20 strings of 1–200 chars → else `400 validation`; the event carries `identity` with the qualifying `grant` |
| GET | `/api/rooms/{id}/shadow?include=data,room` | – | `{run: ShadowRun, events: [Event + "shadow": true]}` | closed shadow rooms only; `409 not_labelled` until the blind label; any logged-in user, viewers included (`include=data` too, as with `tool_result.data` on `GET /events`); `events` have their own `seq` and are never fed to the room reducer; `tool_result.data` is omitted unless `include=data`; `include=room` adds `room: {created, labelled, graded}` for the eval builder. `ShadowRun` = `{room_id, status, model, pipeline_map, started_at, first_hypothesis_at, final_hypothesis_at, finished_at, finished_after_close, final_hypothesis, tool_calls, tokens, revealed_at}` (null times when unset; `status` ∈ `queued|running|hypothesised|no_hypothesis|error|budget|interrupted`) |
| POST | `/api/rooms/{id}/shadow/grade` | `{layer_correct, root_cause, notes?}` | `Event` (`shadow.graded`) | closed, labelled shadow rooms; same caller rule as `blind`; `root_cause` ∈ `yes|partial|no`, `notes` ≤ 2000 chars; re-grade allowed, last wins |
| GET | `/api/shadow/metrics?pipeline=&since=` | – | `{rooms, labelled, graded, layer_correct, root_cause: {yes, partial, no}, by_status: {…}, median_time_to_first_s, median_time_to_final_s, median_human_mttr_s, total_tokens, tokens_available, per_room: [...]}` | over **every closed shadow room** matching the filters (errors included); `since` RFC 3339 on `closed_at`; medians `-1` when no room has the value; `tokens_available` false when every run reports 0 tokens (demo, plain runner); `per_room` rows: `{room_id, pipeline_id, alert_source, status, opened_at, closed_at, labelled, graded, layer_correct, root_cause, time_to_first_s, time_to_final_s, human_mttr_s, tool_calls, tokens, finished_after_close}`; "N of M matched" = `layer_correct` of `rooms` |

**Export row** (`GET /api/rooms/{id}/export`), append to Notes: `?include=shadow` (closed rooms only, else
`409 room_open`; `409 not_labelled` before the blind label) adds a top-level `"shadow": {run, events}`
member (with `data`) after `"schema"`; it is inside the hashed document, so the hash differs from the plain
export. Same access rule.

**Close row** (`POST /api/rooms/{id}/close`), append: `a shadow room refuses an empty resolution with 400
validation`.

**Steer / suggestions / comments rows**, one shared note under the table: `In a shadow room these events are
logged but never start or wake an agent (the run is one-shot and takes no input).`

**New section** after "Watcher alerts":

```
### Shadow mode (`room.created.shadow: true`)

A pipeline with `shadow: {enabled: true}` in `owners.yaml` opens shadow rooms from every alert route
(dev webhook, signed sources, watcher); `POST /api/rooms` never does. In a shadow room the agent runs
once with read tools only, proposes nothing, and writes nothing to the room log: its events live in a
separate shadow log. When the driver closes the room (resolution required) and the run has ended, the
server appends `shadow.revealed` (system actor; numbers only). A member of the room's pipeline then
labels blind (`POST …/shadow/blind`), reads the run (`GET …/shadow`) and grades it
(`POST …/shadow/grade`). `GET /api/shadow/metrics` aggregates. Env: `PIPEROOM_SHADOW_GRACE` (default
`0`: the run is cancelled at close; a duration lets it finish), `PIPEROOM_SHADOW_TOKEN_BUDGET`
(`500000`), `PIPEROOM_SHADOW_MAX_TURNS` (`20`). Shadow runs share `PIPEROOM_MAX_AGENTS`. Events after
`room.closed` in a shadow room: `shadow.revealed`, `shadow.labelled`, `shadow.graded` (one per
re-grade). The Slack mirror stops at `room.closed` and never shows shadow output.
```

**Events list** (if api.md keeps one): `shadow.revealed`, `shadow.labelled`, `shadow.graded` with the §5.2
payloads.

### 5.5 Not lead-owned, shown for review

`internal/owners/owners.go` (A3):
```go
type ShadowSpec struct {
	Enabled *bool `yaml:"enabled"`
}
// UnmarshalYAML rejects unknown keys, like WatchSpec.
// Pipeline gains:
ShadowSpec *ShadowSpec `yaml:"shadow"`
// Shadow reports whether the pipeline opted in (nil block or enabled:false = no).
func (p Pipeline) Shadow() bool
// Owners gains, for the start-up store check:
func (o *Owners) AnyShadow() bool
```

`internal/store/iface.go` (A5):
```go
// ShadowStore is the shadow log and run table. Separate from Store like
// Purger: a store without it cannot host shadow rooms (Load refuses).
type ShadowStore interface {
	CreateShadowRun(ctx context.Context, r ShadowRun) error
	GetShadowRun(ctx context.Context, roomID string) (ShadowRun, error) // wraps ErrNotFound
	UpdateShadowRun(ctx context.Context, r ShadowRun) error             // all mutable columns; revealed_at untouched
	AppendShadow(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error)
	ShadowSince(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error)
	CountShadowEvents(ctx context.Context, roomID string, typ events.Type) (int, error)
	// MarkRevealed sets revealed_at once; first is false when it was set before.
	MarkRevealed(ctx context.Context, roomID string, at time.Time) (first bool, err error)
	// ListShadowRuns returns runs of closed rooms, newest close first, joined
	// with the room row (opened_at, closed_at, pipeline_id, alert_source).
	ListShadowRuns(ctx context.Context, pipelineID string, since time.Time) ([]ShadowRunRoom, error)
	// UnrevealedShadowRuns lists finished runs of closed rooms with no revealed_at (Load sweep).
	UnrevealedShadowRuns(ctx context.Context) ([]string, error)
}
```
`PurgeResult` gains `ShadowEvents, ShadowRuns int64` and `Add` folds them.

`internal/room/service.go` (A5): `Deps.ShadowOptions agent.Options`, `Deps.ShadowGrace time.Duration`;
`RoomSnapshot.Shadow bool` with json tag `shadow` (`snapshot.go`); `emitLocked` routing; closed inbox;
`ensureAgentLocked` guard; supervisor branch; `CloseRoom` rules; the four handlers in `httpapi`.

`web/src/api/types.ts` (A6): `AgentStatusPayload.state` comment adds `queued`; `RoomCreatedPayload.shadow?`;
`RoomSnapshot.shadow`; the three shadow payloads.

`internal/room/demo/demo.go` (A5, PR 1b): nil guard on `deps.Proposals` (3.7).

`internal/agent/endpoint.go` (A4, PR 2): `Run` sets the entry's limit from `Options.TokenBudget` under
`tokenBudget.mu` and calls `Deps.Usage` from a `defer`. `cmd/investigate/main.go` (A4, PR 2): a wrapped
`*BudgetError` from `ep.Run` prints and exits 0.

## 6. Evals and tests

Unit (memstore, `internal/room`):
- **Leak test.** Shadow room, fake runner emits status/step/tool/message/hypothesis, the supervisor's retry
  path writes `error` twice, the slot pool is full so `queued` and `working` are written, then a restart
  (`Load`): `store.Since(room)` holds only `room.created`, human events and system notes; every agent-actor
  event is in `ShadowSince`.
- Steer, `@agent` comment and suggestion send after restart start nothing; `ensureAgentLocked` is a no-op.
- `end_turn` ends the run → `hypothesised` when a hypothesis was emitted, else `no_hypothesis`.
- `MaxTurns` ends the run (status in `shadow_events` says so).
- Runner returns `*BudgetError` → `budget`, no retry; live room: same error → no retry, no `error` status.
- Close while the fake runner is mid-tool: grace `0` → `interrupted`, exactly one `shadow.revealed`;
  grace `10s` → the runner finishes → `hypothesised`, `finished_after_close: true`, one reveal.
- Grace `10s`, the fake runner fails after the close → `interrupted`, exactly one attempt (no retry), one
  reveal; the same failure before the close → three attempts, then `error`.
- `openAlertRoom` with a store whose `CreateShadowRun` fails once → the supervisor's second call creates
  the row and the run proceeds; fails always → no run, `CloseRoom` upserts an `error` row and reveals.
- `canGradeShadow`: solo user passes; owners.yaml approver and backup of one layer of the pipeline pass
  with the right grant; a `users.yaml` user with no layers → `403 not_allowed`; in oidc mode a group-only
  member of another pipeline → `403 not_allowed`; an auditor-only principal → `403 viewer` (httpapi).
- `demo.Run` with `Proposals == nil` emits five steps, a hypothesis, `idle`, and returns nil on the
  closed inbox with no panic.
- Run ends first, close later → `CloseRoom` writes the reveal; `MarkRevealed` returns `first=false` for the
  supervisor's late attempt.
- Empty resolution on a shadow room → `400`; `GET /shadow` before the label → `409 not_labelled`; second
  label → `409 already_decided`; viewer POST → `403 viewer` (httpapi); non-pipeline member → `403
  not_allowed`; identity and grant recorded on label and grade.
- `shadow.revealed` payload has no hypothesis text (reflect over the payload keys).
- Metrics medians with 0/1/2/odd rooms; ungraded rooms count in `rooms` only; `tokens_available`.
- `Load` sweep: a `running` row after restart → `interrupted`; closed + unrevealed → revealed.
- Export: without the flag the bytes equal today's export for a shadow room; with `?include=shadow` the
  `shadow` member is present and the hash header matches the document.
Integration (Postgres): migration up/down; `AppendShadow` seq is gap-free under 20 parallel appends; the
jsonb guard refuses a NUL payload; retention purges shadow rows and counts them.
Agent (`internal/agent`): `Endpoint.Run` honours `Options.TokenBudget` (new entry, and an entry that
`WriteHandoff` created first with `e.limit`, keeping `used`) and calls `Deps.Usage` exactly once with
`budget.Used()` on nil, on error and when the runner panics; `Run` returns `*BudgetError` wrapped.
`cmd/investigate`: a spent budget exits 0 with the budget text; any other error still exits 1.
Evals: `evalrun -from-shadow` output passes `-check`; one demo scenario runs on the fake runner.
Web: Vitest for the chip, blind form, reveal panel, grade form, metrics page; `npm run build && npm run lint`.

## 7. PR split

| PR | Owner | Scope | Done when |
|---|---|---|---|
| 0 `lead/shadow-contract` | Lead | §5.1–5.4 verbatim; migration number `00009` reserved | `go build ./...`; `docs/api.md` matches §5.4 |
| 1a `a5/shadow-store` (+A3 for `owners`) | A3, A5 (after PR 0) | `ShadowSpec` + `Pipeline.Shadow()` + `AnyShadow()` (A3); migration `00009`; `ShadowStore` on `*PG` and `memstore` (idempotent `CreateShadowRun`); `PurgeResult` counts + `purgeRoom` | `internal/owners` and `internal/store` tests green; integration: migration up/down, gap-free `AppendShadow` seq under 20 parallel appends, jsonb guard refuses a NUL payload, retention purges and counts shadow rows |
| 1b `a5/shadow-room` | A5 (after 1a) | `RoomSnapshot.Shadow`; `room.created.shadow` in `openAlertRoom` + `CreateShadowRun` there, in the supervisor and in the `Load` sweep; `emitLocked` routing; closed inbox; `ensureAgentLocked` guard; supervisor branch with statuses, `Deps.Usage`, `ShadowOptions`, no retry after close; live-supervisor guard for `*BudgetError` (lands before PR 2); close rules + grace + `error` upsert when no run row; reveal write-once + `Load` sweep; `noteRestartLocked` skip; `canGradeShadow` over `authorizeLayer`; the four routes; export `?include=shadow`; `ParseShadow` wiring in `cmd/server`; `demo.Run` nil guard | §6 unit tests green; the leak test passes; a shadow room's WS stream and `GET /events` carry no agent-actor event; `--demo` with a shadow `OWNERS_FILE` ends a room `hypothesised` |
| 2 `a4/shadow-agent` | A4 (after PR 0 and 1b) | `Options.TokenBudget` honoured in `Endpoint.Run` (sets the entry's limit under the mutex, keeps `used`); `Deps.Usage` called once from a `defer`; `Run` returns the wrapped `*BudgetError` after `stopForBudget`; `cmd/investigate` exits 0 on it | `internal/agent` tests pass; `cmd/investigate` test for the clean exit; a live room on a spent budget shows one idle status and no retry (test in `internal/room` from PR 1b now exercises the real return) |
| 3 `a6/shadow-ui` | A6 (starts after PR 0 on fixtures; merges after 1b) | shadow chip on list and room (from `snapshot.shadow`), steer box hidden, closed-room panel: blind form → reveal panel (run + shadow timeline, `data` behind a toggle) → grade form; metrics page with the copyable "N of M matched, median X vs Y min, tokens" line; `types.ts` | Vitest; `npm run build && npm run lint`; live check against `--demo` with a shadow `OWNERS_FILE` (works once 1b is in: the demo runner's nil guard is there) |
| 4 `c6/shadow-evals` | C6 (starts after PR 0 on a hand-written `run.json`; merges after 1b) | `evalrun -from-shadow`, README note on partial recordings | output passes `-check`; one demo scenario runs on the fake runner |

Order: PR 0 → PR 1a → PR 1b → PR 2 in series; PR 3 and PR 4 start on the §5.2/§5.4 JSON shapes right
after PR 0 and merge after 1b. PR 1a and 1b stay one agent each so no PR is bigger than one reviewer can read
in a sitting.

## 8. Decisions taken and what was verified

| # | Rev 2 open item | Decision in rev 3 | Where |
|---|---|---|---|
| 1 | F1: any `X-User` can label, read, grade | Closed by auth PRs 2–4: viewers blocked, identity recorded; label/grade need a member of the room's pipeline | 3.5, 5.4 |
| 2 | Grace after close | `0` by default, `PIPEROOM_SHADOW_GRACE` Go duration (lead) | 3.4, 5.3 |
| 3 | Export of shadow data | Only `?include=shadow`, only closed rooms, after the label (lead + blind rule) | 3.6, 5.4 |
| 4 | Grader | Any member of the room's pipeline, not viewers; actor + identity + grant recorded (lead) | 3.5 |
| 5 | Denominator | All closed shadow rooms, errors included (lead) | 3.6, 5.4 |
| 6 | Unverified #48/#49 pieces | Verified on main: `BudgetError`, `tokenBudget`, `Endpoint.budget/Forget`, `EndpointConfig` (`internal/agent/budget.go`, `endpoint.go`); `tools.WithPipelineID`/`ScopeFor` (`internal/tools/scope.go`, used in `runner.toolScope`) | 3.3 |
| 7 | How a room becomes shadow | `owners.yaml` `shadow:` block per pipeline; env holds numbers only; unknown pipelines can never be shadow | 3.1 |
| 8 | Watcher interaction | No watcher change; watcher rooms are shadow when the pipeline is; slots shared; cooldown from the close | 3.1, 3.4 |
| 9 | Reveal vs blind over WS | `shadow.revealed` carries numbers only | 3.4, 3.5, 5.2 |
| 10 | `agent_status shadow` marker | Dropped; `room.created.shadow` / `RoomSnapshot.shadow` instead | 3.1, 5.4 |
| 11 | Tokens to the supervisor | `Deps.Usage` callback from `Endpoint.Run`; no race with `Forget` | 3.3, 5.1 |
| 12 | Migration number | `00009`; lead confirms at merge (goose runs in order; `00008` is the last on main) | 3.2 |
| 13 | Review rev 3 must-fixes | Demo nil guard; `cmd/investigate` clean exit on budget; grader = `authorizeLayer` over layers; no retry after close | top table, 3.3, 3.5, 3.7 |
| 14 | Review rev 3 nice-to-haves | `TokenBudget` sets the entry's limit; `Usage` from a `defer`; idempotent `CreateShadowRun` + `error` upsert at close; `include=data` open to any logged-in user; PR 1 split in 1a/1b | 3.3, 3.6, 3.7, 7 |
