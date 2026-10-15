# F9 — Hypothesis verdict, who-can-act presence, lease awareness, catch-up

Owners: **F9a** leads (contract, part of F0) · **F9b** A5 (`internal/room`, `internal/httpapi`, `internal/ws`) ·
**F9c** A6 (`web/**`, two PRs: F9c1 verdict + presence + nudge, F9c2 lease + catch-up).
Model: F9b Opus 5.5 (checked appends under `s.mu`, a new presence field in the hub, inbox delivery); F9c Sonnet 5.5
(component and reducer work with existing patterns and tests).

Assumes merged: #147 `r11/a5-lease-decline` (`control.lease_expired`, `snapshot.driver.lease_expired`), #135
`a6/verified-badge` (`PersonName`, `verified` on presence and members), #125 `a6/room-over-hours`
(`lib/notify.ts`, `lib/attention.ts`, `useRoomAttention`), #146 (`usePeople` moved fixtures), r9/flaky-metrics
(`presence.go` `connsChanged`; no field conflict). F1 owns the decision bar and `roomStatus()`; F10 fixes lease
minute rounding (`useNow.ts minutesLeft`) — F9 does not touch either.

## Goal
In an approval wait the question is "who can unblock me, and are they looking?". The room answers it: each
person's layers and how long they have been away, the pending row names its owner's presence with a Nudge, a layer
owner can confirm or dispute the hypothesis so "confirmed by Kabir" is real, the driver sees the lease ending,
and whoever comes back after 12 minutes gets one line that says what happened.

## Evidence
- Platform review §4 gap 2: `events.AgentHypothesisPayload.Confirmed` is only ever written `false`
  (`internal/agent/tools_room.go:114`), copied in `snapshot.go:273`; `HypothesisCard.tsx:28` renders a badge that
  cannot appear. UX critique #14 (confidence path, `confirmed by Kabir 10:03`, board Room-Approval).
- UX critique #10: board lists `Kabir — postgres · kafka`, code lists `Kabir — viewing` (`PresencePanel.tsx:54`);
  `GET /api/people` returns no layers (`access.go:224 People`), the presence frame has no timestamps
  (`ws/gateway.go:167 Member`, `presence.go:111 members()`). Oncall journey 6 and 9 ("Waiting for Arjun · Nudge").
- UX critique #17: the driver learns of the lease only from "lease 24m" in `SteerBox.tsx:127`; #147 adds the
  server signal but nothing renders it yet.
- UX critique #15: nothing summarises what changed while a tab was hidden; `useRoom.ts:330` only flips presence
  to `away`.

## Scope — what to build

### F9b (A5)
1. **`POST /api/rooms/{id}/hypothesis {verdict, note?}`** (`internal/httpapi/hypothesis.go`,
   `internal/room/service_hypothesis.go`). Rules, in order: visibility (`404`), viewer (`403 viewer`, middleware),
   JSON body, `verdict ∈ {confirmed, disputed}` else `400 validation`; `note` ≤ 500 chars, typed-text rule (NUL /
   invalid UTF-8 → `400 validation`); room open (`409 room_closed`); shadow room (`409 room_open` — its hypothesis
   is hidden until reveal); a hypothesis must exist (`409 no_hypothesis`); the same actor repeating the same verdict
   on the same `hypothesis_seq` → `409 already_decided` (a changed mind — confirmed then disputed — is a new event).
   Appends `hypothesis.verdict {verdict, note?, hypothesis_seq}` with the caller as actor, as a checked append
   (`emitCheckedLocked`) so two replicas cannot both pass the duplicate check. Renews the driver lease only when
   the caller is the driver (any driver action renews). **Who may: any member, not only the driver.** Reason: the
   verdict is domain knowledge, and it usually sits with the layer owner (Priya knows the DDL ran, Meera knows the
   sink), who is rarely the driver; a dispute from a watcher is exactly the steer the first minutes need (D6).
   Driver-only would gate the one input the agent cannot get elsewhere behind the lease.
2. **Snapshot** (`snapshot.go`): `Hypothesis` gains `Seq int64` (the `agent.hypothesis` event's seq) and
   `Verdicts []Verdict{Verdict, By Person, Note, At}` — the latest verdict per person on the current hypothesis,
   in seq order; `agent.hypothesis` resets `Verdicts` to empty and `Confirmed` to false; `Confirmed` is **derived**:
   at least one `confirmed` and no `disputed` on the current seq. The agent-written `confirmed: false` in the event is
   ignored. Room list rows carry the same object (nothing else to add).
3. **Agent delivery** (`service.go inputFor`): a `hypothesis.verdict` becomes `agent.HumanInput{Kind: "comment",
   Text: "<verdict> the current hypothesis" + (": " + note)}` (no CommentID, no StepID) and wakes the agent like an
   `@agent` comment: a dispute must reach the model, and a confirm lets it move to `propose_fix`. No contract
   change in `agent/contract.go`; the agent already wraps comment text as untrusted.
4. **`GET /api/people` gains `layers`** (`access.go People`): `LayersFor(u)` per entry (`[]` when none); for IdP
   members, from their session groups. Unchanged: viewers get `403 viewer`.
5. **Presence `since`** (`ws/gateway.go Member`, `presence.go`): each `client` gets `stateSince time.Time`, set at
   register and whenever `c.state` changes (`setPresence`, `presenceLoop`); `members()` reports the winning state
   and the earliest `stateSince` among that user's connections in that state, as RFC 3339 UTC. Test with the
   gateway's fake `now`.
6. **Nudge** `POST /api/rooms/{id}/nudge {proposal_id, layer}` → event `approval.nudged {proposal_id, layer}`
   (actor = caller). Rules: member, room open, proposal open and `layer` still pending on it (`409 already_decided`
   when decided), at most one nudge per (proposal, layer) per 10 min (`409 already_nudged`, body message
   "nudged 4 min ago"). The event is the record the room and the Slack thread show; C12's bridge renders it as a
   thread reply `Waiting for <layer> owners — <caller> asked for a look · Open room` (C12 follow-up, one case in
   `render.go`; out of F9b). No other server-side notification exists; the web copies a deep link (item 12).
7. **api.md examples** for every new code and object (F9a writes the contract text; F9b adds examples).

### F9c1 (A6) — verdict, presence, nudge
8. **`room/HypothesisCard.tsx`**: confidence path line under the heading, 12/16 subtle, tabular nums:
   `low 10:00 → medium 10:01 → high 10:03` from `state.hypothesis_history` (new reducer array `{seq, at,
   confidence}` capped at 20; a repeat of the same confidence is not added). Verdict chips after the confidence
   badge: `confirmed by Kabir 10:03` (`success`, `Check`), `disputed by Priya 10:05` (`destructive`, `X`); several
   chips when several people decided; hover shows the note (`ui/tooltip.tsx`); the note is also in the activity
   rail line. Actions (members, open room, hypothesis exists): `[Confirm] [Dispute…]` ghost `size="sm"` at the card's
   right; your own current verdict shows as a pressed toggle (`aria-pressed`), clicking it again is a no-op
   (the server would answer `409 already_decided`). `Dispute…` opens an inline one-line `Input`
   (`placeholder="Why? (optional, 500 chars)"`, `Enter` sends, `Esc` cancels). `actions.hypothesisVerdict(verdict,
   note?)` in `useRoom.ts` (fixture mode synthesizes the event). Errors go to the room error banner via `runAction`;
   `friendlyError` gains `no_hypothesis: 'there is no hypothesis to decide on yet'`,
   `already_nudged: 'already nudged a few minutes ago'`.
9. **Reducer**: `hypothesis.verdict` → `hypothesis.verdicts` (latest per actor, keep order), `hypothesis.confirmed`
   derived as in item 2; `agent.hypothesis` resets verdicts, appends history; `fromSnapshot` seeds nothing (full
   replay). `describe.ts`: `Kabir confirmed the hypothesis` / `Priya disputed the hypothesis: <note>`;
   `Priya nudged kafka-connect owners`.
10. **`PresencePanel.tsx`**: each row `avatar · PersonName · you · Driver · layer badges · state`. Layers from
    `usePeople()` joined by id (`Badge variant={layerVariant(layer)} mono` 18 px, at most 3 then `+N` with a title);
    the people hook is called once in `RoomScreen` and passed down (`people?: Person[]`; viewers get none and see
    no badges). State column: `viewing` → "here"; `away` → `away 12m` from `member.since` (`ageLabel`), updated by
    `useNow()`; a member with no presence entry (in the log but not connected) → "not here". Rows `min-h-8`.
11. **`ProposalsPanel.tsx` `LayerRow`** (pending, not yours): replace the bare `Pending` badge with
    `Waiting for {names}` where names = people whose `layers` include the layer (from `people`), each with a presence
    word: `Arjun · away 12m`, `Priya · here`, `Meera · not here`; at most 3 names then `+N`. When nobody in
    `/api/people` owns the layer: `Waiting for a kafka-connect owner` (group grants are not in the list).
12. **Nudge** button on that row (`BellRing` 14 px, ghost `size="sm"`, `aria-label="Nudge kafka-connect owners"`):
    (a) copies to the clipboard
    `Your approval is needed for kafka-connect on "Warehouse orders stale": <origin>/rooms/<id>` (the #141 deep
    link; title is untrusted text, placed in quotes, newlines removed); toast `Copied — paste it to Arjun`;
    (b) calls `POST /nudge`; on `409 already_nudged` the toast says `Already nudged 4 min ago — link copied anyway`.
    After a successful nudge the row shows `nudged 10:07` (from the event) and the button is disabled for 10 min
    (`useNow`). Hidden for viewers and on closed rooms.

### F9c2 (A6) — lease awareness, catch-up
13. **Lease bar** in `SteerBox.tsx` (driver only): a 2 px `bg-primary` bar above the status line, width
    `left/30`; at `left ≤ 5` the status line turns `text-warning-foreground`:
    `Lease ends in 5m — any action renews · [Renew]`; `Renew` = `actions.grantControl(user.id)` (the server writes
    `control.granted` when < 25 min remain, so the bar refills from the event). When
    `state.driver.lease_expired` (new reducer flag folded from `control.lease_expired` whose `lease_until` equals
    `driver.lease_until`, cleared by `control.granted`): `Your lease expired — you still drive until someone takes
    over · [Renew]`. Non-drivers keep `lease 24m left`; with the expired flag they see
    `Arjun's lease expired · anyone can take control` (the `Take control` button already appears via
    `canTakeControl`). `aria-live="polite"` on the status line, state words only.
14. **`room/CatchUpStrip.tsx`**: `useCatchUp(state, conn, user)` in `hooks/useCatchUp.ts` keeps `{hiddenAt, snapshot:
    RoomState}` when `document.hidden` becomes true (and on `conn: 'reconnecting'`), and on visible / `'live'`
    computes `catchUpSummary(prev, next, user)` (pure, `lib/catchup.ts`): new steps count, hypothesis change
    (`hypothesis → high`, `hypothesis changed`), `a proposal needs you` (pending layer you own) / `a fix was
    proposed` / `fix ran` / `fix failed`, `hand-off for you`, `driver → Kabir`, `room closed`, `agent error`.
    Shown only when hidden ≥ 60 s **and** at least one item: `While you were away (12m): 3 steps · hypothesis →
    high · a proposal needs you` + `[Show me]` (scrolls to the proposal anchor when it needs you, else the
    hypothesis card, else the last step) + dismiss `X`. Reconnect: `Reconnecting… events after #31 pending` while
    `conn === 'reconnecting'` (seq = `state.last_seq`), then `Caught up` for 2 s on `'live'`. `role="status"`,
    `bg-primary-subtle`, one line, under the header (above F1's decision bar when both exist: catch-up first,
    since it explains the bar). Dismissed strips do not return for the same `hiddenAt`.

## Out of scope
Server push of nudges beyond the Slack thread; PagerDuty paging; per-person notification preferences; observe
mode / read receipts (presence frame fields `reading`, `viewing_step`); verdicts on past hypotheses; room-list
"away" columns; lease minute rounding (F10); the decision bar (F1).

## Contract (F9a, part of F0)

`internal/events/events.go`:
```go
	// HypothesisVerdict is a person's confirm or dispute of the agent's
	// current hypothesis (POST /api/rooms/{id}/hypothesis). HypothesisSeq is
	// the seq of the agent.hypothesis it answers; a later agent.hypothesis
	// starts over with no verdicts. Any member may write one; the snapshot
	// keeps the latest per person. AgentHypothesisPayload.Confirmed is never
	// set by the agent: clients and the snapshot derive it from these events.
	HypothesisVerdict Type = "hypothesis.verdict"
	// ApprovalNudged records that a member asked a pending layer's owners to
	// look (POST /api/rooms/{id}/nudge); at most one per proposal and layer
	// per 10 minutes. The Slack mirror renders it as a thread reply.
	ApprovalNudged Type = "approval.nudged"

// HypothesisVerdictPayload: Verdict is "confirmed" or "disputed"; Note is the
// person's optional reason (typed text, at most 500 characters).
type HypothesisVerdictPayload struct {
	Verdict       string `json:"verdict"`
	Note          string `json:"note,omitempty"`
	HypothesisSeq int64  `json:"hypothesis_seq"`
}

// ApprovalNudgedPayload names the proposal and the layer still pending on it.
type ApprovalNudgedPayload struct {
	ProposalID string `json:"proposal_id"`
	Layer      string `json:"layer"`
}
```
`AgentHypothesisPayload.Confirmed` doc comment becomes: `// Confirmed is always false when written by the agent;
derived state (snapshot and clients) sets it from hypothesis.verdict events. Kept for wire compatibility.`

`docs/api.md`:
```md
| POST | `/api/rooms/{id}/hypothesis` | `{verdict: "confirmed"\|"disputed", note?}` | `Event` (`hypothesis.verdict`) | F9 (server F9b, web F9c). Any member (viewers `403 viewer`); open rooms only (`409 room_closed`); a shadow room `409 room_open`; no hypothesis yet `409 no_hypothesis`; the same person repeating the same verdict on the same hypothesis `409 already_decided`; `note` ≤ 500 characters, typed-text rule. The event carries `hypothesis_seq`; a new `agent.hypothesis` clears every verdict. The agent receives it as a comment and wakes |
| POST | `/api/rooms/{id}/nudge` | `{proposal_id, layer}` | `Event` (`approval.nudged`) | F9. Any member; open rooms; the proposal must be open and `layer` pending on it (`409 already_decided`); one per proposal and layer per 10 minutes (`409 already_nudged`). Mirrored to the Slack thread when the bridge is on |
```
Errors table, 409: add `no_hypothesis`, `already_nudged`. `GET /api/people` result: `[{id, name, email?, verified,
layers}]` — `layers` are the layers the person may approve (`LayersFor`), `[]` when none.
RoomSnapshot `hypothesis`: `{ "text", "based_on_steps", "confidence", "confirmed", "seq": 17,
"verdicts": [ {"verdict": "confirmed", "by": {"id":"kabir","name":"Kabir","verified":true}, "note": "", "at": "…"} ] }`;
`confirmed` = at least one confirmed and no disputed verdict on `seq`.
WebSocket presence frame: `{ "type": "presence", "members": [ { "id", "name", "verified", "state": "viewing"|"away",
"since": "2026-10-11T10:52:00Z" } ] }` — `since` is when the member entered its current state on the server
(connect time for a new member); clients show `away 12m` from it.

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `internal/room/service_hypothesis.go` (+ `_test.go`), `service_nudge.go` (+ `_test.go`) | A5 | new |
| `internal/room/snapshot.go` (+ `snapshot_test.go`), `service.go` (`inputFor`), `access.go` (`People` layers), `errors.go` (`noHypothesis`, `alreadyNudged`) | A5 | modified |
| `internal/httpapi/hypothesis.go`, `nudge.go` (+ tests), `server.go` (routes), `handlers.go` | A5 | new/modified |
| `internal/ws/gateway.go` (`Member.Since`), `presence.go` (+ `presence_since_test.go`) | A5 | modified |
| `web/src/components/room/HypothesisCard.tsx` (+ `.test.tsx`), `PresencePanel.tsx` (+ `.test.tsx`), `ProposalsPanel.tsx` (`LayerRow`), `ProposalsPanel.presence.test.tsx` | A6 (F9c1) | modified/new |
| `web/src/state/{types,reducer,describe}.ts`, `hooks/useRoom.ts`, `api/{client,types}.ts`, `lib/errors.ts`, `RoomScreen.tsx` | A6 (F9c1) | modified |
| `web/src/components/SteerBox.tsx` (+ test), `room/CatchUpStrip.tsx` (+ test), `hooks/useCatchUp.ts`, `lib/catchup.ts` (+ test), `state/reducer.ts` (`control.lease_expired`) | A6 (F9c2) | new/modified |
| `internal/slackbridge/render.go` (`approval.nudged` reply) | C12 | follow-up, not in F9 |

## Dependencies and merge order
F0 (contract) → F9b (one PR ≈ 550 lines: route + snapshot + people layers + presence since + nudge) → F9c1 and F9c2
in parallel (F9c2 only needs #147's event and the presence `since`; F9c1 needs F9b's routes, tested in fixture mode
first with synthesized events). F9c1 rebases on #135 (`PersonName`) and #125 (toasts). F2b's post-mortem timeline
renders `hypothesis.verdict` rows when present. F11b places the track record next to the confidence badge — the two
share `HypothesisCard.tsx`; F9c1 merges first, F11b rebases.

## Design details
- **Verdict flow**: click Confirm → `POST` → `hypothesis.verdict` → reducer: `verdicts` upsert by actor id →
  chip + `confirmed` → the agent gets a comment input and may propose. A new `agent.hypothesis` clears chips; the
  history line keeps the confidence path. The card never shows "confirmed" from the event payload field.
- **Who's here data flow**: `people` (one GET per room mount, `usePeople`) × `presence` (WS frame, `since`) ×
  `state.members` (log). Layers are server-computed (`LayersFor`), so the row shows only layers an approve would
  honour. Away duration uses `since`, never a client-measured time, so a newcomer sees the same `away 12m`.
- **Nudge safety**: the copied text contains the room title (untrusted): quotes, newlines stripped, ≤ 120 chars;
  the URL is `window.location.origin + /rooms/ + encodeURIComponent(id)`. The server event carries ids only.
- **Lease**: `left` from `minutesLeft` as today (F10 floors it); the bar and amber line are driver-only; the
  expired flag folds exactly the api.md rule ("the last `control.lease_expired` whose `lease_until` equals the
  driver's current `lease_until`"). No polling: `useNow(30s)` already ticks.
- **Catch-up** is a pure diff of two folded states (like `lib/attention.ts`), so it is tested without timers; it
  never reads the activity window (capped at 20). Fixture mode: `?away=1` simulates a 12-minute hide for screenshots.
- **a11y / theme / mobile**: verdict buttons `aria-pressed`; chips carry the note in `title` and a visually hidden
  sentence; layer badges have `aria-label="owns kafka-connect"`; the strip is `role="status"` and dismissible with
  `Esc` when focused. Tokens only (`bg-primary-subtle`, `text-warning-foreground`, layer tints). Mobile: names list
  truncates to the first owner + `+N`; the catch-up strip wraps to two lines; 44 px targets on coarse pointers.

## Done when
- [ ] Kabir presses Confirm: the card shows `confirmed by Kabir 10:03` for everyone, `GET /api/rooms/{id}` has
      `hypothesis.confirmed: true` and `verdicts[0].by.id === "kabir"`; Priya's Dispute with a note shows a second
      chip and flips `confirmed` to false; the agent's next message acknowledges the dispute (demo: scripted ack).
- [ ] A second Confirm by Kabir on the same hypothesis answers `409 already_decided`; after a new `agent.hypothesis`
      the chips are gone and the path line reads `medium 10:01 → high 10:03`.
- [ ] Who's here shows `Priya — postgres · schema-registry — away 12m` from a presence frame with `since`
      12 minutes old; a second tab of Priya opening the room flips her to `here` within 2 s.
- [ ] A pending kafka-connect row reads `Waiting for Arjun · away 12m` with Nudge; Nudge copies the deep link, writes
      `approval.nudged`, the rail says `Priya nudged kafka-connect owners`, and a second click within 10 min answers
      `409 already_nudged` with the toast copy above.
- [ ] As driver with a lease at 4 min (fake `lease_until`), the composer shows the amber line; Renew writes
      `control.granted` and the bar refills; with `control.lease_expired` folded the expired copy appears for the
      driver and `anyone can take control` for others.
- [ ] Hide the tab for ≥ 60 s while 3 steps, a confidence change and a proposal for your layer arrive: on return the
      strip reads exactly `While you were away (1m): 3 steps · hypothesis → high · a proposal needs you`; Show me
      scrolls to the proposal; a reconnect shows `Reconnecting… events after #31 pending` then `Caught up`.
- [ ] `go test ./...`, `go vet`, `npm run build && npm run lint && npm test` pass; no new dependency; viewers see no
      Confirm/Dispute/Nudge controls and get `403 viewer` from both routes.

## Tests
- Go: `service_hypothesis_test.go` (member ok, viewer 403 via middleware test, closed 409, shadow 409, no
  hypothesis 409, duplicate 409, changed mind ok, note too long 400, NUL 400, snapshot derives `confirmed`, reset on
  new hypothesis, inbox gets a comment input, driver lease renewed only for the driver, two replicas one wins);
  `service_nudge_test.go` (pending ok, decided 409, 10-min window with fake clock, event payload ids only);
  `access_people_layers_test.go`; `ws/presence_since_test.go` (register, viewing→away flip at 45 s, earliest
  since across two connections); `httpapi/hypothesis_test.go`, `nudge_test.go` (codes and bodies).
- Vitest: `HypothesisCard.test.tsx` (path line, chips, pressed state, dispute input Enter/Esc, viewer hidden),
  `PresencePanel.test.tsx` (layer badges, `+N`, `away 12m`, `not here`), `ProposalsPanel.presence.test.tsx`
  (`Waiting for Arjun · away 12m`, nudge copy text, disabled after nudge, 409 toast), `reducer.test.ts`
  (`hypothesis.verdict` upsert, reset, `hypothesis_history` cap, `control.lease_expired` fold and clear,
  `approval.nudged`), `SteerBox.lease.test.tsx` (bar width, amber at 5 m, expired copy, Renew calls grantControl),
  `catchup.test.ts` (every item kind, ordering, empty → null), `CatchUpStrip.test.tsx` (60 s threshold, dismiss,
  reconnect copy with fake timers).

## Effort
F9a **S** · F9b **M** (one PR ≈ 550 lines) · F9c1 **M** (≈ 500 lines) · F9c2 **S–M** (≈ 400 lines).
