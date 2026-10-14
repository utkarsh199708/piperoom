# F10 — Resilience and the small traps (F10a A6 web · F10b A4 agent · F10c A5 server · F10d C1 docs; model: Sonnet 5.5 for all four — each is a list of small, fully specified fixes)

Read `AGENTS.md`, `docs/collab.md`, `docs/api.md`, `docs/product/vision-2026-10.md` (§3 D10, §4 F10) and the on-call
report §3 items 8, 9, 10, 14, 15 (quoted below). Design on top of merged #146 (`usePipelines`, `<select>` in
`NewRoomForm`, `useLoad`), #125 (`useRoomAttention`), #126 (`/shadow` sidebar link), #141 (`MaxMessageChars`),
#140 (`alerttext.go`, `readonly.go`) and F1 (`roomStatus`, `--composer-h`). Each sub-task is one PR in its owner's folders.

## Goal
Nothing in the room should trap a tired engineer: a dead server keeps your message, a dialog never follows you into
another room, an unknown pipeline says what to do, a pasted log line cannot talk to the model as the driver, and
the docs tell the truth about what the server reads.

## Evidence
- Item 8, dialogs leak between rooms: `RoomScreen.tsx:48-50` keeps `traceOpen/handoffOpen/closeOpen` in `useState`
  and `App.tsx:16` mounts `<RoomScreen />` without a key (screens `17b`, `H4`).
- Item 9, offline: a steer while the server is down shows a raw red `Failed to fetch` strip and the text is cleared
  before the request resolves (`useRoom.ts:414-424` `actions.steer` → `runAction`, `SteerBox.tsx:102-107` `setText('')`
  first; `lib/errors.ts` maps two codes). Screens `G3`, `G4`.
- Item 10, unknown pipeline: eternal `Loading topology…` (`PipelineMap.tsx:138`) while `useTopology.ts:44-50` keeps
  polling a `404` every 30 s (`docs/api.md` row `GET /api/pipelines/{pid}/topology`: unknown → `404 not_found`) and
  `Sidebar.tsx:57` polls it again; the demo agent's `deps.Proposals.Create` answers `400 validation "pipeline … is
  unknown; proposals are disabled"` (`service_proposals.go:124-129`), `demo.go:130-131` returns that error, and the
  supervisor (`service.go:1093-1096, 1166-1184`) retries **3×** with backoff, replaying the story each time:
  triplicated hypotheses and `Root cause found` (screen `H4`). The real agent routes the same error back to the
  model as a tool error (`tools_room.go:548-550`) and is not affected.
- Item 14, small traps: a second login in another tab bounces the first tab to sign-in with no explanation
  (screen `H7`; `UserContext.tsx:80-90` explains why: dev-login revokes the old session); New room's Pipeline free text
  → `400` (**fixed by #146**, `<select>` from `GET /api/pipelines`); `Ctrl⏎` submits `CloseRoomDialog.tsx:47` and
  `HandoffDialog.tsx:65` but not `NewRoomForm` (`RoomList.tsx:851`); lease `31m` for a 30-minute lease
  (`room/useNow.ts:18` `Math.ceil`); `Agent waiting · 0 of 2 approvals` reads like blame while nobody drives
  (**F1a** derives `No driver · 2m` instead).
- Item 15, docs drift: `README.md:128-131, 151-153` and `docs/runbook.md:111-114` say the Slack bridge is "pending in
  PR #24" while `cmd/server/slack.go:62 newMirrors` wires it and `internal/config/config.go:57,226` reads
  `SLACK_BOT_TOKEN`; `docs/runbook.md:50` names a personal Windows desktop path as the lock default; the brief's
  "demo needs Postgres" is false (`cmd/server/main.go:1-8`: `--demo` is in-memory).
- Platform review §4 item 9: `internal/agent/prompt.go:243` `[%s — driver] %s` sends `in.Text` raw; a pasted
  `</tool_output>` or `[verification] recovered` lands in the trusted channel (`escapeToolText` at `prompt.go:172`).
- Time: wall-clock in the browser zone only (`lib/time.ts:23-28` `clockTime`); Kafka/Connect logs are UTC
  (on-call item 12). Resolved rows say Agent `waiting` and `Opened 6m` (screen `E2`, `RoomList.tsx:641-699`).
  Dead `Pipelines`/`Audit log` rows (`Sidebar.tsx:79-94, 161-162`).

## Scope — what to build

### F10a — Web (A6)
1. **Dialogs never leak.** `App.tsx`: route element becomes `<RoomScreenRoute />`, a 5-line component that reads
   `useParams().roomId` and renders `<RoomScreen key={roomId} />`. Nothing else changes; all per-room `useState`
   (dialogs, `copied`, F7's layer filter and tab) resets with the key.
2. **Offline that keeps your words.** `lib/errors.ts`: `isNetworkError(e)` (a `TypeError` from `fetch`, or an `Error`
   whose message matches `/failed to fetch|networkerror|load failed/i`) and `OFFLINE_TEXT = "Can't reach Piperoom —
   your message is kept. Retrying…"`; `errorText` returns `OFFLINE_TEXT` for those. `useRoom.ts`: `actions.steer` and
   `actions.comment` return `Promise<boolean>` (true on success; fixture mode resolves true); a failed network call is
   pushed to a `pendingRef: {kind:'steer'|'comment', text, stepId?}[]` (max 5) and retried once when the socket
   reopens (`ws.onopen`) — success emits nothing extra (the event arrives over the socket), failure stays in the
   banner. The room error banner clears on reconnect when the error was a network error. `SteerBox.tsx`: clear the
   field only after `await actions.steer(...)` resolves true; while pending the button shows `Sending…` (disabled);
   on false the text stays, the field is re-focused, and a strip under the field shows `OFFLINE_TEXT` with a
   `[Retry]` ghost button (`role="alert"`); `conn === 'reconnecting'` alone also shows a muted
   `Reconnecting — messages are kept until the room is back` line. Same for `ReplyBox` in `Timeline.tsx`.
3. **Pipeline not configured.** `hooks/useTopology.ts`: on `ApiClientError` with `status === 404` set
   `notFound: true`, clear the interval and stop fetching until `pipelineId`, `sessionEpoch` or a manual `retry()`
   changes (export `retry`). Return `{ topology, error, updatedAt, notFound, retry }`. `PipelineMap.tsx`: new prop
   `notFound?: boolean`; when true render, instead of the skeleton lane, a dashed card: title `Pipeline "{id}" is not
   configured`, body `Alerts for it open a room, but there is no topology to show and the agent cannot propose a fix
   here. Add the pipeline to owners.yaml (see docs/runbook.md) and restart the server.`, buttons `[Try again]`
   (`retry`) and `[Open runbook]` (link to `/docs/runbook.md` only when served; else hidden). The sidebar row
   (`Sidebar.tsx:42-77`) shows the `unknown` dot and stops polling via the same flag. Never retried by a timer.
4. **Explain the bounce.** `context/UserContext.tsx`: when a `session_expired` report turns the phase to signed-out
   while a `/rooms/:id` route is open, write `sessionStorage['piperoom.signin.notice'] = JSON.stringify({ kind, title })`
   with `kind: 'replaced'` when `authMode === 'dev'` (dev-login elsewhere revokes this session) else `'expired'`.
   `LoginScreen.tsx` gets `notice?: { kind: 'expired' | 'replaced'; title?: string }` rendered as a muted line under
   the heading: `Your session ended — sign in to return to "{title}".` / `This tab was signed out because someone
   signed in as another user in a different tab. Sign in again to return to "{title}".` The deep link return
   (`lib/auth.ts:117-128`) is unchanged; the notice is cleared once shown.
5. **Pipeline select.** Done in #146. Verify `NewRoomForm` disables `Open room` until a listed pipeline is chosen;
   add nothing.
6. **Consistent `Ctrl⏎`.** `RoomList.tsx` `NewRoomForm`: the same `onKeyDown` as `CloseRoomDialog.tsx:47` on the
   form; the submit button shows `<Kbd>{mod}⏎</Kbd>` and `aria-keyshortcuts`.
7. **Lease minutes.** `room/useNow.ts`: `minutesLeft` floors (`Math.floor`), and a new `leaseExpired(iso, now)`
   (`Date.parse(iso) <= now`). `SteerBox.tsx:124-134`: expired → `lease expired`; `left === 0` and not expired →
   `lease ends in under a minute`; otherwise `lease {left}m`. Update the comment at `SteerBox.tsx:125`. `RoomList`'s
   `hasLiveDriver` already uses the instant, not the rounded minute.
8. **UTC on hover.** `lib/time.ts`: `utcLabel(iso)` → `2026-10-14 06:06:12 UTC` (invalid → `iso`). New
   `components/room/Clock.tsx`: `<time dateTime={iso} title={utcLabel(iso)} className="tabular-nums …">{clockTime(iso)}</time>`.
   Use it everywhere `clockTime` renders: `ActivityFeed.tsx:17-22`, `RoomHeader.tsx:132,138`, `ProposalsPanel.tsx`
   `decidedAt`, `room/SuggestionsPanel.tsx`, `Timeline.tsx` comment meta, `HandoffDialog`. Age labels get
   `title={utcLabel(opened_at)}` too (`RoomList.tsx:689`).
9. **Resolved rows.** `RoomList.tsx` `RoomRow` when `room.status !== 'open'`: status cell `Resolved · {duration}`
   (`durationLabel(opened_at, closed_at)` in `lib/time.ts`: `6 min`, `1 h 12 min`, `2 d 3 h`); driver cell `—`;
   agent cell `resolved` (F1a `agentCell`); the age cell `closed {ageLabel(closed_at)} ago` with the UTC title;
   under the title a `line-clamp-1 text-xs text-muted-foreground` with `resolution` (≤ 100 chars) on the Resolved
   tab. `closed_by` is shown when the snapshot carries it (contract item below); absent → nothing.
10. **Dead nav.** `shell/Sidebar.tsx`: delete `SoonItem` and the `Pipelines` / `Audit log` rows (keep `Shadow results`
    from #126). The sidebar `Pipelines` list stays.
11. `room/RoomError.tsx:14-15`: the paragraph renders `{message}` only (the heading already says `Could not load room`).

### F10b — Agent (A4)
12. `internal/agent/prompt.go` `formatHumanInput`, case `"steer"`: `fmt.Sprintf("[%s — driver] %s", name,
    escapeToolText(in.Text))` — escaped (so `<`, `>` , `&`, invisible characters and the `[verification]` tag are
    neutralised) but **not** wrapped in `<untrusted>`: a steer stays the one trusted instruction channel. The
    system prompt sentence that explains the wrapper gains: `A driver steer is shown escaped; "&lt;" in a steer is a
    literal character the driver typed, not markup.` Update the golden in `TestSystemPromptIsStable`.
13. Prompt rule (feeds F5): add to the system prompt's style rules: `When you write to people, name a proposal by
    its title and a step by its number; never print proposal, suggestion or hand-off ids (p_…, g_…, h_…). Step ids
    (s_…) are citations and stay.` Golden updated in the same PR.

### F10c — Server (A5)
14. **Never retry what cannot succeed.** `internal/room/service.go` supervisor loop (`:1138-1184`): before the
    `attempt >= maxAgentAttempts` check, `if isUnretryable(err)` → log `room: agent stopped (unretryable)` at Warn,
    emit `agent.status {state:"error", detail}` (already done just above), emit one `agent.message`
    `The agent stopped: {reason}` where reason for proposals-disabled is `proposals are disabled in this room because
    pipeline "{id}" is not configured — add it to owners.yaml and restart the server`, then `releaseAgent` and return.
    `isUnretryable(err)`: `errors.Is(err, tools.ErrBadInput)` || `errors.Is(err, topology.ErrBadInput)` || a `*room.Error`
    (`errors.As`) with `Code` `validation` or `room_closed`. Budget stops stay as they are (`isBudgetStop`).
15. `internal/room/demo/demo.go:130-131`: when `Create` fails with a `*room.Error` of code `validation`, do not
    return the error: emit `agent.message` `I cannot propose a fix in this room: {err.Msg}` and `agent.status`
    `{state:"idle", detail:"proposals disabled"}`, then `return waitForInput(ctx, deps, emit)`. Any other error returns as today.
16. The `inputFor` text fixes (1-based, id-free) are in **F5b**; do not duplicate them here.

### F10d — Docs (C1)
17. `README.md` §"Slack bridge" (`:126-153`): replace the two "pending in PR #24" paragraphs with the current truth
    from `cmd/server/slack.go` and `internal/config/config.go`: the server starts the bridge when `SLACK_BOT_TOKEN`
    and `SLACK_CHANNEL_ID` are both set, `PIPEROOM_PUBLIC_URL` adds the room link, setting one of the two is a
    startup error (quote the exact log line from `slack.go`). Same for `docs/runbook.md` §"Slack bridge operations" (`:109-114`).
18. `docs/runbook.md:49-51`: the lock default is the value in `cmd/devctl/lock.go` (read it; today the text names a
    personal Windows path). Write `PIPEROOM_STACK_LOCK` with its real default and `change it when your checkout
    lives elsewhere`.
19. `README.md` / `deploy/README.md`: one sentence under the demo instructions: `--demo keeps everything in memory
    (no Postgres, no migrations); every restart empties the rooms. Set PIPEROOM_DB_URL only for the real mode.`
20. Add a `## Unknown pipelines` paragraph to `docs/runbook.md`: what an alert for an unlisted pipeline does
    (room opens, proposals disabled, agent stops with the message from item 14), and the fix (owners.yaml entry +
    restart). Cross-check every env var named in both files against `internal/config/config.go` (`go doc` or grep);
    list mismatches fixed in the PR.

## Out of scope
Room list over WebSocket (F6), catch-up strip (F9), the "Needs you" attention (F1d), a room-level "all times UTC"
toggle (later; hover is enough now), rate limiting of reconnects (already backoff 1–10 s), the agent's own
`propose_fix` error wording (it already explains).

## Contract
Optional, for item 9 only — put in **F0** if the lead agrees, else the row shows no closer:
```md
<!-- docs/api.md, RoomSnapshot example and field list -->
"closed_by": { "kind": "human", "id": "u_priya", "name": "Priya", "verified": true },   // only on closed rooms: the actor of room.closed
```
```go
// internal/room/snapshot.go RoomSnapshot (A5 owns the file; the API shape is the contract)
ClosedBy *events.Actor `json:"closed_by,omitempty"` // set from room.closed's actor in Apply
```
Everything else: none.

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `web/src/components/room/Clock.tsx` | A6 | new |
| `web/src/App.tsx`, `lib/errors.ts`, `lib/time.ts`, `hooks/useRoom.ts`, `hooks/useTopology.ts`, `components/{SteerBox,Timeline,PipelineMap,RoomList,RoomScreen,ActivityFeed,ProposalsPanel,HandoffDialog,LoginScreen}.tsx`, `components/room/{useNow.ts,RoomError.tsx,RoomHeader.tsx,SuggestionsPanel.tsx}`, `components/shell/Sidebar.tsx`, `context/UserContext.tsx` | A6 | modified |
| `internal/agent/prompt.go`, `prompt_test.go` | A4 | modified |
| `internal/room/service.go`, `internal/room/demo/demo.go`, tests | A5 | modified |
| `README.md`, `docs/runbook.md`, `deploy/README.md` | C1 | modified |

## Dependencies and merge order
F10b, F10c and F10d are independent of everything and can merge first. F10a after F1a (uses `roomStatus` and
`agentCell`) and F1b (`SteerBox` idle layout); F10a item 1 should land **before F7c** (tabs reset with the key).
F10a splits into F10a-1 (items 1–4, 11: resilience) and F10a-2 (items 6–10: small traps) if over 600 lines.

## Design details
- Pending-send queue: in-memory per `useRoom` instance (a reload loses it on purpose — the draft in `localStorage`
  from F5a survives); retried in order, once, after `onopen`; a retry that fails with a non-network `ApiClientError`
  (e.g. `409 room_closed`) shows the friendly text and is dropped.
- `useTopology` 404 is terminal for that `pipelineId`; a `refreshKey` bump (an executed action) does not resume it;
  `retry()` does. The hook never throws on 404.
- Session notice: written only from the `session_expired` path that already exists (`UserContext.tsx:84-90`,
  `expiring` ref), never from a logout the person chose.
- Supervisor: `isUnretryable` is a 10-line pure function in `service.go` with its own table test; the message is
  emitted through the same `agentEmit` with the run fence, so a stale run cannot write it.
- Steer escaping changes only the text the model sees; the stored `steer.sent` event is untouched. The
  `[name — driver]` label still comes from `sanitizeLabel`.
- Untrusted text: `notice.title` and `resolution` are text nodes; the runbook link is static.
- a11y: offline strip `role="alert"`; `Clock` keeps `<time dateTime>`; the not-configured card has an `h2`.

## Done when
- [ ] Open the hand-off dialog in room A, jump to room B via ⌘K: no dialog is open in B.
- [ ] Stop the server, type a steer, press ⌘⏎: the text stays, the strip says `Can't reach Piperoom — your message is
      kept. Retrying…`; start the server: the steer is sent once and the strip goes away.
- [ ] Webhook alert for pipeline `payments` (not in owners.yaml): the lane shows the `not configured` card, the
      network tab shows exactly one `404` for its topology, the agent writes one `The agent stopped: …` message and
      the hypothesis appears once (no triplication).
- [ ] Sign in as Priya in tab 2 while tab 1 is Arjun in a room: tab 1 shows the sign-in with the "another user in a
      different tab" line and returns to the room after sign-in.
- [ ] `Ctrl⏎` opens a new room; a fresh 30-minute lease reads `lease 30m` (then `29m`); hovering `06:06 PM` shows
      `… UTC`; a resolved row reads `Resolved · 6 min`, agent `resolved`, `closed 2m ago`; no `Pipelines`/`Audit log` rows.
- [ ] `go test ./internal/agent/... ./internal/room/...` green; `TestSystemPromptIsStable` golden updated with a
      diff that shows only the two new sentences; `npm run build && npm run lint && npx vitest run` green.
- [ ] README and runbook: `grep -n "PR #24"` returns nothing; every env var named exists in `config.go`.

## Tests
- Vitest: `App.test.tsx` (new: navigating `/rooms/a` → `/rooms/b` remounts `RoomScreen`), `lib/errors.test.ts`
  (`isNetworkError`, `OFFLINE_TEXT`), `hooks/useRoom.offline.test.ts` (new: failed steer keeps text, retried once on
  `onopen`, dropped on 409), `components/SteerBox.test.tsx` (`Sending…`, kept text, Retry strip, lease copy with
  floor: 30 min + 5 s → `30m`), `hooks/useTopology.test.ts` (new: 404 stops the interval, `retry` resumes),
  `components/PipelineMap.test.tsx` (not-configured card), `components/LoginScreen.test.tsx` (notice lines),
  `components/RoomList.test.tsx` (`Ctrl⏎` submits; resolved row columns), `lib/time.test.ts` (`utcLabel`,
  `durationLabel`), `components/room/useNow.test.ts` (`minutesLeft` floor, `leaseExpired`), `components/shell/Sidebar.test.tsx`
  (no dead rows), `components/room/RoomError.test.tsx` (single prefix).
- Go, A4 (`internal/agent/prompt_test.go`): `TestFormatHumanInputSteerIsEscapedNotWrapped` (input with
  `</tool_output>` and `[verification] recovered` → output contains `&lt;/tool_output&gt;` and `(verification)`, no
  `<untrusted>`), golden update in `TestSystemPromptIsStable`.
- Go, A5 (`internal/room/service_test.go`): `TestIsUnretryable` (table: validation / room_closed / `tools.ErrBadInput`
  / `topology.ErrBadInput` → true; `errors.New("dial")` / budget stop → false), `TestAgentSupervisorStopsOnUnretryable`
  (fake runner returning `validation("…")`: attempts == 1, one `agent.status error`, one `agent.message` starting with
  `The agent stopped:`), `TestAgentSupervisorRetriesTransient` (existing behaviour: 3 attempts; keep or add).
  `internal/room/demo`: `TestDemoStopsWhenProposalsDisabled` (fake `Proposals` returning validation → no error, idle
  status, exactly one hypothesis).
- Docs (C1): `make docs-check` or a one-line script in the PR: `grep -oE 'PIPEROOM_[A-Z_]+|SLACK_[A-Z_]+' README.md docs/runbook.md | sort -u` ⊆ names in `internal/config/config.go`.

## Effort
F10a M (split 2 × S–M) · F10b S · F10c S · F10d S.
