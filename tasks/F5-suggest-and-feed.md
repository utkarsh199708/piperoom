# F5 — Suggest without a driver, composer modes, curated activity feed (F5a A6 web; F5b A5 server; model: Sonnet 5.5 — pure functions and copy, no new protocol)

Read `AGENTS.md`, `docs/collab.md`, `docs/api.md` (Steer rows, `steer.declined` R11), `docs/product/vision-2026-10.md`
(§2 principle 3, §3 D6) and `docs/design/ui-v2/project/Room-Investigating.dc.html` (composer and Activity rail).
Design on top of merged #125 (`SuggestionState.status`, `room/SuggestionsPanel.tsx`, the `HOOK(R10)` comment in
`state/reducer.ts` for a declined event), #147 (server `POST /api/rooms/{id}/suggestions/{sid}/decline` →
`steer.declined`), #135 (`neutralizeBadgeText` on activity rows) and #146 (`handoff.requested`/`handoff.failed` lines
in `state/describe.ts`).

## Goal
The first minutes of an incident are when watchers know the most, and today their composer is disabled until
someone takes the wheel. Every watcher can leave a suggestion from second one, sees what became of it, and the
rail reads like a pulse — one curated line per event — instead of the server's stdout.

## Evidence
- Composer disabled with no driver: `SteerBox.tsx:225` `disabled={noDriver}`, `:103` `if (!text.trim() || noDriver) return`,
  placeholder `Take control to steer the agent` (`oncall-walkthrough.md` journey 7, notes `07-suggest`).
- The server already accepts it: `internal/room/service_steer.go:62-124` `Steer` turns any non-driver's text into
  `steer.suggested` and never checks that a driver exists; pending suggestions survive a take-over
  (`snapshot.go:276-293`). Only the web blocks it.
- Raw event dump in the rail: `state/describe.ts:53-57` prints `${p.tool} returned ${p.summary}` (a 300-char Connect
  stack trace on screen `screens-ux/02`), `:41-44` `Agent is waiting_approval (…)`, `:49-51` `Agent called pg_recent_ddl`,
  lease renewals (`:85-87`); `state/reducer.ts:472-482` keeps 20 entries of plain text.
- Ids in prose: `internal/room/demo/demo.go:132-133` `Proposal p_01M4… is waiting…` / detail `proposal p_…`;
  `internal/room/service.go:1024,1035,1055` feed `proposal p_…` and a **0-based** `step %d` to the agent, which echoes
  them (`oncall` journey 5: `step 0:` vs the card's `step 1`; `ProposalsPanel.tsx:632` and `describe.ts:105` are
  1-based). The runner never needs the id in `Text`: `agent.HumanInput.ProposalID/StepIndex` carry it
  (`internal/agent/contract.go:29-34`).
- The board promises `?ask the agent directly` and a suggester status; the code has one field and no feedback
  (`ux-critique.md` gap table "Composer", item 5).

## Scope — what to build

### F5a — Web (A6)
1. **Suggest with no driver.** `SteerBox.tsx`: remove `disabled={noDriver}` and the `noDriver` early return in
   `send`; with no driver a member's message is a suggestion (the server already does this; fixture mode
   `useRoom.ts:414-424` already synthesises `steer.suggested` for a non-driver). Status line copy:
   `No driver · your suggestion is queued for whoever takes control` (dot `warn`); placeholder
   `Suggest a first step…`; button `Suggest`. Viewer and closed-room bars unchanged.
2. **Composer modes.** New `web/src/lib/composer.ts`:
   ```ts
   export type ComposerRole = 'driver' | 'suggester'
   export type ComposerMode = 'steer' | 'suggest' | 'ask' | 'mention'
   export interface ComposerPlan { mode: ComposerMode; hint: string; send: 'steer' | 'comment'; text: string; stepId?: string }
   /** What this text does for this role: `?` asks the agent, `@name` is a comment, `/` is an explicit steer/suggestion. */
   export function planComposer(raw: string, role: ComposerRole, driverName: string | null): ComposerPlan
   ```
   Rules (prefix = first non-space char): `?` → `ask`: driver → `send:'steer'`, `text: 'Question: ' + rest`, hint
   `Asking the agent · it answers in the stream`; suggester → `send:'comment'`, `text: '@agent ' + rest` (a
   mention wakes the agent and it replies in-thread, `lib/mentions.ts`), hint `Asking the agent · it answers under
   Room comments`. `@` → `mention`: `send:'comment'`, text as typed, hint `Commenting · @agent also wakes the agent`.
   `/` or no prefix: driver → `steer`, text without the `/`, hint `Steering the agent · {mod}⏎ to send`; suggester →
   `suggest`, hint `Suggesting to {driverName} · they can forward it` or `Suggesting · queued until someone drives`.
   `SteerBox` renders the hint under the field (`text-xs text-subtle-foreground`, `aria-live="polite"`) and labels
   the button `Send to agent` / `Suggest` / `Ask` / `Comment`. `comment` goes through `actions.comment(text)` (room
   comment, no step).
3. **Quick-steer chips.** `lib/composer.ts` `quickSteers(state: RoomState): string[]` (max 3, in order): no steps →
   `Check the connector status`, `Check consumer lag on the sink group`, `Check the newest warehouse row`; hypothesis
   `medium`/`high` and no open proposal → `Confirm the hypothesis with one more read`, `Propose a fix`; latest
   proposal `rejected` → `Re-plan after the rejection`; all steps of an approved proposal executed → `Verify the fix
   end to end`. Rendered as `Button variant="ghost" className="h-7"` above the field for drivers and suggesters,
   hidden under `@media (pointer: coarse)` (F7c adds the `coarse:` variant; until then `[@media(pointer:coarse)]:hidden`).
   A click puts the text in the field and focuses it (never sends).
4. **Draft and recall.** Draft per room in `localStorage` key `piperoom.draft.<roomId>` (write on change, debounce
   300 ms, clear on successful send, `try/catch` everywhere); `↑` with an empty field recalls the last sent message
   of this room (`sessionStorage` `piperoom.lastsent.<roomId>`).
5. **Suggestion status for the suggester.** Above the field, for a non-driver, their own suggestions (newest 3):
   `Badge` `Queued · no driver yet` / `Queued for {driver}` (warning) → `Sent by {driver} · 10:02` (success) →
   `Declined by {driver}` (destructive, with the reason as `title`). Needs the reducer to handle `steer.declined`:
   `api/types.ts` `EventType` adds `'steer.declined'`, `SteerDeclinedPayload { suggestion_id: string; reason?: string }`
   (field names from `internal/events/events.go` after R0/#147 — read it, do not guess); `state/reducer.ts` replaces
   the `HOOK(R10)` comment with a case that sets `status: 'declined'`, `declined_by: event.actor`, `reason`. `useRoom`
   `actions.declineSuggestion(id, reason?)` → `api.declineSuggestion` (`POST /api/rooms/{id}/suggestions/{sid}/decline`);
   the driver's queue rows (`SteerBox.tsx:138-156`) gain a ghost-danger `Decline` next to `Send to agent`
   (optional reason via a one-line inline input, `Enter` sends). `409 already_decided` → `lib/errors.ts`
   `already_decided: 'this suggestion was already sent or declined'`.
6. **Curated feed.** `state/describe.ts`:
   ```ts
   export type ActivityKind = 'step' | 'human' | 'decision' | 'control' | 'system'
   export interface Described { text: string; detail?: string; kind: ActivityKind }
   export function describeEvent(e: Event, ctx: { driverBefore: string | null; stepTitles: Record<string, string>; proposalTitles: Record<string, string>; stepIndex: Record<string, number> }): Described
   ```
   Vocabulary (tool names in `<code>`, rendered by the feed from a `{tool}` placeholder; names via `neutralizeBadgeText`):
   | event | text | kind |
   |---|---|---|
   | `room.created` | `Room opened — {title}` (`Room opened by {who}` when human) | system |
   | `agent.status` | `Agent {state words}` e.g. `Agent waiting for approval`, `Agent idle`, `Agent error` (`detail` → `detail`) | system |
   | `agent.step.started` | `Agent started step {n} · {title}` | step |
   | `agent.tool_call` | `Step {n} · ran {tool}` (`detail`: `argTags(input).join(' ')`) | system |
   | `agent.tool_result` | `Step {n} done · {took}` / `Step {n} · {tool} failed` (`detail`: summary ≤ 200 chars) | step |
   | `agent.message` | `Agent replied on step {n}` (reply_to) / `Agent: {text ≤ 120}` | step |
   | `agent.hypothesis` | `Hypothesis {raised to\|lowered to\|at} {confidence} confidence` (`detail`: text) | step |
   | `comment.added` | `{who} commented on step {n}` / `{who} commented` (`detail`: text) | human |
   | `steer.suggested` | `{who} suggested: "{text ≤ 80}"` | human |
   | `steer.sent` | `{who} steered: "{text ≤ 80}"` / `{who} sent {from}'s suggestion` when `suggestion_id` | human |
   | `steer.declined` | `{who} declined {from}'s suggestion` (`detail`: reason) | human |
   | `control.requested` | `{who} asked for control` | control |
   | `control.granted` | `{who} took control` / `{who} handed control to {to}` / lease renewal → `{who} renewed the lease` | control / control / system |
   | `control.lease_expired` (R0, system actor) | `{driver}'s lease expired · no one is driving` | control |
   | `proposal.created` | `Agent proposed a fix · {title}` | decision |
   | `approval.given` | `{who} approved {layer}` | decision |
   | `approval.rejected` | `{who} rejected {layer}` (`detail`: reason) | decision |
   | `action.executed` | `Step {step_index+1} ran` / `Step {step_index+1} failed` (`detail`: output ≤ 200) | decision |
   | `handoff.requested` / `.failed` / `.created` / `.accepted` | keep #146 wording; kind control |
   | `trace.pinned` | `{who} traced {table} · {key}` | human |
   | `shadow.*` | keep #126 wording; kind system |
   | `room.closed` | `{who} closed the room` (`detail`: resolution) | decision |
   | unknown | `e.type` | system |
   Step numbers come from `ctx.stepIndex[step_id]` (1-based position in `state.steps`), never from ids; an unknown
   `step_id` prints `a step`. **No `p_…`, `g_…`, `s_…` or `h_…` id ever appears in `text` or `detail`.**
7. `state/reducer.ts:472-482`: `ActivityEntry` gains `kind` and `detail?`; window 50. `ActivityFeed.tsx`: rows with
   `kind !== 'system'` by default; a footer ghost button `Show all events` / `Show less` (persisted in
   `localStorage` `piperoom.activity.all`); `text` in `line-clamp-1` with `title={detail ?? text}`; `<code>` for the
   tool placeholder; time column `font-mono tabular-nums`; when the room is older than an hour, a muted group header
   per 5 minutes (`10:05`, `10:10`); keep #135's badge/neutralisation on human rows; keep `#125`'s `useJumpToLatest`
   reading `activity[].seq` (the window is bigger, nothing else changes).

### F5b — Server (A5)
8. **Rule (confirmed, already true in code).** `POST /api/rooms/{id}/steer` by any member who is not the live driver
   — including when the room has **no driver** or the driver's lease has expired — appends `steer.suggested` and
   answers `200 {Event}`. The suggestion stays in `pending_suggestions` across `control.granted` and is offered to
   the next driver. Status codes: `403 viewer` (read-only account), `409 room_closed`, `400 validation` (empty, NUL,
   bad UTF-8, or over `MaxMessageChars` after #141). It never wakes the agent (`ensureAgentLocked` runs only on
   `steer.sent`). Write this sentence into `docs/api.md` row `POST /api/rooms/{id}/steer` (the lead applies the
   api.md change; quote it in the PR): `others (also when no one drives or the lease expired) → steer.suggested,
   kept for the next driver`.
9. **Text the agent reads, no ids, 1-based.** `internal/room/service.go` `inputFor`:
   `approval_result` → `layer %q approved your proposal` / `layer %q rejected your proposal: %s`;
   `action_result` → `step %d: %s` with `p.StepIndex+1`. `ProposalID`/`StepIndex` fields stay as they are.
   `internal/room/service_executor.go:297` and `replicas.go:575` "outcome unknown: step %d" → `%d` is `i+1`.
10. `internal/room/demo/demo.go:132-133`: `The fix proposal is waiting for the schema-registry and kafka-connect owners.`
    and status detail `1 proposal open`; `:155` approval results → `waiting_approval` detail `in.Text` is fine now (id-free).

## Out of scope
Nudge/presence on rows (F9), `@name` autocomplete and people directory (later), suggestion threads, hypothesis
verdict (F9), evidence chips (F3), the prompt rule "name proposals by title, never by id" (**F10b**, A4).

## Contract
none. (`docs/api.md` wording change in item 8 is documentation of existing behaviour; the lead edits it.)

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `web/src/lib/composer.ts` | A6 | new |
| `web/src/components/SteerBox.tsx`, `ActivityFeed.tsx`, `state/describe.ts`, `state/reducer.ts`, `state/types.ts`, `api/types.ts`, `api/client.ts`, `hooks/useRoom.ts`, `lib/errors.ts`, `components/room/SuggestionsPanel.tsx` | A6 | modified |
| `internal/room/service.go`, `service_executor.go`, `replicas.go`, `demo/demo.go`, `service_test.go` | A5 | modified |
| `docs/api.md` | lead | one row |

## Dependencies and merge order
F5a and F5b are independent PRs; F5b first is nicer (the demo stops printing ids). F5a must land **after** #125
and #147 (it fills the `HOOK(R10)` the first left for the second). F1b changes `SteerBox` idle height and the
`--composer-h` variable: F5a touches the same file — rebase on whichever merges first; both keep the
`GrantPicker`, suggestion and control-request lists intact. F10b (A4) adds the prompt rule about ids.

## Design details
- Fold: `steer.suggested` → `status:'pending'`; `steer.sent{suggestion_id}` → `accepted`; `steer.declined` → `declined`.
  The suggester's chips read `state.suggestions.filter(s => s.from.id === user.id)`.
- `planComposer` is pure and the only place that decides steer vs comment; `SteerBox` never branches on text itself.
- Untrusted text: suggestion text, comment text and reasons are rendered as text nodes; `describeEvent` clips
  (`≤ 80` / `≤ 120` / `≤ 200` chars, rune-safe) and never builds HTML; `<code>` holds only the tool *name*
  (`/^[a-z0-9_]{1,64}$/`, else plain text).
- a11y: hint line `aria-live="polite"`; chips are buttons with the text as label; `Show all events`
  `aria-pressed`; group headers `role="presentation"`. Light/dark via tokens only. Mobile: chips hidden on coarse
  pointers; status chips wrap.
- Server: no state machine change. Unit test asserts the no-driver path writes `steer.suggested`, the row is
  `pending`, and a later `GrantControl` + `SendSuggestion` by the new driver emits `steer.sent` with the id.
- Failure modes: `declineSuggestion` 409 → friendly text, row stays as the log says; `localStorage` blocked → no
  draft, no error; a suggestion sent while offline → F10a's kept-draft path.

## Done when
- [ ] As Priya in a no-driver room the composer is enabled, says the queued copy, and `Suggest` writes a
      `steer.suggested` event; after Arjun takes control the suggestion is in his queue with `Send to agent` and
      `Decline`; Priya's chip flips `Queued · no driver yet` → `Queued for Arjun` → `Sent by Arjun · 10:02` or `Declined by Arjun`.
- [ ] `?why is task 0 failing` as a suggester posts a room comment `@agent why is task 0 failing`; as the driver it
      sends a steer `Question: why is task 0 failing`; the hint line names the mode before sending.
- [ ] The rail shows one clamped line per visible event in the vocabulary above; `connect_status` output is in the
      row's tooltip, not on screen; lease renewals, `agent.status` and `agent.tool_call` appear only after
      `Show all events`; no `p_`/`g_`/`s_` id anywhere in the rail.
- [ ] The agent's action-result line reads `step 1: …`, never `step 0`; the demo message names no proposal id.
- [ ] `go test ./internal/room/...` green; `npm run build && npm run lint && npx vitest run` green; no new dependency.

## Tests
- Go (`internal/room/service_test.go`): `TestSteerWithNoDriverQueuesSuggestion` (no driver → `steer.suggested`,
  `pending_suggestions` has it, agent not started), `TestSuggestionSurvivesTakeControl` (grant → send → `steer.sent`
  with `suggestion_id`), `TestInputForActionResultIsOneBasedAndIdFree`, `TestInputForApprovalResultIsIdFree`;
  `internal/room/demo`: `TestDemoMessagesCarryNoIds` (regex `\bp_[0-9A-Z]{26}\b` never matches emitted text).
- Vitest: `lib/composer.test.ts` (table: prefix × role → plan; `quickSteers` per state), `state/describe.test.ts`
  (new; every event type → text/kind, id-free assertion with `/\b[pgsh]_[0-9A-Za-z]{6,}\b/`), `state/reducer.test.ts`
  (`steer.declined` sets declined; window 50 with kinds), `components/ActivityFeed.test.tsx` (hidden kinds, Show all,
  clamp + tooltip), `components/SteerBox.test.tsx` (replace `with no driver the field is disabled…` by `with no
  driver a member can suggest`; add mode hint, chips, draft kept across remount, `↑` recall, Decline button,
  suggester status chips).

## Effort
F5a M (split F5a-1 composer + suggest, F5a-2 feed if the diff passes 600 lines) · F5b S.
