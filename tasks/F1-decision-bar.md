# F1 — Decision bar, derived room status, attention system (A6; model: Sonnet 5.5 for F1a/F1c, Opus 5.5 for F1b/F1d — F1b touches the approval card, the one surface that must not regress, and F1d has cross-file state)

Read `AGENTS.md`, `docs/collab.md`, `docs/product/vision-2026-10.md` (§2 principle 1 and 3, §3 D1/D4/D5/D7)
and `docs/design/ui-v2/project/Room-Approval.dc.html`, `Main.dc.html` first. You own `web/**` only.
Design on top of the merged PRs #146 (`usePipelines`, export button, hand-off read dialog), #135 (`VerifiedBadge`,
`neutralizeBadgeText`), #125 (`hooks/useRoomAttention.ts`, `lib/attention.ts`, `lib/notify.ts`,
`room/NotifyToggle.tsx`, `room/SuggestionsPanel.tsx`, `hooks/useJumpToLatest.ts`) and #126 (shadow rooms:
`state.shadow`). Extend those files; never re-create them.

## Goal
When a proposal needs *you*, the first thing on screen — on every device, in every tab — is the decision, and the
room, the list, the tab title and the OS say one true sentence about what is happening right now. Today the
Approve button sits under the composer and the header says "Open · 3d" while the pipeline is half fixed.

## Evidence
- Approve under the composer at 1440×900: ribbon y 517–553, Approve y 849–881, sticky composer y 751–900
  (`ux-critique.md` measurements, screen `screens-ux/04-…-v2.png`; `ProposalsPanel.tsx` `LayerRow` buttons at
  lines 535–552, composer `SteerBox.tsx:59` `sticky bottom-0`).
- Double scroll: `document.documentElement.scrollHeight` 1230–1267 at a 900 px viewport while `#root`/`body` are
  900 (`ux-critique.md`, screens `05`, `10`). `index.css:220-230` sizes `html, body, #root` to 100 % with no
  overflow rule; `RoomScreen.tsx:143` forces `min-h-[calc(100dvh-3rem)]`; `AppShell.tsx:193-196` makes `main` the
  scroller and wraps it in `.legacy-surface` (`index.css:260`, 15 px base, measured).
- The pill lies: "Agent waiting for approval" after execution, after a rejection and on a closed room
  (`oncall-walkthrough.md` §3 item 3, screens `14-close-dialog`, `F4`, `D2`; `lib/agentPill.ts:24-29` is
  status-only; `internal/room/demo/demo.go:155` re-emits `waiting_approval` on every approval result).
- Header status is `Open · <age>` always (`room/RoomHeader.tsx:77-80`); the derived word exists only on the list
  (`lib/rooms.ts:170` `roomStatusCell`), and the list's `agentCell` says "waiting" on resolved rows
  (`oncall` journey 17, screen `E2`).
- No title/favicon/notification when a proposal needs you (`oncall` journey 3: row appears after 14 092 ms with no
  cue; `web/index.html` ships `vite.svg`). #125 added a `(N)` title badge and notifications for proposals,
  hand-offs and driver changes while hidden; nothing for control granted to me, agent error, resolved, and no
  in-app toast.
- Bare `A` approves at once and the second approval runs a production change (`oncall` item 7, screen
  `H5-priya-after-A-key`; `ProposalsPanel.tsx:440-463`). The reject reason field sits under the sticky composer
  (`notes5.json` `covered: true`): `main` has no `scroll-padding-bottom`.
- Lease shows "31m" for a 30-minute lease (`oncall` item 14; `room/useNow.ts:18` rounds up). Fixed in **F10a**
  (`minutesLeft` floors); F1 only consumes it.

## Scope — what to build

### F1a — `roomStatus()` and a pill that tells the truth (S)
1. New `web/src/lib/roomStatus.ts`:
   ```ts
   export type StatusKind = 'closed' | 'resolved' | 'agent-error' | 'executing' | 'fix-failed' | 'fixed'
     | 'needs-approval' | 'no-driver' | 'investigating' | 'idle'
   export type StatusTone = 'ok' | 'warn' | 'fail' | 'agent' | 'muted'
   export interface RoomStatusInfo { kind: StatusKind; label: string; tone: StatusTone; dot: Dot /* lib/rooms Dot */ }
   /** The one sentence every surface shows. Precedence is the order of StatusKind. */
   export function roomStatus(input: RoomStatusInput, now = Date.now()): RoomStatusInfo
   export function statusInputFromState(s: RoomState): RoomStatusInput
   export function statusInputFromSnapshot(r: RoomSnapshot): RoomStatusInput
   ```
   `RoomStatusInput` = `{status, opened_at, closed_at, resolution?, driver: {lease_until?}|null, agent: {state, detail?},
   proposals: {status, required_layers, approvals, steps: number, executed: {ok: boolean}[]}[], steps: number}`
   (the snapshot has only `open_proposals` and no step count: `steps` = 0, `executed` = `[]`).
   Labels (exact): `Closed` · `Resolved` / `Resolved · verified` (agent `done`) · `Agent error` · `Executing fix · k of n`
   (a proposal `approved` with `executed.length < steps` and no failed step; k = executed + 1) · `Fix failed · step k` ·
   `Fix ran · verifying` (all steps ok, agent not `done`) · `Needs approval · <age>` (any proposal `open` with a
   pending layer; age = `ageLabel(opened_at)`) · `No driver · <age>` (open, `!hasLiveDriver`) · `Investigating · step N`
   (agent `working`; `Investigating` alone when N = 0) · `Idle · waiting for a steer`. Tone map: closed muted,
   resolved ok, agent-error fail, executing agent, fix-failed fail, fixed ok, needs-approval warn, no-driver warn,
   investigating agent, idle muted. `dot` = the `lib/rooms.ts Dot` with the same meaning (`info` never used).
2. `lib/rooms.ts`: `roomStatusCell(room)` becomes `roomStatus(statusInputFromSnapshot(room))` (keep the export name;
   `RoomList.tsx:591` and tests keep working). `agentCell` for a closed room returns `{dot:'ok', label:'resolved', agent:false}`
   whatever `agent_status` says; after execution with all `action.executed` ok returns `verifying`.
3. `lib/agentPill.ts`: keep the signature. Derive: room closed → `{tone:'muted', text:'Agent stopped'}`; status kind
   `fixed` → `{tone:'ok', text:'Fix ran · verifying'}`; `fix-failed` → `{tone:'fail', text:'Fix failed · step k'}`;
   `executing` → `{tone:'agent', text:'Executing fix', step: k}`; latest proposal `rejected` and agent `working` →
   `Proposal rejected · re-planning`; rejected and agent `idle`/`waiting_approval` → `Proposal rejected · waiting for a steer`;
   `error` → unchanged text, and `RoomHeader` renders `state.agent_status.detail` next to the pill as
   `<span class="max-w-[48ch] truncate text-xs text-destructive-fg" title={detail}>` (the reason was tooltip-only).
   Everything else as today. The `step` number stays out of the live region (`room/AgentPill.tsx` comment).
4. `room/RoomHeader.tsx:77-80`: the outline badge shows `roomStatus(statusInputFromState(state)).label` with its dot;
   drop the severity-coloured dot there (severity already has its own badge).
5. `shell/CommandMenu.tsx:159-176` "Go to a room" rows: replace the `closed` badge with
   `<span class="text-xs text-muted-foreground">{roomStatusCell(r).label}</span>` before the severity badge.

### F1b — The decision bar, the idle composer, the run confirm (M)
6. New `web/src/lib/decision.ts`:
   ```ts
   export type Decision =
     | { kind: 'needs-you'; proposal: ProposalState; layer: string; completing: boolean }
     | { kind: 'waiting'; proposal: ProposalState; pending: string[]; approved: number; total: number }
     | { kind: 'executing'; proposal: ProposalState; step: number; total: number; action: string }
     | { kind: 'done'; proposal: ProposalState; ok: boolean; failedStep?: number }
     | { kind: 'rejected'; proposal: ProposalState; by: string; layer: string }
   export function decisionFor(state: RoomState, user: CurrentUser): Decision | null
   ```
   Rules: viewers never get `needs-you`. `needs-you` = the same row `ProposalsPanel.tsx:794-801` gives `A`/`R`
   (first proposal, first layer `canDecideLayer`); `completing` = every other required layer is `approved`.
   `waiting` = an open proposal with pending layers none of which are mine. `executing`/`done`/`rejected` from the
   newest proposal with that status; `done` shows for 3 s after the last `action.executed` (the bar, not the
   function, owns the timer) then returns `null`; `rejected` stays until a newer proposal exists or the person
   dismisses it (sessionStorage `piperoom.dismissed.<proposal_id>`). Closed room → `null`.
7. New `web/src/components/room/DecisionBar.tsx`, mounted in `RoomScreen.tsx` directly under `<RoomHeader>` with
   `className="sticky top-0 z-20"` (the header scrolls away, the bar stays), `h-11` (44 px), `border-b`, no shadow,
   `role="region" aria-label="Decision"`. Copy per kind:
   - needs-you: `TriangleAlert` + `Needs your approval · {layer}` + ` — {proposal.title}` (`truncate`,
     `title=` full) · right: `[Reject R]` ghost-danger `[Approve {layer} A]` success `[View steps ↓]` ghost.
     Tint `bg-warning-subtle text-warning-foreground`. The card's ribbon (`ProposalsPanel.tsx:701-707`) loses its
     amber tint when the bar is on screen (one amber per fact): pass `ribbon={false}`.
   - waiting: `Waiting for {pending.join(' · ')} · {approved} of {total} approved`, `bg-card text-muted-foreground`, no buttons, `[View ↓]`.
   - executing: `Running step {step} of {total} · <code>{action}</code>` + a 2 px `Progress` indeterminate bar
     (`ui/progress.tsx`, `bg-agent`) under the text; `bg-agent-subtle`.
   - done ok: `Fix ran · {total} of {total} steps` `bg-success-subtle text-success-foreground`, gone after 3 s
     (`motion-safe:` fade 240 ms). done failed: `Fix failed at step {k} — see the card` `bg-destructive-subtle`, persistent.
   - rejected: `Proposal rejected by {by} · {layer}` muted, `[Dismiss]` ghost (`aria-label="Dismiss"`).
   Buttons act through a registry, not a second copy of the approval logic: add to `shell/context.ts`
   ```ts
   export interface DecisionHandle { approve: () => void; reject: () => void; scrollTo: () => void }
   export const DecisionHandleContext = createContext<{ get(): DecisionHandle | null; set(h: DecisionHandle | null): void } | null>(null)
   ```
   `RoomScreen` provides it; the `LayerRow` that has `shortcuts` (`ProposalsPanel.tsx:358`) registers
   `{approve: startApprove, reject: () => setRejecting(true), scrollTo}` every render (same pattern as
   `useRegisterRoomCommands`). The bar calls `get()?.approve()`. So A, R, the bar and the card all go through
   `startApprove`: solo typed confirm, 409 stale, missing-hash disable all stay in one place. `View steps ↓`
   = `scrollTo` (scrolls `#proposal` into view and focuses the card heading, as `ProposalsSummary.jumpToProposal`).
8. **Run confirm.** In `LayerRow`, `startApprove` becomes: solo → typed dialog (unchanged); not solo and
   `completing` (every other required layer approved) → open `RunConfirmDialog` (new, same file, Radix Dialog,
   `role="alertdialog"`): title `Run this fix now?`, body `Your approval completes the set. Piperoom runs {n}
   {n===1?'step':'steps'} on {layers.join(' and ')} right away.`, a `<ul>` of the steps (`{i+1}. {layer} · {action}`),
   buttons `[Cancel]` `[Approve and run ⏎]` (Enter and `Ctrl/⌘⏎` confirm, Esc cancels, focus returns to Approve
   via `useRestoreFocus`); not completing → approve at once as today. `aria-keyshortcuts="A"` unchanged.
9. **Composer idle height.** `SteerBox.tsx`: when the field is empty and not focused the section is one row:
   status line and the textarea share the row (`h-10` total, `rows={1}`, placeholder only, no submit button); on
   focus or text it grows to today's layout (`max-h-[140px]` as `grow()` already does). Suggestion and control-request
   lists above the field are unchanged. `SteerBox` sets `document.documentElement.style.setProperty('--composer-h',
   `${height}px`)` from a `ResizeObserver` on its section (cleared on unmount).
10. **Scroll padding.** `shell/AppShell.tsx:193` `main` gets `[scroll-padding-bottom:var(--composer-h,0px)]
    [scroll-padding-top:3rem]` (the decision bar), so `scrollIntoView`/focus never lands under the composer.
    Also `#proposal` (`RoomScreen.tsx:190`) keeps `scroll-mt-4`.

### F1c — Kill the double scroll, the 15 px base, the Vite favicon (S)
11. `index.css`: `html, body { height: 100%; overflow: hidden }` ; `#root { overflow: hidden }`; `AppShell` outer div
    (`AppShell.tsx:135`) `flex min-h-0 flex-1 overflow-hidden`; `RoomScreen.tsx:143` `min-h-full` instead of
    `min-h-[calc(100dvh-3rem)]`; `RoomList` root likewise if it uses a dvh height. Verify with the snippet in Tests.
12. Delete `.legacy-surface` (`index.css:259-272`) and the wrapper div in `AppShell.tsx:194` (keep `min-h-full` on a
    plain div). Then delete every rule in `index.css:302-1328` whose selector's class is not referenced in
    `web/src/**/*.tsx` (today only `muted`, `mono`, `empty`, `shell`, `exec__hash`, `handoff__summary` are; keep those six
    rules under a `/* legacy aliases */` comment). `npm run build` and the full test suite must stay green.
13. Favicon: `web/public/favicon.svg` (the `BrandMark` glyph, 32 px, `currentColor` with a `<style>` `@media
    (prefers-color-scheme)` pair) and `web/public/favicon-alert.svg` (same glyph with an amber `--warning` dot bottom-right);
    `index.html` `<link id="favicon" rel="icon" type="image/svg+xml" href="/favicon.svg">`; delete `vite.svg`.
    New `lib/favicon.ts` `setFavicon(alert: boolean)` swaps `href` (idempotent, no-op when the link is missing).

### F1d — Attention: title, favicon dot, notifications, toasts, "Needs you N" (M)
14. `lib/attention.ts` (#125): add kinds `control_granted` (driver became me and I was not the driver before),
    `agent_error` (agent state turned `error`), `resolved` (status turned `closed`). Bodies never carry tool data:
    `approval_needed` body = proposal title (≤ 120 chars, `neutralizeBadgeText`); `agent_error` body =
    `The agent stopped. Open the room to steer it.` (never `detail`); `resolved` body = `Closed by {name}`.
    `attentionItems` is pure; a `kind -> {title, body, tag}` table lives there.
15. `hooks/useRoomAttention.ts` (#125): keep its contract; add (a) in-app toasts via `sonner` for the five kinds
    `approval_needed`, `handoff` (to me), `control_granted`, `agent_error`, `resolved` **whether or not the tab is
    hidden** (notifications stay hidden-only, `document.hidden` honoured as today), with actions:
    `Needs your approval · {layer}` → `[Approve A]` (calls the `DecisionHandle`) `[View]`; `{from} is handing you the
    room` → `[Read summary]` (opens the #146 receive dialog via a new `RoomCommands.readHandoff`); `You now drive
    "{title}"` (no action); `Agent error` → `[Steer]` (focuses the composer); `Resolved · {resolution ≤ 80 chars}`.
    One toast per `tag`, `duration: 8000`, never during replay. (b) `document.title` from a new `lib/title.ts`
    `documentTitle({status, roomTitle, unread})`: `● Needs you — {roomTitle} · Piperoom` when `status.kind ===
    'needs-approval'` and it needs *me*; else `{status.label} — {roomTitle} · Piperoom`; prefix `(N) ` when `unread > 0`
    (replaces `withBadge`); plain `Rooms · Piperoom` on the list. (c) `setFavicon(needsMe || unread > 0)`.
16. New `hooks/useListAttention.ts` used in `AppShell`: diff `roomStats(rooms, user).needsYou` room ids between
    polls; a room that newly needs me → the same toast/notification as `approval_needed` with `[Open]`, and
    `setFavicon(true)` while any room needs me. `shell/Sidebar.tsx:157-158`: the Rooms row count becomes
    `<Badge variant="warning">Needs you {n}</Badge>` when `n > 0`, else the open count as today; `aria-label`
    `Rooms, {n} need you`.
17. `shell/AccountMenu.tsx`: a `DropdownMenuItem` `Browser notifications · {on|off|blocked}` that calls
    `requestNotifyPermission()` (from `lib/notify.ts`) when `default`; `disabled` with the reason when `denied`.

## Out of scope
Presence on pending rows and Nudge (F9), lease bar and renew (F9), catch-up strip (F9), execution card with per-step
checks and verification card (F4), evidence chips (F3), bottom-sheet placement of the bar and tabs below 1280 (F7),
room list over WebSocket (F6), `minutesLeft` floor (F10a), post-mortem/export surfaces (F2).

## Contract
none. (`docs/api.md` unchanged; everything derives from events already in `internal/events/events.go`.)

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `web/src/lib/roomStatus.ts`, `lib/decision.ts`, `lib/title.ts`, `lib/favicon.ts`, `hooks/useListAttention.ts`, `components/room/DecisionBar.tsx` | A6 | new |
| `web/src/lib/rooms.ts`, `lib/agentPill.ts`, `lib/attention.ts`, `hooks/useRoomAttention.ts`, `components/room/RoomHeader.tsx`, `components/ProposalsPanel.tsx`, `components/SteerBox.tsx`, `components/RoomScreen.tsx`, `components/shell/{AppShell,Sidebar,AccountMenu,CommandMenu,context}.tsx/ts`, `index.css`, `index.html`, `public/favicon*.svg` | A6 | modified |

## Dependencies and merge order
F0 not needed. Order inside F1: **F1a → F1b → F1c**, F1d after F1a (needs `roomStatus`) and in parallel with F1b/F1c.
Each sub-task is its own PR (< 600 lines). F7c later changes the bar's placement through a `placement` prop:
export `DecisionBar` with `placement?: 'top'` now and keep its internals placement-free. F10a changes
`minutesLeft`; do not touch `room/useNow.ts` here.

## Design details
- Data flow: `useRoom` → `RoomState` → `roomStatus` / `decisionFor` (pure, memoised in `RoomScreen`) → header badge,
  pill, bar, title, favicon. The list path: `useRoomList` → `RoomSnapshot[]` → `roomStatusCell` / `roomStats`.
  Nothing new in the reducer except reading `executed[].ok` and proposal `status` that are already there.
- The demo agent re-emits `waiting_approval` after every approval (`demo.go:155`); the derivation must win over
  `agent_status` whenever a proposal is `approved`/executed, which is why `executing`/`fixed` outrank it.
- Untrusted text: proposal titles, resolutions, names and agent details are rendered as text nodes and go through
  `neutralizeBadgeText` (#135) before a title/notification/toast; tool summaries and `output` never leave the card.
- Toasts and notifications fire only for state *changes* seen after replay (`replaying` false, #125), never on load
  or on a reload after 409.
- a11y: the bar is a `region`; its state text sits in one `aria-live="polite"` span (same rule as `AgentPill`);
  the Approve button keeps `aria-keyshortcuts`; the run-confirm is an `alertdialog` with focus trapped and
  restored. Light/dark: only tokens (`bg-warning-subtle`, `bg-agent-subtle`, `bg-success-subtle`, `bg-destructive-subtle`).
- Mobile: the bar wraps to two rows below `sm` (text row, buttons row) until F7c turns it into a sheet.
- Failure modes: no `DecisionHandle` registered (card not mounted, e.g. solo banner off and no proposals) → the
  bar renders nothing for `needs-you`; `Notification` undefined → no-ops (`lib/notify.ts`); `localStorage`/`sessionStorage`
  throw → catch and fall back to in-memory.

## Done when
- [ ] At 1440×900 in fixture mode (`VITE_FIXTURES=1`, `?user=priya`, room `schema-change`) the Approve button is
      inside the first 600 px at the moment the proposal lands, with no scrolling; `A` and the bar button both reach
      `startApprove`.
- [ ] `document.documentElement.scrollHeight === window.innerHeight` at 1440×900 and 390×844 on the room and the list.
- [ ] Computed `font-size` of a plain `<p>` inside the room is 13 px.
- [ ] Pill reads `Fix ran · verifying` after both steps executed, `Proposal rejected · …` after a rejection,
      `Agent stopped` on a closed room; header badge shows the derived label; list `Agent` column never says
      `waiting` on a resolved row.
- [ ] Pressing `A` as the second approver opens `Run this fix now?`; Enter runs; Esc leaves the proposal open.
- [ ] With the tab hidden, a proposal that needs me changes the title to `● Needs you — …`, swaps the favicon, and
      (permission granted) raises one notification; showing the tab clears the `(N)`; a toast with `Approve` appears
      in the foreground too.
- [ ] Sidebar shows `Needs you 1` while a room needs me.
- [ ] Pressing `R` scrolls the reason field fully above the composer.
- [ ] `npm run build`, `npm run lint`, `npx vitest run` green; no new dependency.

## Tests (Vitest)
- `lib/roomStatus.test.ts`: table over every `StatusKind` with precedence cases (approved+executing beats
  `waiting_approval`; closed beats error; `Resolved · verified` only with agent `done`); snapshot and state adapters agree.
- `lib/decision.test.ts`: needs-you picks the same row as the `A` key; `completing` true only when all other layers
  approved; viewer → never needs-you; closed → null.
- `components/room/DecisionBar.test.tsx`: five kinds render their exact copy; Approve calls the handle; `View steps`
  focuses the card heading; done state disappears after 3 s (fake timers).
- `components/ProposalsPanel.approval.test.tsx`: add `A as the set-completing approver opens the run confirm`,
  `Enter in the run confirm approves with the on-screen params_hash`, `Esc leaves it open`, `solo users get the typed
  dialog, not the run confirm`, `a non-completing approval needs no confirm`.
- `components/SteerBox.test.tsx`: `idle composer is one row and grows on focus`; `sets --composer-h`.
- `lib/agentPill.test.ts`: executed → verifying; rejected → re-planning / waiting; closed → stopped; error detail inline.
- `lib/attention.test.ts`: new kinds; `agent_error` body never contains `detail`.
- `lib/title.test.ts`, `lib/favicon.test.ts`.
- `components/shell/Sidebar.test.tsx` (new): `Needs you N` badge; `AppShell.attention.test.tsx`: a room newly
  needing me toasts once.
- Scroll check (manual, paste into the PR "How I tested"): `({h: document.documentElement.scrollHeight, w: innerHeight,
  base: getComputedStyle(document.querySelector('main p')).fontSize})` at both viewports.

## Effort
F1a S · F1b M · F1c S · F1d M. Four PRs by one agent in that order, or F1d by a second agent after F1a merges.
