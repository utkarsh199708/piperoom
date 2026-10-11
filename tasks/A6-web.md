# A6 — Web app (React + TypeScript)

Read `AGENTS.md` first. You own: `web/**`.

## Goal

The room UI from the mockup, live over WebSocket, usable by several people at once.

Mockup (look and layout to match): https://claude.ai/artifact/PhH3PpaSCSQ6PW2z4CLChD
API contract: `docs/api.md`. Event payload types: `internal/events/events.go` (mirror them in
`web/src/api/types.ts` — same field names, same JSON).

## Setup facts

- `web/` is a Vite + React + TypeScript app (already installed; `npm run dev` → `http://localhost:5173`).
- npm on this machine blocks install scripts by default. `esbuild` is already approved. If a new package
  needs an install script, say so in the PR instead of approving it yourself.
- Pin exact versions for any new dependency. Allowed without asking: `react-router-dom`, `vitest`,
  `@testing-library/react`. Anything else: justify in the PR. No large UI kits.

## Screens

1. **Room list** — open rooms (title, severity, age, agent state, driver); "New room" form.
2. **Room** (main screen, mockup artboard 1):
   - Header: id, title, severity chip, open-for time, presence avatars, Hand off, Invite (copy link).
   - Left: **pipeline map** from `GET /api/pipelines/{pid}/topology` (refresh every 30 s); node cards with
     health colour, owner, facts; "Trace one record" opens the trace panel.
   - Middle: **current best guess** card; **step timeline** (each `agent.step.started` + its tool call/result;
     tool input in mono; error results styled as errors); **comment threads anchored to steps**
     (reply box on each step; agent replies via `reply_to` appear in the thread);
     **steer box** — driver sends directly; others see "Your message goes to <driver>'s queue".
   - Right: **open proposals** with per-layer approval state and Approve / Reject (enabled only for the
     owner of a pending layer); **who's here** (presence + role); **activity** feed (last 20 events, readable sentences).
   - Driver UI: "Request control" for others; the driver sees pending suggestions with "Send to agent".
3. **Trace panel** (artboard 2): table + key form → `POST /trace`; render `trace.pinned` stages left→right,
   exactly one highlighted `stopped_here`.
4. **Hand-off dialog** (artboard 3): pick next person, "stay as watcher" checkbox → `POST /handoffs`;
   show the generated summary from `handoff.created`; the receiver gets an Accept banner.
5. **Phone layout** (artboard 4): at < 600 px the room becomes one column with the approval card first
   when the current user has a pending approval. Touch targets ≥ 44 px.

## State model (important)

- On open: `GET /api/rooms/{id}` (snapshot) for the header / first paint, then WebSocket with **`after=0`**
  so the full log replays — the snapshot has no timeline, comments or activity, so the room must be built
  from events. (Corrected 2026-10-11: the earlier `after = snapshot.last_seq` left the timeline empty.)
- One reducer `applyEvent(state, event)` folds events into UI state (the same idea as A5's `Apply`).
  Ignore events with `seq <= lastSeq` (dedupe). On a gap (`seq > lastSeq + 1`) fetch
  `/events?after=lastSeq` before applying.
- Reconnect with backoff (1 s → 10 s) and `after = lastSeq`. Show a small "reconnecting" badge.
- Optimistic UI is **not** needed; wait for the event to come back.
- Dev user switcher in the header (`X-User` / `?user=`) so two tabs can act as two people.

## Before the server exists

Run against `go run ./cmd/server --demo` (A5) as soon as it lands. Until then, add a fixture mode
(`VITE_FIXTURES=1`) that replays `web/src/fixtures/schema-change.json` (events in the exact API shape,
built from the mockup story) through the same reducer with timers.

## Style

Dark theme from the mockup, as CSS variables in one file: background `#0D1015`, panel `#141922`,
border `#262E3B`, text `#E8ECF2`, muted `#A3ADBB`, agent/info blue `#6EA8FF`, warning orange `#FFA94D`,
failure red `#FF6B6B`, ok green `#4CC38A`. Fonts: IBM Plex Sans + JetBrains Mono (Google Fonts).
Text contrast ≥ 4.5:1. Real `<button>`, `<label>`, `<form>`; keyboard usable; `aria-label` on icon buttons. No emoji.

## Tests

Vitest: `applyEvent` for every event type, dedupe and gap handling; permission logic (who sees
Approve, Send to agent, Request control). `npm run build` and `npm run lint` pass.

## Done when

Two tabs (Arjun as driver, Kabir as watcher) on the `--demo` server: Kabir's suggestion appears in
Arjun's queue, Arjun sends it, the agent step appears in both tabs within 1 s; Meera's tab can approve
only the warehouse layer; reloading a tab restores the exact same state.
