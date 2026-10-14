# F2 — Hand-off that survives: pinned shift summary, export + post-mortem draft

Owners: **F2a** A6 (web) · **F2b** A5 (route, event, close hook) · **F2c** A4 (agent draft).
Model: F2a Sonnet 5.5 (pattern work over existing components and tests); F2b Opus 5.5 (close path under
`s.mu`, a system event after `room.closed`, Markdown escaping of untrusted text); F2c Opus 5.5 (prompting +
grounding; the same seam as the hand-off writer).

Assumes merged: #146 `a6/first-five-minutes` (recipient "Read summary" dialog, header **Export** button,
`exportRoom()` in `api/client.ts`, `handoff_request/handoff_failure` in the reducer), #141 `r4/first-five-minutes`
(`handoff.requested/failed` on the server, `GET /rooms/{id}` deep link), #125 (`sonner` toasts, `lib/notify.ts`).
F1 owns `lib/agentPill.ts` and `roomStatus()`; F2 never edits them.

## Goal
At shift change the incoming driver reads the agent's summary **before** accepting and everyone can read it for
the rest of the room's life. When the room closes, nobody has to write the post-mortem from scratch: a grounded
draft exists within minutes, and Export / Post-mortem are one click away in the header, ⌘K and the closed banner.

## Evidence
- Oncall journey 13–14 (`11b-handoff-summary-arjun`, `11c-priya-accept-banner`, `B1-priya-handoff-dialog-after-accept`):
  summary shown only to the sender while pending; after accept nobody can see it. `HandoffDialog.tsx:89`
  (`pending ? <HandoffSummary/> : <form/>`), reducer keeps it (`state/reducer.ts:394-447`) but no component renders an
  accepted hand-off.
- Oncall 15–16 (`D2-room-closed-priya-full`, `export.json`): closed room still says "Priya is driving" and
  "Agent waiting for approval" (`RoomHeader.tsx:169-171`, `AgentPill`); export is API-only; no post-mortem anywhere
  (platform review §1 "Post-incident: absent", §4 gap 1 `CheckHandoff` has no non-test caller).
- UX critique #16 (pinned `Shift 2 · from Arjun to Kabir · 10:05`), signature interaction C "Shift Change".
- Export rule: `internal/room/access.go:376` `CheckExportAccess` — approver/backup/auditor only, **not the driver**;
  the web must handle `403 not_allowed` gracefully.

## Scope — what to build

### F2a (A6, `web/**`)
1. **`room/HandoffCard.tsx`** — one card per hand-off, props `{handoff: HandoffState, index: number, from: Actor,
   createdAt: string, acceptedAt?: string, defaultOpen: boolean}`. Header line (13/18/500):
   `Shift {index+1} · from {from.name} to {to.name} · {clockTime(createdAt)}`, then a `Badge`:
   `pending` (`warning`, "waiting for {to.name}"), `accepted` (`success`, "accepted {clockTime(acceptedAt)}"),
   `voided` (`neutral`, "superseded"). Body = the six sections exactly as `HandoffSummary` renders them today
   (extract `HandoffSummary` from `HandoffDialog.tsx:155-178` into this file and import it back). Collapsible:
   a `button` with `aria-expanded`, `aria-controls`; chevron 16 px. Prose `text-sm leading-5 max-w-[72ch]`.
   Every string is untrusted text rendered as React text nodes; never `dangerouslySetInnerHTML`.
2. **Pinned for the room's life.** `RoomScreen.tsx`: between `<PipelineMap>` and `<HypothesisCard>` render
   `state.handoffs.filter(h => h.status === 'accepted')` newest first, `defaultOpen` only for the newest and only
   until the viewer collapses it (remember per room in `localStorage` key `piperoom.handoff.collapsed.<roomId>`,
   wrapped in try/catch). Older accepted cards start collapsed. Pending cards are not pinned (see 3).
3. **Before Accept.** Replace #146's `HandoffReceiveDialog` with the same `HandoffCard` rendered **inline under the
   accept banner**, expanded, with the banner's `Accept` button moved to the card footer:
   `[Not yet] [Accept and take over ⌘⏎]`. The banner keeps one line: `**Arjun** is handing the room to you — read the
   summary below.` Non-recipients see the pending card collapsed under the banner area with "waiting for Kabir".
   When `handoff.accepted` arrives the card moves to the pinned list (same component, status flips).
4. **Reducer**: `HandoffState` gains `from: Actor` (event actor), `created_at`, `accepted_at?`, `accepted_by?`,
   `status: 'pending' | 'accepted' | 'voided'`; a new `handoff.created` marks every older pending one `voided`
   (server rule `service_handoff.go:143 VoidHandoffs`). `handoff.failed/requested` cases from #146 unchanged.
5. **Header overflow** (`room/RoomHeader.tsx`): replace #146's standalone **Export** button with a `DropdownMenu`
   (`ui/dropdown-menu.tsx`, trigger `Ellipsis` icon, `aria-label="More room actions"`) holding:
   `Export JSON` (`Download`), `Post-mortem draft` (`FileText`), `Copy invite link` (`Copy`). The invite icon button
   stays. Export: `exportRoom()` as in #146; on `403 not_allowed` toast
   `Export needs an approver or auditor role for orders` (pipeline id from state), never the raw message.
6. **Post-mortem dialog** `room/PostmortemDialog.tsx`: fetches `GET /api/rooms/{id}/postmortem.md` (new
   `api.postmortem(roomId): Promise<string>` in `api/client.ts`, `Accept: text/markdown`), shows it in a read-only
   `<textarea>` (mono 12/16, `aria-label="Post-mortem draft (Markdown)"`, `max-h-[60vh]`), footer
   `[Copy Markdown] [Download .md] [Close]`. **No Markdown renderer** (no new dependency, no HTML from untrusted text).
   Loading: skeleton line; error: `Could not load the draft: <friendly text>` + Retry. While
   `state.postmortem?.status === 'drafting'` show a `Loader2` line "The agent is writing the closing summary…" above
   the text; the text itself is always available (events-only render).
7. **⌘K**: `RoomCommands` gains `exportJson?` ("Export room as JSON", keywords `download, audit, hash`) and
   `postmortem?` ("Post-mortem draft", keywords `markdown, retro, summary`); both absent for viewers.
8. **Closed-room header**: when `state.status === 'closed'` the facts line and the people line become
   `Closed {clockTime(closed_at)} · {durationLabel(opened_at, closed_at)} · closed by {closedBy.name}` and the row
   `[Export JSON] [Post-mortem draft]` (secondary buttons, 32 px). Hide `AgentPill` and the "X is driving" sentence
   when closed (the driver role is retired at close; F1's derived status covers open rooms). Add
   `durationLabel(startIso, endIso)` to `lib/time.ts`: `6 min`, `1 h 12 min`, `2 d 3 h`.
   The green "Resolved · <resolution>" strip (`RoomScreen.tsx:153`) gains the same two buttons at its right end
   (`ml-auto`), so a closed room offers something to do next.
9. **Reducer**: fold `postmortem.drafted` → `postmortem: {status: 'drafted', source, summary_ref?, at}` and
   `postmortem.failed` → `{status: 'failed', reason}`; `room.closed` sets `{status: 'drafting'}` when the room has
   no draft yet; `describe.ts`: "Post-mortem draft ready" / "Post-mortem draft failed".

### F2b (A5, `internal/room/**`, `internal/httpapi/**`, `cmd/server/**`)
10. **`GET /api/rooms/{id}/postmortem.md`** (`internal/httpapi/postmortem.go`): `text/markdown; charset=utf-8`,
    `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`; `?download=1` adds
    `Content-Disposition: attachment; filename="piperoom-<id>-postmortem.md"` (same filename rule as
    `export.go:80`). Access = whoever may `GET /api/rooms/{id}/events` (viewers included; tenancy 404 first) — it
    renders what the room already shows, unlike the export, which is a signed audit document. Open rooms allowed:
    heading says `Status: open (draft so far)`. Timeout: `exportTimeout` group (reads the whole log).
11. **Renderer** `internal/room/postmortem.go`: `func (s *Service) RenderPostmortem(ctx, roomID) ([]byte, error)`
    walks the log with the export's `walk` (`service_export.go:282`) and folds one pass. Sections, in order:
    `# Post-mortem: <title>` · facts table (severity, pipeline, opened/closed UTC, duration, closed by, resolution,
    room id, `export sha256` only if `?with_hash=1` — default off, it costs a second pass) ·
    `## Timeline` (table `time (UTC) | +mm:ss | what`): room opened (alert source), each `agent.hypothesis`
    ("hypothesis (medium): …"), `hypothesis.verdict` (F9: "confirmed by Kabir"), `proposal.created`,
    `approval.given/rejected` (layer, actor, `identity.grant`), `action.executed` (step N ok/failed), hand-off
    requested/created/accepted/failed, `[verification]` agent messages, `room.closed` ·
    `## Root cause` (final hypothesis + confidence + verdicts; `summary.why`; resolution) ·
    `## What was done` (per proposal: title, steps `action · layer · description`, params as a fenced
    `json` block compacted, approvals `layer — name (grant, method) at`, outcome) ·
    `## Verification` (every `agent.message` starting with `[verification] `, else "No verification recorded") ·
    `## Still open` · `## Risks` · `## Ruled out` (from the draft's summary; "none recorded" when empty) ·
    `## Agent` (steps, tool calls, failed tool calls, first hypothesis after N s, model calls, tokens, cost `$x.xx`
    or `not priced`) · footer `Drafted by Piperoom <version> from <N> events; summary source: handoff h_… | model | none`.
12. **Markdown safety** (`postmortem_escape.go`): every string from an event goes through `mdText(s)`:
    `sanitizeAgentText` → cut to 500 bytes (`truncateDetail`) → collapse `\r\n` → escape `\ ` `` ` `` `*` `_` `#` `>`
    `|` `[` `]` `<` with a backslash, and strip leading `-`, `+`, digits+`.` so no line can become a heading, quote,
    list, table cell or link. Ids go in code spans after removing backticks. Fenced blocks use a fence longer than any
    backtick run in the content. Table-driven test with an injection corpus (`# heading`, `](http://x)`, `<script>`,
    `|cell|`, U+2028, NUL, 10 kB string).
13. **`postmortem.drafted` at close** (`service_room.go` close path, after the `onRoomClosed` hook, outside `s.mu`):
    `s.draftPostmortem(roomID)` in a goroutine on the service ctx (counted by `Close`'s wait group).
    Rule: pick the newest `handoff.created` whose seq is **after** the last `proposal.created`, `action.executed`
    and `agent.hypothesis` → `source: "handoff"`, `summary_ref: h_…`, no model call. Else, if `Deps.WritePostmortem`
    is set and `PIPEROOM_POSTMORTEM_MODEL` is `auto` → call it with the room events (3 min timeout, like
    `handoffSummaryTimeout`) → `source: "model"`, `summary` sanitized with `sanitizeHandoff`, `violations` from the
    writer. Else → `source: "events"`. A writer error → `postmortem.failed {reason}` with the hand-off's fixed reason
    codes (`summary_failed` | `internal_error`), never error text. Written by `systemActor`, appended after
    `room.closed` like `shadow.revealed` (`shadow.go`); the Slack mirror ignores it (it stops at `room.closed`).
    Shadow rooms: no draft (their log has no agent events). Idempotent across replicas: only the replica that
    closed the room drafts; on restart a closed room with neither event and `closed_at` < 10 min ago gets the
    events-only draft from `Load` (no model call after a restart). Usage of the model call goes through the
    existing hand-off usage relay (`cmd/server/main.go:263 usageRelay`) with role `postmortem`.
14. **Snapshot**: fold both events into unexported `postmortem` state; no JSON change beyond
    `postmortem: {status, source?, reason?}` on `RoomSnapshot` (api.md below).

### F2c (A4, `internal/agent/**`)
15. **`WritePostmortem`** (`internal/agent/postmortem.go`): `func (e *Endpoint) WritePostmortem(ctx, roomEvents,
    opts) (PostmortemDraft, error)` and plain `WritePostmortem` for the CLI. Same request shape as `writeHandoff`
    (`handoff.go:25-72`): `renderRoomEvents` input, JSON schema = `handoffSchema()`, `handoffMaxTokens`, adaptive
    thinking, `postmortemPrompt()` = the hand-off prompt reworded for a closed room ("the incident is over; `done`
    lists what was executed and verified; `open` lists follow-ups and action items; `risks` lists what is still
    off, e.g. compatibility set to NONE"). Then run `CheckHandoff(roomEvents, summary)` (`handoff_check.go:79`):
    violations of `unknown_identifier`, `unknown_number`, `unknown_reference` are returned as
    `PostmortemDraft.Violations` (rule names, `field`, no detail text that quotes the log); `empty_field` and the
    length rules make it an error (the draft is dropped, `postmortem.failed summary_failed`). Grounding is the
    product rule: names and numbers must appear in cited steps or they are marked, never silently shipped.
    `postmortemModel()` = `handoffModel()`; a refusal or empty text is an error. Usage event role
    `UsageRolePostmortem` through the same `OnUsage` hook as the hand-off. `cmd/investigate`: `--postmortem`
    flag prints the draft after a run (optional, S).

**Decision: LLM at close, or deterministic render?** Recommendation: **both, deterministic first.** The Markdown
is always a deterministic render of the log (timeline, actions, approvers, verification, cost) — it cannot fail,
costs nothing, and is available while the room is still open. The only part a render cannot produce is prose
(why, ruled out, open items, risks). When the room already has a fresh hand-off summary, reuse it (zero cost, zero
latency, already grounded by F3's wiring of `CheckHandoff`). Only a room that never handed off (most short
incidents) gets one hand-off-sized model call (≈ 4 k output tokens, ≤ 3 min), **after** `room.closed`, so closing
never waits on the model and a model failure degrades to the events-only draft plus a `postmortem.failed` line.
Pure-LLM-at-close would couple the close to cost, latency and a failure mode; pure-deterministic would leave the
"why" empty for exactly the rooms that need a post-mortem most. `PIPEROOM_POSTMORTEM_MODEL=off` gives pilots the
pure-deterministic mode.

## Out of scope
Posting the post-mortem to Slack or Confluence (C12 follow-up); editing the draft in place; impact numbers
(records delayed/lost); the label form on close (F8); hand-off read receipts and observe mode (UX critique C);
`agent.verification` typed event (F4 — F2 parses `[verification] ` messages until it lands).

## Contract (F0 carries these)

`internal/events/events.go`:
```go
	// PostmortemDrafted says a closing draft exists for a closed room; the
	// Markdown itself is rendered on request by GET /api/rooms/{id}/postmortem.md
	// and never stored. Written once by the system actor after room.closed (not
	// in shadow rooms). PostmortemFailed says the model pass failed; the
	// events-only draft still renders. Never written for an open room.
	PostmortemDrafted Type = "postmortem.drafted"
	PostmortemFailed  Type = "postmortem.failed"

	UsageRolePostmortem = "postmortem" // the closing-summary request (F2c)

// PostmortemDraftedPayload names where the draft's prose sections come from.
// Source "handoff": the summary of hand-off SummaryRef, written after the last
// proposal, action and hypothesis. "model": Summary holds a fresh closing
// summary (HandoffSummary shape, sanitized like a hand-off) and Violations the
// grounding rules it failed (rule names of internal/agent CheckHandoff, no
// text). "events": no prose; the render uses the final hypothesis and the
// resolution only.
type PostmortemDraftedPayload struct {
	Source     string          `json:"source"` // "handoff" | "model" | "events"
	SummaryRef string          `json:"summary_ref,omitempty"`
	Summary    *HandoffSummary `json:"summary,omitempty"`
	Violations []string        `json:"violations,omitempty"`
}

// PostmortemFailedPayload: Reason is a fixed code (HandoffFailedSummary or
// HandoffFailedInternal), never error text.
type PostmortemFailedPayload struct {
	Reason string `json:"reason"`
}
```
`internal/agent/contract.go`:
```go
// PostmortemDraft is a closing summary plus the grounding rules it failed.
type PostmortemDraft struct {
	Summary    events.HandoffSummary
	Violations []string // CheckHandoff rule names, e.g. "unknown_number"
}

// PostmortemWriter is implemented by A4 in postmortem.go:
//
//	func WritePostmortem(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error)
//
// One model call over a closed room's log, grounded like a hand-off.
type PostmortemWriter func(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error)
```
`internal/config/config.go`: `PIPEROOM_POSTMORTEM_MODEL` = `auto` (default: model pass only when no fresh
hand-off summary exists) | `off` (events-only). Any other value refuses start.

`docs/api.md` — REST table row:
```md
| GET | `/api/rooms/{id}/postmortem.md` | – | `text/markdown` | F2 (server F2b, web F2a). The room rendered as a post-mortem draft: timeline, root cause, actions + approvers, verification, open items, risks, agent cost; see `docs/design/postmortem.md` for the section list. Same access as `GET /api/rooms/{id}/events` (viewers included; tenancy `404 not_found`). Open rooms answer the draft so far. `?download=1` adds `Content-Disposition: attachment`. Every string from the log is Markdown-escaped; clients show it as text, never as HTML. Own timeout like the export |
```
`RoomSnapshot` gains `"postmortem": {"status": "none" | "drafting" | "drafted" | "failed", "source"?: "handoff"|"model"|"events", "reason"?: "summary_failed"|"internal_error"}` (`drafting` from `room.closed` until one of the two events, at most 3 min).
Events section: `postmortem.drafted` / `postmortem.failed` payloads as above, "written after `room.closed` by the system actor, like `shadow.revealed`; clients must accept events after `room.closed`".

## Ownership and files
| File | Owner | Change |
|---|---|---|
| `web/src/components/room/HandoffCard.tsx` (+ `.test.tsx`) | A6 | new |
| `web/src/components/room/PostmortemDialog.tsx` (+ `.test.tsx`) | A6 | new |
| `web/src/components/HandoffDialog.tsx`, `RoomScreen.tsx`, `room/RoomHeader.tsx`, `shell/CommandMenu.tsx`, `shell/context.ts` | A6 | modified |
| `web/src/state/{types,reducer,describe}.ts`, `api/{client,types}.ts`, `lib/time.ts` | A6 | modified |
| `internal/room/postmortem.go`, `postmortem_escape.go`, `postmortem_test.go`, `postmortem_golden_test.go` | A5 | new |
| `internal/room/service_room.go` (close hook), `service.go` (`Deps.WritePostmortem`), `snapshot.go` | A5 | modified |
| `internal/httpapi/postmortem.go` (+ `_test.go`), `server.go` (route) | A5 | modified |
| `cmd/server/main.go` (wire `ep.WritePostmortem`, `demo.WritePostmortem`), `internal/room/demo/demo.go` | A5 | modified |
| `internal/agent/postmortem.go`, `postmortem_test.go`, `prompt.go` (`postmortemPrompt`), `endpoint.go` | A4 | new/modified |
| `cmd/investigate/main.go` (`--postmortem`) | A4 | modified (optional) |
| `docs/design/postmortem.md` | leads | new (the section list of item 11, copied from this file) |

## Dependencies and merge order
F0 (contract above) → F2c and F2b in parallel (F2b compiles against the contract with a fake writer in
`_test.go`; `cmd/server` wires `ep.WritePostmortem` only once F2c merged — until then `Deps.WritePostmortem` nil →
`source: "events"`) → F2a (fixture events for `postmortem.drafted` in `web/src/fixtures/schema-change.json`, so the
web PR is testable before F2b). F2a must rebase on #146 and #125. F9b's `hypothesis.verdict` is rendered in the
timeline when present; F2b tolerates its absence. F3 wires `CheckHandoff` into the hand-off path; F2c calls it
directly and does not depend on F3.

## Design details
- **Sequence at close**: `POST /close` → `room.closed` (driver) → 200 → hook → goroutine: read log → pick source →
  (model ≤ 3 min) → `postmortem.drafted` (system) or `postmortem.failed`. Web: Resolved strip appears at `room.closed`,
  header flips to the closed layout, `postmortem.status = drafting`; the dialog can be opened at once (events-only
  text), and re-fetches when `drafted` arrives (effect on `state.postmortem?.status`).
- **Untrusted text**: hand-off strings are already sanitized and capped at store time (`service_handoff.go:23`);
  the renderer escapes again for Markdown; the web shows Markdown as text. Agent/tool/action text in the timeline is
  truncated to 200 bytes per line; `action.executed.output` is never included whole (first line, 200 bytes).
- **Replicas**: the close is a checked append; the drafting goroutine runs on the closing replica only. `Load` on a
  restart finds closed rooms (< 10 min, no draft) and writes the events-only draft, so a crash mid-draft leaves no
  room without one.
- **a11y**: cards are `section aria-labelledby`; collapse buttons `aria-expanded`; the dialog traps focus and
  restores it (`useRestoreFocus`); the Resolved strip is `role="status"`; copy actions announce "Copied" via the
  existing toast. Light/dark via tokens only (`bg-card`, `border-border`, `text-subtle-foreground`). Mobile: the
  overflow menu is the only header action besides Invite below 640 px; the closed row wraps.
- **Failure modes**: `GET postmortem.md` on a room over the export cap answers `413 export_too_large` (same as
  export); model refusal → `postmortem.failed summary_failed`; store error on the system append → log at Warn, the
  GET still renders (status stays `drafting` ≤ 3 min, then the web shows the draft without the line).

## Done when
- [ ] Priya sees the full summary under the banner before pressing Accept; after accept the same card is pinned above
      the hypothesis for Arjun, Priya and Kabir, and survives reload (folded from the log).
- [ ] A second hand-off pins a second card; the first collapses and reads "Shift 1 … accepted 10:05".
- [ ] Header overflow and ⌘K list Export JSON and Post-mortem draft; viewers see neither; a driver without a layer
      gets the friendly 403 toast on export.
- [ ] Closing a demo room shows `Closed 06:12 · 6 min · closed by Priya`, no driver line, no agent pill, and the two
      buttons on the Resolved strip; within 3 min "Post-mortem draft ready" appears in the activity rail.
- [ ] `curl -H 'X-User: kabir' /api/rooms/{id}/postmortem.md` returns Markdown whose Timeline has one row per
      hypothesis/proposal/approval/action/hand-off/close, whose approvals name `user:priya`, and whose injected
      `# heading` from a comment arrives escaped.
- [ ] `postmortem.drafted.source` is `handoff` for a room that handed off after its fix, `model` for one that did not
      (with `PIPEROOM_POSTMORTEM_MODEL=auto` and an endpoint), `events` with `off`.
- [ ] `go test ./...`, `go vet`, `npm run build && npm run lint && npm test` pass; no new dependency.

## Tests
- Vitest: `HandoffCard.test.tsx` (pending/accepted/voided badges, collapse persists, no HTML from `<b>` in text),
  `HandoffDialog.test.tsx` (banner + inline card + Accept), `RoomHeader.test.tsx` (closed layout; no pill; overflow
  items by role), `PostmortemDialog.test.tsx` (loading/error/retry, copy, download blob name, re-fetch on `drafted`),
  `reducer.test.ts` (`postmortem.*` fold; `handoff.created` voids older pending), `RoomScreen.commands.test.tsx`.
- Go: `internal/room/postmortem_golden_test.go` (memstore, fake clock, the schema-change demo log → golden `.md`),
  `postmortem_escape_test.go` (injection corpus), `postmortem_source_test.go` (handoff-fresh / stale / none / off /
  writer error → failed; restart fallback), `internal/httpapi/postmortem_test.go` (200 member, 200 viewer, 404
  tenant, 413, `?download=1` header), `internal/agent/postmortem_test.go` (scripted model: valid draft; violations
  returned; empty `why` → error; refusal → error; one usage event with role `postmortem`).

## Effort
F2a **M** (two PRs: `a6/handoff-card` ≈ 450 lines, `a6/postmortem-export` ≈ 450 lines) · F2b **M** (one PR ≈ 550
lines; golden test is most of it) · F2c **S** (≈ 250 lines).
