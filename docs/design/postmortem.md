# Post-mortem draft

Status: contract in F0 (branch `lead/f0-contracts`); server F2b (A5), model pass F2c (A4), web F2a (A6).
Task file: `tasks/F2-handoff-postmortem.md`. Route: `GET /api/rooms/{id}/postmortem.md` (`docs/api.md`).

## 1. What it is

A Markdown draft of a room as a post-mortem, rendered on request from the room log. It is never stored: the log is
the source, so the draft is always as current as the room and an open room answers "the draft so far". The only
stored fact is where the prose came from (`postmortem.drafted`), written once after `room.closed`.

## 2. Decisions (LEAD-ARCH)

| # | Decision |
|---|---|
| 1 | **Deterministic first.** Timeline, actions, approvers, verification and cost are a render of the log: it cannot fail and costs nothing. |
| 2 | **Prose reuses the hand-off.** The newest `handoff.created` whose seq is after the last `proposal.created`, `action.executed` and `agent.hypothesis` supplies why / ruled out / open / risks (`source: "handoff"`, no model call). |
| 3 | **One model call only when needed.** A closed room without such a hand-off gets one hand-off-sized call (`agent.PostmortemWriter`, 3 min, usage role `postmortem`) **after** `room.closed`, so closing never waits on the model (`source: "model"`). `PIPEROOM_POSTMORTEM_MODEL=off` skips it (`source: "events"`). |
| 4 | **Grounded like a hand-off.** The model summary runs through `agent.CheckHandoff`; `unknown_identifier`, `unknown_number`, `unknown_reference` are kept as rule names on `postmortem.drafted.violations` and marked in the draft, never silently shipped. |
| 5 | **Failure degrades.** A writer error appends `postmortem.failed {reason}` (`summary_failed` or `internal_error`, never error text); the events-only draft still renders. |
| 6 | **Not for shadow rooms.** Their log has no agent events. |
| 7 | **One writer.** Only the replica that closed the room drafts. After a restart, a closed room with neither event and `closed_at` less than 10 min ago gets the events-only draft (no model call after a restart). |

## 3. Sections, in order

1. `# Post-mortem: <title>`
2. **Facts table**: severity, pipeline, opened / closed (UTC), duration, closed by, resolution, room id; the export
   SHA-256 only with `?with_hash=1` (default off: it costs a second pass).
3. `## Timeline` — table `time (UTC) | +mm:ss | what`: room opened (alert source), each `agent.hypothesis`
   ("hypothesis (medium): …"), `hypothesis.verdict` ("confirmed by Kabir"), `proposal.created`,
   `approval.given` / `approval.rejected` (layer, actor, `identity.grant`), `action.executed` (step N ok / failed),
   hand-off requested / created / accepted / failed, verification (`agent.verification`, or `[verification] `
   agent messages before F4), `room.closed`.
4. `## Root cause` — final hypothesis, confidence and verdicts; `summary.why`; the resolution.
5. `## What was done` — per proposal: title; steps `action · layer · description`; params as a compact fenced
   `json` block; approvals `layer — name (grant, method) at`; outcome (a withdrawn proposal says so, with its
   reason). Then every fix note (`comment.added` with `kind: "fix_note"`: who, when, text). When the room has
   neither an executed step nor a fix note, the close resolution stands here instead of "No fix was proposed"
   (Q1, QA G10).
6. `## Verification` — the `agent.verification` records; for a room fixed by a person (fix notes or the
   resolution, no executed step) "Fixed outside Piperoom per the resolution / fix notes; no agent verification
   ran"; else "No verification recorded".
7. `## Still open` — from the summary's `open`; "none recorded" when empty. When a fix (an executed step or a
   fix note) came after the hand-off the summary was taken from, its items are labelled "Earlier, from the
   hand-off at HH:MM (before the fix: …)" instead of being shown as current (Q1).
8. `## Risks` — from the summary's `risks`, with the same before-the-fix label; "none recorded" when empty.
   Hand-off bookkeeping ("summary lines were truncated", "a hand-off failed") never appears in these sections:
   it goes to a footnote under the footer.
9. `## Ruled out` — from the summary's `ruled_out`; "none recorded" when empty.
10. `## Agent` — steps, tool calls, failed tool calls, first hypothesis after N s, model calls, tokens, cost `$x.xx`
    ("less than $0.01" for tiny amounts), or `not priced` when no call was priced (the built-in price table is the
    default since Q0, so this needs `PIPEROOM_MODEL_PRICES=off` or an unknown model).
11. Footer: `Drafted by Piperoom <version> from <N> events; summary source: handoff h_… | model | none`.

## 4. Markdown safety

Every string from the log is untrusted. Before it enters the draft it is sanitized like agent text, cut to 500 bytes,
has `\r\n` collapsed, and has `\` `` ` `` `*` `_` `#` `>` `|` `[` `]` `<` escaped with a backslash; leading `-`, `+` and
`<digits>.` are stripped so no line becomes a heading, quote, list, table cell or link. Ids go in code spans with
backticks removed. Fenced blocks use a fence longer than any backtick run inside. Clients show the draft as text
(a read-only textarea), never as rendered HTML. The route sends `Cache-Control: no-store` and
`X-Content-Type-Options: nosniff`.

## 5. Access

Whoever may read `GET /api/rooms/{id}/events` (viewers included; tenancy `404 not_found` first). It renders what the
room already shows, unlike the export, which is a signed audit document with its own access rule.
