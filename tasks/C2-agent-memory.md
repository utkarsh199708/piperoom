# C2 — Long-room memory for the agent (Claude cloud agent, Sonnet)

Read `AGENTS.md`, `docs/collab.md`, `tasks/A4-agent-loop.md` first. For this task you own
`internal/agent/**` except `contract.go` (A4's Qoder agent works only in `cmd/investigate/**` meanwhile).

No Docker stack in the cloud; unit tests with the scripted fake model. Never call the real API without
`ANTHROPIC_API_KEY` in the environment (tests that need it use build tag `claude`).

## Goal
A room can stay open for hours (many steers, tool calls) without the agent dying on
`model_context_window_exceeded`, and without breaking the API's append-only history rule.

## Work
1. **Server-side compaction** for `claude-opus-5-5`: beta `compact-2026-01-12`. Read the official docs before
   coding; verify every `anthropic-sdk-go` symbol with `go doc` (AGENTS.md rule 9). Append the **full**
   `response.content` (including compaction blocks) every turn — never only the text.
2. Keep history append-only: never edit or drop earlier messages yourself.
3. `StopReasonModelContextWindowExceeded` (if it still happens) → `agent.status` `error` + a clear message, then idle
   and wait for human input (same pattern as refusal) — never end the room.
4. Lead 1 follow-up from A4 review: short backoff (e.g. 250 ms → 5 s) in `waitForInput` when `Inbox.Drain` keeps
   failing while `Wait` returns at once.
5. Tests: fake model returns a compaction block → next request carries it; context-exceeded → idle not exit;
   drain-error backoff (no hot loop; use a fake clock).

## Done when
Unit tests prove the above; PR explains the beta header and how compaction state is persisted between turns.
