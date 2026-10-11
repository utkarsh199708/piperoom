# C13 — Audit the agent's prompts and tool descriptions (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-12 00:30 IST):** **delivered** as PR #12 (`docs/audits/prompt-audit.md`), approved by
> both leads; findings are A4/A3/A5/A6 tasks (`a4/prompt-safety`, `a4/prompt-guidance`). Do **not** relaunch
> this file. Follow-up idea from the drafting critic for A4 (not C13): Opus 5.5 returns between-tool notes as
> `thinking` blocks — consider `display: updates` and mapping them to `agent.message`.


Read `AGENTS.md`, `docs/collab.md`, `tasks/A4-agent-loop.md` (§4–§5 say what the prompt must cover), `docs/security/findings.md`
and `docs/api.md` first. You own `docs/audits/**` only. **No code changes**: you write a report and a proposed diff. A4 applies the
`internal/agent/**` hunks the leads accept; A2/A3 apply hunks for their own folders.

No Docker, no dev stack, no `ANTHROPIC_API_KEY` in the cloud: you cannot run the model. Every removal you propose is a hypothesis;
say how to test it once evals can run (C6's eval harness is in an open PR).

If your environment has the `/claude-api` skill, run `/claude-api prompt-audit` and follow its method (inventory, `git blame`
provenance, pattern groups, keep list, report + diff contract). Target model: `claude-opus-5-5` (`internal/agent/options.go:4`).
Without the skill, follow the Work list below.

## Goal
The first live run with real tools and real engineers is close. Find what in the prompts, tool descriptions and request settings
will make Claude Opus 5.5 do the wrong thing in a room — miss evidence, go silent, stop early, follow injected text, or propose
writes outside the room's pipeline — and propose exact fixes.

## Scope (read every line)
- `internal/agent/prompt.go`: system prompt, hand-off prompt, `firstUserMessage`, `formatHumanInput`, `wrapUntrusted`, `renderRoomEvents`.
- `internal/agent/run.go`: `request()` (model, effort, adaptive thinking, `ToolChoice` auto, compaction beta `compact-2026-01-12`,
  cache breakpoints), `splitContent`, `runReadTool`, `wrapToolOutput`, stop-reason handling.
- `internal/agent/toolparams.go` (strict tools, injected `why` property), `tools_room.go` (`update_hypothesis`, `propose_fix`,
  `post_message`), `options.go`, `model.go`, `handoff.go`.
- Every `Description()` / `InputSchema()` in `internal/tools/dbtools/*.go` (8 tools) and `internal/tools/kafkatools/*.go` (10 tools),
  and every `Describe()` / `ParamsSchema()` in `internal/actions/kafka_actions.go` (4 actions, listed inside `propose_fix`).
- Read only: contract files `internal/tools/tool.go`, `internal/actions/action.go`, `internal/agent/contract.go`.

## Check against (cite these in findings; all checked 2026-10-11)
- https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/claude-prompting-best-practices
- https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/prompting-claude-opus-5-5 (calibrate effort, unattended
  agentic runs, user-facing progress updates, mark pasted text in user messages)
- https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools#best-practices-for-tool-definitions
- https://platform.claude.com/docs/en/build-with-claude/thinking-steering-and-cost and
  https://platform.claude.com/docs/en/build-with-claude/effort#recommended-effort-levels-for-claude-opus-5-5
- https://platform.claude.com/docs/en/build-with-claude/prompt-caching#what-invalidates-the-cache and
  https://platform.claude.com/docs/en/build-with-claude/preserved-thinking
- https://platform.claude.com/docs/en/build-with-claude/refusals-and-fallback#keep-reasoning-in-thinking-blocks
- https://platform.claude.com/docs/en/test-and-evaluate/strengthen-guardrails/mitigate-jailbreaks#indirect-prompt-injection
- https://platform.claude.com/docs/en/build-with-claude/compaction

If a page cannot be fetched, say so in the report. Do not quote docs from memory.

## Work
1. Inventory every text that reaches the model (system prompt, first user message, human-input lines, the `tool_result` wrapper,
   21 tool descriptions with their parameter descriptions, the 4 action descriptions, the hand-off prompt) and the request settings.
   Write the `main` commit SHA you audited at the top of the report.
2. Audit each item for: outdated patterns (CAPS / ALWAYS / NEVER shouting, numbered step scripts for judgment work, prohibitions
   against failures that cannot happen, format text that structured outputs already enforce); missing context the agent needs;
   ambiguous or overlapping tools; injection risks; cache-breaking or history-editing content; anything that could lead to
   out-of-scope writes. Keep context, reasons and tool contract detail: the audit is about fit, not length.
3. Confirm or reject each lead below (found on `8e326a3`; line numbers may have moved). A confirmed lead becomes a finding;
   a wrong one goes under "Checked, no issue" with one line why.
   - a. **Step ids never reach the model.** The prompt asks for evidence "by step id" (`prompt.go:20`) and
     `update_hypothesis.based_on_steps` needs them (`tools_room.go:58`), but `runReadTool` returns only
     `wrapToolOutput(u.name, summary)` (`run.go:567-571`); the id from `newStepID()` goes only to events.
   - b. **Silent room.** On Opus 5.5, notes between tool calls come back as `thinking` blocks with empty text at the default
     `display`; `splitContent` (`run.go:415`) reads only `text` blocks. Check `display: "updates"`
     (`anthropic.BetaThinkingConfigAdaptiveDisplayUpdates`, beta `anthropic.AnthropicBetaThinkingDisplayUpdates2026_08_18`)
     and how those notes become `agent.message` events.
   - c. **Early idle.** A text-only `end_turn` makes the runner wait for a human (`run.go:203-213`), and Opus 5.5 may end a turn
     with a progress report. Say what the prompt should state about when to end a turn. This is a human-in-the-loop app:
     do not paste the doc's unattended-run block.
   - d. **No clock.** `pg_recent_ddl` (`dbtools/pg.go:45-48`) and `kafka_find_by_key` (`kafkatools/kafka_tools.go:183-185`)
     require `since`, but no prompt text gives the alert time or the current time, and `Alert` has no time field (`contract.go:49-56`).
   - e. **Prompt names things that do not exist.** "Check recent DDL and deploys" (`prompt.go:19`; no deploy tool is registered)
     and "Never propose dropping a replication slot or deleting data" (`prompt.go:27`; no such action in `actions.Register`).
     Both are required by `tasks/A4-agent-loop.md` §5, so mark any change "needs lead decision". A2 is adding actions now.
   - f. **Untrusted text placement.** Comments and approval/action results go in plain user text blocks wrapped in `<untrusted>`
     (`run.go:300-306`); the alert title and pipeline map are not wrapped (`prompt.go:55-78`); `Actor.Name` is not escaped in
     `formatHumanInput` (`prompt.go:100-132`), so a display name could fake "— driver]". Compare with the injection guide and
     "Mark pasted text in user messages". Cross-reference F3, F9 and F10 in `docs/security/findings.md`; do not repeat them.
   - g. **`why` property** (`toolparams.go:13`) on every read tool: check it against "Ask for an explanation, not reasoning" and
     the `reasoning_extraction` refusal guidance.
   - h. **"One step per tool call"** (`prompt.go:40`) vs. read tools that run in parallel (`maxParallelTools = 4`, `run.go:22`).
   - i. **Tool descriptions.** Most are one sentence (the guide asks for 3–4+ sentences, when not to use, what is not returned).
     `pipeline_id` says "Pipeline id from owners.yaml", which the model never sees. Overlaps: `pipeline_map` vs. the map in the
     first message; `trace_record` vs. `kafka_find_by_key` / `pg_row` / `warehouse_row`; `connect_restart_task` vs.
     `connect_restart_connector`. Action descriptions do not say a step aimed outside the room's pipeline is refused
     (`actions.Scoped`, F10).
   - j. **Request settings.** `maxTokens = 16000` (`run.go:24`) and `handoffMaxTokens = 4096` (`handoff.go:15`) while thinking is
     always on; default effort `high` (`options.go:8`, also `contract.go:71`) vs. the Opus 5.5 "start at medium and sweep" advice;
     no refusal fallback (`Fallbacks: anthropic.BetaFallbacksParamOfDefault()` with beta
     `anthropic.AnthropicBetaServerSideFallback2026_07_01`, Claude API only); compaction without
     `BetaCompact20260112EditParam.Instructions`: does the default summary keep step ids, proposal ids, comment ids and steers?
   - k. **Hand-off prompt** (`prompt.go:47`) lists the JSON fields that `output_config.format` already enforces (`handoff.go:41`).
4. Write `docs/audits/agent-prompt-audit.md` (aim for 400–900 lines):
   - top: base SHA, scope, target model, what you could not check (no API key, pages you could not fetch);
   - summary: the 3–5 findings most likely to hurt the first live run;
   - findings table, one row each: id (P1…), `file:line`, quoted text, issue, why (doc URL or repo fact), severity
     (high/medium/low = harm on the first live run), confidence (high/medium/low as in the prompt-audit method), action
     (`remove` / `rewrite` / `add` / `move` / `replace-with-API-feature` / `flag`), proposed replacement text, owner (A2/A3/A4),
     how to test it once evals can run;
   - "Checked, no issue": what you looked at and found fine (for example cache prefix, append-only history, strict schemas);
   - "Contract change requests": exact proposed code for any contract-file change (for example a time on `Alert`, the effort
     default). These never go in the diff;
   - proposed diff: unified ` ```diff ` blocks against the base SHA, one finding per hunk, grouped by owner (A4 `internal/agent/**`,
     A3 `internal/tools/dbtools/**`, A2 `internal/tools/kafkatools/**` and `internal/actions/**`). Include the test hunks each change
     needs (`prompt_test.go` checks the prompt text and `formatHumanInput` output; `round2_test.go` checks `wrapToolOutput`).
     Rewrite the system prompt as calm prose with reasons, under ~1200 words, with no dates, room ids or user names (A4 §5).
5. Only high- and medium-confidence fixes go in the diff. Low-confidence items stay in the table as `flag`.

## Tests
No Go code changes: `git diff --stat main` shows only `docs/audits/**`. Copy the diff blocks to a file outside the repo and run
`git apply --check` against the base SHA. If Go is available, apply them in a throwaway copy of the repo outside your working
tree, run `go build ./...`, `go vet ./internal/agent/... ./internal/tools/... ./internal/actions/...` and
`go test ./internal/agent/... ./internal/tools/... ./internal/actions/...`, paste the results in the PR, then delete the copy.
Every Go symbol in the diff must pass `go doc` (AGENTS.md rule 9).

## Done when
One PR `[C13] Agent prompt audit` from your `claude/...` branch changes only `docs/audits/agent-prompt-audit.md`. Every lead a–k
is a finding or a "no issue" line. Every finding has `file:line`, a cited reason, a severity and an owner. The diff applies to the
base SHA and compiles. Contract changes are listed separately, not in the diff.
