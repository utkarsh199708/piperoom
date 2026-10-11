# [C13] Prompt audit before the first live run (report only)

## What
Adds `docs/audits/prompt-audit.md`: an audit of every text the agent model reads (system prompt, hand-off prompt, opening user turn, human-input formatting, hand-off renderer, tool descriptions, request assembly). 11 findings (P1-P11), each with file, problem, fix and owner, plus a suggested order. No code changed.

## Why
The agent's prompts have not met a live model yet. Three issues should be fixed first: P1 (step ids are cited in the prompt but never sent to the model), P2 (alert text is unwrapped in a trusted position) and P3 (the hand-off keeps the oldest events and drops the newest).

## How I tested
Report only. I read `internal/agent/{prompt,run,tools_room,handoff,model,options,toolparams,contract}.go`, `cmd/investigate/{main,inbox}.go`, the tool and action descriptions, and `docs/security/findings.md`. I checked P1 by tracing `runReadTool`: the step id goes to events only, not to the tool result. No live model calls; no Go build needed.

## Not done / known gaps
- `tasks/C13-prompt-audit.md` is not on `main`, so "Done when" could not be checked. Scope was taken from the task title and the AGENTS.md ownership line.
- No live model run, so the proposed wording is untested (C6 evals or one `cmd/investigate` run would show it).
- The compaction parameter in P7 was not checked with `go doc`.
- A5 room service and C5 alert ingest are not on `main`; P2 and P4 describe the contract they must meet.

## New dependencies
None.

## Contract change request
None. P1 suggests a change inside `internal/agent` (A4), not to a contract file.
