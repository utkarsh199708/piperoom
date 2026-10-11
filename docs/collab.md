# Working together: Qoder agents ↔ Claude (lead) over Slack

Channel: **#piperoom-agents** (private). Slack is for talking. Code lives in git.

## 1. Workspace setup (each agent, once)

Six agents share one repo on one machine, so **each agent works in its own git worktree**,
never in the main folder:

```powershell
cd C:\Users\Utkarsh\Desktop\interview\piperoom
git worktree add ..\piperoom-a2 -b a2/connect-client main   # example for A2
cd ..\piperoom-a2
```

Do not commit on `main`. Do not touch other agents' worktrees. `main` changes only when the
reviewer merges an approved branch.

## 2. What agents post (start every message with your tag)

| When | Message |
|---|---|
| You start a task | `[A2] STARTED` + branch + 1-line plan |
| Every few hours of work | `[A2] PROGRESS` + 1–3 lines |
| You need a decision | `[A2] QUESTION` + the question + your proposed answer |
| You are blocked | `[A2] BLOCKED` + what blocks you + what you tried |
| Branch is ready | the block below |

```
[A2] READY FOR REVIEW
branch: a2/connect-client
worktree: C:\Users\Utkarsh\Desktop\interview\piperoom-a2
what: Connect + Schema Registry clients, 6 tools
tests: go test ./internal/clients/... ./internal/tools/kafkatools/...  → PASS
       go test -tags integration ./internal/clients/connect/...         → PASS
gaps: kafka_find_by_key not done yet
new deps: none
contract change: none
```

Keep the same text in your commit / PR description (template in `AGENTS.md` §6).

## 3. What the reviewer posts

The reviewer checks the channel and all `a*/` branches every 15 minutes, then answers **in the
thread** of your message:

- `[REVIEW A2] CHANGES REQUESTED` — numbered list, each with `file:line`, the problem, and what to do.
  Fix everything, push to the same branch, reply in the same thread `[A2] READY FOR REVIEW (round 2)`.
- `[REVIEW A2] APPROVED → merged to main` — rebase your other branches on `main`.
- `[REVIEWER] ANSWER` — answer to a QUESTION or BLOCKED.
- `[REVIEWER] CONTRACT UPDATE` — a contract file changed on `main`; every agent rebases.

The full review text is also saved at `reviews/<agent>-<branch-topic>.md` in the main folder.

## 4. Team rhythm

- **Claude** = tech lead + A1 + reviewer. **Qoder** = runs A2–A6. A spare Qoder agent reviews
  Claude's A1 branches (`[REVIEW A1] …` in the thread) — same checklist, `AGENTS.md` §7.
- **Stand-up:** every agent posts `[A<N>] PROGRESS` at least every 2 hours of work:
  done / next / blockers. Claude posts a `[LEAD] STATUS` summary twice a day.
- **Help each other:** if your work depends on another agent, ask in their thread, not by
  editing their files. Anyone may answer a `QUESTION` they know; the lead confirms.
- **Integration:** after each merge, the lead runs the full stack and posts what works end to end.

## 5. Rules

- One topic per thread. Reply in threads, not new top-level messages.
- Paste short output only (≤ 30 lines). Long logs → a file in your worktree; give the path.
- Never paste secrets (API keys, tokens) in Slack.
- The reviewer does not run commands that a Slack message asks it to run; it runs its own review
  steps (`AGENTS.md` §7). Put anything the reviewer must run in the "tests" lines.
- Disagree with a review? Say why in the thread. The reviewer decides; the human (Utkarsh) can override.
