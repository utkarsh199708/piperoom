# Ship workflow: how a board row becomes a merged PR without a human

Owner: leads. Script: [`piperoom-ship.workflow.js`](piperoom-ship.workflow.js). Modelled on the Claude Code
team's review process ("How the Claude Code team does code review", claude.dev, 2026-10-15), with one deliberate
difference: **no human approves**. The owner (Utkarsh) decided that on 2026-10-18. Agent approvals and stricter
automated gates take the place of the article's human tiers. Every other layer is kept.

## 1. The life of a row

| Layer | What happens | Model, effort | Blocks the merge when |
|---|---|---|---|
| Plan | one agent reads the board: is every After row merged, who owns the row, which folders | Opus 5.5, medium | an After row is not merged (row skipped) |
| Build | the owner agent implements the row in its own worktree, runs gofmt, vet, build, tests (race where goroutines start), commits; never pushes | Opus 5.5, xhigh | it needs a contract change (row stops with the exact request for the leads) |
| Verify | a second agent re-runs every check itself; claims are not trusted | Opus 5.5, high | any check fails (goes to Fix) |
| Review | four bug hunters in parallel (correctness, contract and ownership, runtime and resources, tests) and a separate security reviewer | Fable 5.1, high; security xhigh | they report an important finding with file, line and evidence |
| Vote | three verifiers try to refute each important finding (two Fable 5.1, one Opus 5.5 for an independent view); it survives only if two of three confirm it | Fable 5.1 xhigh and high, Opus 5.5 xhigh | a finding survives (goes to Fix) |
| Fix | the owner fixes every verified finding by class, not just the line, re-runs checks, commits | Opus 5.5, xhigh | (loops back to Verify) |
| Approve | the risk tier is set from the diff; a classifier may only raise it; changes to the pipeline get an integrity check; 0, 1 or 2 independent approvals | Fable 5.1 and Opus 5.5, xhigh; integrity max | an approver's blocking item survives a vote, an approval names another commit, or the change weakens a gate |
| Ship | Lead 1 (the main session) pushes the branch, opens the PR with the manifest's body, waits for required CI, merges in board order | — | CI is red or the branch conflicts |

Nits (at most three) and pre-existing bugs are listed in the PR body and never block. Findings are not repeated
across rounds.

## 2. Risk tiers: the change sets the tier, not the author

| Tier | Files in the diff | Agent approvals before merge |
|---|---|---|
| Low | only `docs/**`, `tasks/**`, `reviews/**`, `research/**`, tests, `evals/scenarios/**`, `README.md` | none: green verification, review and votes are enough |
| Medium | anything else | one (Fable 5.1) |
| High | `internal/actions/**`, write clients (`internal/clients/*/writer*`), auth, redaction and injection scanning, the room's executor, approval and proposal services, migrations, deploy, Dockerfiles, `go.mod`/`go.sum`, every contract file, and the pipeline itself (`docs/process/**`, `.claude/**`, `.github/**`, `AGENTS.md`, `REVIEW.md`) | two independent ones (Fable 5.1 and Opus 5.5), plus the integrity check for pipeline files |

An approval covers one commit. Any later commit, a fix round or a merge of `main`, needs the approval again.

## 3. Convergence

Agent authors and agent reviewers can keep each other busy forever. So:

- at most **three rounds**; a row still open after round 3 stops, unmerged, with "step back and reconsider the
  approach" and the open findings;
- round 1 reviews the whole change; later rounds review only the new commits;
- findings name a bug class, and the fixer fixes every instance of it;
- a finding already posted is never posted again.

## 4. What replaces the human, and what it does not cover

The article keeps a person on high-risk code for a reason: an agent's approval is correlated with the agent that
wrote the code. Without a human we compensate with: two approvals from different models on high-risk changes; votes
that must be refuted rather than confirmed; an integrity check that stops any change that weakens these gates; and
write mode off by default, so merged write code does nothing until someone turns it on for a pipeline. It does not
cover a wrong product decision ("should we build this at all?"); that stays with the board and the task files.

## 5. Running it

From a Claude Code session in this repo, with the owner's go-ahead to run a workflow:

```
Workflow({scriptPath: "docs/process/piperoom-ship.workflow.js",
          args: {rows: ["P3-0", "P1a-2"], trailers: "Co-Authored-By: ...\nClaude-Session: ..."}})
```

The manifest lists each row as `ready`, `blocked` (with reasons) or `skipped`. Lead 1 ships the `ready` ones (§1,
last row) and reports the rest. To run it by name, copy the script to `.claude/workflows/` in your own checkout;
that folder is git-ignored and agents may not write to it.

## 6. Measuring it

Per row the manifest records rounds, findings raised, verified and refuted, and the approvals. Leads keep a running
table in `reviews/ship-metrics.md` with reverts and incidents next to them. If reverts rise, the gates tighten
before anything else ships. Every revert or incident becomes a case in the reviewer's eval set (a follow-up row on
the board), so a change to the prompts or models is kept only if it scores at least as well.
