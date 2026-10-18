# Ship workflow: how a board row becomes a merged PR without a human

Owner: leads. Script: [`piperoom-ship.workflow.js`](piperoom-ship.workflow.js). Lead 1's gate at merge time:
[`ship-check.mjs`](ship-check.mjs), which evaluates the script's own rules block. Modelled on the Claude Code
team's review process ("How the Claude Code team does code review", claude.dev, 2026-10-15), with one deliberate
difference: **no human approves**. The owner (Utkarsh) decided that on 2026-10-18. Agent approvals and stricter
automated gates take the place of the article's human tiers. Every other layer is kept.

## 1. The life of a row

In the script, `opus` is Opus 5.5 and `fable` is Fable 5.1.

| Stage | What happens | Model, effort | Stops or loops when |
|---|---|---|---|
| Plan | one agent reads the board on `origin/main` and the task files: are all After rows merged, who owns the row, its folders and exclusions (AGENTS.md §2), its Done-when and PR title | Opus 5.5, medium | an After row is not merged (row skipped) |
| Build | an agent acting as the row's owner works in its own worktree (branch `p/<row>` from `origin/main`). It writes the row with tests, runs gofmt, `go vet ./...`, `go build ./...`, `go test ./...`, `-race` on packages that start goroutines, and for `web/**` lint, build and `npx vitest run`. It commits and never pushes | Opus 5.5, xhigh | it needs a contract change (row blocked with the exact request for the leads), or the worktree already exists (rerun with resume) |
| Verify | a second agent trusts no claim. It computes the base (`git merge-base HEAD origin/main`), head, file list and patch id itself. It re-runs gofmt, `go vet ./...`, `go build ./...`, `go test ./...`, `go test -race -count=3` on changed packages that start goroutines, and for `web/**` `npm run lint`, `npm run build` and `npx vitest run` | Opus 5.5, high | a check fails (goes to Fix); the diff is empty, or a file breaks ownership or the contract-file rule (row blocked, §2) |
| Review | four bug hunters in parallel (correctness, contract fit and Done-when, runtime and resources, tests) and a separate security reviewer. A deleted, skipped or loosened test is always important | Fable 5.1, high; security xhigh | an important finding with file, line and evidence (goes to Vote); a reviewer fails twice (row blocked) |
| Vote | three verifiers try to refute each important finding: two Fable 5.1 and one Opus 5.5 for an independent view. A finding is dropped only when two of three refute it | Fable 5.1 xhigh and high, Opus 5.5 xhigh | a finding survives (goes to Fix) |
| Fix | the owner agent fixes every verified finding by class, not just the line, re-runs the checks and commits `<row>: review round N fixes` | Opus 5.5, xhigh | it makes no commit (row blocked); items it could not fix stay open and are listed in the manifest |
| Check | at the new head, one agent per item sent to Fix confirms that the problem and every other instance of its class are gone. A deleted or loosened test is not a fix | Fable 5.1, high | an item is not gone (it stays open for the next Fix) |
| Classify | once a round ends with nothing open, a risk classifier lists the changed files itself and reads the diff. The tier comes from both file lists together; the classifier may only raise it | Opus 5.5, high | the combined list breaks ownership or the contract-file rule (row blocked) |
| Integrity | runs when any file is a pipeline file (§2): does the change weaken a gate, now or later | Fable 5.1, max | it weakens one, or does not return (row blocked) |
| Approve | 0, 1 or 2 independent approvals of the head commit, by tier (§2). A "no" sends its blocking items to Vote, and verified ones go to Fix. A "no" with nothing verified behind it, no answer, or an approval of another commit gets one re-ask, with the refutations attached | Fable 5.1, plus Opus 5.5 at high tier; xhigh | an approver still does not approve the head after the re-ask (row blocked; the leads decide) |
| Ship | Lead 1 pushes, opens the PR, runs ship-check, keeps the branch current with `main`, waits for the required CI jobs and merges in board order (§5) | none (Lead 1) | ship-check fails, the patch id changed, or a required job fails twice |

Plan, Build and Fix are called once. Every other agent call is retried once when it returns nothing. A stage that
still fails fails closed: the row blocks, a verifier refutes nothing, a checker keeps its item open.

## 2. Risk tiers: the change sets the tier, not the author

| Tier | Files in the diff | Agent approvals of the head commit |
|---|---|---|
| Low | every file is under `docs/**`, `reviews/**` or `research/**`, or is the root `README.md`, and none is high | none: green verification, review and votes are enough |
| Medium | anything that is neither low nor high | one (Fable 5.1) |
| High | at least one file in the list below | two, from different models (Fable 5.1 and Opus 5.5), plus the integrity check when a pipeline file changed |

High-risk paths, copied from the script's `RULES-BEGIN`..`RULES-END` block (if the two differ, the script wins):

- Writes and the gates in front of them: `internal/actions/**`; `internal/clients/connect/**`,
  `internal/clients/schemareg/**`, `internal/clients/pgconv/**`, `internal/clients/netsec/**`; write clients
  (`internal/clients/*/writer*`); `internal/room/**`; `internal/owners/**`; `internal/wiring/**`; `cmd/server/**`;
  `internal/tools/scope.go`.
- Auth, storage and untrusted text: `internal/auth/**`; `internal/httpapi/auth*` and `internal/httpapi/server*`;
  `internal/alertin/**`; `internal/store/**`; `internal/redact/**`; `internal/injectscan/**`; the agent's
  untrusted-text, read-only guard and approval/prompt files (files in `internal/agent/` whose names start with
  `alerttext`, `alertevidence`, `injectflag`, `approvals`, `prompt`, `readonly` or `redact`, and `run.go`, which wraps
  untrusted tool output).
- Build and deploy: `migrations/**`; `deploy/**`; every `Dockerfile`; `go.mod` and `go.sum`; `web/package.json` and
  `web/package-lock.json`.
- Contract files (AGENTS.md §2): `internal/tools/tool.go`, `internal/events/events.go`,
  `internal/clients/clients.go`, `internal/actions/action.go`, `internal/agent/contract.go`,
  `internal/topology/graph.go`, `internal/config/config.go`, `internal/version/version.go`, `docs/api.md`.
- Pipeline files: `docs/process/**`, `.claude/**`, `.github/**`, `tasks/**` (the board and the task files'
  Done-when), `Makefile`, `.gitattributes`, the web lint, Vite, Vitest and TypeScript configs (`web/eslint.config*`,
  `web/.eslintrc*`, `web/vite.config*`, `web/vitest.config*`, `web/tsconfig*`), and any `AGENTS.md`, `CLAUDE.md` or `REVIEW.md` in any folder.

More rules:

- Tests and `evals/scenarios/**` have no tier of their own. A test takes the tier of its directory: high in a high
  path, medium elsewhere. `evals/scenarios/**` is medium: its expected labels are the eval gate's ground truth.
- `docs/api.md` and `docs/process/**` sit under `docs/` but are high. A high match always wins.
- An empty or unreadable file list is treated as high; the gate never fails open. The workflow also blocks a row
  whose diff against `origin/main` is empty.
- The tier comes from the union of the verifier's and the classifier's file lists. The classifier may raise the
  tier, never lower it. Ship-check recomputes the tier from the PR's real file list (§5).
- Ownership and contract-file violations block the row outright, at any tier. No number of approvals ships them.
  Every file must be under one of the owner's folders (AGENTS.md §2) and under none of its exclusions, and a
  contract file may change only on a leads' row. The workflow checks this after every Verify and again on the
  classifier's list; ship-check checks it on the PR.

### Approvals and updates from `main`

An approval covers the commit it names. Approvals stand while the row's own patch is unchanged: the first field of
`git diff $(git merge-base origin/main HEAD) HEAD | git patch-id --verbatim`, run in the row's worktree after
`git fetch origin main`, equals the manifest's `patchId`. A conflict resolution or any other change means rerunning
the workflow on that row with resume (`args.resume = {ROW: {worktree, branch}}`).

A clean merge of `main` keeps the patch id unless `main` changed lines right next to the row's own change. Then the
id changes, and that counts as a change. `--verbatim` (git 2.39 or later) keeps whitespace, so an indentation-only edit
to YAML or a Makefile also changes the id; `--stable` would hide it. A resumed run skips Build and runs Verify, the full review, the votes, the
classifier and the approvals again at the new head.

## 3. Convergence

Agent authors and agent reviewers can keep each other busy forever. So:

- at most **three rounds**; a row still open after round 3 stops, unmerged, with "step back and reconsider the
  approach" and the open findings;
- the first round whose checks pass reviews the whole change; later rounds review only the commits since that
  review;
- findings name a bug class, and the fixer fixes every instance of it;
- a finding already posted is never posted again, and a finding the verifiers refuted is not raised again;
- a verified finding stays open until Check confirms it is gone; a fix round that makes no commit blocks the row.

Nits (at most three per row) and pre-existing bugs are listed in the PR body and never block.

## 4. What replaces the human, and what it does not cover

The article keeps a person on high-risk code for a reason: an agent's approval is correlated with the agent that
wrote the code. Without a human we compensate with:

- **Deterministic gates first.** Path rules set the tier, and an empty file list counts as high. Ownership and the
  contract-file rule are enforced by the script, not just asked of the builder: a violation blocks the row.
  Ship-check runs the same rules on the PR's real file list.
- **A risk classifier on a second model** (Opus 5.5, high effort). It reads the diff and may only raise the tier:
  when the change can alter what is written to a customer system, who may approve or run a fix, authentication,
  what data leaves the server, or the review and merge gates, or when it deletes or weakens a test assertion.
- **Two approvals from different models** on high-risk changes, each of the head commit.
- **Votes that must refute** a finding to drop it. A reviewer that fails blocks the row; a verifier that fails
  refutes nothing.
- **No weakened tests.** Builders and fixers may not delete, skip or loosen a test, or add a longer timeout. The
  tests hunter treats any such change as important, and Check does not accept it as a fix.
- **An integrity check** (Fable 5.1, max effort) on every pipeline file: `docs/process/**`, `.claude/**`,
  `.github/**`, `tasks/**` with the board and the task files' Done-when (the workflow's own acceptance input), the
  `Makefile`, the web lint, Vite and Vitest configs, and any `AGENTS.md`, `CLAUDE.md` or `REVIEW.md`. It stops a
  change that removes, skips, loosens or bypasses a check, vote, tier rule, approval, round cap, required CI job,
  ownership rule or Done-when, or that makes this easier later.
- **Arguments that can only tighten the gates.** `maxRounds` is clamped to 1..3. The two approval models must
  differ: if `secondModel` equals `reviewModel`, the script uses the other model for the second opinion.
- **Write mode off by default**, so merged write code does nothing until someone turns it on for a pipeline.
  High-tier behaviour changes also name their off switch (§7).

It does not cover a wrong product decision ("should we build this at all?"); that stays with the board and the
task files.

## 5. Ship

Lead 1 ships each `ready` row of the manifest, one PR per row, in board order. Lead 1 runs commands and merges, and
writes no feature code. Save the row's manifest entry as a JSON file (`<row.json>`). It holds `title`, `branch`,
`worktree`, `head`, `patchId`, `tier`, `files`, `approvals` and `prBody`.

1. **Push.** In the row's worktree, check that `git rev-parse HEAD` is the manifest's `head`. Then run
   `git push -u origin <branch>`.
2. **Open the PR** against `main` with the manifest's `title` and `prBody`. GraphQL is not available from a Claude
   Code session, so use REST:
   `gh api repos/utkarsh199708/piperoom/pulls -f base=main -f head=<branch> -f title="<title>" -F body=@<body.md>`.
3. **Ship-check.** Take the file list from GitHub, never from the manifest:
   `gh api repos/utkarsh199708/piperoom/pulls/<n>/files --paginate --jq '.[] | .filename, (.previous_filename // empty)' > files.txt` (both sides of
   a rename). Compute the
   patch id in the worktree with the command in §2. Run
   `node docs/process/ship-check.mjs <row.json> <files.txt> <patch-id>`. It evaluates the script's rules block on
   the real files (tier, ownership, contract files) and compares the result and the patch id with the manifest. If
   it fails, stop: do not merge, and report the row to the leads.
4. **Keep the branch current** (the merge-queue equivalent). Before each merge the branch must contain the current
   `origin/main`. Run `git fetch origin main`. If the branch is behind, run `git merge origin/main` in the worktree.
   The merge must be conflict-free and the patch id unchanged (§2). Then push and run ship-check again on the new
   file list. If the merge conflicts, run `git merge --abort`: an agent acting as the row's owner resolves the
   conflict in the worktree, and the workflow reruns on the row with resume (§6). If the patch id changed, rerun
   with resume too.
5. **Required CI.** These jobs must be green on that exact head: Go, Go race detector, Go integration (full dev
   stack), Web, govulncheck, Deploy manifests. OIDC end-to-end is manual and not required. Re-run a failing job at
   most once. A second failure blocks the row: the failure becomes a board row, and the PR waits.
6. **Merge** in board order, with a merge commit titled `Merge PR #<n>: <title>`. A PR whose After rows have not
   merged waits, even when it is ready. If `main` moved since step 4, go back to step 4.
7. **Integration run.** After each merge-order step, Lead 1 runs the integration run as before. Task-level
   Done-when that spans rows (P2's one-browser-context e2e, P3's dev-stack scenarios) is checked only there; no
   per-row gate sees it. A failure there becomes a board row.

## 6. Running it

From a Claude Code session in this repo, with the owner's go-ahead to run a workflow:

```
Workflow({scriptPath: "docs/process/piperoom-ship.workflow.js",
          args: {rows: ["P3-0", "P1a-2"], trailers: "Co-Authored-By: ...\nClaude-Session: ..."}})
```

Each row gets a worktree `<worktreeRoot>/<row>` (default `../piperoom-ship/<row>`) on branch `p/<row>`. Rows whose
folders overlap run in separate waves, so they do not conflict at Ship.

To rerun a row on its existing worktree (after a conflict resolution or any other change), pass `resume`. Carry the
earlier manifest's `howToRevert` and `offSwitch`, so the new PR body keeps them:

```
args: {rows: ["P3-0"], resume: {"P3-0": {worktree: "<absolute path>", branch: "p/P3-0",
       howToRevert: "<from the earlier manifest>", offSwitch: "<from the earlier manifest>"}}}
```

The manifest lists each row as `ready`, `blocked` (with reasons) or `skipped`. Lead 1 ships the `ready` ones (§5)
and reports the rest. To run it by name, copy the script to `.claude/workflows/` in your own checkout; that folder
is git-ignored and agents may not write to it.

## 7. Measuring it

Per row the manifest records the tier, rounds, findings raised, verified and refuted, and the approvals. Leads keep
a running table in `reviews/ship-metrics.md` with reverts and red-main incidents next to them.

- **Pause rule.** 2 reverts or red-main incidents in the last 10 workflow merges pause the workflow. Rows go back to
  the two-lead review of AGENTS.md §7 until the leads find the cause and tighten the gates.
- **Revert first.** A red `main` after a workflow merge is reverted first, then debugged. The PR's "How to revert"
  section says what else to undo.
- **Off switch.** A high-tier change that alters behaviour names its off switch under `## Risk`; the manifest
  carries it from the builder. Example: `PIPEROOM_AUTO_APPROVE_DELAY=0` for P2b-4.
- **Eval cases.** Every revert or incident becomes a case in the reviewer's eval set (a follow-up row on the
  board), so a change to the prompts or models is kept only if it scores at least as well.
