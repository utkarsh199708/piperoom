# Daily reporter (Opus 5.5): what each squad did today, how it was tested, whether it broke anything, and whether the right agent did it

Runs once a day. Output: one comment on the pinned **Daily reports** issue (#255) and the report page republished at
https://claude.ai/artifact/1HWHJXz1Y4bzw3MRt45f7J (read it with `action: read` first, then publish to the same `url`).

## 1. Collect

Window: from the `until` of the newest `[REPORT]` comment on #255 (else 24 h ago) to now.

- **PRs** merged in the window, and PRs opened or updated in the window that are still open. For each: title tags
  (`[#N][lane]` or a wave tag), lane (title, else `routing.yaml` by changed files), files changed with additions and
  deletions, the `## How I tested` section of the body, review rounds (reviews with `CHANGES_REQUESTED` plus
  "round N" comments), check runs on the head SHA (job names and conclusions), and for a merged PR the check runs on
  its merge commit on `main`.
- **Issues** opened and closed in the window by label (`type:bug`, `type:part`, `type:feature`), with `sev:`,
  `squad:`, `lane:`, `model:`.
- **QA findings vs. PRs:** for each bug filed in the window, the PRs of the last 3 days that touched the file in its
  `Where` field.
- **evals-live:** workflow runs in the window and their pass or regression summary (BRAIN merges).
- **End-of-day health:** on `origin/main` run `go build ./... && go vet ./... && go test ./...` and, in `web/`,
  `npm ci && npm run lint && npm run build && npx vitest run`. Record pass or fail and the first failing package.

## 2. Judge each item

**tests** — `unit`, `integration`, `web`, `ci`: each `pass | fail | none | claimed`. `claimed` means the PR body says
it ran but no CI job or pasted output shows it. `evidence`: one line quoting the command or CI job name.

**regression** — `ci_main`: `green | red | n/a` (merge commit on main); `qa_issues`: numbers of bugs filed whose
`Where` is in this PR's files; `evals`: `pass | regress | n/a`. `verdict`: `clean` (green, no qa_issues, no regress) |
`regressed` (name the issues or the red job) | `unknown` (no CI ran).

**assignment** —
- `complexity`: `high` when any of: prompt or system text (`internal/agent/prompt*`, evals judge text), security
  (`internal/auth`, `redact`, `injectscan`, `actions`, `netsec`), concurrency (`internal/room`, `internal/ws`, executor,
  leases), SQL or `migrations/`, contract-adjacent types, more than 400 changed lines, 3 or more review rounds.
  `medium`: 150–400 lines, two packages, or a new tool or route. `low`: otherwise (docs, copy, small fixes, tests only).
- `model_used`: the ticket's `model:` label, else the task file's model line, else `unknown`.
- `verdict`: `right` (high→opus, low→sonnet, medium→either) | `under` (high on sonnet, or sonnet with 3+ rounds) |
  `over` (low on opus) | `unknown`.
- `lane_right`: every changed file is inside the lane's folders (`routing.yaml`, `AGENTS.md` §2); else list the
  foreign files. A PR with no ticket or lane tag is a flag on its own.

## 3. Write

The comment, in this order:
1. `[REPORT] <date>` and one line: merged N · opened N · bugs filed N · bugs closed N · main health pass/fail.
2. One section per squad (PIPE, BRAIN, ROOM, WEB, PLATFORM, TRUST, LEADS) with a table:
   `item | lane · model | status | tests | regression | assignment`.
3. **Flags**: anything red, `under` or `over`, a lane breach, `claimed` tests, a PR without a ticket, a `sev:critical`
   bug open for more than one dispatcher run.
4. A fenced `json` block in the schema below (the page is built from it).

Then rebuild the page: save the `json` blocks of the newest 7 `[REPORT]` comments as `reports/<date>.json`, run
`node docs/agents/tracker/report-build.js out.html reports/*.json`, and publish `out.html` to the report URL.

## JSON schema

```json
{"date":"2026-10-15","until":"2026-10-15T18:36:00Z",
 "summary":{"merged":0,"opened":0,"bugs_filed":0,"bugs_closed":0,"main_health":"pass|fail","main_health_note":""},
 "squads":[{"squad":"WEB","items":[
   {"id":"#245","kind":"part|bug|feature|chore","title":"","lane":"A6","model":"opus|sonnet|unknown",
    "status":"merged|in-review|in-progress|blocked|open",
    "tests":{"unit":"pass","integration":"none","web":"pass","ci":"pass","evidence":""},
    "regression":{"ci_main":"green","qa_issues":[],"evals":"n/a","verdict":"clean"},
    "assignment":{"complexity":"high","model_used":"opus","verdict":"right","lane_right":true,"foreign_files":[],"why":""}}]}],
 "flags":["..."]}
```
