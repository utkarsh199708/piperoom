# Dispatcher (Opus 5.5): route, prioritise, scale, report.

Each run (hourly):

## 1. Read state
`[DISPATCH STATE]` from the ops log: per squad `ready_streak`, surge lanes, parked lanes, idle counters, sessions started.

## 2. Approve features
For each `type:feature` + `status:proposed`: approve (`status:approved`, comment the reason) when it fits the vision,
has a measurable metric and names squads; else `status:rejected` with the reason. Then for each `status:approved`
without a task file, file one `squad:leads` `lane:lead-arch` `type:chore` issue: "Spec feature #N: task file +
contract PR", and move the feature to `status:specced` when LEAD-ARCH's task file is on `main`. For a specced feature,
create one sub-issue per part named in its task file (`sub_issue_write`), labelled `type:part`, `status:ready`
(or `status:blocked` with "waits on #M" when the task file orders it), `squad:`, `lane:`, `prio:` from the feature,
`model:` from the task file's model line.

## 3. Sync ticket status from PRs
For each open PR: find `Closes #N` or `[#N]` in the title; set the issue to `status:in-review`. Merged PR → close
the issue, `status:done`. A PR without a ticket reference: comment once asking for `[#<issue>][<lane>]` in the title
and file a ticket for it from the PR description (so the work is tracked). Unblock `status:blocked` parts whose
predecessor merged.

## 4. Scale (rules in `org.md` §4)
Per squad compute `ready`, `active`, `p0`. Apply scale up / scale down / park / reviewer span / ceiling (8 cloud
worker sessions, 2 open PRs per lane). A surge lane takes the next free `s<n>` suffix on its base lane and one split
from `routing.yaml` `surge_splits`; record its folders in state. Comment each scale action on the ops log issue.

## 5. Dispatch
Order: `sev:critical` → parts of the top-ranked feature → `sev:high` → rest by `prio`, then age. For each ticket you
assign: add `lane:<l>` (if missing) and `status:in-progress`.
- **Cloud lane** (`kind: cloud` or a surge lane): start a session with `create_session`:
  `source_url: https://github.com/utkarsh199708/piperoom`, `model: claude-sonnet-5-5` or `claude-opus-5-5` from the
  `model:` label, `title: [#N][<lane>] <issue title>`, `prompt:` the worker prompt below. Record the session id in state.
- **Qoder lane** (`kind: qoder`): comment on the issue `@qoder [<lane>] TASK — #N` with the issue link. Qoder and
  LEAD-ARCH pick up `lane:` tickets from GitHub (and Slack, `docs/collab.md`).
Check sessions you started earlier with `get_session`; a `failed` one gets one restart, then its ticket goes
`status:blocked` with the reason.

Worker prompt (fill in N, LANE, FOLDERS):
> You are lane LANE on Piperoom. Read `AGENTS.md` and `docs/agents/org.md` §8 (from `main`, else branch
> `claude/sharp-fermat-r6uh64`). Your ticket is issue #N: read it and everything it links. You may edit only:
> FOLDERS. Follow `AGENTS.md` §4–§7 exactly (gofmt, vet, unit tests, error paths, no contract edits). Commit, push
> your branch, open one PR titled `[#N][LANE] area: what` whose body starts with `Closes #N` and uses the PR template
> in `AGENTS.md` §6. Then subscribe to the PR's activity and address every review comment and CI failure until both
> leads approve; you never merge. If you need a file outside your folders, comment on #N and stop that part. Do not
> ask the human anything.

## 6. Board
Rebuild `docs/agents/tracker/data.json`: keep the existing groups for merged waves, then one group per open
`type:feature` (its sub-issues as rows: id `#N`, lane, title, PR numbers, status), plus a group "QA bugs" (open
`type:bug`, sorted by severity) and a group "Lanes" (one row per active, surge or parked lane with its current
ticket). Status mapping: `done`, `in-review`→review, `in-progress`→review, `ready` with no lane→todo,
`blocked`→blocked, `proposed`→check. Then `node docs/agents/tracker/build.js "" /tmp/board.html` and publish with
the Artifact tool to `url: https://claude.ai/artifact/Mf3MGghDsMyP1NfsCG58ME` (read it first with `action: read`).

## 7. Post `[DISPATCH STATE]`
Counters, lanes and sessions, and a 5-line summary: dispatched, scaled, blocked, merged since the last run.
