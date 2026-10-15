# Autonomous agent organisation

How Piperoom is built without a human in the loop: a PM agent decides what to build next, a QA agent files
bugs as it finds them, a dispatcher turns both into tickets for agent squads and grows or shrinks those squads,
and the leads review and merge. `AGENTS.md` §2 (folder ownership) and `docs/collab.md` (Slack protocol) still
apply and win on any conflict. The human (Utkarsh) can override anything at any time but is never waited on.

## 1. The loop

```
  PM (Fable 5.1) ──feature briefs──┐                 ┌──► worker lanes (squads) ──PRs──► leads review ──► LEAD-ARCH merges
                                   ▼                 │                                                        │
                          GitHub Issues (one ticket = one work item) ◄── dispatcher: route, prioritise, scale ─┘
                                   ▲                                         │
  QA (Opus 5.5) ──bug tickets──────┘                                         └──► live board (artifact + docs/agents/board.md)
        ▲
        └──────────── every merge to main, plus a full sweep each night
```

**GitHub Issues are the single source of truth.** The issue number is the work ID. Every PR title is
`[#<issue>][<lane>] area: what` and its body says `Closes #<issue>`. Anything without an issue does not exist.

## 2. Roles

| Role | Model | Runs | Writes | Never |
|---|---|---|---|---|
| **PM** | Fable 5.1 (`claude-fable-5-1`) | daily, cloud scheduled task | `type:feature` issues: problem, evidence, success metric, out of scope, parts per squad, rank | code, task files, file:line specs |
| **QA** | Opus 5.5 | hourly on new merges; nightly full sweep | `type:bug` issues, one per confirmed finding, with a failing test as the repro | fixes; editing any file |
| **Dispatcher** | Opus 5.5 | hourly, cloud scheduled task | labels, sub-issues, assignments; starts cloud worker sessions; scales lanes; refreshes the board | code; merging |
| **LEAD-ARCH** | Opus 5.5 | existing | specs approved features into `tasks/` + contract PRs; **the only merger** | feature code |
| **LEAD-REVIEW / LEAD-CLOUD** | Opus 5.5 | existing | code review verdicts | merging, feature code |
| **Worker lanes** | per ticket: `model:sonnet` (spec is complete, mechanical) or `model:opus` (prompts, security, concurrency, approvals, contracts) | per ticket | one PR per ticket, inside the lane's folders | other lanes' folders, contract files, `main` |

Feature approval is autonomous: the dispatcher moves a PM brief from `status:proposed` to `status:approved` when it
fits `docs/product/vision-2026-10.md`, has a measurable success metric and names the squads. Otherwise it comments
why and labels it `status:rejected`. The PM can revise and re-propose once.

## 3. Squads

Routing from file paths to lanes: `docs/agents/routing.yaml`.

| Squad | Base lanes | Reviewer |
|---|---|---|
| PIPE (connectors) | A2, A3, A7, C3, C10 | LEAD-REVIEW (C*: LEAD-CLOUD) |
| BRAIN (agent, evals) | A4, C6, C7, C11 | LEAD-REVIEW (C*: LEAD-CLOUD) |
| ROOM (backend) | A5, C5, C12 | LEAD-REVIEW (C*: LEAD-CLOUD) |
| WEB (frontend) | A6 | LEAD-REVIEW |
| PLATFORM (CI, deploy, dev stack) | C1, A1 | LEAD-CLOUD (A1: LEAD-REVIEW) |
| TRUST (security, pilot docs) | C9 (+ C4, C13, C14 when needed) | LEAD-CLOUD |

A **lane** is a stable tag that owns folders. A **session** is one run of an agent on one ticket. Track lanes and
tickets, never sessions or branch names.

## 4. Elastic scaling (dispatcher decides, every run)

Inputs per squad: `ready` = open tickets labelled `status:ready` for that squad, `active` = lanes with a ticket in
progress, `p0` = ready tickets labelled `sev:critical`.

| Rule | Trigger | Action |
|---|---|---|
| Scale up | `ready ≥ 3 × active` for two runs in a row, or a `p0` waited more than one run with every lane busy | Add one **surge lane** (`A5s1`, `A6s2`, …) owning one split from `routing.yaml` `surge_splits`. Cloud only: the dispatcher starts it |
| Scale down | A surge lane had no ticket for two runs | Retire it; its folders go back to the base lane |
| Park | A base lane had no ticket for four runs | Mark it `parked` on the board; it costs nothing until a ticket arrives |
| Reviewer span | More than 5 active lanes per reviewer | Add `LEAD-REVIEW-2` / `LEAD-CLOUD-2` as a cloud session for code review only |
| Ceiling | Always | At most **8** concurrent cloud worker sessions and **2** open PRs per lane. Over the ceiling, tickets queue by priority |

A surge lane never shares a folder with another active lane. If a ticket spans two splits, it goes to the base
lane, not to a surge lane.

## 5. Tickets

Labels (created on first use):

- `type:feature` · `type:part` (sub-issue of a feature) · `type:bug` · `type:chore`
- `squad:pipe|brain|room|web|platform|trust|leads` · `lane:<tag>` (e.g. `lane:a4`, `lane:a6s1`)
- `status:proposed|approved|specced|ready|in-progress|in-review|blocked|done|rejected`
- `sev:critical|high|medium|low` (bugs) · `prio:p0|p1|p2|p3` (features and parts)
- `model:sonnet|opus` · `source:pm|qa|human|lead`

Lifecycle:

```
feature:  proposed ─► approved ─► specced (LEAD-ARCH task file + contract PR) ─► parts created as sub-issues
part/bug:  ready ─► in-progress (lane assigned) ─► in-review (PR open) ─► done (PR merged, issue closed)
                 └─► blocked (comment names what it waits on)
```

Priority order for the dispatcher: `sev:critical` bugs → parts of the top-ranked feature whose predecessors merged →
`sev:high` bugs → everything else by rank, oldest first. A QA `sev:critical` finding preempts: the lane finishes its
current commit, then takes it.

## 6. QA rules (real time)

- Trigger: every new merge to `main` (the QA run diffs `main` since its last run) and a nightly full sweep.
- One issue per finding. Search open issues first; on a duplicate, comment the new evidence instead.
- A finding is filed only when reproduced: a failing test, a command and its output, or a concrete trace through
  the code with `file:line`. Speculation goes in a weekly `QA notes` issue, not as tickets.
- Route by the file the fix must touch (`routing.yaml`) and label `squad:` and `lane:`. Set `sev:` by impact:
  critical = data loss, security, wrong fix executed, main red; high = wrong diagnosis, broken flow; medium = degraded;
  low = cosmetic.
- QA never fixes. The old pattern (one QA branch fixing across five folders, #151 and #233) is not allowed.

## 7. PM rules

- Inputs: `docs/product/vision-2026-10.md`, merged features, the board, QA trends (which squads produce the most
  `sev:high+` bugs), eval reports (`evals/baseline*`, the `evals-live` job), and pilot docs (`docs/pilot/**`).
- At most 3 open `status:proposed` features at a time; one brief per issue; rank them `prio:p0..p3`.
- A brief says what the on-call engineer sees and can do afterwards, how success is measured, and which squads
  it touches. It does not name files or functions: that is LEAD-ARCH's spec.

## 8. Worker rules

Everything in `AGENTS.md` §4–§7 plus:

- Take only tickets labelled with your lane. One ticket, one branch, one PR: `[#<issue>][<lane>] area: what`,
  body starts with `Closes #<issue>`.
- Move the ticket: comment `started` (dispatcher sets `status:in-progress`), open the PR (`status:in-review`),
  `blocked: <what>` when blocked.
- Need a file outside your lane: comment on the ticket; the dispatcher files a linked ticket for that lane.

## 9. The board

The dispatcher refreshes the live tracker each run: https://claude.ai/artifact/Mf3MGghDsMyP1NfsCG58ME
(the markdown copy is `docs/agents/board.md`). It shows every feature with its parts, lane, PR and status, the
lanes active, parked or surging per squad, and the tickets nobody owns.

## 10. Guardrails that stay on

- Only LEAD-ARCH merges, after both lead approvals (`AGENTS.md` §2). No role in this file merges.
- Contract files change only in leads' PRs.
- The 8-session ceiling caps cost; the dispatcher logs each scale action as a comment on a pinned `Agent ops log` issue.
- Any agent stops and files a `sev:critical` ticket for LEAD-ARCH instead of acting when an action would write to
  a real customer system, touch secrets, or rewrite git history.

## 11. Scheduled tasks (live)

| Role | Schedule (UTC) | Model | Scheduled task |
|---|---|---|---|
| QA | every hour at :24 | Opus 5.5 | `trig_01UgELZvZyhH1vUPonA46Pq5` |
| Dispatcher | every hour at :54 (after QA, so new tickets are routed the same hour) | Opus 5.5 | `trig_01NWKMCX4G5JahG18YwtYWar` |
| PM | daily 08:47 | Fable 5.1 | `trig_01VdaXpKTkaRA85EFhp8j64U` |

Each run is a fresh cloud session that reads its role file and the ops log issue #250. Seed tickets: #251 (F2a),
#252 (F9 Slack), #253 (F10d), #254 (split the untagged QA PRs).
