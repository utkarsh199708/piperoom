# AI agent organisation: squads, lanes, and how to track who is fixing what

Owner: the human (Utkarsh) and `[LEAD-ARCH]`. This file sits on top of `AGENTS.md` §2 (folder ownership stays
there and still wins) and `docs/collab.md` (Slack protocol). It answers one question at any moment:
**which agent is working on what, in which PR, and what is it waiting for.** The live answer is
`docs/agents/board.md`.

## 1. Why this exists

On 2026-10-15 the open PR list showed the problem:

- PRs titled `Claude/nice volta 752jes` (#243), `Claude/busy einstein y9dxdl` (#233) and `QA sweep fixes …`
  (#151) name no agent, no task and no squad.
- A whole wave (Q1–Q7, QA findings) lives only in `tasks/Q*.md` on an unmerged branch. It is not in
  `AGENTS.md`, so nobody can see who owns its fixes.
- One branch merges another (#243 merges #233's commits) and the PR spans A4, A5 and N6 folders. That breaks
  "one agent, own folders" and means nobody can review it as a unit.
- Tags such as `A4` say who owns a folder, not what that agent is doing now, so `A4` can have two PRs open
  (#242, #244) and nothing says which comes first.

The fix: stable squads, one **work ID** on every artefact, WIP limits, and one board.

## 2. Structure

```
                         Human owner (Utkarsh): priorities, overrides, final say
                                          │
              ┌───────────────────────────┴───────────────────────────┐
              │                 COMMAND (Claude leads)                 │
              │ LEAD-ARCH  dispatch, contracts, board, the only merger │
              │ LEAD-REVIEW code review for Qoder (A*) PRs             │
              │ LEAD-CLOUD  code review + status for cloud (C*) PRs    │
              │ LEAD-QA     QA sweeps; turns findings into work items  │
              └──┬──────────┬──────────┬──────────┬──────────┬────────┘
                 │          │          │          │          │
             PIPE squad  BRAIN squad ROOM squad  WEB squad  PLATFORM squad      TRUST squad
             connectors  agent+evals backend     frontend   CI, deploy, dev     security, field
```

`LEAD-QA` formalises what the Q-wave sessions already do (QA rounds 1–3). It writes findings only. It never
fixes them: each finding becomes a work item for the squad that owns the file.

### Squads

Each squad has a **squad reviewer**: the lead who code-reviews its PRs. Qoder lanes go to `LEAD-REVIEW` and
cloud lanes go to `LEAD-CLOUD`, as in `AGENTS.md`. Squads are a grouping for tracking and planning. They do not
change folder ownership.

| Squad | Mission | Lanes (agent tags) | Folders (from `AGENTS.md` §2) | Human counterpart (when hired) |
|---|---|---|---|---|
| **PIPE** | Read-only visibility into Kafka, Connect, Debezium, Postgres, warehouses; fix actions | A2, A3, A7, C3, C10 | `internal/clients/**`, `internal/tools/{kafkatools,dbtools,debeziumtools,changes}/**`, `internal/tools/scope.go`, `internal/topology/**`, `internal/owners/**`, `internal/actions/**`, `internal/watch/**`, `internal/dlq/**`, `internal/discover/**`, `cmd/discover/**` | CDC / data-platform engineer |
| **BRAIN** | The agent finds the right cause, calibrated, with receipts, within budget | A4, C6, C7, C11 | `internal/agent/**`, `internal/wiring/**`, `cmd/investigate/**`, `evals/**`, `internal/tools/runbooks/**`, `internal/tools/similarincidents/**`, `internal/textsearch/**`, `internal/textnorm/**`, `internal/redact/**`, `internal/injectscan/**` | AI / agent engineer |
| **ROOM** | Event log, driver lease, steering, approvals, executor, hand-off, alert in, Slack | A5, C5, C12 | `migrations/**`, `internal/{store,room,httpapi,ws,auth}/**`, `cmd/server/**`, `internal/alertin/**`, `internal/slackbridge/**` | Backend lead |
| **WEB** | The live room UI | A6 | `web/**` | Frontend engineer + designer |
| **PLATFORM** | CI, build, release, dev stack, deploy, load tests | C1, A1 | `.github/**`, `Makefile`, `Dockerfile`, `deploy/**`, `dev/**`, `cmd/devctl/**`, `cmd/devconsumer/**`, `internal/testenv/**`, `cmd/loadtest/**`, `internal/loadtest/**` | SRE / platform engineer |
| **TRUST** | Security design and audits, pilot onboarding docs (no code) | C4, C9, C13, C14 | `docs/security/**`, `docs/design/**` (security sections), `docs/audits/**`, `docs/pilot/**` | Security engineer, solutions engineer |

Packages created by the N wave (`textnorm`, `textsearch`, `similarincidents`, `redact`, `injectscan`) are listed
under BRAIN because the agent is their only consumer. Their owning tag in `AGENTS.md` does not change. A
follow-up in those packages is dispatched to the BRAIN lane that `[LEAD-ARCH]` names in the dispatch.

**Retired lanes** take no work: C2 (merged `9519d23`, its folder went back to A4), C8 (merged `7c7bdac`, folders back to
A2), and the E/T/N/S wave tags (`AGENTS.md`, "Earlier waves").

### Lane vs. session

A **lane** is a stable tag (`A4`, `C6`) that owns folders. A **session** is one run of Qoder or one claude.ai/code
cloud session. Many sessions over time serve one lane. Tracking is by lane and work ID, never by session or
branch name (`claude/nice-volta-752jes` tells you nothing).

## 3. The work ID: one ID on everything

Every unit of work has a **work ID** before anyone writes code:

| Source | Work ID form | Example |
|---|---|---|
| A wave task file | `<wave><n><sub>` | `F3d`, `R7`, `L1c` |
| A QA finding (`LEAD-QA`) | `Q<round>-<finding>` | `Q7-V1` |
| A review fix round on a PR | the PR's own ID plus a round number | `F3d r2` |
| A bug or ad-hoc request from the human | `X<n>` (next free number, assigned by `LEAD-ARCH`) | `X12` |

The ID appears in every artefact:

| Artefact | Format | Example |
|---|---|---|
| PR title | `[<ID>][<lane>] area: what` | `[F3d][A4] agent: provenance ledger` |
| Branch | `<lane>/<id>-<topic>`. A cloud session's own `claude/...` branch is accepted, but then the PR title is required to carry the tags | `a4/f3d1-provenance-ledger` |
| Commit subject | `area: what` as before; the PR carries the ID | `agent: provenance ledger` |
| Slack | `[<lane>][<ID>] STARTED / PROGRESS / BLOCKED / READY FOR REVIEW` | `[A4][F3d] READY FOR REVIEW` |
| Review files | `reviews/<lane>-<ID>.{code,arch}.md` | `reviews/a4-f3d.code.md` |
| GitHub labels | `squad:<name>`, `lane:<tag>`, `wave:<letter>`, `status:<state>` | `squad:brain`, `lane:a4`, `wave:f` |

**No ID, no work.** A session that starts without one asks `[LEAD-ARCH]` for one first. A PR whose title lacks
`[<ID>][<lane>]` is not reviewed. The reviewer replies "title" and nothing else until it is fixed.

## 4. Rules that keep the board true

1. **One PR = one work ID = one lane = that lane's folders.** No branch merges another agent's branch. Work
   that needs another lane's file goes through a thread (`docs/collab.md` §4) and becomes a work item for that
   lane.
2. **WIP limits.** A Qoder lane has at most **2** PRs open, and at most 1 of them is not yet in review. A cloud
   lane has at most **2**. A lead dispatches nothing new to a lane at its limit.
3. **Ordered parts.** When one work ID is split into PRs, each part takes a number (`F3d1`, `F3d2`) and the PR
   body names its predecessor: `waits on #242`.
4. **QA findings are dispatched, not fixed in place.** `LEAD-QA` writes `tasks/Q<n>-*.md` and opens one line per
   finding on the board, owned by the lane of the file. A QA session never opens a fix PR across folders
   again (see #151, #243).
5. **Stale = escalated.** A PR without a push or a comment for 12 h is marked `stale` on the board. Its lane gets
   one ping. After 24 h, `LEAD-ARCH` either re-dispatches it or closes it, and records which.
6. **Status changes only through the leads.** Agents post in Slack. `LEAD-ARCH` (and `LEAD-CLOUD` for C*) moves
   the board row. The board is never edited by a feature agent.

## 5. Status lifecycle

```
queued ─► started ─► in-review ─► changes-requested ─► in-review (round N) ─► approved ─► merged
              │            │
              └─► blocked ◄┘   (always names what it waits on: a PR, a question, the human)
```

`approved` means both approvals are in (`LEAD-REVIEW`/`LEAD-CLOUD` + `LEAD-ARCH`). `merged` rows move to the
"Done this week" table of the board, and rows older than a week are deleted.

## 6. The board

`docs/agents/board.md` is the single place to look. It has one table per squad. Each row is one work ID:

| ID | Lane | What | PR | Status | Waits on | Updated |
|---|---|---|---|---|---|---|

The board is updated by `LEAD-ARCH`:

- at every `[LEAD-ARCH] STATUS` post (twice a day, `docs/collab.md` §4). The STATUS post links the board and
  lists only the rows that changed;
- at every merge (row moves to Done).

**Upgrade path (recommended once the human approves):** the same columns as a GitHub Project
(table view, grouped by `squad`), fed by the labels in §3. A GitHub Action in `.github/**` (C1's folder) can
then check every PR title against `^\[[A-Z][0-9]+[a-z0-9-]*( r[0-9]+)?\]\[(A|C)[0-9]+\]` and add the labels
automatically. Until that lands, the markdown board is the source of truth.

## 7. Daily rhythm

| When | Who | What |
|---|---|---|
| Start of day | `LEAD-ARCH` | Board refresh; dispatch to lanes under their WIP limit, highest merge-order item first |
| Every 2 h of work | each lane | `[lane][ID] PROGRESS`: done / next / blocked |
| Midday and end of day | `LEAD-ARCH` | `STATUS` post: changed rows, blocked rows, stale rows |
| Each QA round | `LEAD-QA` | `tasks/Q<n>-*.md` and one board row per finding, owned by the file's lane |
| Weekly | human + leads | Squad review: throughput per squad, longest-open PRs, lanes to add or retire |

## 8. Adding or retiring a lane

A new lane takes the next free tag (`A8`, `C15`), a squad, a task file, and a row in `AGENTS.md` §2 **and** in the
squad table above, in the same leads' PR. A lane whose task is merged and whose folders return to another
owner is moved to "Retired lanes" in that same PR.
