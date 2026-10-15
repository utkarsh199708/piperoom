# Agent board: who is fixing what

Rules and status meanings: `docs/agents/org.md`. Updated by `[LEAD-ARCH]` only.
**Last refresh: 2026-10-15 10:10 UTC** (from the open PR list on GitHub).

## Needs a decision now

| # | Problem | Proposed action |
|---|---|---|
| 1 | #243 `Claude/nice volta 752jes` and #233 `Claude/busy einstein y9dxdl` have no work ID or lane. #233 contains every commit of #243 plus `tasks/Q6`, `tasks/Q7`. Together they carry Q5 fixes across A4 (`internal/agent`), A5 (`internal/room`), N6 (`internal/injectscan`) and the Q1–Q7 task files | Close #243 as a duplicate. Split #233 into: `[Q5][A4] agent: …`, `[Q5][A5] room: …`, `[Q5][A4] injectscan: …`, plus a leads' PR `[Q][LEAD-QA] tasks: Q1–Q7 findings` |
| 2 | #151 `QA sweep fixes …` has no ID or lane, spans A1/A2/A3/A7/C5 folders, and has had no update since 00:13 (stale) | Turn its commits into one PR per lane (`[Q2][A2] kafka: …`, `[Q2][A3] dbtools: …`, `[Q2][A7] dlq: …`, `[Q2][C5] alertin: …`, `[Q2][A1] testenv: …`); then close #151. Contract requests in its `reviews/` commits go to `LEAD-ARCH` |
| 3 | The Q wave (QA rounds, `tasks/Q1`–`Q7`) is missing from `AGENTS.md` | Add a "Q wave" table to `AGENTS.md` §2 with `LEAD-QA` as author and the file owners as fixers |

## PIPE: connectors (A2, A3, A7, C3, C10)

| ID | Lane | What | PR | Status | Waits on | Updated |
|---|---|---|---|---|---|---|
| Q2 | A2, A3, A7 | QA sweep fixes (kafka truncation, warehouse clock skew, DLQ redaction) | #151 (to split) | blocked | Decision 2 | 10-15 00:13 (stale) |

## BRAIN: agent and evals (A4, C6, C7, C11)

| ID | Lane | What | PR | Status | Waits on | Updated |
|---|---|---|---|---|---|---|
| F3d1 | A4 | Provenance on tool results, step ledger, `update_hypothesis` step checks | #242 | in-review | review | 10-15 10:06 |
| F3d2 | A4 | Unbacked terms in hypotheses, hand-off check with one retry | #244 | in-review | #242 merge | 10-15 10:08 |
| Q5 | A4 (+A5, N6 parts) | Recap-aware verify, `pg_table_activity` idle rule, injectscan tool names | #243 / #233 (to split) | blocked | Decision 1 | 10-15 10:07 |

## ROOM: backend (A5, C5, C12)

| ID | Lane | What | PR | Status | Waits on | Updated |
|---|---|---|---|---|---|---|
| Q5 | A5 | Wake on executed fixes, room recap, change-feed wiring, post-mortem fixes | inside #233 (to split) | blocked | Decision 1 | 10-15 10:07 |

## WEB: frontend (A6)

| ID | Lane | What | PR | Status | Waits on | Updated |
|---|---|---|---|---|---|---|
| F3e | A6 | Ran receipts, citation chips, unbacked marks, hand-off checks | #245 | in-review | F3d (#242, #244) for the data it renders | 10-15 10:01 |
| F3g | A6 | Structured evidence for six read tools | #247 | in-review | review | 10-15 10:05 |

## PLATFORM: CI, deploy, dev stack (C1, A1)

| ID | Lane | What | PR | Status | Waits on | Updated |
|---|---|---|---|---|---|---|
| R7 | C1 | Full dev-stack integration job with smoke, release image | #246 | in-review | review | 10-15 10:10 |
| L1c | C1 | `evals-live` compares misses per run over shared scenarios | #248 | in-review | review | 10-15 10:06 |

## TRUST: security and field (C4, C9, C13, C14)

Nothing open.

## Unowned (no lane yet)

| ID | What | Next step |
|---|---|---|
| Q6, Q7 | QA round 3 findings (`tasks/Q6-qa-round3-findings.md`, `tasks/Q7-qa-review3-findings.md`, on #233's branch only) | `LEAD-QA` lands the task files; `LEAD-ARCH` adds one row per finding under the squad that owns the file |

## Done this week (latest merges on `main`)

| ID | Lane | What | PR | Merged |
|---|---|---|---|---|
| — | A5, A2 | Room cross-room passes, alert-key lock, log wins over lost status writes; kafka transactional read tests | #241 | 10-15 |
| E6b | C6/C7 | Scenario budgets, negated causes are not claims | #239, #240 | 10-15 |
| E6a | A4 | Prompt etiquette: pause, healthy outcome, status reads | #238 | 10-15 |
