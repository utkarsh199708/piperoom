# Feature board: who is building what

Rules: `docs/agents/org.md`. Updated by `[LEAD-ARCH]` only. **Snapshot: 2026-10-15 14:50 UTC** (task files + every PR on GitHub).

## Needs an owner, unblocking or a decision

| Part | Agent | What | PR | Status | Note |
|---|---|---|---|---|---|
| A4-sweep | A4 | agent/wiring/redact/injectscan: evals + security + bugs | — | in progress | Session hit the usage limit at 11:23 with 7 commits pushed and no PR; restarted 14:46 to finish and open the PR |
| small-lanes | C5 + C12 + A1 + C3 + C10 + C11 | alertin, slackbridge, dev stack, snowflake, discover, runbooks | — | in progress | Local workflow: first run lost 17 agents to the usage limit; resumed 14:46 with seeds cached |
| F2a | A6 | Web: pinned shift summary, export, post-mortem view | — | **NOBODY ON IT** | No PR and no web code for it on main. Nobody is working on it. |
| F9-slack | C12 | Slack follow-up: nudges and lease-expiry paging | — | **NOBODY ON IT** | Named as a C12 follow-up in the task file and AGENTS.md (R11). No PR found. |
| F10d | C1 | README and runbook: Slack bridge, stack lock | — | **NOBODY ON IT** | No PR, and README.md / docs/runbook.md have not changed since the F wave started. |
| F11-evals | C6 | evals/README reconciles the cost numbers | — | needs your call | No PR found. Confirm with C6 whether it is still needed. |
| F12d | C6 + C1 | Eval baseline and regression gate | — | needs your call | No [F12d] PR. L1c's evals-live job and baseline (#215, #248) may cover it. Confirm and close. |
| R10-fu | A6 | Show declined suggestions | #152 | needs your call | #152 was closed without merging. Decide: drop it or re-dispatch. |
| Q2 | ? | QA sweep fixes: PII redaction, read-only transactions | #151 | **blocked** | No lane tag; touches A1, A2, A3, A7, C5 folders. Stale since 00:13. Split it per agent. |
| Q5 | ? | Room recap, wake on executed fixes, injectscan names | #243, #233 | **blocked** | Untagged. #233 contains all of #243 plus the Q6 and Q7 task files. Close #243, split #233 per agent. |
| Q6, Q7 | ? | QA round 3 findings | — | **NOBODY ON IT** | Task files exist only on #233's branch. No agent assigned. |

## In review now

| Part | Agent | What | PR |
|---|---|---|---|
| A5-sweep | A5 | room/store/httpapi/ws/auth: security + bugs | #289 |
| A2-sweep | A2 | kafka/connect/schemareg/netsec/actions: security + bugs | #319 |
| A7-sweep | A7 | watch/dlq: restart re-post (Q7-V1), redaction | #360 |
| C6-sweep | C6 | evals harness: scoring, compare, evalrun CLI | #347 |
| C7-sweep | C7 | eval scenarios: right-answer failures, scorer fit | #341 |
| F3e | A6 | Ran receipts, citation chips, unbacked marks | #245 |
| F3g | A6 | Structured evidence for six read tools | #247 |
| F3-fix | LEAD-ARCH | Contract: clean hand-off keeps violations: [] | #249 |
| R7 | C1 | Full dev-stack integration job, smoke, release image | #246 |

## By feature

### SWEEP — Fix sweep 2026-10-15: evals, security, bugs (one PR per lane) (3/10 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| A5-sweep | A5 | room/store/httpapi/ws/auth: security + bugs | #289 | in review |
| A4-sweep | A4 | agent/wiring/redact/injectscan: evals + security + bugs | — | in progress |
| A6-sweep | A6 | web: bugs + security | #359 | done |
| A2-sweep | A2 | kafka/connect/schemareg/netsec/actions: security + bugs | #319 | in review |
| A3-sweep | A3 | postgres/warehouse/dbtools/topology/owners: security + bugs; makes #246 test robust | #333 | done |
| A7-sweep | A7 | watch/dlq: restart re-post (Q7-V1), redaction | #360 | in review |
| C6-sweep | C6 | evals harness: scoring, compare, evalrun CLI | #347 | in review |
| C7-sweep | C7 | eval scenarios: right-answer failures, scorer fit | #341 | in review |
| C1-sweep | C1 | CI/build/deploy: exit codes, secrets, dependency upgrades; closes #253 | #264 | done |
| small-lanes | C5 + C12 + A1 + C3 + C10 + C11 | alertin, slackbridge, dev stack, snowflake, discover, runbooks | — | in progress |

### F1 — Decision bar, room status, attention (4/4 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F1a | A6 | One derived room status for header, list, pill, ⌘K | #154 | done |
| F1b | A6 | Sticky decision bar, run confirm, idle composer | #162 | done |
| F1c | A6 | Kill double scroll, legacy CSS, Vite favicon | #156 | done |
| F1d | A6 | Attention: title, favicon dot, toasts, "Needs you N" | #163 | done |

### F2 — Hand-off that survives, post-mortem draft (2/3 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F2a | A6 | Web: pinned shift summary, export, post-mortem view | — | **NOBODY ON IT** |
| F2b | A5 | Post-mortem route, events, close hook, verification list | #185, #236 | done |
| F2c | A4 | WritePostmortem closing-summary writer | #168 | done |

### F3 — Evidence receipts (4/7 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F3b | A2 | Connect, Schema Registry, Kafka record provenance | #157 | done |
| F3c | A3 | Postgres and warehouse record SQL provenance | #158 | done |
| F3d | A4 | Provenance ledger, unbacked terms, hand-off check | #242, #244 | done |
| F3e | A6 | Ran receipts, citation chips, unbacked marks | #245 | in review |
| F3f | C3 | Snowflake records SQL provenance | #159 | done |
| F3g | A6 | Structured evidence for six read tools | #247 | in review |
| F3-fix | LEAD-ARCH | Contract: clean hand-off keeps violations: [] | #249 | in review |

### F4 — Fix ceremony: preview, run, verify, roll back (5/5 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F4b | A2 | Action preview and dry-run | #155 | done |
| F4c | A5 | Preview proposals, store proposal.previewed | #171 | done |
| F4d | A4 | Typed verification event and follow-ups | #234, #235 | done |
| F4e | A6 | Blast radius and execution card | #202 | done |
| F4f | A6 | Verification card, close nudge, roll back | #203 | done |

### F5 — Suggest without a driver, activity feed (2/2 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F5a | A6 | Composer modes, chips, curated feed | #180, #195 | done |
| F5b | A5 | Id-free agent text, suggestions with no driver | #169 | done |

### F6 — Proactive triage (5/5 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F6b | A7 | Watcher: consumer-group signals, crash loops | #179 | done |
| F6b-owners | A3 | Owners: parse new signal keys | #167 | done |
| F6c | A4 | Agent triage pass, alert_update inputs | #178 | done |
| F6d | A5 | First look, rooms list over WebSocket | #194, #201 | done |
| F6e | A6 | Pushed rooms list, first-look card | #197 | done |

### F7 — Lane, trace, mobile (3/3 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F7a | A6 | Compact pipeline lane | #165 | done |
| F7b | A6 | Trace leads with the verdict | #166 | done |
| F7c | A6 | Tabs below 1280px, bottom sheet, touch | #184, #186 | done |

### F8 — What changed, similar incidents, runbook checklist (6/6 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F8b | A3 | what_changed tool, similar_incidents labels | #176 | done |
| F8c | A7 | Watcher feeds the change log, masks secrets | #183, #200 | done |
| F8d | A5 | Change log store, label and checklist routes | #174 | done |
| F8e | A6 | What-changed lane, checklist, label form | #188 | done |
| F8f | C11 | Runbook items in runbook_lookup data | #164 | done |
| F8g | A4 | what_changed is the second read | #191 | done |

### F9 — Hypothesis verdict, presence, catch-up (3/4 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F9b | A5 | Verdicts, nudges, people layers, presence | #177 | done |
| F9c1 | A6 | Verdicts, who's-here layers, nudge | #187 | done |
| F9c2 | A6 | Lease awareness, catch-up strip | #189 | done |
| F9-slack | C12 | Slack follow-up: nudges and lease-expiry paging | — | **NOBODY ON IT** |

### F10 — Resilience and small traps (3/4 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F10a | A6 | Dialogs per room, offline sends, lease floor | #190, #192 | done |
| F10b | A4 | Escape driver steers, ids out of prose | #160 | done |
| F10c | A5 | Stop retrying unretryable agent errors | #161 | done |
| F10d | C1 | README and runbook: Slack bridge, stack lock | — | **NOBODY ON IT** |

### F11 — Cost and insight (2/3 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F11a | A5 | Snapshot insight, agent-stats route | #175 | done |
| F11b | A6 | Cost chip, first insight, track record | #198 | done |
| F11-evals | C6 | evals/README reconciles the cost numbers | — | needs your call |

### F12 — Deep signals and new scenarios (3/4 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| F12a | A2 | Cluster health, rates, ACLs, TLS expiry | #172, #182 | done |
| F12b | A3 | Load errors, reconcile counts, Debezium metrics | #170, #173, #181 | done |
| F12c | C7 | Four new incident scenarios | #196, #199 | done |
| F12d | C6 + C1 | Eval baseline and regression gate | — | needs your call |

### L1 — Learn from closed rooms (3/3 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| L1a | C6 | Export labelled rooms as eval cases | #207 | done |
| L1b | A4 | Past incidents as worked examples | #205 | done |
| L1c | C1 + C6 | evals-live CI job and baseline | #208, #211, #212, #214, #215, #248 | done |

### R — R wave leftovers (0/2 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| R7 | C1 | Full dev-stack integration job, smoke, release image | #246 | in review |
| R10-fu | A6 | Show declined suggestions | #152 | needs your call |

### Q — QA rounds (no row in AGENTS.md) (0/3 done)

| Part | Agent | What | PR | Status |
|---|---|---|---|---|
| Q2 | ? | QA sweep fixes: PII redaction, read-only transactions | #151 | **blocked** |
| Q5 | ? | Room recap, wake on executed fixes, injectscan names | #243, #233 | **blocked** |
| Q6, Q7 | ? | QA round 3 findings | — | **NOBODY ON IT** |
