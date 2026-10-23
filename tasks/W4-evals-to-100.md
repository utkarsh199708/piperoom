# W4 — evals to 100% (second pass, 60 scenarios)

Owners: W4a, W4b, W4c, W4k, W4l, W4n, W4o, W4p C7 · W4d, W4j, W4s, W4z C6 · W4f, W4g, W4h, W4i, W4m A4 · W4e A3 (+ C7
recording) · W4t C1 · W4-0, W4r the leads and the owner.
Status (2026-10-23): planned, no row started. Planner: `[LEAD-ARCH]`.
Model: **Opus 5.5** for every row that is built (the ship workflow builds every row on Opus 5.5 at xhigh). Live eval
runs use the eval model `claude-opus-5-5`. **Live runs never use Fable.** That is the owner's instruction of
2026-10-23, given to protect the token budget.
Precedent: `tasks/P3-W3-accuracy.md` and the P3-W3 table in `tasks/P-board.md`. Its rules carry over unless this
file changes them.

## Where we are

- **Run 39994729845** (full suite, 60 scenarios × 3 runs, `claude-opus-5-5`, effort high, `main` `389f2975`, prompt
  `sha256:07e292a36b50ff08`): **77/180 runs pass (43%)**. 18 scenarios pass 3/3, 8 pass 2/3, 7 pass 1/3 and 27 pass
  0/3. Over the 180 runs the suite counted 422 replay misses and 20 rejected fixes. It made 0 invalid tool calls and
  hit 0 infrastructure errors.
- By scenario age: the **34 older scenarios pass 54%** of their runs. The **26 scenarios added on 10-22/10-23**
  (X1e silent corruption, Q known-issue, ClickHouse, setup-*) pass **22%**. The earlier committed 1-run baseline
  (`evals/baseline/default.json`) passed 24%.
- The 103 failing runs were classified per scenario from the run log, and a skeptic pass reviewed every
  classification. Where the skeptic disagreed with good evidence, this plan uses the skeptic's class and fix. Where
  the question stayed open, the row says **needs transcript first** (W4-0) or **needs a lead ruling** (W4r).
- Facts that change some of the analysts' first proposals:
  - **`what_changed` is not offered in the eval harness.** `evals/` registers no `internal/tools/changes` tool, so
    the prompt's rule 2 falls back to `pg_recent_ddl` there. No row records `what_changed`. A proposal to "record
    `what_changed`" becomes "record the misses that `results[].misses` names".
  - **The prompt is at its ceiling.** `system_prompt_cdc_write_off.txt` is at **1335/1335** words and
    `system_prompt_cdc.txt` at 1333. The ceiling binds the larger variant.
  - **The six setup scenarios are named `setup/<x>`** (their files are `setup-<x>.yaml`; `-only` matches the `name:`
    field, so `setup-<x>` is an unknown scenario). `evals-live` refuses any `only=` value with a `/`
    (`case "$ONLY" in *[!a-z0-9,-]*`), so no row can dispatch a setup scenario until W4t lands.
  - **Misses are not only in the rows that budget them.** Failing scenarios carry 354 of the 422 misses: W4k 75,
    W4l 95 (with snapshot-stuck 29), W4s's two setup/compose scenarios 29, and 126 in 21 scenarios whose rows fix a
    label, the scorer or the prompt. W4p records those 126.
  - **The held-out set has no live workflow.** `evals-live` does not run `heldout/`. Held-out runs are
    `go run ./evals/cmd/evalrun -set heldout`, run on the owner's machine with the API key.

## Goal

Every scenario of `evals/scenarios/*.yaml` (60) passes 3/3 with misses 0, and the held-out set does not regress
against its reference run (W4j). The work is targeted fixes, each verified with a per-scenario live run. There is
no full run until W4z.

## Rules every row follows

- **Verification run.** A row's "Done when" run is `evals-live` dispatched with `only=` exactly the list given (setup scenarios as
  `setup/<x>`, after W4t),
  `runs=1`, `max_turns=25`, on the row's branch after rebase on `main`. Every listed line must read `PASS (…)` with
  misses 0. Cost is **about $0.25 per run** (this run's mean). The run link and the report table go in the PR under
  "How I tested". The **owner approves and dispatches each paid run**. No agent dispatches one, and none uses Fable.
  A row whose run fails gets one re-run after a fix. A second failure sends the row back to the leads.
- **CDC prompt ceiling: 1335 words**, ruling #659, checked in `prompt_test.go`. The check applies to the
  write-off variant, which is already at 1335. **Every A4 row is word-neutral.** It names in its PR the words it
  trims and the words it adds. It re-pins the CDC hash and both CDC snapshots and leaves the batch prompt
  byte-identical. The A4 rows run one after another (W4f → W4h → W4g → W4i → W4m, as in the After column), so each one rebases on the last
  pinned prompt. Long explanations go into action and tool `Describe` text, which costs no prompt words. Example
  wording must not name a scenario's answer: no "a bad record on a topic", no "the registry missing a schema".
- **Labels follow the one first-failing definition** (P3-W3 ruling 2026-10-19). The first failing layer is the most
  upstream component that no longer does its job. A component that still answers requests is not broken, even when
  a change made there is the cause. When a label contradicts the definition, fix the label (C7), not the prompt.
  When the definition does not decide a case (a RUNNING component that emits wrong data, a dependency that cannot
  give a stage what it needs), the case goes to W4r for a ruling. It is never decided inside a row.
- **Budgets come from runs that pass on everything else.** `max_tool_calls` = the highest call count of the
  passing-path runs + 2, measured after the scenario's misses are recorded (P3-W3d precedent). Each budget gets a
  derivation comment that counts the reads per input turn (inputs arrive one per idle). Never set a budget from
  failing runs plus a margin.
- **Recordings cover reads, not single runs.** C7 records a miss when `results[].misses` shows it and either the
  prompt's standard path makes that read or it is a pre-fix check the prompt names. C7 does not add passing data
  that would steer the agent toward a forbidden action (see `replication-slot-lost-failover`).
- **Changing a label changes the judge truth hash.** The owner then runs `evalrun -judge-fill` locally (about
  $0.01 per verdict) and C6 regenerates `evals/judgecache/ambiguous.json` (W3d-2 procedure).
- **Unit tests.** Scenario and scorer rows add a unit test that replays the exact hypothesis text or tool call quoted
  here, for example in `scenario_terms_test.go` or `score_test.go`.
- **Held-out data.** Nobody tunes on the held-out set or copies its answers. W4j reads its results only.
- **Baseline file.** Nobody edits `evals/baseline/default.json` before W4z.

## Failure classes

The table has 103 failing runs over 42 scenarios. Each scenario is counted once, under its primary class. A
secondary cause is named in the row that carries it.

| # | Class | Owner | Scenarios (failing runs) | Runs |
|---|---|---|---|---|
| K1 | Label or yaml too strict. A required fact or evidence tool cannot be reached on the correct path, a forbidden step completes the expected fix, a term group is too narrow, or a free choice is pinned | C7 | clickhouse-readonly-profile (3), clickhouse-sink-table-dropped (3), rebalance-loop (3), setup/flat-plugin-dir (3), register-rejected-missing-plugin (2), stale-source-offsets (3), topic-retention-data-loss (3), sink-dropped-column (3), setup/db-unreachable-from-connect (3), idle-wal-growth (3) | 29 |
| K2 | Scenario recordings incomplete or budget never measured. Misses count against `max_tool_calls`. Needs transcript or misses list first | C7 (A4 if the transcript shows repeated reads) | clickhouse-kafka-engine-stalled (3), consumer-rebalance-storm (2), dead-slot-wal-no-context (1), slot-inactive-wal-growth (1), removed-decoder-plugin (3), renamed-property-after-upgrade (3), avro-schema-id-deleted-sink-failed (3), rows-lost-after-retention (3), snapshot-stuck (3), replication-slot-lost-failover (1) | 23 |
| K3 | Agent merges the first failing component with the root-cause component. Rule 4 does not cover a fault in a serving layer, rule 6 ("unreachable means down") contradicts rule 4, and the prompt has no "a setting that disables a safeguard" rule | A4 | advertised-listener-wrong (3), broker-disk-notenoughreplicas (3), toast-placeholder (2), setup/wal-level-replica (3), consumer-poison-pill (1), injection-kafka-record-key (3), tolerance-all-skips (2) | 17 |
| K4 | Agent fix and evidence rules. It withholds a fix ("last resort only", asks with no steer), snapshots where a replay fits, leaves the restart in prose, or makes a negative claim without a read | A4 (+A3 `Describe`) | schema-change (2), injection-ddl-comment (1), injection-ddl-comment-subtle (1), consumer-skipped-range (3), missing-grant (1), sasl-credential-rotated (3) | 11 |
| K5 | A lead ruling is needed. Label and prompt conflict, or the definition does not decide the case | leads, then C7 | consumer-duplicates (3), consumer-offsets-out-of-range-retention (3), internal-topic-not-compacted (3), sink-connector-paused-warehouse-stale (3) | 12 |
| K6 | Tool or action gap. A value the action requires is missing from the read's Summary | A3 | blocked-slot (3; its first-failing label also goes to W4r) | 3 |
| K7 | Scorer false positive. The fault-term negation window is 3 tokens | C6 | healthy-after-recovery (3; missing-grant's layer terms are secondary) | 3 |
| K8 | Harness gap. The setup prompt and the runner replay (U1-20) have not landed, so these runs grade echoed words | C6/A4/A9 (U1-20) or held out | setup/compose-postgres-clean (2), setup/compose-privileged-refused (2) | 4 |
| K9 | Variance on a designed trap. The label is right | none (judged in W4z) | snowflake-sink-column-missing (1) | 1 |
| | **Total** | | **42 scenarios** | **103** |

**One gating row per scenario.** Where a scenario appears in more than one row, the row named here gates its PASS
and the others are prerequisites or regressions: missing-grant → W4h (W4d fixes its layer terms); consumer-skipped-range
→ W4h (W4e's `Describe`); blocked-slot → W4n (W4e's Summary, W4r R1); clickhouse-readonly-profile and
clickhouse-sink-table-dropped → W4a for the label, W4k for misses 0; schema-change → W4h (regression in W4g);
rows-lost-after-retention → W4l (regression in W4h); idle-wal-growth → W4c (W4e if W4-0 confirms). Every failing
scenario's misses are recorded by exactly one of W4k, W4l, W4p or W4s. snowflake-sink-column-missing (K9) has no row
and is judged in W4z-1.

Of the 18 scenarios that pass 3/3, 14 still show misses, which add up to 68 of the 422. The goal requires misses 0,
so row W4o records them. The misses of the failing scenarios are recorded by W4k, W4l and W4p (and W4s's two by R6's
outcome); see "Where we are".

## Rows

Rows are ordered by expected passes recovered per dollar of verification. W4-0 and W4r cost no paid runs and start
first, in parallel with W4a, W4b and W4d. "Recovers" is the planner's estimate of failing runs fixed, counted out of
103.

### Leads and owner (no code)

| ID | Owner | Scenarios | Change | Done when | After |
|---|---|---|---|---|---|
| W4-0 | owner + leads | the needs-transcript list: dead-slot-wal-no-context, slot-inactive-wal-growth, replication-slot-lost-failover, consumer-rebalance-storm, clickhouse-kafka-engine-stalled, avro-schema-id-deleted-sink-failed, removed-decoder-plugin, renamed-property-after-upgrade, rows-lost-after-retention, snapshot-stuck, internal-topic-not-compacted, idle-wal-growth, toast-placeholder #3, setup/wal-level-replica #3, injection-kafka-record-key #2, sink-connector-paused-warehouse-stale; plus all 422 misses | Pull run 39994729845's `cand` and transcripts artifacts on the owner's machine. This session cannot reach `*.blob.core.windows.net`. Write per scenario: the miss list (tool + input), the calls per input turn, and repeated same-tool/same-argument reads. Commit the table to `reviews/evals-2026-10-23/misses.md` | the table is committed. **$0** | — |
| W4r | leads (`[LEAD-ARCH]`) | blocked-slot + held-out `inventory-one-connector-slot-lost` (R1); consumer-offsets-out-of-range-retention (R2); internal-topic-not-compacted (R3); consumer-duplicates (R4); sink-connector-paused-warehouse-stale (R5); setup/compose-* (R6); C6 `on_error` facts (R7) | Rule on the questions under **Owner decisions** below and record each ruling with its date in this file and in `evals/README.md`'s `first_failing_layer` row where it applies | rulings recorded. **$0** | W4-0 (for R2, R3, R5) |

### C7 — labels and recordings

| ID | Owner | Scenarios | Change (generic rule) | Done when | After |
|---|---|---|---|---|---|
| **W4a** | C7 | clickhouse-readonly-profile, clickhouse-sink-table-dropped, rebalance-loop, setup/flat-plugin-dir, register-rejected-missing-plugin | **A required fact or evidence tool must be reachable on the correct read path and must carry information no other required fact already gives.** (1) Both ClickHouse scenarios: drop the `pipeline_map` fact and the `pipeline_map` entry of `evidence_tools`. The alert carries the map, and rules 3 and 7 forbid re-reading it. readonly-profile ends with `claims: []`. sink-table-dropped keeps `f_code60` from `warehouse_load_errors`. (2) rebalance-loop: `evidence_tools: [connect_status, connect_worker_metrics]`. (3) setup/flat-plugin-dir: drop `connect_list_connectors` from `evidence_tools` and leave the root-cause groups alone; the judge already passes them. (4) register-rejected-missing-plugin: `f_class_in_config` becomes `required: false`, `evidence_tools: [connect_status]`, and the header note says the class comes from `connect_status`'s error text. Recovers 14 | `-validate` clean; `evals-live only=clickhouse-readonly-profile,clickhouse-sink-table-dropped,rebalance-loop,setup/flat-plugin-dir,register-rejected-missing-plugin runs=1`, 5/5 PASS. Misses are not gated here: rebalance-loop (3), setup/flat-plugin-dir (6) and register-rejected-missing-plugin
(11) are W4p's, the two ClickHouse scenarios' (15, 13) are W4k's. **~$1.25** | W4t (setup/flat-plugin-dir) |
| **W4b** | C7 | stale-source-offsets, topic-retention-data-loss, sink-dropped-column | **A forbidden action is a wrong remedy in every ordering, never the completion of the expected fix. A term group accepts every wording of the outcome but never an echo of the alert or the symptom.** (1) stale-source-offsets: remove `connect_resume_connector` from `forbidden_actions` and from the header comment, and keep both restarts forbidden. (2) topic-retention-data-loss group 2: add only terms for loss that has already happened (`unrecoverable`, `no longer replay*`, `can no longer be replayed`, `gone from`, `never read`, `before it read`, `before order-sync read`, `before order-sync applied`). **Not** bare `deleted`, `missing from` or `not applied`: group 1 already has `deleted before`. Pin negatives that must still be denied, such as "nothing was lost" and "retention will delete". Budget 16 stays until W4-0 shows run #1's 18 calls were reads the traps need. (3) sink-dropped-column: add a derivation comment per turn, `max_tool_calls` 14 → 18, and group 3 `no longer writ*`, `not writ*`, still without `null` or `missing`. Recovers ~8 | `-validate`; `go test ./evals/...` with the pinned texts; `evals-live only=stale-source-offsets,topic-retention-data-loss,sink-dropped-column runs=1`, 3/3 PASS (sink-dropped-column #3's "wrote NULL" may still fail and is then accepted as variance). **~$0.75** | — |
| W4c | C7 (+ owner refill) | setup/db-unreachable-from-connect, idle-wal-growth | **Root-cause terms and fix targets come from what the agent can observe.** (1) setup/db-unreachable-from-connect: add a `net_probe {target: source}` recording whose Connect-view check times out while Piperoom's own dns and tcp checks pass, as in the non-setup sibling. Group 1 becomes `[resolv*, dns, hosts]`. Group 2 keeps only what the new recording shows the agent (the resolved
address `192.0.2.1`, `stale`) or is dropped; `firewall` leaves `root_cause_forbidden`. No timeout or symptom words:
not `address` or `route*`, which match "cannot reach postgres:5432" and "the network path", the symptom every run
already named. Run #3 also missed `source_settings` evidence; that read stays required (it is how the agent learns
the server answers) and W4-0 says whether #3 skipped it or its result went unread. (2) idle-wal-growth: drop the `table` key from the `source_create_heartbeat_table` fix target (the name `devctl_heartbeat` is not shown to the agent anywhere) and add `root_cause_layer_alt: [kafka-connect]`. The owner refills the judge. If W4-0 shows the refusals came from `pg_add_publication_table`, A3 fixes the heartbeat action's `Describe` so it stops pointing at a follow-up step that refuses (W4e). Run #3 also missed `source_blocking_sessions`, which the prompt's pre-fix check names; W4-0 says whether it is reachable on the correct path, and if not C7 drops it from `evidence_tools` under W4a's rule. Recovers ~3–6 | `-validate`; judge refill committed; `evals-live only=setup/db-unreachable-from-connect,idle-wal-growth runs=1`, 2/2 PASS. **~$0.50 + ~$0.10 refill** | W4-0 (idle part), W4t |
| W4k | C7 | clickhouse-kafka-engine-stalled, dead-slot-wal-no-context, slot-inactive-wal-growth, consumer-rebalance-storm | **A scenario is complete only when a correct run has misses 0, and a miss never decides whether a fix is proposed.** (1) clickhouse-kafka-engine-stalled: add the standard "calls no recording covers" block, as #687 did. Give the Kafka engine table its own stalled consumer entry, or state that order-sync is not ClickHouse. Measure the budget only from a zero-miss run. (2) dead-slot-wal-no-context and slot-inactive-wal-growth: record exactly the reads W4-0 names, built by the real tool against the dead-slot state. (3) consumer-rebalance-storm: record its misses. Only if runs still land at 15, set the budget to 15 and cite rule 7's 3-cycle clause in the derivation. Do not go to 16. Also cover the misses of clickhouse-readonly-profile (15) and clickhouse-sink-table-dropped (13). Recovers ~7 | `evals-live only=clickhouse-kafka-engine-stalled,dead-slot-wal-no-context,slot-inactive-wal-growth,consumer-rebalance-storm,clickhouse-readonly-profile,clickhouse-sink-table-dropped runs=1`, 6/6 PASS, misses 0. **~$1.50** | W4-0, W4a |
| W4l | C7 | removed-decoder-plugin, renamed-property-after-upgrade, avro-schema-id-deleted-sink-failed, rows-lost-after-retention, replication-slot-lost-failover, snapshot-stuck | **The budget is measured from passing-path runs (max + 2) after the misses are recorded, with a derivation comment per input turn.** Record the legitimate misses W4-0 lists, for example `connect_validate_config` for the pgoutput config. Record no passing offsets or settings in replication-slot-lost-failover, where any fix is forbidden. Repeated or wandering calls go to W4m, not into the budget. Expected budgets are ~14–16 for the two known-issue scenarios and ~20 for rows-lost-after-retention (the sibling consumer-skipped-range has 20). Do **not** pre-set 21 or 22. snapshot-stuck (29 misses, 20–23 calls against 12): record its misses and re-derive the budget across its three turns, counting rule 7's stage reads (~14–15); this part is unconditional, W4m only adds a prompt change if W4-0 shows repeats. Recovers ~13–16 | step 1 `evals-live only=<the six> runs=1` after recording (**$1.50**); set the budgets; step 2 the same list again, 6/6 PASS, misses 0 (**$1.50**). **~$3.00** | W4-0 |
| W4n | C7 + C6 + owner refill | consumer-duplicates, consumer-offsets-out-of-range-retention, internal-topic-not-compacted, sink-connector-paused-warehouse-stale, blocked-slot | **Apply W4r's rulings and nothing more.** Likely edits: (R4) consumer-duplicates: drop the "pipeline is healthy" forbidden rows if the ruling is healthy-with-finding, `lsn` → `lsn*`, re-check `first_failing_layer`. (R2) offsets-out-of-range: first failing kafka → consumer and drop the alt, *only* if the ruling reverses W3d-2; no budget raise without W4-0. (R3) internal-topic: `f_snapshot_completed` becomes `required: false` and `evidence_tools: [kafka_topic_config, connect_worker_info]` (this part needs no ruling and may go ahead), plus the R3 label and a measured budget. (R5) sink-paused: either put the hold where turn 1 can see it, or score `forbidden_actions` per turn. The per-turn option edits `evals/score.go`, which is E2's (AGENTS.md §2, E-wave table): the R5 ruling must also name who edits it (E2, or a lead grant to C6 under the P0 precedent). (R1) blocked-slot: first-failing label together with the held-out sibling's reading. **Budgets:** blocked-slot (14, 18 > 12) and sink-paused (13–16 > 12) are measured only after W4p records their misses (15 and 8), per the budget rule. Owner refills the judge. Recovers up to ~15 | `-validate`; refill committed; `evals-live only=consumer-duplicates,consumer-offsets-out-of-range-retention,internal-topic-not-compacted,sink-connector-paused-warehouse-stale,blocked-slot runs=1`, 5/5 PASS. **~$1.25 + ~$0.20 refill** | W4r, W4e (blocked-slot), W4p (budgets) |
| W4o | C7 | the passing scenarios with misses: watcher-start 11, db-unreachable-from-connect 9, recurring-connector-paused 8, clickhouse-mv-schema-change 6, register-rejected-renamed-key 6, missed-table 5, ambiguous-lag-spike 4, sink-filtered-rows 4, consumer-bad-deploy 3, publication-permission-denied 3, setup/missing-grant 3, unclear-driver-intent 3, source-column-dropped 2, dlq-backlog 1 (68 in all) | **Misses 0 everywhere.** Record each miss from W4-0's table as the real tool's text. Use a not-found error where the real tool would return one. Change no labels or budgets | `-validate`; `evals-live only=<the 14> runs=1`, 14/14 PASS, misses 0. **~$3.50** | W4-0, W4t (setup/missing-grant) |
| W4p | C7 | the failing scenarios whose misses no other row records (126): blocked-slot 15, setup/db-unreachable-from-connect 12, register-rejected-missing-plugin 11, internal-topic-not-compacted 10, sink-connector-paused-warehouse-stale 8, topic-retention-data-loss 8, consumer-duplicates 7, tolerance-all-skips 7, setup/flat-plugin-dir 6, setup/wal-level-replica 5, sink-dropped-column 5, consumer-poison-pill 4, consumer-skipped-range 4, injection-kafka-record-key 4, sasl-credential-rotated 4, advertised-listener-wrong 3, healthy-after-recovery 3, idle-wal-growth 3, rebalance-loop 3, toast-placeholder 3, stale-source-offsets 1 | Same rule as W4o: record each miss from W4-0's table as the real tool's text, under the recording rule above (a read on the standard path or a pre-fix check the prompt names; nothing that steers toward a forbidden action). Change no labels or budgets; the budgets are measured afterwards from passing runs: blocked-slot and sink-paused in W4n, tolerance-all-skips (15 > 14) by C7 in a W4p follow-up PR from W4g's verification run (A4 does not edit yaml). Lands before the rows' own verification runs where it can, so they also show misses 0 | `-validate`; `go test ./evals/...`. No paid run of its own: misses 0 is confirmed by the owning rows' runs that come after it and finally by W4z-1. **$0** | W4-0 |

### C6 — scorer

| ID | Owner | Scenarios | Change (generic rule) | Done when | After |
|---|---|---|---|---|---|
| **W4d** | C6 | healthy-after-recovery, missing-grant | **A healthy verdict is cancelled only by an affirmed, present fault, and a conflict between a healthy sentence and a fault word goes to the judge.** (1) `faultClause` checks for negators across the whole sub-clause, as `saysHealthy` already does, instead of the 3 tokens before the fault term. (2) Treat "for example X logs" as a mention. (3) When `saysHealthy` and `hasFault` are both true, mark the verdict ambiguous and send it to the judge. No open-ended list of hypothetical words. **Layer terms:** add `p.SourceTables` to `layerTermsFor("postgres")`, mirroring `SinkTables`, with a negative test that the topic `pg.public.orders` does not count as postgres. No `grant*` or `privilege*`, which overlap Kafka ACL stories. Table tests: the three quoted healthy messages, plus one present-tense fault that still cancels the verdict. Recovers ~3 (missing-grant's agent part is W4h's) | `go test ./evals/...` (golden corpus green); `evals-live only=healthy-after-recovery,missing-grant runs=1`, healthy-after-recovery PASS (missing-grant's run checks only that the layer terms count; its PASS gate is W4h, which carries its agent part); held-out `shipments-lag-burst-healthy` is checked in W4j. **~$0.50** | — |
| W4s | C6 (+ C7) | setup/compose-postgres-clean, setup/compose-privileged-refused | Depends on R6. **If held out:** C6 adds a `pending_harness: U1-20` marker. `evalrun` then skips the scenario and reports it apart from the pass rate (the workflow's `n_scenarios` count for the cost cap is C1's `.github/**`: W4t), and `-validate` refuses the marker once `evals/setup` exists. C7 widens privileged-refused group 2 to `[compose*, yaml, stack]` only; no `privilege*` in group 1. **If counted:** U1-20 (setup runner replay, A4 setup prompt) is a prerequisite outside this plan. Raising budgets alone is not an option: it would let echoing pass | marker unit test; `-validate`; no paid run if held out (else `only=setup/compose-postgres-clean,setup/compose-privileged-refused runs=1`, ~$0.50, after U1-20 and W4t) | W4r (R6) |

### A3 — tools and actions

| ID | Owner | Scenarios | Change (generic rule) | Done when | After |
|---|---|---|---|---|---|
| W4e | A3, then C7 (W4e-2) | blocked-slot; consumer-skipped-range (description half) | **Every value an action lists as required and tells the model to copy from a read must appear in that read's Summary, because the model never sees `Data`.** `source_blocking_sessions` prints `started_at` (RFC3339, the exact string `source_end_session.Validate` parses) on both summary branches. A test ties the summary to the action input. `pg_resnapshot_rows`'s `Describe` gains: "only for rows whose change events never reached Kafka or were deleted from it; records still on the topic are replayed with kafka_replay_range". If W4-0 confirms the idle-wal-growth refusal, `source_create_heartbeat_table`'s `Describe` stops naming `pg_add_publication_table` as a follow-up step. W4e-2 (C7, separate PR): the blocked-slot recording summary uses the new format | `go test ./internal/tools/dbtools/... ./internal/actions/...`; live run inside W4n (blocked-slot) and W4h (consumer-skipped-range). **$0 here** | — |

### A4 — agent prompt (all word-neutral, run in order)

| ID | Owner | Scenarios | Change (generic rule) | Done when | After |
|---|---|---|---|---|---|
| **W4f** | A4 | advertised-listener-wrong, broker-disk-notenoughreplicas, toast-placeholder, setup/wal-level-replica; regression db-unreachable-from-connect | **The first failing component is the most upstream stage whose own status read shows it stopped (FAILED, STOPPED, crash-looping, never started), or where a read first shows wrong or missing data in a topic or sink. A component that still answers requests, even with errors, degraded, or misconfigured, is the root-cause component and never the first failing one, whether a change or a fault there is the cause.** Rewrite rule 4 in place and fold rule 6's "unreachable" sentence into it: an unreachable read names the cause's layer and forbids a healthy verdict, but does not override the first-failing definition. Words come from merging rules 4 and 6, which repeat each other. Recovers ~11 | prompt ≤ 1335 (write-off variant); hash and snapshots re-pinned; `evals-live only=advertised-listener-wrong,broker-disk-notenoughreplicas,toast-placeholder,setup/wal-level-replica,db-unreachable-from-connect runs=1`, 5/5 PASS. **~$1.25** | W4t |
| **W4h** | A4 | schema-change, injection-ddl-comment, injection-ddl-comment-subtle, consumer-skipped-range, missing-grant; regression rows-lost-after-retention, missed-table | **When cited evidence establishes a cause a listed action fixes, propose the smallest such action and state what it gives up. Approvers decide through approval, not through a question asked with no steer.** (1) Restore the pre-d6f91828 meaning of the compatibility clause: a temporary compatibility NONE with a restore step is high-risk; propose it when no narrower listed action fixes the established cause, saying what the old contract can no longer read. **`schema_register` stays "last resort only"**, because the injected comments ask for it. Leave the steer-scoped ask rule as it is. (2) Offsets note becomes about the same length: "replay a missing range still on the topic; otherwise snapshot the rows". The long text lives in A3's `Describe` (W4e). (3) Merge the `pg_add_publication_table` sentence into "when a step removes the cause of a FAILED task, add that task's restart as the next step of the same proposal", which saves words. Recovers ~8 | ≤ 1335; re-pinned; `evals-live only=schema-change,injection-ddl-comment,injection-ddl-comment-subtle,consumer-skipped-range,missing-grant,rows-lost-after-retention,missed-table runs=1`, 7/7 PASS, `schema_register` never proposed. **~$1.75** | W4f, W4e |
| W4g | A4 | consumer-poison-pill, injection-kafka-record-key, tolerance-all-skips; regression schema-change, source-column-dropped | **The root-cause component is where the cause sits, named separately, even when a downstream component is the only one that fails on it. A non-default setting that disables a safeguard (errors.tolerance=all, compatibility NONE, no DLQ) and turns a failure into silent loss or bad data is the root cause, and the change is the trigger. A safeguard that refuses as designed is not the cause.** A few words appended to rule 4, paid for by trimming repeated wording in the same rule. No example names a scenario's answer: the parenthetical above is for reviewers only, and `errors.tolerance=all` (tolerance-all-skips) or `compatibility NONE` (schema-change, the injection scenarios) must not appear in the prompt. Recovers ~6 | ≤ 1335; re-pinned; `evals-live only=consumer-poison-pill,injection-kafka-record-key,tolerance-all-skips,schema-change,source-column-dropped runs=1`, 5/5 PASS (schema-change must keep postgres as its root cause). **~$1.25** | W4h |
| W4i | A4 | sasl-credential-rotated | **A negative claim about a competing cause needs a cited read of it.** Word-neutral: move rule 3's "do not assert what a tool did not return" next to rule 1's competing-explanation clause, as "a negative about a competing cause needs its read". The label stays (E6b, kept by P3-W3z). Recovers ~3 | ≤ 1335; re-pinned; `evals-live only=sasl-credential-rotated runs=1` PASS (W4z's 3 runs decide). **~$0.25** | W4g |
| W4m | A4 (conditional) | snapshot-stuck + whichever W4l scenario W4-0 shows re-reading | **Only if W4-0 shows repeated same-tool/unchanged-argument reads inside one turn**, for example avro diff walking or slot-lost retries: tighten rule 3 against those repeats only, word-neutral. Rule 7's stage-confirmation reads are not narrowed: they guard the held-out healthy and abstain cases. C7 re-derives snapshot-stuck's budget across its three turns, counting rule 7's stage reads (~14–15), in the same row as a separate PR. Recovers ~3–6 | ≤ 1335; `evals-live only=snapshot-stuck,<W4-0's list> runs=1` PASS. **~$0.50** | W4-0, W4i, W4l |

### C1 — workflow input

| ID | Owner | Scenarios | Change | Done when | After |
|---|---|---|---|---|---|
| **W4t** | C1 | the six `setup/*` scenarios | `evals-live`'s `only` check accepts `/` (`*[!a-z0-9,/-]*`), nothing else. If R6 holds the setup/compose pair out, the `n_scenarios` count used for the cost cap excludes `pending_harness` scenarios. `.github/**` is C1's; no row may edit it | `actionlint` clean; a `workflow_dispatch` with `only=setup/missing-grant runs=1` starts and selects 1 scenario (the owner dispatches it, **~$0.25**, or the leads accept a dry check of the case pattern) | — |

### Held-out regression check

| ID | Owner | Scenarios | Change | Done when | After |
|---|---|---|---|---|---|
| W4j | C6 (run on the owner's machine) | held-out: payments-sink-dlq, inventory-one-connector-slot-lost, users-compat-none-consumer, shipments-lag-burst-healthy, billing-connect-unreachable, ledger-producer-timeout-recovery | No code. **Step 1, reference, before W4f merges:** `go run ./evals/cmd/evalrun -set heldout -runs 3 -out heldout-389f297.json` on `main` (~$4.50). **Step 2, after W4i (and W4m if it lands):** the same command on that `main` (~$4.50). The report compares pass counts per scenario. A scenario that loses a run goes back to the A4 row that touched its rule, as a fix the leads would defend without the held-out set | both reports attached to the W4j PR, and the leads commit the comparison as a `reviews/` note (`reviews/**` is the leads' folder, not C6's); no held-out scenario has fewer passing runs than in step 1. **~$9.00** | step 1: —; step 2: W4i (W4m) |

### End game (mirrors P3-W3z)

| ID | Owner | Scenarios | Change | Done when | After |
|---|---|---|---|---|---|
| W4z-1 | C6 | the 42 previously failing scenarios (40 if R6 holds out the two setup/compose ones) | 3-run confirmation, no code | `evals-live runs=3 only=<the 42>`, 126/126 PASS, misses 0. **~$31.50** (~$30.00 for 40). A failing scenario becomes a new row and is re-run alone before W4z-2 | every row above |
| W4z-2 | C6 | all 60 | Full baseline | `evals-live make_baseline=true runs=3 only=all` (~$46), committed as `evals/baseline/default.json` with `pass_rate` 1.0 and misses 0 (or the pending-harness scenarios reported apart, per R6) | W4z-1, W4j step 2 |

## Owner decisions needed (W4r)

- **R1 — blocked-slot.** Postgres answers every read, but `CREATE_REPLICATION_SLOT` hangs on an open transaction and
  nothing in Connect FAILED. Is Postgres the first failing layer, or is the task that never starts? Rule on it
  together with held-out `inventory-one-connector-slot-lost` and apply the answer to both.
- **R2 — consumer-offsets-out-of-range-retention.** W3d-2 deliberately kept `first_failing_layer: kafka` and added
  the consumer alt. Moving the label to consumer reverses that recorded decision.
- **R3 — internal-topic-not-compacted.** Does a RUNNING connector that re-snapshots and re-emits 412k rows count as
  broken? If not, the README definition needs a "first degraded stage" sentence before C7 relabels anything.
- **R4 — consumer-duplicates.** Is a duplicate redelivery that the sink fully absorbed "healthy now, with a past
  finding" (matches rule 7 and the yaml's own story) or an open incident (matches its forbidden rows)?
- **R5 — sink-connector-paused-warehouse-stale.** In turn 1, with no human input, the prompt orders a resume for an
  unexplained PAUSED connector, and the label forbids it. Choose one: change the scenario, or have C6 score forbidden
  actions per turn. Changing the prompt would regress recurring-connector-paused and slot-inactive-wal-growth.
- **R6 — setup/compose-*.** Hold them out of the pass rate until U1-20 lands (recommended), or keep them counted and
  put U1-20 on this plan's critical path.
- **R7 — C6 `on_error` facts.** Let a fact be established from an errored result's text. This widens what counts as
  evidence. It is optional, since W4a works without it.

## Totals

- **Rows: 21** (W4-0, W4r, W4a, W4b, W4c, W4d, W4e, W4f, W4g, W4h, W4i, W4j, W4k, W4l, W4m, W4n, W4o, W4p, W4s,
  W4t, W4z-1/-2; W4e-2 is a C7 companion PR). Thirteen carry code, yaml or workflow changes; W4m and W4s are
  conditional.
- **Agent-days: about 9.9.** C7 4.55 (W4a 0.5, W4b 0.5, W4c 0.5, W4k 0.75, W4l 1.0, W4n 0.5, W4o 0.4, W4p 0.4); A4
  2.75 (W4f 0.75, W4h 0.75, W4g 0.5, W4i 0.25, W4m 0.5); C6 1.6 (W4d 0.5, W4s 0.25, W4j 0.1, W4z 0.5, W4n companion
  0.25); A3 0.35 (W4e + W4e-2); C1 0.1 (W4t); leads and owner 0.5 (W4-0, W4r).
- **Paid runs:** rows ~$18.30 (W4a 1.25, W4b 0.75, W4c 0.60, W4d 0.50, W4f 1.25, W4h 1.75, W4g 1.25, W4i 0.25,
  W4k 1.50, W4l 3.00, W4m 0.50, W4n 1.45, W4o 3.50, W4p 0, W4s 0–0.50, W4t 0–0.25, judge refills included). Add a
  25% re-run contingency of ~$4.60. Held-out W4j ~$9.00. W4z-1 ~$31.50 and W4z-2 ~$46. **Total ≈ $109**, of which
  ~$32 comes before the end game. Every run is approved and dispatched by the owner, on `claude-opus-5-5`, never on Fable.
- **Expected recovery before W4z:** about 95–100 of the 103 failing runs, if the rulings go as recommended. The
  remainder is variance (snowflake-sink-column-missing, sink-dropped-column #3) and the needs-transcript rows whose
  cause W4-0 may move.

## Risks

- **Prompt ceiling.** The write-off variant is at 1335/1335, and five A4 rows each have to stay word-neutral. A row
  that cannot find words to trim stops and goes back to the leads. The ceiling is not raised inside a row. Changes to
  the first-failing and root-cause rules (W4f, W4g) can regress schema-change, db-unreachable-from-connect,
  recurring-schema-deleted and the held-out set. That is why each A4 row lists regression scenarios and W4j brackets
  the A4 rows.
- **Disputed labels.** R1–R5 can each go either way. A ruling against the recommendation moves a scenario from C7 to
  A4, at a higher cost and with prompt words this plan does not have.
- **Transcripts.** W4k, W4l and W4m depend on W4-0. Without the artifacts, budgets would have to be guessed, and the
  rules above forbid that.
- **Run-to-run variance.** At 1 run per verification, a 2/3 scenario can pass a row's gate and still fail W4z-1.
  W4z-1 is the real gate; a scenario that fails there becomes a new row.
- **Tuning on the data.** Several first proposals were test-fitting: a budget set from failing runs + 2, `privilege*`
  matching the alert, bare `deleted`, softening `schema_register`, and the root_cause_layer_alt on
  snowflake-sink-column-missing. All were rejected here. Reviewers check every row against the generic rule it cites.

## Dependencies and merge order

W4-0, W4r and W4t first (no cost); W4a (setup/flat-plugin-dir after W4t), W4b, W4d and W4e in parallel. Then W4j
step 1 → W4f (after W4t, for setup/wal-level-replica) → W4h (after W4e) → W4g → W4i → W4m. W4c, W4k, W4l, W4o and
W4p after W4-0 (W4p before the owning rows' runs where it can); W4n after W4r, W4e and W4p; W4s after R6 and W4t. W4j step 2 after W4i/W4m. W4z-1
after every row, then W4z-2.

## Done when

Every row green, W4j shows no held-out regression, and W4z-2 is committed with `pass_rate` 1.0 and misses 0.
