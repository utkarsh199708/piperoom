# W4 — evals: full suite to 100%

Planner: `[LEAD-ARCH]`. The full plan (where we are, rules, failure classes K1–K9, the row table, costs and merge
order) is the Jira epic **KAN-489**; each row is its own ticket (the epic's "ROW -> KAN key" list). This file holds
what the epic points here for: the leads' rulings. Row W4r is KAN-491.

The owner delegated the rulings R1–R7 to the leads on 2026-10-23. A ruling changes no yaml, scorer or prompt by
itself; the row named in it applies it.

## Rulings

- **R1 — blocked-slot and held-out `inventory-one-connector-slot-lost`.** A component that answers every read is not
  the first failing one. In `blocked-slot`, Postgres answers every read while `CREATE_REPLICATION_SLOT` waits on an
  open transaction, so the connector task never starts: the task that never starts is the first failing layer.
  `blocked-slot`: `first_failing_layer: kafka-connect`, `root_cause_layer: postgres`. Held-out
  `inventory-one-connector-slot-lost` has the same reading: `first_failing_layer: kafka-connect`,
  `root_cause_layer: postgres`. Who applies it: the `blocked-slot` relabel is C7's, in W4n (KAN-497);
  `replication-slot-lost-failover` gets the same reading in W4l (KAN-496). The held-out label edit is **not** made in
  W4r or by any W4 row: the held-out label owner (E3, `evals/scenarios/heldout/**`) applies it, and nobody tunes on
  held-out data (W4j reads results only). `evals/README.md`'s `first_failing_layer` row carries the one sentence.
  Ruled 2026-10-23 (leads, owner-delegated).
- **R2 — consumer-offsets-out-of-range-retention.** Keep `first_failing_layer: kafka` with the consumer alternative
  (`root_cause_layer_alt: [consumer]`), as decided in P3-W3d-2. No relabel. Ruled 2026-10-23 (leads,
  owner-delegated).
- **R3 — internal-topic-not-compacted.** A RUNNING connector that re-snapshots is not "broken", and no "first degraded
  stage" sentence is added to the definition. The scenario is labelled by the first-failing definition: the most
  upstream component that no longer does its job. Here that is the internal offsets topic, which is not compacted, so
  `first_failing_layer: kafka`. C7 applies it in W4n (KAN-497). Ruled 2026-10-23 (leads, owner-delegated).
- **R4 — consumer-duplicates.** A duplicate redelivery the sink fully absorbed is "healthy now, with a past finding"
  (rule 7, matching the yaml's own story), not an open incident. C7 removes the forbidden rows that contradict it, in
  W4n (KAN-497). Ruled 2026-10-23 (leads, owner-delegated).
- **R5 — sink-connector-paused-warehouse-stale.** Change the scenario, not the scorer or the prompt. C7 changes the
  scenario so that a turn-1 resume of the PAUSED connector is not forbidden, in W4n (KAN-497). Forbidden actions are
  not scored per turn, so R5 needs no `evals/score.go` edit and names no editor for it. Ruled 2026-10-23 (leads,
  owner-delegated).
- **R6 — setup/compose-\*.** The pair `setup/compose-postgres-clean` and `setup/compose-privileged-refused` is
  **held out** of the pass rate until U1-10 lands (A4: setup mode, `wiring.SetupStack`), followed by a C6 eval hookup
  that runs the episode in `Alert.Mode = setup` against the U1-20 replay session.
  - Why: U1-20 (PR #920, `edf4497`) delivered the replay harness, not an agent that uses it. Live run 40030412424
    showed that the agent has no setup tools (10 of 12 calls errored).
  - How: W4s (KAN-501) adds `pending_harness: U1-10`. evalrun reports the pair apart, and `-validate` refuses the
    marker once `SetupStack` exists.
  - Effect: W4z-1 (KAN-511) covers **40** scenarios, and W4z-2 (KAN-512) reports the pair apart. A C1 follow-up
    (after W4t, KAN-509) excludes pending scenarios from `n_scenarios`.

  Ruled 2026-10-23 and amended the same day (leads, owner-delegated).
- **R7 — C6 `on_error` facts.** A fact is not established from an errored result: `on_error` facts stay unsupported.
  W4a works without it. Ruled 2026-10-23 (leads, owner-delegated).

- **R8 — Budgets.** A budget is the highest call count of a passing path plus 2. A run whose only failure is the
  budget counts as a passing path; a run that also failed on anything else does not. A measured budget may lower or
  raise the provisional one, and it overrides a ticket test case that named a different number (rows-lost-after-
  retention: 21). When the turn split of the run cannot be read, the derivation comment gives the standard path's
  per-turn split and says it is not measured. A scenario with no passing path keeps its provisional budget
  (sink-dropped-column stays 18). Ruled 2026-10-24 (leads, owner-delegated).
- **R9 — Evidence lists only what the story needs.** `evidence_tools` names only the reads the scenario's story needs
  to establish the cause (W4a's rule). A read the ticket listed may move to an optional fact when the story does not
  need it: `source_blocking_sessions` in idle-wal-growth (no open transaction), `source_settings` in
  setup/db-unreachable-from-connect. Ruled 2026-10-24 (leads, owner-delegated).
- **R10 — setup/db-unreachable-from-connect terms.** The cause (the Connect container resolves `postgres` to a dead
  address) is not visible to any read tool, so the hypothesis must name name resolution: the root-cause group is
  `[resolv*, dns, hosts, name resolution]`. "network path" and "route*" are not accepted (they passed judged-wrong
  hypotheses), and an echo of net_probe's own "dns ok" line, or a hypothesis that clears DNS, must not pass.
  Ruled 2026-10-24 (leads, owner-delegated).
- **R11 — replication-slot-lost-failover retried reads.** The two reads the agent retries get honest recordings that
  support no fix: `connect_source_offsets` returns the old primary's committed LSN, ahead of the new primary's, and
  `source_settings` the new primary's ordinary settings. The ticket's "record nothing" is superseded; the test is that
  no recording makes a fix look right, and every fix stays forbidden. Ruled 2026-10-24 (leads, owner-delegated).
- **R12 — Model ids.** No model id in commit messages, PR text, code, comments or prose artifacts. A judge-cache
  entry's `model` field is cache-schema data written by the tool and is allowed. Ruled 2026-10-24 (leads,
  owner-delegated).
- **R13 — W4j enabler.** KAN-510's "no code" covered the comparison itself; adding `set=heldout` to `evals-live`
  (C1) is the enabler the dispatch needs and is accepted. Ruled 2026-10-24 (leads, owner-delegated).

### Related rulings

- **W4f first-failing definition (KAN-504).** Rule 4 of the CDC prompt keeps the P3-W3 wording: "the most upstream
  stage whose own status read shows it failed, else the most upstream one that no longer does its job; if a stage
  failed, a component that still answers requests is at most the root cause". This supersedes the ticket's "wrong or
  missing data in a topic or sink" clause, which a review showed would regress `missed-table`,
  `no-replica-identity`, `dead-slot-wal` and `sink-filtered-rows`. Ruled 2026-10-23 (leads, owner-delegated).
- **`evals/score.go` grant for W4d (KAN-500).** The owner granted C6 a lead grant to edit `evals/score.go` (E2's) for
  W4d, under the P0 precedent (AGENTS.md §2, E2 row). The grant covers W4d only; further `score.go` edits stay E2's.
  Ruled 2026-10-23 (leads, owner-delegated).
