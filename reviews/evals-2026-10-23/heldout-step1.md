# W4j step 1: held-out reference run

Jira KAN-510 (parent KAN-489). Written by the leads' folder rule (`reviews/**`); no code changed.

## Run

| | |
|---|---|
| Workflow run | `evals-live` run 40058038424, job `evals-live` |
| Dispatch | `set=heldout only=all runs=3 max_turns=25` |
| Commit | `e09c27e26a37365b9032a485470a1902f227756f` (main) |
| Date | 2026-10-24 (job 03:43 to 03:47 UTC, 3m21s of evals) |
| Report artifact | `heldout-e09c27e26a37365b9032a485470a1902f227756f` (`cand.json`, artifact id 12546881446, kept 14 days) |
| Prompt hash | `sha256:964870517bc76dd6` |
| Judge | 9 live calls, 0 cached, cap 36; none left ambiguous and unjudged |
| Mode | report only: no baseline comparison, no gate beyond evalrun errors |

Deviation from the ticket: the ticket names main `389f2975` and a local `-out heldout-389f297.json`. This run is on `e09c27e`
via the new `set=heldout` input (PR #959). Step 2 must be compared against this run, not against a `389f297` file, which
does not exist. Whether W4f had already merged by `e09c27e` is not checked here.

## Per scenario (3 runs each)

| Scenario | Pass | Mean score | Root cause / layer | Misses (3 runs) | Cost per run | Main failure |
|---|---|---|---|---|---|---|
| payments-sink-dlq | 3/3 | 100.0 | 100% / 100% | 16 | $0.168 | none |
| inventory-one-connector-slot-lost | 3/3 | 93.3 | 100% / 100% | 7 | $0.248 | none (scores 100, 90, 90) |
| shipments-lag-burst-healthy | 1/3 | 70.0 | 33% / 33% | 9 | $0.153 | final message never states the pipeline is healthy or that nothing needs fixing; run 1 also claimed a cause at medium confidence |
| billing-connect-unreachable | 0/3 | 60.4 | 33% / 33% | 22 | $0.402 | over the 12-call tool budget (14 to 16 calls); runs 1 and 2 also claimed a cause at medium confidence when the evidence names none |
| ledger-producer-timeout-recovery | 0/3 | 70.0 | 100% / 100% | 14 | $0.166 | expected restart fix never proposed; the agent diagnosed the producer timeout, then held back and asked the owner to check the brokers |
| users-compat-none-consumer | 0/3 | 95.0 | 100% / 100% | 4 | $0.218 | root-cause layer named as the consumer, not schema-registry (all 3 runs, identical) |

## Overall

- Pass rate: 7/18 (39%). Mean score 81.5. Root cause and layer each 78%.
- Misses (tool calls with no recording): 72 over 18 runs, 4.0 per run. Tool calls 10.9 per run; 0 of 197 invalid.
- Cost: $0.226 per run mean (p90 $0.383), about $4.07 for the 18 runs.
- Evidence: expected facts found 39/45; claims backed by a cited fact 12/12.
- Calibration: Brier 0.285 over 12 rated runs; medium confidence 6/6 right, high confidence 3/6 right; 3 overconfident (high and wrong), all in `users-compat-none-consumer`.
- Abstention: held back on 3/6 healthy/abstain/ask runs; false abstention 0/12 on diagnose runs.
- Gate: job green by design (held-out gates on evalrun errors only).

## What generalises and what does not

The comparison with the default set is qualitative: the default set's current pass rate was not re-read for this note.

Generalises:
- Straight diagnosis with a clear signal: `payments-sink-dlq` and `inventory-one-connector-slot-lost` pass every run, with evidence backed and few tool calls.
- Evidence discipline: every claim cites a fact, and no tool call was invalid.
- Naming the failing component and the root-cause layer in `ledger-producer-timeout-recovery` and `users-compat-none-consumer`.

Does not generalise:
- Healthy verdicts (`shipments-lag-burst-healthy`, W4d check): the agent finds the pipeline is draining, but 2 of 3 times does not say "healthy / nothing to fix" in the final message, and once names a cause at medium confidence.
- Honest "unknown" under tool budget (`billing-connect-unreachable`): the agent is right that Connect is down and says so, but spends 14 to 16 calls probing silent channels and twice states a cause it cannot back. Mean score 60 is the lowest of the set.
- Proposing the fix (`ledger-producer-timeout-recovery`): diagnosis is right, but the agent withholds the restart because it cannot confirm the broker side is healthy. Right action 0%. This is the one scenario where caution costs the whole row.
- Layer attribution when the visible symptom is downstream (`users-compat-none-consumer`): the agent blames the consumer and the migration, not the registry's compatibility setting. High confidence while wrong, which drives the calibration miss. All three runs fail the same way, so this is systematic, not noise.

Note the shape: three scenarios fail on a check other than root cause (verdict wording, tool budget, fix proposal), so the root-cause number (78%) is better than the pass rate.

## What step 2 compares against

Step 2 (after W4i, and W4m if it lands) runs the same dispatch on that main: `set=heldout only=all runs=3 max_turns=25`.

Compare per scenario passes out of 3, against these step 1 counts:

| Scenario | Step 1 passes | Step 2 must show |
|---|---|---|
| payments-sink-dlq | 3 | 3 |
| inventory-one-connector-slot-lost | 3 | 3 (R1 consistency) |
| shipments-lag-burst-healthy | 1 | at least 1 (W4d check) |
| billing-connect-unreachable | 0 | at least 0 |
| ledger-producer-timeout-recovery | 0 | at least 0 |
| users-compat-none-consumer | 0 | at least 0 |

Ticket rule: no scenario may have fewer passing runs than here. Only `payments-sink-dlq`, `inventory-one-connector-slot-lost`
and `shipments-lag-burst-healthy` can regress in count. The three 0/3 rows cannot lose a run, so for them also compare
mean score (60.4, 70.0, 95.0), misses per run and calibration (Brier 0.285, 3 overconfident). A scenario that loses a run
goes back to the A4 row that touched its rule, as a fix the leads would defend without the held-out set. Nobody tunes on
or copies held-out answers; this note quotes no held-out expected answer.

Caveats: 3 runs per scenario is a small sample, so a one-run swing on `shipments-lag-burst-healthy` is within noise; judge
calls were live (9), so a cached judge in step 2 could move a borderline row.
