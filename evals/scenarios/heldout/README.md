# Held-out incident scenarios

**Do not tune prompts, tool descriptions or the agent loop on these scenarios.** They measure
whether a change that helps the default set (`evals/scenarios/*.yaml`, all on the `orders`
pipeline) also helps on pipelines the agent was never tuned on. Read their results, report
them, and if one fails, fix the cause in a way you would also defend without this set. Do not
copy their answers into prompts, runbooks or examples. Do not change a held-out scenario to make
a candidate pass.

`evalrun` reads this directory only with `-set heldout`, so the default run never includes it:

```sh
go run ./evals/cmd/evalrun -validate -set heldout            # validate; no API key
go run ./evals/cmd/evalrun -set heldout -runs 3 -out heldout.json
```

## The incidents

Every pipeline is defined in [`fixtures/owners.yaml`](fixtures/owners.yaml) (its own layer
owners too), none is `orders`, and each has a shape the default set lacks.

| Scenario | Pipeline shape | What happened | Expected outcome |
|---|---|---|---|
| `payments-sink-dlq` | Postgres → Debezium → Kafka → **JDBC sink connector** → warehouse | a new column the warehouse lacks makes every sink write fail; `errors.tolerance=all` sends them to the DLQ while everything stays RUNNING | diagnose: kafka-connect (sink), no fix |
| `inventory-one-connector-slot-lost` | **two source connectors, one topic prefix** | one connector's slot was invalidated (`wal_status=lost`); the other connector is healthy and must not be blamed | diagnose: postgres slot, no fix |
| `users-compat-none-consumer` | single connector | a subject-level compatibility override `NONE` let an incompatible schema through; the consumer crash-loops; lag grows on every read | diagnose: root cause schema-registry, first failing consumer |
| `shipments-lag-burst-healthy` | single connector | a burst of updates set off a static lag alert; lag drains on every read | **healthy**: no incident, no action |
| `billing-connect-unreachable` | single connector | Kafka Connect's REST API times out; the evidence fits several causes | **abstain**: say the cause cannot be determined |
| `ledger-producer-timeout-recovery` | sink timestamp column `posted_at` | task failed on a producer timeout during a broker restart; **phased**: after the approved restart the stack recovers | diagnose, propose the restart, **re-check before claiming recovery** |

Each file's header gives the first failing and root-cause layers, why each forbidden action is
wrong, the correct human fix, the traps and the sources for every error string. The `expect`
blocks use the shared-schema keys (`outcome`, `root_cause_layer`, `expected_facts`, `claims`)
exactly as E2's scoring reads them; `healthy` and `abstain` have no `root_cause`, layers, fix or
claims. Until E2's scoring lands, `evalrun` accepts those keys with a warning, and no run on the
healthy or abstain scenario can pass (`go test` skips their focused runs).

## How the recordings are made

Not by hand. [`evals/gen`](../../gen/) holds each incident's state in fake clients and runs the
real `kafkatools`, `dbtools` and `topology` code on top, scoped with `fixtures/owners.yaml` the
way the agent's tools are scoped to the room's pipeline, with timestamps moved to the fixed
clock 2026-10-11T09:40:00Z. The Connect timeouts of `billing-connect-unreachable` come from the
real Connect client against a server that never answers. Wildcard recordings at the end of each
file answer names the scenario does not know with what the real tools return for them (mostly
the scope refusal).

```sh
go run -tags evalgen ./evals/gen          # regenerate after a tool's output format changes
go run -tags evalgen ./evals/gen -check   # exit 1 if a file is out of date
```

gen writes `pipeline`, `resources`, `alert.pipeline_map` and `recordings`; everything else in a
file (comments, description, alert title, phases, transitions, inputs, expect) is hand-written
and kept. `go test ./evals` runs a focused correct investigation of every scenario (it must pass
with no replay miss) and a schema-change-shaped wrong one (it must fail).
