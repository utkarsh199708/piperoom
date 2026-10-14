# evals — agent evaluation harness

Shows whether a prompt, tool-description or model change makes the investigation agent better or
worse. The harness runs the **real** agent loop (`agent.Run`, real Claude model) on recorded incidents,
with tool results replayed from a YAML file. It scores every run and compares two reports.

- Tool **definitions** (names, descriptions, input schemas) come from the live code: A2's
  `kafkatools`, A3's `dbtools` and A2's `actions`. A changed tool description is part of what gets
  measured. Only tool **results** are replayed.
- Nothing can write anywhere. The actions get replay clients whose writes always fail, and the
  agent can only propose a fix anyway.
- Proposals go through each action's real `Validate`, checked against the scenario's `resources`,
  and its real `CheckScope`, checked against the scenario's `pipeline` (F10).

## Run it

```sh
go run ./evals/cmd/evalrun -validate                                 # (alias -check) validate scenarios; no API key, no model calls
go run ./evals/cmd/evalrun -scenarios evals/scenarios -out report.json   # needs ANTHROPIC_API_KEY
go run ./evals/cmd/evalrun -validate -set heldout                    # the held-out set (see below)
go run ./evals/cmd/evalrun -set heldout -runs 3 -out heldout.json    # report it; never tune on it
```

Compare two configurations. The model is not deterministic, so use `-runs 3` or more:

```sh
git switch main
go run ./evals/cmd/evalrun -runs 3 -label main -out base.json
git switch my-prompt-change              # prompt.go / tool descriptions edited
go run ./evals/cmd/evalrun -runs 3 -label prompt-v2 -baseline base.json -out cand.json
go run ./evals/cmd/evalrun -runs 3 -label sonnet -model claude-sonnet-5-5 -baseline base.json
go run ./evals/cmd/evalrun -compare base.json,cand.json -fail-on-regression   # exit 2 on regression
```

Optional LLM judge: `judge: claude` in a scenario, or `-judge claude` (overrides the scenario;
`-judge terms` forces the default). It needs a build with `-tags claude` and `ANTHROPIC_API_KEY`:

```sh
go run -tags claude ./evals/cmd/evalrun -judge claude -out report.json   # model: PIPEROOM_MODEL, or -judge-model
```

A model grades the final hypothesis (JSON-schema structured output, low effort, no sampling
parameters: current models reject them; the reply is still parsed strictly) against the scenario `description`,
`first_failing_layer`, `root_cause` and `root_cause_forbidden`, answering
`{layer_correct, root_cause_correct, reason}`; a refusal or a cut-off reply is an error; the hypothesis is wrapped as untrusted data. Its verdict
replaces the terms for root cause and layer; the terms verdict stays in the report as `judged`.
`go test -tags claude ./evals -run TestJudgeAgreement -v` (needs the key) measures judge-vs-terms
agreement. Default tests and CI never call the network.

| flag | default | meaning |
|---|---|---|
| `-scenarios` | `evals/scenarios` | directory of `*.yaml` scenarios |
| `-set` | `default` | `default`: the `-scenarios` directory. `heldout`: its `heldout/` subdirectory, which a default run never reads |
| `-only` | all | comma-separated scenario names |
| `-runs` / `-parallel` | 1 / 1 | runs per scenario / runs at once |
| `-model` / `-effort` / `-max-turns` | config (`PIPEROOM_MODEL`, `PIPEROOM_EFFORT`) / 40 | passed to `agent.Options` |
| `-timeout` | 15m | per run |
| `-label` | — | name of this configuration in the report |
| `-out` / `-md` | — | write JSON report / markdown table (the table is always printed to stdout) |
| `-transcripts DIR` | — | write every run's full event log and result, `DIR/<scenario>-<run>.json` |
| `-baseline FILE` | — | after the run, compare against an earlier report |
| `-compare A,B` | — | compare two reports and exit, with no model calls |
| `-tolerance` | 5 | score drop per scenario that counts as a regression |
| `-fail-on-regression` | off | exit 2 if any scenario regressed |

A scenario **regresses** when its pass rate drops, its mean score drops by more than `-tolerance`,
or it is missing from the candidate report.

Tests: `go test ./evals/...` needs no network and no key. It uses a scripted fake `agent.Runner`
that drives the real replay tools, inbox, sink and proposals. The live test is
`go test -tags claude ./evals -run TestLiveSchemaChange -v` and needs `ANTHROPIC_API_KEY`.

## Scenario format (YAML)

One file per scenario in `evals/scenarios/<name>.yaml`. **Unknown fields are an error**, so a typo
cannot silently drop an expectation. `evalrun -check` and `go test ./evals` check every file
against the live tool and action names.

| field | type | required | meaning |
|---|---|---|---|
| `name` | string | no (defaults to file name) | unique scenario name |
| `description` | string | no | what broke, for humans |
| `pipeline` | object | yes (`id`, `topic_prefix`) | the room's pipeline, same fields as `owners.yaml`: `id`, `title`, `topic_prefix` (required: subject scope checks compare `<topic_prefix>.<source_table>`), `source_tables`, `connectors`, `consumer_groups`, `sink_tables`, `sink_ts_column`. Used for `alert.pipeline_id` and for `CheckScope` on every proposed step |
| `resources.connectors` | []string | no | connectors that exist on the recorded stack (action `Validate` checks against these) |
| `resources.subjects` | []string | no | Schema Registry subjects that exist |
| `alert.title` | string | yes | alert title the room opens with |
| `alert.pipeline_map` | string | no | topology text the model gets up front |
| `phases` | []string | no | the states the recorded stack goes through, start phase first (e.g. `[broken, recovered]`). Absent = a static scenario. See [Phases, sequences and executed fixes](#phases-sequences-and-executed-fixes) |
| `transitions` | list | no | when the run moves to another phase (needs `phases`) |
| `recordings` | list | no | replayed tool results, see below |
| `inputs` | list | no | human input, one item delivered each time the agent goes idle; when none are left, the run ends |
| `expect` | object | yes | how runs are judged, see below |

**`recordings[]`**

| field | type | meaning |
|---|---|---|
| `tool` | string | a live tool name, e.g. `connect_status` (required) |
| `input` | map | fields the call must have, with equal values. Fields not listed are ignored, so leave out ones the model picks freely (e.g. `since`). Empty/absent = matches any call to the tool. Every key must be a property of the tool's live input schema (e.g. `connector` for `connect_status`, not `connector_name`); `-check` rejects anything else with `recordings[i] (tool): unknown input field "x"`, because such a recording could never match a real call |
| `summary` | string | `Result.Summary` the model reads. Write it **byte for byte** as the real tool would print it: same format string as in `internal/tools/{kafkatools,dbtools}` / `internal/topology/render.go`, including the trailing newline. YAML `|` keeps one final newline (`pg_*` tools, `pipeline_map`, `trace_record`); `|-` keeps none (all kafkatools: `connect_*`, `schema_registry_*`, `kafka_*`; `warehouse_freshness`) |
| `data` | any | `Result.Data` (structured detail for the UI) |
| `truncated` | bool | `Result.Truncated` |
| `error` | string | if set, the call fails with this text, as if the target failed (e.g. `"connect: connector not found (404)"`) |
| `phase` | string | answer only in this phase. Absent = every phase |
| `sequence` | list | instead of `summary`/`data`/`truncated`/`error`: results (each with those fields) served one per matching call, in order; the last one repeats |

A `connect_status` recording with `data` also sets the connector state that `connect_resume_connector`
validates against (`state`, `tasks[].state`, as the real tool's Data has them; for a sequence, its first
item). A scenario that proposes a resume must record it. Without it, that `Validate` fails with
`not available in replay`. The restart actions only check that the connector exists, so they do not need
it. The first such recording of a connector wins, and it must not belong to a later phase.

Each recording needs `summary` or `error` (or a non-empty `sequence`). If several recordings match, the
one with the most `input` fields wins; on a tie, one of the current phase beats one without a phase,
then the earlier one in the file wins. Write `error` text in the real
client's format too (e.g. `schemareg: GET /subjects/x/versions/2: not found: …`); when the real message
would name the input, use an exact recording rather than a wildcard. A call that
matches nothing returns the error `no data available for this request` and is counted as a **miss**
in the report. Misses mean the scenario should cover more inputs.

**`inputs[]`**: `kind` (`steer` | `comment` | `approval_result` | `action_result`, required),
`text` (required), `from` (display name; defaults to `driver` / `watcher` / `system` by kind),
`step_id`, `comment_id`. An `action_result` may also name `action` (with `ok`: true or false, and
optionally `params`): it is then an **executed fix step**, see below.

**`expect`**: term groups are an AND of ORs. Every group must match, and a group matches if any one
of its terms appears. Terms match **whole words**, not substrings: text and term are lower-cased and
split on punctuation and camelCase (`PreparingRebalance` = `preparing rebalance`; `log-start` =
`log start`). A multi-word term must occur as consecutive words. A term ending in `*` is an explicit
prefix match on its last word: `rebalanc*` matches `rebalancing` and `PreparingRebalance`, plain
`rebalanc` matches nothing. Write `deserializ*`, `compatib*`, `retain*` where a stem is meant.

| field | type | meaning |
|---|---|---|
| `root_cause` | [][]string | required. The **final** hypothesis (`update_hypothesis`) must match every group |
| `root_cause_forbidden` | [][]string | optional wrong stories. Each **row** is an AND of terms (same matching) that must all sit in **one clause** (clauses split at `; ! ?`, newline and `. `). A clause with a negator (`not`, `never`, `no`, `avoid*`, `without`, `n't`, `instead of`, `rather than`, `against`) is skipped, so "do not reset to latest" is fine. If a row fires the root cause is wrong even when `root_cause` matches, e.g. `[[reset*, latest]]` |
| `first_failing_layer` | string | one of `postgres`, `kafka-connect`, `schema-registry`, `kafka`, `consumer`, `warehouse`. The final hypothesis must name it (the words per layer are `layerTerms` in `score.go`) |
| `evidence_tools` | []string | tools that must each be called at least once |
| `fix` | [][]string | action-name groups that must all appear among the proposed steps. Absent = no particular fix expected |
| `forbidden_actions` | []string | actions that must never be proposed in this scenario. A **rejected attempt** counts too: `propose_fix` drops a proposal whose step fails `Validate` and leaves no event, so the harness records every `Validate` call and fails a run that even tried a forbidden action |
| `max_tool_calls` | int | read-tool budget; 0 = none |

### Example

```yaml
name: connector-down
description: Connector paused; slot stays active, retained WAL grows.
pipeline:
  id: orders
  topic_prefix: pg
  source_tables: [public.orders]
  connectors: [pg-orders-connector]
  consumer_groups: [order-sync]
  sink_tables: [analytics.orders]
  sink_ts_column: updated_at
resources:
  connectors: [pg-orders-connector]
  subjects: [pg.public.orders-key, pg.public.orders-value]
alert:
  title: "Warehouse analytics.orders stale for 9 min"
  pipeline_map: |
    pipeline orders
    2 connect:pg-orders-connector [kafka-connect, owner arjun] WARN state=PAUSED
recordings:
  - tool: connect_status
    input: {connector: pg-orders-connector}
    summary: |-
      pg-orders-connector (source): PAUSED on 172.18.0.6:8083.
      Task 0: PAUSED
  - tool: kafka_consumer_lag
    input: {group: order-sync}
    summary: |-
      order-sync: Stable, 1 members, total lag 0
        pg.public.orders/0: committed 34, end 34, lag 0
inputs:
  - kind: steer
    text: "Who paused it? Check before proposing anything."
expect:
  root_cause:
    - [paused]
    - [connector, connect]
  first_failing_layer: kafka-connect
  evidence_tools: [connect_status]
  forbidden_actions: [schema_set_compatibility, schema_register]
  max_tool_calls: 10
```

The full seed is [`scenarios/schema-change.yaml`](scenarios/schema-change.yaml).

### Phases, sequences and executed fixes

A static scenario answers a call the same way every time. A **phased** one can show lag that changes,
a pipeline that recovers after an approved fix, a retry, and whether the agent re-checks before it
says the fix worked (prompt rule 5).

```yaml
phases: [broken, recovered]          # the first entry is the start phase
transitions:
  - on: {action: connect_restart_task, ok: true}   # an executed proposal step with this action and outcome
    to: recovered
    verify: [connect_status]         # optional: only these reads count as the re-check
  - on: {after_calls: {tool: kafka_consumer_lag, n: 3}}   # time passing, measured in tool calls
    from: broken                     # optional: fire only in this phase
    to: recovered
recordings:
  - tool: kafka_consumer_lag
    phase: broken                    # absent = every phase
    sequence:                        # ordered results; the last one repeats
      - {summary: "lag 1204", data: {...}}
      - {summary: "lag 1310", data: {...}}
inputs:
  - kind: action_result              # the executed fix
    action: connect_restart_task
    params: {connector: pg-orders-connector, task: 0}   # optional: the step this outcome is for
    ok: true                         # required with action
    text: restarted pg-orders-connector task 0 (RUNNING)
expect:
  max_verify_calls: 6                # optional read budget after the first executed step (default 6)
```

- **Phase state is per run.** Every run starts in the first phase. Transitions are checked in file
  order and the first that matches fires; one to the current phase does nothing. A transition with
  `from` fires only while the run is in that phase.
- **`on.action`** fires when the harness executes a proposal step with that action and, when `on.ok`
  is set, that outcome (absent matches any outcome).
- **`on.after_calls`** fires once, right after the n-th call of the tool in the run, so the n-th call
  is still answered in the old phase. The count covers every call of the tool with any input, misses
  included, across all phases, and never resets. In a scenario with a recovered phase an `after_calls`
  transition needs `from`, so time passing cannot undo a fix.
- **Sequences** advance per recording: each matching call gets the next result, and the last one
  repeats. A sequence's position lasts for the whole run: a phase change does not reset it, and
  re-entering a phase continues where its sequences were. Specificity is unchanged within a phase.
- **Executed fixes.** The agent can only propose; the harness plays the room and the executor, with
  the room's rules. When an `action_result` input with `action` comes up on an idle, the harness binds
  it to the newest proposal of the run that has a not yet executed step of that action, skipping
  proposals the room would have refused at creation (unknown action, failed `Validate` or
  `CheckScope`) and proposals whose execution already stopped. It then delivers what the room would:
  one `approval_result` per required layer, then the proposal's steps in order, each as an
  `action_result` with `proposal_id`, `step_index` and `ok` set as `internal/room` sets them. Earlier
  steps succeed, the named step gets the input's `ok` and `text`, and the later steps succeed too.
  The executor stops at the first step that fails, so the rest of that proposal never runs and later
  inputs for it are skipped. If the input has `params` and the named step's params do not contain
  them (same rule as a recording's `input`), the step is the wrong fix: it fails with a `not found`
  result and fires no transition. Each executed step can fire a transition. An input with nothing to
  execute is skipped and the next input is delivered on the same idle; skips are listed as
  `unexecuted_inputs` in the run result (and printed by `evalrun`).
- The replay is deterministic for a given call order and safe under concurrent tool calls.

**Re-check rule.** A *recovered phase* is the target of an `on.action` transition with `ok: true`.
Each move into one starts an episode, and an episode is verified when, after the move and before any
message, hypothesis or status claims recovery, the agent makes a read that:
- is answered by a recording tagged with that phase,
- returns something different from what the same call got in the phase the run left (so it really
  shows the new state), and
- uses one of the transition's `verify` tools, when it lists any.

`verified_after_fix` is true when every episode was verified. Recovery claims are judged only once a
fix step has been executed (a claim before any fix, "recovered on its own", is left to the outcome
grading); after that, a claim outside a recovered phase or before its episode's re-check sets
`claimed_recovery_without_check`. A claim is a phrase such as "recovered", "data is flowing again" or
"the issue is resolved" that is not negated or conditional in its clause ("NOT recovered", "should
be flowing again") and whose own clause (bounded by `. ; : ! ? ,`, newline or `—`) neither ends in
`?` nor opens with an auxiliary verb: "The pipeline has recovered, shall I close the room?" is a
claim, "Has it recovered?" is not. Reads after the first executed step are reported as
`verify_calls`, count against `max_verify_calls` (default 6) and not against `max_tool_calls`, which
then budgets the investigation only.

`-check` rejects: an unknown phase on a recording or in `from`, a transition to an unknown phase,
transitions without phases, an unknown `verify` tool, an `after_calls` transition without `from` in a
scenario with a recovered phase, an empty sequence, a sequence plus a top-level
`summary`/`data`/`error`/`truncated`, a transition that can never fire (same trigger and `from` as an
earlier one), `action` without `ok`, `params` without `action`, and a negative `max_verify_calls`. It
warns about a phase no transition leads to, and an action transition no input can fire.

Every run of a phased scenario also reports `phases_visited`, `verified_after_fix`,
`claimed_recovery_without_check`, `verify_calls` and `unexecuted_inputs`; the transcript's `replay`
lists every executed step, transition and phase-tagged answer (with `changed`). Anything that reads
what a recording shows (fact checks, for example) must read every sequence element
(`recordingResults` in `phases.go`), not only the top-level `summary`.

## Held-out set

[`scenarios/heldout/`](scenarios/heldout/) holds incidents on pipelines the prompt was never tuned on
(their own owners fixture, not `orders`). `evalrun` reads it only with `-set heldout`. **Do not tune
prompts or tool descriptions on these scenarios**; report them. See its README.

The held-out files use the shared-schema `expect` keys `outcome`, `root_cause_layer`,
`expected_facts` and `claims`. Until E2's scoring defines them, they are accepted with a warning (any
other unknown `expect` key is still an error), a `healthy` or `abstain` scenario may leave out
`root_cause`, and runs on those two cannot pass.

## Generating recordings

Recordings are the real tools' output, not hand-typed. [`gen/`](gen/) (build tag `evalgen`) holds each
generated incident's state in fake clients and runs the real `kafkatools`, `dbtools` and `topology`
code on top, with timestamps moved to a fixed clock:

```sh
go run -tags evalgen ./evals/gen          # rewrite the generated parts (from the repo root)
go run -tags evalgen ./evals/gen -check   # exit 1 if a file is out of date
```

It owns `pipeline`, `resources`, `alert.pipeline_map` and `recordings` of the held-out files, and only
the phase-tagged and sequence recordings it added to `slot-inactive-wal-growth` and
`consumer-poison-pill`. Comments, description, alert title, phases, transitions, inputs and `expect` are
written by hand. Regenerate after a tool's output format changes on main.

## Scoring (per run, 0–100)

| part | points | rule |
|---|---|---|
| root cause | 35 | final hypothesis matches `expect.root_cause` |
| layer | 10 | final hypothesis names `expect.first_failing_layer` |
| evidence | 10 | share of `evidence_tools` called |
| fix | 20 | `expect.fix` met (or none expected), no forbidden action, and every step passes the action's `Validate` and `CheckScope` |
| citations | 10 | final hypothesis cites step ids, and all of them are real |
| efficiency | 15 | full within `max_tool_calls`, else scaled by budget ÷ calls |

A run **passes** when all of the following hold:
- no run error or timeout
- the root cause matches and the layer is named
- no evidence tool is missing
- the expected fix is proposed
- no forbidden action is proposed or attempted, and no invalid or out-of-scope step is proposed
- it stays within the tool budget
- **verify after fix** (phased scenarios): every move into a recovered phase is re-checked before
  recovery is claimed, no recovery is claimed outside one once a fix ran, and the reads after the first
  executed step stay within `max_verify_calls` (see the re-check rule above). This rule only fails
  runs; the score changes only because post-fix reads leave `tool_calls`

The report also gives:
- `tool_calls_to_root_cause`: read-tool calls made before the first hypothesis that matched
- misses
- action attempts and how many `Validate` rejected (`rejected_attempts`, the "rejected fixes" column)
- tool errors
- idles
- duration
