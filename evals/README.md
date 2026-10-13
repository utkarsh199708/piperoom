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
`first_failing_layer`, `root_cause_layer`, `root_cause` and `root_cause_forbidden`, answering
`{layer_correct, root_cause_correct, root_cause_layer_correct, reason}`; a refusal or a cut-off reply is an error; the hypothesis is wrapped as untrusted data. Its verdict
replaces the terms for the root cause and both layers; the terms verdict stays in the report as `judged`.

**Word grading is a floor; `-judge claude` is authoritative.** The deterministic grader reads the
agent's words with the rules below. They catch the common ways a text mentions the right words while
denying them, and accept the common ways a correct agent writes, but they are not an understanding of
English: when the two disagree, trust the judge.
`go test -tags claude ./evals -run TestJudgeAgreement -v` (needs the key) measures judge-vs-terms
agreement. Default tests and CI never call the network.

| flag | default | meaning |
|---|---|---|
| `-scenarios` | `evals/scenarios` | directory of `*.yaml` scenarios |
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

A `connect_status` recording with `data` also sets the connector state that `connect_resume_connector`
validates against (`state`, `tasks[].state`, as the real tool's Data has them). A scenario that proposes a
resume must record it. Without it, that `Validate` fails with `not available in replay`. The restart
actions only check that the connector exists, so they do not need it.

Each recording needs `summary` or `error`. If several recordings match, the one with the most
`input` fields wins; on a tie, the earlier one in the file wins. Write `error` text in the real
client's format too (e.g. `schemareg: GET /subjects/x/versions/2: not found: …`); when the real message
would name the input, use an exact recording rather than a wildcard. A call that
matches nothing returns the error `no data available for this request` and is counted as a **miss**
in the report. Misses mean the scenario should cover more inputs.

**`inputs[]`**: `kind` (`steer` | `comment` | `approval_result` | `action_result`, required),
`text` (required), `from` (display name; defaults to `driver` / `watcher` / `system` by kind),
`step_id`, `comment_id`.

**`expect`**: term groups are an AND of ORs. Every group must match, and a group matches if any one
of its terms appears. Terms match **whole words**, not substrings: text and term are lower-cased and
split on punctuation and camelCase (`PreparingRebalance` = `preparing rebalance`; `log-start` =
`log start`). A multi-word term must occur as consecutive words. A term ending in `*` is an explicit
prefix match on its last word: `rebalanc*` matches `rebalancing` and `PreparingRebalance`, plain
`rebalanc` matches nothing. Write `deserializ*`, `compatib*`, `retain*` where a stem is meant.

In a hypothesis a `root_cause` or layer term counts only where no **denial** covers it, and never in a
question (a sentence that ends in `?`). Text is cut into clauses (`; !`, newline, and `.` or `?`
before a space), fragments (at `: ( ) [ ]` and before `but`, `while`, `whereas`, `although`, `though`,
`however`), and inside a fragment at `,`, `and`, `so`, `which`. The denials (`textmatch.go`):

- **to the end of the clause**: `doubt`, `unlikely`, `don't think`/`believe`, and `not`/`n't`/`no`/`never`
  directly followed by `because`, `due`, `caused`, `the case`, `a case` or `that` ("it is not because
  ...", "it is not the case that ...", "not a case where ...", "I doubt that ...");
- **forward to the next `,` / `and` / `so` / `which`**: `no`, `nobody`, `neither`, `nor`, `instead of`,
  `rather than` always ("no amount change"); `not`, `n't`, `never`, `cannot` only before a noun phrase
  or `be` ("not a Connect problem", "it can't be amount"), not before a verb ("cannot register the
  schema", "can't decode the record", "is not active") or a passive ("cannot be read as numeric");
- **backward over the same part of the fragment** (a plain comma list stays one part): `ruled out`,
  `is not the cause`/`problem`/`issue`, `isn't the cause`/`problem`, `not to blame`, `not at fault`,
  `not involved`, `red herring` ("an ALTER TABLE ... is ruled out").

`no longer`, `without` and `nothing` are not denials ("records no longer include amount", "records
without amount", "nothing reads the slot" all state the cause). A term that starts with a negator
(`no longer`, `not avro`, `never reached`, `not the owner`) is its own phrase. For layers,
`healthy`, `fine`, `unrelated`, `unaffected` or `working normally` (and not `not healthy`) clears what
comes before it up to the previous `,` or `and`: "Connect is healthy" does not name Connect, while
"Connect is healthy and order-sync crashes" and "order-sync is stuck on partition 1, partitions 0 and
2 are fine" still name the consumer (and Kafka). The Kafka layer is also named by the pipeline's topics
(`<topic_prefix>.<source_table>`, e.g. `pg.public.orders/1`).

**Known limits, left to the judge** (or, for healthy, ask and abstain, to a later judge extension):
- negation **after** the term: "the amount change did not cause the 409", "that is wrong", "both are
  red herrings" (plural), "we can exclude ...", "I'm not convinced ...";
- "piperoom is not the owner" reads as a denial of `owner` (scenarios add the phrase as a term), and a
  denial set off by commas ("it was not, as some thought, the amount") is not seen;
- a self-answered rhetorical question ("Restarting now? No: it would fail again.", "Why did it fail?
  Because ...") or a quoted "?" counts as asking the driver;
- healthy verdicts with a caveat the rules do not list ("healthy for now", "mostly fine") pass.

| field | type | meaning |
|---|---|---|
| `outcome` | string | how a good run ends: `diagnose` (default), `healthy`, `abstain` or `ask`. See **Outcomes** below. `root_cause`, `root_cause_forbidden`, both layers, `fix` and `claims` are graded only for `diagnose`; validation rejects them on any other outcome |
| `root_cause` | [][]string | required for `diagnose`. The **final** hypothesis (`update_hypothesis`) must match every group, affirmed |
| `root_cause_forbidden` | [][]string | optional wrong stories. Each **row** is an AND of terms (same matching) that must all sit in **one clause** (clauses split at `;`, `!`, newline, and at `.` or `?` followed by a space or the end, so a `?` inside a URL does not split). A clause with a negator (`not`, `never`, `no`, `avoid*`, `without`, `n't`, `instead of`, `rather than`, `against`) is skipped, so "do not reset to latest" is fine. If a row fires the root cause is wrong even when `root_cause` matches, e.g. `[[reset*, latest]]` |
| `first_failing_layer` | string | one of `postgres`, `kafka-connect`, `schema-registry`, `kafka`, `consumer`, `warehouse`: the most upstream layer that is broken. The final hypothesis must name it (the words per layer are `layerTerms` in `textmatch.go`, plus the pipeline's `consumer_groups`, `connectors` and `sink_tables`), not denied and not cleared (see above) |
| `root_cause_layer` | string | the layer the root cause sits in, graded apart from `first_failing_layer` with the same matcher: a schema change fails in Connect first, but the cause is a Postgres DDL |
| `evidence_tools` | []string | tools that must each return at least one **successful** result. A call that errored was attempted, not observed |
| `expected_facts` | list | what the diagnosis must rest on, see below |
| `claims` | list | `[{about: root_cause, facts: [ids]}]`: the final hypothesis's `based_on_steps` must include a step that established at least one of these facts **before** the hypothesis was posted |
| `fix` | [][]string | action-name groups that must all appear among the proposed steps. Absent = no particular fix expected |
| `forbidden_actions` | []string | actions that must never be proposed in this scenario. A **rejected attempt** counts too: `propose_fix` drops a proposal whose step fails `Validate` and leaves no event, so the harness records every `Validate` call and fails a run that even tried a forbidden action |
| `max_tool_calls` | int | read-tool budget (calls attempted); 0 = none |

**`expected_facts[]`**: `id` (unique), `tool`, `match` ([]string), `required` (bool). A fact is
established at a step when the step called `tool`, the result was **not an error**, and every `match`
term occurs in that result's summary (whole words, as above; no negation handling: `active=false` is
data, not a denial). The fact counts only from its own tool: "STOPPED" in `pipeline_map` does not
establish a `connect_status` fact. A diagnosis is judged as of its **final hypothesis**: steps that ran
after it, or that it cites before they ran, do not count for its facts, citations or claims (healthy,
abstain and ask runs end on a message, so their facts count over the whole run). A required fact the
run never established fails it. `-validate` rejects a fact that no successful recording of its tool
contains in full, so a typo fails before any model call.

```yaml
expect:
  root_cause_layer: postgres
  expected_facts:
    - id: f_task_failed
      tool: connect_status
      match: [task 0, failed]
      required: true
    - id: f_ddl_amount
      tool: pg_recent_ddl
      match: [alter column amount, numeric]
  claims:
    - about: root_cause
      facts: [f_ddl_amount, f_task_failed]
```

### Outcomes

| outcome | a run passes the outcome when |
|---|---|
| `diagnose` | root cause, both layers, claims and fix as above |
| `healthy` | no proposal; the final hypothesis, if any, is low confidence or is itself a healthy verdict; and the final hypothesis or the **last** agent message gives a healthy verdict, while the last message states no current fault. A healthy verdict is a phrase (`healthy`, `fine`, `all good`, `nothing is wrong`, `recovered`, `resolved`, `caught up`, `back to normal`, `operating`/`running normally`, `all clear`, `nothing to fix`, `nothing to do`, `no fix needed`, `no action needed`/`necessary`, `no issues`, `stale alert` and similar) with no negator elsewhere in its part of the sentence, not followed by `yet` or `until ...`, with no caveat in its clause (`except`, `apart from`, `other than`, `otherwise`, `would`, `if`), not in a question, not answered by a bare "no", and with no current fault in the same text: `stuck`, `wedged`, `fail*`, `error*`, `crash*`, `broken`, `break*`, `is down`, `stopped`, `paused`, `lagging`, `behind`, `stale`, `degraded`, `unhealthy`, unless negated just before ("no errors", "nothing is stuck"), told in the past in the same part ("order-sync was stuck until 09:24"), about the alert ("this alert is stale"), or a short normal delay ("1 minute behind") |
| `abstain` | no proposal, and no hypothesis or a final one at low confidence. Known gap: a cause claimed only in a message, with no hypothesis, is not seen |
| `ask` | after the first steer or comment reached the agent, an agent message to the room (not a reply to a watcher's comment) has a sentence that ends in `?` (a `?` inside a URL does not count), and the run proposes **nothing**: the run ends without an answer, so a proposal acts on an unanswered question. The scenario needs a `steer` or `comment` input (validation checks): the harness logs each delivered one as `steer.sent` / `comment.added`, and only questions after the first count, so "What broke?" before the steer is not asking. There is no `ask_driver` tool; a question in `post_message` or the agent's own text is how it asks |

Every outcome also fails on a run error, a timeout, a missing evidence tool, a missing required fact,
a forbidden or invalid proposal step, and the tool budget.

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
  root_cause_layer: kafka-connect
  evidence_tools: [connect_status]
  expected_facts:
    - id: f_paused
      tool: connect_status
      match: [task 0, paused]
      required: true
  claims:
    - about: root_cause
      facts: [f_paused]
  forbidden_actions: [schema_set_compatibility, schema_register]
  max_tool_calls: 10
```

The full seed is [`scenarios/schema-change.yaml`](scenarios/schema-change.yaml).

## Scoring (per run, 0–100)

| part | points | rule |
|---|---|---|
| root cause | 35 | final hypothesis matches `expect.root_cause`, affirmed (on other outcomes: the outcome passed, 35 + 10) |
| layer | 10 | share of `first_failing_layer` and `root_cause_layer` the final hypothesis names |
| evidence | 10 | share of `evidence_tools` with a successful result and **required** `expected_facts` established; optional facts count only through claims |
| fix | 20 | `expect.fix` met (or none expected), no forbidden action, and every step passes the action's `Validate` and `CheckScope` |
| citations | 10 | final hypothesis cites steps, all of them exist and succeeded, and every claim is backed by a cited step. At low confidence a cited step may also be an errored one (prompt rule 7: an error is evidence for what is unknown, never proof); it still backs no claim. On other outcomes a run with no hypothesis gets these points |
| efficiency | 15 | full within `max_tool_calls`, else scaled by budget ÷ calls |

A `diagnose` run **passes** when all of the following hold:
- no run error or timeout
- the root cause matches, and both layers are named
- every evidence tool returned a successful result, and every required fact was established
- every claim is backed by a cited step that established one of its facts
- the expected fix is proposed
- no forbidden action is proposed or attempted, and no invalid or out-of-scope step is proposed
- it stays within the tool budget

A `healthy`, `abstain` or `ask` run passes on its outcome rule (see **Outcomes**) plus the same run error,
evidence, fact, action and budget checks.

Per run the result also gives:
- `tool_calls` (attempted), `tool_calls_ok` and `tool_errors`; `evidence_errored` lists evidence tools
  whose every call errored
- `facts_found` / `facts_missing`, `supported_claims` / `unsupported_claims`
- `layer_named`, `root_cause_layer_named`, `correct` (root cause and root-cause layer on `diagnose`, the
  outcome otherwise), `claims_cause` (final hypothesis at medium or high confidence), `abstained` (no
  proposal and no `claims_cause`; on `ask`, asked after the steer and proposed nothing), `asked_driver`, `says_healthy`
- `tool_calls_to_root_cause`: read-tool calls made before the first hypothesis that matched
- misses, action attempts and how many `Validate` rejected (`rejected_attempts`, the "rejected fixes"
  column), idles, duration

The report adds, over all runs (`calibration.go`; runs that ended in a run error are left out):
- **Calibration**, on **diagnose** runs only: confidence is read as a probability (low 0.3, medium 0.6,
  high 0.9) that the stated root cause is right (`correct`: root cause and root-cause layer). The Brier
  score is the mean of (p − right)² over runs with a final hypothesis (0 is perfect); a reliability
  table gives runs and accuracy per confidence; "overconfident" counts high-confidence runs that were
  wrong. Healthy, abstain and ask runs are left out: there a correct answer is often low confidence,
  so their confidence is not a probability of being right. They are measured by abstention instead.
- **Abstention**: how often the agent held back on `healthy`/`abstain`/`ask` scenarios, against how
  often it held back on `diagnose` scenarios, where that is a false abstention.
- **Evidence**: tool calls attempted / ok / errored, expected facts found, claims backed.
