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
`first_failing_layer`, `root_cause` and `root_cause_forbidden`, answering
`{layer_correct, root_cause_correct, reason}`; a refusal or a cut-off reply is an error; the hypothesis is wrapped as untrusted data. Its verdict
replaces the terms for root cause and layer; the terms verdict stays in the report as `judged`.
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

## Agreement scoring for shadow runs

`evalrun -agreement <dir-or-file>` reads saved `GET /api/rooms/{id}/shadow?include=data,room`
responses (one JSON document per file; `data` is optional) and checks the agent's final hypothesis
against the human's **blind label** (`room.labelled`: `actual_layer`, `root_cause_terms`). It never
calls a server.

```sh
go run ./evals/cmd/evalrun -agreement evals/testdata/shadow-agreement/            # table
go run ./evals/cmd/evalrun -agreement saved/ -out agreement.json                  # + JSON summary
go run -tags claude ./evals/cmd/evalrun -agreement saved/ -judge claude           # LLM judge, needs the key
```

- **layer_agree**: the layer the hypothesis blames equals the label. The layer is that of the first
  clause that reports a failure (`fail*`, `stuck`, `rejected`, `stopped` ...) and names a layer by
  keyword (`layerTerms` of `score.go` plus a few cause words); after "because", "due to", "caused by"
  the cause side wins; the keyword that comes first in the clause wins. Consumer-group and connector
  names from the run's `pipeline_map` count as keywords. Clauses that clear a layer ("Connect is
  healthy") or deny it ("not the importer") are skipped.
- **cause_agree**: any one label term (the terms are **one OR group**, as `evalrun -from-shadow`
  builds them) is stated outside a negation. A term is negated by: a negator (`not`, `never`, `no`
  except "no longer", `n't`, `without`, `avoid*`, `nor`, `neither`, `instead of`, `rather than`) in the
  five words before it; a clearing word (`healthy`, `fine`, `ok`, `normal`, `ruled out`, ...) in the
  five words after it; a negator that ends an earlier comma clause of the same sentence ("It is not,
  as the on-call assumed, the compat mode"); or a later subject-less denial clause ("The bulk import,
  which many suspected, is not the problem"). A contrast word (`but`, `however`, `actually`,
  `although` ...) ends the reach, except that clearing follows "but it/this/that/they"
  ("the task failed but it was fine"). So "it is not the bulk import; it's the poll timeout" agrees
  with `poll timeout` and not with `bulk import`. Matching is `score.go`'s whole-word matcher.
  Labellers must pick distinctive terms: a term the whole pipeline mentions makes every run agree.
  The `Reason` says which term matched, or that terms occur only negated.

**Known limits** (the deterministic mode is a screen, not a grade; use `-judge claude` or the human
grade where it matters):
- A paraphrase that uses none of the label terms scores `cause_agree = no`, however right it is.
- Negation reaches a fixed distance (5 words) and only the cross-comma patterns above; a denial far
  from its term, in another sentence ("Many suspected X. That was wrong.") is not seen.
- A clearing word can clear the wrong term ("the connector failed, the consumer is fine" keeps the
  connector; "the connector task, whose consumer is fine, failed" may not).
- "but another subject" ends a clearing scan, so "failed but the consumer is fine" is read correctly,
  while "failed but this was expected" is read as cleared.
- Layer detection is keyword based: a cause described in another layer's words (a Postgres migration
  that breaks the consumer) is attributed to the layer whose word comes first in the failure clause.
  `Reason` lists "also names ..." layers from that clause so the ambiguity is visible.
- Hypotheses are read as text only; step citations, tools called and proposals are ignored.
- A run with no hypothesis (`no_hypothesis`, `error`, `budget` before one, `interrupted`) scores
  `no`/`no` and still counts in the totals, like the metrics denominator ("every closed room").
  A response without `room.labelled` (saved without `?include=room`, or not yet labelled) is listed
  as skipped.
- **Totals**: layer, cause and both agreement rates over scored rooms. When rooms carry a grade
  (`room.graded`, the last one wins): **auto equals human** is how often `cause_agree` equals the
  human's `root_cause` (`yes` = true, `no` = false; `partial` has no yes/no, so it is counted apart and
  left out of the rate) and how often `layer_agree` equals the human's `layer_correct`. Disagreements
  are split into auto-yes/human-no and auto-no/human-yes.
- `-judge claude` (build tag `claude`) replaces both verdicts with the LLM judge's, using the label as
  ground truth, the hypothesis in the untrusted wrapper, the configured model, no temperature, and the
  strict schema of the scenario judge. An empty hypothesis never reaches the model.

**Blind rule** (`docs/design/shadow.md` 3.5). The agreement tool is for scoring *after* grading and for
tuning: it **must never be used to show a run's hypothesis, or an automatic verdict, to a grader
before they have graded it.** It reads only files you saved after the blind label. It refuses to score
a run whose status is `queued` or `running` (the whole call fails; no partial table), and an unknown
status. The hypothesis text is untrusted data: it is tokenised and compared, never executed.

Accuracy of the deterministic mode on the 6 scenarios' good, lazy (the alert title) and wrong
hypotheses, against outcomes decided by reading each text: `go test ./evals -run
TestAgreementSyntheticAccuracy -v` (35 of 36). Known miss: `source-column-dropped` good text blames
Postgres (the migration), the scenario's label says `consumer`.

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

The report also gives:
- `tool_calls_to_root_cause`: read-tool calls made before the first hypothesis that matched
- misses
- action attempts and how many `Validate` rejected (`rejected_attempts`, the "rejected fixes" column)
- tool errors
- idles
- duration
