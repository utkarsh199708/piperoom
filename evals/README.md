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
of its terms appears (case-insensitive substring).

| field | type | meaning |
|---|---|---|
| `root_cause` | [][]string | required. The **final** hypothesis (`update_hypothesis`) must match every group |
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

## Scenarios from shadow runs

A graded shadow room (`docs/design/shadow.md`) is a real incident with a human label. Turn it into a
scenario:

```sh
# save the run as the labeller sees it (after the blind label), data included
curl -s -H "X-User: you" "$SERVER/api/rooms/$ROOM/shadow?include=data,room" > run.json
go run ./evals/cmd/evalrun -from-shadow run.json -o evals/scenarios/my-incident.yaml   # or stdout
go run ./evals/cmd/evalrun -check
```

`-from-shadow` reads one `GET /api/rooms/{id}/shadow?include=data,room` response and writes a scenario
YAML to `-o`/`-out` (stdout when neither is given, so it pipes; warnings go to stderr). It runs the same
validation as `-check` before it writes anything, so a file it writes loads. `-owners` picks the
`owners.yaml` the room's `pipeline_id` is looked up in (default `OWNERS_FILE`, else `dev/owners.yaml`);
`-name` overrides the scenario name (default `shadow-<room id>`). It never calls a model.

| scenario field | from |
|---|---|
| `pipeline`, `resources.connectors` | the room's pipeline in `owners.yaml` (plain fields only) |
| `resources.subjects` | derived: `<topic_prefix>.<table>-key` and `-value` for each source table |
| `alert.title` | `room.created.title` |
| `alert.pipeline_map` | `run.pipeline_map` (capped at 16 KiB) |
| `recordings[]` | each `agent.tool_call` paired with its `agent.tool_result` by `tool_use_id`: `input` without the model's `why`, `summary` (cut to the tool limit), `data`, `truncated`; a failed call (`is_error`) becomes `error` |
| `expect.first_failing_layer` | the human label's `actual_layer` (must be one of the six layers) |
| `expect.root_cause` | the label's `root_cause_terms`, as **one OR group** (any term matches) |
| `expect.max_tool_calls` | calls the run made + 5 |
| `description` | run id, status, label layer and the grade (`layer_correct`, `root_cause`). The schema has no grade field, so the grade lives only here; the grader's notes and the agent's own text are left out |
| `inputs`, `evidence_tools`, `fix`, `forbidden_actions` | not set: the shadow run had no human input and proposed nothing |

Ground truth is the **human** label, never the agent's hypothesis. A run nobody labelled is refused.

Things to know before you commit the file:

- **Recordings cover only the path this run took.** A different investigation will call tools the run
  never called; those calls miss (`no data available for this request`) and show in the report's
  misses. Add recordings by hand for the other branches you want to test. Calls repeated with identical
  input are folded into the first result; `runbook_lookup` is not recorded (it runs for real in an eval).
- **`data` must be in the response.** Without `include=data` the file is refused, because replayed
  `data` is what `connect_resume_connector` validates against. `include=room` is required too: it carries
  the alert and the label.
- **Recorded `data` may hold production values** (hosts, IPs, record values, names) because the agent
  read them from the real stack. Review and redact the file before you commit it. The command prints
  this warning whenever any recording has `data`.
- **Everything in the response is untrusted** (agent and tool output). It is written only through the
  YAML encoder as string and number values and never executed. After encoding, the file is read back
  and compared with what was meant (every recording, the alert, the label terms). That check exists
  because YAML can change meaning on the way back: a `"<<"` map key is a merge key, so a recording whose
  `data` holds one has its `data` dropped (with a warning), and a call whose `input` holds one fails the
  conversion. Sizes are capped (summary 8000 bytes, data 64 KiB per recording, 200 recordings, 16 MiB
  input); data over the cap is dropped with a warning.
- **Failed calls.** An `is_error` result becomes an `error` recording (a leading `error: ` is removed,
  the replay adds its own). A call the agent loop rejected before running it (unknown tool, or input the
  schema does not allow) is skipped with a warning and still counts toward `max_tool_calls`. A
  *successful* result for a tool or input field that no longer exists in this build fails validation,
  and nothing is written.
- **Free-choice inputs.** A recorded input field such as `since`, `limit` or a timestamp matches only
  that exact value. The command lists them per recording; delete the ones whose value does not matter
  (see "Matching" above), or the replay will miss when the agent picks another.
- Read the file once: the root-cause terms were written for a human reading a resolution, so check that
  they are specific enough to fail a wrong hypothesis.

### Partial runs

A run that did not end with a hypothesis has `status` `interrupted` (cut off at room close, or the
process restarted), `budget` (token budget spent), `error`, or `no_hypothesis`. The label still says what
was wrong, so these are written too, with a warning and `PARTIAL RUN` at the start of the description:

- recordings are the calls that **finished**; a call with no result (cut off mid-flight) is skipped and
  warned about, but still counts toward `max_tool_calls`.
- the scenario holds only the evidence the agent had when it stopped. It may be unsolvable from those
  recordings alone. Treat it as a hard case, not a regression baseline: do not use it with
  `-fail-on-regression` until you have added the recordings a full investigation needs, or a run that
  fails it tells you about the recordings, not the agent.
- `max_tool_calls` is a lower bound; raise it when you complete the scenario.
- `queued` and `running` runs are refused (nothing is final), and so is a run with no finished tool call.

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
