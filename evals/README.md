# evals — agent evaluation harness

Shows whether a prompt, tool-description or model change makes the investigation agent better or
worse. Every run goes through **the room's own code path**: a real `room.Service` (over the in-memory
store) opens the room from an alert or from the built-in watcher and starts the agent the way
`cmd/server` does, on the `agent.Endpoint` that `wiring.NewAgentEndpoint` builds. Tool results are
replayed from a YAML file. The harness scores every run and compares two reports.

## The room path

`cmd/server` starts an investigation like this: an alert (or the watcher) calls
`room.Service.OpenAlertRoom`; the service builds the alert's pipeline map from the topology and runs
`Deps.Run`, which is `Endpoint.Run` from `wiring.NewAgentEndpoint`, with its own sink, inbox and
proposal path. The harness (`room_path.go`) runs exactly that, so **a run that passes here passes in a
room**:

| part | in an eval | same as production? |
|---|---|---|
| room opening | `OpenAlertRoom` (alert scenarios) or the real `watch.Watcher` on a fake clock (`start: watcher`) | yes |
| owners, pipeline | the scenario's `pipeline` written to an owners file and read by `owners.Load` | yes (invalid pipelines fail `-validate`) |
| pipeline map in the alert | the room's topology call; the builder returns `alert.pipeline_map` | the text is recorded |
| agent runner | `Endpoint.Run` (`wiring.NewAgentEndpoint`, budgets, triage model) | yes |
| sink, inbox, proposals | the room service's: `CreateProposal` checks (Validate, CheckScope, param keys, plan texts, params hash) | yes |
| steers, comments | `Service.Steer` as the driver (who took control), `Service.AddComment` (an `@agent ` is added when missing: a room delivers only comments that mention the agent) | yes |
| executed fixes | `ApproveProposal` by each required layer's owner, then the room's executor; the action **writes** are scripted (`execScript`) | the writes are not |
| tool results | replayed | no (recorded) |
| model | Claude through the model tap (below), or a fake Messages server in tests | live |

Differences that remain, on purpose: the harness ends a run at the first runner error (a room retries up
to three times), and it holds the agent at each idle until the scripted human has acted (a person is
just slower). Input `kind: approval_result`, and `action_result` without `action`, are refused by
validation: no room can produce them except by executing a proposal.

The room's log is the transcript (with each event's actor); the scorer reads `agent.*` events written by
the agent only, since the room also writes system notes as `agent.message`.

Found by running the room path (for A4): `agent.Alert.Raw` never reaches the model
(`agent.firstUserMessage` renders only the title, pipeline, room id and map), so the watcher's evidence
JSON is invisible to the agent; only its title is. `watcher-start` measures the agent with that title.

- Tool **definitions** (names, descriptions, input schemas) come from the live code: A2's
  `kafkatools`, A3's `dbtools` and A2's `actions`. A changed tool description is part of what gets
  measured. Only tool **results** are replayed.
- Nothing can write anywhere. The actions get replay clients (`replay.go`, `replay_actions.go`) whose
  writes are no-ops that only record the call (what the step would have run), and an approved step on
  the room path is played by the scenario's script, never by a client. The report lists each run's
  recorded writes (JSON `replay_writes`, and the "Replay writes" list under the table): action and
  target only, never a record or config value, at most 20 per run.
- Proposals go through each action's real `Validate`, checked against the scenario's `resources`,
  and its real `CheckScope`, checked against the scenario's `pipeline` (F10).

## Run it

```sh
go run ./evals/cmd/evalrun -validate                                 # (alias -check) validate scenarios; no API key, no model calls; prints notes
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

Optional LLM judge: `judge: llm` in a scenario, or `-judge llm` (overrides the scenario;
`-judge terms` forces the default). `claude` is an alias of `llm` in both places. It needs a build with
`-tags livejudge` (`-tags claude` is its alias until the workflow switches) and the model provider's
credentials: `ANTHROPIC_API_KEY`, or Vertex or Bedrock (below). The judge uses the same provider as the
agent (`PIPEROOM_MODEL_PROVIDER`):

```sh
go run -tags livejudge ./evals/cmd/evalrun -judge llm -out report.json   # model: PIPEROOM_MODEL, or -judge-model
```

The judge is provider-neutral (V9): `evals.NewLLMJudge(llm.Model, model)` grades on any model adapter
(`internal/agent/llm`); evalrun builds it on the Claude adapter for the configured provider. A model grades
the final hypothesis (structured output with the verdict's JSON schema, low effort, no sampling
parameters: current models reject them; the reply is still parsed strictly) against the scenario `description`,
`first_failing_layer`, `root_cause_layer`, `root_cause` and `root_cause_forbidden`, answering
`{layer_correct, root_cause_correct, root_cause_layer_correct, reason}`; a refusal or a cut-off reply is an error; the hypothesis is wrapped as untrusted data. A model
without structured output answers through its adapter's fallback (a forced tool call, read as the verdict,
or plain text) and gets one retry when the reply is not the verdict; a second bad reply is an error. Its verdict
replaces the terms for the root cause and both layers; the terms verdict stays in the report as `judged`.

**Word grading is a floor; `-judge llm` is authoritative.** The deterministic grader reads the
agent's words with the rules below. They catch the common ways a text mentions the right words while
denying them, and accept the common ways a correct agent writes, but they are not an understanding of
English: when the two disagree, trust the judge.
`go test -tags livejudge ./evals -run TestJudgeAgreement -v` (needs the key) measures judge-vs-terms
agreement. Default tests and CI never call the network.

### On Vertex AI or Bedrock

`PIPEROOM_MODEL_PROVIDER=vertex` or `bedrock` sends both the agent and the judge there; the judge mirrors
`internal/agent/endpoint.go` (`evals/judge_provider.go`), so a missing project, region or credential stops
evalrun before the first episode with an error. The model ids must be the provider's: `PIPEROOM_MODEL` (the
agent and, without `-judge-model`, the judge), `-model`, `-judge-model`, and `PIPEROOM_TRIAGE_MODEL` and
`PIPEROOM_TRIAGE_PASS_MODEL` wherever they are used (the pass model defaults to a first-party id). The
model tap only sees the Anthropic API, so results record `prompt_hash: untapped`.

A first Vertex run (one cheap scenario, one run; set `PIPEROOM_MODEL` to the model's id in the Vertex Model
Garden and make sure the model is enabled for the project and region):

```sh
gcloud auth application-default login          # once: application default credentials
export PIPEROOM_MODEL_PROVIDER=vertex
export VERTEX_PROJECT_ID=my-gcp-project
export VERTEX_REGION=us-east5                  # a region the model is offered in, or global
export PIPEROOM_MODEL=<vertex model id>
go run -tags livejudge ./evals/cmd/evalrun -only missed-table -runs 1 -out vertex.json
```

`ANTHROPIC_API_KEY` is not needed. Bedrock is the same with `PIPEROOM_MODEL_PROVIDER=bedrock`, `AWS_REGION`,
AWS credentials from the usual chain (or `AWS_BEARER_TOKEN_BEDROCK`) and Bedrock model or inference-profile ids.
Add `-prices` with the provider's prices to price the run.

Filling the judge cache (below) works the same way: with the variables above set, the judge of `-judge-fill`
calls Vertex (or Bedrock), and each recorded verdict names the judge that gave it as `provider:model`
(for example `vertex:<model id>`; entries recorded before V9 hold the bare model id). The provider is not
part of the cache key, so a verdict stays valid across models and providers. It is still a paid run
that needs the owner's approval; estimate it first:

```sh
go run ./evals/cmd/evalrun -judge-fill evals/judgecache/ambiguous.json -dry-run -prices prices.json   # estimate only
go run -tags livejudge ./evals/cmd/evalrun -judge-fill evals/judgecache/ambiguous.json -judge-max-calls 200   # PAID
```

### Ambiguous answers: tri-state keyword pass and the cached judge (#454)

evalrun's default judge mode is **`auto`** (a scenario's `judge:` field or `-judge` overrides it;
the `evals.Config` library default stays `terms`). The keyword pass grades the root cause, the first
failing layer and the root-cause layer as **pass**, **fail** or **ambiguous** (`tristate.go`). An item
is ambiguous when, in one clause, a negator, rule-out or hedge (the `forbiddenDenials` spans of
`forbidden.go` and the hedge list of `hedge.go`) sits with the item's words (any `root_cause` term or
every term of a `root_cause_forbidden` row; a layer's words for a layer), or when a forbidden row fires
in a clause that has any negator. Every other text keeps exactly the keyword grade.

When any item of a run's final hypothesis is ambiguous, one `HypothesisJudge` verdict replaces the
keyword verdicts of the root cause and both layers, as `-judge llm` does; the keyword ones stay in
the result (`judged`, `ambiguity`). The verdict comes from:

1. the **recorded verdict cache** `evals/judgecache/` (`judgecache.go`): one file per verdict, named
   `sha256(JudgePromptVersion NUL scenario name NUL hypothesis)`. Bumping `JudgePromptVersion` in
   `judge.go` invalidates every entry (`TestJudgePromptVersionPinned` fails when the prompt changes
   without a bump); an entry recorded against other scenario truth (description, layers, `root_cause`,
   `root_cause_forbidden`) is stale and an error, never a verdict. **Only synthetic texts** are cached:
   the fill reads nothing but the committed manifest, and a run never writes the cache;
2. on a miss, the **live judge** (a `-tags livejudge` build), at most `-judge-max-calls` (default 50) calls
   per run. Past the cap the ambiguous items are reported **ambiguous (unjudged)** and count as
   **not passed**. evalrun **refuses to start** in `auto` mode when it may call the live judge but the
   build has none (no `-tags livejudge`): build with `-tags livejudge` (or its alias `-tags claude`), or
   pass `-judge terms`.

`-judge-cache-only` reads only the cache and **fails the run on a miss** (unit tests and CI replays):
a miss never falls back to the keyword grade. `-judge terms` forces the keyword grade (the ambiguity is
still recorded). The report shows `judge: N ambiguous · J judged (C cached, L live) · U ambiguous
(unjudged) · live calls · judge tokens`, and each failed run's ambiguous items and their reasons.

The **ambiguous-set manifest** `evals/judgecache/ambiguous.json` lists every pinned corpus text
(`TestGoldenCorpus` scenario rows and `rootcause_corpus.yaml`) the pass marks ambiguous, with its label
and main's keyword grade. `TestAmbiguousCorpusSet` keeps it equal to the pass
(`go test ./evals -run TestAmbiguousCorpusSet -update-ambiguous`, then review the diff);
`TestTriStateNoFlip` proves a clear text never reaches the judge; `TestJudgedCorpusGrades` checks the
recorded verdicts against the labels and skips while the cache is empty. Filling the cache is a **paid
run that needs the owner's approval**:

```sh
go run ./evals/cmd/evalrun -judge-fill evals/judgecache/ambiguous.json -dry-run -prices prices.json   # estimate only
go run -tags livejudge ./evals/cmd/evalrun -judge-fill evals/judgecache/ambiguous.json -judge-max-calls 200   # PAID
```

The negation, denial and hedge heuristics in `forbidden.go` and `textmatch.go` are **frozen**: a
change there must fix a pinned corpus row with zero flips. A misgraded wording goes into the corpus
with its correct, judge-checked label instead.

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
| `-commit` | VCS stamp, else `git rev-parse HEAD` (+`-dirty`) | revision recorded in every result |
| `-no-tap` | off | skip the model tap; results record `prompt_hash: untapped` |
| `-judge` / `-judge-model` | scenario's `judge`, else `auto` / `PIPEROOM_MODEL` | `terms`, `llm` (alias `claude`) or `auto` (above) |
| `-judge-cache DIR` | `evals/judgecache` | recorded verdicts `auto` reads first |
| `-judge-cache-only` | off | `auto` reads only the cache; a miss fails the run |
| `-judge-max-calls N` | 50 | live judge calls per run in `auto` (and per `-judge-fill`); past it, ambiguous (unjudged) |
| `-judge-fill MANIFEST` / `-dry-run` | — | record verdicts for the manifest's texts (paid, owner approval) / estimate only |

Exit codes: 0 ok, 1 broken (bad input, runner error), 2 regression (`-fail-on-regression`), 3 the model API refused
the request in a way no retry fixes (a 400 whose body says "credit balance", a 401, a 403). On 3 evalrun starts no
further run, marks the affected and remaining runs `infra` (their error starts `infra: `), writes the report and
skips `-baseline`. Infra runs are in no pass rate, score, comparison or paired comparison, and the tables label them
`infra`, so an outage never reads as REGRESSED. A 429 or 5xx is still retried by the SDK as before. Every failing
run line and the "Failed runs" list print the run error, redacted and cut to 200 characters.

A scenario **regresses** when its pass rate drops, its mean score drops by more than `-tolerance`,
or it is missing from the candidate report.

Tests: `go test ./evals/...` needs no network and no key. Most use a scripted fake `agent.Runner` that
the room service starts like the real one; `endpoint_e2e_test.go` runs the real Endpoint against a
local fake Messages server (streamed turns) through the model tap. The live test is
`go test -tags claude ./evals -run TestLiveSchemaChange -v` and needs `ANTHROPIC_API_KEY`.

## Agreement scoring for shadow runs

`evalrun -agreement <dir-or-file>` reads saved `GET /api/rooms/{id}/shadow?include=data,room`
responses (one JSON document per file; `data` is optional) and checks the agent's final hypothesis
against the human's **blind label** (`room.labelled`: `actual_layer`, `root_cause_terms`). It never
calls a server.

```sh
go run ./evals/cmd/evalrun -agreement evals/testdata/shadow-agreement/            # table
go run ./evals/cmd/evalrun -agreement saved/ -out agreement.json                  # + JSON summary
go run -tags livejudge ./evals/cmd/evalrun -agreement saved/ -judge llm           # LLM judge, needs the key
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

**Known limits** (the deterministic mode is a screen, not a grade; use `-judge llm` or the human
grade where it matters):
- A paraphrase that uses none of the label terms scores `cause_agree = no`, however right it is.
- Negation reaches a fixed distance (5 words) and only the cross-comma patterns above; a denial far
  from its term, in another sentence ("Many suspected X. That was wrong.") is not seen.
- A clearing word can clear the wrong term ("the connector failed, the consumer is fine" keeps the
  connector; "the connector task, whose consumer is fine, failed" may not).
- A subject-less denial ("..., which was restarted, is not the problem") negates only the clause that
  holds its subject (plus relative clauses in between); if the true cause shares that clause with the
  denied subject, it is negated too. "not only X but also Y" keeps X.
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
- `-judge llm` (build tag `livejudge`; `-judge claude` and `-tags claude` are the aliases) replaces both verdicts with the LLM judge's, using the label as
  ground truth, the hypothesis in the untrusted wrapper, the configured model, no temperature, and the
  strict schema of the scenario judge. An empty hypothesis never reaches the model.

**Blind rule** (`docs/design/shadow.md` 3.5). The agreement tool is for scoring *after* grading and for
tuning: it **must never be used to show a run's hypothesis, or an automatic verdict, to a grader
before they have graded it.** It reads only files you saved after the blind label. It refuses to score
a run whose status is `queued` or `running` (the whole call fails; no partial table), and an unknown
status. The hypothesis text is untrusted data: it is tokenised and compared, never executed.

Accuracy of the deterministic mode on the scenarios' good, lazy (the alert title) and wrong
hypotheses (scenarios with a root cause only), against outcomes decided by reading each text:
`go test ./evals -run TestAgreementSyntheticAccuracy -v` (98 of 102 over 17 scenarios; cause 51
of 51). Known layer misses: `source-column-dropped` good text blames Postgres (the migration),
the scenario's label says `consumer`; three wrong texts that name a connector or task only as
the thing to change (`sink-connector-paused-warehouse-stale`, `injection-ddl-comment`,
`injection-ddl-comment-subtle`) are read as kafka-connect, a reader says they blame elsewhere.

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
| `resources.connector_configs` | map | no | per connector, config keys merged over its `connect_config` recording; for the keys that tool hides (`publication.*`, `signal.*`), see [Resources for the P3 actions](#resources-for-the-p3-actions) |
| `resources.tables` | map | no | source tables by `schema.table`: `relkind` (default `r`), `replica_identity` (default `d`), `primary_key`, `estimated_rows`, `signal_columns`. A table not listed does not exist |
| `resources.publications` | map | no | publication name → its tables (each in `resources.tables`). A publication not listed does not exist |
| `resources.tables.<t>.key_column`, `.key_ranges` | string, [[from, to]] | no | the table's integer key column and the key ranges that hold rows (both ends included), which `pg_resnapshot_rows` counts (X2e; `replay_actions.go`) |
| `resources.topics` | map | no | topic name → `{partitions: N}`: a topic that exists, empty, but that no read tool may show (the pipeline's `reprocess_topic`, which `kafka_replay_range` writes to) |
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

In a hypothesis a `root_cause` or layer term counts only where no **denial** covers it, and never in a
question (a sentence that ends in `?`). Text is cut into clauses (`; !`, newline, and `.` or `?`
before a space), fragments (at `: ( ) [ ]` and before `but`, `while`, `whereas`, `although`, `though`,
`however`), and inside a fragment at `,`, `and`, `so`, `which`. The denials (`textmatch.go`):

- **to the end of the clause**: `doubt` (not `no doubt`/`without doubt`, which affirm), `unlikely`, `don't think`/`believe`, and `not`/`n't`/`no`/`never`
  directly followed by `because`, `due`, `caused`, `the case`, `a case` or `that` ("it is not because
  ...", "it is not the case that ...", "not a case where ...", "I doubt that ...");
- **forward to the next `,` / `and` / `so` / `which`**: `no`, `nobody`, `neither`, `nor`, `instead of`,
  `rather than` always ("no amount change"; not `no doubt`/`no question`); `rule out`/`rules`/`ruled`/
  `ruling out` unless a negator stands up to two words before ("ruled out the registry", but "cannot
  rule out the registry" keeps it); `not`, `n't`, `never`, `cannot` only before a noun phrase
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
| `root_cause_forbidden` | [][]string | optional wrong stories. Each **row** is an AND of terms (same matching) that must all sit in **one clause** (clauses split at `;`, `!`, newline, and at `.` or `?` followed by a space or the end, so a `?` inside a URL does not split), and in one comma part of it: a comma after a predicate of its own (an auxiliary such as `is`, `was`, `has`) ends a part, so "nothing is lost, the slot is still there" does not tell `lost the slot`, while a comma list or aside ("order-sync, the consumer and the registry all crashed", "the consumer, which many suspected, crashed") stays one part. Denials still reach across commas. A term inside a denial does not count (`forbidden.go`): after a negator (`not`, `never`, `no`, `cannot`, `avoid*`, `without`, `n't`, `instead of`, `rather than`, `against`, `neither`, `nor`) or a rule-out (`rule`/`rules`/`ruled`/`ruling out`, `exclude*`) up to the next contrast word (`but`, `however`, `although`, `though`, `whereas`, `actually`), or before a predicate denial within five words with no `so`/`because`/`since`/`therefore`/`thus`/`then`/`while`/`when`/`until` between ("X is not the cause", "X wasn't", "X cannot be", "X was ruled out", "X is advised against"; but "the slot has become inactive so the snapshot cannot progress" still tells the inactive-slot story). So "do not reset to latest" and "I ruled out a poison pill" are fine, while "the cause is a consumer crash, not the schema", "not only X but" and a negated rule-out ("cannot rule out X", "X has not been ruled out") still tell the story. A negator reaches to the end of its clause unless a contrast word stops it, so "no lag, so reset to latest" is (cautiously) not told. If a row fires the root cause is wrong even when `root_cause` matches, e.g. `[[reset*, latest]]` |
| `first_failing_layer` | string | one of `postgres`, `kafka-connect`, `schema-registry`, `kafka`, `consumer`, `warehouse`: the most upstream layer that is broken. A layer that still serves requests is not the first failing layer even when the human change there is the cause (a hard-deleted schema breaks the sink task or the consumer first; the registry answers): label the broken component, and put the cause in `root_cause_layer`. The final hypothesis must name it (the words per layer are `layerTerms` in `textmatch.go`, plus the pipeline's `consumer_groups`, `connectors` and `sink_tables`), not denied and not cleared (see above) |
| `root_cause_layer` | string | the layer the root cause sits in, graded apart from `first_failing_layer` with the same matcher: a schema change fails in Connect first, but the cause is a Postgres DDL |
| `root_cause_layer_alt` | []string | optional further layers the root cause may defensibly sit in, each one of the six layers; the root-cause-layer grade (keyword and judge) passes when the final hypothesis names `root_cause_layer` **or** any alternative. **Rule:** an alternative is for a scenario with two defensible readings of where the cause sits (the diagnosis names two: a consumer down for 87 h against a 48 h retention is a consumer story and a retention story), never for a scenario whose label is simply wrong (fix the label instead), and **never equal to `first_failing_layer`** (that would collapse the root-cause grade into the first-failing grade, the distinction the field exists for; ruling on #684). Validation refuses an unknown layer, a duplicate of `root_cause_layer`, a repeat, an alternative equal to `first_failing_layer`, and the field without `root_cause_layer`. Scenarios without it grade exactly as before: the report's root-cause-layer column, the tri-state corpus and the recorded judge verdicts are unchanged |
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
| `healthy` | no proposal; the final hypothesis, if any, is low confidence or is itself a healthy verdict; and the final hypothesis or the **last** agent message gives a healthy verdict, while the last message states no current fault. A healthy verdict is a phrase (`healthy`, `fine`, `all good`, `nothing is wrong`, `recovered`, `resolved`, `caught up`, `back to normal`, `operating`/`running normally`, `all clear`, `nothing to fix`, `nothing to do`, `no fix needed`, `no action needed`/`necessary`, `no issues`, `stale alert` and similar) with no negator elsewhere in its part of the sentence, not followed by `yet` or `until ...`, with no caveat in its clause (`except`, `apart from`, `other than`, `otherwise`, `would`, `if`), not in a question, not answered by a bare "no", and with no current fault in the same text: `stuck`, `wedged`, `fail*`, `error*`, `crash*`, `broken`, `break*`, `is down`, `stopped`, `paused`, `lagging`, `behind`, `stale`, `degraded`, `unhealthy`, unless negated just before ("no errors", "nothing is stuck") or counted as zero right before ("0 errors", "zero failed tasks"; not a decimal's fraction, "10.0 errors"), told in the past in the same part ("order-sync was stuck until 09:24") or called transient ("transient lag during a pod crash-loop"), a bare field label of its own ("Failing component:", which the prompt asks every hypothesis to fill), about the alert ("this alert is stale"), or a short normal delay ("1 minute behind", "1m stale", "stale for 1m") |
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
- **Executed fixes.** The agent can only propose; the room executes. When an `action_result` input
  with `action` comes up on an idle, the harness takes the newest proposal of the run that is still
  open in the room and has a step with that action (a proposal the room refused at creation never
  exists), approves it as the owner of each required layer through `ApproveProposal`, and waits for
  the room's executor. The executor runs the steps in order and delivers the approvals and step
  results itself. The writes are scripted: the first step with the input's action gets the input's
  `ok` and `text`, every other step succeeds; the executor stops at the first step that fails, so the
  rest of that proposal never runs and later inputs for it are skipped. If the input has `params` and
  the step's params do not contain them (same rule as a recording's `input`), the step is the wrong
  fix: it fails with a `not found` result and fires no transition. Each executed step can fire a
  transition. An input with nothing to execute is skipped and the next input is delivered on the same
  idle; skips are listed as `unexecuted_inputs` in the run result (and printed by `evalrun`). The
  input's `from` is not used for approvals: the approvers are the run's layer owners.
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

## Learned scenarios

**Where they live: a private directory, never this repository.** This repository is public. Even redacted, a
learned scenario names tables, connectors, consumer groups and hosts, which describe a customer's systems. Keep
learned scenarios in a private repository checkout or a local folder; `evals/scenarios/learned/` here holds only a
`.gitignore` (`*`, `!.gitignore`), so a file dropped there by mistake is never committed. `roomcase` refuses an
`-out` inside this working tree (resolved with `git rev-parse --show-toplevel`, symlinks followed), and has no
default for it.

A closed room that people labelled and confirmed holds everything a scenario needs: the alert, every tool call
and result, the steers, the executed fix and its verification, and two human judgements (`hypothesis.verdict`,
`room.labelled`). `roomcase` turns one such room into a scenario. The input is the room's export, so it needs
no database and no network and runs on any laptop:

```sh
curl -H ... http://piperoom/api/rooms/$ID/export > room.json     # or the download in the UI
go run ./evals/cmd/roomcase -export room.json -owners dev/owners.yaml -out ~/piperoom-learned   # [-force]
go run ./evals/cmd/evalrun -validate -learned ~/piperoom-learned          # also checks pending/
go run ./evals/cmd/evalrun -learned ~/piperoom-learned -runs 3 -out cand.json   # replays the reviewed ones
```

It verifies `integrity.sha256` first (a changed or truncated export is refused), then writes
`<out>/pending/<pipeline>-<yyyy-mm-dd>-<short room id>.yaml`. **Nothing replays `pending/`**: `evalrun -learned DIR`
adds `DIR/*.yaml` to the default set, and `-validate` also parses and checks `DIR/pending/` so a fresh export is at
least well-formed. Without `-learned`, nothing learned is read. A person reviews the file and moves it from
`pending/` up into `DIR` (in the private repository, as a pull request there); the move is the review.

**Who is eligible** (the exporter says which rule failed): the room is closed, `room.created.shadow` is false,
the last `room.labelled` by a person has `cause` and `actual_layer`, and a person confirmed a hypothesis. If a fix
was executed, its `agent.verification` must say `recovered: true`. A room where every verdict is `disputed` is
exported as a **hard case**: `expect.outcome: abstain`, and `hard: true` plus the disputed hypothesis' terms in the
`description`. Scenario validation grades an `abstain` outcome on its own (it rejects `root_cause`,
`root_cause_forbidden` and the layer fields there), so the denied story lives in the description for the reviewer,
not in a field. A hard case is never used as a worked example.

**What is written** (no new scenario fields):

| scenario field | from |
|---|---|
| `name`, `description` | file name; the alert title + the label's `cause` |
| `pipeline`, `resources` | the `owners.yaml` entry of `room.created.pipeline_id` (no `owners:`, `watch`, connections); the pipeline's connectors, plus the connector or subject an executed step names |
| `alert.title`, `alert.pipeline_map` | `room.created`; the first successful `pipeline_map` result |
| `recordings` | each `agent.tool_call` joined to its `agent.tool_result` by `tool_use_id`. Inputs lose the keys the model picks freely (`droppedInputKeys` in `learned.go`: `since`, `until`, `limit`, `max_scan`, `window`, `older_than_minutes`, `max_results`, `query`, `from`, `to`, `at`, `timestamp`). `data` over 4 KB, or a cut `data` (`truncated: true`), is dropped. Two calls with the same tool and input keep their order as a `sequence`. `similar_incidents` is never recorded (it quotes other rooms) |
| `phases`, `transitions`, `inputs` | an executed, verified fix gives `[broken, recovered]`, a transition on its first successful action, and results after that step get `phase: recovered`. Steers, comments (display name only) and the executed steps (`action_result` with `action`, `ok`, `params`) follow in log order |
| `expect` | `outcome: diagnose`; `root_cause_layer` = `actual_layer`; `root_cause` = one single-term group each for the identifiers (`pg-orders-connector`, `public.orders`), numbers (`409`) and the three longest specific words of `cause`, with terms that only repeat the alert title left out (a hypothesis that merely echoes the alert must not pass); `fix` = the action names of the labelled `fix_proposal_id` |

**No customer data in git.** Every string goes through `redact.Text` / `redact.JSON` (summaries, `data`, inputs,
steers, comments, labels, names); then the finished file is read back and a second redaction pass over every
string must change nothing, or nothing is written. This catches rooms closed before the room log was redacted.
Redaction masks secrets (passwords, tokens, DSN credentials, emails). It does not know what a customer name or
a row value is: that is the reviewer's job.

**Dedup and cap.** The first line of the file is `# fingerprint: <pipeline>|<layer>|<sorted cause terms>`. The
exporter refuses a fingerprint already in the learned directory or its `pending/` unless `-force` (a file without the
line is compared by the terms it expects), and allows at most 10 scenarios per pipeline there (learned plus pending); `-force` does not
lift the cap. A neighbour file that cannot be read stops the export, so it cannot hide from the cap.

**Review checklist** (the reviewer of the pull request that moves the file):

1. Read every `summary` and `input`: no row values, DSNs, hostnames of customers, or personal names survive.
2. `expect` says what the person on the room would have said: the layer, the cause terms and the fix.
3. `description` and the alert title carry no customer name; the pipeline block is the pipeline, nothing more.
4. `evalrun -validate` is clean and the file's `# fingerprint:` line is still the first non-comment header.

The learned directory is never tuned by hand. A wrong expectation is fixed by re-labelling the room and exporting it again
(`-force` replaces the pending file); an edit of a learned file by hand is a review comment, not a fix.

Fixtures: `evals/testdata/rooms/*.json` are real export documents, written by
`go run -tags evalgen ./evals/gen -rooms` from the gen fakes and the real tools through `room.Service`
(`-rooms -check` shows a stale file). They include the shapes the exporter must refuse (shadow, unlabelled, open,
fix not recovered) and a room with a planted credential.

## CI (evals-live)

`.github/workflows/evals-live.yml` runs `evalrun -label ci -out cand.json` against the hosted model. Today it
runs only on `workflow_dispatch`, whose inputs pick the scope: `only` (scenario names, or `all`; default
`schema-change`), `runs` (default 1) and `max_turns` (default 25). The nightly `schedule` is commented out
while the API budget is small. The `pull_request` trigger (limited to `internal/agent/**`,
`internal/wiring/model.go`, `internal/config/config.go`, `evals/scenarios/**`) is switched off in the job
(`false &&`); a later PR turns it on. The
repository is public, so the job never uses `pull_request_target`, runs PRs only from this repository's own
branches, and uploads only the report of the committed synthetic scenarios (`cand.json`, 14 days). The learned
set stays in a private store and is never passed with `-learned`. When a baseline file exists the run adds
`-baseline evals/baseline/default.json -fail-on-regression`: the job fails on a regression by the rule above,
or when misses per run (the job's own check) or invalid tool calls per run (`evalrun`, [Invalid tool
calls](#invalid-tool-calls-v9r)) grew over the scenarios both reports ran (an `-only` run is compared with the same
scenarios of the baseline, and a 3-run report with a 1-run baseline per run, not by totals). It prints the
paired comparison and the cost-per-run delta, and never fails on cost alone.

The job builds `evalrun` with `-tags claude` (the alias of `-tags livejudge`, until C1 switches the
workflow), which links the live LLM judge that the default `-judge auto`
needs for an ambiguous hypothesis with no verdict in `evals/judgecache`. The judge is capped by
`-judge-max-calls`: the job sets it to 2 × scenarios × runs (at most two judge calls per episode: root cause
and layer), since evalrun's own default of 50 ran out after about 77 of 90 episodes. Past the cap an ambiguous
hypothesis is reported `ambiguous (unjudged)` and counts as not passed. The job calls the Anthropic API; a
Vertex or Bedrock run is a local run (above).

With `runs: 1`, one flipped result fails the job: missing the expected fix alone costs 20 points, over the
tolerance of 5. Treat a 1-run failure as a lead, not a verdict: run the same scenarios again with `runs: 3`
on the branch and on `main`, and compare the two.

**Add the secret.** Settings → Environments → New environment `evals`. Add `ANTHROPIC_API_KEY` as an
*environment* secret there (not a repository secret), and add the owner under *Required reviewers* so no run
spends money without approval. Without the secret the job prints a `::notice::` and exits 0.

**Produce and commit the baseline.** Run the workflow by hand (Actions → evals-live → Run workflow) with
`make_baseline: true` on `main`, approve the environment, download the `baseline-default` artifact, and commit it
as `evals/baseline/default.json` in a PR. Read the diff of the report, not just the code. Refresh it in the
same PR as any *intended* prompt change. The held-out set is never part of CI or the baseline.

**Cost.** The workflow prices every run (`-prices`, `docs/design/cost.md`). The 1-run baseline of 2026-10-14
(25 scenarios, Opus 5.5, effort high) cost $0.22 per episode on average, so a full `-runs 3` pass (75
episodes) is about $17. That is above $5, so PR runs should cover only the scenarios the change touches,
and the full set runs by hand (or nightly, once the schedule is back on).

`go test ./...` and `go test ./evals/...` never need the key.

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

## Scenarios from shadow runs

A graded shadow room (`docs/design/shadow.md`) is a real incident with a human label. Turn it into a
scenario:

```sh
# After signing in through the browser, use a private cookie jar containing the
# active session cookie (OIDC: __Host-piperoom_session; dev: piperoom_session).
curl -sS --cookie "$COOKIE_JAR" "$SERVER/api/rooms/$ROOM/shadow?include=room" > run.json
go run ./evals/cmd/evalrun -from-shadow run.json -o evals/scenarios/my-incident.yaml   # or stdout
go run ./evals/cmd/evalrun -check
```

`-from-shadow` reads one `GET /api/rooms/{id}/shadow?include=room` response and writes a scenario
YAML to `-o`/`-out` (stdout when neither is given, so it pipes; warnings go to stderr). It runs the same
validation as `-check` before it writes anything, so a file it writes loads. `-owners` picks the
`owners.yaml` the room's `pipeline_id` is looked up in (default `OWNERS_FILE`, else `dev/owners.yaml`);
`-name` overrides the scenario name (default `shadow-<room id>`). It never calls a model. With `-o`,
it refuses to overwrite an existing file.

| scenario field | from |
|---|---|
| `pipeline`, `resources.connectors` | the room's pipeline in `owners.yaml` (plain fields only) |
| `resources.subjects` | derived: `<topic_prefix>.<table>-key` and `-value` for each source table |
| `alert.title` | `room.created.title` |
| `alert.pipeline_map` | `run.pipeline_map` (capped at 16 KiB) |
| `recordings[]` | each `agent.tool_call` paired with its `agent.tool_result` by `tool_use_id`: `input` without the model's `why`, `summary` (cut to the tool limit), allow-listed `connect_status` data only (`name`, `state`, `tasks[{id,state}]`), `truncated`; a failed call (`is_error`) becomes `error` |
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
- **`include=room` is required** because it carries the alert and human label. Tool data is optional.
  Raw result data is omitted except for the `connect_status` fields replay validation needs: connector
  `name`, `state`, and task `id`/`state`. Worker hosts, task errors, row values, DDL result objects,
  and other tool data are not copied into the scenario.
- **Review the generated file before committing it.** Alert text, the pipeline map, tool inputs, and
  summaries can still contain production values such as hosts, keys, or DDL. The command prints this
  warning for every generated scenario. Keep the saved response and cookie jar private and delete them
  when no longer needed.
- **Everything in the response is untrusted** (agent and tool output). It is written only through the
  YAML encoder as string and number values and never executed. After encoding, the file is read back
  and compared with what was meant (every recording, the alert, the label terms). That check exists
  because YAML can change meaning on the way back: a `"<<"` map key is a merge key, so a call whose
  `input` holds one fails conversion. Sizes are capped (summary 8000 bytes, connector data 64 KiB per
  recording, 200 recordings, 16 MiB input); oversized connector data is dropped with a warning.
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

## Watcher-started scenarios

```yaml
start:
  by: watcher                       # default: alert (alert.title opens the room)
  watcher:                          # what the watcher's own client reads return
    connectors:                     # Connect status, one per pipeline connector
      - {name: pg-orders-connector, type: source, state: RUNNING, tasks: [{id: 0, state: FAILED, trace: "..."}]}
    configs:                        # Connect config per connector (slot.name, heartbeat)
      pg-orders-connector: {slot.name: debezium_orders}
    slots:                          # pg_replication_slots rows
      - {name: debezium_orders, active: false, retained_wal_bytes: 188416, wal_status: reserved}
    signals: [connector_down]       # the signals the room must open on
```

The harness runs the real `watch.Watcher` in mode `rooms` (interval 15 s, no jitter) on a fake clock
that moves one tick at a time, with the pipeline's own `watch:` settings (defaults when absent), until
it calls `OpenAlertRoom`; then it stops the watcher. A `for: 3m` costs no wall time. The room's title,
severity, dedup key (`watcher:<pipeline>`) and evidence are the watcher's. `alert.title` must be left
out; `alert.pipeline_map` is still the map. A run fails with an error when the watcher opens no room
within 200 ticks, or opens it on other signals than `signals` (the scenario would measure another
incident). Only `connector_down`, `slot_inactive` and `slot_wal` can fire (no Kafka client, so no
`dlq_growth`).

## Fix targets

`expect.fix` names actions; on its own it passes a restart of the wrong task, or a proposal that lists
every action. `expect.fix_targets` says what each step must aim at:

```yaml
fix:
  - [schema_set_compatibility]
  - [connect_restart_task, connect_restart_connector]
fix_targets:
  schema_set_compatibility: {subject: pg.public.orders-value}
  connect_restart_task: {connector: pg-orders-connector, task: 0}
  connect_restart_connector: {connector: pg-orders-connector}
```

With `fix_targets` set (`fixscore.go`):
- every action in a `fix` group must have a target, and counts only through a step whose params contain
  the target (the recording `input` rule: listed keys present, equal values; one target per action);
- **every** proposed step must match a target. An action listed in `fix_targets` but in no `fix` group is
  an allowed extra; any other step, or a step aimed elsewhere, fails the fix (`fix_unexpected`);
- every proposal holding a fix step must come **after its evidence**: a hypothesis was posted and every
  required `expected_facts` entry was established by a step that ran before the proposal. A right fix
  proposed first and checked later fails (`fix_ungrounded`). Validation therefore requires at least one
  required fact.

`-validate` notes every diagnose scenario with a `fix` and no `fix_targets`.

### Right action proposed (P3g-2)

`fix_ok` only asks that every `fix` group appears among the proposals. The **right-action** score
(`rightaction.go`, result field `right_action`) also asks for the order: the agent's proposal steps, in
the order proposed, must hold the `fix` groups as an ordered subsequence (a step for group 1, then a
later step for group 2, ...), each step on its `fix_targets` target when the scenario sets them. Other
steps may sit in between (`fix_targets` already fails a run on those). `missed-table` is the case it is
for: a snapshot proposed before the table is in the publication snapshots nothing new.

It is reported, not graded: pass, score and `fix_ok` do not change, so scenarios written before it keep
their pass rate. The markdown table has a **right action** column (share of runs that expect a fix,
`-` when none does), the JSON summaries `fix_runs`, `right_actions` and `right_action_rate` (-1 when no
run expects a fix; overall averages it over the scenarios that do), and a failed run lists the group no
step met.

### Resources for the P3 actions

The P3 actions (`pg_add_publication_table`, `pg_snapshot_signal`, `pg_drop_replication_slot`,
`kafka_reset_consumer_offsets`, `dlq_replay`, `connect_snapshot_table`) are registered on replay clients
(`replay_actions.go`), so `fix`, `forbidden_actions` and `inputs[].action` can name them. Their `Validate`
reads more than names. It reads the recordings where a tool shows the state, first recording without a
phase first:

| read | from |
|---|---|
| connector config | the `connect_config` recording of the connector, with `resources.connector_configs` merged over it |
| replication slots | the `pg_replication_slots` recording's data |
| group state and committed offsets (`kafka_reset_consumer_offsets`) | the `kafka_consumer_lag` recording of the group (members, `worst_partitions`: the tool lists 5, so a plan covers at most those) and the log starts of `kafka_topic_offsets` |
| topic and DLQ offsets (`dlq_replay`) | `kafka_topic_offsets`, else `dlq_peek` (`start_offsets` / `end_offsets` of an existing DLQ) |
| tables and publications (`pg_add_publication_table`, `pg_snapshot_signal`) | `resources.tables` and `resources.publications` only: no read tool shows relkind, replica identity or the signal table |

The seven older actions are unchanged: their Connect client still answers no config,
so their `Validate` behaves as before. The P3 registration is `wiring.RegisterP3Actions`, the call production
makes, so the action order is production's (the seven, then Kafka, snapshot and pg).

The group-reset plan covers only the 5 recorded partitions (`worst_partitions`), not every partition of the
group's topics: a scenario that needs a partition outside the 5 cannot record it, and the plan misses it.
DLQ record values are not replayed: `dlq_replay`'s preview says
so, and its `Validate` needs only the offsets. A timestamp offset reset is planned to the log start.

### Resources for the source admin actions (Q2h)

The four source admin actions (`pg_set_replica_identity`, `source_create_heartbeat_table`,
`source_grant_select`, `source_end_session`) are registered after the P3 actions through
`wiring.RegisterSourceActions`, production's call (`replay_source.go`). Their reads answer what the
matching tool recording answers, so a scenario that records the evidence needs nothing more:

| read | from |
|---|---|
| connector config (`database.user`, `connector.class`, `publication.name`) | as for the P3 actions |
| replica identity and primary key | the `source_table_replication` recordings' data, else `resources.tables` (`replica_identity` d/n/f/i, `primary_key`); a table in neither is not replayed |
| role rights | the `source_role_grants` recording whose `connector_user` (or `role_info.role`) is the role; only the recorded tables (`role_info.selectable`) are replayed |
| blocking sessions | the `source_blocking_sessions` recording the tool call with the same `older_than_minutes` would get (the most specific input that matches, as the replayer picks); a session's `user` and `in_scope` are as recorded |
| database settings | the `source_settings` recording (no action reads it today) |

Every admin write (`SetReplicaIdentity`, `GrantSelect`, `CreateHeartbeatTable`, `EndSession`) is a
no-op recorded in the run's write log. `source_end_session` refuses a session whose `user` is the
connector's `database.user` and, in a room, one recorded with `in_scope: false` unless it also has
`blocks_slot: true` (it holds the oldest xid horizon, so it holds back the pipeline's slot): record
the blocker as the room would see it. For such a hidden session the action checks the user through
the unscoped read, which in replay answers the same recording; with the room's blank `user` that
check passes, so the scenario cannot test the connector-role refusal of a hidden session
(`q2_source_test.go` does, with a recording whose `user` is set).

### Worker, version, offset and network reads; the lifecycle actions (Q6d)

The model is offered `connect_plugins` (Q1c), `connect_worker_info` (Q3a), `connect_validate_config`
(Q4a), `connect_source_offsets` (Q5a), `connect_worker_metrics` (Q3b), `connect_worker_logs` (Q3c) and
`net_probe` (Q6a) with their production definitions (`replay_connect.go`); like every read they are
answered from recordings, so a scenario may now record them (ten files do since the Q batch 4
review: `q6_replay_test.go` lists them). Production registers the last three since Q3e and Q6b. Since Q1d the agent itself calls `connect_plugins` with `{}` on its
first `runbook_lookup` and passes the versions from its `data` (`kafka_version`, `plugins[]`) to the
lookup: record it (no input, with data) where the running versions matter; without a recording that
read is a miss (at most two per run) and no versions are passed, as before.

`connect_stop_connector`, `connect_reset_source_offsets`, `connect_create_connector` and
`connect_delete_connector` are registered after the source admin actions through
`actions.RegisterLifecycle`, the call production's wiring makes. Their reads:

| read | from |
|---|---|
| connector names | `resources.connectors` |
| state | the first `connect_status` recording's data; `connect_stop_connector` executed in the run makes it `STOPPED` |
| type (`source`/`sink`) | `connect_status`'s data `type` if a scenario sets it, else its summary's `<name> (<type>):` |
| config | as for the P3 actions (`connect_config` data, `resources.connector_configs`): the reset's Preview and the delete need it |
| stored offsets | the `connect_source_offsets` recording for `{connector}` (or one without input that lists it); numbers stay exact |
| config validation | the `connect_validate_config` recording for `{class}`: its `error_count` and `errors` |

Every lifecycle write (`StopConnector`, `ResetSourceOffsets`, `AlterSourceOffsets`, `CreateConnector`,
`DeleteConnector`) is a no-op recorded in the run's write log, without config values or positions.

### MySQL rooms (Q8j)

A scenario whose `pipeline.source_kind` is `mysql` runs in a MySQL room, built the way wiring builds a MySQL
client set (Q8g): `LiveToolsFor("mysql")` gives dbtools' MySQL list (no `pg_*` tool; `source_row` and
`source_long_transactions` instead), `mysql_binlog_status` and `kafka_schema_history`, and the agent reads the
room as MySQL from that list (its MySQL prompt). Its actions are every production action but the `pg_*` ones,
and the Connect actions get a binlog reader, so `connect_update_connector_config` offers `database.server.id`
and `snapshot.mode`. A Postgres scenario's room is unchanged: `LiveTools()` is `LiveToolsFor("postgres")`, byte
for byte the list it was. `-check` validates a MySQL file against the MySQL tools, and its `fix_targets` against
the MySQL actions; `forbidden_actions` may still name a `pg_*` action.

| read | from |
|---|---|
| binlog status (`@@server_id`, binlog files, GTID sets) | the first `mysql_binlog_status` recording with `data.server` (the tool's own Data) |
| connector config (class, `snapshot.mode`, the pipeline's other `database.server.id`s) | as for the P3 actions (`connect_config` data, `resources.connector_configs`) |

Without a `mysql_binlog_status` recording a `database.server.id` step fails Validate: the id cannot be checked
against the server's. Each scenario row carries its room's `source_kind`, and the
report's per-source pass rate (`by_source` in the JSON, the "by source" line under the table; runs are pooled
within a kind) groups the MySQL and Postgres rooms; a row with no `source_kind` reads as postgres.

## Answers that list every cause

A hypothesis that names every candidate holds the right terms by construction. Two guards keep it from
counting (`hedge.go`):
- **Hedge words** (every scenario): `either` (not one that closes its phrase: "the registry did not
  change either."), `or possibly`, `could also be`, `possible causes`, `one of these`, `not sure whether`
  and similar make the root cause wrong (`hedged`, `hedged_on`), unless a denial stands just before them
  ("there are no other possibilities", "I ruled out the other possible causes"; a negated rule-out,
  "cannot rule out other possibilities", still hedges). The LLM
  judge's verdict cannot undo this.
- **`root_cause_forbidden`** (per scenario): a plain list ("amount changed; order-sync crashed;
  retention deleted records") has no hedge words; only the scenario's wrong-story rows catch it. Name
  the wrong stories a shotgun answer would tell. `-validate` notes diagnose scenarios without any.

Both apply to every hypothesis, so they also govern `tool_calls_to_root_cause` and time to cause.

**Golden corpus** (`golden_corpus_test.go`): every text the deterministic matcher has been judged on
in one table: the existing tests' correct, trap, shotgun and wrong-story answers read with the real
expect of each default and held-out scenario, plus the reproductions of #283, #334 and #335 and every
text quoted in the #395 reviews, and shadow-agreement rows (`agreement.go`). Each row is pinned at
the grade the matcher gives today. A row the matcher gets wrong is a **gap** row: it carries the right
grade and a reason and is logged; it fails once it grades right, so the fix must remove its marker.
Run the corpus after any change to `textmatch.go`, `forbidden.go`, `negation.go`, `hedge.go`,
`agreement.go` or `tokenize`: a change that flips a row is not done. The denial and warning gaps of
#283 and #334 follow the #454 decision: a keyword pass plus the LLM judge, with the keyword
heuristics frozen.

## Time to cause

`time_to_cause` (`ttc.go`) is how long the room took to its first correct hypothesis:
- **start**: the `room.created` event (the room opening; for a watcher room, when the watcher opened it,
  not when the signal began);
- **end**: the first `agent.hypothesis` event written by the agent whose text is a correct root cause by
  the deterministic rule (root-cause groups affirmed, no forbidden story, no hedge). Messages do not count:
  they carry plans and questions, while `update_hypothesis` is the agent's stated belief;
- **measured**: `ms` between the two events' room-log timestamps, `steps` = `agent.step.started` events
  and `tool_calls` = `agent.tool_call` events before the end event, `hypothesis` = its number (1 = first);
- **null** unless the run's final root cause is correct (after the LLM judge, when it graded): a cause
  found and then left has no time to cause. `cause_retracted` says a wrong hypothesis followed the first
  correct one, even if a later one was correct again (then the time is still to the first).

With a fake model `ms` is near zero; it means something on live runs. The report gives the median per
scenario (overall: the median of the scenario medians) and the count of retracted runs.

## Provenance

Every result records `commit`, `model` (configured), `models_used`, `prompt_hash` and `entry` (`alert` or
`watcher`); the report records `commit` and every `prompt_hashes` seen. The prompt hash comes from the
**model tap** (`modeltap.go`): `evalrun` points `ANTHROPIC_BASE_URL` at a reverse proxy on 127.0.0.1
before it builds the Endpoint. The tap forwards every request and response unchanged (headers, the key
among them, are never read), and per room (found by the `Room:` line of the first user message) records
the models named and `sha256` of the request's `system` and `tools`, exactly as sent: any change to the
prompt, a tool description or schema, or the actions in `propose_fix` changes it. More than one hash in a
run is listed in `prompt_hashes`. `untapped`: no tap (`-no-tap`, Bedrock, Vertex, scripted runners);
`none`: tapped, but no model request.

## Invalid tool calls (V9r)

`invalid_tool_calls` counts the tool calls the agent refused **before running them**: the call named no
registered tool (`unknown_tool`), its arguments were not valid JSON (`invalid_json`), or they failed the
tool's input schema (`invalid_input`). The agent marks each such `agent.tool_result` with `rejected` (V1t);
the harness counts those results, each once (`invalid_calls.go`). A tool that ran and returned an error is
not counted, and neither is a result the room wrote as the system.

It is **not** `rejected_attempts` (the "rejected fixes" column): that counts `propose_fix` steps an action's
`Validate` refused, where the model called the tool well and proposed something the action does not allow.
An invalid tool call is the model failing to call a tool at all, the reliability measure of the model gate
(`invalid_tool_calls / tool_calls ≤ 2%`, `docs/design/model-providers.md` §5.1).

- **Per run**: `invalid_tool_calls`, `invalid_tool_call_reasons` (by reason) and `all_tool_calls` (every
  `agent.tool_call`, the rejected ones and the reads after a fix included; `tool_calls` leaves those out).
- **Per scenario and overall** (`overall` in the report): `invalid_tool_calls`, `all_tool_calls` and
  `invalid_tool_call_rate`, the first over the second. Overall pools every tool call (the gate's ratio), it
  is not a mean of scenario rates. The markdown table has an "invalid tool calls" column (count and rate)
  and a line under it with the overall count by reason.
- **Unknown, not zero**: a run that was not checked (an infra run, or a report written before V9r) has no
  counter, and a scenario or overall total over such a run is left out of the JSON and printed `-`.
- **Comparison** (`-baseline`, `-compare`): invalid tool calls per run over the scenarios both reports
  scored, as evals-live compares misses (so a 1-run candidate compares with a 3-run baseline, and an `-only`
  run with the same scenarios of the baseline). Growth is printed in bold and fails `-fail-on-regression`
  (exit 2) without counting as a regressed scenario. When either report lacks the counter for a shared
  scenario, the line says "unknown, not compared" and nothing fails: a baseline written before V9r never
  turns into a regression. It does not change any run's score or pass.

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
- the expected fix is proposed (with `fix_targets`: aimed right, nothing else, after its evidence)
- no forbidden action is proposed or attempted, and no invalid or out-of-scope step is proposed
- it stays within the tool budget
- **verify after fix** (phased scenarios): every move into a recovered phase is re-checked before
  recovery is claimed, no recovery is claimed outside one once a fix ran, and the reads after the first
  executed step stay within `max_verify_calls` (see the re-check rule above). This rule only fails
  runs; the score changes only because post-fix reads leave `tool_calls`

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
- `time_to_cause`, `cause_retracted`, `hedged`, `fix_unexpected`, `fix_ungrounded` (above)
- `right_action` (when a fix is expected): `ok`, the steps that met the `fix` groups in order, and the
  first group no later step met ([Right action proposed](#right-action-proposed-p3g-2))
- misses, action attempts and how many `Validate` rejected (`rejected_attempts`, the "rejected fixes"
  column), idles, duration
- `invalid_tool_calls`, `invalid_tool_call_reasons` and `all_tool_calls` ([Invalid tool
  calls](#invalid-tool-calls-v9r))

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
