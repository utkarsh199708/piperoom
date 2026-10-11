# C6 — Agent evaluation harness (Claude cloud agent, Sonnet)

> **Status (lead, 2026-10-11 23:50 IST):** implemented as **PR #7** (`claude/jolly-gates-o8xtay`). It uses the
> existing `agent.Runner` seam, so the `ModelClient` precondition and contract change below were **not applied
> and are not needed** — ignore them. Open review points for round 2: `evals/scenarios/README.md` is C7's;
> seed `schema-change.yaml` replay summaries must match the real tools byte for byte (trailing newline: `|` for
> `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`; `|-` for the kafkatools ones); seed
> `alert.pipeline_map` must equal the `pipeline_map` replay summary; consider a `must_call` key and scoring
> `propose_fix` attempts that fail `Validate`.


Read first: `AGENTS.md`, `docs/collab.md`, `tasks/A4-agent-loop.md`, and `dev/scenarios.md` §1. Then read the code: `internal/agent/contract.go`, `internal/agent/run.go`, `internal/agent/run_test.go` (it has a scripted model), `internal/agent/fake/faketools.go`, `internal/tools/tool.go`, `internal/actions/action.go`, `internal/actions/kafka_actions.go`, and `cmd/investigate/main.go` + `wire.go`.

You own a **new** top-level folder, `evals/**` (package `piperoom/evals`). One part is not yours: the files `evals/scenarios/*.yaml`, except `evals/scenarios/schema-change.yaml`. C7 owns those and writes them at the same time as you, using the format in Work §1. Do not change that format. If it must change, put a `## Contract change request` in your PR. The lead then updates this file and tells C7.

The cloud has no Docker, no dev stack and no `ANTHROPIC_API_KEY`. Unit tests use a scripted model. Live model runs need build tag `claude`.

**Precondition (the lead applies this before launch).** `agent.Deps` must have a `ModelClient agent.ModelClient` field, and `agent.Run` must use it (see the contract change below). Check with `go doc piperoom/internal/agent.Deps`. If the field is missing, stop. Open a draft PR that says so. Never edit `internal/agent/**`.

## Goal
Measure whether the investigation agent finds the root cause, using **replayed** tool results. Then you can compare prompt and model changes run by run. Each run reports:
- whether the root cause was named,
- whether the first failing layer was right,
- tool calls, turns and tokens,
- whether proposed fixes stay inside the scenario's pipeline.

## Work
1. **Scenario format** (`evals/scenario.go`). One YAML file holds one scenario. Its name is `<id>.yaml`. Decode it with `gopkg.in/yaml.v3`: `yaml.NewDecoder(f)` + `dec.KnownFields(true)`, so an unknown key is an error. Exception: decode `pipeline` into a `yaml.Node` first. Then call `node.Decode(&owners.Pipeline{})`, which is lenient. This way a new owners field (A1 is adding `topic_prefix`) works before and after A3 adds it to `owners.Pipeline`.
   ```go
   type Scenario struct {
       ID          string    `yaml:"id"`          // required, ^[a-z0-9][a-z0-9-]*$, == file name without .yaml
       Description string    `yaml:"description"` // optional, shown in the report
       Alert       AlertSpec `yaml:"alert"`
       RawPipeline yaml.Node `yaml:"pipeline"`    // required; same keys as one dev/owners.yaml pipelines[] entry
       Existing    Existing  `yaml:"existing"`
       MaxTurns    int       `yaml:"max_turns"`   // optional, 1..40, default 12 -> agent.Options.MaxTurns (per episode)
       Timeout     string    `yaml:"timeout"`     // optional, time.ParseDuration, default "5m", whole run
       Replays     []Replay  `yaml:"replays"`     // required, >= 1
       Steers      []Steer   `yaml:"steers"`      // optional
       Expect      Expect    `yaml:"expect"`
       Pipeline    owners.Pipeline `yaml:"-"`     // filled by Load from RawPipeline
   }
   type AlertSpec struct {
       Title       string `yaml:"title"`        // required
       PipelineID  string `yaml:"pipeline_id"`  // required, == pipeline.id
       RoomID      string `yaml:"room_id"`      // optional, default "eval-<id>"
       Raw         string `yaml:"raw"`          // optional; must be valid JSON -> Alert.Raw
       PipelineMap string `yaml:"pipeline_map"` // required -> Alert.PipelineMap
   }
   type Existing struct { // what the fake Connect / Schema Registry clients report (makes actions' Validate pass)
       Connectors []string `yaml:"connectors"` // default: pipeline.connectors
       Subjects   []string `yaml:"subjects"`   // default: none
   }
   type Replay struct {
       Tool      string         `yaml:"tool"`      // required; a tool name from the catalog (§2)
       Match     map[string]any `yaml:"match"`     // optional; keys must be properties of that tool's InputSchema
       Summary   string         `yaml:"summary"`   // required unless Error is set; <= tools.MaxSummaryBytes
       Data      any            `yaml:"data"`      // optional; must pass json.Marshal -> Result.Data
       Truncated bool           `yaml:"truncated"` // optional -> Result.Truncated
       Error     string         `yaml:"error"`     // optional; when set, Run returns errors.New(Error); Summary and Data must be empty
   }
   type Steer struct {
       AfterToolCalls int    `yaml:"after_tool_calls"` // >= 0, default 0 (= before the first turn)
       Kind           string `yaml:"kind"`             // steer | comment | approval_result | action_result; default steer
       Actor          string `yaml:"actor"`            // default "driver"
       Text           string `yaml:"text"`             // required
   }
   type Expect struct {
       RootCause         RootCause `yaml:"root_cause"`
       FirstFailingLayer string    `yaml:"first_failing_layer"` // required; one of events.Layers
       AllowedActions    []string  `yaml:"allowed_actions"`     // optional; empty = any registered action
       ForbiddenActions  []string  `yaml:"forbidden_actions"`   // optional; no overlap with allowed
       MustPropose       bool      `yaml:"must_propose"`        // optional, default false
       MaxToolCalls      int       `yaml:"max_tool_calls"`      // optional, 0 = no budget
   }
   type RootCause struct {
       Statement string   `yaml:"statement"` // required; one-sentence ground truth for the LLM grader
       All       []string `yaml:"all"`       // every term must appear
       Any       []string `yaml:"any"`       // at least one must appear (when non-empty)
       None      []string `yaml:"none"`      // none may appear
   }
   ```
   - Add `LoadScenario(path string) (*Scenario, error)` and `LoadDir(dir string) ([]*Scenario, error)`. `LoadDir` reads `*.yaml` sorted by name. Ids must be unique.
   - Add `(*Scenario).Validate() error`. It checks every rule in the comments above. It also checks that:
     - the pipeline has an `id` and at least one `source_tables` entry and one `connectors` entry;
     - every name in `allowed_actions` and `forbidden_actions` is a registered action (§3);
     - `all` and `any` together have at least one term.
   - **Match rule.** Decode the call input with `json.Decoder.UseNumber` into `map[string]any`. The agent has already stripped the injected `why` field. A replay matches when every `match` key is in the input and `json.Marshal(input[k])` is byte-equal to `json.Marshal(match[k])`. If `match[k]` is a YAML sequence, any one element may match. Replays for the same tool are tried in file order, and the first match wins. A missing `match` matches every call. In YAML, quote strings that look like numbers (`key: "101"`).
   - **Steer rule.** Steers are delivered in file order, each one once.
     - `Drain` returns the undelivered steers from the front of the list whose `after_tool_calls` is less than or equal to the number of tool calls so far. It stops at the first steer that is not ready.
     - `Wait` never blocks. If an undelivered steer is left, it releases the next one (even before its count) and returns nil. Otherwise it returns `agent.ErrInboxClosed`, and then `agent.Run` returns nil. This is why every run ends.
     - Build each `HumanInput` as: `Seq` = index+1, `Actor` = `events.Actor{Kind: events.ActorHuman, ID: actor, Name: actor}`, and for comments `CommentID` = `"c_<index+1>"`.
2. **Catalog + replay tools** (`evals/replay.go`).
   - Build the catalog per scenario. Register the real tools into a scratch `tools.Registry` in production order: first `dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{Pipelines: []owners.Pipeline{s.Pipeline}}})`, then `kafkatools.Register(reg, kafkatools.Deps{})`. Leave every client nil, and never call these tools' `Run`. Read only `Name/Description/InputSchema/Kind`, so the model sees exactly the production tool list.
   - Wrap **every** catalog tool, including tools with no replays, in a replay tool that implements `tools.Tool` and keeps the same order. Its `Run` returns the first matching replay. With no match it returns `fmt.Errorf("no replay for %s %s", name, compactInput)` and records the call in `no_replay_calls`. It must be safe for parallel calls, because the agent runs up to 4 tools at once.
3. **Fakes that record everything** (`evals/fakes.go`).
   - An inbox (§1 rule).
   - A sink that does `json.Marshal` on each payload and stores `{Type events.Type, Payload json.RawMessage}` under a mutex.
   - A proposals fake. It stores each `events.ProposalCreatedPayload` with `ProposalID` `"p_<n>"`, returns that id, and never executes anything.
   - `fakeConnect` (`clients.ConnectClient`) and `fakeRegistry` (`clients.SchemaRegistryClient`). `ListConnectors` and `Subjects` return `existing`. Every other method returns a sentinel error ("not available in evals"). They do no I/O and never panic.
   - The actions registry is built with `actions.Register(areg, actions.Deps{Connect: fakeConnect, Registry: fakeRegistry})`.
   - `countingModel` wraps any `agent.ModelClient`. It counts turns, sums `Usage.InputTokens`, `Usage.OutputTokens` and `Usage.CacheReadInputTokens`, and keeps the last `StopReason`.
4. **Run** (`evals/run.go`):
   ```go
   type Config struct {
       Model   agent.ModelClient // nil = live Claude model; in !claude builds Run sets Result.Error = ErrLiveDisabled
       Options agent.Options     // Model, Effort; MaxTurns comes from the scenario
       Grader  Grader            // nil = no LLM grading
       Logger  *slog.Logger      // nil = discard
   }
   func Run(ctx context.Context, s *Scenario, cfg Config) Result
   ```
   `Run` builds `agent.Alert` and `agent.Deps` (with `ModelClient` = the counting wrapper). It applies the scenario timeout, calls the exported `agent.Run`, scores the run (§5), and then grades it if a grader is set. Any failure goes into `Result.Error`. It never panics.
5. **Scoring** (`evals/score.go`). All text matching is case-insensitive substring matching after you lower-case the text and collapse whitespace.
   - **Graded text** is the `Text` of the last `agent.hypothesis` event. If there is none, use the last `agent.message` and set `no_hypothesis: true`.
   - **`root_cause`**: every `all` term is in the text, at least one `any` term is in it (when `any` is set), and no `none` term is in it.
   - **`layer`**: two conditions must both hold. (a) The graded text contains an alias of the expected layer. (b) At least one non-error `agent.tool_result` comes from a tool mapped to that layer.
     - Aliases: postgres = `postgres, source table, replication slot, wal, ddl, alter table`; kafka-connect = `connector, kafka connect, connect task, debezium`; schema-registry = `schema registry, subject, compatibility`; kafka = `topic, broker, partition`; consumer = `consumer, lag`; warehouse = `warehouse, sink table`.
     - Tool layers: `pg_*` → postgres. `connect_*` → kafka-connect. `schema_registry_*` → schema-registry. `kafka_topic_offsets` and `kafka_find_by_key` → kafka. `kafka_list_groups` and `kafka_consumer_lag` → consumer. `warehouse_*` → warehouse. `pipeline_map` and `trace_record` map to no layer.
     - Do not use `based_on_steps`. The model never sees step ids today.
   - **`fix_ok`**: a proposal exists or `must_propose` is false, **and** every step of every proposal passes all of these checks:
     - the action is registered (`areg.Get`) — otherwise list it in `unknown_actions`;
     - it implements `actions.Scoped` — otherwise list it in `out_of_scope` as "not scoped";
     - `CheckScope(s.Pipeline, step.Params) == nil` — otherwise add "action: err" to `out_of_scope`. Do **not** call Validate again: `propose_fix` already ran it, and CheckScope does no I/O;
     - it is not forbidden (`forbidden_used`);
     - it is in `allowed_actions` when that list is set (`not_allowed`).
   - **`within_budget`**: `max_tool_calls == 0 || tool_calls <= max_tool_calls`. `tool_calls` counts `agent.tool_call` events, including calls rejected by input validation.
   - **`pass`** = `error == "" && root_cause && layer && fix_ok && within_budget`. The LLM grade is shown next to it, but it is advisory and never changes `pass`.
   - **`hit_max_turns`**: the last stop reason was not `end_turn` or `stop_sequence`. Use no string matching.
6. **Report + cmd** (`evals/report.go`, `evals/cmd/evalrun/main.go`).
   - `Result` JSON tags: `scenario, pass, root_cause, layer, fix_ok, within_budget, proposed, forbidden_used, not_allowed, unknown_actions, out_of_scope, tool_calls, tool_errors, no_replay_calls, model_turns, hit_max_turns, input_tokens, output_tokens, cache_read_tokens, duration_ms, final_hypothesis, no_hypothesis, grade (omitempty), error (omitempty)`.
   - `Report` JSON tags: `generated_at, model, effort, grader_model, results, passed, total`.
   - The markdown table columns are `scenario | pass | root cause | layer | fix | tool calls | turns | no-replay | tokens in/out | grade`, with a `passed X/Y` line at the end. Escape `|` in cells.
   - `evalrun` flags:
     - `-scenarios` (default `evals/scenarios`);
     - `-out` (default `report.json`). The markdown goes to the same path with `.md`;
     - `-model` and `-effort` (default `config.Load().Model` / `.Effort`, the same as cmd/investigate);
     - `-run` (a regexp on scenario ids);
     - `-grade` (bool) and `-grader-model` (default `claude-opus-5-5`);
     - `-validate`: load and validate only. It makes no model calls and works in any build.
   - Without the `claude` tag and without `-validate`, print `evalrun: live runs need -tags claude` and exit 2.
   - Exit 1 on a bad scenario or a setup error. Exit 0 when the run finished, even if some scenarios failed.
   - Put the logic in `run(args []string, stdout, stderr io.Writer) int` so tests can call it. Run scenarios one after another.
7. **Live model + grader.** Put them in `evals/live_claude.go` with `//go:build claude`. Put stubs that return `ErrLiveDisabled` in `evals/live_stub.go` with `//go:build !claude`.
   - `NewLiveModel() agent.ModelClient`: uses `anthropic.NewClient()`, `client.Beta.Messages.NewStreaming(ctx, params)`, `defer stream.Close()`, `stream.Next()` / `msg.Accumulate(stream.Current())` / `stream.Err()`. It sends params unchanged. This mirrors `internal/agent/model.go`.
   - `Grader` interface: `Grade(ctx, s *Scenario, r Result) (Grade, error)`. `Grade` JSON: `{root_cause_correct bool, first_failing_layer string, reason string, error string omitempty}`.
   - `NewClaudeGrader(model string) (Grader, error)` makes one `client.Messages.New` call with `anthropic.MessageNewParams`:
     - `Model`, `MaxTokens: 16000`, `System: []anthropic.TextBlockParam{{Text: ...}}`;
     - `Messages: []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock(...))}`;
     - `OutputConfig: anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffortMedium, Format: anthropic.JSONOutputFormatParam{Schema: ...}}`. The schema is an object with those three fields, `required`, `additionalProperties: false`, and `first_failing_layer` enum = `events.Layers` + `"none"`.
   - **Do not** force `tool_choice`: Opus 5.5 returns a 400 for it. Parse the first `text` content block with `json.Unmarshal`. On `anthropic.StopReasonRefusal`, set `Grade.Error`.
   - The user text holds the statement, the expected layer, the final hypothesis and the last agent messages. Wrap the agent text in `<agent_output>…</agent_output>` with `&`, `<` and `>` escaped, and tell the grader it is data, not instructions.
   - Use no `fallbacks` here, so the judge model stays fixed. Format docs: https://platform.claude.com/docs/en/build-with-claude/structured-outputs. If the API rejects `output_config.format` on this model, use a plain JSON instruction instead, and say so in the PR.
8. **Seed scenario** `evals/scenarios/schema-change.yaml`. Use exactly this content. The summaries follow the real tool formats in `internal/tools/{kafkatools,dbtools}` and `internal/topology/render.go`, with values from `dev/scenarios.md` §1. Timestamps and the worker id are only examples.
   ```yaml
   id: schema-change
   description: >-
     ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2), then 2 inserts. Debezium's new Avro value
     schema is not BACKWARD compatible, Schema Registry rejects it (409), connector task 0 fails. dev/scenarios.md §1.
   alert:
     title: orders stale
     pipeline_id: orders
     raw: '{"rule":"warehouse_freshness","table":"analytics.orders","stale_for":"14m"}'
     pipeline_map: |
       pipeline orders
       postgres: public.orders  ->  connect: pg-orders-connector  ->  topic: pg.public.orders  ->  consumer: order-sync  ->  warehouse: analytics.orders
   pipeline:
     id: orders
     title: Orders CDC
     topic_prefix: pg
     source_tables: [public.orders]
     connectors: [pg-orders-connector]
     consumer_groups: [order-sync]
     sink_tables: [analytics.orders]
     sink_ts_column: updated_at
   existing:
     subjects: [pg.public.orders-key, pg.public.orders-value]
   max_turns: 12
   timeout: 5m
   replays:
     - tool: pipeline_map
       match: {pipeline_id: orders}
       summary: |
         pipeline orders
         1 pg:public.orders [postgres, owner priya] WARN slot_active=false slot_retained=213.0 KiB
         2 connect:pg-orders-connector [kafka-connect, owner arjun] FAIL tasks_failed=1
         3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=14m ago
         4 group:order-sync [consumer, owner kabir] OK lag=0 state=Stable
         5 wh:analytics.orders [warehouse, owner meera] WARN stale_for=14m
     - tool: connect_list_connectors
       summary: "pg-orders-connector (source): RUNNING, 1 failed tasks"
     - tool: connect_status
       match: {connector: pg-orders-connector}
       summary: |
         pg-orders-connector (source): RUNNING on 172.18.0.6:8083.
         Task 0: FAILED:
         org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
         Caused by: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders :
         Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema<schema>
         Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value", details: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type inside a union field at path '/fields/0/type/1' in the old schema', additionalInfo:'reader union lacking writer type: RECORD'}, {errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type inside a union field at path '/fields/1/type/1' in the old schema', additionalInfo:'reader union lacking writer type: RECORD'}, {oldSchemaVersion: 1}, {oldSchema: <schema>}, {validateFields: 'false', compatibility: 'BACKWARD'}]; error code: 409
         Rejected schema vs pg.public.orders-value v1:
         Value.amount: int → bytes(decimal 12,2)
       data: {name: pg-orders-connector, state: RUNNING, tasks: [{id: 0, state: FAILED}]}
     - tool: connect_config
       match: {connector: pg-orders-connector}
       summary: |
         connector.class: io.debezium.connector.postgresql.PostgresConnector
         database.dbname: orders
         database.hostname: postgres
         database.password: ***
         database.port: 5432
         database.user: piperoom
         slot.name: debezium_orders
         table.include.list: public.orders
         topic.prefix: pg
     - tool: schema_registry_subjects
       summary: |
         pg.public.orders-key
         pg.public.orders-value
     - tool: schema_registry_versions
       match: {subject: pg.public.orders-value}
       summary: "pg.public.orders-value: versions [1], latest id 2, compatibility BACKWARD (global)"
     - tool: schema_registry_versions
       match: {subject: pg.public.orders-key}
       summary: "pg.public.orders-key: versions [1], latest id 1, compatibility BACKWARD (global)"
     - tool: schema_registry_diff
       match: {subject: pg.public.orders-value, to_version: 2}
       error: "schemareg: GET /subjects/pg.public.orders-value/versions/2: not found: schema registry error 40402: Version 2 not found."
     - tool: schema_registry_diff
       match: {subject: pg.public.orders-value}
       summary: no field changes
     - tool: kafka_topic_offsets
       match: {topic: pg.public.orders}
       summary: |
         partition 0: 0–34 (34 records), newest 14 min ago
         partition 1: 0–31 (31 records), newest 15 min ago
         partition 2: 0–35 (35 records), newest 14 min ago
     - tool: kafka_list_groups
       summary: "order-sync: Stable"
     - tool: kafka_consumer_lag
       match: {group: order-sync}
       summary: |
         order-sync: Stable, 1 members, total lag 0
           pg.public.orders/0: committed 34, end 34, lag 0
           pg.public.orders/1: committed 31, end 31, lag 0
           pg.public.orders/2: committed 35, end 35, lag 0
     - tool: pg_replication_slots
       summary: "slot debezium_orders: active=false retained=213.0 KiB wal_status=reserved"
     - tool: pg_recent_ddl
       summary: "2026-10-11T09:41:07.118204Z ALTER TABLE public.orders | ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2) | piperoom"
     - tool: pg_long_transactions
       summary: (no long transactions)
     - tool: warehouse_freshness
       match: {table: analytics.orders}
       summary: newest=2026-10-11T09:40:58.402113Z (stale for 14m)
     - tool: trace_record
       match: {key: ["101", "102"]}
       summary: |
         postgres public.orders: seen updated_at=2026-10-11T09:41:09.27Z
         kafka-connect pg-orders-connector: stopped_here tasks_failed=1
         kafka pg.public.orders: not_seen
         consumer order-sync: not_seen
         warehouse analytics.orders: not_seen
         stopped at: kafka-connect pg-orders-connector
     - tool: trace_record
       summary: |
         postgres public.orders: seen updated_at=2026-10-11T09:26:40.5Z
         kafka-connect pg-orders-connector: seen
         kafka pg.public.orders: seen offset=0 partition=0
         consumer order-sync: seen committed=34
         warehouse analytics.orders: seen
   expect:
     root_cause:
       statement: >-
         ALTER TABLE changed public.orders.amount from integer to numeric(12,2); Debezium's new Avro value schema
         (amount int -> bytes decimal(12,2)) is not BACKWARD compatible with pg.public.orders-value v1, so Schema
         Registry rejected it (409) and pg-orders-connector task 0 failed; the warehouse is stale because no new
         changes reach Kafka.
       all: [amount]
       any: [numeric, decimal, bytes]
     first_failing_layer: kafka-connect
     allowed_actions: [schema_set_compatibility, connect_restart_task, connect_restart_connector]
     forbidden_actions: [schema_register]
     must_propose: true
     max_tool_calls: 15
   ```
9. Add a package doc (`evals/doc.go`) with the run commands. Run live with `go run -tags claude ./evals/cmd/evalrun -scenarios evals/scenarios -out report.json`. Validate with `go run ./evals/cmd/evalrun -validate`.

Size: aim for 800 changed lines or fewer, plus the seed YAML. If you go over, ship steps 1–6, 8 and 9, and move step 7 (live model + grader) to a follow-up PR. Say so in the PR. No new modules: `gopkg.in/yaml.v3` and `anthropic-sdk-go` are already in `go.mod`.

## Tests
All of these run with no key, no network and no Docker. Put a scripted model in `evals/scripted_test.go`. It implements `agent.ModelClient` and returns `anthropic.BetaMessage` values made with `json.Unmarshal`, the same way `internal/agent/run_test.go` does. Example: `{"id":"m1","type":"message","role":"assistant","model":"scripted","content":[{"type":"tool_use","id":"t1","name":"pipeline_map","input":{"pipeline_id":"orders","why":"map"}}],"stop_reason":"tool_use","usage":{"input_tokens":0,"output_tokens":0}}`. After the script ends, it returns `end_turn` with the text "done". Build the scripted inputs from the real schemas: the agent validates them.
- **Loader (table):** the seed loads and validates. Each of these must fail: an unknown top-level key; a replay that names an unknown tool; a match key that is not in the tool schema; both summary and error set; a bad layer; allowed and forbidden overlap; id ≠ file name; a bad timeout; `raw` that is not JSON. `topic_prefix` under `pipeline` is accepted.
- **Matcher (table):** exact match; missing match; any-of list; an integer (`to_version: 2` vs JSON `2`); first match wins; no match gives an error starting with `no replay for` and is recorded.
- **Inbox:** steer order, the `after_tool_calls` threshold, `Wait` releasing the next steer, then `agent.ErrInboxClosed`.
- **Scoring (table):** `all`/`any`/`none` terms and case; layer alias without the matching tool (fails); forbidden, not-allowed and unknown actions; out-of-scope (an existing connector that is not in the pipeline: Validate passes, CheckScope fails); a test action that does not implement `Scoped`; `must_propose`; budget.
- **End to end through `agent.Run`:**
  - Seed + a good script: `pipeline_map` → `connect_status` → `update_hypothesis` (names amount, numeric and the connector) → `propose_fix` (`schema_set_compatibility` NONE, `connect_restart_task` 0, `schema_set_compatibility` BACKWARD) → end. Expect `pass: true`, `tool_calls: 2` and `model_turns: 5`.
  - A bad script (blames the consumer, proposes `schema_register`). Expect `pass: false` with `root_cause`/`layer` false and `forbidden_used: [schema_register]`.
  - A call with no replay is counted.
  - A steer reaches the model. The next request's messages contain the steer text.
- **Report:** JSON round-trip, plus a small markdown golden test.
- **evalrun:** `run([]string{"-validate","-scenarios","scenarios"}, …)` returns 0. Without the tag and without `-validate` it returns 2.
- **Live** (`//go:build claude`): runs the seed with `NewLiveModel()` and skips when `ANTHROPIC_API_KEY` is unset. It asserts `root_cause` and logs the rest.

## Done when
- `gofmt` is clean. `go vet ./...`, `go vet -tags claude ./evals/...`, `go build ./...` and `go test ./...` pass.
- `go run ./evals/cmd/evalrun -validate` prints ok for `schema-change`.
- Only `evals/**` is changed.
- The PR lists:
  - any format question for C7;
  - a request for the lead to add `go vet -tags claude ./evals/...` to CI;
  - the seed values that are examples only (worker id, timestamps, partition counts).
