# Design: AWS MSK Connect + MSK IAM auth (rev 4, read-only scope)

Grounded against `main` @ `8283a58` (auth PRs 1–4, watcher #54/#56/#57, retention + JCS export #46, model providers #49, read scoping #48, propose_fix JSON-text params #45, jsonb guard #58, signed alerts #42 all merged). Every `file:line` below was read from that tree. SDK names were checked with `go doc` on `kafkaconnect@v1.39.4`, `cloudwatchlogs@v1.89.3`, `franz-go@v1.22.1/pkg/sasl/aws` and `aws-msk-iam-sasl-signer-go@v1.0.4` (the last two downloaded into a scratch module for this check). Anything not checked is marked **unverified**.

**Scope of this revision.** PR 0 (contracts + one AGENTS.md row), PR 1 (MSK IAM + one shared `aws.Config`), PR 2a–2e (MSK Connect reads + action gating + worker logs, split so each owner can build alone). All are read-only on the customer side. PR 3 (MSK restart + pending outcome) and PR 4 (CloudWatch RDS forecast) stay **held until a signed MSK pilot**; their contract items are listed in §5.6 so the lead does not apply them by mistake. Lead decisions carried in: config update on MSK stays off; worker logs ship in PR 2.

## 1. Problem and who asked

Persona **P2** (quick-commerce DE, RDS PG15 → MSK Connect → MSK) raised theme **T10** "AWS-native: MSK IAM auth, MSK Connect API, CloudWatch RDS metrics" (`reviews/product-feedback-2026-10-13.md:8,27`). The GTM plan says teams on MSK Connect are invisible to Piperoom because there is no Connect REST API, and pulls MSK work forward only when a design partner commits (`reviews/gtm-plan-2026-10-13.md:13,37`).

Today the only Connect client is REST (`internal/clients/connect/connect.go`), the only Kafka logins are PLAIN and SCRAM (`internal/clients/netsec/kafka.go:24-28,96-119`), and the only AWS code is the Bedrock endpoint, which loads its own `aws.Config` (`internal/agent/endpoint.go:73-82,138-159`).

## 2. Goals / non-goals

Goals, in this scope:
- (a) Every `connect_*` read tool, the watcher, the topology health and `trace_record` work against MSK Connect, and the agent is told plainly what MSK does not expose (per-task state).
- (b) Kafka reads over SASL `AWS_MSK_IAM`.
- (c) One `aws.Config` per process, shared by MSK IAM, MSK Connect, CloudWatch Logs and the Bedrock model endpoint. This holds for `cmd/server` **and** `cmd/investigate`; `cmd/discover` has no model endpoint, so it loads once for its own clients.
- (d) Write actions the backend cannot run are never offered to the model (`propose_fix` lists only real actions).
- (e) `connect_worker_logs`, because without it the P2 incident shape ("task 0 FAILED" with a stack trace) is invisible on MSK.
- (f) A least-privilege IAM policy for the read-only pilot.

Non-goals: MSK restart and the pending-outcome flow (PR 3, held); CloudWatch RDS forecast (PR 4, held); `connect_update_connector_config` on MSK (lead decision, §8); Glue Schema Registry; MSK Connect create/delete; capacity changes; multi-account role assumption.

## 3. Design

### 3a. Connect backend

`clients.ConnectClient` (`internal/clients/clients.go:20-27`) stays the interface. New package `internal/clients/mskconnect` (**A2**; PR 0 adds the ownership row, §5.4, because AGENTS.md §2 today gives A2 only `connect`, `schemareg`, `kafka`, `netsec`, `kafkatools` and `actions`) wraps `github.com/aws/aws-sdk-go-v2/service/kafkaconnect`. The client is built in wiring (`internal/wiring/wiring.go:85-131`, `buildA2Clients`) when `CONNECT_KIND=msk`, from the shared `aws.Config` (§3d), with `kafkaconnect.NewFromConfig(cfg)` (verified).

Verified SDK names (`kafkaconnect@v1.39.4`): `ListConnectorsInput{ConnectorNamePrefix, MaxResults, NextToken}`, `ListConnectorsOutput{Connectors []types.ConnectorSummary}`, `NewListConnectorsPaginator(client ListConnectorsAPIClient, params, ...)` with `NextPage(ctx)`, `types.ConnectorSummary{ConnectorArn, ConnectorName, ConnectorState, CurrentVersion, LogDelivery}`, `DescribeConnectorInput{ConnectorArn}`, `DescribeConnectorOutput{ConnectorArn, ConnectorName, ConnectorState, ConnectorConfiguration map[string]string, CurrentVersion, StateDescription *types.StateDescription{Code, Message}, LogDelivery *types.LogDeliveryDescription}`, `types.LogDeliveryDescription.WorkerLogDelivery *WorkerLogDeliveryDescription`, `.CloudWatchLogs *CloudWatchLogsLogDeliveryDescription{Enabled bool, LogGroup *string}`, `types.ConnectorState` values `RUNNING CREATING UPDATING DELETING FAILED RESTARTING`, error types `types.NotFoundException, TooManyRequestsException, ForbiddenException, UnauthorizedException, ConflictException`. (Note: the rev 2 path `LogDelivery.WorkerLogDelivery.CloudWatchLogs.LogGroup` is right, but the *Describe* output uses the `...Description` struct names, not `types.LogDelivery`.)

| Interface method | MSK calls | Notes |
|---|---|---|
| `ListConnectors` | one paginated `ListConnectors` | fills the name→ARN cache; returns names only, as today. (No state snapshot: `Status` always Describes, so a snapshot would never be read.) |
| `Status(name)` | cache hit → `DescribeConnector(arn)`; miss → `ListConnectors(ConnectorNamePrefix=name)`, exact-name match, then Describe | `State` = `ConnectorState`; `Detail` = `StateDescription.Code + ": " + Message` when set; `Tasks` empty, `TasksUnknown=true`, `Backend="msk"`, `WorkerID=""`; `Type` is a best-effort guess from `ConnectorConfiguration["connector.class"]` in the same Describe (`"sink"` when the class ends in `SinkConnector`, else `"source"`) |
| `Config(name)` | `DescribeConnector.ConnectorConfiguration` | the `connect_config` redaction (`internal/tools/kafkatools/connect_tools.go:154-210`) applies unchanged |
| `RestartTask` | returns `clients.ErrUnsupported` | no per-task API on MSK |
| `RestartConnector` | returns `clients.ErrUnsupported` **in PR 2** | the real call (`RestartConnectorInput{ConnectorArn, OnlyFailedTasks}`, verified) is PR 3 |
| `Resume` adapter (`internal/actions/kafka_actions.go:368-370`) | not implemented | no pause/resume API |
| `UpdateConfig` adapter (`internal/actions/connector_config.go:68-70`) | not implemented | lead decision |
| `Capabilities()` (new, §5.1) | — | all four false in PR 2 |

The REST client (`internal/clients/connect`) is **not touched**: it keeps `Backend` empty, `TasksUnknown` false and `Detail` empty, and all three new fields are `omitempty`, so every existing recording, fake and golden file stays byte-identical.

Cache: name→ARN only, held on the client struct (no package globals, AGENTS.md §4 rule 5), 5 minute TTL, **dropped on `NotFoundException` and resolved once more** before returning a wrapped `clients.ErrNotFound` (teams delete and recreate connectors under the same name). `connect_list_connectors` (`connect_tools.go:29-71`) still calls `Status` per in-scope connector, so MSK costs one `ListConnectors` plus one `DescribeConnector` per pipeline connector per call; pipelines have one to three connectors, so no extra batching.

Errors: `ForbiddenException`/`UnauthorizedException` → text naming the `kafkaconnect:*` action that was refused; `TooManyRequestsException` → "MSK Connect rate limit; retry later", with no retry on top of the SDK's own retryer. The Describe output (full config, may hold secrets) is never written to a log or an error; only the wrapped SDK error is.

Readers of `ConnectorStatus` and what changes for MSK:

| Reader | Today | On MSK | PR |
|---|---|---|---|
| `connect_status` tool (`connect_tools.go:86-139`, A2) | state + per-task lines; `Data` = `name, state, worker, tasks` | summary adds `Detail` after the state and, when `TasksUnknown`, one line: "MSK Connect: per-task state is not exposed; use connect_worker_logs." `Data` gains `tasks_unknown` (bool) and `detail` (string), both only when set, so the PR 3 verify fix (below) needs no tool change | 2c |
| `connect_list_connectors` (`connect_tools.go:68`, A2) | `%d failed tasks` | `tasks: n/a` when `TasksUnknown` | 2c |
| watcher `classifyOne` (`internal/watch/signals.go:204-250`, A7) | `len(Tasks)==0 && RUNNING` → `NO_TASKS`, **sev2** (`:232-235`); evidence map at `:245` is `name, state, tasks` | the `NO_TASKS` rule is skipped when `TasksUnknown`; otherwise every healthy MSK connector would open a room. Evidence gains `state_detail` when `Detail` is set (through `clean`, capped like `state`), so a FAILED MSK connector opens a room **with** the reason in it. `CREATING/UPDATING/DELETING/RESTARTING` stay non-RUNNING → sev2 breach, which is right: they are transient but the watcher has duration tracking | 2a |
| topology `connectorHealth` (`internal/topology/health.go:155-186`, A3) | warn only for `PAUSED/UNASSIGNED/STOPPED`; `RESTARTING` and the MSK states fall through to **OK** | add `CREATING, UPDATING, DELETING, RESTARTING` to the warn case at `:177`, and `facts["state_detail"] = Detail` when set | 2a |
| `trace_record` (`internal/topology/trace.go:104-119`) | `connectorStuck = State != "RUNNING"` | correct as is | — |
| `restartConnector.Execute` `connectorAndTasks` (`kafka_actions.go:343-359`) | needs ≥1 task | never reached on MSK in this scope (action not registered); PR 3 handles it | 3 (held) |
| verify `connectRecovered` (`internal/agent/verify.go:822-841`, A4) | `RUNNING && len(Tasks) > 0 && all RUNNING`, read from `connect_status` `Data` | returns false on MSK forever, so a verify after a `kafka-connect` step (`checkForStep`, `verify.go:560-567`) never sees recovery. Not reached in this scope (no `connect_*` action on MSK, so no `kafka-connect` step is ever executed). PR 3 fix: `tasks_unknown && state == RUNNING` → recovered; **held** (§5.6) | 3 (held) |
| `cmd/discover` (`cmd/discover/main.go:234-244`, C10) | builds its own REST client with `connect.NewSecure` | needs the same `CONNECT_KIND` switch; its `ConnectReader` (`internal/discover/api.go:23-27`) is `ListConnectors` + `Config`, both served by MSK | 2d |

### 3b. Capability gating (what the model may propose)

`clients.ConnectCapable` is **optional** (§5.1). `actions.Register` (`kafka_actions.go:38-52`, A2) type-asserts `d.Connect`; when present it skips `connect_restart_task`, `connect_restart_connector`, `connect_resume_connector` and `connect_update_connector_config` whose capability is false. A client without the interface (REST `connect.Client`, `replayConnect` at `evals/replay.go:274-299`, `demo.FakeConnect` at `internal/room/demo/fakes.go:26-37`, every test fake) keeps every action, so nothing changes for REST, demo or the evals. `propose_fix` builds its description from `Registry.All()` (`internal/agent/tools_room.go:185-210`), so on MSK the model never sees the gated names; a proposal that names one anyway fails in `propose_fix` as an unknown action and in the executor the same way (`internal/room/service_executor.go:213-216`). The compile-time `Scoped` checks (`kafka_actions.go:54-61`) are untouched.

In PR 2 the MSK client reports every capability false, so an MSK pilot is "see and explain": the two Schema Registry actions stay, no `connect_*` action is offered. The GTM copy must say so until PR 3.

### 3c. MSK IAM SASL

Verified: `franz-go/pkg/sasl/aws.ManagedStreamingIAM(authFn func(context.Context) (Auth, error)) sasl.Mechanism`, `aws.Auth{AccessKey, SecretKey, SessionToken, UserAgent}`, "the returned Auth is used for a single session". `aws-sdk-go-v2/aws.CredentialsProvider.Retrieve(ctx) (Credentials, error)` is already used at `endpoint.go:153`. The separate module `github.com/aws/aws-msk-iam-sasl-signer-go` (v1.0.4, `signer.GenerateAuthTokenFromCredentialsProvider`) exists but produces an OAUTHBEARER token; it is **not needed**, because franz-go implements `AWS_MSK_IAM` itself and is already in `go.mod`. No new module for PR 1.

`netsec.Kafka` (`netsec/kafka.go:35-46`, A2) gains one constant and one field, with no AWS import in `netsec`:

```go
const MechAWSMSKIAM = "AWS_MSK_IAM"

// AWSCredentials are one set of AWS credentials for one SASL session.
type AWSCredentials struct{ AccessKey, SecretKey, SessionToken string }

// AWSCredentialsFunc supplies MSK IAM credentials per SASL session. Required
// with Mechanism AWS_MSK_IAM, must be nil otherwise.
AWSCredentialsFunc func(ctx context.Context) (AWSCredentials, error)
```

`mechanism()` (`kafka.go:96`) branches on `MechAWSMSKIAM` **before** the username/password check at `:97`. Rules, in the style of `kafka.go:69-76`, every one `ErrInvalid` (`netsec/netsec.go:30`): `AWS_MSK_IAM` needs `SASL_SSL` (MSK IAM listeners are TLS only); `Username` or `Password` set with it; `AWSCredentialsFunc == nil`; `AWSCredentialsFunc` set with any other mechanism or protocol. The franz-go `Auth` gets `UserAgent: "piperoom"`. Wiring (`wiring.go:114-125`, A4) fills the func from the shared `aws.Config.Credentials.Retrieve` (the default chain caches, so a call per SASL session is cheap). The broker port (AWS docs say 9098 for IAM) is the operator's `KAFKA_BROKERS` value; the code never needs it, so it is **not verified here**.

### 3d. One shared `aws.Config`

Today `agent.EndpointConfig` has no AWS config field; `providerLoaders.awsConfig` (`endpoint.go:68-82`) is an unexported test hook that calls `awsconfig.LoadDefaultConfig(ctx, WithRegion(region))`. `cmd/server/main.go:228,236` and `cmd/investigate/main.go:48,60` **both** call `wiring.NewAgentEndpoint(ctx, cfg, log)` and then `wiring.Open(ctx, cfg)`. If `Open` loaded AWS on its own, each process would load the chain twice (once in the endpoint, once in `Open`), which breaks goal (c). So the load moves out of both and is passed in. `internal/agent`, `internal/wiring` and `cmd/investigate` are all A4's, `cmd/server` is A5's; no contract file changes:

- New `internal/wiring/aws.go` (A4), exported:
  ```go
  // LoadAWS loads the default AWS credential chain once, in cfg.AWSRegion,
  // when something needs it: ModelProvider=bedrock, CONNECT_KIND=msk, or
  // KAFKA_SASL_MECHANISM=AWS_MSK_IAM. Otherwise it returns nil, nil. It does
  // one bounded Credentials.Retrieve (5 s) so a broken chain stops the
  // process at start.
  func LoadAWS(ctx context.Context, cfg config.Config) (*aws.Config, error)
  ```
- Two wiring signatures change (A4 owns every caller and test of them):
  ```go
  func NewAgentEndpoint(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error)
  func Open(ctx context.Context, cfg config.Config, awsCfg *aws.Config) (*Wired, error)
  ```
  `awsCfg` may be nil; `NewAgentEndpoint` then keeps today's behaviour (the endpoint's own loader), and `Open` fails with a config error if `msk` or `AWS_MSK_IAM` is on. Tests in `internal/wiring/model_test.go:44,53,60,72` pass `nil` (A4 updates them).
- `agent.EndpointConfig` gains `AWSConfig *aws.Config`; `providerOptions` (`endpoint.go:138-159`) uses it when non-nil and otherwise keeps the loader (existing agent tests unchanged). `wiring.EndpointConfig` (`internal/wiring/model.go:15-24`) copies it.
- `Open` passes the same pointer to `buildA2Clients` for the Kafka SASL func and the MSK Connect and Logs clients.
- Callers, one `LoadAWS` each, before both calls:
  - `cmd/server/main.go:228-236` (A5): `awsCfg, err := wiring.LoadAWS(ctx, cfg)` → `NewAgentEndpoint(ctx, cfg, log, awsCfg)` → `Open(ctx, cfg, awsCfg)`. No new import: the type is inferred.
  - `cmd/investigate/main.go:48,60` (A4): same three lines. The `-fake-tools` path still calls `NewAgentEndpoint` (with the loaded config or nil) and skips `Open`.
  - `cmd/discover/main.go` (C10): no endpoint; it calls `wiring.LoadAWS` once and `mskconnect.New(awsCfg)` directly in its own client build (PR 2d).

Done-when for PR 1 includes a test that counts loader calls: exactly one with Bedrock + MSK IAM both on, zero with neither.

**Module versions.** `go.mod:7-8` pins `aws-sdk-go-v2 v1.38.1` and `config v1.27.27`. PR 1 needs no new module. PR 2c adds `service/kafkaconnect v1.39.4` and `service/cloudwatchlogs v1.89.3`; both require core `v1.47.3` + `smithy-go v1.28.5` (read from their `go.mod`). `anthropic-sdk-go v1.80.0` requires only core `>= v1.38.0`, so Bedrock keeps building. The bump of core, smithy and `config` (latest `v1.33.9`; the exact number is whatever `go get` picks on the day) is its own PR **2b** with no code change, so a Bedrock break shows up alone (`go test ./internal/agent/...`), and PR 2c then adds the two service modules on top with no further core change.

### 3e. Worker logs (`connect_worker_logs`)

Tool `connect_worker_logs(connector)` in kafkatools (A2), `tools.KindRead`. It calls `ScopeFor` and `CheckConnector` first like every #48 tool (`internal/tools/scope.go:102-120,170-175`), then asks a new small reader `LogReader` on `kafkatools.Deps` (A2-owned, not a contract file):

```go
// WorkerLogs returns up to limit log lines of the connector's worker log
// group that match pattern, newest last, within [since, now]. ok=false when
// the connector has no CloudWatch log delivery.
WorkerLogs(ctx context.Context, connector, pattern string, since time.Time, limit int) (lines []LogLine, group string, ok bool, err error)
```

The MSK implementation (in `mskconnect`, same package as the Connect client so it can reuse the ARN cache): `DescribeConnector` → `LogDelivery.WorkerLogDelivery.CloudWatchLogs` (nil-safe at every step; `Enabled=false` or empty `LogGroup` → `ok=false`, tool summary "worker logs are not delivered to CloudWatch for this connector"), then `cloudwatchlogs.FilterLogEvents` (verified: `FilterLogEventsInput{LogGroupName, StartTime *int64 ms, EndTime *int64 ms, FilterPattern, Limit *int32}`, output `Events []types.FilteredLogEvent{Timestamp *int64, LogStreamName, Message}`, paginator `NewFilterLogEventsPaginator` with `NextPage(ctx)`). Fixed settings, no new env vars: pattern `ERROR`, last 30 minutes, 50 lines, one page (`Limit=50`), each line cut to 500 runes, the whole summary through `tools.Truncate`. Timestamps are printed as UTC. The tool is registered only when the wiring passes a `LogReader` (`CONNECT_KIND=msk`); on REST it does not exist.

Two things the pilot must check (**unverified**): how MSK Connect names log streams inside the group, and whether the pilot team shares one log group across connectors. If they share, lines of other connectors (other pipelines) would show, which breaks the F11 scoping promise. Until the stream naming is known the setup doc says "one log group per connector", and the tool prints the group it read so a reviewer can see it.

Log lines are untrusted text: they go through the usual `<tool_output untrusted="true">` wrapper (`internal/agent/run.go:791-797`). A bad connector can print secrets; the existing `sensitiveKey` redaction (`connect_tools.go:245-254`) is for config keys, so the tool adds a best-effort line redaction of `password=…`, `secret=…`, `token=…` values (regex, value replaced by `***`). This is not a guarantee, and the setup doc says so.

### 3f. Config for `msk`

`CONNECT_KIND` (§5.2) defaults to `rest`, so nothing changes for anyone today. With `msk`: `AWS_REGION` is required; a non-empty `CONNECT_USERNAME`, `CONNECT_PASSWORD` or `CONNECT_TLS_*` is a start-up error naming the variable (the operator thinks they are used); `CONNECT_URL` is ignored because it has a default (`config.go:132`) and cannot be told apart from "unset". `config.ParseConnectKind` does these checks in the contract file so every command agrees. It has **two** code callers, and both must call it before building any client:

1. `wiring.buildA2Clients` (`wiring.go:85`, A4) — this covers `cmd/server` and `cmd/investigate`, which both go through `wiring.Open`.
2. `cmd/discover/main.go:234` (C10), which builds its own clients.

(`cmd/investigate` is not a third caller: it never builds clients itself.)

## 4. Safety and security

- **Approval flow unchanged** (`docs/api.md:168-170`). `CheckScope` runs on names (`kafka_actions.go:63-71`); ARN resolution happens inside the client after the check, so F10 holds. #48 read scoping is backend-neutral: every new tool call starts with `ScopeFor`.
- **Gating is additive.** A backend without `ConnectCapable` keeps every action, so REST, demo and evals do not change. The executor re-check (`service_executor.go:207-227`: hash, `Validate`, `CheckScope`) is untouched.
- **Credentials.** Default AWS chain only (env, shared config, instance or task role), the same rule as Bedrock (`docs/pilot/permissions.md:279-283`). No AWS key is ever stored in `config.Config`; nothing is logged.
- **No write on MSK in this scope.** Every write method of the MSK client returns `ErrUnsupported`; no action that reaches them is registered. The pilot IAM policy below grants no write action at all.
- **IAM policy** (goes in `docs/pilot/permissions.md`, C14, under a new "AWS MSK" section). Action strings are **not checked** against the Service Authorization Reference:

```json
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["kafkaconnect:ListConnectors"],"Resource":"*"},
 {"Effect":"Allow","Action":["kafkaconnect:DescribeConnector"],
  "Resource":"arn:aws:kafkaconnect:REGION:ACCOUNT:connector/pg-orders-connector/*"},
 {"Effect":"Allow","Action":["logs:FilterLogEvents"],
  "Resource":"arn:aws:logs:REGION:ACCOUNT:log-group:WORKER_LOG_GROUP:*"},
 {"Effect":"Allow","Action":["kafka-cluster:Connect","kafka-cluster:DescribeCluster"],
  "Resource":"arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER/*"},
 {"Effect":"Allow","Action":["kafka-cluster:DescribeTopic","kafka-cluster:DescribeTopicDynamicConfiguration","kafka-cluster:ReadData"],
  "Resource":"arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER/*/pg.*"},
 {"Effect":"Allow","Action":["kafka-cluster:DescribeGroup"],
  "Resource":"arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER/*/*"}]}
```
`DescribeGroup` covers all groups because `kafka_consumer_lag` reads the customer's group through `kadm.Lag` (`internal/clients/kafka/kafka.go:244`). **That call also fetches end offsets of every topic the group reads**, so `DescribeTopic` is needed on those topics too. If the group reads topics outside `pg.*` (a mixed group), those partitions fail the fetch and are silently skipped (`kafka.go:265-267`), so the reported lag is too low with no error. The permissions doc must say: widen the topic resource to every topic the pipeline's consumer group reads, not only the CDC prefix. No `AlterGroup`: `FindByKey` uses `kgo.ConsumePartitions` with no group (`kafka.go:324,395`); whether `ReadData` alone is enough without a group is tested on the pilot cluster before the doc claims it. `Topics()` already skips per-topic auth errors (`kafka.go:88`), so a topic-prefixed `DescribeTopic` is fine for listing. `kafkaconnect:RestartConnector`, `DescribeConnectorOperation` and `cloudwatch:GetMetricData` are **not** in this policy; they come with PR 3 and PR 4.
- **What can go wrong.** (1) A shared worker log group leaks other pipelines' log lines (§3e; setup doc rule, group name printed). (2) A connector in `CREATING/UPDATING/DELETING/RESTARTING` would show as healthy on the pipeline map today; the health change in §3a fixes that. (3) The watcher pages on every MSK connector if `TasksUnknown` is forgotten; the PR 2a done-when has a test for exactly that. (4) The core SDK bump could break Bedrock; it is its own PR 2b gated by the endpoint tests. (5) Secrets in log lines; best-effort redaction plus the untrusted wrapper, stated honestly in the docs. (6) Lag under-reported on a mixed consumer group if the IAM topic resource is too narrow (above; doc rule).
- **Rollback.** Nothing writes, so none. `CONNECT_KIND=rest` (the default) turns the whole feature off.

## 5. Contract changes for the lead (PR 0)

Only the four files below change (three contract files plus one AGENTS.md row). Everything is additive; `go build ./... && go test ./...` stay green with no other edit, because no existing field, method or signature changes.

### 5.1 `internal/clients/clients.go`

```diff
 // ErrNotFound is returned (wrapped) when the named object does not exist.
 var ErrNotFound = errors.New("not found")
 
-// ---- Kafka Connect (REST) ----
+// ErrUnsupported is returned (wrapped) by a Connect write method the
+// configured backend has no API for (MSK Connect: RestartTask, Resume, and
+// RestartConnector until the MSK restart PR). Actions over such methods are
+// not registered (see ConnectCapable), so this is a last line of defence,
+// not the normal path.
+var ErrUnsupported = errors.New("not supported by this connect backend")
+
+// ---- Kafka Connect (REST or AWS MSK Connect) ----
 
 type ConnectClient interface {
 	ListConnectors(ctx context.Context) ([]string, error)
 	Status(ctx context.Context, name string) (ConnectorStatus, error)
 	Config(ctx context.Context, name string) (map[string]string, error)
 	// Write — used only by actions after approval.
 	RestartTask(ctx context.Context, name string, task int) error
 	RestartConnector(ctx context.Context, name string) error
 }
 
+// ConnectCapabilities says which write actions the backend can really run.
+// A false field means the matching action is not registered at all.
+type ConnectCapabilities struct {
+	RestartTask      bool // connect_restart_task
+	RestartConnector bool // connect_restart_connector
+	Resume           bool // connect_resume_connector (local Resume adapter)
+	UpdateConfig     bool // connect_update_connector_config (local UpdateConfig adapter)
+}
+
+// ConnectCapable is optional. actions.Register asks for it and skips every
+// action whose capability is false, so propose_fix never offers it. A
+// ConnectClient that does not implement it (REST, fakes, replay) has every
+// capability.
+type ConnectCapable interface {
+	Capabilities() ConnectCapabilities
+}
+
 type ConnectorStatus struct {
 	Name     string       `json:"name"`
-	Type     string       `json:"type"`  // "source" | "sink"
-	State    string       `json:"state"` // RUNNING | PAUSED | FAILED | UNASSIGNED | RESTARTING | STOPPED
+	Type     string       `json:"type"`  // "source" | "sink" (msk: best-effort from connector.class)
+	State    string       `json:"state"` // REST: RUNNING | PAUSED | FAILED | UNASSIGNED | RESTARTING | STOPPED. MSK adds CREATING | UPDATING | DELETING
 	WorkerID string       `json:"worker_id"`
 	Tasks    []TaskStatus `json:"tasks"`
+	// TasksUnknown is true when the backend does not expose per-task state
+	// (MSK Connect). Tasks is then empty, and a reader must not treat "no
+	// tasks" as a fault (the watcher's NO_TASKS rule, connect_list_connectors).
+	TasksUnknown bool `json:"tasks_unknown,omitempty"`
+	// Detail is the backend's own explanation of State when it has one (MSK:
+	// StateDescription code and message). Free text from the customer's
+	// system: show it, never parse it.
+	Detail string `json:"detail,omitempty"`
+	// Backend names the client that produced this status: "" for the REST
+	// client (unchanged, so recordings stay identical), "msk" for MSK Connect.
+	Backend string `json:"backend,omitempty"`
 }
```

### 5.2 `internal/config/config.go`

Struct fields:

```diff
 	KafkaBrokers      string // KAFKA_BROKERS (comma separated)
-	ConnectURL        string // CONNECT_URL
+	ConnectURL        string // CONNECT_URL (CONNECT_KIND=rest only; ignored for msk)
+	ConnectKind       string // CONNECT_KIND: rest (default) | msk (AWS MSK Connect API; see ParseConnectKind)
 	SchemaRegistryURL string // SCHEMA_REGISTRY_URL
```

```diff
 	KafkaSecurityProtocol string // KAFKA_SECURITY_PROTOCOL: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
-	KafkaSASLMechanism    string // KAFKA_SASL_MECHANISM: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
+	KafkaSASLMechanism    string // KAFKA_SASL_MECHANISM: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | AWS_MSK_IAM (needs SASL_SSL and AWS_REGION, no username/password)
```

```diff
-	// Kafka Connect login. TLS is on when CONNECT_URL is https.
+	// Kafka Connect login (CONNECT_KIND=rest). TLS is on when CONNECT_URL is
+	// https. All five must be empty with CONNECT_KIND=msk (ParseConnectKind).
 	ConnectUsername    string // CONNECT_USERNAME
```

```diff
 	ModelProvider string // PIPEROOM_MODEL_PROVIDER: anthropic | bedrock | vertex
-	AWSRegion     string // AWS_REGION (bedrock)
+	AWSRegion     string // AWS_REGION (bedrock; CONNECT_KIND=msk; KAFKA_SASL_MECHANISM=AWS_MSK_IAM). One aws.Config per process.
```

`Load()`:

```diff
 		ConnectURL:        get("CONNECT_URL", "http://localhost:8083"),
+		ConnectKind:       get("CONNECT_KIND", DefaultConnectKind),
 		SchemaRegistryURL: get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
```

New block, placed after the `Watch` helpers (before `getSet`):

```go
// Connect backend kinds (CONNECT_KIND).
const (
	DefaultConnectKind = "rest"
	ConnectKindREST    = "rest"
	ConnectKindMSK     = "msk"
)

// ParseConnectKind validates CONNECT_KIND and the settings that go with it.
// "msk" talks to the AWS MSK Connect API with the default AWS credential
// chain in AWS_REGION instead of the Connect REST API, so the REST login
// variables must be empty: a value there means the operator thinks they are
// used. The error names the env var. Empty means rest. Callers: wiring
// (buildA2Clients, for cmd/server and cmd/investigate) and cmd/discover.
func ParseConnectKind(c Config) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(c.ConnectKind))
	if kind == "" {
		kind = DefaultConnectKind
	}
	switch kind {
	case ConnectKindREST:
		return kind, nil
	case ConnectKindMSK:
		if strings.TrimSpace(c.AWSRegion) == "" {
			return "", fmt.Errorf("config: CONNECT_KIND=msk needs AWS_REGION")
		}
		restVars := []struct{ name, value string }{
			{"CONNECT_USERNAME", c.ConnectUsername},
			{"CONNECT_PASSWORD", c.ConnectPassword},
			{"CONNECT_TLS_CA_FILE", c.ConnectTLSCAFile},
			{"CONNECT_TLS_CERT_FILE", c.ConnectTLSCertFile},
			{"CONNECT_TLS_KEY_FILE", c.ConnectTLSKeyFile},
		}
		for _, v := range restVars {
			if v.value != "" {
				return "", fmt.Errorf("config: %s is set but CONNECT_KIND=msk uses the AWS SDK, not the Connect REST API; unset it", v.name)
			}
		}
		return kind, nil
	default:
		return "", fmt.Errorf("config: CONNECT_KIND=%q: want rest or msk", c.ConnectKind)
	}
}
```

| Env | Default | Meaning |
|---|---|---|
| `CONNECT_KIND` | `rest` | `msk` uses the AWS SDK; REST login vars must be empty; `AWS_REGION` required |
| `KAFKA_SASL_MECHANISM` | existing | new value `AWS_MSK_IAM`: needs `SASL_SSL` and `AWS_REGION`, no user/password |
| `AWS_REGION` | existing (`""`) | now also read for `msk` and `AWS_MSK_IAM` |

No env var for the worker-log window or line count (fixed, §3e) and none for the ARN cache TTL.

### 5.3 `docs/api.md`

No endpoint row changes. Insert one paragraph after the Proposals paragraph (after `docs/api.md:170`, "frozen at creation; approvals bind to those exact steps."):

```
Actions the configured Connect backend cannot run are not registered (`clients.ConnectCapable`): they are
absent from the `propose_fix` action list, and a proposal that names one is refused as an unknown action,
both at creation and by the executor. With `CONNECT_KIND=msk` (read-only until the MSK restart PR) that is
every `connect_*` action; the Schema Registry actions stay. `connect_status` on MSK reports
`tasks_unknown: true` and no tasks: per-task state is not exposed there, and `connect_worker_logs` is the
tool for the task trace.
```

In the Topology section (`docs/api.md:135`), extend the sentence on `health`: add ` A connector `facts` map may carry `state_detail` (MSK Connect's own explanation of the state).`

### 5.4 `AGENTS.md` §2 ownership row (lead-owned)

Without this row A2 breaks "Never edit a file you do not own" (`AGENTS.md:46`) on its first PR 2c commit.

```diff
-| A2 | `tasks/A2-kafka-tools.md` | `internal/clients/connect/**`, `internal/clients/schemareg/**`, `internal/clients/kafka/**`, `internal/tools/kafkatools/**`, `internal/actions/**` (not `action.go`) |
+| A2 | `tasks/A2-kafka-tools.md` | `internal/clients/connect/**`, `internal/clients/mskconnect/**`, `internal/clients/schemareg/**`, `internal/clients/kafka/**`, `internal/tools/kafkatools/**`, `internal/actions/**` (not `action.go`) |
```

(`internal/clients/netsec/**` is already A2's by the C8 row, `AGENTS.md:64`.)

### 5.5 Unchanged contract files

`internal/agent/contract.go`, `internal/events/events.go`, `internal/tools/tool.go`, `internal/actions/action.go`, `internal/topology/graph.go`: no change in this scope.

### 5.6 Non-contract edits the lead should expect in PRs 1–2 (for review, not for PR 0)

`netsec.Kafka` field + constant (A2, §3c); `wiring/aws.go` `LoadAWS`, the `NewAgentEndpoint`/`Open` signature change, `model_test.go`, `agent.EndpointConfig.AWSConfig`, `cmd/investigate` call order (A4, §3d); `cmd/server/main.go` call order (A5); `kafkatools.Deps.Logs LogReader` and `connect_status` `Data` fields (A2, §3e, §3a); `actions.Register` gating (A2, §3b); `health.go:177` warn states (A3); `signals.go:232` `TasksUnknown` guard and `:245` `state_detail` evidence (A7); `cmd/discover` backend switch (C10); `go.mod` bump (Lead 1, PR 2b); `evals` `resources.connect_backend` (C6); scenario (C7); pilot docs (C14).

### 5.7 Held for PR 3/4 — do **not** apply in PR 0

`actions.Pending` error type (`action.go`); `events.ActionExecutedPayload.Pending bool` (`events.go:247-255`) and `OK: nil` mapping in `service.go:709-722`; eval `inputs[]` `ok`/`proposal_id`/`step_index`; `RDS_DB_INSTANCE_ID` and `CLOUDWATCH_LOOKBACK` config fields; **`verify.connectRecovered` (`internal/agent/verify.go:822-841`, A4)**: accept `tasks_unknown: true` with `state: RUNNING` as recovered, otherwise an MSK restart (PR 3) can never verify. The `connect_status` `Data` field it needs ships in PR 2c, so the PR 3 edit is only in `verify.go`. They are ready in rev 2 §3b/§3d/§5 and will be re-grounded when a pilot signs.

## 6. Evals and tests

Evals (C6 harness, C7 scenario), PR 2e:
- `resources.connect_backend: rest | msk` (default `rest`) in the scenario format. With `msk`, `LiveTools` registers `connect_worker_logs` (a replay `LogReader`) and `LiveActions` wraps `replayConnect` in a type that implements `ConnectCapable` with every field false, so the `propose_fix` description the model sees is the production MSK one. `-check` rejects a `connect_worker_logs` recording in a `rest` scenario.
- Scenario `msk-connector-failed`: `connect_status` recording in the MSK format ("FAILED. Detail: …; MSK Connect: per-task state is not exposed; use connect_worker_logs." with `tasks_unknown: true` and `detail` in `Data`), a `connect_worker_logs` recording with the task trace in the log lines, `expect.first_failing_layer: kafka-connect`, `evidence_tools: [connect_status, connect_worker_logs]`, **no `fix`** (nothing can be proposed on MSK in this scope), `forbidden_actions: [connect_restart_task, connect_restart_connector, connect_resume_connector, connect_update_connector_config]`. The harness also asserts that none of the four names appears in the `propose_fix` description.

Unit tests:
- `netsec` (PR 1): the fake broker (`netsec/helpers_test.go:152-167`) records SASL mechanisms; add a case asserting `AWS_MSK_IAM` in `SASLHandshake` and that `AWSCredentialsFunc` was called; table cases for the four `ErrInvalid` rules, each returned by `Opts()` before any dial.
- `wiring`/`agent` (PR 1): one `LoadAWS` loader call with Bedrock + MSK IAM both on, zero with neither; `Open` with `msk` and a nil `awsCfg` fails with a config error; `model_test.go` passes `nil`.
- `watch` (PR 2a): `classifyOne` with `TasksUnknown=true, State=RUNNING, Tasks=nil` → healthy; with `TasksUnknown=false` → `NO_TASKS` as today; `State=FAILED, Detail="X"` → evidence has `state_detail`.
- `topology` (PR 2a): `connectorHealth` for `CREATING`, `UPDATING`, `DELETING`, `RESTARTING` → warn with `state` and `state_detail` facts.
- `mskconnect` (PR 2c): against a small `api` interface (`ListConnectors`, `DescribeConnector` with the SDK's `(ctx, *Input, ...func(*Options)) (*Output, error)` shape, so the real `*kafkaconnect.Client` satisfies it): name→ARN cache; cache drop and one re-resolve on `NotFoundException`; `Detail` mapping; `Type` guess; `TasksUnknown`/`Backend` set; rate-limit text; the Describe output absent from every error string. For logs: nil `LogDelivery`, `Enabled=false`, empty group, 50-line cap, line cut, redaction of `password=`.
- `kafkatools` (PR 2c): `connect_status` on a REST-shaped status has no `tasks_unknown`/`detail` keys in `Data` (existing goldens unchanged); on an MSK-shaped one it has both and the extra summary line.
- `actions` (PR 2c): `Register` with a fake `ConnectCapable` (all false) registers only the two schema actions; without the interface, all six.
- `config` (PR 0 or 2c): `ParseConnectKind` table: empty → rest; `msk` without region; `msk` with each REST var set (error names it); bad value.
- Integration tests behind the `integration` tag, skipped without `AWS_REGION` and a `PIPEROOM_IT_MSK_CONNECTOR` name.

## 7. PR split

Each PR touches only folders of its owners, so each can be written without waiting for the others beyond its "depends on" column. Lead 1 merges in the listed order.

| PR | Owner | Depends on | Done-when |
|---|---|---|---|
| 0 contracts | Lead 1 | — | §5.1–5.4 applied verbatim; `go build ./... && go vet ./... && go test ./...` green with no other change |
| 1 MSK IAM + shared `aws.Config` | A2 (`netsec`, `clients/kafka`), A4 (`wiring/aws.go`, `NewAgentEndpoint`/`Open` signatures, `model.go`, `model_test.go`, `EndpointConfig.AWSConfig`, `cmd/investigate`), A5 (`cmd/server/main.go` call order) | PR 0 | fake-broker test sees `AWS_MSK_IAM` and the credentials func; the four `ErrInvalid` rules pass; loader-count test shows one `aws.Config` reaching Bedrock and Kafka; `cmd/server` and `cmd/investigate` each call `LoadAWS` exactly once; no new module in `go.mod` |
| 2a status readers | A3 (`health.go`), A7 (`signals.go`) | PR 0 only (runs beside PR 1) | watcher test: healthy MSK connector (`TasksUnknown`) opens no room, FAILED one carries `state_detail`; health test: the four transient states warn with `state_detail` |
| 2b SDK core bump | Lead 1 (`go.mod`, `go.sum` only) | PR 1 | core → `v1.47.3`, smithy → `v1.28.5`, `config` → latest; no code change; `go build ./... && go test ./...` green, `go test ./internal/agent/...` named in the PR |
| 2c MSK client + tools + gating | A2 (`clients/mskconnect`, `kafkatools`, `actions.Register`, `connect_status` Data) | PR 0, PR 2b | reads work on the fake api; gated names absent from `propose_fix` description; `connect_status` Data carries `tasks_unknown`/`detail` on MSK and nothing new on REST; new modules listed in the PR (`kafkaconnect v1.39.4`, `cloudwatchlogs v1.89.3`); `ErrUnsupported` from every write method |
| 2d wiring switch + discover | A4 (`CONNECT_KIND` in `buildA2Clients`, `LogReader`/tool registration), C10 (`cmd/discover` backend switch) | PR 1, PR 2c | `msk` with a REST login var fails at start naming it, in `cmd/server`, `cmd/investigate` and `cmd/discover`; `connect_worker_logs` registered only with `msk`; `discover` lists MSK connectors against the fake api |
| 2e evals + docs | C6 (harness), C7 (scenario), C14 (`docs/pilot/permissions.md`, `setup.md`) | PR 2d | `msk-connector-failed` passes `-check` and a live run; IAM section in permissions.md with the mixed-group note (§4); setup.md states "one log group per connector" and the redaction limits |
| 3 MSK restart + pending (**held**) | A2 actions, A4 `verify.go`, A5 executor, C6, C7 | PR 2e, signed pilot | see rev 2 §3b plus §5.7 |
| 4 CloudWatch RDS forecast (**held**) | A3 | PR 1, signed pilot | see rev 2 §3d |

New modules (AGENTS.md §4 rule 10, `AGENTS.md:122-124`): PR 2c only — `service/kafkaconnect`, `service/cloudwatchlogs`; `aws-sdk-go-v2/config` and `credentials` are already present (`go.mod:8,38`). `aws-msk-iam-sasl-signer-go` is deliberately not added.

## 8. Risks and open decisions

1. **Config update on MSK stays off** (lead decision). `UpdateConnector` needs the whole config map, possibly plaintext secrets, through this process. Revisit when a partner on the Secrets Manager config provider asks.
2. **No MSK design partner yet** (`gtm-plan:37`). PR 0–2 are cheap and read-only; PR 3/4 wait for a signed pilot. Until PR 3 the GTM copy for MSK is "see and explain, no connector actions".
3. **Worker-log scoping** depends on one log group per connector (§3e). If the pilot shares groups, either accept cross-pipeline lines in that tool (documented) or drop the tool until the stream naming is known.
4. **SDK core bump** (`v1.38.1` → `v1.47.3`) is its own PR 2b with no code, gated by the endpoint tests, so a Bedrock break is seen alone and can be reverted alone.
5. **Signature change in wiring** (`NewAgentEndpoint`, `Open` gain `awsCfg`). All callers and tests are A4's or A5's; a future command that forgets `LoadAWS` and passes nil gets a clear config error from `Open` when `msk` or `AWS_MSK_IAM` is on, not a double load.
6. **Unverified**: IAM action strings and the `logs:` resource ARN form; `ReadData` without a group; MSK worker log stream names; port 9098 (not needed by code); whether `ListConnectors(ConnectorNamePrefix)` can return more than the exact name (handled by exact match on the client side either way).

## Changes after review rev 3

Review: `msk-review-rev3.md` (APPROVE WITH CHANGES). Every claim in it was re-checked against `8283a58` before applying.

Applied (must-fix):
1. **`mskconnect` ownership** — §5.4 adds the exact AGENTS.md row for PR 0 (confirmed: `AGENTS.md:51` lists no `mskconnect`).
2. **PR 2 split** into 2a–2e (§7) with one owner group and a done-when each; 2a needs only PR 0; the go.mod bump is 2b on its own (Lead 1, no code).
3. **Wiring signatures** — §3d now states `LoadAWS`, `NewAgentEndpoint(…, awsCfg)`, `Open(…, awsCfg)`, and that A4 updates `cmd/investigate/main.go:48,60` and `model_test.go`; A5 updates `cmd/server/main.go:228,236`; C10 calls `LoadAWS` in discover. Goal (c) now names both commands. Rev 3's "investigate unchanged" was wrong: it would have loaded AWS twice.
4. **`verify.connectRecovered`** (`verify.go:822-841`) added to the readers table and to the §5.7 held list for PR 3; `connect_status` `Data` gains `tasks_unknown` and `detail` in PR 2c so PR 3 edits only `verify.go`.

Applied (nice-to-have):
- Watcher evidence carries `state_detail` (§3a, PR 2a, with a test).
- Dead ListConnectors state snapshot dropped; cache is name→ARN only.
- REST client leaves `Backend` empty; stated in §3a and in the §5.1 field comment.
- IAM: `kadm.Lag` end-offset fetch needs `DescribeTopic` on every topic the group reads; mixed groups under-report lag silently (`kafka.go:265-267`); doc rule added to §4 and PR 2e.

Changed with a reason (not rejected outright):
- **`ParseConnectKind` callers: two, not three.** `cmd/investigate` never builds clients; it goes through `wiring.Open` → `buildA2Clients` (`cmd/investigate/main.go:60`). The callers named in §3f and in the function comment are `wiring.buildA2Clients` (serving `cmd/server` and `cmd/investigate`) and `cmd/discover/main.go:234`.

Rejected: none.

## Changes in rev 3 (vs rev 2)

- Re-grounded on `8283a58`: all line refs replaced; the auth PR 4 executor re-check, `ParamsHash` and identity are untouched by this design and now cited as such.
- **Scope cut to PR 0–2**; PR 3/4 contract items moved to the "held" list so the lead applies only what PR 1–2 need. `actions.Pending`, `ActionExecutedPayload.Pending`, `RDS_DB_INSTANCE_ID`, `CLOUDWATCH_LOOKBACK` are out of PR 0.
- New finding: the watcher's `NO_TASKS` rule (`signals.go:232`) would page on every MSK connector; added `ConnectorStatus.TasksUnknown` to the contract and an A7 item. Found the same gap in `health.go` for transient states.
- `ConnectCapabilities` now has four named fields (adds `RestartConnector` and `UpdateConfig`) instead of `TaskStatus`, because in PR 2 the MSK client runs no write at all.
- Shared `aws.Config` made concrete: `wiring/aws.go` + `EndpointConfig.AWSConfig`, no contract change, with the `main.go` ordering issue and the loader-count test named.
- SDK facts re-verified; `cloudwatchlogs` names now verified (`FilterLogEvents` input/output/paginator); the Describe log-delivery types corrected to the `...Description` structs; `aws-msk-iam-sasl-signer-go` checked and rejected as unnecessary; the core SDK version bump that PR 2 forces is stated with versions.
- §5 rewritten as exact diffs with context lines and json tags, plus `ParseConnectKind` in the contract file so the commands agree; exact `docs/api.md` text given.
- IAM policy cut to read-only (no `RestartConnector`, `DescribeConnectorOperation`, `GetMetricData`); worker-log scoping risk (shared groups) added.
- `cmd/discover` added as a PR 2 item (it builds its own REST client).
- Resolved and removed: "worker logs make PR 2 bigger" (lead: ship them); "where the RDS id lives" (PR 4, held); `ConnectCapable` default-all and `Detail` carrying the code (kept, now in the diff).
