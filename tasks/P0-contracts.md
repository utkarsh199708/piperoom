# P0 — Contracts of the P wave (leads only)

Owner: leads (`[LEAD-ARCH]`), one PR titled `[P0] contracts: write mode, solo ceremony, write clients, pipeline kinds`.
Plan: `docs/product/fit-2026-10.md`. Merges first, alone; every P PR codes against the items below by number
("Uses (P0)" in `AGENTS.md`). This file is the final text, matching the code of the P0 PR (#583, branch `claude/p0-contracts`); the
code and its doc comments win where the two ever differ.

Agents **never edit** these files: `internal/clients/clients.go`, `internal/events/events.go`,
`internal/actions/action.go`, `internal/agent/contract.go`, `internal/topology/graph.go`,
`internal/config/config.go`, `internal/tools/tool.go`, `docs/api.md`. A gap found while coding is a
`## Contract change request` in the PR, with a local adapter until the leads decide.

## Items

### 1. Write mode (P1) — `internal/config/config.go`, `docs/api.md`

```go
// WriteMode is PIPEROOM_WRITE_MODE: "off" (default) keeps every write action
// out of the registry the executor uses; proposals are still created,
// previewed and approved, and `action.executed` is never emitted. "on" wires
// the write clients. --demo forces "on" (fake clients only).
WriteMode string // PIPEROOM_WRITE_MODE: off | on

// in Load:
WriteMode: get("PIPEROOM_WRITE_MODE", "off"),

// ParseWriteMode reports whether write mode is on. "" and "off" are off, "on"
// is on (spaces trimmed, values exact); any other value is an error naming the
// variable. A plain function taking Config, like ParseDevAck.
func ParseWriteMode(c Config) (bool, error)
```
The three pieces are byte-identical to the P1a-1 branch (`7ff1b47`) and sit where it put them (after `DevAck`,
after `ParseDevAck`), so the two merge cleanly.
`docs/api.md`: snapshot gains `mode: {write: bool, solo: bool, auto_approve: []string}` (item 4 fills `solo` and
`auto_approve`) and `open_proposals[].blocked: "write_mode_off"` (every layer approved, nothing ran); `GET /healthz`
gains `write_mode` (`"off"` | `"on"`); a "Write mode" paragraph under Proposals. Lead decision recorded here: the
default is **off** for every new install; `deploy/pilot.env.example` (`PIPEROOM_WRITE_MODE=on`),
`deploy/docker-compose.pilot.yml` (`${PIPEROOM_WRITE_MODE:-on}`), the Helm values (`"on"`), `dev/demo.env` and the
`piperoom-shadow` service of `dev/docker-compose.yml` set it explicitly so nothing that works today stops working.

### 2. Auto-approve delay (P2) — `internal/config/config.go`

```go
AutoApproveDelay string // PIPEROOM_AUTO_APPROVE_DELAY: Go duration before a policy-approved proposal runs; default "60s"; "0" turns the policy off even when owners.yaml lists actions

const DefaultAutoApproveDelay = "60s"
func ParseAutoApproveDelay(c Config) (time.Duration, error) // "0" → 0 (off); else a positive Go duration; "" = 60s
```

### 3. owners.yaml schema (P2a, P4a implement in `internal/owners`, A3's folder; the schema is fixed here)

```yaml
pipelines:
  - id: orders
    kind: cdc            # cdc (default) | batch   (item 8; events.PipelineKindCDC / PipelineKindBatch)
    auto_approve: [connect_restart_task, connect_resume_connector]   # item 4; empty = none
    # a layer missing from both `layers:` and `owners:` is *unowned*: the driver approves it (item 4)
```
`kind: batch` pipelines carry `dags: [..]`, `dbt_project: <path or URL>`, `models: [..]`, `sink_tables`,
`sink_ts_column`; they have no `topic_prefix`, `connectors`, `consumer_groups` or `source_tables`. `Load` refuses a
field of the other kind. A pipeline's layers are `events.LayersOf(kind)` (item 8).

### 4. Solo ceremony events and routes (P2) — `internal/events/events.go`, `docs/api.md`

- `ApprovalGivenPayload` gains `Grant string json:"grant,omitempty"` and `Policy string json:"policy,omitempty"`.
  `Grant` is one of `events.GrantOwner` (`"owner"`), `GrantSolo` (`"solo"`), `GrantFallback` (`"fallback"`: unowned
  layer, the driver approved), `GrantPolicy` (`"policy"`: auto-approve; system actor, no `Identity`).
  `Policy` names the owners.yaml list that allowed a policy approval: `events.PolicyAutoApprove` (`"auto_approve"`).
  `Identity.Grant` keeps the principal detail (`"user:…"`, `"group:…"`, `"backup:…"`, `"solo"`, and now
  `"fallback"`). An event without `grant` predates P2: `"solo"` from `identity.grant`, anything else is owner.
- `ProposalCreatedPayload.FallbackLayers []string json:"fallback_layers,omitempty"`: the required layers nobody
  owns, fixed at creation; not part of `params_hash`.
- New events, system actor for `proposal.scheduled`, the person for `proposal.cancelled`; both count against the
  room log caps like `approval.given` (the 2% reserve; not exempt like `room.closed`):
```go
ProposalScheduled Type = "proposal.scheduled"
ProposalCancelled Type = "proposal.cancelled"

type ProposalScheduledPayload struct {
	ProposalID  string    `json:"proposal_id"`
	RunAt       time.Time `json:"run_at"`
	Policy      string    `json:"policy"` // PolicyAutoApprove
	CancelUntil time.Time `json:"cancel_until"` // = RunAt in this wave
}
type ProposalCancelledPayload struct {
	ProposalID string `json:"proposal_id"`
	By         Actor  `json:"by"`     // the person; also the event actor
	Reason     string `json:"reason"` // typed text, ≤ 500 chars
}
```
- `POST /api/proposals/{pid}/approve-all` `{params_hash, confirm?}` → `[Event]`: caller must own (or, in solo mode,
  be allowed, or as driver hold a `fallback_layers` entry) **every** `required_layers` entry; one `approval.given`
  per not-yet-approved layer in `required_layers` order, then execution as `approve` does. `403 not_layer_owner`
  names the first layer the caller lacks; nothing is recorded. Solo confirm `"<pipeline_id>/*"` once per call.
- `POST /api/proposals/{pid}/cancel` `{reason?}`: driver or any required-layer owner; only while a
  `proposal.scheduled` is pending; emits `proposal.cancelled`; `409 not_scheduled` otherwise.
- Snapshot: `open_proposals[].scheduled: {run_at, cancel_until}` while pending, `open_proposals[].fallback_layers`;
  `mode.solo` is true when the caller may approve every layer of the pipeline's kind (derived;
  `PIPEROOM_SOLO_USER` still implies it); `mode.auto_approve` lists the pipeline's policy actions.
- `ControlGrantedPayload.Reason string json:"reason,omitempty"`; `events.ControlReasonSoleMember` (`"sole_member"`)
  when the room's first member becomes driver without a request (system actor).

### 5. Driver-authored proposals (P3c) — `internal/events/events.go`, `docs/api.md`

- `ProposalCreatedPayload.Origin string json:"origin,omitempty"`: `events.ProposalOriginAgent` (`"agent"`, also
  what an empty value reads as) | `ProposalOriginDriver` (`"driver"`). Not part of `params_hash`.
- `POST /api/rooms/{id}/proposals` `{title, risk, rollback, steps: [{action, layer, params, description?}]}`:
  driver only; never in a shadow room; the same validation, scope check, `params_hash` and preview as an agent
  proposal; `400 validation` names the step; at most 5 open driver proposals per room (`429 too_many_proposals`).
  The agent gets a `HumanInput` kind `proposal_created` (item 6) so it can verify after execution.
- `GET /api/actions`: `[{name, layer, describe, params_schema, preview: bool}]` of the registry the agent proposes
  from (also with write mode off), for any logged-in member; `params_schema` is the action's `ParamsSchema()`,
  `preview` whether it implements `actions.Preview`. Never includes client state or credentials. The web form of
  P3d is generated from it.

### 6. Agent contract (P2d, P3e, P4b) — `internal/agent/contract.go`

- `HumanInput.Kind` gains `"proposal_created"` and `"proposal_executed"`, with two new fields:
```go
Origin  string  `json:"origin,omitempty"`   // both kinds: the proposal's Origin ("driver" in P3)
StepsOK []*bool `json:"steps_ok,omitempty"` // proposal_executed only: per step, nil = outcome unknown, false = failed or not run
```
  `ProposalID` names the proposal; for `proposal_created`, `Text` is the server's rendering of it (title, steps,
  risk, rollback), the driver's words: untrusted data. `proposal_executed` is sent once after the last step ran.
  The runner already receives `action_result` for its own proposals; these two let it verify a driver's.
- `Alert.PipelineKind string json:"pipeline_kind,omitempty"` (`"cdc"` | `"batch"`; empty reads as `"cdc"`), filled
  by the room service from owners.

### 7. Write client interfaces (P3) — `internal/clients/clients.go`, `internal/config/config.go`

The P3 writers are **new interfaces**, not embedded in `ConnectClient`, `KafkaAdmin` or `SourceDB`, so every
existing fake and the eval replay compile unchanged; the actions assert or receive them, and wiring builds them
only with write mode on.
```go
// ConnectFixWriter: implemented by internal/clients/connect (P3a-1), asserted by the actions.
type ConnectFixWriter interface {
	PauseConnector(ctx context.Context, name string) error                                     // PUT /connectors/{name}/pause
	UpdateConnectorConfigKeys(ctx context.Context, name string, changes map[string]string) error // PATCH /connectors/{name}/config, N keys
}

// KafkaWriter is implemented by the Kafka client (P3a-2); wiring hands it to the actions only when write mode is on.
type KafkaWriter interface {
	// ResetGroupOffsets moves every partition of the group's committed topics to
	// the target; the group must have no active members (ErrGroupActive).
	ResetGroupOffsets(ctx context.Context, group string, to OffsetTarget) (moved int, err error)
	// Produce writes records to topic in order and returns how many were acked;
	// it stops at the first record that is not acked.
	Produce(ctx context.Context, topic string, recs []Record) (acked int, err error)
}
type OffsetTarget struct {
	Earliest  bool      `json:"earliest,omitempty"`
	Timestamp time.Time `json:"timestamp,omitzero"` // first offset at or after this time
}
var ErrGroupActive = errors.New("clients: consumer group has active members")

// Record is a whole Kafka record (key, value, headers): actions only, never logged or put in an event.
// Produce ignores Topic, Partition, Offset and Timestamp (partitioner by key; produce time).
type Record struct {
	Topic string; Partition int32; Offset int64; Timestamp time.Time
	Key, Value []byte; Headers []RecordHeader // json: topic, partition, offset, timestamp, key, value, headers
}
type RecordHeader struct { Key string; Value []byte }

// RecordReader reads whole records for dlq_replay; optional, actions only (Preview may call it).
type RecordReader interface {
	ReadRecords(ctx context.Context, topic string, partition int32, from int64, limit int) ([]Record, error)
}

// SourceWriter is implemented by the source Postgres client when write mode is on (separate pool).
type SourceWriter interface {
	AddPublicationTables(ctx context.Context, publication string, tables []string) error // ALTER PUBLICATION … ADD TABLE
	DropReplicationSlot(ctx context.Context, slot string) error                           // only inactive slots (ErrSlotActive)
	InsertSnapshotSignal(ctx context.Context, signalTable, connectorID string, tables []string) (signalID string, err error)
}
var ErrSlotActive = errors.New("clients: replication slot is active")
```
`config.go`: `SourceDBWriteURL string // SOURCE_DB_WRITE_URL` (secret; empty = no `SourceWriter`, and the pg actions
refuse at `Validate` with "no write credentials"). Every method takes the room's pipeline scope from `ctx`
(`tools.WithPipeline`) and the actions re-check it (`actions.Scoped`).

### 8. Pipeline kinds (P4) — `internal/events/events.go`, `internal/topology/graph.go`, `internal/clients/clients.go`, `internal/config/config.go`

- `events.Layers` += `"orchestrator"`, `"transform"` (appended after the six CDC layers: `Layers` is "every known
  layer"). `events.PipelineKindCDC = "cdc"`, `events.PipelineKindBatch = "batch"`.
  `events.LayersOf(kind string) []string`: `cdc` (and `""`) → the six of today; `batch` → `orchestrator`,
  `transform`, `warehouse`; unknown → nil; a new slice on every call.
- `topology.NodeKind` += `KindDAG = "dag"`, `KindTask = "task"`, `KindModel = "model"` (ids `dag:<dag_id>`,
  `task:<dag_id>.<task_id>`, `model:<name>`); `Node.Layer` for them is `orchestrator`, `orchestrator`, `transform`.
  `Graph.Partial bool json:"partial,omitempty"`: an optional reader the kind needs is missing or answers
  `ErrNotSupported`, so its nodes are left out.
- Readers (optional, like `ClusterReader`: a nil one makes the tool answer `clients.ErrNotSupported`; unknown DAG,
  run, task or model → `ErrNotFound`):
```go
type OrchestratorReader interface {
	DAGRuns(ctx context.Context, dagID string, limit int) ([]DAGRun, error)                               // newest first
	TaskInstances(ctx context.Context, dagID, runID string) ([]TaskInstance, error)
	TaskLogTail(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (string, error) // from the END
}
// DAGScheduleReader is optional; GET /api/v1/dags/{dag_id}.
type DAGScheduleReader interface {
	DAG(ctx context.Context, dagID string) (DAG, error)
}
type TransformReader interface {
	RunResults(ctx context.Context) (RunResults, error)              // latest run_results.json
	ModelLineage(ctx context.Context, model string) (Lineage, error) // parents and children from manifest.json
}

type DAGRun struct {
	ID          string    `json:"id"`
	State       string    `json:"state"`
	StartedAt   time.Time `json:"started_at"`
	EndedAt     time.Time `json:"ended_at"`
	Note        string    `json:"note,omitempty"` // untrusted text
	LogicalDate time.Time `json:"logical_date"`
	RunType     string    `json:"run_type"`
}
type TaskInstance struct {
	TaskID    string    `json:"task_id"`
	Try       int       `json:"try"`
	State     string    `json:"state"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Operator  string    `json:"operator"`
	MapIndex  int       `json:"map_index"` // -1 unmapped
}
type DAG struct {
	ID        string    `json:"id"`
	Schedule  string    `json:"schedule"`
	Paused    bool      `json:"paused"`
	NextRunAt time.Time `json:"next_run_at"`
}
type RunResults struct {
	GeneratedAt time.Time     `json:"generated_at"`
	Results     []ModelResult `json:"results"`
}
type ModelResult struct {
	Model   string  `json:"model"`             // unique_id
	Status  string  `json:"status"`
	Message string  `json:"message,omitempty"` // untrusted text
	Rows    int64   `json:"rows"`              // -1 when not reported
	Seconds float64 `json:"seconds"`
}
type Lineage struct {
	Model    string   `json:"model"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
}
```
Log text, notes and messages are untrusted: the tool wraps them like every other read.
- `config.go`: `AIRFLOW_URL`, `AIRFLOW_USERNAME` + `AIRFLOW_PASSWORD` or `AIRFLOW_TOKEN` (`ParseAirflow`: http(s)
  URL without user info, one login or none, a login without a URL is an error); `DBT_ARTIFACTS_DIR` (absolute) or
  `DBT_CLOUD_URL` + `DBT_CLOUD_TOKEN` + `DBT_CLOUD_ACCOUNT_ID` + `DBT_CLOUD_JOB_ID` (`ParseDBT`: one source, the
  cloud four together, ids whole numbers). `WAREHOUSE_KIND` accepts `bigquery`, `databricks` with
  `WAREHOUSE_DB_URL` as the driver DSN (`internal/wiring` validates the value and adds the cases in P4e).
- `docs/api.md`: `GET /api/pipelines` gains `kind`; `GET /api/pipelines/{pid}/topology` nodes carry the new kinds
  and the graph `partial`; the P-wave events section says the two layers are valid wherever a layer is.

### 9. Deferred (not in this wave)

A `piperoom.yaml` config file with env overrides; Slack inbound; a free-form command runner. Each needs its own
design page first.

## Files the P0 PR touches outside the contract set

Kept to what the build, the tests and "nothing that works today changes" need:
- `evals/judge.go` (C6): the judge prompt lists `events.LayersOf("cdc")`, not `events.Layers`, so the prompt text
  and every recorded verdict stay valid.
- `internal/discover/render.go` (C10): the draft names the CDC layers only (discovery finds CDC pipelines).
- `internal/room/access_test.go`, `internal/httpapi/solo_test.go` (A5): the solo user's `/api/me` layers are
  "every layer", now `events.Layers` (eight), not a literal six.
- `deploy/pilot.env.example`, `deploy/docker-compose.pilot.yml`, `deploy/helm/piperoom/values.yaml` (C1),
  `dev/demo.env`, `dev/docker-compose.yml` (A1): `PIPEROOM_WRITE_MODE=on` (item 1).

## Done when

- [x] The P0 PR changes only the contract files, `AGENTS.md`, this file and `docs/api.md`, plus the files listed
      above; `go build ./...`, `go vet ./...`, `go test ./...` pass with every stub and fake compiling: new interface
      methods went on **new** interfaces.
- [x] Every item above has a line in `docs/api.md` or a doc comment, and the four task files cite it by number.
