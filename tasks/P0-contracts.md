# P0 — Contracts of the P wave (leads only)

Owner: leads (`[LEAD-ARCH]`), one PR titled `[P0] contracts: write mode, solo ceremony, write clients, pipeline kinds`.
Plan: `docs/product/fit-2026-10.md`. Merges first, alone; every P PR codes against the items below by number
("Uses (P0)" in `AGENTS.md`). The code here is the proposal; the P0 PR carries the final text and updates this
file to match before P1–P4 are dispatched.

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
```
`ParseWriteMode()` refuses any other value. `docs/api.md`: snapshot gains
`mode: {write: bool, solo: bool, auto_approve: []string}` (item 4 fills `solo` and `auto_approve`); `GET /healthz`
gains `write_mode`. Lead decision recorded here: the default is **off** for every new install; `deploy/pilot.env.example`
and `dev/` set it explicitly so nothing that works today stops working.

### 2. Auto-approve delay (P2) — `internal/config/config.go`

```go
AutoApproveDelay string // PIPEROOM_AUTO_APPROVE_DELAY: Go duration before a policy-approved proposal runs; default "60s"; "0" turns the policy off even when owners.yaml lists actions
```

### 3. owners.yaml schema (P2a, P4a implement in `internal/owners`, A3's folder; the schema is fixed here)

```yaml
pipelines:
  - id: orders
    kind: cdc            # cdc (default) | batch   (item 8)
    auto_approve: [connect_restart_task, connect_resume_connector]   # item 4; empty = none
    # a layer missing from both `layers:` and `owners:` is *unowned*: the driver approves it (item 4)
```
`kind: batch` pipelines carry `dags: [..]`, `dbt_project: <path or URL>`, `models: [..]`, `sink_tables`,
`sink_ts_column`; they have no `topic_prefix`, `connectors`, `consumer_groups` or `source_tables`. `Load` refuses a
field of the other kind.

### 4. Solo ceremony events and routes (P2) — `internal/events/events.go`, `docs/api.md`

- `ApprovalGiven.Grant` accepts `"solo"` (exists), `"owner"`, `"fallback"` (unowned layer, driver approved),
  `"policy"` (auto-approve). `ApprovalGiven.Policy string json:"policy,omitempty"` names the list entry.
- New events: `proposal.scheduled` `{proposal_id, run_at (RFC3339), policy: "auto_approve", cancel_until}` and
  `proposal.cancelled` `{proposal_id, by, reason}`; both `classAlways` is **not** granted: they count against
  the room log like `approval.given`.
- `POST /api/proposals/{pid}/approve-all` `{params_hash, confirm}`: caller must own (or, in solo mode, be
  allowed) **every** `required_layers` entry; one `approval.given` per layer in `required_layers` order, then
  execution as `approve` does. `403 not_layer_owner` names the first layer the caller lacks; nothing is recorded.
- `POST /api/proposals/{pid}/cancel` `{reason}`: driver or any required-layer owner; only while a
  `proposal.scheduled` is pending; emits `proposal.cancelled`; `409 not_scheduled` otherwise.
- Snapshot: `open_proposals[].scheduled: {run_at, cancel_until}` while pending; `mode.solo` is true when the
  caller may approve every layer of the pipeline (derived; `PIPEROOM_SOLO_USER` still implies it); `mode.auto_approve`
  lists the pipeline's policy actions.
- `control.granted` gains `reason: "sole_member"` when the room's first member becomes driver without a request.

### 5. Driver-authored proposals (P3c) — `internal/events/events.go`, `docs/api.md`

- `ProposalCreated.Origin string json:"origin"`: `"agent"` (default when empty on read) | `"driver"`.
- `POST /api/rooms/{id}/proposals` `{title, risk, rollback, steps: [{action, layer, params}]}`: driver only;
  the same validation, scope check, `params_hash` and preview as an agent proposal; `400 validation` names the
  step. The agent gets a `HumanInput` kind `proposal_created` (item 6) so it can verify after execution.
- `GET /api/actions` (does not exist today): `[{name, layer, describe, params_schema, preview: bool}]` of the
  registry the agent proposes from, for any logged-in member; `params_schema` is the action's `ParamsSchema()`.
  Never includes client state or credentials. The web form of P3d is generated from it.

### 6. Agent contract (P2d, P3e) — `internal/agent/contract.go`

- `HumanInput.Kind` gains `"proposal_created"` (payload: proposal id, origin `driver`) and `"proposal_executed"`
  (proposal id, per-step ok). The runner already receives `action.executed` for its own proposals; these two let
  it verify a driver's.
- `Alert` gains `PipelineKind string` (`"cdc"` | `"batch"`), filled by the room service from owners.

### 7. Write client interfaces (P3) — `internal/clients/clients.go`

```go
type ConnectWriter interface {
	RestartTask(ctx context.Context, name string, task int) error
	RestartConnector(ctx context.Context, name string) error
	PauseConnector(ctx context.Context, name string) error                                   // PUT /connectors/{name}/pause
	UpdateConnectorConfigKeys(ctx context.Context, name string, changes map[string]string) error // PATCH /connectors/{name}/config, N keys
}

// KafkaWriter is implemented by the Kafka client when write mode is on.
type KafkaWriter interface {
	// ResetGroupOffsets moves every partition of the group's committed topics to
	// the target; the group must have no active members (ErrGroupActive).
	ResetGroupOffsets(ctx context.Context, group string, to OffsetTarget) (moved int, err error)
	// Produce writes records to topic in order and returns how many were acked.
	Produce(ctx context.Context, topic string, recs []Record) (acked int, err error)
}
type OffsetTarget struct {
	Earliest  bool      `json:"earliest,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"` // first offset at or after this time
}
var ErrGroupActive = errors.New("clients: consumer group has active members")

// SourceWriter is implemented by the source Postgres client when write mode is on.
type SourceWriter interface {
	AddPublicationTables(ctx context.Context, publication string, tables []string) error // ALTER PUBLICATION … ADD TABLE
	DropReplicationSlot(ctx context.Context, slot string) error                           // only inactive slots (ErrSlotActive)
	InsertSnapshotSignal(ctx context.Context, signalTable, connectorID string, tables []string) (signalID string, err error)
}
var ErrSlotActive = errors.New("clients: replication slot is active")
```
`Record` is the DTO `PeekDLQ` returns today. Every method takes the room's pipeline scope from `ctx`
(`tools.WithPipeline`) and the actions re-check it (`actions.Scoped`).

### 8. Pipeline kinds (P4) — `internal/events/events.go`, `internal/topology/graph.go`, `internal/clients/clients.go`

- `events.Layers` += `"orchestrator"`, `"transform"`. `events.LayersOf(kind string) []string`: `cdc` → the six
  of today; `batch` → `orchestrator`, `transform`, `warehouse`.
- `topology.NodeKind` += `KindDAG = "dag"`, `KindTask = "task"`, `KindModel = "model"`; `Node.Layer` for them is
  `orchestrator`, `orchestrator`, `transform`.
- Readers (optional, like `ClusterReader`: a nil one makes the tool answer `clients.ErrNotSupported`):
```go
type OrchestratorReader interface {
	DAGRuns(ctx context.Context, dagID string, limit int) ([]DAGRun, error)
	TaskInstances(ctx context.Context, dagID, runID string) ([]TaskInstance, error)
	TaskLogTail(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (string, error)
}
type TransformReader interface {
	RunResults(ctx context.Context) (RunResults, error)          // latest run_results.json
	ModelLineage(ctx context.Context, model string) (Lineage, error) // parents and children from manifest.json
}
```
DTOs `DAGRun{ID, State, StartedAt, EndedAt, Note}`, `TaskInstance{TaskID, Try, State, StartedAt, EndedAt,
Operator}`, `RunResults{GeneratedAt, Results []ModelResult}`, `ModelResult{Model, Status, Message, Rows, Seconds}`,
`Lineage{Model, Parents, Children []string}`. Log text is untrusted: the tool wraps it like every other read.
- `config.go`: `AIRFLOW_URL`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` (or `AIRFLOW_TOKEN`), `DBT_ARTIFACTS_DIR`
  or `DBT_CLOUD_URL` + `DBT_CLOUD_TOKEN` + `DBT_CLOUD_JOB_ID`; `WAREHOUSE_KIND` accepts `bigquery`, `databricks`
  with `WAREHOUSE_DB_URL` as the driver DSN.
- `docs/api.md`: `GET /api/pipelines` gains `kind`; `GET /api/pipelines/{pid}/topology` nodes carry the new kinds.

### 9. Deferred (not in this wave)

A `piperoom.yaml` config file with env overrides; Slack inbound; a free-form command runner. Each needs its own
design page first.

## Done when

- [ ] The P0 PR changes only the contract files, `AGENTS.md`, this file and `docs/api.md`; `go build ./...`,
      `go vet ./...`, `go test ./...` pass with every stub (`internal/agent/stubs.go`, fakes in `internal/clients/fake`
      if present, `demo.Fake*`, evals replay clients) compiling: new interface methods go on **new** interfaces
      or are added to fakes in the same PR.
- [ ] Every item above has a line in `docs/api.md` or a doc comment, and the four task files cite it by number.
