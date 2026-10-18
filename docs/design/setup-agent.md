# Setup agent: design (main @ 6cf24f60, 2026-10-18)

Owner of the design: LEAD-ARCH. Plan: `docs/product/setup-2026-10.md`. Board: `tasks/U-board.md`. Contract
changes land in U0 and the design rows (`U1-0`, `U12-0`, `U13-0`); the shapes below are proposals in the style of
`tasks/P0-contracts.md` and the Q0 items: new interfaces and new events only, so every existing fake and the eval
replay compile unchanged. Where this text and merged code differ, the code and its doc comments win.

**Decision recorded:** setup is a functionality of Piperoom (plan §2). One agent, one room, one approval model, one
`owners.yaml`, one web UI. This page is about how the setup capability is built inside that.

## 1. Components and folders

```
browser ──► server (cmd/server) ◄──── runner (cmd/setup) ── docker · kubectl · aws/confluent/aiven CLIs/APIs
   ▲            │  ▲                        │                · the source DB · the user's project dir
   │            │  └─ /api/runners/ws ──────┘  (outbound from the runner; typed ops in, results out)
   │            ▼
   │      room_events (setup.* events, proposals, approvals, action.executed with receipts)
   │            │
   └── same web room, setup stepper (web/**)
            agent.Run in setup mode: setup tools (KindRead, over the runner) + setup actions (forwarded to the runner)
```

| Folder | Owner | Holds |
|---|---|---|
| `cmd/setup/**` | A9 (new) | the runner binary: `piperoom-setup [--server URL --code XXXX] [--standalone] [--project DIR]`; the no-server mode embeds `cmd/server`'s quickstart (`internal/setup/standalone`) |
| `internal/setup/runner/**` | A9 | the runner: WebSocket client, pairing, capability detection, op dispatch, the local secret store, redaction before transport |
| `internal/setup/ops/**` | A9 | the operation catalogue: one Go type per op with its JSON Schema, `argv` builder or API call, tier, preview, rollback; `ops.Registry` |
| `internal/setup/protocol/**` | A9 | the wire types between server and runner (`Hello`, `OpRequest`, `OpResult`, `Receipt`, `Cancel`), versioned |
| `internal/setup/plan/**` | A9 | the plan model (`Plan`, `Phase`, `Step`), the verification ladder, the environment classifier, the bounds |
| `internal/setup/tools/**` | A9 | the KindRead tools the setup agent calls (each one forwards a T0 op to the runner) and the KindRoom `setup_plan` tool |
| `internal/setup/actions/**` | A9 | the `actions.Action` adapters that forward an op to the paired runner; `Preview` and `Scoped` per op |
| `internal/setup/standalone/**` | A9 | no-server mode: starts the quickstart server in-process, attaches the runner, opens the browser |
| `internal/setup/targets/compose/**`, `targets/host/**` | C19 (new) | the Compose and plain-host drivers: the ops, templates and checks for each target |
| `internal/setup/targets/strimzi/**`, `targets/server/**` | C20 (new) | the Strimzi and Debezium Server drivers |
| `internal/setup/targets/managed/**`, `internal/clients/mskconnect/**`, `internal/clients/confluentcloud/**`, `internal/clients/aiven/**` | C21 (new) | the managed drivers and their API clients |
| `internal/setup/sources/postgres/**` | A3 | Postgres prerequisites over the Q2 interfaces |
| `internal/setup/sources/mysql/**` | A8 | MySQL prerequisites over the Q8 client |
| `internal/setup/sources/sqlserver/**`, `sources/mongodb/**`, `sources/oracle/**`, `internal/clients/sqlserver/**`, `internal/clients/mongodb/**`, `internal/clients/oracle/**` | A10 (new) | the three new thin clients and their prerequisite drivers |
| `internal/setup/knowledge/**` | C22 (new) | SMT, sink, Debezium Server sink and tuning knowledge: embedded YAML like the Q1 known-issues index, with the config keys, defaults, version ranges and the checks each implies; the embedded reference project templates |
| `internal/room/runners*.go`, `internal/httpapi/runners*.go`, `internal/ws/runner*.go`, `internal/owners/reload*.go` | A5 | runner registry, pairing, the `/api/runners/ws` endpoint, the setup routes, owners hot-reload (A5 asks A3 for `internal/owners`) |
| `internal/agent/setup*.go`, `internal/wiring/setup.go` | A4 | the setup prompt frame, the mode switch in `Run`, the tiering hook, wiring of setup registries |
| `web/src/setup/**` | A6 | the setup stepper, runner status, canary prompt, the "New pipeline" flow |
| `evals/setup/**`, `evals/scenarios/setup/*.yaml` | C6, C7 | runner recordings replay, setup scenarios |
| `dev/setup/**`, `cmd/devctl` `setup-break` | A1 | broken environments for the agent to fix on the dev stack |
| `.github/workflows/setup-matrix.yml` | C1 | the three-OS matrix, kind for Strimzi |
| `docs/pilot/setup-agent.md` | C14 | the user's page |

A target driver and a source driver are **data plus checks**, not code paths the agent branches on: each registers
its ops, its templates, its verification rungs and its known failure signatures with the plan package. The agent
sees one catalogue whose entries say which target and source they belong to.

## 2. The runner

### 2.1 Pairing and transport

- `piperoom-setup --server https://piperoom.acme --code 7Q4K` opens one WebSocket to `/api/runners/ws` with the
  pairing code in the first frame. The code comes from `POST /api/setup` (the UI's **Set up** button), is 6
  characters from a 32-character alphabet, lives 10 minutes, and pairs the runner to exactly one room and pipeline
  id. The server answers with a runner token (random, 32 bytes, stored only in memory on both sides) that
  authenticates reconnects for the life of the room. Pairing is the only moment a human links a machine to a room.
- The runner's first frame after pairing is `Hello`: OS, architecture, hostname (redacted to a label the user may
  change), the tools found on `PATH` with versions (`docker`, `docker compose`, `kubectl`, `helm`, `aws`,
  `confluent`, `avn`, `java`, `systemctl`), the project directory, the catalogue version. The server stores it on
  `setup.runner_attached`.
- Frames are JSON, at most 1 MiB; an `OpResult` larger than that is cut on the runner with the summary marker and
  `truncated: true`. Ping every 20 s; a runner silent for 90 s is `setup.runner_lost` and every in-flight step is
  marked unknown (`OK == nil`), as an interrupted executor step is today.
- TLS to the server is required unless loopback (`netsec` rule). The runner never listens on a port.
- One runner per room. A second pairing for the same room replaces the first only after the first is lost.

### 2.2 Operation execution

```go
// protocol.OpRequest is what the server sends; the runner executes exactly this.
type OpRequest struct {
	ID        string          `json:"id"`         // step id; results and receipts carry it back
	Op        string          `json:"op"`         // ops.Registry name, e.g. "compose_up"
	Params    json.RawMessage `json:"params"`     // validated against the op's schema on both sides
	Tier      string          `json:"tier"`       // T0..T3 as the server decided; the runner refuses a tier below the op's own
	Deadline  time.Time       `json:"deadline"`
	ProposalID string         `json:"proposal_id,omitempty"` // T1+ only: the approved proposal; the runner refuses a T1+ op without one
}

type OpResult struct {
	ID        string          `json:"id"`
	OK        bool            `json:"ok"`
	Summary   string          `json:"summary"`            // redacted on the runner, ≤ tools.MaxSummaryBytes
	Data      json.RawMessage `json:"data,omitempty"`     // redacted, ≤ 64 KiB
	Receipt   Receipt         `json:"receipt"`
	Truncated bool            `json:"truncated,omitempty"`
}

// Receipt is the setup equivalent of tools.Call: what a person would re-run.
type Receipt struct {
	Kind     string    `json:"kind"`              // "exec" | "http" | "sql" | "file"
	Argv     []string  `json:"argv,omitempty"`    // exec: the exact argv, secrets replaced by ${secret:name}
	Target   string    `json:"target"`            // host, URL, file path, database
	ExitCode int       `json:"exit_code"`
	At       time.Time `json:"at"`
	TookMS   int64     `json:"took_ms"`
	OutHash  string    `json:"out_hash"`          // SHA-256 of the raw output before redaction, so a dispute can be settled without the output
}
```

The runner validates `Params` against the op's schema a second time, builds `argv` or the API call itself, runs it
with the op's own timeout under the deadline, and never passes model text to a shell. There is no shell: every exec
op uses `os/exec` with an argv slice; on Windows the ops that need a service manager call the Win32 APIs through
`golang.org/x/sys/windows/svc/mgr` (rule 9: confirmed with `go doc` before use).

### 2.3 Catalogue rules

Every op is a Go value implementing:

```go
// ops.Op is one typed operation the runner can execute. There is no generic exec.
type Op interface {
	Name() string                       // snake_case, unique: "compose_up", "file_write", "k8s_apply"
	Target() string                     // "compose" | "host" | "strimzi" | "msk" | "confluent" | "aiven" | "dbzserver" | "" (any)
	Tier() string                       // the op's floor: T0 read-only ops never carry a lower tier
	Layer() string                      // the layer whose owner approves: "environment", "postgres", "kafka-connect", ...
	Describe() string
	ParamsSchema() map[string]any
	Validate(params json.RawMessage, env Env) error    // no I/O; checks paths against Env.ProjectDir, hosts against Env.Targets
	Preview(ctx context.Context, params json.RawMessage, env Env) (events.PreviewResult, error) // read-only; diff, dry-run
	Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error)
	Rollback(params json.RawMessage, result Result) (name string, rollbackParams json.RawMessage, ok bool)
}
```

Rules, each with a test in `internal/setup/ops`:

- A `file_write` path must resolve (symlinks followed) under `Env.ProjectDir`; `..`, absolute paths outside it,
  device files and paths under `.git/` are refused. Content with a key that matches the `looksSecret` rule of `internal/actions/connector_config.go` (exported by U0 as
  `actions.LooksSecret`) or a value that `redact.Text` changes is refused: secrets go through `${secret:name}`.
- An op that reaches a host reaches only a host in `Env.Targets` (the list the user named at start plus what the
  plan registered through `setup_register_target`, which is itself a T0 op the user sees).
- No op deletes data: there is no `drop_table`, no `delete_topic`, no `compose_down_volumes`. `compose_down` keeps
  volumes. Rollback ops are ordinary ops under the same rules.
- Output goes through `redact.Text` on the runner before it enters `OpResult`, and `Data` is capped at 64 KiB.
- The runner refuses a T1 or higher request without a `ProposalID`, and refuses an op whose `Tier()` is above the
  request's tier. The server is the authority on tiers (§3.3); the runner is the second lock.

The catalogue at the end of U1 (Compose, Postgres), by tier:

| Tier | Ops |
|---|---|
| T0 | `env_detect`, `file_read`, `dir_list`, `docker_info`, `compose_ps`, `compose_config` (render), `container_logs`, `container_inspect`, `net_probe` (the Q6a probe from the runner's position), `http_get` (Connect and Registry REST, hosts from `Env.Targets` only), `source_settings`, `source_table_replication`, `source_role_grants` (Q2a through the runner's DB connection), `jar_list` (plugin dir contents and manifest versions), `java_version` |
| T1 | `file_write`, `file_delete` (under the project dir), `compose_up`, `compose_restart`, `compose_down`, `plugin_install` (download from `PIPEROOM_SETUP_PLUGIN_HOSTS` only, verify the published SHA-256, unpack into the plugin dir), `secret_set` (asks the person on the runner's terminal or the UI; never the model) |
| T2 | `connect_create_connector`, `connect_delete_connector` (Q5c, through the runner's reach), `source_create_publication`, `source_set_replica_identity`, `source_grant_select`, `source_create_heartbeat_table`, `source_create_canary_table`, `source_insert_canary` (Q2 admin actions plus three setup-only ones) |
| T3 | `source_alter_system` (`wal_level`, `max_replication_slots`, `max_wal_senders`, with `pending_restart` in the preview), `source_restart` (only for a database the setup created) |

Each later target or source adds its own ops under the same interface (U-board rows name them).

### 2.4 Secrets on the runner

`PIPEROOM_SETUP_SECRETS_FILE` (default `<project dir>/.piperoom/secrets.env`, created 0600, listed in a generated
`.gitignore`) or the environment. `secret_set {name}` prompts on the runner's terminal (or the UI's secret field,
which posts to the runner through the server in a frame the server stores nowhere and logs nowhere). The model learns
only that `secret:db_password` exists. Materialisation points, all on the runner: compose `.env` files, Connect
connector configs as `${file:/secrets/db.properties:password}` with the file written next to the compose file,
Kubernetes Secrets (`k8s_secret_apply`, T2), cloud API calls. A receipt shows `${secret:name}`, never the value.

## 3. The agent loop in setup mode

### 3.1 Same loop, three differences

`agent.Run` is unchanged. Setup mode is `Alert.Mode == "setup"` (contract item 2) and changes:

1. **The prompt frame** (`internal/agent/setup_prompt.go`): the alert is "no pipeline yet"; the goal is the
   verification ladder; the rules say plan before propose, re-plan after failure, never ask the person to run a
   command, never ask for a secret value, stop and ask after the bounds. The untrusted-data wrapper, the receipts
   and the hypothesis grounding apply unchanged.
2. **The registries**: `Deps.Tools` holds the setup read tools (plus `pipeline_map`, `runbook_lookup`,
   `similar_incidents`, `connect_*` and `kafka_*` reads scoped to the new pipeline id); `Deps.Actions` holds the
   setup action adapters. Both are built by `wiring.SetupStack` only when a runner is paired to the room.
3. **The bounds** (`Options.Setup`): 3 attempts per step, 12 executed steps per plan, 45 min wall clock, a per-setup
   token budget (`PIPEROOM_SETUP_TOKEN_BUDGET`, default 2 M); on any bound the runner emits `setup.blocked` and goes
   idle like a budget stop today (`*BudgetError`).

### 3.2 Plan, act, observe, re-plan

- `setup_plan` (KindRoom): the model records `{phases: [{name, steps: [{op, params_hint, tier_expected, verify}]}],
  supersedes?}`. The server fills the real tier per step from the environment classes and stores `setup.plan`. A
  proposal whose step is not in the current plan is refused by `Proposals.Create` (`ErrNotInPlan`), so the model
  cannot act outside what it planned and showed.
- `propose_fix` with setup actions: the existing path, including `params_hash`, preview and approvals. The tier
  decides the route: T0 ops are tools, never proposals; T1 proposals are auto-scheduled (`proposal.scheduled`,
  policy `setup_local`, 20 s, cancellable) by the P2 mechanism; T2 and T3 wait for the layer owner.
- Observe: each step's `Verify` sentence is checked by the agent's existing verify-after-fix path with the setup
  read tools; the `agent.verification` event is the outcome. A failed verification is a failed step.
- Re-plan: the runner's `OpResult`, `container_logs` (redacted, untrusted), `runbook_lookup` with `versions` (Q1)
  and `similar_incidents` over labelled setup rooms go back to the model; it must emit a new `setup_plan` before
  the next proposal.

### 3.3 Environment classes and tiers

The server derives one class per target from facts in `Hello` and the T0 `env_detect` result, and from the
person's answer at start:

| Class | When |
|---|---|
| `local` | the target is loopback, a Docker Desktop or local engine socket, a compose project under the project dir, a database or Kafka the setup created in this room |
| `shared` | anything else, by default |
| `production` | the person said so at start for that target, or the target matches `PIPEROOM_SETUP_PROD_PATTERNS` (host globs, `kubectl` context names, cloud account ids), or the source database reports a production-looking fact (`rds.*` parameter group names are not enough; a non-loopback host is `shared`, not `production`) |

Tier of a step = max(op floor, class of its target): T0 on any class is a read; T1 only on `local`; `shared` makes
every write T2; `production` makes every write T3 and refuses the guard table (plan §3.6). The function
`plan.TierOf(op, target, class)` is pure and table-tested. The model cannot change a class; the person can raise
one from the UI (`POST /api/rooms/{id}/setup/targets/{t}/class`), never lower it below what the server derived.

### 3.4 The verification ladder

`plan.Ladder(source, target, sink)` returns the rungs in order; each is a read tool call and an expected predicate:

1. `connect_status`: connector RUNNING (or `describe-connector` RUNNING on MSK, the Confluent and Aiven equivalents)
2. every task RUNNING
3. `kafka_topic_offsets`: the canary topic exists
4. snapshot finished (`debezium_metrics` `SnapshotCompleted`, else the canary row count on the topic)
5. `source_insert_canary` (T2 on a shared source; T1 on a local one) then `kafka_find_by_key` within 60 s
6. `schema_registry_versions` for the canary subject when a registry is configured
7. `warehouse_row` for the canary key when a sink was set up
8. a real change the person makes (`setup.awaiting_change`, the UI shows the SQL to run), seen within 10 min

`setup.verified` carries every rung's outcome and receipt ids. Rungs 1 to 7 are the agent's; rung 8 is the person's
and the only thing the agent waits on a human for that is not an approval.

### 3.5 Finishing: the pipeline is watched

On `setup.verified` with rungs 1 to 7 green, the agent proposes one last step, `pipeline_register` (T2, layer
`environment`): the `owners.yaml` entry (id, kind `cdc`, topic prefix, source tables, connectors, consumer groups,
sink tables if any, the user as owner of every layer, `auto_approve: []`, `setup: {target, runner_label,
room_id}`). On the standalone server it writes the file; on a team server it writes `OWNERS_DIR/<id>.yaml` and the
server reloads (`owners.Reload`, `pipeline.registered` event, the watcher picks it up on its next tick), or, when the
pipeline's config lives in a repo listed in `repos:`, it opens the PR through Q7's `repo_open_pull_request` and the
room waits for `pr_merged` as Q7e does. The room then closes with resolution `pipeline live`; the hand-off writer
produces the summary; `room.labelled` gets `cause: setup` so L1 can learn from it.

## 4. Contract additions (proposed; U0 lands items 1 to 6, the design rows the rest)

### 1. Events, `internal/events/events.go`

```go
SetupRunnerAttached Type = "setup.runner_attached" // {runner_label, os, arch, tools: {name: version}, project_dir, catalogue_version}
SetupRunnerLost     Type = "setup.runner_lost"     // {runner_label, last_seen}
SetupPlan           Type = "setup.plan"            // {plan_id, supersedes?, phases: [{name, steps: [{index, op, target, tier, layer, verify}]}]}
SetupTargetClassed  Type = "setup.target_classed"  // {target, class, reason, by: "server" | "person"}
SetupVerified       Type = "setup.verified"        // {rungs: [{name, ok, step_id?, receipt_id?, note}], first_event_latency_ms?}
SetupAwaitingChange Type = "setup.awaiting_change" // {table, example_sql}
SetupBlocked        Type = "setup.blocked"         // {reason: "attempts" | "steps" | "wall_clock" | "budget" | "runner_lost" | "refused", step_index?, question}
PipelineRegistered  Type = "pipeline.registered"   // {pipeline_id, path | pull_request_url, by}
```
`ActionExecutedPayload` gains `Receipt *SetupReceipt json:"receipt,omitempty"` (the §2.2 receipt), set only by
setup actions. `ProposalScheduledPayload.Policy` gains the value `events.PolicySetupLocal` (`"setup_local"`).
`LayersOf("cdc")` gains `"environment"` **at the end** (unowned by every existing owners file, so the driver
approves it with grant `fallback` as P2b-3 does; no existing action names it). `room.created` gains
`mode: "setup"` next to `shadow`.

### 2. Agent, `internal/agent/contract.go`

```go
// Alert.Mode: "" (incident) or ModeSetup. In setup mode PipelineID is the id the setup will register and
// Raw holds the SetupRequest (item 5). (U)
Mode string `json:"mode,omitempty"`
const ModeSetup = "setup"

// Options.Setup bounds a setup run; nil outside setup mode. (U)
Setup *SetupBounds
type SetupBounds struct {
	AttemptsPerStep int           // default 3
	StepsPerPlan    int           // default 12
	WallClock       time.Duration // default 45m
	TokenBudget     int64         // PIPEROOM_SETUP_TOKEN_BUDGET, default 2_000_000
}
var ErrNotInPlan = errors.New("agent: proposal step is not in the current setup plan")
```

### 3. Actions, `internal/actions/action.go`

```go
// Tiered is implemented by an action whose approval route depends on the target's environment class (setup
// actions). The room service calls Tier after Validate with the class it derived for the step's target and routes
// the proposal: T0 is refused as a proposal (it is a tool), T1 is scheduled by policy setup_local, T2 waits for
// the layer owner, T3 waits for the owner plus the typed confirm. (U)
type Tiered interface {
	Tier(params json.RawMessage, class string) (string, error) // "T0".."T3"; error when the target is unknown
}
```
No change to `Action`, `Scoped` or `Preview`: setup actions implement all three. `looksSecret` of
`internal/actions/connector_config.go` is exported as `actions.LooksSecret(key string) bool` so the runner's ops and
Q4a share one rule.

### 4. Clients, `internal/clients/clients.go`

```go
// RunnerExecutor is how a setup action reaches the paired runner. The room service implements it over the
// runner registry; wiring hands it to the setup actions and tools of a room. ErrNoRunner when none is paired. (U)
type RunnerExecutor interface {
	Execute(ctx context.Context, roomID string, req setupproto.OpRequest) (setupproto.OpResult, error)
}
var ErrNoRunner = errors.New("clients: no runner paired to this room")
```
(The protocol types live in `internal/setup/protocol`, A9's; `clients.go` imports it, which is new and accepted:
the package has no dependencies of its own.)

### 5. Config, `internal/config/config.go`

`PIPEROOM_SETUP_ENABLED` (`off` | `on`, default `off`: the routes and the runner endpoint exist only when on),
`PIPEROOM_SETUP_PROD_PATTERNS` (comma list of globs), `PIPEROOM_SETUP_TOKEN_BUDGET`, `PIPEROOM_SETUP_LOCAL_DELAY`
(default `20s`, `0` = every T1 step waits for a click), `PIPEROOM_SETUP_PLUGIN_HOSTS` (default
`repo1.maven.org,hub.docker.com,debezium.io`), `OWNERS_DIR` (optional; a directory of per-pipeline YAML files loaded
after `OWNERS_FILE` and reloaded on `pipeline.registered`), `PIPEROOM_SETUP_SECRETS_FILE` (runner side),
`PIPEROOM_SETUP_SERVER_URL` and `PIPEROOM_SETUP_CODE` (runner side, the flags' defaults).

### 6. API, `docs/api.md`

| Method | Path | Body | Returns | Notes |
|---|---|---|---|---|
| POST | `/api/setup` | `{pipeline_id, title, source_kind, target, tables: [..], classes: {target: class}}` | `{room: RoomSnapshot, code, expires_at, command}` | opens a setup room (creator is driver), returns the pairing code and the exact `piperoom-setup` command; `pipeline_id` must not exist; `PIPEROOM_SETUP_ENABLED=on` |
| GET | `/api/runners/ws` | first frame `{code}` or `{token}` | WebSocket | the runner endpoint; one runner per room |
| POST | `/api/rooms/{id}/setup/targets/{t}/class` | `{class}` | `Event` (`setup.target_classed`) | driver only; raise only |
| POST | `/api/rooms/{id}/setup/secret` | `{name}` plus the value in a header that is never logged | `204` | forwarded to the runner in one frame, stored nowhere on the server |
| GET | `/api/pipelines` | | rows gain `setup: {room_id, target, registered_at}` when the pipeline was set up by Piperoom | |

Snapshot gains `setup: {mode: true, runner: {label, connected, tools}, plan_id, phase, step_index, bounds: {attempts, steps, wall_clock_left_s}, classes: {target: class}, verified: [...]}`.

### 7. Settled by the design rows

`U1-0`: the wire protocol's final field list and versioning; the Windows service API. `U12-0`: the Strimzi CRD
versions, the in-cluster runner Job and its RBAC. `U13-0`: the three managed API clients' auth (AWS SigV4 through the
shared AWS config, Confluent Cloud API keys, Aiven tokens) and which operations each exposes.

## 5. Targets: ops, verify, what is new per target

| Target | Ops added (tier) | Verify rung 1 source | New code |
|---|---|---|---|
| Compose (U1) | §2.3 | `connect_status` through `http_get` to the project's Connect | the driver, templates for the compose file (Kafka KRaft, Connect with the plugin dir mounted, optional Registry), the plugin layout check (flat jars, two Debezium versions, converter jars missing from 2.x images) |
| Host (U8) | `service_status`, `journal_tail` (T0); `properties_write` (T1 under the project dir, T2 elsewhere), `service_install`, `service_restart` (T2; `systemctl` on Linux, `mgr` on Windows), `java_check` | the same | the systemd unit and Windows service templates; `connect-standalone` versus `connect-distributed` args; plugin path in properties |
| Strimzi (U12) | `k8s_get`, `k8s_logs`, `k8s_events` (T0, one namespace); `k8s_apply` (T2, `--dry-run=server` as preview), `k8s_secret_apply` (T2), `strimzi_build_status` (T0) | `KafkaConnector` status conditions | the in-cluster runner Job (Helm), the `KafkaConnect` manifest with `build.plugins`, RBAC for one namespace |
| MSK Connect (U13) | `msk_describe_*` (T0); `msk_create_plugin` (from an S3 object the person uploaded; T2), `msk_create_worker_config`, `msk_create_connector`, `msk_delete_connector` (T2; T3 on production) | `describe-connector` state | the client over the AWS SDK with the shared AWS config (`wiring/aws.go`), VPC and IAM checks |
| Confluent Cloud (U13) | `ccloud_list_plugins`, `ccloud_connector_status` (T0); `ccloud_create_connector` (validate first), `ccloud_acl_create` (T2) | connector status | the client over the Connect v1 API |
| Aiven (U13) | `aiven_service_get`, `aiven_connector_status` (T0); `aiven_create_connector`, `aiven_integration_create` (T2) | connector status | the client over the Aiven API |
| Debezium Server (U11) | `properties_write` (T1), `compose_up` or `service_install` for the server process; `sink_probe` (T0: Kinesis, Pub/Sub, Redis, HTTP) | process up plus the sink's canary read | the properties template per sink; the sink probes |
| Embedded (U6) | `project_inspect` (T0: build file, dependency versions); `file_write` for a generated reference project under `<project dir>/piperoom-reference/` (T1) | the reference project's log shows the canary event | the templates (Maven and Gradle), the version mismatch rules |

## 6. Sources: prerequisite drivers

Each driver implements `plan.SourceDriver`: `Checks()` (ordered reads with the expected value and the fix op),
`FixFor(check)` (the op and params, with its tier floor), `CanaryTable()`, `ConnectorConfig(target, tables,
secrets)` (the Debezium config with `${secret:}` references). Postgres (A3, U1) uses Q2's `SourceSettingsReader`
and `SourceAdminWriter` through the runner's DB connection; MySQL (A8, U2) Q8's. SQL Server, MongoDB and Oracle
(A10) get thin read-only clients plus the admin methods each needs, behind the same two interfaces where they fit
and `ErrNotSupported` where they do not (`SetReplicaIdentity` on MySQL as Q8f does). Drivers (`go-mssqldb`, the
MongoDB Go driver, `go-ora`) are new modules, each listed in its PR with why (rule 10).

## 7. Security model

- **Credentials never cross the boundary.** The runner holds the Docker socket, `kubectl` context, cloud
  credentials and database passwords; the server holds the model key and the room database. Neither needs the
  other's. The pairing token authenticates the WebSocket; it is random, in memory only, and dies with the room.
- **The model never writes a command.** Only typed ops, validated on both sides, with the path and host rules of
  §2.3. The guard table refuses the destructive subset on production whatever is asked.
- **Two locks on every write.** The server routes by tier and approval (`params_hash`, the executor's claim); the
  runner refuses a T1+ op without a proposal id and a tier below the op's floor. A compromised server cannot make
  the runner run an op outside the catalogue; a compromised runner can only lie about results, which the receipts'
  output hashes and the verification ladder's independent reads (through the server's own clients when it has
  them) make visible.
- **Untrusted output.** Everything the runner returns is redacted on the runner, injection-scanned and wrapped on
  the server, and treated as data by the prompt, as every tool result today. Container logs are the largest new
  untrusted surface; they are capped (200 lines, 64 KiB) like Q3c's worker logs.
- **Audit.** Every op is in the room log with its receipt; the export's integrity hash covers it; `room.labelled`
  and the L1 exporter cover blocked and rolled-back setups.
- **Blast radius.** One runner, one room, one pipeline id, one project directory, the target list the person named.
  `net_probe` and `http_get` reach only listed hosts. Plugin downloads come only from `PIPEROOM_SETUP_PLUGIN_HOSTS`
  with a verified checksum.
- **Threat model update.** C4 adds a section to `docs/security/threat-model.md` (the runner as a new actor with a
  Docker socket; the pairing code; the secret frame) before U1 ships; its findings are U1 rows.

## 8. Test strategy

- **Unit, no Docker:** every op's `Validate` and `argv` builder (table tests, path escapes, symlinks, secrets in
  content), `plan.TierOf`, the ladder, the classifier, the protocol (`httptest` WebSocket with a fake runner and a
  fake server), the prompt snapshot in setup mode, the room routes with a fake runner. `go test ./...` stays
  Docker-free.
- **Integration (`-tags integration`), Linux:** the Compose driver against a real Docker on the CI runner: the
  agent (recorded model turns, as evals do) takes an empty project dir to a RUNNING Postgres connector and a canary
  event; the dev stack's `devctl setup-break` scenarios (flat plugin dir, missing converter jars, wrong `wal_level`,
  missing grant, DB unreachable, stale plugin after install) each end green. `it_` prefixes, cleanup per test.
- **CI matrix (`.github/workflows/setup-matrix.yml`):** `ubuntu-latest`, `macos-latest`, `windows-latest` run the
  unit tests and the runner's capability detection and service ops with fakes; `ubuntu-latest` also runs the Compose
  integration; `macos-latest` runs Compose through Colima when it is available on the runner image and is otherwise
  a documented skip; **Windows Linux-container runs need a self-hosted runner** (GitHub-hosted Windows runners run
  Windows containers only), the owner's dev machine nightly (decided, plan §7 item 7).
- **Strimzi:** `kind` on `ubuntu-latest` with the Strimzi operator, a one-broker KRaft Kafka and the in-cluster
  runner Job; the ladder to a canary event; 20-minute budget, nightly not per PR.
- **Managed clouds:** recorded fakes (`httptest` cassettes recorded once against the sandbox accounts, secrets
  scrubbed) for every API call in CI; **credentialed manual checks** under `//go:build manual` with the sandbox
  accounts, run by a lead before each U13 row merges and listed in the PR's "How I tested" with the run date.
- **Databases:** Postgres and MySQL on the dev stack; SQL Server (`mcr.microsoft.com/mssql/server`), MongoDB
  (replica set of one) and Oracle (`gvenzl/oracle-free`) as compose profiles for integration on Linux; RDS, Azure
  SQL and Atlas as manual checks.
- **Evals:** `evals/setup/**` replays runner recordings as it replays readers; each U row ships a scenario under
  `evals/scenarios/setup/` whose label names the failing check and the fixing op; the pass rate per target and per
  source is in the eval report; the CDC incident pass rate must not move.
- **Real runs** are in every target row's "Done when": a person runs `piperoom-setup` on the named OS against the
  named target from an empty directory and the room closes `pipeline live`; the run's export is attached to the PR.

## 9. Known gaps and follow-ups

- The runner trusts the server for tiers beyond the op floor; a server bug cannot lower a floor but could raise a
  `local` class, which only costs clicks. The reverse (the server lowering a class) is impossible by construction.
- The standalone mode keeps rooms in memory; a laptop that sleeps loses the setup room. Setup writes
  `<project dir>/.piperoom/setup-log.jsonl` with every event so the person keeps the record; a later `--resume` is
  not in this wave.
- Oracle's prerequisites vary by edition and by `CDB`/`PDB`; the U9 driver covers 19c and 21c free editions and
  says so when it meets another.
- Windows service ops are estimated, not measured; U1-0 re-estimates after a spike on the dev machine.
