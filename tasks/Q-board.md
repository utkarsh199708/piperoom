# Q wave dispatch board: one row = one PR

Plan `docs/product/so-gaps-2026-10.md`: a measured analysis of 722 Stack Overflow Debezium problems. Today plus P3
resolves 7.1% of them and diagnoses 7.5%. This wave takes resolved to 18.8% (up to 26.5% with the pull-request
action) and diagnosed to 26.5%. Each row is a PR an agent finishes in one or two sessions (target < 600 changed
lines, §6 of `AGENTS.md`).

- **ID** is the PR tag (`[Q2c] sourceactions: replica identity and heartbeat table`).
- **Model**: every row runs on Opus 5.5, cloud rows included (the owner's standing instruction: the strongest model
  for every row; leads' decision on #605). This overrides AGENTS.md §2's "Sonnet 5" for cloud agents in this wave.
- **Days**: one agent's working days including tests and review fixes.
- **After**: rows that must be merged first. Q0 always; P rows by their `tasks/P-board.md` ID.

**Order.** Steps 1 to 8 are priority order by coverage gain per day of work: version knowledge, database settings
and admin actions, Connect worker visibility, config validation before registration, connector lifecycle and source
offsets, network and TLS probes, the pull-request fix, MySQL. An agent with two rows ready takes the lower step first.
The After column is the only hard dependency, so the design rows Q7-0 and Q8-0 can start as soon as Q0 merges. The
plan's §4 has the gain per day of each step and why MySQL stays last although that table ranks it second.

**Before the wave.** Q0 merges after the P wave's open contract follow-ups. Every row that adds a write action waits
on P3-0 (the executor runs Validate and Execute with the room's pipeline scope). Every wiring row that builds a writer
waits on P1a-3 (with write mode off no write client is built).

**Source-neutral.** New interfaces, tools and actions are not named `pg_` unless they are Postgres-only
(`pg_set_replica_identity`). Step 8 implements them for MySQL; it does not copy them. On a Postgres-only Piperoom
steps 1 to 6 add 22 resolved problems; with MySQL the same steps add 60 (plan §4).

## Q0 items: the contract additions each row needs

The leads write items 1 to 5 in the Q0 PR. Items 6 and 7 are proposed shapes that the design rows Q7-0 and Q8-0
settle and land with their own contract change. Agents never edit the contract files (`AGENTS.md` §2). New methods go
on **new** interfaces, so every existing fake and the eval replay compile unchanged. Where this text and the merged
code differ, the code and its doc comments win.

### 1. Connect info (Q1c, Q3a, Q5a), `internal/clients/clients.go`

```go
// ConnectInfoReader is optional: a nil one makes the tools answer ErrNotSupported.
type ConnectInfoReader interface {
	ServerInfo(ctx context.Context) (ConnectServerInfo, error)    // GET /
	Plugins(ctx context.Context) ([]ConnectPlugin, error)         // GET /connector-plugins?connectorsOnly=false (Kafka 3.2+; older: connectors only)
	LoggerLevels(ctx context.Context) (map[string]string, error) // GET /admin/loggers
}
type ConnectServerInfo struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterID string `json:"kafka_cluster_id"`
}
type ConnectPlugin struct {
	Class   string `json:"class"`
	Type    string `json:"type"` // source | sink | converter | header_converter | transformation | predicate
	Version string `json:"version"`
}
```

### 2. Source settings and admin (Q2a to Q2e, Q8b, Q8f), `internal/clients/clients.go`

```go
// SourceSettingsReader is optional and read-only: Postgres in Q2a, MySQL in Q8b.
type SourceSettingsReader interface {
	Settings(ctx context.Context) ([]DBSetting, error) // a fixed list per source kind
	TableReplication(ctx context.Context, tables []string) ([]TableReplication, error)
	RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error)
	Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error)
}
type DBSetting struct {
	Name           string `json:"name"`
	Value          string `json:"value"`
	Unit           string `json:"unit,omitempty"`
	PendingRestart bool   `json:"pending_restart,omitempty"`
}
type TableReplication struct {
	Table           string `json:"table"`
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity,omitempty"` // default | full | index | nothing; "" when the source has none
}
type RoleInfo struct {
	Role        string          `json:"role"`
	Exists      bool            `json:"exists"`
	CanLogin    bool            `json:"can_login"`
	Replication bool            `json:"replication"` // Postgres REPLICATION; MySQL REPLICATION SLAVE and CLIENT
	Selectable  map[string]bool `json:"selectable"`  // table -> SELECT
}
type DBSession struct {
	PID        int64     `json:"pid"`
	StartedAt  time.Time `json:"started_at"` // backend start; with PID, the identity EndSession re-checks
	XactStart  time.Time `json:"xact_start"`
	User       string    `json:"user"`
	App        string    `json:"application"`
	State      string    `json:"state"`
	Query      string    `json:"query"` // untrusted, redacted, at most 200 bytes
	BlocksSlot bool      `json:"blocks_slot"`
}

// SourceAdminWriter is built only with write mode on, on the SOURCE_DB_WRITE_URL pool.
// Scope: the fail-closed rule of SourceWriter (P0 item 7) applies to every method.
type SourceAdminWriter interface {
	SetReplicaIdentity(ctx context.Context, table, mode, index string) error // mode full | default | index; ErrNotSupported on MySQL
	GrantSelect(ctx context.Context, role string, tables []string) error    // SELECT only
	CreateHeartbeatTable(ctx context.Context, table string) error
	EndSession(ctx context.Context, pid int64, startedAt time.Time) error   // ErrSessionChanged when pid now names another session
}
var ErrSessionChanged = errors.New("clients: database session changed since it was read")
```

### 3. Worker visibility (Q3a to Q3d), `internal/clients/clients.go`, `internal/config/config.go`

```go
// ConnectClusterReader is optional; the Kafka client implements it.
type ConnectClusterReader interface {
	ConnectWorkerGroups(ctx context.Context) ([]ConnectWorkerGroup, error) // groups with protocol type "connect"
	InternalTopics(ctx context.Context, names []string) ([]InternalTopic, error)
}
type ConnectWorkerGroup struct {
	GroupID string          `json:"group_id"`
	State   string          `json:"state"`
	Workers []ConnectWorker `json:"workers"`
}
type ConnectWorker struct {
	MemberID string `json:"member_id"`
	Host     string `json:"host"`
	URL      string `json:"url,omitempty"` // advertised REST URL from the member metadata; "" when it cannot be read
}
type InternalTopic struct {
	Name              string `json:"name"`
	Role              string `json:"role,omitempty"` // config | offset | status | ""
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	CleanupPolicy     string `json:"cleanup_policy"`
	MinInSyncReplicas int    `json:"min_insync_replicas"`
}

// WorkerLogReader is optional (Loki in this wave). Text is untrusted.
type WorkerLogReader interface {
	WorkerLogs(ctx context.Context, q LogQuery) ([]LogLine, error)
}
type LogQuery struct {
	Connector string
	Task      int    // -1 = any
	MinLevel  string // default WARN
	Since     time.Time
	Limit     int // at most 200
}
type LogLine struct {
	At     time.Time `json:"at"`
	Level  string    `json:"level"`
	Logger string    `json:"logger,omitempty"`
	Worker string    `json:"worker,omitempty"`
	Text   string    `json:"text"` // untrusted
}
```
`config.go`: `CONNECT_INTERNAL_TOPICS` (optional comma list of the config, offset and status topics),
`CONNECT_METRICS_PREFIX` (default `kafka_connect_`; read from the `DEBEZIUM_METRICS_URL` endpoint),
`WORKER_LOGS_LOKI_URL`, `WORKER_LOGS_SELECTOR` (a LogQL stream selector set by the operator, never by the model),
`WORKER_LOGS_TOKEN` (secret). `ParseWorkerLogs`: https unless loopback (the netsec rule); the selector parses as
`{...}`; a token without a URL is an error.

### 4. Config validation (Q4a, Q5c, Q6a), `internal/clients/clients.go`

```go
// ConfigValidator is implemented by the Connect client. It asks Connect and creates nothing.
type ConfigValidator interface {
	ValidateConfig(ctx context.Context, class string, cfg map[string]string) (ConfigValidation, error) // PUT /connector-plugins/{class}/config/validate
	PluginConfigDef(ctx context.Context, class string) ([]ConfigKeyDef, error)                        // GET /connector-plugins/{class}/config (Kafka 3.2+), else ErrNotSupported
}
type ConfigValidation struct {
	ErrorCount int                 `json:"error_count"`
	Errors     map[string][]string `json:"errors"` // key -> messages; values are never kept
}
type ConfigKeyDef struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Default  string `json:"default,omitempty"`
}
```
The F4b `connect.ValidateResult` becomes `= clients.ConfigValidation`.

### 5. Connector lifecycle and source offsets (Q5a to Q5c, Q8c, Q8e), `internal/clients/clients.go`

```go
// SourceOffsetReader is optional: GET /connectors/{name}/offsets (KIP-875).
type SourceOffsetReader interface {
	SourceOffsets(ctx context.Context, connector string) ([]SourceOffset, error)
}
type SourceOffset struct {
	Partition map[string]any `json:"partition"`
	Offset    map[string]any `json:"offset"` // positions only (lsn, file, pos, gtids); never record data
}

// ConnectLifecycleWriter is built only with write mode on.
type ConnectLifecycleWriter interface {
	CreateConnector(ctx context.Context, name string, cfg map[string]string) error   // POST /connectors; 409 -> ErrConnectorExists
	DeleteConnector(ctx context.Context, name string) error                          // DELETE /connectors/{name}
	StopConnector(ctx context.Context, name string) error                            // PUT /connectors/{name}/stop
	ResetSourceOffsets(ctx context.Context, name string) error                       // DELETE /connectors/{name}/offsets; connector STOPPED
	AlterSourceOffsets(ctx context.Context, name string, offsets []SourceOffset) error // PATCH /connectors/{name}/offsets; connector STOPPED
}
var ErrConnectorExists = errors.New("clients: connector already exists")
var ErrConnectorNotStopped = errors.New("clients: connector is not stopped")
```
`ConnectorStatus.State`'s doc gains `STOPPED`. KIP-875 shipped reading offsets and stop in Kafka 3.5 and alter and
reset in 3.6; Q5a confirms each against the Apache Kafka docs (rule 9). A Connect without an endpoint answers
`ErrNotSupported`. Steps 1 to 6 need no new event and no `docs/api.md` change: new actions show in `GET /api/actions`.

### 6. Pull-request fix (proposed; Q7-0 settles it)

- `owners.yaml`, per pipeline:
```yaml
repos:
  - id: connect-config
    url: https://github.com/acme/connect-config
    base: main
    paths:
      - glob: "connectors/orders-*.json"
        layer: kafka-connect
      - glob: "docker/connect/Dockerfile"
        layer: kafka-connect
```
- `ForgeReader` (`File(ctx, repo, ref, path)`, `PullRequest(ctx, repo, number)`) and `ForgeWriter`
  (`OpenPullRequest(ctx, repo, base, baseSHA, branch, title, body string, files []FileChange) (PullRequest, error)`);
  `FileChange{Path, Content}`, `PullRequest{Number, URL, State, Merged, HeadSHA, Checks}`; `ErrBaseMoved`.
- Event `pull_request.updated` `{proposal_id, step, url, number, state: open | merged | closed, checks}`;
  `HumanInput.Kind` `"pr_merged"`; snapshot `open_proposals[].pull_request`; `docs/api.md` rows.
- `config.go`: `FORGE_KIND` (`github`), `GITHUB_API_URL` (GitHub Enterprise allowed), `GITHUB_APP_ID`,
  `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY_FILE` (secret file).

### 7. MySQL source (proposed; Q8-0 settles it)

- `owners.yaml` `source_kind: postgres | mysql` (default `postgres`). The layer id of a MySQL source: a `mysql` layer
  with `LayersOf` taking the source kind, or a neutral `source` layer with a migration of owners files.
- A topology node kind for a MySQL source; `SOURCE_DB_URL` and `SOURCE_DB_WRITE_URL` accept a `mysql://` DSN.
- `BinlogReader`: `BinlogStatus(ctx) (BinlogStatus, error)` with `file`, `pos`, `gtid_executed`, `gtid_purged`,
  `binlogs [{name, size}]`, `expire_seconds`.
- The MySQL client implements `SourceSettingsReader` and `SourceAdminWriter` (item 2), `LongTransactions` and `Row`;
  `ReplicationSlots` answers `ErrNotSupported`; `RecentDDL` reads the schema history topic or answers
  `ErrNotSupported`.
- Identifier rule: backtick quoting after the same `^[a-zA-Z_][a-zA-Z0-9_]*$` check; `AGENTS.md` §4 rule 6 gains a
  MySQL line.

## Step 0: contracts

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q0 | leads | Opus 5.5 | 1.5 | contracts: Connect info, source settings and admin, worker visibility, config validation, connector lifecycle and source offsets | items 1 to 5 in code; every new interface optional or built only with write mode on; all stubs and fakes compile; `go build ./...`, `go vet ./...`, `go test ./...` green | P-wave contract follow-ups |

## Step 1: version knowledge

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q1a | C11 | Opus 5.5 | 1.5 | runbooks: known-issues index (`library/known-issues/*.yaml`, embedded) and `runbook_lookup` `versions` | an entry has id, component (`debezium-postgres`, `debezium-mysql`, `connect`, `kafka`, `pgjdbc`, ...), affected range, fixed in, symptom keywords, workaround, renamed keys (old to new), source URL; `issues.redhat.com` and `github.com/debezium` join the allowed hosts; a bad range or host fails start-up; with `versions` the lookup ranks entries whose range holds the running version first and says "fixed in X"; the tool still reads no customer system | Q0 |
| Q1b | C11 | Opus 5.5 | 1.5 | runbooks: seed the known-issues index | at least 40 entries over the five themes of the plan's §6, Debezium 2.x and 3.x first; each cites a release note or an issue; a table test resolves every renamed key old to new | Q1a |
| Q1c | A2 | Opus 5.5 | 1 | connect: `ConnectInfoReader`; tool `connect_plugins` | lists connector, converter, transform and predicate plugins with versions, and the Kafka version; a pre-3.2 Connect (connectors only) is marked; httptest: 404, timeout, oversized, bad JSON | Q0 |
| Q1d | A4 | Opus 5.5 | 0.5 | agent: read versions once per room and pass them to `runbook_lookup`; register `connect_plugins` | the prompt names the version and the fixed-in release when it recommends an upgrade; prompt snapshot; `make evals` pass rate unchanged | Q1a, Q1c |
| Q1e | C7 | Opus 5.5 | 1 | scenarios: `renamed-property-after-upgrade`, `removed-decoder-plugin` | the label names the known-issue id | Q1b |

## Step 2: database settings read and approved DB admin actions

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q2a | A3 | Opus 5.5 | 2 | postgres + dbtools: `SourceSettingsReader`; tools `source_settings`, `source_table_replication`, `source_role_grants`, `source_blocking_sessions` | settings `wal_level`, `max_replication_slots`, `max_wal_senders`, `max_slot_wal_keep_size`, `wal_sender_timeout`, `rds.logical_replication` when present, with `pending_restart`; replica identity and primary key per `source_tables` entry; the connector role (`database.user` from `connect_config`): login, REPLICATION, SELECT per table; blockers: sessions whose open transaction is older than a waiting slot creation or holds the oldest xmin, query redacted (N5) and cut to 200 bytes; scoped like the other dbtools; integration with `it_` tables | Q0 |
| Q2b | A3 | Opus 5.5 | 1.5 | postgres: `SourceAdminWriter` on the `SOURCE_DB_WRITE_URL` pool | identifiers through `Sanitize()` after the rule 6 check; `GrantSelect` grants SELECT and nothing else; `EndSession` re-reads `pid` and `backend_start` and refuses a mismatch (`ErrSessionChanged`), the pipeline's own walsender and Piperoom's own sessions; fail-closed scope like `SourceWriter`; fake-pool unit tests; integration with `it_` objects | Q0 |
| Q2c | A3 | Opus 5.5 | 1.5 | sourceactions: `pg_set_replica_identity`, `source_create_heartbeat_table` | Validate: table in `source_tables`, write credentials present; Preview: the brief `ACCESS EXCLUSIVE` lock, the extra WAL of `FULL` from the table size, and for the heartbeat table the follow-up steps in the same proposal (`pg_add_publication_table` when the publication is filtered, `heartbeat.action.query` through the N-key config action); rollback text; Preview never calls a writer (`readonly_test.go` style) | Q2b, P3-0 |
| Q2d | A3 | Opus 5.5 | 1.5 | sourceactions: `source_grant_select`, `source_end_session` | grant: the role is the connector's `database.user`, tables in `source_tables`, SELECT only; end session: the pid is in `source_blocking_sessions` at Validate and again at Execute; Preview shows age, application and the redacted query; both refuse a target outside the pipeline before any I/O | Q2b, P3-0 |
| Q2e | A4 | Opus 5.5 | 0.5 | wiring: build `SourceAdminWriter` with write mode on; register `sourceactions` and the Q2a tools; prompt "read database settings before proposing a config change" | write mode off: no admin writer is reachable (the P1a-3 test extended); prompt snapshot; evals unchanged | Q2a, Q2c, Q2d, P1a-3 |
| Q2f | A1 | Opus 5.5 | 1.5 | devctl: `break`/`fix` `no-replica-identity`, `missing-grant`, `blocked-slot`, `idle-wal-growth` | each break shows in a Q2a tool and trips an alert or the watcher; each fix restores; `dev/scenarios.md` rows | Q0 |
| Q2g | C7 | Opus 5.5 | 1 | scenarios: the four Q2f breaks | the expected action name in each label | Q2f |
| Q2h | C6 | Opus 5.5 | 1 | evals: replay `SourceSettingsReader`; `SourceAdminWriter` as recording no-ops | the four scenarios score; CDC pass rate unchanged | Q2g |
| Q2i | C14 | Opus 5.5 | 0.5 | docs: the write role's additions | each action with its SQL and the minimal grant: table ownership (or a role that owns the captured tables) for REPLICA IDENTITY, GRANT OPTION on SELECT, `pg_signal_backend`, CREATE on the heartbeat schema | Q2c, Q2d |

## Step 3: Connect worker visibility

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q3a | A2 | Opus 5.5 | 2 | kafka + kafkatools: `ConnectClusterReader`; tool `connect_worker_info` | finds worker groups by protocol type `connect` (several are listed); the worker URL comes from the member metadata only if `go doc` shows `kadm` exposes it, else host only; internal topics from `CONNECT_INTERNAL_TOPICS` or the `-configs`/`-offsets`/`-status` suffixes; flags a config topic with more than one partition, an internal topic that is not compacted, replication factor 1 on a multi-broker cluster; includes plugins and logger levels from Q1c; `go doc` for every `kadm` call | Q0, Q1c |
| Q3b | A3 | Opus 5.5 | 1 | debeziumtools: `connect_worker_metrics` from the JMX exporter behind `DEBEZIUM_METRICS_URL` | task error totals, records skipped, DLQ produce failures, task startup failures, rebalance state; a missing family says "the exporter does not export X"; fixture tests; `CONNECT_METRICS_PREFIX` | Q0 |
| Q3c | C18 | Opus 5.5 | 2.5 | workerlogs + logtools: `WorkerLogReader` over Loki `query_range`; tool `connect_worker_logs` | at most 200 lines and 64 KiB, newest last; text wrapped, redacted (N5) and injection-scanned (N6); the LogQL selector only from config, the model passes connector, task, level and since; 10 s, 4 MB, `netsec`; httptest: 401, 404, timeout, oversized, bad JSON | Q0 |
| Q3d | A1 | Opus 5.5 | 1.5 | dev: compose profile `logs` (Loki and Promtail on the Connect container); JMX exporter rules export `kafka_connect_*` | `docker compose --profile logs up` healthy in < 2 min; `connect_worker_logs` and `connect_worker_metrics` return data on the dev stack | Q0 |
| Q3e | A4 | Opus 5.5 | 0.5 | wiring + prompt: register `logtools`; "a task that is RUNNING but skips records: read worker metrics and logs" | prompt snapshot; evals unchanged | Q3a, Q3b, Q3c |
| Q3f | C7 | Opus 5.5 | 1 | scenarios: `tolerance-all-skips`, `internal-topic-not-compacted`, `rebalance-loop` | the label names the tool that shows the cause | Q3d |
| Q3g | C14 | Opus 5.5 | 0.5 | docs: Loki read token scope, JMX exporter rules, data-handling (worker log lines reach the model) | citations per claim | Q3c |

## Step 4: config validation before registration

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q4a | A2 | Opus 5.5 | 1.5 | connect + kafkatools: `ConfigValidator`; tool `connect_validate_config {class, config, base_connector?}` | secret keys come from `base_connector` on the server, never from the model (a `looksSecret` value in the input is refused); host keys must equal the pipeline's source host or the base connector's, so Connect is never asked to reach another host; an unknown key gets the nearest known key from `PluginConfigDef`; an error on a renamed key names the Q1 known-issue id; creates nothing; httptest: 400, 404, 405, 500; U0 note (#649): `${secret:...}` is accepted as a non-secret value, and the secret check calls `actions.LooksSecret` (U0) instead of a private copy | Q0, Q1a |
| Q4b | A4 | Opus 5.5 | 0.5 | agent: the "connector does not exist yet" path (validate before proposing a create); register the tool | prompt snapshot; evals unchanged | Q4a |
| Q4c | C7 | Opus 5.5 | 0.5 | scenarios: `register-rejected-renamed-key`, `register-rejected-missing-plugin` | the label names the key or the plugin | Q4a |

## Step 5: connector lifecycle and source offsets

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q5a | A2 | Opus 5.5 | 1.5 | connect: `ConnectLifecycleWriter`, `SourceOffsetReader`; tool `connect_source_offsets` | each KIP-875 endpoint checked against the Apache Kafka docs; a Connect without one answers `ErrNotSupported` and the tool names the Kafka version that adds it (from `ServerInfo`); offsets shown as positions only; httptest per endpoint incl. 409 and a running connector; integration with an `it_` connector | Q0, Q1c |
| Q5b | A2 | Opus 5.5 | 1.5 | actions: `connect_stop_connector`, `connect_reset_source_offsets` (to empty, or to a position) | refuses a connector outside the pipeline, a sink connector, a position the source cannot have; a running connector needs the stop step first in the same proposal; Preview: the stored offsets now (the rollback), what a restart does under `snapshot.mode`, the table size estimate when a snapshot follows; executes on the dev stack | Q5a, P3-0 |
| Q5c | A2 | Opus 5.5 | 1.5 | actions: `connect_create_connector`, `connect_delete_connector` | create: the name is in the pipeline's `connectors` and absent; Preview runs `ValidateConfig` and refuses on errors; secret values must be config-provider references (`${file:...}`, `${env:...}`, and `${secret:...}`, U0 note #649); delete: refuses a connector whose config holds a plaintext secret (its rollback could not restore it); Preview says stored offsets stay under the name; re-register is delete then create in one proposal | Q5a, Q4a, P3-0 |
| Q5d | A4 | Opus 5.5 | 0.5 | wiring + prompt: register the four actions; when to clear offsets and when to snapshot one table (P3a-5) | write mode off: none reachable; prompt snapshot; evals unchanged | Q5b, Q5c, P1a-3 |
| Q5e | A1 | Opus 5.5 | 1 | devctl: `break`/`fix` `stale-source-offsets` (a connector repointed at a fresh database keeps its old offsets) | the break shows in `connect_source_offsets`; the fix restores; `dev/scenarios.md` row | Q0 |
| Q5f | C7 | Opus 5.5 | 0.5 | scenario: `stale-source-offsets` | expected actions stop and reset in the label | Q5e |
| Q5g | C14 | Opus 5.5 | 0.5 | docs: the Q5a Connect calls with method, path and the Kafka version each needs | citations per claim | Q5c |

## Step 6: network and TLS probes

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q6a | C18 | Opus 5.5 | 2 | nettools: `net_probe` | DNS, TCP and TLS (chain, SAN, expiry) per target, plus the brokers' advertised listeners from metadata; the database as Connect sees it, through `ValidateConfig` with the running config; targets only from config, the pipeline's `connections:` and its connectors' `database.hostname`/`database.port`: the model names a target label, never an address; no credentials sent; 3 s per step; reuses `kafkatools.CertTargets` and `netsec`; tests with loopback listeners and test certificates; U0 note (#649): the probe takes a location interface (the runner's position) and a target source (the provisional pipeline's hosts), so U1-2 runs it from the runner | Q0, Q4a |
| Q6b | A4 | Opus 5.5 | 0.5 | wiring + prompt: register `nettools`; "compare Piperoom's view with Connect's" | prompt snapshot; evals unchanged | Q6a |
| Q6c | C7 | Opus 5.5 | 0.5 | scenarios: `advertised-listener-wrong`, `db-unreachable-from-connect` | the label names the failing probe step | Q6a |
| Q6d | C6 | Opus 5.5 | 1.5 | evals: replay for the Q1c, Q3a, Q3c, Q4a and Q5a readers; writers as recording no-ops | the step 1 and 3 to 6 scenarios score; CDC pass rate unchanged | Q1e, Q3f, Q4c, Q5f, Q6c |

## Step 7: propose the fix as a pull request

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q7-0 | leads | Opus 5.5 | 1.5 | design: `docs/design/fix-pr.md` and Q0 item 6 | the page answers three questions. **Which repos**: only those in the pipeline's `repos:`, each path glob mapped to one layer; GitHub and GitHub Enterprise in this wave. **How it is approved**: an ordinary proposal; per-layer approval of the glob's layer; `params_hash` over the whole diff; the write mode gate; never auto-approved; Piperoom opens the PR and never merges, so the repo's branch protection and reviewers still apply. **How it is verified**: the room follows the PR; after merge the agent waits until the change reaches the system (connector config, plugin version or a `what_changed` entry), then runs its verification; closed unmerged reads "not applied". Also the refusals (`.github/**`, `CODEOWNERS`, files outside the globs, binary, secrets) and the GitHub App scopes. Item 6 lands in code with it | Q0 |
| Q7a | A3 | Opus 5.5 | 1 | owners: `repos:` per pipeline | load refuses a glob without a layer, a layer outside `p.Layers()`, a glob under `.github/`, a non-https URL, two repos with one id; table tests | Q7-0 |
| Q7b | C17 | Opus 5.5 | 2.5 | forge: GitHub client (`ForgeReader`, `ForgeWriter`) with a GitHub App installation token | writes only to `piperoom/<room>-<proposal>` branches; never pushes the base branch, never merges; commits on the recorded base SHA (`ErrBaseMoved` otherwise); 10 s, 4 MB, `netsec`; the token is never logged; httptest: 401, 403, 404, 409, 422, timeout, oversized | Q7-0 |
| Q7c | C17 | Opus 5.5 | 2 | fixpr: action `repo_open_pull_request` | Validate: repo in the pipeline's `repos`, every path matches one glob, all files of a step share one layer, at most 10 files of 64 KiB, no binary, no `looksSecret` or redact hit; Preview: unified diff against the base head SHA, which it records; `params_hash` covers the diff; Execute refuses if the base moved; the result carries the PR URL | Q7a, Q7b, P3-0 |
| Q7d | C17 | Opus 5.5 | 1 | forgetools: `repo_read_file`, `pr_status` | scope: the pipeline's repos and globs only; file text wrapped as untrusted; at most 64 KiB | Q7a, Q7b |
| Q7e | A5 | Opus 5.5 | 2 | room: follow opened PRs (`pull_request.updated`, `HumanInput` `pr_merged`, snapshot `open_proposals[].pull_request`) | the room's lease holder polls every 60 s, one poller per room across replicas; polling stops on merge, close or room close; fake-clock tests | Q7-0, Q7c |
| Q7f | A4 | Opus 5.5 | 1.5 | agent + wiring: propose a PR when the fix lives in a listed repo; verify after `pr_merged`; build `ForgeWriter` with write mode on | one step never mixes two layers' files; prompt snapshot; evals unchanged | Q7c, Q7d, Q7e, P1a-3 |
| Q7g | A6 | Opus 5.5 | 1.5 | web: diff view in the proposal preview; PR link and state badge | Vitest; the file list is keyboard and screen-reader complete; mobile wraps long lines | Q7e |
| Q7h | C14 | Opus 5.5 | 1 | docs: GitHub App setup (contents and pull requests write, checks read, listed repos only); data-handling (repo files reach the model) | citations per claim | Q7b |
| Q7i | C7 | Opus 5.5 | 1 | scenarios: `plugin-missing-in-image` (Dockerfile), `debezium-upgrade` (version pin), `consumer-deserializer` (consumer config) | the label names repo and path | Q7c |
| Q7j | C6 | Opus 5.5 | 1 | evals: forge replay (writer as recording no-op); "right files changed" score | the three scenarios score; CDC pass rate unchanged | Q7i |

## Step 8: MySQL source

| ID | Agent | Model | Days | PR | Done when | After |
|---|---|---|---|---|---|---|
| Q8-0 | leads | Opus 5.5 | 1.5 | design: `docs/design/mysql-source.md` and Q0 item 7 | decides the layer id of a MySQL source, `source_kind`, the DSN, which interfaces MySQL implements, where `RecentDDL` comes from, the MySQL identifier rule (and its line in `AGENTS.md` §4), the driver (`github.com/go-sql-driver/mysql`); item 7 lands in code with it | Q0 |
| Q8a | A1 | Opus 5.5 | 2.5 | dev: compose profile `mysql` (MySQL 8.4 with GTID, Debezium MySQL connector `orders-mysql`); `devctl break`/`fix` `binlog-purged`, `history-topic-deleted`, `missing-replication-grant`, `server-id-clash`; the pipeline in `dev/owners.yaml` | profile healthy in < 3 min; each break trips the watcher or an alert; each fix restores | Q8-0 |
| Q8b | A8 | Opus 5.5 | 3 | mysql: read-only client (`BinlogReader`, `SourceSettingsReader`, long transactions, row) | read-only session; 10 s per call; identifiers by the Q8-0 rule; the new module listed in the PR; settings `binlog_format`, `binlog_row_image`, `gtid_mode`, `server_id`, `log_bin`, `binlog_expire_logs_seconds`; integration on the profile | Q8-0, Q8a |
| Q8c | A8 | Opus 5.5 | 1.5 | mysqltools: `mysql_binlog_status` | compares the connector's stored position (`SourceOffsetReader`) with the purged binlogs and GTIDs and says "the binlog the connector needs is purged"; a test shows the Q2a `source_*` tools work on MySQL through the interface | Q8b, Q5a |
| Q8d | A3 | Opus 5.5 | 1.5 | owners + topology + scope: `source_kind`, the MySQL source node, `db.table` names in scope | builder and scope table tests for both source kinds | Q8-0 |
| Q8e | A2 | Opus 5.5 | 1.5 | kafkatools + actions: `kafka_schema_history`; MySQL positions (file, pos, GTID set) in `connect_reset_source_offsets` | the tool flags a missing history topic, finite retention, more than one partition; the action refuses a position past the binlog end or in a purged file | Q8-0, Q5b |
| Q8f | A8 | Opus 5.5 | 1.5 | mysql: `SourceAdminWriter` on the `SOURCE_DB_WRITE_URL` pool | `GrantSelect`, `EndSession` (`KILL` after the same re-read), `CreateHeartbeatTable`; `SetReplicaIdentity` answers `ErrNotSupported`; the Q2b refusals; integration on the profile | Q8b, Q2b |
| Q8g | A4 | Opus 5.5 | 1.5 | wiring + agent: build MySQL clients for `source_kind: mysql` (writers only with write mode on); per-source prompt (binlog, GTID, schema history) | a MySQL room offers no `pg_*` tool and a Postgres room no `mysql_*`; prompt snapshots per source; Postgres evals unchanged | Q8b, Q8d, Q8f, P1a-3 |
| Q8h | C10 | Opus 5.5 | 1 | discover: MySQL connectors give `source_kind: mysql` | the output loads with `owners.Load`; dev-profile integration test | Q8a, Q8d |
| Q8i | C7 | Opus 5.5 | 1.5 | scenarios: the four Q8a breaks | labels name layer and action | Q8a |
| Q8j | C6 | Opus 5.5 | 1.5 | evals: MySQL replay readers; pass rate per source kind | the four scenarios score; Postgres pass rate unchanged | Q8c, Q8i |
| Q8k | A6 | Opus 5.5 | 1 | web: MySQL source node and layer label | Vitest; Postgres lane unchanged | Q8d |
| Q8l | C14 | Opus 5.5 | 1 | docs: MySQL read user (`SELECT`, `REPLICATION CLIENT`, `REPLICATION SLAVE`) and write user grants; setup rows | citations per claim | Q8b, Q8f |
| Q8m | C1 | Opus 5.5 | 0.5 | ci: unit tests for the new packages; README lists MySQL | CI green | Q8c |

## Leads' decisions on the plan (review of #605, 2026-10-18)

1. **Order.** The planned order stands: steps 2 to 6 are within the error of the day estimates, and MySQL (step 8)
   implements the source-neutral interfaces of steps 2 and 5, so it stays last. Step 4 (config validation, 2.4
   resolved per agent-day) waits only on Q0 and Q1a, so it is dispatched as soon as Q1a lands, in parallel with
   steps 2 and 3; the After column already allows it.
2. **Counting rule.** Accepted as written in the plan (§ coverage curve): a problem's first-pass source and runtime
   count as needs. Any later re-count uses the same rule or says why not.
3. **Offsets reset.** Counted only once step 5 lands (Q5b, Q5c), as the plan does. The 7 resolvable ones are Q5f's
   scenarios.
4. **New owners.** A8 (Qoder, local; A3 takes the A8 rows after step 2 if no eighth local agent runs), C17 and C18
   (cloud) are accepted with the folders below. `docs/process/ownership.json` carries them in the same PR as
   AGENTS.md (its own rule).
5. **Model column.** Every row runs on Opus 5.5, cloud rows included (see the Model note at the top).
6. **Unconfirmed facts.** The Connect offsets API versions (KIP-875: stop and read in Kafka 3.5, alter and reset in
   3.6) stay unconfirmed until Q5a's Done-when confirms them against the Kafka release notes and the dev stack's
   Connect; a refusal message names the version it needs.

## New folders and owners

| Agent | Owns (new in the Q wave) |
|---|---|
| A3 | `internal/actions/sourceactions/**` |
| A8 (Qoder, new) | `internal/clients/mysql/**`, `internal/tools/mysqltools/**` |
| C17 (cloud, new) | `internal/clients/forge/**`, `internal/tools/forgetools/**`, `internal/actions/fixpr/**` |
| C18 (cloud, new) | `internal/clients/workerlogs/**`, `internal/tools/logtools/**`, `internal/tools/nettools/**` |

A2 keeps the rest of `internal/actions/**` except `action.go`, `pgactions/**`, `sourceactions/**` and `fixpr/**`. If
no eighth local agent runs, A3 takes the A8 rows after step 2; the After column does not change. Taking them is a leads' PR that sets those rows' Agent cells to A3 and adds `internal/clients/mysql/**` and `internal/tools/mysqltools/**` to A3 in AGENTS.md and `docs/process/ownership.json` together, because ship-check reads the Agent cell and the ownership file.

## Load per agent

| Agent | PRs | Days | Opus days | Critical for |
|---|---|---|---|---|
| A2 | 7 | 10.5 | 10.5 | the Connect side of steps 1, 3, 4, 5 and 8; the longest chain |
| A3 | 7 | 10 | 10 | step 2; `repos:` (step 7); MySQL owners and topology |
| A4 | 8 | 6 | 6 | every step's agent and wiring line |
| A8 | 3 | 6 | 6 | step 8 |
| A1 | 4 | 6.5 | 6.5 | dev breaks of steps 2, 3, 5 and 8 |
| C7 | 8 | 7 | 7 | scenarios |
| C17 | 3 | 5.5 | 5.5 | step 7 |
| C6 | 4 | 5 | 5 | evals |
| C18 | 2 | 4.5 | 4.5 | steps 3 and 6 |
| leads | 3 | 4.5 | 4.5 | contracts and both designs |
| C14 | 5 | 3.5 | 3.5 | docs each step |
| C11 | 2 | 3 | 3 | step 1 |
| A6 | 2 | 2.5 | 2.5 | steps 7 and 8 |
| A5 | 1 | 2 | 2 | step 7 |
| C10 | 1 | 1 | 1 | step 8 |
| C1 | 1 | 0.5 | 0.5 | step 8 |

Total about 78 agent-days over 61 PRs. The critical path is A2: Q0, Q1c, Q3a, Q4a, Q5a, Q5b, Q5c, Q8e, about 12
working days. The MySQL chain (Q0, Q8-0, Q8a, Q8b, Q8f, Q8g) is about as long. If A2 slips, Q8e moves to the end;
only the MySQL offsets shape waits on it.

## Suggested weekly dispatch (all agents in parallel)

| Week | Lands |
|---|---|
| 1 | Q0; Q7-0, Q8-0; Q1a, Q1b, Q1c, Q1d, Q2a, Q2b, Q2f, Q3c |
| 2 | Q1e, Q2c, Q2d, Q2g, Q2i, Q3a, Q3b, Q3g, Q4a, Q4b, Q4c, Q5a, Q6a, Q7b, Q8a |
| 3 | Q2e, Q2h, Q3d, Q3e, Q5b, Q5c, Q5e, Q6b, Q6c, Q7a, Q7h, Q8b, Q8d, Q8i |
| 4 | Q3f, Q5d, Q5f, Q5g, Q7c, Q7d, Q7e, Q7i, Q8c, Q8e, Q8f, Q8h, Q8k |
| 5 | Q6d, Q7f, Q7g, Q7j, Q8g, Q8j, Q8l, Q8m; integration run of every step's "Done when" by Lead 1 |

A1 takes Q8a before Q3d and Q5e: it is the only row that holds up a whole agent (A8). Review load for the leads is
about 15 PRs a week at the peak (weeks 2 and 3); the code review of C* PRs goes to `[LEAD-CLOUD]` as usual.
