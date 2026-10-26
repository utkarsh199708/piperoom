# MySQL source (Q8-0, Jira KAN-302; main @ `47ad7b2f`, 2026-10-23)

Owner: LEAD-ARCH. Plan: `docs/product/so-gaps-2026-10.md` (§3.3: 228 of the 722 Stack Overflow problems, 31.6%, have
a MySQL source, more than Postgres); board: `tasks/Q-board.md` "Q0 items" §7 and Step 8 (Q8-0 to Q8m, plus the rows
§13 adds). The Q-wave rule this design applies everywhere: **new interfaces, tools and actions are source-neutral, so
MySQL implements them instead of copying them.**

This row is a design plus one contract change, "Q0 item 7". The design is this page. The contract change is the
exact Go code in §5. It lands in **one separate leads PR, "[Q8-0] contracts: item 7"**, after this page merges
and before any Q8 code row starts. The board marks Q8-0 done only when that PR merges, as U13-0 was. §12 lists the
decisions the owner makes.

**What was verified and how.** The repository facts below were read from `origin/main` at `47ad7b2f`. The driver
API was checked with `go doc` against `github.com/go-sql-driver/mysql v1.10.1` (released 2026-09-02) in a scratch
module outside the repo: `Config` (with `TLS *tls.Config`, `Params`, `ConnectionAttributes`, `AllowAllFiles`,
`MultiStatements`, `InterpolateParams`, `ParseTime`, `Timeout`, `ReadTimeout`, `WriteTimeout`), `NewConfig`,
`NewConnector` and `MySQLError{Number, SQLState, Message}`. The README of that version says every unknown DSN
parameter is sent as `SET <name>=<value>`. The MySQL and Debezium facts (variable names, privilege names,
`performance_schema` tables, offset keys, config keys) come from the leads' knowledge of MySQL 8.0/8.4 and Debezium
2.x/3.x. The vendor docs were not fetched in this session, so **each one carries a "confirm" note naming the row
that checks it against the vendor docs or the dev profile before code relies on it** (AGENTS.md §4 rule 9 applies to
SQL and config keys too).

## 1. Decisions in one table

| Question (board Q8-0 "Done when") | Decision |
|---|---|
| Layer id of a MySQL source | **`mysql`**, a new layer, not a renamed `postgres` and not a neutral `source` (§4.1). `events.LayersOfSource(kind, sourceKind)` puts it first in a MySQL pipeline's layers, where `postgres` is in a Postgres one. A source action whose layer depends on the pipeline says so with the marker `actions.SourceLayered`, and the room asks `actions.LayerFor(a, sourceKind)` |
| `source_kind` | an optional per-pipeline `owners.yaml` key: `postgres` (the default, so every existing file still loads) or `mysql`. It is valid on `kind: cdc` only. Load refuses the Postgres-only fields (`watch.slot`, `watch.wal_headroom_bytes`, a `slot_*` signal) on a MySQL pipeline (§8.1) |
| DSN | the same `SOURCE_DB_URL` / `SOURCE_DB_WRITE_URL` and the same `connections:` keys (`source_db_url`, `source_db_write_url`). A MySQL source is a `mysql://user:pass@host:3306/[db]?tls=...` URL. Wiring picks the driver by URL scheme and refuses a scheme that does not match the pipeline's `source_kind` (§6.1). There is **no** go-sql-driver DSN syntax and **no** parameter pass-through |
| Interfaces MySQL implements | `clients.SourceDB` (`LongTransactions`, `Row`; `ReplicationSlots` and `RecentDDL` answer `ErrNotSupported`); `SourceSettingsReader` and `ScopedBlockerReader` (item 2); `SourceAdminWriter` (item 2: `GrantSelect`, `CreateHeartbeatTable`, `EndSession`; `SetReplicaIdentity` answers `ErrNotSupported`); the new `BinlogReader` (item 7). It does not implement `SourceWriter`, `SnapshotConditionWriter`, `Reconciler`, `CountReader`, `ToastColumnReader` or `MigrationReader` in this wave (§3) |
| Where `RecentDDL` comes from | **not from the database.** MySQL has no DDL event trigger, so the MySQL client's `RecentDDL` answers `ErrNotSupported`. The DDL history of a MySQL pipeline is the connector's **schema history topic**, which the new source-neutral tool `kafka_schema_history` (Q8e, A2) reads (§7.3) |
| Identifier rule | the same `^[a-zA-Z_][a-zA-Z0-9_]*$` check (MySQL caps a name at 64 characters), then **backtick quoting** `` `name` ``. A name that passes the check cannot hold a backtick. Values are always `?` parameters with server-side prepared statements (`InterpolateParams` false). The new line for AGENTS.md §4 rule 6 is in §5.6 |
| Driver | **`github.com/go-sql-driver/mysql`** (v1.10.1 now; Q8b pins the newest release that is at least two weeks old), through `database/sql` with `mysql.NewConnector(cfg)` and `sql.OpenDB`. It brings one indirect module, `filippo.io/edwards25519`. Adding it is a `go.mod` change that no agent owns, so Q8b, like V5, is built outside the ship workflow and merged after the §7 two-lead review (§6.1) |
| Versions supported | MySQL 8.0 and 8.4 LTS (9.x innovation releases best-effort); Amazon RDS and Aurora MySQL 8.0-compatible. **MariaDB is out of scope**: Debezium has a separate MariaDB connector, and MariaDB's GTIDs have another format |
| Prerequisite | items 2 and 5 of Q0 still live as local adapters (`internal/clients/postgres/settings.go`, `source_admin.go`; `internal/clients/connect/lifecycle.go`). The item 7 PR moves **item 2** into `clients.go` (the postgres package keeps aliases, as X4 did with `MigrationReader`), so that MySQL implements a neutral contract and does not import the postgres package (§5.2) |

## 2. Scope

**In.** The Debezium MySQL connector (`io.debezium.connector.mysql.MySqlConnector`), which reads the binlog and
writes to Kafka like the Postgres connector. Everything downstream of the connector (Kafka, Schema Registry,
consumers, warehouse, the DLQ, recovery) is already source-neutral, so it works unchanged. Step 8 adds the source
end:

- reads: settings, tables, grants, blocking sessions, binlog status, the schema history topic, rows by primary key,
  long transactions;
- approved writes: grant SELECT, create the heartbeat table, end a session; MySQL positions (file and position, GTID
  set) in `connect_reset_source_offsets`; two new connector config keys (`database.server.id` and `snapshot.mode`,
  §8.3);
- the map, scope, owners, watcher, discover, web, dev stack, scenarios and evals for `source_kind: mysql`.

**Out (this wave).** MariaDB. Percona XtraDB Cluster and Group Replication specifics (they work as plain MySQL;
nothing checks group membership). Reading the binlog stream ourselves (Debezium does that; Piperoom never opens a
replication connection, so it never needs `REPLICATION SLAVE`). Reconcile, TOAST, placeholder and migration-history
readers on MySQL (their tools say `ErrNotSupported`; follow-up rows only if the evals ask for them). A
`what_changed` source for the schema history topic (D6). Setup of a MySQL source: that is U2a, which builds on the
Q8 client (Jira KAN-5). `SET PERSIST` of server settings: no `AlterSystem` on MySQL in this wave (U2a says
the same).

## 3. Source-neutral and MySQL-specific

| Piece | Source-neutral (one implementation, both kinds) | Postgres-only (not offered in a MySQL room) | MySQL-only (not offered in a Postgres room) |
|---|---|---|---|
| Contracts | `SourceDB`, `SourceSettingsReader`, `ScopedBlockerReader`, `SourceAdminWriter`, `SourceOffsetReader`, `ConnectLifecycleWriter` | `SourceWriter`, `SnapshotConditionWriter`, `ToastColumnReader`, `ReplicationSlot` | `BinlogReader`, `BinlogStatus` |
| Read tools | `source_settings`, `source_table_replication`, `source_role_grants`, `source_blocking_sessions` (Q2a), `connect_source_offsets`, `kafka_schema_history` (new, any connector with a schema history), `source_row`, `source_long_transactions` (new names, §7.4) | `pg_replication_slots`, `pg_recent_ddl`, `pg_long_transactions`, `pg_row`, `pg_publication_tables`, `pg_toast_risk`, `pg_connector_scope_gap`, `pg_streaming_liveness` | `mysql_binlog_status` |
| Actions | `source_grant_select`, `source_create_heartbeat_table`, `source_end_session`, `connect_stop_connector`, `connect_reset_source_offsets`, `connect_update_connector_config` | `pg_set_replica_identity`, every `pgactions` action (`pg_add_publication_table`, `pg_drop_replication_slot`, the snapshot signals) | none: MySQL gets no `mysql_*` action |
| Owners, map | `source_tables` (`db.table` on MySQL), `topic_prefix`, `connectors`, the CDC layers after the source | `watch.slot`, slot signals, the `pg:` node | the `mysql:` node, layer `mysql` |

"Not offered" means wiring does not register the tool or action for that pipeline (Q8g). The agent never sees a
`pg_*` name in a MySQL room or a `mysql_*` name in a Postgres room.

## 4. Owners, layer and source kind

### 4.1 Why a new `mysql` layer

Three options were on the board.

- **Keep `postgres` as the id for every source database.** Nothing to migrate, but the room, the map, the approvals
  and `owners.yaml` would call a MySQL DBA the "postgres" owner. A deployment with both kinds could not give the two
  database teams different owners in the top-level `layers:` map. Rejected.
- **A neutral `source` layer.** It is the cleanest name, but `postgres` is persisted in every `room_events` row
  (`layer`, `suspected_layer`, `actual_layer`), in every customer `owners.yaml`, in eval labels and in the web's
  colours. A rename needs a data migration and an owners-file migration for no new capability. Rejected.
- **A new `mysql` layer (chosen).** It is additive. Old events, old owners files and old labels stay valid. A
  MySQL pipeline's layers are `mysql, kafka-connect, schema-registry, kafka, consumer, warehouse, environment`, so
  code that reads index 0 as "the source" still works.

The cost is that the layer of a source action now depends on the pipeline. `Action.Layer()` is static, and the room
refuses a step whose layer differs from it (`internal/room/service_proposals.go`). The contract therefore gains an
optional marker, `actions.SourceLayered`, and a helper, `actions.LayerFor(a, sourceKind)` (§5.3). The four
source-neutral source actions carry the marker (Q8n). The room and the agent's action list call `LayerFor` with the
room pipeline's source kind (Q8o, Q8g). `Layer()` still answers `postgres` for them, so a caller that knows no
pipeline (`GET /api/actions`, the loadtest) shows today's value.

### 4.2 `source_kind` in `owners.yaml` (Q8d, A3)

```yaml
pipelines:
  - id: orders-mysql
    kind: cdc                 # optional, as today
    source_kind: mysql        # optional; postgres when absent
    topic_prefix: shop
    source_tables: [shop.orders, shop.order_items]   # db.table; the topic is shop.shop.orders
    connectors: [orders-mysql]
    owners:
      mysql: { users: [dana] }
```

- `Pipeline.SourceKind string` (`yaml:"source_kind"`), read with `SourceKindOrDefault()` (`postgres` when empty).
  Load refuses any other value, and refuses the key on a `kind: batch` pipeline.
- `Pipeline.Layers()` returns `events.LayersOfSource(p.KindOrDefault(), p.SourceKindOrDefault())`. A per-pipeline
  `owners:` block of a MySQL pipeline therefore refuses `postgres:`, and the reverse, as it already refuses a layer
  outside `Layers()`.
- `Pipeline.SourceLayer()` = `events.SourceLayer(p.SourceKindOrDefault())`.
- A MySQL pipeline refuses `watch.slot`, `watch.wal_headroom_bytes` and the `slot_inactive` / `slot_wal` signals.
  `ConnectorSlot` already answers "" for a non-Postgres connector.
- Provisional pipelines (setup): `targetConnKey("mysql")` = `source_db_url`. `owners.Provisional` sets `SourceKind`
  from `SetupRequest.SourceKind` when that is `mysql`.
- Overlap (`overlap.go`): `schemeDefaultPort("mysql")` = `3306`, so two pipelines on one MySQL server are detected
  as host overlap.
- Connector table filter (`connector_scope.go`): the MySQL connector uses the same `table.include.list` /
  `table.exclude.list` with `db.table` entries, plus `database.include.list` / `database.exclude.list`. A database
  filter that excludes a source table's database is reported like an excluded table. Confirm the key names against
  the Debezium MySQL connector docs in Q8d.

`docs/process/ownership.json` does not change: `owners/**`, `topology/**` and `tools/scope.go` are A3's already.

## 5. Contract change: Q0 item 7 (exact code; lands in the "[Q8-0] contracts: item 7" PR)

The board's rule holds. New methods go on new interfaces, and every existing fake, the eval replay and the postgres
client compile unchanged. Every addition below is either new or additive to a type the item 2 move creates.

### 5.1 `internal/events/events.go`

```go
// Source kinds of a CDC pipeline: owners.yaml source_kind (Q8). The same
// strings as SetupRequest.SourceKind.
const (
	SourceKindPostgres = "postgres"
	SourceKindMySQL    = "mysql"
)

// LayerMySQL is the source layer of a pipeline whose source_kind is mysql: it
// takes the place of "postgres" in LayersOfSource. (Q8)
const LayerMySQL = "mysql"

// Layers ... (doc unchanged; "mysql" is appended after LayerEnvironment so no
// existing position moves)
var Layers = []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", "orchestrator", "transform", LayerEnvironment, LayerMySQL}

// SourceLayer returns the layer of a source database of the given source kind
// ("" = postgres), or "" for an unknown kind, which fails closed: no owner
// owns "". (Q8)
func SourceLayer(sourceKind string) string {
	switch sourceKind {
	case "", SourceKindPostgres:
		return "postgres"
	case SourceKindMySQL:
		return LayerMySQL
	}
	return ""
}

// LayersOfSource returns the layers of a pipeline of kind and sourceKind,
// upstream first: LayersOf(kind) with its source layer (index 0 of a cdc
// pipeline) replaced by SourceLayer(sourceKind). A batch pipeline ignores
// sourceKind. An unknown sourceKind on a cdc pipeline answers nil. New slice
// per call. (Q8)
func LayersOfSource(kind, sourceKind string) []string {
	l := LayersOf(kind)
	if kind != "" && kind != PipelineKindCDC {
		return l
	}
	sl := SourceLayer(sourceKind)
	if sl == "" || len(l) == 0 {
		return nil
	}
	l[0] = sl
	return l
}
```

`IncidentLayers()` is derived from `Layers`, so `suspected_layer` / `actual_layer` accept `mysql` with no change.
If a test outside the leads' files pins nine layers, the item 7 PR names it and its owner fixes it in the same
merge window.

### 5.2 `internal/clients/clients.go`

**(a) Item 2 moves in.** `SourceSettingsReader`, `DBSetting`, `TableReplication`, `RoleInfo`, `DBSession`,
`SourceAdminWriter` and `ErrSessionChanged` move into `clients.go` exactly as the board's item 2 and the merged
Q2a/Q2b adapters have them. That includes `DBSession.InScope` and the scoped read, which Q2a added and item 7
adopts:

```go
// ScopedBlockerReader is the pipeline-scoped Blockers: a session that holds or
// waits for no lock on one of tables comes back anonymised (InScope false).
// Postgres (Q2a) and MySQL (Q8b) implement it next to SourceSettingsReader.
type ScopedBlockerReader interface {
	BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]DBSession, error)
}

// ErrSessionProtected is returned (wrapped) by SourceAdminWriter.EndSession
// for a session Piperoom never ends: its own pools' sessions, the CDC
// stream's session (a Postgres walsender, a MySQL binlog dump thread), the
// server's background threads. Nothing was ended.
var ErrSessionProtected = errors.New("clients: refusing to end a protected session")
```

`internal/clients/postgres` keeps `type SourceSettingsReader = clients.SourceSettingsReader` and the other
aliases, and `postgres.ErrSessionChanged = clients.ErrSessionChanged`, so `sourceactions`, `dbtools`, `wiring` and
their tests compile unchanged. The same message prefix was kept on purpose (Q2b). `postgres.ErrSessionProtected`
becomes `= clients.ErrSessionProtected`; its message changes from `postgres: refusing...` to
`clients: refusing...`, and nothing compares that text.

**(b) Additive fields** on the moved types. The JSON is additive (`omitempty`), so a Postgres result does not change:

```go
type TableReplication struct {
	Table           string `json:"table"`
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity,omitempty"` // default | full | index | nothing; "" when the source has none (MySQL)
	// Engine is the MySQL storage engine (InnoDB, MyISAM, ...); "" on
	// Postgres. A non-transactional engine changes what a snapshot can
	// promise. (Q8)
	Engine string `json:"engine,omitempty"`
}

type RoleInfo struct {
	Role        string          `json:"role"`
	Exists      bool            `json:"exists"`
	CanLogin    bool            `json:"can_login"`
	Replication bool            `json:"replication"` // Postgres REPLICATION; MySQL REPLICATION SLAVE and REPLICATION CLIENT
	Selectable  map[string]bool `json:"selectable"`  // table -> SELECT
	// Accounts are the MySQL accounts ("user@host") with this user name;
	// nil on Postgres. With more than one, every flag above and in
	// Privileges is true only if it is true for every account. (Q8)
	Accounts []string `json:"accounts,omitempty"`
	// Privileges are the further global privileges the source's CDC
	// connector needs, by the source's own name (MySQL: RELOAD, LOCK TABLES,
	// SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT); nil when the
	// source has none to report. (Q8)
	Privileges map[string]bool `json:"privileges,omitempty"`
}
```

`DBSession` keeps its fields. The doc comments gain the MySQL meaning: `PID` is the processlist id (what `KILL`
takes); `StartedAt` is the open transaction's start (`innodb_trx.trx_started`), or zero for a session without one;
`BlocksSlot` on MySQL means "holds a lock a session of the connector's user waits for".

**(c) `SourceReader` doc** (no signature change):

```go
// SourceReader is the source database client as read-only tools take it:
// Postgres, or MySQL when the pipeline's source_kind is mysql (Q8).
type SourceReader interface {
	// ReplicationSlots lists the Postgres replication slots. MySQL has none:
	// ErrNotSupported.
	ReplicationSlots(ctx context.Context) ([]ReplicationSlot, error)
	// RecentDDL reads the DDL audit table written by an event trigger
	// (see dev/postgres-init). Newest first. MySQL has no DDL trigger:
	// ErrNotSupported; a MySQL pipeline's DDL is in the connector's schema
	// history topic (tool kafka_schema_history).
	RecentDDL(ctx context.Context, since time.Time, limit int) ([]DDLChange, error)
	// LongTransactions ... (unchanged; on MySQL PID is the processlist id and
	// HasXID means the transaction has modified rows)
	LongTransactions(ctx context.Context, olderThan time.Duration) ([]Transaction, error)
	// Row reads one row by primary key. table is "schema.table" on Postgres,
	// "db.table" on MySQL.
	Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error)
}
```

**(d) New: the binlog reader.**

```go
// ---- Q8: MySQL binlog (optional on a SourceDB, asserted at use time) ----

// MaxBinlogFiles caps BinlogStatus.Binlogs; the oldest files are kept,
// because the oldest file is the fact a purge check needs. (Q8)
const MaxBinlogFiles = 500

// BinlogReader is implemented by the MySQL source client (Q8b). It is never
// embedded in SourceDB: a Postgres client, the fakes and the eval replay have
// none, and a tool that asserts it on them answers ErrNotSupported. Read-only;
// 10 s per call. (Q8)
type BinlogReader interface {
	BinlogStatus(ctx context.Context) (BinlogStatus, error)
}

// BinlogStatus is the binlog state of a MySQL server: positions and names
// only, never event contents. (Q8)
type BinlogStatus struct {
	// File and Pos are the current write position (SHOW BINARY LOG STATUS,
	// SHOW MASTER STATUS before 8.2).
	File string `json:"file"`
	Pos  int64  `json:"pos"`
	// GTIDMode is @@gtid_mode (OFF, OFF_PERMISSIVE, ON_PERMISSIVE, ON).
	GTIDMode string `json:"gtid_mode"`
	// GTIDExecuted and GTIDPurged are @@global.gtid_executed and
	// @@global.gtid_purged, as the server prints them; "" with GTIDs off.
	GTIDExecuted string `json:"gtid_executed"`
	GTIDPurged   string `json:"gtid_purged"`
	// ServerID and ServerUUID identify the server, so a stored position from
	// another server (after a failover) can be told apart.
	ServerID   int64  `json:"server_id"`
	ServerUUID string `json:"server_uuid"`
	// Binlogs are the files the server still has (SHOW BINARY LOGS), oldest
	// first, at most MaxBinlogFiles; BinlogsOmitted counts the newest ones
	// left out.
	Binlogs        []BinlogFile `json:"binlogs"`
	BinlogsOmitted int          `json:"binlogs_omitted,omitempty"`
	// ExpireSeconds is binlog_expire_logs_seconds; 0 = never purged by age.
	// RetentionHours is RDS/Aurora's "binlog retention hours"
	// (mysql.rds_show_configuration); nil when not on RDS or not readable.
	ExpireSeconds  int64  `json:"expire_seconds"`
	RetentionHours *int64 `json:"retention_hours,omitempty"`
}

// BinlogFile is one binlog file the server still has. (Q8)
type BinlogFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}
```

Nothing else is new in `clients.go`. `SourceOffsetReader` (item 5) stays in `internal/clients/connect` until Q0
moves it. `mysql_binlog_status` and the reset action import it from there.

### 5.3 `internal/actions/action.go`

```go
// SourceLayered marks an action on the pipeline's source database whose
// approving layer is the pipeline's source layer (events.SourceLayer of its
// source_kind: postgres or mysql), not Layer(). Layer() of such an action
// answers "postgres", the value a caller without a pipeline shows. The room
// and the agent take the layer from LayerFor. (Q8)
type SourceLayered interface {
	Action
	// SourceLayered has no behaviour; it only marks the action.
	SourceLayered()
}

// LayerFor returns the layer whose owner approves a step of a on a pipeline
// whose source_kind is sourceKind ("" = postgres): SourceLayer(sourceKind)
// for a SourceLayered action ("" for an unknown kind, which no owner owns),
// a.Layer() otherwise. (Q8)
func LayerFor(a Action, sourceKind string) string {
	if _, ok := a.(SourceLayered); ok {
		return events.SourceLayer(sourceKind)
	}
	return a.Layer()
}
```

`action.go` already imports `events`. It takes a string, not an `owners.Pipeline`, so the contract does not wait
on Q8d's `SourceKindOrDefault`.

### 5.4 `internal/topology/graph.go` (doc only)

`Node`'s comment lists the id prefixes. It gains `mysql` ("`mysql:shop.orders`", layer `mysql`, kind
`source_table`). **No new `NodeKind`**: a MySQL table is a `source_table` like a Postgres one, the layer tells them
apart, and `IsDataset` and every lineage rule keep working.

### 5.5 `internal/config/config.go` (doc only) and `docs/api.md`

- `SourceDBURL`: "SOURCE_DB_URL (the CDC source database: `postgres://` or `mysql://`; a pipeline's `source_kind`
  must match the scheme)". The same for `SourceDBWriteURL`. The defaults do not change.
- `docs/api.md`: the layer list gains `mysql` (topology `layer`, proposal `layer`, `suspected_layer`, the people
  `layers`); the "nine layers" sentence becomes ten; a topology example node `mysql:shop.orders`.

### 5.6 `AGENTS.md` §4 rule 6 (the new line; lands with the item 7 PR)

> MySQL: values as `?` parameters (server-side prepared, `InterpolateParams` off); identifiers only after the same
> `^[a-zA-Z_][a-zA-Z0-9_]*$` check (at most 64 characters), quoted with backticks (`` `db`.`table` ``); an account
> host only after `^[A-Za-z0-9_.:%-]{1,255}$`, quoted as `'host'`; a number (a processlist id) only from an `int64`
> formatted with `strconv`. Never a DSN parameter from configuration passed to the driver.

## 6. The MySQL client: `internal/clients/mysql` (A8; Q8b read, Q8f write)

### 6.1 Driver, DSN and connections

- `New(ctx, rawURL string, opts ...Option) (*DB, error)` for the read pool and `NewWriter(ctx, rawURL, ...)` for
  the write pool. Each parses `mysql://user:pass@host[:3306]/[db]?tls=...` itself into a `mysql.NewConfig()` and
  calls `sql.OpenDB(mysql.NewConnector(cfg))`. `ParseDSN` and `FormatDSN` are never used, so no URL text reaches
  the driver's own DSN parser.
- **Accepted URL parameters are an allow-list: `tls` (`true`, the default off loopback; `false` only on loopback,
  the netsec rule), `tls_ca` (a CA file), `tls_server_name`.** Any other parameter is refused at start-up. That
  blocks `allowAllFiles` (`LOAD DATA LOCAL INFILE` reads the Piperoom host's files), `multiStatements`,
  `allowCleartextPasswords`, `allowOldPasswords`, and the driver's "unknown parameter = `SET` statement" path.
- The client sets these itself, never from the URL: `AllowAllFiles=false`, `MultiStatements=false`,
  `InterpolateParams=false`, `AllowCleartextPasswords=false`, `AllowOldPasswords=false`, `ParseTime=true`,
  `Loc=time.UTC`, `Timeout=5s`, `ReadTimeout`/`WriteTimeout=10s`, `TLS` as a `*tls.Config` (no global
  `RegisterTLSConfig`), and `ConnectionAttributes="program_name:piperoom-reader"` (`piperoom-writer` on the write
  pool). These are the MySQL twin of the Postgres `application_name` that `EndSession` protects.
- **Read-only session:** `Params = {"transaction_read_only": "1", "max_execution_time": "10000"}` on the read pool.
  The driver turns them into `SET` statements on each new connection. The read role has no write grant either
  (§10.2), so it is read-only by grant and by session. `max_execution_time` bounds each `SELECT` on the server as
  well as the 10 s context.
- Pools: `SetMaxOpenConns(4)` read and `2` write; `SetConnMaxLifetime(5m)`.
- `mariadb://`, `mysqlx://` and a URL with `unix(` are refused. Unix sockets are not supported in this wave.
- Wiring (Q8g): `op.source(ctx, dsn)` dispatches on the scheme (`postgres`/`postgresql` to `postgres.New`, `mysql`
  to `mysql.New`). It refuses a pipeline whose `source_kind` disagrees with its resolved DSN scheme: "pipeline
  orders-mysql: source_kind mysql but source_db_url is a postgres:// URL". The same goes for the write URL, and the
  existing "write URL is not the read URL" rule (`writemode.go`) applies to both kinds.
- Errors: `*mysql.MySQLError` is matched with `errors.As` and its `Number`, never its text. Q8b checks the numbers
  it uses against the MySQL error reference: 1044/1142/1143/1227 access denied (named with the missing privilege),
  1094 unknown thread, 1095 not thread owner, 1146 no such table (`clients.ErrNotFound`), 3024 max execution time
  exceeded.

### 6.2 `SourceSettingsReader.Settings`: a fixed list

Read from `performance_schema.global_variables`, left-joined to `performance_schema.persisted_variables`:

| Name | Why it is there | Healthy for Debezium |
|---|---|---|
| `version` | the server version (8.0 vs 8.4 statements, known issues of Q1) | 8.0 or 8.4 |
| `log_bin` | binlog on at all | `ON` |
| `binlog_format` | row events | `ROW` |
| `binlog_row_image` | full before and after images | `FULL` |
| `gtid_mode`, `enforce_gtid_consistency` | GTID positions survive a failover | `ON`, `ON` (recommended, not required) |
| `server_id` | the server's id; a connector's `database.server.id` must differ from it and from every replica's | non-zero |
| `binlog_expire_logs_seconds`, `binlog_expire_logs_auto_purge` (8.0.29+) | how long a stopped connector can stay stopped | longer than the longest planned downtime |
| `lower_case_table_names` | whether `Shop.Orders` and `shop.orders` are one table, so a scope or include-list mismatch can be explained | reported only |
| `rds.binlog_retention_hours` (optional, from `CALL mysql.rds_show_configuration` on RDS/Aurora; absent elsewhere) | RDS purges by this, not by `binlog_expire_logs_seconds` | set |

`PendingRestart` is true when a persisted value differs from the running value: a `SET PERSIST_ONLY` that takes
effect at the next restart. `Unit` is `s` for the seconds setting. The tool's healthy column is a summary line, not
a refusal. Confirm against the MySQL 8.0 and 8.4 references in Q8b: the variable names, `persisted_variables`, and
whether `binlog_format` (deprecated in 8.0.34) is still readable on 8.4.

### 6.3 `TableReplication`

One query over `information_schema.TABLES` left-joined to `TABLE_CONSTRAINTS` (`CONSTRAINT_TYPE = 'PRIMARY KEY'`)
for the `(TABLE_SCHEMA, TABLE_NAME)` pairs, all bound as `?`. `ReplicaIdentity` is `""`, `Engine` is
`TABLES.ENGINE`. A table the read user cannot see is `clients.ErrNotFound`, as on Postgres. `source_table_replication`
then says "missing, or not visible to the read user". The tool's summary on MySQL says what a missing key means:
Debezium emits keyless events unless `message.key.columns` names a key. It does not say "set replica identity".

### 6.4 `RoleInfo`

The connector's user (`database.user` from the connector config, as on Postgres) is looked up in the grant tables.
The client never runs `SHOW GRANTS`, which cannot be bound:

- `mysql.user` rows with `User = ?`: `Host`, `account_locked`, `password_expired`, `Repl_slave_priv`,
  `Repl_client_priv`, `Select_priv`, `Reload_priv`, `Lock_tables_priv`, `Show_db_priv`. **Never
  `authentication_string`.** The read role gets column-level SELECT on exactly these columns (§10.2), so it cannot
  read password hashes.
- `mysql.db` (`Select_priv` for the account and database; `Db` holds `LIKE` patterns, matched as the server
  matches them) and `mysql.tables_priv` (`FIND_IN_SET('Select', Table_priv)`).
- One level of roles: `mysql.role_edges` for roles granted to the account, counted only when they are active at
  login (`mysql.default_roles`, or `activate_all_roles_on_login = ON`). Partial revokes are not modelled. The tool
  says so in one line when `partial_revokes = ON`.
- `Exists` = at least one account. `CanLogin` = `account_locked = 'N'` and `password_expired = 'N'`. `Replication` =
  `REPLICATION SLAVE` and `REPLICATION CLIENT`. `Privileges` carries those two plus `RELOAD`, `LOCK TABLES` and
  `SHOW DATABASES`. `Accounts` lists `user@host`, and several accounts combine with AND (§5.2).

Confirm the column names on 8.0 and 8.4 in Q8b, and the role activation rule against the MySQL reference.

### 6.5 `Blockers` and `BlockersOnTables`

Two kinds of session hold a MySQL connector back:

1. **Long transactions**: `information_schema.innodb_trx` joined to `performance_schema.threads` on
   `trx_mysql_thread_id = PROCESSLIST_ID`, where `trx_started < NOW() - INTERVAL ? SECOND`. They hold metadata
   locks that stall the snapshot's locking and DDL, and a big uncommitted writer reaches the binlog only at commit.
2. **Lock holders the connector waits for**: `performance_schema.metadata_locks` with a `GRANTED` lock on an object
   on which a thread whose `PROCESSLIST_USER` is the connector's user has a `PENDING` lock. That covers the
   snapshot's `FLUSH TABLES WITH READ LOCK` or `LOCK TABLES` waiting behind an open transaction or a `LOCK TABLES`.
   These rows get `BlocksSlot = true`.

`DBSession`: `PID` = `PROCESSLIST_ID`; `StartedAt` = `trx_started`, or zero; `XactStart` = `trx_started`; `User`;
`App` = the `program_name` connection attribute (`performance_schema.session_connect_attrs`) or ""; `State` =
`PROCESSLIST_COMMAND` plus `PROCESSLIST_STATE`; `Query` = `PROCESSLIST_INFO`, redacted by N5 and cut to 200 bytes
(untrusted). `BlockersOnTables` blanks `User`, `App` and `Query` (`InScope` false) for a session that holds or waits
for no lock on one of the pipeline's tables, as Postgres does. Threads of `TYPE = 'BACKGROUND'` are never listed.

Needs: the read role's `PROCESS` (other users' threads and `innodb_trx`) and `SELECT` on `performance_schema`. The
`wait/lock/metadata/sql/mdl` instrument is enabled by default in 8.0. When it is off, part 2 is empty and the tool
says "metadata lock instrumentation is off". Confirm in Q8b on the profile.

### 6.6 `LongTransactions` and `Row` (the `SourceDB` methods)

- `LongTransactions`: part 1 of §6.5 as `clients.Transaction`. `PID` is the processlist id. `Age` is now minus
  `trx_started`. `WaitEvent` is `lock:<trx_state>` when `trx_state = 'LOCK WAIT'`. `Query` is only the first
  keyword of `PROCESSLIST_INFO`, as on Postgres. `HasXID` = `trx_rows_modified > 0`.
- `Row(table, pkColumn, pk)`: ``SELECT * FROM `db`.`table` WHERE `pk` = ? LIMIT 1`` after the identifier rule.
  `pk` is bound as a string, and MySQL converts it to the column type. `[]byte` values become strings when they are
  valid UTF-8 and `<binary N bytes>` otherwise. The tool redacts and wraps values exactly as `pg_row` does (they are
  customer data and untrusted).
- `ReplicationSlots`, `RecentDDL`: `clients.ErrNotSupported`.

### 6.7 `BinlogReader.BinlogStatus`

`SELECT @@global.gtid_mode, @@global.gtid_executed, @@global.gtid_purged, @@server_id, @@server_uuid,
@@binlog_expire_logs_seconds`, then `SHOW BINARY LOG STATUS` on 8.2+ or `SHOW MASTER STATUS` before 8.2 (chosen
by `version`), then `SHOW BINARY LOGS`, then on RDS `CALL mysql.rds_show_configuration` (best effort; an error
leaves `RetentionHours` nil). Needs `REPLICATION CLIENT`. GTID sets can be long. They are kept whole in `Data`, and
the tool's summary prints at most 200 bytes of each.

**GTID set arithmetic** (parse, union, contains, subtract) is a small pure-Go package,
`internal/clients/mysql/gtid` (A8, Q8b). It is table-tested against the server's own `GTID_SUBSET` /
`GTID_SUBTRACT` on the profile. `mysql_binlog_status` (A8) and the reset action (A2) both import it. Importing
another agent's package is allowed; editing it is not.

### 6.8 `SourceAdminWriter` (Q8f, on the `SOURCE_DB_WRITE_URL` pool)

Every method keeps the Q2b rules: write mode on, the room pipeline's scope in ctx (`tools.ErrNoScope` /
`tools.ErrOutOfScope` before any I/O), identifiers by §5.6, 10 s deadline.

- **`GrantSelect(role, tables)`**: the role must match the identifier rule, and the account is resolved from
  `mysql.user`. Exactly one account with that user name proceeds; several are refused ("user debezium has accounts
  debezium@% and debezium@10.0.%; grant by hand or remove one"), because a guess could widen the wrong account.
  ``GRANT SELECT ON `db`.`t` TO 'user'@'host'`` per table, with the host by the §5.6 rule. SELECT only. The write
  role needs `SELECT ... WITH GRANT OPTION` on those tables (or the databases).
- **`CreateHeartbeatTable(table)`**: ``CREATE TABLE IF NOT EXISTS `db`.`t` (id INT NOT NULL PRIMARY KEY, ts
  TIMESTAMP(6) NOT NULL) ENGINE=InnoDB``, with the Q2c naming rules (the name contains `heartbeat`; it sits in a
  database of the pipeline's `source_tables`; it is not itself a source table).
- **`EndSession(pid, startedAt)`**: on one pinned connection (`db.Conn`), re-read the thread by `PROCESSLIST_ID`
  (user, host, command, `TYPE`, `program_name`, the transaction's `trx_started`). Refuse with
  `clients.ErrSessionChanged` when the thread is gone, when it has no open transaction, or when `trx_started` is not
  `startedAt`. A zero `startedAt` is always refused: a session without a transaction has no stable identity (D3).
  Refuse with `clients.ErrSessionProtected` for: the pinned connection itself (`CONNECTION_ID()`), any session of
  the reader's or writer's user, `program_name` `piperoom-*`, a `Binlog Dump` or `Binlog Dump GTID` command (a
  connector's or a replica's stream), `TYPE = 'BACKGROUND'`, and `system user` or `event_scheduler`. Then `KILL
  CONNECTION <pid>`, with the pid formatted by `strconv.FormatInt`. Error 1094 maps to `ErrSessionChanged`; 1095
  maps to "the write role lacks CONNECTION_ADMIN". The window between the re-read and the kill is one round trip on
  the same connection. A processlist id is not reused while its thread lives, and the identity was just matched.
- **`SetReplicaIdentity`**: `clients.ErrNotSupported` before any I/O.

Q8f's integration test tries the bound forms (`KILL ?`, an account bound as `?@?`). If the server takes them, the
writer binds them, and the literal forms above remain the documented fallback.

## 7. Tools

### 7.1 The Q2a `source_*` tools on MySQL (A3, Q8n)

They already reach the reader through a type assertion. After the item 7 move they assert `clients.*` types
instead of `postgres.*`, and `*mysql.DB` satisfies them. Per-source wording comes from `Settings()` names and
`TableReplication.Engine` / `ReplicaIdentity == ""`, not from a source-kind switch where avoidable. The healthy
column of §6.2 lives in the tool (a table keyed by setting name). Q8c's test shows the four tools on a MySQL fake
through the interface (board Done-when).

### 7.2 `mysql_binlog_status` (A8, `internal/tools/mysqltools`, Q8c)

Input: optional `connector` (one of the pipeline's MySQL source connectors; default all of them). Reads
`BinlogStatus` and, per connector, the stored offsets (`connect.SourceOffsetReader`) and `database.server.id` from
its config. Verdict lines, most severe first:

- **"the binlog the connector needs is purged"**: with GTIDs, the stored `gtids` set does not contain
  `gtid_purged`, so purged transactions were never read. Without GTIDs, the stored `file` sorts before the oldest
  file in `Binlogs` (same base name, numeric suffix compared as a number).
- "the stored position is from another server": the offset's `server_id` differs from `@@server_id` and GTIDs are
  off, or the stored base name differs.
- "position past the end": the stored `file`/`pos` is beyond the current one, or the stored `gtids` is not a
  subset of `gtid_executed`.
- "retention is shorter than the connector's age": the offset's `ts_sec` is older than `now - expire` (or the RDS
  retention hours) and the connector is not RUNNING.
- `database.server.id` equals `@@server_id`, or equals another of the pipeline's connectors' ids (the
  `server-id-clash` cause).
- `gtid_mode` is not ON: a failover to a replica loses the position. This is a note, not a fault.

Positions only, never event contents. Scope: the pipeline's connectors; the server is the pipeline's source. No
input names a host, a file or a GTID.

### 7.3 `kafka_schema_history` (A2, `internal/tools/kafkatools`, Q8e)

Source-neutral by name: any Debezium connector with a schema history topic (MySQL now; SQL Server and Oracle later).
The topic comes from the in-scope connector's config, `schema.history.internal.kafka.topic` (Debezium 2.0+), or
the pre-2.0 `database.history.kafka.topic`, named as a renamed key from the Q1 index. Never from the model, which is
the DLQ-topic pattern of `tools.Scope`. Checks:

- the topic is missing ("history-topic-deleted");
- more than one partition;
- finite retention (`retention.ms` or `retention.bytes` not `-1`), or `cleanup.policy` not `delete`;
- `schema.history.internal.store.only.captured.tables.ddl` is set (a note: a newly added table then needs a
  `recovery` snapshot).

It also shows the last at most 20 records: `ts_ms`, `databaseName`, `position` (file/pos/gtids), and `ddl` cut to
200 bytes, redacted (N5) and wrapped as untrusted. Confirm the record field names and the topic requirements
against the Debezium MySQL docs in Q8e. This is where a MySQL pipeline's DDL history lives (§1, `RecentDDL`).

### 7.4 `source_row`, `source_long_transactions` (A3, `internal/tools/dbtools`, Q8n)

The same code as `pg_row` and `pg_long_transactions` (over `clients.SourceReader`) under neutral names, registered
**in MySQL rooms only** in this wave. Postgres rooms keep `pg_row` and `pg_long_transactions` unchanged, so no
Postgres prompt snapshot or eval label moves. Unifying the names for Postgres is a later, eval-gated change (D5).

## 8. Actions

### 8.1 The Q2 `source*` actions on MySQL (A3, Q8n)

- `source_grant_select`, `source_create_heartbeat_table` and `source_end_session` implement
  `actions.SourceLayered`, so a MySQL pipeline's step is approved by the `mysql` owner. Their deps switch from
  `postgres.*` to the `clients.*` types of §5.2, so wiring hands them the MySQL client unchanged.
- `source_create_heartbeat_table` on MySQL: no publication step (there is none). Preview says "on MySQL,
  `heartbeat.interval.ms` alone keeps the stored position moving when the captured tables are idle; the table is
  needed only for `heartbeat.action.query`". It adds the follow-up `connect_update_connector_config` step with
  `heartbeat.interval.ms`, and the query only when asked.
- `source_end_session` on MySQL: Preview shows the age, user, `program_name`, the redacted query, and "KILL
  CONNECTION rolls back the open transaction; a large one can take as long to roll back as it ran". A session with
  a zero `StartedAt` is refused at Validate (D3).
- `source_grant_select` on MySQL: Preview names the resolved account `user@host`. Validate refuses an ambiguous
  account before approval, not at Execute.
- `pg_set_replica_identity` and every `pgactions` action are not registered for a MySQL pipeline (Q8g).

### 8.2 MySQL positions in `connect_reset_source_offsets` (A2, Q8e)

The action already takes a `position {partition, offset}` checked against the stored shape. For a MySQL connector
(`connector.class` ends in `MySqlConnector`) it adds, at Validate and again at Execute (reading `BinlogStatus`
through a `clients.BinlogReader` that wiring passes; none means `ErrNotSupported` with "cannot check a MySQL
position"):

- partition `{"server": <topic.prefix>}`;
- offset `file` + `pos` (required without GTIDs), `gtids` (required when `gtid_mode = ON`), optional `row`,
  `event`, `ts_sec`, `server_id`; a `snapshot` key is refused (never write a mid-snapshot position);
- refused: `file` not among `Binlogs` (purged, "the binlog the position names is purged", or never existed); `pos`
  beyond that file's size, or beyond `Pos` in the current file; `gtids` not a superset of `gtid_purged`; `gtids`
  not a subset of `gtid_executed` ("past the end");
- Preview prints the stored offsets (the rollback), the target, and the GTID difference in transaction counts.

Clearing the offsets (no position) is unchanged. Preview adds the `snapshot.mode` consequence for MySQL (`initial`
re-snapshots, `when_needed` too, `no_data` / `never` resumes from the current binlog position and **skips** every
change in between). Confirm the value names per version through the Q1 index.

### 8.3 Two MySQL connector keys in `connect_update_connector_config` (A2, Q8e)

- `database.server.id`: MySQL source connectors only. An integer in 1..4294967295, different from `@@server_id`
  and from every other connector's id in the pipeline. Rollback is the old value. This fixes `server-id-clash`.
- `snapshot.mode` = the running version's recovery value (`recovery`; `schema_only_recovery` before the rename the
  Q1 index records) on MySQL source connectors only, for "history topic lost". Preview carries Debezium's warning:
  safe only if no schema change happened since the stored position, which `kafka_schema_history` and the DDL of the
  recreated topic cannot prove. The proposal's Verify asks for the connector RUNNING with the history topic present.
  Rollback is the old value.
- `heartbeat_keys.go` accepts the MySQL heartbeat form: ``INSERT INTO db.t (id, ts) VALUES (1, NOW(6)) ON
  DUPLICATE KEY UPDATE ts = NOW(6)``, the same literal-only grammar with MySQL's clause instead of `ON CONFLICT`,
  chosen by `connector.class`.

### 8.4 Not added: a replication grant

`missing-replication-grant` is diagnosed (`source_role_grants` shows `REPLICATION SLAVE` false) but gets **no
action**. `REPLICATION SLAVE` lets an account read every change on the whole server, not only the pipeline's
tables, so it stays a human's `GRANT`. The room drafts it as text in the hand-off (D4).

## 9. Map, scope, watcher (A3 Q8d, A7 Q8p)

- **Topology**: `sourceNode` makes `mysql:<db>.<table>` with layer `mysql` for a MySQL pipeline. Its facts come
  from `BinlogStatus` (`oldest` binlog, `expire`, `gtid_mode`), not from slots. A failed read gives `error:
  unreachable` as today. `trace_record`'s source step uses `p.SourceLayer()`. Edges are unchanged
  (`source_table → connector → topic`).
- **Scope** (`tools/scope.go`): no change of rule. `source_tables` entries are `db.table` (the same two-part shape),
  and topics are `<topic_prefix>.<db>.<table>`, which `Scope.Topics` already builds. Q8d adds table tests for both
  kinds. The schema history topic is admitted only as derived from an in-scope connector's config (§7.3).
- **Watcher**: `observe.go` reads replication slots for every CDC pipeline. For a MySQL pipeline it must not:
  `ReplicationSlots` answers `ErrNotSupported`, and the slot signals are off for that pipeline (one INFO line, the
  `drift.go` pattern). The connector-state, lag and DLQ signals are source-neutral and stay on. They are what trips
  the four Q8a breaks. A "binlog retention shorter than connector downtime" signal is a follow-up (it needs its own
  `devctl break` and scenario by the X-wave rule).

## 10. Dev stack, discover, evals, web, docs, CI

### 10.1 Dev profile `mysql` and four breaks (A1, Q8a)

- `mysql` service, profile `mysql`: MySQL 8.4 LTS pinned by digest, port `3306`, `--gtid-mode=ON
  --enforce-gtid-consistency=ON --binlog-format=ROW --binlog-row-image=FULL --server-id=223344
  --binlog-expire-logs-seconds=86400`. An init script creates database `shop`, the tables `orders` and
  `order_items`, and the users of §10.2 with dev passwords (local stack only, AGENTS.md §3).
- Connector `orders-mysql` on the existing Connect image (it ships the MySQL connector): `topic.prefix: shop`,
  `database.server.id: 5401`, `table.include.list: shop.orders,shop.order_items`, schema history topic
  `schema-history.shop` (1 partition, infinite retention).
- `dev/owners.mysql.yaml` (like `owners.q2.yaml`) with pipeline `orders-mysql`, `source_kind: mysql`, layer owner
  `mysql`. It loads only once Q8d merges, so Q8a is After Q8d (§13).
- Breaks, each with its `fix` and a `dev/scenarios.md` row:

| Break | How | Expected symptom | Shows in |
|---|---|---|---|
| `binlog-purged` | stop the connector, write, `FLUSH BINARY LOGS` twice, `PURGE BINARY LOGS TO` the newest, resume | task FAILED: the binlog position or GTIDs it needs are no longer on the server | `mysql_binlog_status`, watcher connector signal |
| `history-topic-deleted` | stop, delete `schema-history.shop`, resume | task FAILED: the schema history topic is missing | `kafka_schema_history`, watcher |
| `missing-replication-grant` | `REVOKE REPLICATION SLAVE ON *.* FROM` the connector user, restart the task | task FAILED: access denied for the binlog stream | `source_role_grants` |
| `server-id-clash` | a second connector `orders-mysql-shadow` with the same `database.server.id` | the server drops the older binlog stream; reconnect loop, errors | `mysql_binlog_status` (id clash), watcher |

The fixes: reset offsets and re-snapshot; `snapshot.mode` recovery; re-`GRANT`; delete the shadow connector or
change its id. Confirm the exact Debezium error texts on the profile in Q8a. Scenarios label by cause, not by error
text.

### 10.2 Database users (documented by C14 in Q8l)

| User | Grants | Why |
|---|---|---|
| connector (`debezium`, Debezium's own docs) | `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT` (+ `LOCK TABLES` on RDS/Aurora) | the binlog stream and the snapshot |
| Piperoom read (`SOURCE_DB_URL`) | `SELECT` on the captured tables; `REPLICATION CLIENT`; `PROCESS`; `SELECT ON performance_schema.*`; column-level `SELECT (User, Host, account_locked, password_expired, Repl_slave_priv, Repl_client_priv, Select_priv, Reload_priv, Lock_tables_priv, Show_db_priv) ON mysql.user`; `SELECT ON mysql.db, mysql.tables_priv, mysql.role_edges, mysql.default_roles`; on RDS `EXECUTE ON PROCEDURE mysql.rds_show_configuration` | §6.2 to §6.7. **No `REPLICATION SLAVE`**: Piperoom reads no binlog stream. The board's Q8l text said otherwise and is corrected (§13). Never `authentication_string` |
| Piperoom write (`SOURCE_DB_WRITE_URL`) | `SELECT ... WITH GRANT OPTION` on the captured tables; `CONNECTION_ADMIN` (8.0+; `SUPER` is deprecated); `CREATE` on the heartbeat database; the read grants needed for the re-reads | §6.8 |

### 10.3 Discover (C10, Q8h)

`MySqlConnector` becomes a pipeline with `source_kind: mysql`. `source_tables` come from the literal
`table.include.list` entries (the `connector_scope.go` rule), `topic_prefix` from `topic.prefix`, and there is no
slot. The OpenLineage namespace becomes `mysql://host:port` (`openlineage/namespaces.go`). The output loads with
`owners.Load`.

### 10.4 Scenarios and evals (C7 Q8i, C6 Q8j)

- Four scenarios, `mysql-binlog-purged`, `mysql-history-topic-deleted`, `mysql-missing-replication-grant` and
  `mysql-server-id-clash`, each with `source_kind: mysql`. Labels: layer `mysql`, `kafka`, `mysql` and
  `kafka-connect`; actions `connect_stop_connector` + `connect_reset_source_offsets`;
  `connect_update_connector_config` (`snapshot.mode`); none (a drafted GRANT, §8.4);
  `connect_update_connector_config` (`database.server.id`) or `connect_delete_connector` for the shadow.
- Replay is per tool (`evals/replay.go`), so "MySQL replay readers" means recordings for `mysql_binlog_status`,
  `kafka_schema_history`, `source_*` on MySQL and `connect_source_offsets`. The harness builds the MySQL tool set
  when a scenario says `source_kind: mysql`, and the report gives the pass rate per source kind. The Postgres pass
  rate must not move.

### 10.5 Web (A6, Q8k)

Layer `mysql` gets the label "MySQL" and its own colour in `badge.tsx` (contrast-checked in both themes). A
`mysql:` node renders in the source lane. The proposal, approval and people views show the `mysql` layer. Vitest
covers it, and the Postgres lane does not change.

### 10.6 Docs (C14, Q8l) and CI (C1, Q8m)

Q8l writes the §10.2 grants with SQL and a citation per claim (Debezium MySQL connector docs, MySQL reference), the
`mysql://` URL rules of §6.1, and the data-handling row (process-list query text and row values reach the model,
redacted and wrapped, as on Postgres). Q8m: unit tests of the new packages run in `go test ./...` (no Docker); the
README lists MySQL; the integration job runs the `mysql` profile if the CI integration job brings the dev stack up
(else a follow-up).

## 11. Security and untrusted data

- **Read-only by grant and session**: the read role has no write grant (§10.2), and the read pool sets
  `transaction_read_only` and `max_execution_time`. Writers exist only on the write pool, with write mode on (P1a-3),
  behind approval (P3-0).
- **No DSN pass-through** (§6.1): the URL parameter allow-list is the guard against `LOAD DATA LOCAL INFILE` file
  reads, stacked statements and arbitrary `SET`s through configuration. A test feeds every refused parameter.
- **No password hashes**: column-level grant on `mysql.user`; the client selects named columns only.
- **Untrusted text**: `PROCESSLIST_INFO`, DDL text from the schema history topic, table and column names from the
  server, row values, Debezium error traces. Each is redacted (N5), injection-scanned where it reaches the model
  (N6), wrapped by the agent's untrusted-data wrapper, and cut (200 bytes per query or DDL line). GTID sets and
  binlog names are positions, but they are still capped and printed as data.
- **Identifiers**: §5.6. Every name that reaches SQL is either bound or matched by the rule regex and backticked.
  The account host has its own regex. The pid is an `int64`.
- **Kill safety**: identity re-check on a pinned connection; protected sessions (§6.8); a session without a
  transaction is never killed by Piperoom.
- **Grants Piperoom may make**: SELECT only, on in-scope tables, to an unambiguous connector account. Never
  `REPLICATION SLAVE` (§8.4).
- **Secrets**: the connector config's `database.password` is masked by the existing config redaction. `mysql://`
  URLs are secrets in `config.go` and `connections:` like the Postgres ones, and are never logged. The driver's
  logger is set to an `slog` adapter that drops packet dumps.

## 12. Failure modes

| Failure | What the user sees |
|---|---|
| MySQL down or unreachable | every source tool: "the source database is unreachable" (wrapped dial error, 5 s); the map node `error: unreachable` |
| A read grant missing | the tool names the privilege (`PROCESS`, `REPLICATION CLIENT`, `SELECT on performance_schema`) from the error number, and still answers what it could read |
| MySQL 5.7 or MariaDB | start-up probe reads `version`; 5.7 gives "MySQL 5.7 is not supported (end of life)"; MariaDB gives "MariaDB is not a MySQL source; not supported in this wave" |
| `SHOW BINARY LOG STATUS` vs `SHOW MASTER STATUS` | chosen by version; an "unknown statement" error falls back once to the other form |
| Query over 10 s | error 3024 or context deadline: "timed out after 10 s", with no partial table |
| GTIDs off | GTID checks skipped and said so; file/pos checks apply |
| RDS without `rds_show_configuration` access | `RetentionHours` nil; the tool says "RDS retention not readable" |
| `source_kind` vs DSN scheme mismatch | wiring fails Open with the pipeline id and both values |
| Ambiguous account for `GrantSelect` | refused at Validate with the account list |
| Session gone or changed before `KILL` | `ErrSessionChanged`; nothing killed; Execute's result says so |
| `KILL` denied | "the write role lacks CONNECTION_ADMIN"; on RDS, see D7 |

## 13. Board changes (applied to `tasks/Q-board.md` in this PR)

- Q0 item 7 points here. The proposed shape is replaced by §5.
- **Q8-0**: Done-when adds "the item 7 contracts PR merged (it also moves item 2 into `clients.go`)".
- **Q8a** After adds Q8d (it ships `dev/owners.mysql.yaml`).
- **Q8b**: Done-when names the URL allow-list, `transaction_read_only`, `ScopedBlockerReader`, `LongTransactions`,
  `Row`, `ErrNotSupported` for slots and DDL, the `gtid` package, and the go.mod review path.
- **Q8c**: Done-when names the verdicts of §7.2.
- **Q8e**: adds the two connector keys and the MySQL heartbeat form (§8.3), and is After Q8b (the `gtid` package).
  Days 1.5 to 2.
- **Q8f**: names the account resolution and the protected sessions.
- **Q8g**: also `actions.LayerFor` in the agent's action list.
- **Q8l**: the read user's grants corrected (no `REPLICATION SLAVE`; §10.2).
- New rows:
  - **Q8n** (A3, 1.5 d): sourceactions and dbtools source-neutral (clients types, `SourceLayered`, MySQL previews,
    `source_row` / `source_long_transactions`). After Q8-0.
  - **Q8o** (A5, 0.5 d): room uses `actions.LayerFor` with the room pipeline's source kind. After Q8-0, Q8d.
  - **Q8p** (A7, 0.5 d): watcher skips slot reads for a MySQL pipeline. After Q8d.
  - **Q8q** (C18, 0.25 d): `nettools` default port `3306` for a `mysql://` source URL. After Q8-0.

Total step 8 goes from 21 to 24.25 agent-days (A2 +0.5, A3 +1.5, A5 +0.5, A7 +0.5, C18 +0.25). The MySQL chain (Q8-0, Q8d, Q8a, Q8b, Q8f, Q8g) gains Q8d before
Q8a (1.5 days, in parallel with A8's start on Q8b's unit tests against fakes).

## 14. Open decisions for the owner (recommended default first)

| # | Question | Recommendation | Alternative |
|---|---|---|---|
| D1 | Layer id | a new `mysql` layer (§4.1) | a neutral `source` layer with a migration of events, owners files and labels |
| D2 | Driver | `github.com/go-sql-driver/mysql` behind the §6.1 allow-list, two-lead review for `go.mod` | none viable in the standard library; `go-mysql-org/go-mysql` speaks the replication protocol we do not need |
| D3 | Ending a session that has no open transaction (`LOCK TABLES`, `FLUSH TABLES WITH READ LOCK` holders) | not offered: no stable identity under the `EndSession(pid, startedAt)` contract; the room names the session and the DBA ends it | add `DBSession.ThreadID` (`performance_schema` thread id, never reused while the server runs) and an optional `EndSessionByThread`: a contract change for a rare case |
| D4 | An action that grants `REPLICATION SLAVE`/`CLIENT` | no (§8.4) | a `source_grant_replication` action, T2, never auto-approved |
| D5 | `source_row` / `source_long_transactions` in Postgres rooms | MySQL rooms only now; rename for Postgres later behind an eval gate | register both names in Postgres rooms now (prompt snapshots move) |
| D6 | Schema history as a `what_changed` source | later (a `ChangeSourceSchemaHistory` constant needs X4's contract and a watcher reader) | in this wave as Q8r (A7, 1 d) |
| D7 | RDS/Aurora `KILL` | try `KILL`; on 1095 on RDS (`@@basedir` under `/rdsdbbin/`) call `mysql.rds_kill(?)`, which binds the id | refuse on RDS and name the procedure |
| D8 | Who maintains `internal/connprobe` for `mysql://` (it probes `postgres://` only; no owner row in AGENTS.md §2) | the leads assign it to A3 with the U-wave ownership sweep; until then `GET /api/admin/config` shows a MySQL source as unprobed | a one-row grant to A8 |
| D9 | Floor version | MySQL 8.0.x and 8.4 LTS; Aurora MySQL 3 | also 5.7 (end of life since 2023-10; extra statement forms) |
