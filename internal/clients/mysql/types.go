package mysql

import (
	"context"
	"time"
)

// Local adapters for the contract of docs/design/mysql-source.md §5.2 (Q0 item
// 2 moved into internal/clients/clients.go, plus item 7's BinlogReader). That
// move has not landed: item 2 still lives in internal/clients/postgres
// (settings.go), and this package must not import the postgres client. So the
// shapes are declared here exactly as the design gives them (names, fields,
// JSON tags); when the "[Q8-0] contracts: item 7" PR lands, each becomes an
// alias of its clients type (type DBSetting = clients.DBSetting, ...) and
// nothing else changes. See the contract change request of the Q8b PR.

// SourceSettingsReader is the read-only source-settings surface of a CDC
// source (design §5.2 (a)): Postgres in Q2a, MySQL here. Every method is
// read-only and bounded by a 10 s deadline.
type SourceSettingsReader interface {
	Settings(ctx context.Context) ([]DBSetting, error)
	TableReplication(ctx context.Context, tables []string) ([]TableReplication, error)
	RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error)
	Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error)
}

// ScopedBlockerReader is the pipeline-scoped Blockers: a session that holds or
// waits for no lock on one of tables comes back anonymised (InScope false).
type ScopedBlockerReader interface {
	BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]DBSession, error)
}

// BinlogReader is implemented by the MySQL source client. It is never
// embedded in SourceDB. Read-only; 10 s per call. (design §5.2 (d))
type BinlogReader interface {
	BinlogStatus(ctx context.Context) (BinlogStatus, error)
}

// DBSetting is one server setting with its changed-but-not-yet-applied flag.
// On MySQL PendingRestart means a persisted value (SET PERSIST_ONLY) differs
// from the running one.
type DBSetting struct {
	Name           string `json:"name"`
	Value          string `json:"value"`
	Unit           string `json:"unit,omitempty"`
	PendingRestart bool   `json:"pending_restart,omitempty"`
}

// TableReplication is a table's primary key presence, replica identity
// (Postgres) and storage engine (MySQL).
type TableReplication struct {
	Table           string `json:"table"`
	HasPrimaryKey   bool   `json:"has_primary_key"`
	ReplicaIdentity string `json:"replica_identity,omitempty"` // default | full | index | nothing; "" when the source has none (MySQL)
	// Engine is the MySQL storage engine (InnoDB, MyISAM, ...); "" on
	// Postgres. A non-transactional engine changes what a snapshot can
	// promise.
	Engine string `json:"engine,omitempty"`
}

// RoleInfo is a source user's login and replication rights and its SELECT
// privilege per table.
type RoleInfo struct {
	Role        string          `json:"role"`
	Exists      bool            `json:"exists"`
	CanLogin    bool            `json:"can_login"`
	Replication bool            `json:"replication"` // Postgres REPLICATION; MySQL REPLICATION SLAVE and REPLICATION CLIENT
	Selectable  map[string]bool `json:"selectable"`  // table -> SELECT
	// Accounts are the MySQL accounts ("user@host") with this user name;
	// nil on Postgres. With more than one, every flag above and in
	// Privileges is true only if it is true for every account.
	Accounts []string `json:"accounts,omitempty"`
	// Privileges are the further global privileges the source's CDC
	// connector needs, by the source's own name (MySQL: RELOAD, LOCK TABLES,
	// SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT); nil when the
	// source has none to report.
	Privileges map[string]bool `json:"privileges,omitempty"`
}

// DBSession is one session that can hold back the CDC stream. Query is
// untrusted text: redacted and cut before it leaves the client. On MySQL PID
// is the processlist id (what KILL takes); StartedAt and XactStart are the
// open transaction's start (innodb_trx.trx_started), zero for a session
// without one; BlocksSlot means the session holds a metadata lock that
// another session waits for. InScope is false for a session a scoped read
// anonymised: its User, App and Query are blank.
type DBSession struct {
	PID        int64     `json:"pid"`
	StartedAt  time.Time `json:"started_at"`
	XactStart  time.Time `json:"xact_start"`
	User       string    `json:"user"`
	App        string    `json:"application"`
	State      string    `json:"state"`
	Query      string    `json:"query"` // untrusted, redacted, at most MaxSessionQueryBytes
	BlocksSlot bool      `json:"blocks_slot"`
	InScope    bool      `json:"in_scope"`
	// OldXact, SameDB and ClientBackend are never shown (Q2a): the session's
	// transaction is older than the read's threshold (server clock), the
	// session's default database is the one this client's URL names (false
	// when the URL names none), and it is an ordinary client thread (not the
	// replication applier's "system user" or the event scheduler).
	OldXact       bool `json:"-"`
	SameDB        bool `json:"-"`
	ClientBackend bool `json:"-"`
}

// MaxSessionQueryBytes caps DBSession.Query: the statement often holds
// literal row values, so only a short redacted fragment leaves the client.
const MaxSessionQueryBytes = 200

// MaxBinlogFiles caps BinlogStatus.Binlogs; the oldest files are kept,
// because the oldest file is the fact a purge check needs.
const MaxBinlogFiles = 500

// BinlogStatus is the binlog state of a MySQL server: positions and names
// only, never event contents.
type BinlogStatus struct {
	// File and Pos are the current write position (SHOW BINARY LOG STATUS,
	// SHOW MASTER STATUS before 8.2); "" and 0 with the binlog off.
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

// BinlogFile is one binlog file the server still has.
type BinlogFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}
