package mysql

import (
	"piperoom/internal/clients"
)

// The item 2 and item 7 contract of docs/design/mysql-source.md §5.2, as the
// "[Q8-0] contracts: item 7" change put it in internal/clients/clients.go.
// Each name here is an alias of its clients type, so *DB and *Writer satisfy
// the clients interfaces directly and a caller needs no mysql.* type (the
// postgres client keeps the same aliases).

// SourceSettingsReader is clients.SourceSettingsReader.
type SourceSettingsReader = clients.SourceSettingsReader

// ScopedBlockerReader is clients.ScopedBlockerReader.
type ScopedBlockerReader = clients.ScopedBlockerReader

// BinlogReader is clients.BinlogReader.
type BinlogReader = clients.BinlogReader

// DBSetting is clients.DBSetting. On MySQL PendingRestart means a persisted
// value (SET PERSIST_ONLY) differs from the running one.
type DBSetting = clients.DBSetting

// TableReplication is clients.TableReplication (Engine is the MySQL storage
// engine; ReplicaIdentity is "").
type TableReplication = clients.TableReplication

// RoleInfo is clients.RoleInfo (Accounts and Privileges are filled on MySQL).
type RoleInfo = clients.RoleInfo

// DBSession is clients.DBSession. On MySQL PID is the processlist id (what
// KILL takes); StartedAt and XactStart are the open transaction's start
// (innodb_trx.trx_started), zero for a session without one. BlocksSlot is
// set as the reader that produced the row documents (blockers.go): with the
// connector's user known, "holds a metadata lock a session of that user
// waits for"; without it, "holds a metadata lock another session waits for",
// and never on a session a scoped read anonymised. OldXact, SameDB and
// ClientBackend: the transaction is older than the read's threshold (server
// clock), the session's default database is the one this client's URL names
// (false when the URL names none), and it is an ordinary client thread (not
// the replication applier's "system user" or the event scheduler).
type DBSession = clients.DBSession

// BinlogStatus is clients.BinlogStatus ("" and 0 for File and Pos with the
// binlog off).
type BinlogStatus = clients.BinlogStatus

// BinlogFile is clients.BinlogFile.
type BinlogFile = clients.BinlogFile

// SourceAdminWriter is clients.SourceAdminWriter. *Writer implements it on
// the SOURCE_DB_WRITE_URL pool; SetReplicaIdentity has no MySQL equivalent
// and answers clients.ErrNotSupported.
type SourceAdminWriter = clients.SourceAdminWriter

// MaxSessionQueryBytes caps DBSession.Query: the statement often holds
// literal row values, so only a short redacted fragment leaves the client.
const MaxSessionQueryBytes = 200

// MaxBinlogFiles caps BinlogStatus.Binlogs; the oldest files are kept,
// because the oldest file is the fact a purge check needs.
const MaxBinlogFiles = clients.MaxBinlogFiles

// ErrSessionChanged is clients.ErrSessionChanged: EndSession's pid no longer
// names the session that was read (the thread is gone, has no open
// transaction, or its transaction started at another time). Nothing was
// ended.
var ErrSessionChanged = clients.ErrSessionChanged

// ErrSessionProtected is clients.ErrSessionProtected: EndSession refused a
// session Piperoom never ends (design §5.2 (a), §6.8). Nothing was ended.
var ErrSessionProtected = clients.ErrSessionProtected

// The contract surfaces the MySQL client implements (design §5.2).
var (
	_ clients.SourceSettingsReader = (*DB)(nil)
	_ clients.ScopedBlockerReader  = (*DB)(nil)
	_ clients.BinlogReader         = (*DB)(nil)
	_ clients.SourceAdminWriter    = (*Writer)(nil)
)
