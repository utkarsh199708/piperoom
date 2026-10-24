// Package mysql is the MySQL prerequisite driver of the setup wave
// (docs/design/setup-agent.md §6; U2a): the ordered reads that say whether a
// source MySQL can serve a Debezium connector, the setup-only writes that make
// it able to (the ladder's canary table and insert, and a read user with the
// connector's grants), and the connector config with ${secret:} references.
//
// It mirrors internal/setup/sources/postgres. A driver is data plus checks,
// never a code path: each Check names the read that answers it, the condition
// that passes, the operation that fixes a failure and that operation's tier
// floor. The room routes the fix through its proposal path, so the driver never
// runs a write itself: it holds read-only readers only (the Q8 client's
// SourceSettingsReader, plus the TableReplication and RoleInfo reads).
//
// What MySQL does not have, this package does not pretend to. There is no
// ALTER SYSTEM equivalent in this wave: binlog_format, binlog_row_image, log_bin
// and the rest are changed in the option file or, on RDS/Aurora, in the DB
// parameter group, so a failing settings check names no op, only the documented
// ask, and a SET PERSIST is never offered (docs/design/mysql-source.md §3).
// There are no publications, so CreatePublication answers clients.ErrNotSupported.
//
// Two types implement clients.SetupSourceWriter: SetupWriter executes the
// writes on a write pool of its own (the runner's local implementation), and
// Writer validates every argument before handing the call to another
// SetupSourceWriter (the runner's remote client in a setup room, or a
// SetupWriter).
//
// Every identifier is checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ (at most 64
// characters, 32 for a user) and backtick-quoted; an account host passes
// ^[A-Za-z0-9_.:%-]{1,255}$ and is quoted as a literal; values are ? parameters
// (AGENTS.md §4 rule 6). The only value spliced into a statement is the
// generated read-user password, as lower-case hex, because CREATE USER has no
// safe parameter position on every server this wave supports.
package mysql
