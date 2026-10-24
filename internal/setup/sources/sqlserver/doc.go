// Package sqlserver is the SQL Server prerequisite driver of the setup wave
// (docs/design/setup-agent.md §6; U4b), the counterpart of
// internal/setup/sources/postgres: the ordered reads that say whether a source
// SQL Server can serve the Debezium SQL Server connector (version and edition,
// SQL Server Agent, CDC on the database and on every table, the capture login
// and its SELECT grants), the validating wrapper around the setup-only writes
// (CDC enablement as CreatePublication, the ladder's canary table, the capture
// login), and the connector config with ${secret:} references.
//
// A driver is data plus checks, never a code path (design §1): each Check
// names the read that answers it, the condition that passes, the operation
// that fixes a failure (or the documented ask when no op can), and that
// operation's tier floor. The room routes the fix through its proposal path,
// so the driver never runs a write itself: it holds read-only readers only.
//
// Every identifier is checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ (and the
// 128-character sysname limit) before it reaches a reader, a writer or a
// message (AGENTS.md §4 rule 6); the executing writer in
// internal/clients/sqlserver []-quotes what it must interpolate.
package sqlserver
