// Package sourceactions holds the approved database admin actions of the Q
// wave's step 2 (Q2c, Q2d) on the pipeline's source database:
// pg_set_replica_identity (Postgres only), source_create_heartbeat_table,
// source_grant_select and source_end_session. The source_ names are
// source-neutral: the MySQL source (Q8f) implements the same admin writer
// methods, so it reuses these actions instead of copying them.
//
// Validate and Preview read only, through the read-only source and Connect
// clients; Execute writes only through the admin writer (postgres.Writer's
// SourceAdminWriter methods, Q2b) that wiring hands over with write mode on
// and SOURCE_DB_WRITE_URL set. Every action refuses, before any I/O, a target
// outside the room's pipeline (actions.Scoped), and Preview and Execute fail
// closed without the room's pipeline (or the explicit tools.WithUnscoped
// opt-out) in ctx. Wiring (Q2e) registers them; this package registers
// nothing.
package sourceactions
