// Package sourceactions holds the approved database admin actions of the Q
// wave's step 2 (Q2c, Q2d) on the pipeline's source database:
// pg_set_replica_identity (Postgres only), source_create_heartbeat_table,
// source_grant_select and source_end_session. The source_ names are
// source-neutral: they run on a Postgres or a MySQL pipeline (Q8n) over the
// same reader and writer interfaces, are SourceLayered (the pipeline's
// source layer approves them), and render MySQL previews on a MySQL
// pipeline; pg_set_replica_identity refuses a MySQL pipeline.
//
// Validate and Preview read only, through the read-only source and Connect
// clients; Execute writes only through the admin writer (the
// SourceAdminWriter methods of postgres.Writer, Q2b, or of the MySQL writer,
// Q8f) that wiring hands over with write mode on and SOURCE_DB_WRITE_URL
// set. Every action refuses, before any I/O, a target outside the room's
// pipeline (actions.Scoped), and Preview and Execute fail closed without the
// room's pipeline (or the explicit tools.WithUnscoped opt-out) in ctx.
// Wiring (Q2e, Q8g) registers them; this package registers nothing.
package sourceactions
