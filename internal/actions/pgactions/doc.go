// Package pgactions holds the write actions on the source Postgres (P3b):
// pg_drop_replication_slot, pg_add_publication_table and the signal_table half
// of connect_snapshot_table. They read through the read-only source client in
// Validate and Preview and write only in Execute, through a
// clients.SourceWriter that wiring hands over with write mode on.
package pgactions
