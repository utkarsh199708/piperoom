// Package pgactions holds the write actions on the source Postgres (P3b):
// pg_drop_replication_slot, pg_add_publication_table and pg_snapshot_signal
// (a Debezium incremental snapshot signalled through the connector's signal
// table), and the recovery action pg_resnapshot_rows (X2a: the same snapshot
// limited to a key range or time window, over the signal table or the Kafka
// signal topic). They read through the read-only source client in Validate
// and Preview (and Verify) and write only in Execute, through a
// clients.SourceWriter (or, for a Kafka-signal connector, the Kafka writer)
// that wiring hands over with write mode on.
package pgactions
