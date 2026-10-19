package wiring

import (
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
)

// The P3 write actions (P3a Kafka, P3b source Postgres) are registered in both
// write modes, so GET /api/actions lists them, the agent may propose them and
// the room previews them. Validate and Preview read only: through the
// read-only Connect adapter, the source reader, and the two read-only views
// of the Kafka reader client (kafka.Planner, kafka.ReplayReader), which hold
// the Writer's read code over the reader's connections and nothing that
// produces or commits. Execute alone needs a writer, and the writers exist
// only with write mode on (writemode.go); off, every writer field below is a
// nil interface, so Execute refuses ("no write credentials", "no Kafka write
// client") before any I/O.

// actionWriters returns the P3 writers of w for the actions: nil interfaces
// unless w was opened with write mode on. Each one is converted from the
// field only when the field holds a value, so a missing writer is a nil
// interface, never an interface holding a typed nil (an action checks
// Writer == nil). openWriters never stores a typed nil in the fields.
func actionWriters(w *Wired) (src clients.SourceWriter, kw clients.KafkaWriter) {
	if !w.write {
		return nil, nil
	}
	if w.writers.source != nil {
		src = w.writers.source
	}
	if w.writers.kafka != nil {
		kw = w.writers.kafka
	}
	return src, kw
}

// actionKafkaReads returns the read-only views of w's Kafka reader client
// that the Kafka actions validate and preview through (both write modes): the
// group reset planner and the DLQ record reader. Without a real reader client
// (no CDC set, or a Wired built by hand over a fake) both are nil interfaces
// and the actions say no Kafka client is configured.
func actionKafkaReads(w *Wired) (clients.GroupResetPlanner, actions.DLQRecordReader) {
	kc, ok := w.Kafka.(*kafka.Client)
	if !ok || kc == nil {
		return nil, nil
	}
	return kafka.NewPlanner(kc), kafka.NewReplayReader(kc)
}

// registerP3Actions registers kafka_reset_consumer_offsets, dlq_replay,
// connect_snapshot_table, pg_drop_replication_slot, pg_add_publication_table
// and pg_snapshot_signal over w's clients. Every one reads the connector
// config through the read-only Connect adapter in both modes: none needs a
// Connect write. connect_snapshot_table gets the Kafka writer only (the real
// *kafka.Writer has SendSnapshotSignal), never a source writer: a signal
// table row is pg_snapshot_signal's, approved by the postgres owner (#620).
func registerP3Actions(areg *actions.Registry, w *Wired) error {
	sw, kw := actionWriters(w)
	planner, records := actionKafkaReads(w)
	// readConnect is nil (a nil interface) without a Connect client, and an
	// interface conversion keeps it nil.
	var connReader clients.ConnectReader = readConnect(w)

	if err := actions.RegisterKafka(areg, actions.KafkaDeps{Writer: kw, Planner: planner, Records: records}); err != nil {
		return err
	}
	if err := actions.RegisterSnapshot(areg, actions.SnapshotDeps{Connect: connReader, Source: w.Source, Kafka: kw}); err != nil {
		return err
	}
	for _, a := range []actions.Action{
		pgactions.NewDropReplicationSlot(pgactions.DropSlotDeps{Source: w.Source, Connect: connReader, Writer: sw}),
		pgactions.NewAddPublicationTable(pgactions.AddPublicationTableDeps{Connect: connReader, Source: w.Source, Writer: sw}),
		pgactions.NewSnapshotSignal(pgactions.SnapshotSignalDeps{Connect: connReader, Source: w.Source, Writer: sw}),
	} {
		if err := areg.Register(a); err != nil {
			return fmt.Errorf("wiring: register %s: %w", a.Name(), err)
		}
	}
	return nil
}
