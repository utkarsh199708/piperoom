package wiring

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/actions/pgactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
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

// P3ActionDeps are the clients the six P3 actions read and write through
// (RegisterP3Actions). Any field may be nil. The actions refuse on
// Writer == nil before any I/O, so RegisterP3Actions turns a field holding a
// typed nil (say a (*kafka.Writer)(nil)) into a nil interface first.
type P3ActionDeps struct {
	// Connect reads connector configs and status for every P3 action. It
	// should hold no Connect write: none of the six needs one.
	Connect clients.ConnectReader
	// Source is the read-only source Postgres (slots, publications, tables).
	Source clients.SourceDB
	// Planner plans kafka_reset_consumer_offsets and Records reads the DLQ
	// records dlq_replay replays. Both only read.
	Planner clients.GroupResetPlanner
	Records actions.DLQRecordReader
	// SourceWriter executes the pg_* actions; KafkaWriter executes the Kafka
	// actions and connect_snapshot_table. In wiring both are nil unless
	// write mode is on; a harness may pass writers that perform nothing.
	SourceWriter clients.SourceWriter
	KafkaWriter  clients.KafkaWriter
	// Sink is the read-only warehouse: pg_resnapshot_rows (X2a) counts the
	// sink side through it in Verify only. nil = unverifiable.
	Sink clients.Warehouse
}

// RegisterP3Actions registers kafka_reset_consumer_offsets, dlq_replay,
// connect_snapshot_table, pg_drop_replication_slot, pg_add_publication_table
// and pg_snapshot_signal over d, in that order, then the X2a recovery action
// pg_resnapshot_rows (both writers: its signal goes to the signal table or
// the Kafka signal topic, as the connector config says). It is the one place the P3
// actions are put together with their readers and writers: wiring calls it
// for each client set (registerP3Actions), and the evals harness can call it
// over its replay clients, so both register the same actions the same way.
// connect_snapshot_table gets the Kafka writer only (the real *kafka.Writer
// has SendSnapshotSignal), never a source writer: a signal table row is
// pg_snapshot_signal's, approved by the postgres owner (#620).
func RegisterP3Actions(areg *actions.Registry, d P3ActionDeps) error {
	// A typed nil would pass the actions' == nil checks and then panic or
	// fail oddly at Execute; as a nil interface the action refuses cleanly.
	if isNilIface(d.Connect) {
		d.Connect = nil
	}
	if isNilIface(d.Source) {
		d.Source = nil
	}
	if isNilIface(d.Planner) {
		d.Planner = nil
	}
	if isNilIface(d.Records) {
		d.Records = nil
	}
	if isNilIface(d.SourceWriter) {
		d.SourceWriter = nil
	}
	if isNilIface(d.KafkaWriter) {
		d.KafkaWriter = nil
	}
	if isNilIface(d.Sink) {
		d.Sink = nil
	}
	if err := actions.RegisterKafka(areg, actions.KafkaDeps{Writer: d.KafkaWriter, Planner: d.Planner, Records: d.Records}); err != nil {
		return fmt.Errorf("wiring: register P3 kafka actions: %w", err)
	}
	if err := actions.RegisterSnapshot(areg, actions.SnapshotDeps{Connect: d.Connect, Source: d.Source, Kafka: d.KafkaWriter}); err != nil {
		return fmt.Errorf("wiring: register connect_snapshot_table: %w", err)
	}
	for _, a := range []actions.Action{
		pgactions.NewDropReplicationSlot(pgactions.DropSlotDeps{Source: d.Source, Connect: d.Connect, Writer: d.SourceWriter}),
		pgactions.NewAddPublicationTable(pgactions.AddPublicationTableDeps{Connect: d.Connect, Source: d.Source, Writer: d.SourceWriter}),
		pgactions.NewSnapshotSignal(pgactions.SnapshotSignalDeps{Connect: d.Connect, Source: d.Source, Writer: d.SourceWriter}),
		pgactions.NewResnapshotRows(pgactions.ResnapshotRowsDeps{Connect: d.Connect, Source: d.Source, Sink: sinkView(d.Sink), Writer: d.SourceWriter, Kafka: d.KafkaWriter}),
	} {
		if err := areg.Register(a); err != nil {
			return fmt.Errorf("wiring: register %s: %w", a.Name(), err)
		}
	}
	return nil
}

// registerP3Actions registers the P3 actions over w's clients through
// RegisterP3Actions. Every one reads the connector config through the
// read-only Connect adapter in both write modes, and gets writers only when
// w was opened with write mode on (actionWriters).
func registerP3Actions(areg *actions.Registry, w *Wired) error {
	sw, kw := actionWriters(w)
	planner, records := actionKafkaReads(w)
	// readConnectBase is nil (a nil interface) without a Connect client, and an
	// interface conversion keeps it nil.
	var connReader clients.ConnectReader = readConnectBase(w)
	return RegisterP3Actions(areg, P3ActionDeps{
		Connect: connReader, Source: w.Source, Planner: planner, Records: records,
		SourceWriter: sw, KafkaWriter: kw, Sink: w.Warehouse,
	})
}

// isNilIface reports whether v is nil or an interface holding a nil value of
// a nilable kind (a typed nil pointer, map, slice, func or chan). A typed nil
// whose methods are safe on a nil receiver is reported nil too, on purpose:
// RegisterP3Actions then drops it, so an action refuses cleanly instead of
// relying on each method's nil-receiver handling.
func isNilIface(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return rv.IsNil()
	}
	return false
}

// sinkCounts is the sink view pg_resnapshot_rows' Verify gets (X2a): the
// warehouse's range and window counts and nothing else, the client in an
// unexported field, so the action never holds the whole warehouse client.
// A warehouse without a method answers clients.ErrNotSupported, which makes
// the recovery unverifiable rather than failed.
type sinkCounts struct{ wh clients.Warehouse }

var _ pgactions.SinkCounter = sinkCounts{}

// sinkView is nil for no warehouse, so Verify says so before any call.
func sinkView(wh clients.Warehouse) pgactions.SinkCounter {
	if wh == nil {
		return nil
	}
	return sinkCounts{wh: wh}
}

func (s sinkCounts) RangeColumn(ctx context.Context, table, column string) (postgres.RangeColumn, error) {
	if r, ok := s.wh.(pgactions.RowRangeCounter); ok {
		return r.RangeColumn(ctx, table, column)
	}
	return postgres.RangeColumn{}, fmt.Errorf("wiring: the warehouse client cannot read a column's kind: %w", clients.ErrNotSupported)
}

func (s sinkCounts) CountRange(ctx context.Context, table, column, from, to string) (int64, error) {
	if r, ok := s.wh.(pgactions.RowRangeCounter); ok {
		return r.CountRange(ctx, table, column, from, to)
	}
	return 0, fmt.Errorf("wiring: the warehouse client cannot count rows by range: %w", clients.ErrNotSupported)
}

func (s sinkCounts) CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error) {
	// The window count of clients.Reconciler alone: the view needs no other
	// method of it.
	if r, ok := s.wh.(interface {
		CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error)
	}); ok {
		return r.CountBetween(ctx, table, tsColumn, from, to)
	}
	return 0, fmt.Errorf("wiring: the warehouse client cannot count a time window: %w", clients.ErrNotSupported)
}
