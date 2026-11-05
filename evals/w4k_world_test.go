package evals

// The stack the dead-slot-wal-no-context and slot-inactive-wal-growth
// recordings of W4k describe, as fake clients under the real connect, dbtools
// and logtools code (the method of evals/gen: a recording is what the
// production tool returns, not a hand-written imitation). Used by
// w4k_recordings_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
)

// w4kWorld is one scenario's stack at one moment: a source connector, the
// replication slot as two successive reads (pg_streaming_liveness samples the
// slot twice), and the connector's stored offsets.
type w4kWorld struct {
	status    clients.ConnectorStatus
	config    map[string]string
	offsets   []connect.SourceOffset
	slotReads [][]clients.ReplicationSlot
	// open, when set, is what the source answers to the open-transaction read
	// (a w4kOpenSource). Left nil, the source cannot do that read at all, and the
	// tools fall back to the output shape they had before it existed.
	open *postgres.OpenTransactions
}

// w4kConnect is the Connect client: status, config and the KIP-875 offsets read.
type w4kConnect struct {
	clients.ConnectClient
	w *w4kWorld
}

func (c w4kConnect) ListConnectors(context.Context) ([]string, error) {
	return []string{c.w.status.Name}, nil
}

func (c w4kConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	if name != c.w.status.Name {
		return clients.ConnectorStatus{}, fmt.Errorf("connect: GET /connectors/%s/status: %w", name, clients.ErrNotFound)
	}
	return c.w.status, nil
}

func (c w4kConnect) Config(_ context.Context, name string) (map[string]string, error) {
	if name != c.w.status.Name {
		return nil, fmt.Errorf("connect: GET /connectors/%s/config: %w", name, clients.ErrNotFound)
	}
	out := map[string]string{}
	for k, v := range c.w.config {
		out[k] = v
	}
	return out, nil
}

// SourceOffsets implements connect.SourceOffsetReader.
func (c w4kConnect) SourceOffsets(_ context.Context, name string) ([]connect.SourceOffset, error) {
	if name != c.w.status.Name {
		return nil, fmt.Errorf("connect: GET /connectors/%s/offsets: %w", name, clients.ErrNotFound)
	}
	return c.w.offsets, nil
}

var _ connect.SourceOffsetReader = w4kConnect{}

// w4kSource is the source Postgres: the slot reads in order (the last one
// repeats), no open transaction and no blocking session.
type w4kSource struct {
	clients.SourceDB
	w     *w4kWorld
	calls *int
}

func (s w4kSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	i := *s.calls
	*s.calls++
	if i >= len(s.w.slotReads) {
		i = len(s.w.slotReads) - 1
	}
	return append([]clients.ReplicationSlot(nil), s.w.slotReads[i]...), nil
}

func (s w4kSource) LongTransactionsOnTables(context.Context, time.Duration, []string) (postgres.TableTransactions, error) {
	return postgres.TableTransactions{}, nil
}

// The settings reader source_blocking_sessions needs; only the blockers are read.
func (s w4kSource) Settings(context.Context) ([]postgres.DBSetting, error) { return nil, nil }
func (s w4kSource) TableReplication(context.Context, []string) ([]postgres.TableReplication, error) {
	return nil, nil
}
func (s w4kSource) RoleInfo(context.Context, string, []string) (postgres.RoleInfo, error) {
	return postgres.RoleInfo{}, nil
}
func (s w4kSource) Blockers(context.Context, time.Duration) ([]postgres.DBSession, error) {
	return nil, nil
}
func (s w4kSource) BlockersOnTables(context.Context, time.Duration, []string) ([]postgres.DBSession, error) {
	return nil, nil
}

// w4kOpenSource is w4kSource plus the age-independent open-transaction read
// (dbtools' optional openTxnReader), which pg_long_transactions and
// pg_streaming_liveness use to name a young xid holder.
type w4kOpenSource struct{ w4kSource }

func (s w4kOpenSource) OpenTransactions(context.Context, []string, bool) (postgres.OpenTransactions, error) {
	return *s.w.open, nil
}

// w4kOwners is the orders pipeline as the scenarios declare it.
func w4kOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", Title: "Orders CDC", TopicPrefix: "pg",
		SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"},
		ConsumerGroups: []string{"order-sync"}, SinkTables: []string{"analytics.orders"}, SinkTSColumn: "updated_at",
	}}}
}

// w4kConfig is pg-orders-connector's config, as the orders scenarios record it.
func w4kConfig() map[string]string {
	return map[string]string{
		"connector.class":    "io.debezium.connector.postgresql.PostgresConnector",
		"database.dbname":    "orders",
		"database.hostname":  "postgres",
		"database.password":  "redacted-by-the-tool",
		"database.port":      "5432",
		"database.user":      "piperoom",
		"slot.name":          "debezium_orders",
		"table.include.list": "public.orders",
		"topic.prefix":       "pg",
	}
}

func w4kLSN(pos uint64) string { return fmt.Sprintf("%X/%X", pos>>32, pos&0xFFFFFFFF) }

func w4kLSNPos(s string) uint64 {
	var hi, lo uint64
	if _, err := fmt.Sscanf(s, "%X/%X", &hi, &lo); err != nil {
		panic("w4k: bad LSN " + s)
	}
	return hi<<32 | lo
}

// w4kOffset is the stored offset of a Debezium Postgres connector (partition
// {server: topic prefix}) that last flushed lsn in transaction tx at ts. The
// numbers are json.Number, as the Connect client decodes them.
func w4kOffset(lsn uint64, tx int64, ts time.Time) connect.SourceOffset {
	n := func(v int64) json.Number { return json.Number(fmt.Sprint(v)) }
	return connect.SourceOffset{
		Partition: map[string]any{"server": "pg"},
		Offset: map[string]any{
			"lsn_proc":    n(int64(lsn)),
			"messageType": "UPDATE",
			"lsn_commit":  n(int64(lsn)),
			"lsn":         n(int64(lsn)),
			"txId":        n(tx),
			"ts_usec":     n(ts.UnixMicro()),
		},
	}
}

func w4kSlot(active bool, restart, confirmed uint64, retained int64, walStatus string) clients.ReplicationSlot {
	return clients.ReplicationSlot{
		Name: "debezium_orders", Plugin: "pgoutput", Active: active,
		RestartLSN: w4kLSN(restart), ConfirmedFlushLSN: w4kLSN(confirmed),
		RetainedWALBytes: retained, WALStatus: walStatus,
	}
}

func w4kGiB(f float64) int64 { return int64(f * float64(int64(1)<<30)) }

// w4kDeadSlot is dead-slot-wal-no-context: the connector was stopped at
// 06:27:54Z on 2026-10-18, its slot is inactive at 1.4 GiB and the stored
// offset sits at the slot's confirmed_flush_lsn. devctl load keeps writing
// (1.4 GiB in 3.2 h is 130 kB/s: 652000 bytes in the 5 s window).
func w4kDeadSlot() *w4kWorld {
	restart, confirmed := w4kLSNPos("3A/F28394E6"), w4kLSNPos("3A/F28396DE")
	const retained, written = 1503238554, 652000
	return &w4kWorld{
		status: clients.ConnectorStatus{Name: "pg-orders-connector", Type: "source", State: "STOPPED", WorkerID: "connect:8083"},
		config: w4kConfig(),
		slotReads: [][]clients.ReplicationSlot{
			{w4kSlot(false, restart, confirmed, retained, "extended")},
			{w4kSlot(false, restart, confirmed, retained+written, "extended")},
		},
		offsets: []connect.SourceOffset{w4kOffset(confirmed, 7413, time.Date(2026, 10, 18, 6, 27, 54, 0, time.UTC))},
	}
}

// w4kSlotHead is the source's WAL position in slot-inactive-wal-growth.
const w4kSlotHead = "4F/6C0E91A8"

// w4kSlotInactive is slot-inactive-wal-growth before the resume: stopped at
// 14:04:56Z on 2026-10-10, the slot inactive at 48.6 GiB, the stored offset at
// the slot's confirmed_flush_lsn, and the source still writing (48.6 GiB in
// 19.6 h is 740 kB/s: 3698000 bytes in the 5 s window).
func w4kSlotInactive() *w4kWorld {
	retained := w4kGiB(48.6)
	restart := w4kLSNPos(w4kSlotHead) - uint64(retained)
	confirmed := restart + 0x1F8
	const written = 3698000
	return &w4kWorld{
		status: clients.ConnectorStatus{Name: "pg-orders-connector", Type: "source", State: "STOPPED", WorkerID: "connect:8083"},
		config: w4kConfig(),
		slotReads: [][]clients.ReplicationSlot{
			{w4kSlot(false, restart, confirmed, retained, "extended")},
			{w4kSlot(false, restart, confirmed, retained+written, "extended")},
		},
		offsets: []connect.SourceOffset{w4kOffset(confirmed, 5634108, time.Date(2026, 10, 10, 14, 4, 56, 0, time.UTC))},
	}
}

// w4kSlotRecovered is slot-inactive-wal-growth after the resume: the task
// streams the backlog, so the slot is active at 44.1 GiB and its confirmed
// position (and the stored offset behind it) moves 6 MiB in the window while
// the source writes another 3698000 bytes.
func w4kSlotRecovered() *w4kWorld {
	restart := w4kLSNPos(w4kSlotHead) - uint64(w4kGiB(48.6)) + 5*1024*1024*1024
	confirmed := restart + 0x1F8
	retained := w4kGiB(44.1)
	const advance, written = 6 * 1024 * 1024, 3698000
	return &w4kWorld{
		status: clients.ConnectorStatus{Name: "pg-orders-connector", Type: "source", State: "RUNNING", WorkerID: "connect:8083",
			Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING", WorkerID: "connect:8083"}}},
		config: w4kConfig(),
		slotReads: [][]clients.ReplicationSlot{
			{w4kSlot(true, restart, confirmed, retained, "extended")},
			{w4kSlot(true, restart+advance, confirmed+advance, retained-advance+written, "extended")},
		},
		offsets: []connect.SourceOffset{w4kOffset(confirmed+advance, 5634999, time.Date(2026, 10, 11, 9, 39, 58, 0, time.UTC))},
	}
}

// w4kHorizonHeld is long-transaction-horizon-held: the connector streams (the
// slot is active and confirmed_flush_lsn moves 6 MiB in the window), but
// restart_lsn does not move while the source writes 740 kB/s, because pid 51877
// has held an xid for 4 min 50 s ("refund-backfill", idle in transaction). That
// is under the 5 min default of pg_long_transactions, so the long list is
// empty and only oldest_open names it.
func w4kHorizonHeld() *w4kWorld {
	head := w4kLSNPos("52/9A3C41D8")
	const written, retained, advance = 3698000, 214600000, 6 * 1024 * 1024
	restart := head - retained
	confirmed := head - 4096
	holder := &postgres.OpenTransaction{
		PID: 51877, OnTables: true, SameDatabase: true, ApplicationName: "refund-backfill",
		State: "idle in transaction", AgeSeconds: 290, HasXID: true, XIDAge: 3120, WaitEventType: "Client",
	}
	return &w4kWorld{
		status: clients.ConnectorStatus{Name: "pg-orders-connector", Type: "source", State: "RUNNING", WorkerID: "connect:8083",
			Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING", WorkerID: "connect:8083"}}},
		config: w4kConfig(),
		slotReads: [][]clients.ReplicationSlot{
			{w4kSlot(true, restart, confirmed, retained, "reserved")},
			{w4kSlot(true, restart, confirmed+advance, retained+written, "reserved")},
		},
		offsets: []connect.SourceOffset{w4kOffset(confirmed+advance, 6120344, time.Date(2026, 10, 24, 11, 34, 58, 0, time.UTC))},
		open:    &postgres.OpenTransactions{Open: 1, Writers: 1, Oldest: holder, OldestWriter: holder},
	}
}
