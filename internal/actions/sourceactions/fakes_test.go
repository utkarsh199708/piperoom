package sourceactions

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const testConnector = "pg-orders"

// fakeConnect is a read-only Connect config reader.
type fakeConnect struct {
	cfg   map[string]map[string]string
	err   error
	calls int
}

func (c *fakeConnect) Config(_ context.Context, name string) (map[string]string, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	cfg, ok := c.cfg[name]
	if !ok {
		return nil, fmt.Errorf("connector %q: %w", name, clients.ErrNotFound)
	}
	return cfg, nil
}

// fakeSource is a read-only source client with the two Q2c reads. Its other
// clients.SourceDB methods are never called (nil embedded interface).
type fakeSource struct {
	clients.SourceDB
	st        postgres.PublicationAddState
	stErr     error
	ts        postgres.TableSize
	tsErr     error
	pubCalls  int
	sizeCalls int
	deadline  bool
	pub       string
	table     string
	visible   []string
}

func (s *fakeSource) PublicationAddState(ctx context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error) {
	s.pubCalls++
	s.pub, s.table, s.visible = publication, table, visible
	if _, ok := ctx.Deadline(); ok {
		s.deadline = true
	}
	return s.st, s.stErr
}

func (s *fakeSource) TableSize(ctx context.Context, table string) (postgres.TableSize, error) {
	s.sizeCalls++
	if _, ok := ctx.Deadline(); ok {
		s.deadline = true
	}
	return s.ts, s.tsErr
}

// noSizeSource is a source client without the TableSize read: the action must
// say the estimate is unavailable instead of inventing one.
type noSizeSource struct{ clients.SourceDB }

func (s noSizeSource) PublicationAddState(context.Context, string, string, []string) (postgres.PublicationAddState, error) {
	return postgres.PublicationAddState{TableExists: true, RelKind: "r", HasPrimaryKey: true, EstimatedRows: 10}, nil
}

// spyWriter fakes the Q2b admin writer and records every write method. A
// Preview that touches it shows up as a non-empty calls slice.
type spyWriter struct {
	postgres.SourceAdminWriter
	calls     []string
	table     string
	mode      string
	index     string
	heartbeat string
	err       error
}

func (w *spyWriter) SetReplicaIdentity(_ context.Context, table, mode, index string) error {
	w.calls = append(w.calls, "SetReplicaIdentity")
	w.table, w.mode, w.index = table, mode, index
	return w.err
}

func (w *spyWriter) GrantSelect(context.Context, string, []string) error {
	w.calls = append(w.calls, "GrantSelect")
	return w.err
}

func (w *spyWriter) CreateHeartbeatTable(_ context.Context, table string) error {
	w.calls = append(w.calls, "CreateHeartbeatTable")
	w.heartbeat = table
	return w.err
}

func (w *spyWriter) EndSession(context.Context, int64, time.Time) error {
	w.calls = append(w.calls, "EndSession")
	return w.err
}

// testPipeline is a two-table CDC pipeline.
func testPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders",
		Kind:         "cdc",
		TopicPrefix:  "pg",
		Connectors:   []string{testConnector},
		SourceTables: []string{"public.orders", "public.customers"},
	}
}

// scopedCtx is the room's pipeline scope.
func scopedCtx() context.Context {
	return tools.WithPipeline(context.Background(), testPipeline())
}

// pgConnectorConfig is a Debezium Postgres source config.
func pgConnectorConfig(extra map[string]string) map[string]string {
	cfg := map[string]string{
		"connector.class":             "io.debezium.connector.postgresql.PostgresConnector",
		"publication.name":            "dbz_publication",
		"publication.autocreate.mode": "filtered",
	}
	for k, v := range extra {
		cfg[k] = v
	}
	return cfg
}

// ordersState is a healthy ordinary table with a primary key.
func ordersState() postgres.PublicationAddState {
	return postgres.PublicationAddState{
		TableExists:     true,
		RelKind:         "r",
		ReplicaIdentity: "d",
		HasPrimaryKey:   true,
		InPublication:   true,
		EstimatedRows:   1000,
	}
}

// ordersSize is an 8 MiB heap with a 1 MiB index.
func ordersSize() postgres.TableSize {
	return postgres.TableSize{TableBytes: 8 << 20, IndexBytes: 1 << 20, TotalBytes: 9 << 20}
}

// newReplicaFixture builds the action over fakes.
func newReplicaFixture(cfg map[string]string, st postgres.PublicationAddState) (*fakeConnect, *fakeSource, *spyWriter, *setReplicaIdentity) {
	if cfg == nil {
		cfg = pgConnectorConfig(nil)
	}
	c := &fakeConnect{cfg: map[string]map[string]string{testConnector: cfg}}
	s := &fakeSource{st: st, ts: ordersSize()}
	w := &spyWriter{}
	a := NewSetReplicaIdentity(SetReplicaIdentityDeps{Connect: c, Source: s, Writer: w}).(*setReplicaIdentity)
	return c, s, w, a
}

// newHeartbeatFixture builds the action over fakes.
func newHeartbeatFixture(cfg map[string]string, st postgres.PublicationAddState) (*fakeConnect, *fakeSource, *spyWriter, *heartbeatTable) {
	if cfg == nil {
		cfg = pgConnectorConfig(nil)
	}
	c := &fakeConnect{cfg: map[string]map[string]string{testConnector: cfg}}
	s := &fakeSource{st: st}
	w := &spyWriter{}
	a := NewCreateHeartbeatTable(CreateHeartbeatTableDeps{Connect: c, Source: s, Writer: w}).(*heartbeatTable)
	return c, s, w, a
}
