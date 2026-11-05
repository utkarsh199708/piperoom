package sourceactions

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

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

// fakeSource is the read-only source client with every optional read the
// actions use. Its SourceDB methods are never called (nil embedded
// interface).
type fakeSource struct {
	clients.SourceDB

	tables   map[string]TableReplication
	role     RoleInfo
	sessions []DBSession
	rows     int64
	toast    []clients.ToastTable
	pub      postgres.PublicationAddState
	pubErr   error
	err      error // returned by every read when set

	calls       int
	noDeadline  bool // a read ran without a deadline
	lastOlder   time.Duration
	lastTables  []string
	unscoped    int    // calls of the unscoped Blockers
	scoped      int    // calls of BlockersOnTables
	sessionHook func() // runs on each Blockers call (to change sessions between calls)
}

func (s *fakeSource) note(ctx context.Context) {
	s.calls++
	if _, ok := ctx.Deadline(); !ok {
		s.noDeadline = true
	}
}

func (s *fakeSource) TableReplication(ctx context.Context, tables []string) ([]TableReplication, error) {
	s.note(ctx)
	s.lastTables = tables
	if s.err != nil {
		return nil, s.err
	}
	out := make([]TableReplication, 0, len(tables))
	for _, t := range tables {
		tr, ok := s.tables[t]
		if !ok {
			return nil, fmt.Errorf("table %s: %w", t, clients.ErrNotFound)
		}
		out = append(out, tr)
	}
	return out, nil
}

func (s *fakeSource) RoleInfo(ctx context.Context, role string, tables []string) (RoleInfo, error) {
	s.note(ctx)
	s.lastTables = tables
	if s.err != nil {
		return RoleInfo{}, s.err
	}
	return s.role, nil
}

// Blockers is the unscoped read: like the real client, every session comes
// back in full with InScope true.
func (s *fakeSource) Blockers(ctx context.Context, olderThan time.Duration) ([]DBSession, error) {
	s.note(ctx)
	s.unscoped++
	s.lastOlder = olderThan
	if s.sessionHook != nil {
		s.sessionHook()
	}
	if s.err != nil {
		return nil, s.err
	}
	out := make([]DBSession, len(s.sessions))
	for i, x := range s.sessions {
		x.InScope = true
		out[i] = x
	}
	return out, nil
}

// BlockersOnTables is the scoped read: a session keeps the InScope the test
// gave it, and an out-of-scope one is anonymised like the real client does.
func (s *fakeSource) BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]DBSession, error) {
	s.note(ctx)
	s.scoped++
	s.lastOlder = olderThan
	s.lastTables = tables
	if s.sessionHook != nil {
		s.sessionHook()
	}
	if s.err != nil {
		return nil, s.err
	}
	out := make([]DBSession, len(s.sessions))
	for i, x := range s.sessions {
		if !x.InScope {
			x.User, x.App, x.Query = "", "", ""
		}
		out[i] = x
	}
	return out, nil
}

func (s *fakeSource) EstimatedRows(ctx context.Context, table string) (int64, error) {
	s.note(ctx)
	if s.err != nil {
		return 0, s.err
	}
	return s.rows, nil
}

func (s *fakeSource) ToastColumns(ctx context.Context, tables []string) ([]clients.ToastTable, bool, error) {
	s.note(ctx)
	if s.err != nil {
		return nil, false, s.err
	}
	return s.toast, false, nil
}

func (s *fakeSource) PublicationAddState(ctx context.Context, publication, table string, visible []string) (postgres.PublicationAddState, error) {
	s.note(ctx)
	if s.err != nil {
		return postgres.PublicationAddState{}, s.err
	}
	return s.pub, s.pubErr
}

// The fake must satisfy every read interface the actions assert.
var (
	_ TableReplicationReader    = (*fakeSource)(nil)
	_ RoleInfoReader            = (*fakeSource)(nil)
	_ BlockerReader             = (*fakeSource)(nil)
	_ ScopedBlockerReader       = (*fakeSource)(nil)
	_ rowEstimator              = (*fakeSource)(nil)
	_ clients.ToastColumnReader = (*fakeSource)(nil)
	_ PublicationStateReader    = (*fakeSource)(nil)
)

// adminCall is one recorded admin write.
type adminCall struct {
	method string
	args   []any
}

// fakeWriter records every admin write; it implements all four writer
// interfaces, like postgres.Writer.
type fakeWriter struct {
	err   error
	calls []adminCall
}

func (w *fakeWriter) SetReplicaIdentity(_ context.Context, table, mode, index string) error {
	w.calls = append(w.calls, adminCall{"SetReplicaIdentity", []any{table, mode, index}})
	return w.err
}

func (w *fakeWriter) GrantSelect(_ context.Context, role string, tables []string) error {
	w.calls = append(w.calls, adminCall{"GrantSelect", []any{role, tables}})
	return w.err
}

func (w *fakeWriter) CreateHeartbeatTable(_ context.Context, table string) error {
	w.calls = append(w.calls, adminCall{"CreateHeartbeatTable", []any{table}})
	return w.err
}

func (w *fakeWriter) EndSession(_ context.Context, pid int64, startedAt time.Time) error {
	w.calls = append(w.calls, adminCall{"EndSession", []any{pid, startedAt}})
	return w.err
}

const pgClass = "io.debezium.connector.postgresql.PostgresConnector"

func testPipeline() owners.Pipeline {
	return owners.Pipeline{
		ID:           "orders",
		TopicPrefix:  "pg",
		Connectors:   []string{"pg-orders"},
		SourceTables: []string{"public.orders", "public.customers", "public.refunds"},
	}
}

func scopedCtx() context.Context {
	return tools.WithPipeline(context.Background(), testPipeline())
}

func newConnect() *fakeConnect {
	return &fakeConnect{cfg: map[string]map[string]string{
		"pg-orders": {"connector.class": pgClass, "database.user": "debezium", "publication.name": "dbz_publication"},
		"jdbc-sink": {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"},
	}}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
