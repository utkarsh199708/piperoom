package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// A minimal fake database/sql driver: each statement is answered by the first
// route whose substring it contains, and every statement is recorded with its
// bound arguments, so tests can check that values were bound and never spliced
// into the text.

type fakeCall struct {
	query string
	args  []any
	// left is the time to the context's deadline (-1 without one).
	left time.Duration
}

type fakeRoute struct {
	substr string
	cols   []string
	rows   [][]driver.Value
	err    error
}

type fakeServer struct {
	mu     sync.Mutex
	routes []fakeRoute
	calls  []fakeCall
	// block makes every statement wait for its context to end.
	block bool
}

func (f *fakeServer) on(substr string, cols []string, rows ...[]driver.Value) *fakeServer {
	f.routes = append(f.routes, fakeRoute{substr: substr, cols: cols, rows: rows})
	return f
}

func (f *fakeServer) onErr(substr string, err error) *fakeServer {
	f.routes = append(f.routes, fakeRoute{substr: substr, err: err})
	return f
}

func (f *fakeServer) recorded() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

// texts returns the recorded statement texts.
func (f *fakeServer) texts() []string {
	var out []string
	for _, c := range f.recorded() {
		out = append(out, c.query)
	}
	return out
}

func (f *fakeServer) route(ctx context.Context, q string, args []driver.NamedValue) (fakeRoute, error) {
	f.mu.Lock()
	vals := make([]any, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	left := time.Duration(-1)
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	f.calls = append(f.calls, fakeCall{query: q, args: vals, left: left})
	routes, block := f.routes, f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return fakeRoute{}, ctx.Err()
	}
	for _, r := range routes {
		if strings.Contains(q, r.substr) {
			return r, r.err
		}
	}
	return fakeRoute{}, nil // an unrouted statement succeeds, one row affected
}

func (f *fakeServer) open(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(fakeConnector{srv: f})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type fakeConnector struct{ srv *fakeServer }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{srv: c.srv}, nil
}
func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake: use the connector")
}

type fakeConn struct{ srv *fakeServer }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("fake: no prepare") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("fake: no tx") }

func (c *fakeConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if _, err := c.srv.route(ctx, q, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	r, err := c.srv.route(ctx, q, args)
	if err != nil {
		return nil, err
	}
	return &fakeRows{cols: r.cols, rows: r.rows}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

// writableRoute answers the writable check as a primary.
func writableRoute(f *fakeServer) *fakeServer {
	return f.on("@@session.transaction_read_only", []string{"a", "b", "c"}, []driver.Value{int64(0), int64(0), int64(0)})
}

// memStore is a SecretStore that keeps values in memory.
type memStore struct {
	values map[string]string
	err    error
}

func (m *memStore) Store(_ context.Context, name, value string) error {
	if m.err != nil {
		return m.err
	}
	if m.values == nil {
		m.values = map[string]string{}
	}
	m.values[name] = value
	return nil
}

var errBoom = fmt.Errorf("boom")
