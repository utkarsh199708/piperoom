package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// A fake database/sql driver: each statement is answered by the first route
// whose substring it contains. It exercises database/sql's own conversions
// (driver values into the client's Scan targets), and records every
// statement with its bound arguments, so tests can check that values were
// bound and never spliced into the text.

type result struct {
	cols []string
	rows [][]driver.Value
	err  error
}

type route struct {
	substr string
	res    result
	fn     func(args []driver.NamedValue) result
}

type call struct {
	query string
	args  []any
}

type fakeServer struct {
	mu     sync.Mutex
	routes []route
	calls  []call
	// deadlines holds, per statement, the time left until its context's
	// deadline (-1 without one).
	deadlines []time.Duration
	ping      error
	// block, when set, makes every query wait for the context to end.
	block bool
}

func (f *fakeServer) on(substr string, cols []string, rows ...[]driver.Value) *fakeServer {
	f.routes = append(f.routes, route{substr: substr, res: result{cols: cols, rows: rows}})
	return f
}

func (f *fakeServer) onErr(substr string, err error) *fakeServer {
	f.routes = append(f.routes, route{substr: substr, res: result{err: err}})
	return f
}

func (f *fakeServer) onFunc(substr string, fn func(args []driver.NamedValue) result) *fakeServer {
	f.routes = append(f.routes, route{substr: substr, fn: fn})
	return f
}

func (f *fakeServer) queries() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *fakeServer) answer(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	f.mu.Lock()
	vals := make([]any, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	f.calls = append(f.calls, call{query: q, args: vals})
	left := time.Duration(-1)
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	f.deadlines = append(f.deadlines, left)
	routes := f.routes
	block := f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for _, r := range routes {
		if !strings.Contains(q, r.substr) {
			continue
		}
		res := r.res
		if r.fn != nil {
			res = r.fn(args)
		}
		if res.err != nil {
			return nil, res.err
		}
		return &fakeRows{cols: res.cols, rows: res.rows}, nil
	}
	return nil, fmt.Errorf("fake: no route for %q", q)
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

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("fake: prepare") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("fake: no transactions") }
func (c *fakeConn) Ping(context.Context) error          { return c.srv.ping }
func (c *fakeConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.srv.answer(ctx, q, args)
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

// newFake returns a DB over srv with the version probe answered as 8.4.3.
func newFake(t *testing.T, srv *fakeServer) *DB {
	t.Helper()
	return newFakeVersion(t, srv, "8.4.3")
}

func newFakeVersion(t *testing.T, srv *fakeServer, version string) *DB {
	t.Helper()
	srv.routes = append([]route{{substr: "SELECT VERSION()", res: result{
		cols: []string{"VERSION()", "@@version_comment", "@@session.transaction_read_only"},
		rows: [][]driver.Value{{[]byte(version), []byte("MySQL Community Server - GPL"), int64(1)}},
	}}}, srv.routes...)
	sdb := sql.OpenDB(fakeConnector{srv: srv})
	t.Cleanup(func() { _ = sdb.Close() })
	d, err := open(context.Background(), sdb, "mysql shop@db:3306")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return d
}

func row(vals ...driver.Value) []driver.Value { return vals }

func newTestLogger(w io.Writer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }

// onColumns answers Row's information_schema.COLUMNS lookup from tables
// ("db.table" in the server's case, mapped to its columns). Names match
// case-insensitively, as on a server with lower_case_table_names = 1, so the
// row carries the server's spelling, not the caller's.
func (f *fakeServer) onColumns(tables map[string][]string) *fakeServer {
	return f.onFunc("information_schema.COLUMNS", func(args []driver.NamedValue) result {
		cols := []string{"TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "matched"}
		if len(args) != 3 {
			return result{err: fmt.Errorf("fake: columns lookup with %d args", len(args))}
		}
		col, _ := args[0].Value.(string)
		db, _ := args[1].Value.(string)
		name, _ := args[2].Value.(string)
		for t, tcols := range tables {
			sdb, sname, _ := strings.Cut(t, ".")
			if !strings.EqualFold(sdb, db) || !strings.EqualFold(sname, name) || len(tcols) == 0 {
				continue
			}
			for _, c := range tcols {
				if strings.EqualFold(c, col) {
					return result{cols: cols, rows: [][]driver.Value{row([]byte(sdb), []byte(sname), []byte(c), int64(1))}}
				}
			}
			return result{cols: cols, rows: [][]driver.Value{row([]byte(sdb), []byte(sname), []byte(tcols[0]), int64(0))}}
		}
		return result{cols: cols}
	})
}
