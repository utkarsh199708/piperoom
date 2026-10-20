package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// ---- fakes ----

// assign copies v into dest for the pointer kinds the client scans.
func assign(dest any, v any) error {
	switch d := dest.(type) {
	case *bool:
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("fake: want bool, got %T", v)
		}
		*d = b
	case *string:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("fake: want string, got %T", v)
		}
		*d = s
	case *int64:
		switch n := v.(type) {
		case int64:
			*d = n
		case int:
			*d = int64(n)
		default:
			return fmt.Errorf("fake: want int64, got %T", v)
		}
	default:
		return fmt.Errorf("fake: unsupported dest %T", dest)
	}
	return nil
}

type fakeRows struct {
	vals [][]any
	pos  int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.pos >= len(r.vals) {
		return false
	}
	r.pos++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.vals[r.pos-1]
	for i, d := range dest {
		if i >= len(row) {
			return fmt.Errorf("fake: too few values")
		}
		if err := assign(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRows) Err() error   { return r.err }
func (r *fakeRows) Close() error { return nil }

type fakeRow struct {
	vals []any
	err  error
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		if i >= len(r.vals) {
			return fmt.Errorf("fake: too few values")
		}
		if err := assign(d, r.vals[i]); err != nil {
			return err
		}
	}
	return nil
}

// fakeDB answers every statement from its queues and records them. rows is
// consumed FIFO by Query; qrows is consumed FIFO by QueryRow (an empty queue
// means ErrNoRows); err, when set, is returned for every call.
type fakeDB struct {
	rows  []*fakeRows
	qrows []*fakeRow
	err   error

	sqls []string
	args [][]any
	ctxs []context.Context
}

func (f *fakeDB) Query(ctx context.Context, q string, args ...any) (rows, error) {
	f.sqls, f.args = append(f.sqls, q), append(f.args, args)
	f.ctxs = append(f.ctxs, ctx)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.rows) > 0 {
		r := f.rows[0]
		f.rows = f.rows[1:]
		return r, nil
	}
	return &fakeRows{}, nil
}

func (f *fakeDB) QueryRow(ctx context.Context, q string, args ...any) scanner {
	f.sqls, f.args = append(f.sqls, q), append(f.args, args)
	f.ctxs = append(f.ctxs, ctx)
	if f.err != nil {
		return &fakeRow{err: f.err}
	}
	if len(f.qrows) > 0 {
		r := f.qrows[0]
		f.qrows = f.qrows[1:]
		return r
	}
	return &fakeRow{err: sql.ErrNoRows}
}

func (f *fakeDB) Ping(context.Context) error { return f.err }
func (f *fakeDB) Close() error               { return nil }

func testClient(f *fakeDB) *Client { return &Client{db: f} }

// lastSQL returns the last recorded statement with leading/trailing space gone.
func (f *fakeDB) lastSQL() string {
	if len(f.sqls) == 0 {
		return ""
	}
	return strings.TrimSpace(f.sqls[len(f.sqls)-1])
}

// ---- tests ----

func TestQuoteIdent(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		ok   bool
	}{
		{"dbo", "[dbo]", true},
		{"orders", "[orders]", true},
		{"_x1", "[_x1]", true},
		{"", "", false},
		{"1abc", "", false},
		{"has space", "", false},
		{"a-b", "", false},
		{"a]b", "", false},
		{"a;DROP TABLE x", "", false},
	} {
		got, err := quoteIdent(tc.in)
		if tc.ok {
			if err != nil || got != tc.want {
				t.Errorf("quoteIdent(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
			}
		} else if err == nil {
			t.Errorf("quoteIdent(%q): want error, got %q", tc.in, got)
		}
	}
}

func TestSplitTable(t *testing.T) {
	for _, tc := range []struct {
		in          string
		schema, tbl string
		ok          bool
	}{
		{"dbo.orders", "dbo", "orders", true},
		{"orders", "", "", false},
		{"a.b.c", "", "", false},
		{"dbo.1x", "", "", false},
		{"1x.orders", "", "", false},
		{"dbo.orders; drop", "", "", false},
	} {
		schema, tbl, err := splitTable(tc.in)
		if tc.ok {
			if err != nil || schema != tc.schema || tbl != tc.tbl {
				t.Errorf("splitTable(%q) = %q,%q,%v; want %q,%q", tc.in, schema, tbl, err, tc.schema, tc.tbl)
			}
		} else if err == nil {
			t.Errorf("splitTable(%q): want error", tc.in)
		}
	}
}

func TestServerInfo(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{{vals: []any{"16.0.1000.6", "Enterprise Edition", "SP2"}}}}
	c := testClient(f)
	info, err := c.ServerInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != "16.0.1000.6" || info.Edition != "Enterprise Edition" || info.ProductLevel != "SP2" {
		t.Fatalf("info = %+v", info)
	}
	if !strings.HasPrefix(f.lastSQL(), "SELECT") {
		t.Fatalf("not a SELECT: %q", f.lastSQL())
	}
}

func TestAgentState(t *testing.T) {
	t.Run("running", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{{vals: []any{"Running"}}}}
		c := testClient(f)
		st, err := c.AgentState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !st.Running || st.Status != "Running" {
			t.Fatalf("state = %+v", st)
		}
		if !strings.Contains(f.lastSQL(), "LIKE N'SQL Server Agent%'") {
			t.Fatalf("services query does not use the LIKE form: %q", f.lastSQL())
		}
	})
	t.Run("stopped", func(t *testing.T) {
		c := testClient(&fakeDB{qrows: []*fakeRow{{vals: []any{"Stopped"}}}})
		st, err := c.AgentState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Running || st.Status != "Stopped" {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("no service row and no sessions", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{
			{err: sql.ErrNoRows},    // sys.dm_server_services has no agent row
			{vals: []any{int64(0)}}, // sys.dm_exec_sessions has no agent session
		}}
		c := testClient(f)
		st, err := c.AgentState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Running || st.Status != "Unknown" {
			t.Fatalf("state = %+v", st)
		}
		if len(f.sqls) != 2 {
			t.Fatalf("ran %d queries, want the session fallback", len(f.sqls))
		}
	})
	t.Run("no service row but agent sessions", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{
			{err: sql.ErrNoRows},
			{vals: []any{int64(3)}},
		}}
		c := testClient(f)
		st, err := c.AgentState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !st.Running || st.Status != "Running" {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("permission error", func(t *testing.T) {
		c := testClient(&fakeDB{err: errors.New("VIEW SERVER STATE denied")})
		if _, err := c.AgentState(context.Background()); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestDatabaseCDC(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		c := testClient(&fakeDB{qrows: []*fakeRow{{vals: []any{true}}}})
		on, err := c.DatabaseCDC(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		if !on {
			t.Fatal("want enabled")
		}
	})
	t.Run("missing database", func(t *testing.T) {
		c := testClient(&fakeDB{}) // ErrNoRows
		_, err := c.DatabaseCDC(context.Background(), "nope")
		if !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("invalid name does not query", func(t *testing.T) {
		f := &fakeDB{}
		c := testClient(f)
		if _, err := c.DatabaseCDC(context.Background(), "orders; DROP TABLE x"); err == nil {
			t.Fatal("want error")
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d queries for bad input", len(f.sqls))
		}
	})
}

func TestTableCDC(t *testing.T) {
	t.Run("maps found and missing", func(t *testing.T) {
		f := &fakeDB{rows: []*fakeRows{{
			vals: [][]any{
				{"dbo", "orders", true, true},
				{"dbo", "customers", false, false},
			},
		}}}
		c := testClient(f)
		got, err := c.TableCDC(context.Background(), []string{"dbo.orders", "dbo.customers", "dbo.missing"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d rows: %+v", len(got), got)
		}
		if got[0].Table != "dbo.orders" || !got[0].Exists || !got[0].CDCEnabled || !got[0].HasPrimaryKey {
			t.Fatalf("orders = %+v", got[0])
		}
		if got[1].Table != "dbo.customers" || !got[1].Exists || got[1].CDCEnabled || got[1].HasPrimaryKey {
			t.Fatalf("customers = %+v", got[1])
		}
		if got[2].Table != "dbo.missing" || got[2].Exists || got[2].CDCEnabled || got[2].HasPrimaryKey {
			t.Fatalf("missing = %+v", got[2])
		}
		sql := f.lastSQL()
		if !strings.Contains(sql, "@p1") || !strings.Contains(sql, "@p6") {
			t.Fatalf("positional params missing: %q", sql)
		}
	})
	t.Run("empty list does not query", func(t *testing.T) {
		f := &fakeDB{}
		c := testClient(f)
		got, err := c.TableCDC(context.Background(), nil)
		if err != nil || got != nil {
			t.Fatalf("got %v, %v", got, err)
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d queries", len(f.sqls))
		}
	})
	t.Run("invalid name does not query", func(t *testing.T) {
		f := &fakeDB{}
		c := testClient(f)
		if _, err := c.TableCDC(context.Background(), []string{"dbo.orders; DROP"}); err == nil {
			t.Fatal("want error")
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d queries", len(f.sqls))
		}
	})
	t.Run("query error", func(t *testing.T) {
		c := testClient(&fakeDB{err: errors.New("boom")})
		if _, err := c.TableCDC(context.Background(), []string{"dbo.orders"}); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestRoleInfo(t *testing.T) {
	t.Run("full grants", func(t *testing.T) {
		f := &fakeDB{
			qrows: []*fakeRow{{vals: []any{int64(1), int64(0), int64(0), int64(1), int64(0)}}}, // exists, not disabled, not sysadmin, db_owner, not datareader
			rows:  []*fakeRows{{vals: [][]any{{"dbo", "orders"}}}},                             // direct SELECT on orders
		}
		c := testClient(f)
		info, err := c.RoleInfo(context.Background(), "piperoom_capture", []string{"dbo.orders", "dbo.other"})
		if err != nil {
			t.Fatal(err)
		}
		if !info.Exists || info.Disabled || info.Sysadmin || !info.DBOwner {
			t.Fatalf("info = %+v", info)
		}
		// db_owner implies SELECT on everything.
		if !info.Selectable["dbo.orders"] || !info.Selectable["dbo.other"] {
			t.Fatalf("selectable = %+v", info.Selectable)
		}
	})
	t.Run("db_datareader", func(t *testing.T) {
		f := &fakeDB{
			qrows: []*fakeRow{{vals: []any{int64(1), int64(0), int64(0), int64(0), int64(1)}}}, // datareader only
		}
		c := testClient(f)
		info, err := c.RoleInfo(context.Background(), "reader", []string{"dbo.orders", "dbo.other"})
		if err != nil {
			t.Fatal(err)
		}
		if !info.DataReader {
			t.Fatalf("info = %+v", info)
		}
		if !info.Selectable["dbo.orders"] || !info.Selectable["dbo.other"] {
			t.Fatalf("selectable = %+v", info.Selectable)
		}
	})
	t.Run("direct grant only", func(t *testing.T) {
		f := &fakeDB{
			qrows: []*fakeRow{{vals: []any{int64(1), int64(0), int64(0), int64(0), int64(0)}}}, // exists, no fixed roles
			rows:  []*fakeRows{{vals: [][]any{{"dbo", "orders"}}}},
		}
		c := testClient(f)
		info, err := c.RoleInfo(context.Background(), "reader", []string{"dbo.orders", "dbo.other"})
		if err != nil {
			t.Fatal(err)
		}
		if !info.Selectable["dbo.orders"] || info.Selectable["dbo.other"] {
			t.Fatalf("selectable = %+v", info.Selectable)
		}
	})
	t.Run("schema grant", func(t *testing.T) {
		f := &fakeDB{
			qrows: []*fakeRow{{vals: []any{int64(1), int64(0), int64(0), int64(0), int64(0)}}},
			rows:  []*fakeRows{{vals: [][]any{{"dbo", ""}}}}, // GRANT SELECT ON SCHEMA::dbo
		}
		c := testClient(f)
		info, err := c.RoleInfo(context.Background(), "reader", []string{"dbo.orders", "dbo.other", "sales.items"})
		if err != nil {
			t.Fatal(err)
		}
		if !info.Selectable["dbo.orders"] || !info.Selectable["dbo.other"] {
			t.Fatalf("schema grant should cover every dbo table: %+v", info.Selectable)
		}
		if info.Selectable["sales.items"] {
			t.Fatalf("schema grant leaked into sales: %+v", info.Selectable)
		}
	})
	t.Run("no login", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{{vals: []any{int64(0), int64(0), int64(0), int64(0), int64(0)}}}}
		c := testClient(f)
		info, err := c.RoleInfo(context.Background(), "ghost", []string{"dbo.orders"})
		if err != nil {
			t.Fatal(err)
		}
		if info.Exists {
			t.Fatalf("info = %+v", info)
		}
		if len(f.sqls) != 1 {
			t.Fatalf("ran %d queries; a missing login should not run the grant query", len(f.sqls))
		}
	})
	t.Run("empty tables", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{{vals: []any{int64(1), int64(0), int64(1), int64(0), int64(0)}}}}
		c := testClient(f)
		info, err := c.RoleInfo(context.Background(), "sa", nil)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Exists || !info.Sysadmin {
			t.Fatalf("info = %+v", info)
		}
		if len(f.sqls) != 1 {
			t.Fatalf("ran %d queries; no grant query for an empty table list", len(f.sqls))
		}
	})
	t.Run("invalid table name", func(t *testing.T) {
		f := &fakeDB{}
		c := testClient(f)
		if _, err := c.RoleInfo(context.Background(), "x", []string{"dbo.orders; DROP"}); err == nil {
			t.Fatal("want error")
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d queries", len(f.sqls))
		}
	})
	t.Run("info query error", func(t *testing.T) {
		c := testClient(&fakeDB{err: errors.New("boom")})
		if _, err := c.RoleInfo(context.Background(), "x", []string{"dbo.orders"}); err == nil {
			t.Fatal("want error")
		}
	})
}

// TestAllQueriesAreSelects pins the read-only guarantee at the SQL level: no
// method may ever emit a statement that writes.
func TestAllQueriesAreSelects(t *testing.T) {
	f := &fakeDB{
		qrows: []*fakeRow{
			{vals: []any{"16.0.0.0", "Developer", ""}},                      // ServerInfo
			{vals: []any{"Running"}},                                        // AgentState
			{vals: []any{true}},                                             // DatabaseCDC
			{vals: []any{int64(1), int64(0), int64(0), int64(0), int64(0)}}, // RoleInfo
		},
		rows: []*fakeRows{
			{vals: [][]any{{"dbo", "orders", true, true}}}, // TableCDC
			{vals: [][]any{{"dbo", "orders"}}},             // RoleInfo direct grants
		},
	}
	c := testClient(f)
	ctx := context.Background()
	if _, err := c.ServerInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AgentState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DatabaseCDC(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TableCDC(ctx, []string{"dbo.orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RoleInfo(ctx, "reader", []string{"dbo.orders"}); err != nil {
		t.Fatal(err)
	}
	for _, q := range f.sqls {
		trimmed := strings.ToUpper(strings.TrimSpace(q))
		if !strings.HasPrefix(trimmed, "SELECT") {
			t.Fatalf("non-SELECT statement issued: %q", q)
		}
		for _, bad := range []string{"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "EXEC", "MERGE", "CREATE"} {
			if strings.Contains(trimmed, bad) {
				t.Fatalf("statement contains %s: %q", bad, q)
			}
		}
	}
}

// TestCallsHaveDeadline checks every call applies the 10 s deadline.
func TestCallsHaveDeadline(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{{vals: []any{"16.0.0.0", "Developer", ""}}}}
	c := testClient(f)
	start := time.Now()
	if _, err := c.ServerInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.ctxs) != 1 {
		t.Fatalf("contexts = %d", len(f.ctxs))
	}
	dl, ok := f.ctxs[0].Deadline()
	if !ok {
		t.Fatal("no deadline on the call context")
	}
	left := time.Until(dl)
	if left <= 0 || left > 10*time.Second {
		t.Fatalf("deadline %s from now, want within 10s", left)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("test took too long")
	}
}
