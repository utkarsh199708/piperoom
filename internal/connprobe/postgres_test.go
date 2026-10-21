package connprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// leak is text a server error carries; no Result may ever contain it.
const leak = "SERVERLEAK-relation-secret_table"

// pw is the distinctive password of the test DSNs; no Result may contain it.
const pw = "Zx9QvDistinctPw7"

const goodDSN = "postgres://piperoom_ro:" + pw + "@db.example.test:5432/orders?sslmode=disable"

// oneAddrResolver answers every name with one address.
type oneAddrResolver struct{ addr string }

func (r oneAddrResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr(r.addr)}, nil
}

// fakeDialer hands out an in-memory connection, or fails.
type fakeDialer struct {
	err   error
	dials []string
}

func (d *fakeDialer) DialContext(_ context.Context, _, addr string) (net.Conn, error) {
	d.dials = append(d.dials, addr)
	if d.err != nil {
		return nil, d.err
	}
	c, srv := net.Pipe()
	_ = srv.Close()
	return c, nil
}

func testGuard(addr string, d *fakeDialer) *Guard {
	return &Guard{AllowLoopback: true, AllowPrivate: true, Resolver: oneAddrResolver{addr}, Dialer: d}
}

// fakePG answers queries by SQL text; a missing handler is an error.
type fakePG struct {
	h      map[string]func(args []any, dest []any) error
	closed bool
	ran    []string
}

func (f *fakePG) queryRow(_ context.Context, sql string, args []any, dest ...any) error {
	f.ran = append(f.ran, sql)
	h, ok := f.h[sql]
	if !ok {
		return fmt.Errorf("fake: no handler: %w", &pgconn.PgError{Code: "XX000", Message: leak})
	}
	return h(args, dest)
}

func (f *fakePG) close() { f.closed = true }

func set(dest []any, vals ...any) error {
	for i, v := range vals {
		switch d := dest[i].(type) {
		case *string:
			*d = v.(string)
		case *bool:
			*d = v.(bool)
		case *int64:
			*d = v.(int64)
		default:
			return fmt.Errorf("fake: unsupported dest %T", d)
		}
	}
	return nil
}

func ret(vals ...any) func([]any, []any) error {
	return func(_ []any, dest []any) error { return set(dest, vals...) }
}

func fail(code string) func([]any, []any) error {
	return func([]any, []any) error {
		return fmt.Errorf("query: %w", &pgconn.PgError{Code: code, Message: leak})
	}
}

// goodHandlers is a healthy, correctly granted read-only role.
func goodHandlers() map[string]func([]any, []any) error {
	return map[string]func([]any, []any) error{
		sqlReadOnly: ret("on"),
		sqlTimeout:  ret("10s", int64(10000)),
		sqlSlots:    ret(int64(1)),
		sqlPubs:     ret(int64(1)),
		sqlSuper:    ret(false),
		sqlTableSel: ret(true, true),
		sqlColSel:   ret(true),
		sqlTableWr:  ret(false),
	}
}

func connectorFor(f *fakePG, err error) pgConnector {
	return func(context.Context, *pgx.ConnConfig) (pgSession, error) {
		if err != nil {
			return nil, err
		}
		return f, nil
	}
}

func target(dsn string, tables ...string) Target {
	return Target{
		Kind:   KindSourceDB,
		Config: config.Config{SourceDBURL: dsn, WarehouseDBURL: dsn},
		Pipelines: []owners.Pipeline{{
			ID: "orders", SourceTables: tables, SinkTables: []string{"analytics.orders"}, SinkTSColumn: "updated_at",
		}},
	}
}

func statuses(r Result) map[string]string {
	m := map[string]string{}
	for _, c := range r.Checks {
		m[c.ID] = c.Status
	}
	return m
}

func ids(r Result) []string {
	var out []string
	for _, c := range r.Checks {
		out = append(out, c.ID)
	}
	return out
}

func get(r Result, id string) Check {
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	return Check{}
}

// noLeaks asserts the marshalled result holds no secret and no server text.
func noLeaks(t *testing.T, r Result) {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, bad := range []string{pw, leak, goodDSN, "@db.example.test", "piperoom_ro:"} {
		if strings.Contains(s, bad) {
			t.Errorf("result contains %q: %s", bad, s)
		}
	}
}

func TestSourceDBAllOK(t *testing.T) {
	f := &fakePG{h: goodHandlers()}
	d := &fakeDialer{}
	r := probeSourceDBWith(context.Background(), target(goodDSN, "public.orders", "public.items"), testGuard("10.1.2.3", d), connectorFor(f, nil))
	want := []string{"config", "reach", "login", "read_only", "statement_timeout", "read_slots", "read_publications", "select_tables", "can_write"}
	if got := ids(r); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	for _, c := range r.Checks {
		if c.Status != StatusOK {
			t.Errorf("%s = %s (%s / %s), want ok", c.ID, c.Status, c.Problem, c.Fix)
		}
	}
	if got := get(r, "select_tables").Detail; got != "SELECT on 2 source tables" {
		t.Errorf("select_tables detail = %q", got)
	}
	if got := get(r, "read_only").Detail; got != "default_transaction_read_only = on" {
		t.Errorf("read_only detail = %q", got)
	}
	if !f.closed {
		t.Error("session not closed")
	}
	if len(d.dials) != 1 || d.dials[0] != "10.1.2.3:5432" {
		t.Errorf("reach dialed %v, want the checked address once", d.dials)
	}
	noLeaks(t, r)
}

func TestSourceDBChecks(t *testing.T) {
	tests := []struct {
		name    string
		mod     func(h map[string]func([]any, []any) error)
		tables  []string
		id      string
		status  string
		problem string // substring
		fix     string // substring
	}{
		{"read_only off", func(h map[string]func([]any, []any) error) { h[sqlReadOnly] = ret("off") }, nil,
			"read_only", StatusWarn, "default_transaction_read_only = off", "ALTER ROLE piperoom_ro SET default_transaction_read_only = on;"},
		{"read_only unreadable", func(h map[string]func([]any, []any) error) { h[sqlReadOnly] = fail("XX000") }, nil,
			"read_only", StatusFail, "cannot read", ""},
		{"timeout unset", func(h map[string]func([]any, []any) error) { h[sqlTimeout] = ret("0", int64(0)) }, nil,
			"statement_timeout", StatusWarn, "no statement_timeout", "ALTER ROLE piperoom_ro SET statement_timeout = '10s';"},
		{"timeout unreadable", func(h map[string]func([]any, []any) error) { h[sqlTimeout] = fail("XX000") }, nil,
			"statement_timeout", StatusFail, "cannot read", ""},
		{"slots denied", func(h map[string]func([]any, []any) error) { h[sqlSlots] = fail("42501") }, nil,
			"read_slots", StatusFail, "cannot read pg_replication_slots", "GRANT SELECT ON pg_catalog.pg_replication_slots TO piperoom_ro;"},
		{"slots timeout", func(h map[string]func([]any, []any) error) {
			h[sqlSlots] = func([]any, []any) error { return fmt.Errorf("q: %w", context.DeadlineExceeded) }
		}, nil, "read_slots", StatusFail, "timed out", "answers queries"},
		{"publications denied", func(h map[string]func([]any, []any) error) { h[sqlPubs] = fail("42501") }, nil,
			"read_publications", StatusFail, "cannot read pg_publication_tables", "GRANT SELECT ON pg_catalog.pg_publication_tables TO piperoom_ro;"},
		{"select denied", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = ret(true, false) }, []string{"public.orders"},
			"select_tables", StatusFail, "cannot SELECT public.orders", "GRANT SELECT ON public.orders TO piperoom_ro;"},
		{"select no schema usage", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = ret(false, true) }, []string{"public.orders"},
			"select_tables", StatusFail, "cannot SELECT public.orders", "GRANT USAGE ON SCHEMA public TO piperoom_ro;"},
		{"select permission error", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = fail("42501") }, []string{"public.orders"},
			"select_tables", StatusFail, "cannot SELECT public.orders", "GRANT SELECT ON public.orders TO piperoom_ro;"},
		{"select unknown table", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = fail("42P01") }, []string{"public.nope"},
			"select_tables", StatusFail, "public.nope does not exist", "owners.yaml"},
		{"select unknown schema", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = fail("3F000") }, []string{"nope.orders"},
			"select_tables", StatusFail, "nope.orders does not exist", "owners.yaml"},
		{"select bad name", nil, []string{"orders; DROP TABLE x"},
			"select_tables", StatusFail, "is not written schema.table", "owners.yaml"},
		{"select other error", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = fail("XX000") }, []string{"public.orders"},
			"select_tables", StatusFail, "cannot check SELECT on public.orders", ""},
		{"select no tables", nil, nil, "select_tables", StatusSkip, "", ""},
		{"superuser", func(h map[string]func([]any, []any) error) { h[sqlSuper] = ret(true) }, []string{"public.orders"},
			"can_write", StatusWarn, "is a superuser", "NOSUPERUSER"},
		{"can write table", func(h map[string]func([]any, []any) error) { h[sqlTableWr] = ret(true) }, []string{"public.orders"},
			"can_write", StatusWarn, "can change public.orders", "REVOKE INSERT, UPDATE, DELETE ON public.orders FROM piperoom_ro;"},
		{"can_write unknown table is not a write", func(h map[string]func([]any, []any) error) { h[sqlTableWr] = fail("42P01") }, []string{"public.orders"},
			"can_write", StatusOK, "", ""},
		{"roles unreadable", func(h map[string]func([]any, []any) error) { h[sqlSuper] = fail("XX000") }, []string{"public.orders"},
			"can_write", StatusFail, "cannot read the role's attributes", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := goodHandlers()
			if tc.mod != nil {
				tc.mod(h)
			}
			tables := tc.tables
			if tables == nil && tc.id != "select_tables" {
				tables = []string{"public.orders"}
			}
			f := &fakePG{h: h}
			r := probeSourceDBWith(context.Background(), target(goodDSN, tables...), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(f, nil))
			c := get(r, tc.id)
			if c.Status != tc.status {
				t.Fatalf("%s = %s (%s / %s), want %s", tc.id, c.Status, c.Problem, c.Fix, tc.status)
			}
			if !strings.Contains(c.Problem, tc.problem) {
				t.Errorf("problem %q lacks %q", c.Problem, tc.problem)
			}
			if !strings.Contains(c.Fix, tc.fix) {
				t.Errorf("fix %q lacks %q", c.Fix, tc.fix)
			}
			if tc.status != StatusOK && tc.status != StatusSkip && c.Problem == "" {
				t.Error("a non-ok check must say what is wrong")
			}
			noLeaks(t, r)
		})
	}
}

// A bad table name must never reach SQL, and valid ones travel quoted as parameters.
func TestSourceDBSelectArgsAreQuotedParameters(t *testing.T) {
	var got [][]any
	h := goodHandlers()
	h[sqlTableSel] = func(args []any, dest []any) error { got = append(got, args); return set(dest, true, true) }
	f := &fakePG{h: h}
	probeSourceDBWith(context.Background(), target(goodDSN, "public.Orders", "x.y; drop"), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(f, nil))
	if len(got) != 1 || got[0][0] != "public" || got[0][1] != `"public"."Orders"` {
		t.Fatalf("args = %v", got)
	}
	for _, q := range f.ran {
		if strings.Contains(q, "Orders") || strings.Contains(q, "drop") {
			t.Errorf("object name interpolated into SQL: %s", q)
		}
	}
}

func TestSourceDBConfigFailures(t *testing.T) {
	tests := []struct{ name, dsn, problem string }{
		{"empty", "", "no connection string"},
		{"unparsable", "postgres://u:" + pw + "@host:notaport/db", "cannot be parsed"},
		{"unix socket", "postgres://u:" + pw + "@/db?host=/var/run/postgresql", "does not name a TCP host"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakePG{h: goodHandlers()}
			d := &fakeDialer{}
			r := probeSourceDBWith(context.Background(), target(tc.dsn, "public.orders"), testGuard("10.1.2.3", d), connectorFor(f, nil))
			if c := get(r, "config"); c.Status != StatusFail || !strings.Contains(c.Problem, tc.problem) {
				t.Fatalf("config = %+v", c)
			}
			if !strings.Contains(get(r, "config").Fix, "SOURCE_DB_URL") && tc.name == "empty" {
				t.Errorf("fix should name SOURCE_DB_URL: %q", get(r, "config").Fix)
			}
			assertSkips(t, r, 1)
			if len(d.dials) != 0 || len(f.ran) != 0 {
				t.Errorf("nothing may run after a config failure: dials=%v ran=%v", d.dials, f.ran)
			}
			noLeaks(t, r)
		})
	}
}

// assertSkips checks the first n checks are real and the rest are skipped.
func assertSkips(t *testing.T, r Result, n int) {
	t.Helper()
	want := len(sourceDBChecks)
	if len(r.Checks) != want {
		t.Fatalf("checks = %v, want all %d ids", ids(r), want)
	}
	for i, c := range r.Checks {
		if c.ID != sourceDBChecks[i] {
			t.Errorf("check %d id = %s, want %s", i, c.ID, sourceDBChecks[i])
		}
		if i >= n && c.Status != StatusSkip {
			t.Errorf("%s = %s, want skip", c.ID, c.Status)
		}
		if i >= n && c.MS != 0 {
			t.Errorf("%s: a skipped check has ms %d", c.ID, c.MS)
		}
	}
}

func TestSourceDBReachFailures(t *testing.T) {
	timeout := fmt.Errorf("dial: %w", context.DeadlineExceeded)
	tests := []struct {
		name    string
		addr    string // what the resolver answers
		dialErr error
		problem string
	}{
		{"metadata address refused", "169.254.169.254", nil, "is not allowed"},
		{"connection refused", "10.1.2.3", errors.New("connection refused " + leak), "cannot open a TCP connection"},
		{"timeout", "10.1.2.3", timeout, "timed out"},
		{"guard error from dialer", "10.1.2.3", fmt.Errorf("x: %w", ErrBlockedAddress), "is not allowed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakePG{h: goodHandlers()}
			d := &fakeDialer{err: tc.dialErr}
			r := probeSourceDBWith(context.Background(), target(goodDSN, "public.orders"), testGuard(tc.addr, d), connectorFor(f, nil))
			c := get(r, "reach")
			if c.Status != StatusFail || !strings.Contains(c.Problem, tc.problem) {
				t.Fatalf("reach = %+v", c)
			}
			if c.Fix == "" {
				t.Error("reach failure needs a fix")
			}
			assertSkips(t, r, 2)
			if r2 := get(r, "login"); !strings.Contains(r2.Detail, "reach failed") {
				t.Errorf("login skip detail = %q", r2.Detail)
			}
			if tc.addr == "169.254.169.254" && len(d.dials) != 0 {
				t.Errorf("the metadata address was dialed: %v", d.dials)
			}
			noLeaks(t, r)
		})
	}
}

func TestSourceDBLoginFailuresSkipTheRest(t *testing.T) {
	pgErr := func(code string) error {
		return fmt.Errorf("connect: %w", &pgconn.PgError{Code: code, Message: leak + " password " + pw})
	}
	tests := []struct {
		name    string
		err     error
		problem string
	}{
		{"bad password", pgErr("28P01"), "password authentication failed for role piperoom_ro"},
		{"not allowed", pgErr("28000"), "not allowed to log in"},
		{"no database", pgErr("3D000"), "database orders does not exist"},
		{"too many connections", pgErr("53300"), "no free connection slots"},
		{"timeout", fmt.Errorf("c: %w", context.DeadlineExceeded), "timed out"},
		{"other", errors.New("tls: handshake failure " + leak), "cannot log in as role piperoom_ro"},
		{"blocked at dial time", fmt.Errorf("c: %w", ErrBlockedAddress), "address is not allowed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakePG{h: goodHandlers()}
			r := probeSourceDBWith(context.Background(), target(goodDSN, "public.orders"), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(f, tc.err))
			c := get(r, "login")
			if c.Status != StatusFail || !strings.Contains(c.Problem, tc.problem) {
				t.Fatalf("login = %+v", c)
			}
			if c.Fix == "" {
				t.Error("login failure needs a fix")
			}
			assertSkips(t, r, 3)
			if r.Checks[3].Detail != "not run: login failed" {
				t.Errorf("skip detail = %q", r.Checks[3].Detail)
			}
			if len(f.ran) != 0 {
				t.Error("no query may run after a failed login")
			}
			noLeaks(t, r)
		})
	}
}

// Run sets OK from the checks: a warning keeps it true, a failure clears it.
func TestRunOKAndWarnings(t *testing.T) {
	h := goodHandlers()
	h[sqlSuper] = ret(true) // can_write warn
	f := &fakePG{h: h}
	r := probeSourceDBWith(context.Background(), target(goodDSN, "public.orders"), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(f, nil))
	if statuses(r)["can_write"] != StatusWarn {
		t.Fatalf("can_write = %s", statuses(r)["can_write"])
	}
	// Through Run with the real probe: an empty DSN fails the config check, no network.
	rr := Run(context.Background(), target("", "public.orders"), &Guard{})
	if rr.OK || rr.Kind != KindSourceDB || len(rr.Checks) != len(sourceDBChecks) {
		t.Fatalf("Run = %+v", rr)
	}
	// Placeholder kind coverage is not ours; an unknown kind is one failed check.
	if bad := Run(context.Background(), Target{Kind: "nope"}, nil); bad.OK {
		t.Error("unknown kind must not be ok")
	}
}

// Every outcome, whatever the server says, stays free of the DSN, the
// password and server text.
func TestNoSecretsInAnyResult(t *testing.T) {
	servers := []error{
		fmt.Errorf("x: %w", &pgconn.PgError{Code: "28P01", Message: "password " + pw + " " + leak}),
		errors.New("failed to connect to `host=db user=piperoom_ro password=" + pw + "`: " + leak),
		fmt.Errorf("x: %w", &pgconn.PgError{Code: "42501", Message: leak, Detail: leak}),
	}
	for _, serr := range servers {
		h := map[string]func([]any, []any) error{}
		for k := range goodHandlers() {
			h[k] = func([]any, []any) error { return serr }
		}
		f := &fakePG{h: h}
		for _, connErr := range []error{nil, serr} {
			r := probeSourceDBWith(context.Background(), target(goodDSN, "public.orders", "bad name"), testGuard("10.1.2.3", &fakeDialer{err: nil}), connectorFor(f, connErr))
			noLeaks(t, r)
			w := probeWarehousePG(context.Background(), target(goodDSN), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(f, connErr))
			noLeaks(t, w)
		}
		d := &fakeDialer{err: serr}
		noLeaks(t, probeSourceDBWith(context.Background(), target(goodDSN, "public.orders"), testGuard("10.1.2.3", d), connectorFor(f, nil)))
	}
}

func TestSplitQualified(t *testing.T) {
	tests := []struct {
		in     string
		ok     bool
		schema string
		table  string
	}{
		{"public.orders", true, "public", "orders"},
		{"Public.Orders_1", true, "Public", "Orders_1"},
		{"orders", false, "", ""},
		{"a.b.c", false, "", ""},
		{"a.", false, "", ""},
		{"1a.b", false, "", ""},
		{`a."b`, false, "", ""},
		{"a.b; drop", false, "", ""},
	}
	for _, tc := range tests {
		s, tb, ok := splitQualified(tc.in)
		if ok != tc.ok || s != tc.schema || tb != tc.table {
			t.Errorf("splitQualified(%q) = %q %q %v", tc.in, s, tb, ok)
		}
	}
}

func TestRoleSQL(t *testing.T) {
	if got := roleSQL("piperoom_ro"); got != "piperoom_ro" {
		t.Errorf("plain role = %q", got)
	}
	if got := roleSQL(`Weird "Role"`); got != `"Weird ""Role"""` {
		t.Errorf("quoted role = %q", got)
	}
}

func TestWarehousePostgres(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		f := &fakePG{h: goodHandlers()}
		r := probeWarehouseWith(context.Background(), target(goodDSN), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(f, nil), nil)
		want := "config,reach,login,read_only,select_sink"
		if got := strings.Join(ids(r), ","); got != want {
			t.Fatalf("ids = %s", got)
		}
		for _, c := range r.Checks {
			if c.Status != StatusOK {
				t.Errorf("%s = %s (%s)", c.ID, c.Status, c.Problem)
			}
		}
		if d := get(r, "select_sink").Detail; d != "SELECT on 1 sink table and 1 timestamp column" {
			t.Errorf("detail = %q", d)
		}
		if !f.closed {
			t.Error("session not closed")
		}
		if r.Kind != KindWarehouse {
			t.Errorf("kind = %s", r.Kind)
		}
	})
	tests := []struct {
		name    string
		mod     func(h map[string]func([]any, []any) error)
		id      string
		status  string
		problem string
		fix     string
	}{
		{"read_only is only a warning", func(h map[string]func([]any, []any) error) { h[sqlReadOnly] = ret("off") },
			"read_only", StatusWarn, "default_transaction_read_only = off", "ALTER ROLE piperoom_ro SET"},
		{"sink table denied", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = ret(true, false) },
			"select_sink", StatusFail, "cannot SELECT analytics.orders", "GRANT SELECT ON analytics.orders TO piperoom_ro;"},
		{"sink table missing", func(h map[string]func([]any, []any) error) { h[sqlTableSel] = fail("42P01") },
			"select_sink", StatusFail, "analytics.orders does not exist", "sink_tables"},
		{"ts column missing", func(h map[string]func([]any, []any) error) { h[sqlColSel] = fail("42703") },
			"select_sink", StatusFail, "no such column: updated_at of analytics.orders", "sink_ts_column"},
		{"ts column denied", func(h map[string]func([]any, []any) error) { h[sqlColSel] = ret(false) },
			"select_sink", StatusFail, "cannot SELECT column updated_at", `GRANT SELECT ("updated_at") ON analytics.orders TO piperoom_ro;`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := goodHandlers()
			tc.mod(h)
			r := probeWarehouseWith(context.Background(), target(goodDSN), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(&fakePG{h: h}, nil), nil)
			c := get(r, tc.id)
			if c.Status != tc.status || !strings.Contains(c.Problem, tc.problem) || !strings.Contains(c.Fix, tc.fix) {
				t.Fatalf("%s = %+v", tc.id, c)
			}
			noLeaks(t, r)
		})
	}
	t.Run("login failure skips", func(t *testing.T) {
		r := probeWarehouseWith(context.Background(), target(goodDSN), testGuard("10.1.2.3", &fakeDialer{}),
			connectorFor(nil, fmt.Errorf("c: %w", &pgconn.PgError{Code: "28P01", Message: leak})), nil)
		if got := statuses(r); got["login"] != StatusFail || got["read_only"] != StatusSkip || got["select_sink"] != StatusSkip {
			t.Fatalf("statuses = %v", got)
		}
		if !strings.Contains(get(r, "login").Fix, "WAREHOUSE_DB_URL") {
			t.Errorf("fix should name WAREHOUSE_DB_URL: %q", get(r, "login").Fix)
		}
		noLeaks(t, r)
	})
	t.Run("config empty", func(t *testing.T) {
		r := probeWarehouseWith(context.Background(), target(""), testGuard("10.1.2.3", &fakeDialer{}), connectorFor(nil, nil), nil)
		if got := statuses(r); got["config"] != StatusFail || got["reach"] != StatusSkip || len(r.Checks) != 5 {
			t.Fatalf("statuses = %v", got)
		}
		if !strings.Contains(get(r, "config").Fix, "WAREHOUSE_DB_URL") {
			t.Errorf("fix should name WAREHOUSE_DB_URL")
		}
	})
	t.Run("no sink tables", func(t *testing.T) {
		tg := target(goodDSN)
		tg.Pipelines[0].SinkTables = nil
		r := probeWarehouseWith(context.Background(), tg, testGuard("10.1.2.3", &fakeDialer{}), connectorFor(&fakePG{h: goodHandlers()}, nil), nil)
		if get(r, "select_sink").Status != StatusSkip {
			t.Fatalf("select_sink = %+v", get(r, "select_sink"))
		}
	})
	t.Run("unknown kind", func(t *testing.T) {
		tg := target(goodDSN)
		tg.Config.WarehouseKind = "bigquery"
		r := probeWarehouseWith(context.Background(), tg, testGuard("10.1.2.3", &fakeDialer{}), connectorFor(nil, nil), nil)
		if get(r, "config").Status != StatusFail || len(r.Checks) != 5 {
			t.Fatalf("checks = %+v", r.Checks)
		}
	})
}
