package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// ---- helpers ----

// adminTableCtx is pipelineCtx (public.orders is a source table), the context
// the admin writer's scope gate accepts.
func adminTableCtx() context.Context { return pipelineCtx() }

// grantRow answers one EndSession terminate row.
func terminateRow(ok bool) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(*bool) = ok
		return nil
	}
}

// sessionRow answers adminSessionSQL. start nil stands for a NULL
// backend_start.
func sessionRow(start *time.Time, backendType, app, user string, self, sameRole bool) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(**time.Time) = start
		*dest[1].(*string) = backendType
		*dest[2].(*string) = app
		*dest[3].(*string) = user
		*dest[4].(*bool) = self
		*dest[5].(*bool) = sameRole
		return nil
	}
}

// endSessionTx dispatches each statement of EndSession to its own row; a
// statement it does not expect fails the test.
func endSessionTx(t *testing.T, read, terminate func(dest ...any) error) *fakeTx {
	t.Helper()
	return &fakeTx{scanSQL: func(sql string, dest ...any) error {
		switch sql {
		case adminSessionSQL:
			return read(dest...)
		case terminateSessionSQL:
			return terminate(dest...)
		}
		t.Fatalf("unexpected statement %q", sql)
		return nil
	}}
}

// ---- the local contract ----

func TestPiperoomApp(t *testing.T) {
	cases := map[string]bool{
		"":                 false,
		"psql":             false,
		"debezium":         false,
		"piperoom-reader":  true,
		"piperoom-writer":  true,
		"Piperoom-Writer":  true,
		"PIPEROOM":         true,
		"xpiperoom-reader": false,
	}
	for app, want := range cases {
		if got := piperoomApp(app); got != want {
			t.Errorf("piperoomApp(%q) = %v, want %v", app, got, want)
		}
	}
}

func TestAdminWriterNilIsNoWriter(t *testing.T) {
	for _, w := range []*Writer{nil, {}} {
		ctx := context.Background()
		start := time.Unix(1, 0)
		if err := w.SetReplicaIdentity(ctx, "public.orders", "full", ""); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("SetReplicaIdentity: %v", err)
		}
		if err := w.GrantSelect(ctx, "debezium", []string{"public.orders"}); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("GrantSelect: %v", err)
		}
		if err := w.CreateHeartbeatTable(ctx, "public.heartbeat"); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("CreateHeartbeatTable: %v", err)
		}
		if err := w.EndSession(ctx, 1, start); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("EndSession: %v", err)
		}
	}
}

// ---- SetReplicaIdentity ----

func TestSetReplicaIdentitySQL(t *testing.T) {
	cases := []struct {
		name  string
		table string
		mode  string
		index string
		want  string
	}{
		{"full", "public.orders", "full", "", `ALTER TABLE "public"."orders" REPLICA IDENTITY FULL`},
		{"default", "Sales.Items", "default", "", `ALTER TABLE "Sales"."Items" REPLICA IDENTITY DEFAULT`},
		{"index", "public.orders", "index", "orders_pkey", `ALTER TABLE "public"."orders" REPLICA IDENTITY USING INDEX "orders_pkey"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{}
			w, _ := testWriter(tx)
			if err := w.SetReplicaIdentity(unscopedCtx(), tc.table, tc.mode, tc.index); err != nil {
				t.Fatal(err)
			}
			checkTx(t, tx, true)
			if got := tx.sqls(); !reflect.DeepEqual(got, []string{tc.want}) {
				t.Fatalf("statements:\n got %q\nwant %q", got, []string{tc.want})
			}
			if len(tx.stmts[1].args) != 0 {
				t.Fatalf("the ALTER takes no parameters: %v", tx.stmts[1].args)
			}
		})
	}
}

func TestSetReplicaIdentityRefusesBadInput(t *testing.T) {
	cases := []struct {
		name  string
		table string
		mode  string
		index string
	}{
		{"empty table", "", "full", ""},
		{"table injection", `public.orders; DROP TABLE x; --`, "full", ""},
		{"quoted table", `public."orders"`, "full", ""},
		{"no schema", "orders", "full", ""},
		{"three parts", "a.b.c", "full", ""},
		{"empty mode", "public.orders", "", ""},
		{"unknown mode", "public.orders", "nothing", ""},
		{"mode case", "public.orders", "FULL", ""},
		{"full with index", "public.orders", "full", "orders_pkey"},
		{"default with index", "public.orders", "default", "orders_pkey"},
		{"index without name", "public.orders", "index", ""},
		{"index injection", "public.orders", "index", `x"; DROP TABLE y; --`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			err := w.SetReplicaIdentity(unscopedCtx(), tc.table, tc.mode, tc.index)
			if err == nil {
				t.Fatal("expected a refusal")
			}
			if !strings.HasPrefix(err.Error(), "postgres: ") {
				t.Fatalf("error %q lacks the package prefix", err)
			}
			if b.begins != 0 {
				t.Fatal("a refused call must not reach the database")
			}
		})
	}
}

func TestSetReplicaIdentityScope(t *testing.T) {
	w, b := testWriter(&fakeTx{})
	ctx := pipelineCtx()
	if err := w.SetReplicaIdentity(ctx, "public.payroll", "full", ""); !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "public.payroll") {
		t.Fatalf("out-of-scope table: %v", err)
	}
	if b.begins != 0 {
		t.Fatal("an out-of-scope call must not reach the database")
	}
	if err := w.SetReplicaIdentity(ctx, "public.orders", "full", ""); err != nil {
		t.Fatalf("in-scope table refused: %v", err)
	}
	if err := w.SetReplicaIdentity(context.Background(), "public.orders", "full", ""); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v, want tools.ErrNoScope", err)
	}
}

func TestSetReplicaIdentityErrors(t *testing.T) {
	cases := []struct {
		name     string
		execErr  func(string) error
		commitEr error
		want     error
		wantText string
	}{
		{name: "missing table", execErr: failOn("ALTER", pgErr("42P01")), want: clients.ErrNotFound},
		{name: "missing index", execErr: failOn("ALTER", pgErr("42704")), want: clients.ErrNotFound, wantText: "orders_pkey"},
		{name: "not the owner", execErr: failOn("ALTER", pgErr("42501")), wantText: "42501"},
		{name: "commit fails", commitEr: errors.New("connection lost"), wantText: "commit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{execErr: tc.execErr, commitErr: tc.commitEr}
			w, _ := testWriter(tx)
			err := w.SetReplicaIdentity(unscopedCtx(), "public.orders", "index", "orders_pkey")
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil && errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("error %v wraps ErrNotFound", err)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q lacks %q", err, tc.wantText)
			}
			if !strings.HasPrefix(err.Error(), "postgres: set replica identity of public.orders: ") {
				t.Fatalf("error %q lacks the method context", err)
			}
			if tc.commitEr == nil {
				checkTx(t, tx, false)
			}
		})
	}
}

// ---- GrantSelect ----

func TestGrantSelectSQL(t *testing.T) {
	tx := &fakeTx{}
	w, _ := testWriter(tx)
	if err := w.GrantSelect(unscopedCtx(), "debezium", []string{"public.orders", "Sales.Items", "public.orders"}); err != nil {
		t.Fatal(err)
	}
	checkTx(t, tx, true)
	want := `GRANT SELECT ON TABLE "public"."orders", "Sales"."Items" TO "debezium"`
	if got := tx.sqls(); !reflect.DeepEqual(got, []string{want}) {
		t.Fatalf("statements:\n got %q\nwant %q", got, []string{want})
	}
	if len(tx.stmts[1].args) != 0 {
		t.Fatalf("the GRANT takes no parameters: %v", tx.stmts[1].args)
	}
	for _, forbidden := range []string{"ALL", "GRANT OPTION", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "USAGE", "CREATE"} {
		if strings.Contains(want, forbidden) {
			t.Fatalf("the GRANT carries %q", forbidden)
		}
	}
}

func TestGrantSelectRefusesBadInput(t *testing.T) {
	many := make([]string, maxWriteTables+1)
	for i := range many {
		many[i] = fmt.Sprintf("public.t%d", i)
	}
	longRole := strings.Repeat("r", maxAdminRoleBytes+1)
	cases := []struct {
		name   string
		role   string
		tables []string
	}{
		{"empty role", "", []string{"public.orders"}},
		{"role injection", `d"; DROP ROLE x; --`, []string{"public.orders"}},
		{"role with dash", "debezium-user", []string{"public.orders"}},
		{"role starts with a digit", "9role", []string{"public.orders"}},
		{"role too long", longRole, []string{"public.orders"}},
		{"role public", "public", []string{"public.orders"}},
		{"role PUBLIC", "PUBLIC", []string{"public.orders"}},
		{"role current_user", "current_user", []string{"public.orders"}},
		{"role Current_User", "Current_User", []string{"public.orders"}},
		{"role session_user", "session_user", []string{"public.orders"}},
		{"predefined role", "pg_monitor", []string{"public.orders"}},
		{"predefined role read all", "pg_read_all_data", []string{"public.orders"}},
		{"predefined role mixed case", "PG_Monitor", []string{"public.orders"}},
		{"no tables", "debezium", nil},
		{"no schema", "debezium", []string{"orders"}},
		{"table injection", "debezium", []string{"public.orders; DROP TABLE x"}},
		{"quoted table", "debezium", []string{`public."orders"`}},
		{"too many tables", "debezium", many},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			if err := w.GrantSelect(unscopedCtx(), tc.role, tc.tables); err == nil {
				t.Fatal("expected a refusal")
			}
			if b.begins != 0 {
				t.Fatal("a refused call must not reach the database")
			}
		})
	}
}

func TestGrantSelectScope(t *testing.T) {
	w, b := testWriter(&fakeTx{})
	ctx := pipelineCtx()
	err := w.GrantSelect(ctx, "debezium", []string{"public.orders", "public.payroll"})
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "public.payroll") {
		t.Fatalf("out-of-scope table: %v", err)
	}
	if b.begins != 0 {
		t.Fatal("an out-of-scope call must not reach the database")
	}
	if err := w.GrantSelect(ctx, "debezium", []string{"public.orders"}); err != nil {
		t.Fatalf("in-scope table refused: %v", err)
	}
	if err := w.GrantSelect(context.Background(), "debezium", []string{"public.orders"}); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v, want tools.ErrNoScope", err)
	}
	if b.begins != 1 {
		t.Fatalf("begins = %d, want only the in-scope call", b.begins)
	}
}

func TestGrantSelectErrors(t *testing.T) {
	cases := []struct {
		name     string
		execErr  func(string) error
		want     error
		wantText string
	}{
		{name: "missing table", execErr: failOn("GRANT", pgErr("42P01")), want: clients.ErrNotFound},
		{name: "missing schema", execErr: failOn("GRANT", pgErr("3F000")), want: clients.ErrNotFound},
		{name: "missing role", execErr: failOn("GRANT", pgErr("42704")), want: clients.ErrNotFound, wantText: "debezium"},
		{name: "no grant option", execErr: failOn("GRANT", pgErr("42501")), wantText: "GRANT OPTION"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{execErr: tc.execErr}
			w, _ := testWriter(tx)
			err := w.GrantSelect(unscopedCtx(), "debezium", []string{"public.orders"})
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil && errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("error %v wraps ErrNotFound", err)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q lacks %q", err, tc.wantText)
			}
			checkTx(t, tx, false)
		})
	}
}

// ---- CreateHeartbeatTable ----

func TestCreateHeartbeatTableSQL(t *testing.T) {
	tx := &fakeTx{}
	w, _ := testWriter(tx)
	if err := w.CreateHeartbeatTable(unscopedCtx(), "public.heartbeat"); err != nil {
		t.Fatal(err)
	}
	checkTx(t, tx, true)
	want := `CREATE TABLE IF NOT EXISTS "public"."heartbeat" (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())`
	if got := tx.sqls(); !reflect.DeepEqual(got, []string{want}) {
		t.Fatalf("statements:\n got %q\nwant %q", got, []string{want})
	}
	// The primary key is what Debezium's ON CONFLICT (id) heartbeat query needs.
	if !strings.Contains(want, "PRIMARY KEY") || !strings.Contains(want, "IF NOT EXISTS") {
		t.Fatalf("heartbeat DDL %q lacks the primary key or idempotence", want)
	}
}

func TestCreateHeartbeatTableRefusesBadInput(t *testing.T) {
	for _, table := range []string{"", "heartbeat", "a.b.c", `public."heartbeat"`, "public.hb; DROP TABLE x"} {
		w, b := testWriter(&fakeTx{})
		if err := w.CreateHeartbeatTable(unscopedCtx(), table); err == nil {
			t.Fatalf("table %q accepted", table)
		}
		if b.begins != 0 {
			t.Fatalf("table %q reached the database", table)
		}
	}
}

func TestCreateHeartbeatTableScope(t *testing.T) {
	w, b := testWriter(&fakeTx{})
	ctx := pipelineCtx()
	if err := w.CreateHeartbeatTable(ctx, "public.heartbeat"); err != nil {
		t.Fatalf("a source schema refused: %v", err)
	}
	err := w.CreateHeartbeatTable(ctx, "other.heartbeat")
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "other") {
		t.Fatalf("a foreign schema accepted: %v", err)
	}
	if b.begins != 1 {
		t.Fatalf("begins = %d; the foreign-schema call must not reach the database", b.begins)
	}
	if err := w.CreateHeartbeatTable(context.Background(), "public.heartbeat"); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v, want tools.ErrNoScope", err)
	}
}

func TestCreateHeartbeatTableErrors(t *testing.T) {
	cases := []struct {
		name     string
		execErr  func(string) error
		commitEr error
		want     error
		wantText string
	}{
		{name: "missing schema", execErr: failOn("CREATE", pgErr("3F000")), want: clients.ErrNotFound},
		{name: "no create", execErr: failOn("CREATE", pgErr("42501")), wantText: "CREATE on schema"},
		{name: "commit fails", commitEr: errors.New("connection lost"), wantText: "commit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{execErr: tc.execErr, commitErr: tc.commitEr}
			w, _ := testWriter(tx)
			err := w.CreateHeartbeatTable(unscopedCtx(), "public.heartbeat")
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil && errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("error %v wraps ErrNotFound", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q lacks %q", err, tc.wantText)
			}
			if tc.commitEr == nil {
				checkTx(t, tx, false)
			}
		})
	}
}

// ---- EndSession ----

func TestEndSessionSQL(t *testing.T) {
	start := time.Date(2026, 10, 6, 12, 0, 0, 123456000, time.UTC)
	tx := endSessionTx(t, sessionRow(&start, "client backend", "psql", "app", false, false), terminateRow(true))
	w, _ := testWriter(tx)
	if err := w.EndSession(unscopedCtx(), 4242, start); err != nil {
		t.Fatal(err)
	}
	checkTx(t, tx, true)
	// The snapshot clear must sit between the re-read and the terminate:
	// within one transaction pg_stat_activity is served from the first
	// read's snapshot, and without the clear the terminate's backend_start
	// recheck could never see a pid reused since that first read.
	if got, want := tx.sqls(), []string{adminSessionSQL, clearStatSnapshotSQL, terminateSessionSQL}; !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	if got, want := tx.stmts[1].args, []any{int64(4242)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("re-read args = %#v, want %#v", got, want)
	}
	if got, want := tx.stmts[3].args, []any{int64(4242), start}; !reflect.DeepEqual(got, want) {
		t.Fatalf("terminate args = %#v, want pid %d and backend_start bound", got, 4242)
	}
}

func TestEndSessionRefusals(t *testing.T) {
	start := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	other := start.Add(time.Second)
	cases := []struct {
		name    string
		read    func(dest ...any) error
		want    error
		wantTxt string
	}{
		{name: "no such pid", read: func(...any) error { return pgx.ErrNoRows }, want: ErrSessionChanged, wantTxt: "no session"},
		{name: "null backend_start", read: sessionRow(nil, "client backend", "psql", "app", false, false), want: ErrSessionChanged},
		{name: "session replaced", read: sessionRow(&other, "client backend", "psql", "app", false, false), want: ErrSessionChanged, wantTxt: "started"},
		{name: "own backend", read: sessionRow(&start, "client backend", "psql", "app", true, false), want: ErrSessionProtected, wantTxt: "own session"},
		{name: "walsender", read: sessionRow(&start, "walsender", "orders-connector", "debezium", false, false), want: ErrSessionProtected, wantTxt: "walsender"},
		{name: "writer role", read: sessionRow(&start, "client backend", "psql", "piperoom_writer", false, true), want: ErrSessionProtected, wantTxt: "writer's role"},
		{name: "piperoom reader", read: sessionRow(&start, "client backend", "piperoom-reader", "piperoom_ro", false, false), want: ErrSessionProtected, wantTxt: "Piperoom's own"},
		{name: "piperoom writer name", read: sessionRow(&start, "client backend", "Piperoom-Writer", "app", false, false), want: ErrSessionProtected, wantTxt: "Piperoom's own"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			terminated := false
			tx := endSessionTx(t, tc.read, func(dest ...any) error {
				terminated = true
				return terminateRow(true)(dest...)
			})
			w, _ := testWriter(tx)
			err := w.EndSession(unscopedCtx(), 4242, start)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.wantTxt != "" && !strings.Contains(err.Error(), tc.wantTxt) {
				t.Fatalf("error %q lacks %q", err, tc.wantTxt)
			}
			if terminated {
				t.Fatal("a refused EndSession must not call pg_terminate_backend")
			}
			for _, sql := range tx.sqls() {
				if sql == clearStatSnapshotSQL || sql == terminateSessionSQL {
					t.Fatalf("a refused EndSession must stop at the re-read, ran %q", sql)
				}
			}
			checkTx(t, tx, false)
		})
	}
}

func TestEndSessionRefusesBadInput(t *testing.T) {
	start := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		ctx   context.Context
		pid   int64
		start time.Time
	}{
		{"zero pid", unscopedCtx(), 0, start},
		{"negative pid", unscopedCtx(), -1, start},
		{"zero backend_start", unscopedCtx(), 42, time.Time{}},
		{"no scope", context.Background(), 42, start},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			err := w.EndSession(tc.ctx, tc.pid, tc.start)
			if err == nil {
				t.Fatal("expected a refusal")
			}
			if tc.name == "no scope" && !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("error %v does not wrap ErrNoScope", err)
			}
			if b.begins != 0 {
				t.Fatal("a refused call must not reach the database")
			}
		})
	}
}

func TestEndSessionTerminateErrors(t *testing.T) {
	start := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		term     func(dest ...any) error
		commitEr error
		want     error
		wantText string
	}{
		{name: "gone since the read", term: func(...any) error { return pgx.ErrNoRows }, want: ErrSessionChanged, wantText: "gone or was replaced"},
		{name: "not signalled", term: terminateRow(false), want: ErrSessionChanged, wantText: "not to signal"},
		{name: "no pg_signal_backend", term: func(...any) error { return pgErr("42501") }, wantText: "pg_signal_backend"},
		{name: "commit fails after the terminate", term: terminateRow(true), commitEr: errors.New("connection lost"), wantText: "not rolled back"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := endSessionTx(t, sessionRow(&start, "client backend", "psql", "app", false, false), tc.term)
			tx.commitErr = tc.commitEr
			w, _ := testWriter(tx)
			err := w.EndSession(unscopedCtx(), 4242, start)
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q lacks %q", err, tc.wantText)
			}
			if tc.commitEr == nil {
				checkTx(t, tx, false)
			}
		})
	}
}

func TestEndSessionScope(t *testing.T) {
	start := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	tx := endSessionTx(t, sessionRow(&start, "client backend", "psql", "app", false, false), terminateRow(true))
	w, b := testWriter(tx)
	if err := w.EndSession(context.Background(), 4242, start); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v, want tools.ErrNoScope", err)
	}
	if b.begins != 0 {
		t.Fatal("a scopeless call must not reach the database")
	}
	// A pid cannot be mapped to a pipeline here; a room pipeline in ctx is
	// what the gate requires.
	if err := w.EndSession(adminTableCtx(), 4242, start); err != nil {
		t.Fatalf("a scoped call refused: %v", err)
	}
}

// TestEndSessionDeadline: the terminate and the re-read carry the writer's
// deadline even when the caller's ctx has none.
func TestEndSessionDeadline(t *testing.T) {
	start := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	tx := endSessionTx(t, sessionRow(&start, "client backend", "psql", "app", false, false), terminateRow(true))
	w, _ := testWriter(tx)
	if err := w.EndSession(unscopedCtx(), 4242, start); err != nil {
		t.Fatal(err)
	}
	for _, s := range tx.stmts {
		if !s.deadline {
			t.Fatalf("statement without a deadline: %q", s.sql)
		}
	}
}
