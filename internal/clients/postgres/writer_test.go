package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

type fakeStmt struct {
	sql      string
	args     []any
	deadline bool // the call's ctx had a deadline
}

// fakeTx answers QueryRow with scan (no scan: no rows) and Exec with execErr,
// and remembers every statement.
type fakeTx struct {
	stmts     []fakeStmt
	scan      func(dest ...any) error
	execErr   func(sql string) error
	commitErr error
	commits   int
	rollbacks int
}

func (f *fakeTx) note(ctx context.Context, sql string, args []any) {
	_, ok := ctx.Deadline()
	f.stmts = append(f.stmts, fakeStmt{sql: sql, args: args, deadline: ok})
}

func (f *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.note(ctx, sql, args)
	if f.execErr != nil {
		if err := f.execErr(sql); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.NewCommandTag("OK"), nil
}

func (f *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.note(ctx, sql, args)
	if f.scan == nil {
		return fakeRow{err: pgx.ErrNoRows}
	}
	return scanRow(f.scan)
}

func (f *fakeTx) Commit(context.Context) error   { f.commits++; return f.commitErr }
func (f *fakeTx) Rollback(context.Context) error { f.rollbacks++; return nil }

// sqls lists the statements after the per-transaction set_config.
func (f *fakeTx) sqls() []string {
	var out []string
	for _, s := range f.stmts {
		if s.sql != setTimeoutsSQL {
			out = append(out, s.sql)
		}
	}
	return out
}

type scanRow func(dest ...any) error

func (r scanRow) Scan(dest ...any) error { return r(dest...) }

type fakeBeginner struct {
	tx     *fakeTx
	err    error
	begins int
}

func (b *fakeBeginner) begin(context.Context) (writeTx, error) {
	b.begins++
	if b.err != nil {
		return nil, b.err
	}
	return b.tx, nil
}

func testWriter(tx *fakeTx) (*Writer, *fakeBeginner) {
	b := &fakeBeginner{tx: tx}
	return &Writer{db: b, target: "postgres orders@db.internal:5432", timeout: defaultWriteTimeout, lockTimeout: defaultLockTimeout}, b
}

// pubRow answers publicationCheckSQL.
func pubRow(allTables bool, already ...string) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(*bool) = allTables
		*dest[1].(*[]string) = already
		return nil
	}
}

// slotRow answers slotCheckSQL.
func slotRow(active, here bool) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(*bool) = active
		*dest[1].(*bool) = here
		return nil
	}
}

func pgErr(code string) error {
	return &pgconn.PgError{Severity: "ERROR", Code: code, Message: "server said " + code}
}

func failOn(prefix string, err error) func(string) error {
	return func(sql string) error {
		if strings.HasPrefix(sql, prefix) {
			return err
		}
		return nil
	}
}

// unscopedCtx is the explicit opt-out of CLI and dev callers.
func unscopedCtx() context.Context { return tools.WithUnscoped(context.Background()) }

func pipelineCtx() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{
		ID:           "orders",
		SourceTables: []string{"public.orders"},
		Connectors:   []string{"pg-orders-connector"},
	})
}

// checkTx asserts the transaction ran with deadlines, set its timeouts first,
// and ended exactly once the expected way.
func checkTx(t *testing.T, tx *fakeTx, committed bool) {
	t.Helper()
	if len(tx.stmts) == 0 || tx.stmts[0].sql != setTimeoutsSQL {
		t.Fatalf("first statement is not the timeouts: %+v", tx.stmts)
	}
	if got := tx.stmts[0].args; !reflect.DeepEqual(got, []any{"5000", "10000"}) {
		t.Fatalf("timeouts args = %v, want lock 5000 ms, statement 10000 ms", got)
	}
	for _, s := range tx.stmts {
		if !s.deadline {
			t.Fatalf("statement without a deadline: %q", s.sql)
		}
	}
	if committed && (tx.commits != 1 || tx.rollbacks != 0) {
		t.Fatalf("commits=%d rollbacks=%d, want a commit", tx.commits, tx.rollbacks)
	}
	if !committed && (tx.commits != 0 || tx.rollbacks != 1) {
		t.Fatalf("commits=%d rollbacks=%d, want a rollback", tx.commits, tx.rollbacks)
	}
}

// ---- NewWriter ----

func TestNewWriterEmptyDSNIsNoWriter(t *testing.T) {
	for _, dsn := range []string{"", "  \t"} {
		w, err := NewWriter(context.Background(), dsn)
		if w != nil || !errors.Is(err, ErrNoWriter) {
			t.Fatalf("NewWriter(%q) = %v, %v; want nil, ErrNoWriter", dsn, w, err)
		}
	}
}

func TestNewWriterErrorsNeverCarryTheDSN(t *testing.T) {
	cases := []struct{ name, dsn string }{
		// pgx prints the connection string in a parse error.
		{"parse", "postgres://writer:hunter2secret@db.internal:badport/orders"},
		// Loopback port 1 is closed: the connect fails without leaving the host.
		{"connect", "postgres://writer:hunter2secret@127.0.0.1:1/orders?connect_timeout=2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			w, err := NewWriter(ctx, tc.dsn)
			if err == nil {
				w.Close()
				t.Fatal("expected an error")
			}
			if errors.Is(err, ErrNoWriter) {
				t.Fatalf("a bad DSN is not \"no writer\": %v", err)
			}
			for _, secret := range []string{"hunter2secret", tc.dsn, "user=writer", "db.internal"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("%q in error %q", secret, err)
				}
			}
		})
	}
}

func TestWriterPoolConfig(t *testing.T) {
	cfg, err := writerPoolConfig("postgres://w:pw@db:5432/orders", newWriterOptions(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxConns != writerMaxConns || cfg.MinConns != 0 {
		t.Fatalf("MaxConns=%d MinConns=%d", cfg.MaxConns, cfg.MinConns)
	}
	if cfg.AfterConnect != nil {
		t.Fatal("the write pool must not set up its sessions (no read-only AfterConnect)")
	}
	if cfg.ConnConfig.ConnectTimeout != 10*time.Second {
		t.Fatalf("ConnectTimeout = %v, want 10s", cfg.ConnConfig.ConnectTimeout)
	}
	rp := cfg.ConnConfig.RuntimeParams
	if rp["application_name"] != writerAppName {
		t.Fatalf("application_name = %q", rp["application_name"])
	}
	for _, k := range []string{"default_transaction_read_only", "statement_timeout", "lock_timeout"} {
		if _, ok := rp[k]; ok {
			t.Fatalf("start-up parameter %s set; the writer sets timeouts per transaction", k)
		}
	}

	// What the DSN says wins, except a pool larger than writerMaxConns.
	cfg, err = writerPoolConfig("postgres://w:pw@db:5432/orders?connect_timeout=3&application_name=mine&pool_max_conns=1", newWriterOptions(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.ConnectTimeout != 3*time.Second || cfg.ConnConfig.RuntimeParams["application_name"] != "mine" || cfg.MaxConns != 1 {
		t.Fatalf("DSN settings lost: timeout=%v app=%q max=%d", cfg.ConnConfig.ConnectTimeout, cfg.ConnConfig.RuntimeParams["application_name"], cfg.MaxConns)
	}
	cfg, err = writerPoolConfig("postgres://w:pw@db:5432/orders?pool_max_conns=50", newWriterOptions(nil))
	if err != nil || cfg.MaxConns != writerMaxConns {
		t.Fatalf("pool_max_conns=50 gave MaxConns=%d, %v; want %d", cfg.MaxConns, err, writerMaxConns)
	}
}

func TestWriterOptions(t *testing.T) {
	o := newWriterOptions([]WriterOption{WithWriteTimeout(3 * time.Second), WithLockTimeout(time.Second)})
	if o.timeout != 3*time.Second || o.lockTimeout != time.Second {
		t.Fatalf("options = %+v", o)
	}
	o = newWriterOptions([]WriterOption{WithWriteTimeout(0), WithLockTimeout(-1)})
	if o.timeout != defaultWriteTimeout || o.lockTimeout != defaultLockTimeout {
		t.Fatalf("non-positive durations must keep the defaults: %+v", o)
	}
}

type fakeRowQuerier struct{ row pgx.Row }

func (f fakeRowQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return f.row }

func TestCheckWritable(t *testing.T) {
	setting := func(readOnly string, inRecovery bool) pgx.Row {
		return scanRow(func(dest ...any) error {
			*dest[0].(*string) = readOnly
			*dest[1].(*bool) = inRecovery
			return nil
		})
	}
	if err := checkWritable(context.Background(), fakeRowQuerier{setting("off", false)}); err != nil {
		t.Fatalf("a writable primary refused: %v", err)
	}
	if err := checkWritable(context.Background(), fakeRowQuerier{setting("on", false)}); !errors.Is(err, errReadOnlyRole) {
		t.Fatalf("a read-only role accepted: %v", err)
	}
	if err := checkWritable(context.Background(), fakeRowQuerier{setting("off", true)}); !errors.Is(err, errStandby) || !strings.Contains(err.Error(), "hot standby") {
		t.Fatalf("a hot standby accepted: %v", err)
	}
	down := errors.New("connection refused")
	if err := checkWritable(context.Background(), fakeRowQuerier{fakeRow{err: down}}); !errors.Is(err, down) {
		t.Fatalf("ping error lost: %v", err)
	}
}

func TestNilWriterIsNoWriter(t *testing.T) {
	for _, w := range []*Writer{nil, {}} {
		ctx := context.Background()
		if err := w.AddPublicationTables(ctx, "p", []string{"public.t"}); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("AddPublicationTables: %v", err)
		}
		if err := w.DropReplicationSlot(ctx, "s"); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("DropReplicationSlot: %v", err)
		}
		if _, err := w.InsertSnapshotSignal(ctx, "public.sig", "c", []string{"public.t"}); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("InsertSnapshotSignal: %v", err)
		}
		w.Close()
	}
}

// ---- AddPublicationTables ----

func TestAddPublicationTablesSQL(t *testing.T) {
	tx := &fakeTx{scan: pubRow(false)}
	w, _ := testWriter(tx)
	ctx, rec := recordCtx()
	ctx = tools.WithUnscoped(ctx)
	if err := w.AddPublicationTables(ctx, "dbz_publication", []string{"public.orders", "Sales.Items", "public.orders"}); err != nil {
		t.Fatal(err)
	}
	checkTx(t, tx, true)
	want := []string{publicationCheckSQL, `ALTER PUBLICATION "dbz_publication" ADD TABLE "public"."orders", "Sales"."Items"`}
	if got := tx.sqls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	if got, want := tx.stmts[1].args, []any{"dbz_publication", []string{"public", "Sales"}, []string{"orders", "Items"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("check args = %#v, want %#v", got, want)
	}
	if len(tx.stmts[2].args) != 0 {
		t.Fatalf("the ALTER takes no parameters: %v", tx.stmts[2].args)
	}
	calls := rec.Calls()
	if len(calls) != 2 || calls[1].Text != want[1] || calls[1].Kind != tools.CallSQL || calls[1].Target != "postgres orders@db.internal:5432" {
		t.Fatalf("receipts = %+v", calls)
	}
}

func TestAddPublicationTablesRefusesBadInput(t *testing.T) {
	many := make([]string, maxWriteTables+1)
	for i := range many {
		many[i] = fmt.Sprintf("public.t%d", i)
	}
	cases := []struct {
		name   string
		pub    string
		tables []string
	}{
		{"empty publication", "", []string{"public.orders"}},
		{"publication injection", `dbz"; DROP TABLE x; --`, []string{"public.orders"}},
		{"publication with dash", "dbz-pub", []string{"public.orders"}},
		{"no tables", "dbz", nil},
		{"no schema", "dbz", []string{"orders"}},
		{"three parts", "dbz", []string{"a.b.c"}},
		{"table injection", "dbz", []string{"public.orders; DROP TABLE x"}},
		{"quoted table", "dbz", []string{`public."orders"`}},
		{"regex table", "dbz", []string{"public.*"}},
		{"empty table", "dbz", []string{""}},
		{"too many", "dbz", many},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{scan: pubRow(false)}
			w, b := testWriter(tx)
			if err := w.AddPublicationTables(unscopedCtx(), tc.pub, tc.tables); err == nil {
				t.Fatal("expected a refusal")
			}
			if b.begins != 0 {
				t.Fatal("a refused call must not reach the database")
			}
		})
	}
}

func TestAddPublicationTablesRefusals(t *testing.T) {
	cases := []struct {
		name     string
		scan     func(dest ...any) error
		execErr  func(string) error
		want     error
		wantText string
		altered  bool // the ALTER was sent
	}{
		{name: "missing publication", want: clients.ErrNotFound},
		{name: "for all tables", scan: pubRow(true), want: ErrAlreadyPublished},
		{name: "already published", scan: pubRow(false, "public.orders"), want: ErrAlreadyPublished, wantText: "public.orders"},
		{name: "published since the check", scan: pubRow(false), execErr: failOn("ALTER", pgErr("42710")), want: ErrAlreadyPublished, altered: true},
		{name: "no such table", scan: pubRow(false), execErr: failOn("ALTER", pgErr("42P01")), want: clients.ErrNotFound, altered: true},
		{name: "publication dropped since the check", scan: pubRow(false), execErr: failOn("ALTER", pgErr("42704")), want: clients.ErrNotFound, altered: true},
		{name: "not the owner", scan: pubRow(false), execErr: failOn("ALTER", pgErr("42501")), wantText: "42501", altered: true},
		{name: "read fails", scan: func(...any) error { return pgErr("57014") }, wantText: "read publication"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{scan: tc.scan, execErr: tc.execErr}
			w, _ := testWriter(tx)
			err := w.AddPublicationTables(unscopedCtx(), "dbz_publication", []string{"public.orders"})
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil && (errors.Is(err, clients.ErrNotFound) || errors.Is(err, ErrAlreadyPublished)) {
				t.Fatalf("error %v wraps a sentinel it should not", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) || !strings.HasPrefix(err.Error(), "postgres: ") {
				t.Fatalf("error %q lacks %q", err, tc.wantText)
			}
			altered := false
			for _, s := range tx.sqls() {
				altered = altered || strings.HasPrefix(s, "ALTER")
			}
			if altered != tc.altered {
				t.Fatalf("ALTER sent = %v, want %v", altered, tc.altered)
			}
			checkTx(t, tx, false)
		})
	}
}

func TestWriterTransactionErrors(t *testing.T) {
	down := errors.New("pool closed")
	w, _ := testWriter(nil)
	w.db = &fakeBeginner{err: down}
	if err := w.AddPublicationTables(unscopedCtx(), "dbz", []string{"public.orders"}); !errors.Is(err, down) || !strings.Contains(err.Error(), "begin") {
		t.Fatalf("begin error = %v", err)
	}

	tx := &fakeTx{scan: pubRow(false), execErr: failOn(setTimeoutsSQL, pgErr("42501"))}
	w, _ = testWriter(tx)
	if err := w.AddPublicationTables(unscopedCtx(), "dbz", []string{"public.orders"}); err == nil || !strings.Contains(err.Error(), "set timeouts") {
		t.Fatalf("set timeouts error = %v", err)
	}
	if len(tx.stmts) != 1 || tx.rollbacks != 1 {
		t.Fatalf("after a failed set_config: %d statements, %d rollbacks", len(tx.stmts), tx.rollbacks)
	}

	lost := errors.New("connection lost")
	tx = &fakeTx{scan: pubRow(false), commitErr: lost}
	w, _ = testWriter(tx)
	if err := w.AddPublicationTables(unscopedCtx(), "dbz", []string{"public.orders"}); !errors.Is(err, lost) || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("commit error = %v", err)
	}
}

func TestAddPublicationTablesScope(t *testing.T) {
	tx := &fakeTx{scan: pubRow(false)}
	w, b := testWriter(tx)
	err := w.AddPublicationTables(pipelineCtx(), "dbz", []string{"public.orders", "public.payroll"})
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "public.payroll") {
		t.Fatalf("out-of-scope table: %v", err)
	}
	if b.begins != 0 {
		t.Fatal("an out-of-scope call must not reach the database")
	}
	if err := w.AddPublicationTables(pipelineCtx(), "dbz", []string{"public.orders"}); err != nil {
		t.Fatalf("in-scope table refused: %v", err)
	}
}

// ---- DropReplicationSlot ----

func TestDropReplicationSlotSQL(t *testing.T) {
	tx := &fakeTx{scan: slotRow(false, true)}
	w, _ := testWriter(tx)
	if err := w.DropReplicationSlot(unscopedCtx(), "debezium_orders"); err != nil {
		t.Fatal(err)
	}
	checkTx(t, tx, true)
	if got, want := tx.sqls(), []string{slotCheckSQL, "SELECT pg_drop_replication_slot($1)"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	for _, s := range tx.stmts[1:] {
		if !reflect.DeepEqual(s.args, []any{"debezium_orders"}) {
			t.Fatalf("%q args = %v, want the slot as $1", s.sql, s.args)
		}
	}
}

func TestDropReplicationSlotRefusesBadNames(t *testing.T) {
	for _, name := range []string{"", "Debezium", "dbz-orders", "x'); DROP TABLE t; --", "a b", strings.Repeat("s", 64)} {
		w, b := testWriter(&fakeTx{scan: slotRow(false, true)})
		if err := w.DropReplicationSlot(unscopedCtx(), name); err == nil {
			t.Fatalf("slot %q accepted", name)
		}
		if b.begins != 0 {
			t.Fatalf("slot %q reached the database", name)
		}
	}
}

func TestDropReplicationSlotRefusals(t *testing.T) {
	cases := []struct {
		name    string
		scan    func(dest ...any) error
		execErr func(string) error
		want    error
		dropped bool // the drop was sent
	}{
		{name: "missing slot", want: clients.ErrNotFound},
		// slot_type = 'logical' in the check: a physical slot is no row.
		{name: "physical slot", want: clients.ErrNotFound},
		{name: "another database", scan: slotRow(false, false), want: clients.ErrNotFound},
		{name: "active", scan: slotRow(true, true), want: clients.ErrSlotActive},
		{name: "active since the check", scan: slotRow(false, true), execErr: failOn("SELECT pg_drop", pgErr("55006")), want: clients.ErrSlotActive, dropped: true},
		{name: "dropped since the check", scan: slotRow(false, true), execErr: failOn("SELECT pg_drop", pgErr("42704")), want: clients.ErrNotFound, dropped: true},
		{name: "no privilege", scan: slotRow(false, true), execErr: failOn("SELECT pg_drop", pgErr("42501")), dropped: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{scan: tc.scan, execErr: tc.execErr}
			w, _ := testWriter(tx)
			err := w.DropReplicationSlot(unscopedCtx(), "debezium_orders")
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			if tc.want == nil && (errors.Is(err, clients.ErrNotFound) || errors.Is(err, clients.ErrSlotActive)) {
				t.Fatalf("error %v wraps a sentinel it should not", err)
			}
			sent := false
			for _, s := range tx.sqls() {
				sent = sent || strings.HasPrefix(s, "SELECT pg_drop")
			}
			if sent != tc.dropped {
				t.Fatalf("drop sent = %v, want %v", sent, tc.dropped)
			}
			checkTx(t, tx, false)
		})
	}
}

func TestDropReplicationSlotCommitFailsAfterDrop(t *testing.T) {
	tx := &fakeTx{scan: slotRow(false, true), commitErr: errors.New("connection lost")}
	w, _ := testWriter(tx)
	err := w.DropReplicationSlot(unscopedCtx(), "debezium_orders")
	if err == nil || !strings.Contains(err.Error(), "the slot is dropped") {
		t.Fatalf("a failed commit after the drop must say the slot is gone: %v", err)
	}
}

// ---- InsertSnapshotSignal ----

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestInsertSnapshotSignalSQL(t *testing.T) {
	tx := &fakeTx{}
	w, _ := testWriter(tx)
	id, err := w.InsertSnapshotSignal(unscopedCtx(), "debezium.signal", "pg-orders-connector", []string{"public.orders", "public.items", "public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !uuidV4.MatchString(id) {
		t.Fatalf("id %q is not a v4 UUID", id)
	}
	checkTx(t, tx, true)
	if got, want := tx.sqls(), []string{`INSERT INTO "debezium"."signal" (id, type, data) VALUES ($1, 'execute-snapshot', $2)`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	if got, want := tx.stmts[1].args, []any{id, `{"data-collections":["public.orders","public.items"],"type":"incremental"}`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insert args = %#v, want %#v", got, want)
	}
	var data snapshotSignal
	if err := json.Unmarshal([]byte(tx.stmts[1].args[1].(string)), &data); err != nil || data.Type != "incremental" {
		t.Fatalf("data does not parse: %v %+v", err, data)
	}
}

func TestInsertSnapshotSignalRefusesBadInput(t *testing.T) {
	cases := []struct {
		name, table, connector string
		tables                 []string
	}{
		{"signal table without schema", "debezium_signal", "c", []string{"public.orders"}},
		{"signal table injection", "public.sig; DROP TABLE x", "c", []string{"public.orders"}},
		{"signal table three parts", "a.b.c", "c", []string{"public.orders"}},
		{"no connector", "public.sig", " ", []string{"public.orders"}},
		{"no tables", "public.sig", "c", nil},
		{"regex table", "public.sig", "c", []string{"public.*"}},
		{"table with quote", "public.sig", "c", []string{`public.o"rders`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			id, err := w.InsertSnapshotSignal(unscopedCtx(), tc.table, tc.connector, tc.tables)
			if err == nil || id != "" {
				t.Fatalf("= %q, %v; want a refusal", id, err)
			}
			if b.begins != 0 {
				t.Fatal("a refused call must not reach the database")
			}
		})
	}
}

func TestInsertSnapshotSignalErrors(t *testing.T) {
	tx := &fakeTx{execErr: failOn("INSERT", pgErr("42P01"))}
	w, _ := testWriter(tx)
	id, err := w.InsertSnapshotSignal(unscopedCtx(), "public.debezium_signal", "pg-orders-connector", []string{"public.orders"})
	if !errors.Is(err, clients.ErrNotFound) || id != "" {
		t.Fatalf("missing signal table = %q, %v; want ErrNotFound", id, err)
	}
	checkTx(t, tx, false)

	tx = &fakeTx{execErr: failOn("INSERT", pgErr("22001"))}
	w, _ = testWriter(tx)
	if id, err := w.InsertSnapshotSignal(unscopedCtx(), "public.debezium_signal", "pg-orders-connector", []string{"public.orders"}); err == nil || id != "" || errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("value too long = %q, %v", id, err)
	}
}

func TestInsertSnapshotSignalScope(t *testing.T) {
	cases := []struct {
		name, connector string
		tables          []string
		ok              bool
	}{
		{"in scope", "pg-orders-connector", []string{"public.orders"}, true},
		{"other connector", "pg-payroll-connector", []string{"public.orders"}, false},
		{"other table", "pg-orders-connector", []string{"public.payroll"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			_, err := w.InsertSnapshotSignal(pipelineCtx(), "public.debezium_signal", tc.connector, tc.tables)
			if tc.ok && err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !tc.ok && (!errors.Is(err, tools.ErrOutOfScope) || b.begins != 0) {
				t.Fatalf("not refused before the database: %v (begins %d)", err, b.begins)
			}
		})
	}
}

func TestNewUUID(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id, err := newUUID()
		if err != nil {
			t.Fatal(err)
		}
		if !uuidV4.MatchString(id) || seen[id] {
			t.Fatalf("bad or repeated id %q", id)
		}
		seen[id] = true
	}
}

// ---- round 1: scope fails closed, physical slots, signal size, timeouts ----

// Every method refuses before any I/O unless ctx has the room's whole
// pipeline or the explicit unscoped opt-out (tools.ScopeFor's order: a
// pipeline id wins over the opt-out, and the writer cannot resolve an id).
func TestWriterScopeFailsClosed(t *testing.T) {
	bg := context.Background()
	cases := []struct {
		name string
		ctx  context.Context
		ok   bool
	}{
		{"no scope", bg, false},
		{"pipeline id only", tools.WithPipelineID(bg, "orders"), false},
		{"pipeline id and the opt-out", tools.WithPipelineID(tools.WithUnscoped(bg), "orders"), false},
		{"recorder only", tools.WithRecorder(bg, tools.NewRecorder()), false},
		{"unscoped", tools.WithUnscoped(bg), true},
		{"whole pipeline", pipelineCtx(), true},
	}
	calls := []struct {
		name string
		scan func(dest ...any) error
		run  func(ctx context.Context, w *Writer) error
	}{
		{"AddPublicationTables", pubRow(false), func(ctx context.Context, w *Writer) error {
			return w.AddPublicationTables(ctx, "dbz", []string{"public.orders"})
		}},
		{"DropReplicationSlot", slotRow(false, true), func(ctx context.Context, w *Writer) error {
			return w.DropReplicationSlot(ctx, "debezium_orders")
		}},
		{"InsertSnapshotSignal", nil, func(ctx context.Context, w *Writer) error {
			_, err := w.InsertSnapshotSignal(ctx, "public.debezium_signal", "pg-orders-connector", []string{"public.orders"})
			return err
		}},
	}
	for _, c := range calls {
		for _, tc := range cases {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				tx := &fakeTx{scan: c.scan}
				w, b := testWriter(tx)
				err := c.run(tc.ctx, w)
				if tc.ok {
					if err != nil || b.begins != 1 {
						t.Fatalf("refused with a scope: %v (begins %d)", err, b.begins)
					}
					return
				}
				if !errors.Is(err, tools.ErrNoScope) {
					t.Fatalf("error %v, want tools.ErrNoScope", err)
				}
				if b.begins != 0 || len(tx.stmts) != 0 {
					t.Fatalf("no scope reached the database: begins %d, statements %d", b.begins, len(tx.stmts))
				}
			})
		}
	}
}

func TestSlotCheckOnlyLogicalSlotsOfThisDatabase(t *testing.T) {
	for _, want := range []string{"slot_type = 'logical'", "COALESCE(database = current_database(), false)"} {
		if !strings.Contains(slotCheckSQL, want) {
			t.Fatalf("slotCheckSQL lacks %q:\n%s", want, slotCheckSQL)
		}
	}
	tx := &fakeTx{} // a physical slot is no row of the check
	w, _ := testWriter(tx)
	err := w.DropReplicationSlot(unscopedCtx(), "standby_1")
	if !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), "physical slots are never dropped") {
		t.Fatalf("physical slot: %v", err)
	}
	for _, s := range tx.sqls() {
		if strings.HasPrefix(s, "SELECT pg_drop") {
			t.Fatal("a slot the check did not return was dropped")
		}
	}
}

var fitRe = regexp.MustCompile(`at most (\d+) of these tables per snapshot signal`)

func TestInsertSnapshotSignalRefusesDataOverColumn(t *testing.T) {
	tables := make([]string, 60)
	for i := range tables {
		tables[i] = fmt.Sprintf("public.a_rather_long_table_name_for_the_signal_%02d", i)
	}
	w, b := testWriter(&fakeTx{})
	_, err := w.InsertSnapshotSignal(unscopedCtx(), "public.debezium_signal", "c", tables)
	m := fitRe.FindStringSubmatch(fmt.Sprint(err))
	if m == nil || b.begins != 0 {
		t.Fatalf("over 2048 bytes: %v (begins %d); want a refusal naming how many fit, before the database", err, b.begins)
	}
	fit, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatal(err)
	}
	if fit < 1 || fit >= len(tables) {
		t.Fatalf("fit = %d of %d", fit, len(tables))
	}

	// Exactly fit tables go; one more does not.
	tx := &fakeTx{}
	w, _ = testWriter(tx)
	if _, err := w.InsertSnapshotSignal(unscopedCtx(), "public.debezium_signal", "c", tables[:fit]); err != nil {
		t.Fatalf("%d tables (said to fit): %v", fit, err)
	}
	if n := len(tx.stmts[1].args[1].(string)); n > maxSignalDataBytes {
		t.Fatalf("data of %d bytes sent", n)
	}
	w, b = testWriter(&fakeTx{})
	if _, err := w.InsertSnapshotSignal(unscopedCtx(), "public.debezium_signal", "c", tables[:fit+1]); err == nil || b.begins != 0 {
		t.Fatalf("%d tables (one more than fit): %v", fit+1, err)
	}
}

func TestTimeoutsNeverRenderAsZero(t *testing.T) {
	cases := map[time.Duration]string{
		500 * time.Microsecond:  "1",
		time.Millisecond:        "1",
		1500 * time.Millisecond: "1500",
		10 * time.Second:        "10000",
	}
	for d, want := range cases {
		if got := millis(d); got != want {
			t.Fatalf("millis(%v) = %q, want %q", d, got, want)
		}
	}
	tx := &fakeTx{}
	w, _ := testWriter(tx)
	w.lockTimeout, w.timeout = 300*time.Microsecond, 900*time.Microsecond
	if _, err := w.InsertSnapshotSignal(unscopedCtx(), "public.debezium_signal", "c", []string{"public.orders"}); err != nil {
		t.Fatal(err)
	}
	if got := tx.stmts[0].args; !reflect.DeepEqual(got, []any{"1", "1"}) {
		t.Fatalf("sub-millisecond timeouts sent as %v, want 1 ms each (0 turns them off)", got)
	}
}
