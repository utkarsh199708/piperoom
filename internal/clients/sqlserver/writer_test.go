package sqlserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- writer fakes ----

// memStore records the one secret the writer puts, so a test can assert the
// password never came back in a return value or in SQL text.
type memStore struct {
	name  string
	value string
}

func (m *memStore) Put(_ context.Context, name, value string) error {
	m.name, m.value = name, value
	return nil
}

// failStore always fails Put, so a test can assert CreateReadRole rolls the
// freshly created login and user back.
type failStore struct {
	err error
}

func (s *failStore) Put(_ context.Context, _, _ string) error { return s.err }

// blockingDB answers Exec only after ctx is done, so the 10 s (or shorter,
// under WithSetupTimeout) deadline is the thing that ends a call.
type blockingDB struct{}

func (blockingDB) Exec(ctx context.Context, _ string, _ ...any) error {
	<-ctx.Done()
	return ctx.Err()
}

func (blockingDB) Query(ctx context.Context, _ string, _ ...any) (rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingDB) QueryRow(ctx context.Context, _ string, _ ...any) scanner {
	return &fakeRow{err: context.DeadlineExceeded}
}

func (blockingDB) Ping(context.Context) error { return nil }
func (blockingDB) Close() error               { return nil }

func testWriter(f *fakeDB) *SetupWriter { return &SetupWriter{db: f, timeout: callTimeout} }

func unscopedCtx() context.Context { return tools.WithUnscoped(context.Background()) }

// hasStatement counts how many recorded statements contain frag (upper-cased).
func (f *fakeDB) count(frag string) int {
	n := 0
	for _, q := range f.sqls {
		if strings.Contains(strings.ToUpper(q), strings.ToUpper(frag)) {
			n++
		}
	}
	return n
}

// ---- tests ----

func TestNewSetupWriterNeedsWriteMode(t *testing.T) {
	for _, dsn := range []string{"", "   "} {
		w, err := NewSetupWriter(context.Background(), dsn)
		if w != nil {
			t.Fatalf("dsn %q: got a writer without a write DSN", dsn)
		}
		if !errors.Is(err, ErrNoWriter) {
			t.Fatalf("dsn %q: err = %v, want ErrNoWriter", dsn, err)
		}
		msg := err.Error()
		for _, bad := range []string{"sqlserver://", "password", "user id"} {
			if strings.Contains(msg, bad) {
				t.Fatalf("dsn %q: error leaks %q: %q", dsn, bad, msg)
			}
		}
	}
}

func TestCreatePublicationSQL(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{
		{vals: []any{int64(0)}}, // change_tables: capture 1 does not exist
		{vals: []any{int64(0)}}, // capture 2 does not exist
	}}
	w := testWriter(f)
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "p1", SourceTables: []string{"dbo.orders", "dbo.customers"}})
	if err := w.CreatePublication(ctx, "piperoom", []string{"dbo.orders", "dbo.customers"}); err != nil {
		t.Fatal(err)
	}

	if got := f.count("sp_cdc_enable_db"); got != 1 {
		t.Fatalf("sp_cdc_enable_db ran %d times, want 1", got)
	}
	if got := f.count("sp_cdc_enable_table"); got != 2 {
		t.Fatalf("sp_cdc_enable_table ran %d times, want 2", got)
	}

	// Order: enable db, check capture 1, check capture 2, enable table 1,
	// enable table 2. Every capture check runs before any table enable. Names
	// are bound (@pN), never interpolated.
	if len(f.sqls) != 5 {
		t.Fatalf("ran %d statements, want 5: %q", len(f.sqls), f.sqls)
	}
	if !strings.Contains(f.sqls[1], "capture_instance = @p1") {
		t.Fatalf("capture check not bound: %q", f.sqls[1])
	}
	if !strings.Contains(f.sqls[2], "capture_instance = @p1") {
		t.Fatalf("second capture check not bound: %q", f.sqls[2])
	}
	enable := f.sqls[3]
	for _, want := range []string{"@source_schema = @p1", "@source_name = @p2", "@role_name = NULL", "@capture_instance = @p3"} {
		if !strings.Contains(enable, want) {
			t.Fatalf("enable table SQL missing %q: %q", want, enable)
		}
	}
	// A two-table publication derives name_schema_table per table.
	if got := f.args[3]; len(got) != 3 || got[0] != "dbo" || got[1] != "orders" || got[2] != "piperoom_dbo_orders" {
		t.Fatalf("table 1 args = %v", got)
	}
	if got := f.args[4]; len(got) != 3 || got[0] != "dbo" || got[1] != "customers" || got[2] != "piperoom_dbo_customers" {
		t.Fatalf("table 2 args = %v", got)
	}
}

func TestCreatePublicationExistingCaptureIsError(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{
		{vals: []any{int64(1)}}, // the capture instance already exists
	}}
	w := testWriter(f)
	err := w.CreatePublication(unscopedCtx(), "piperoom", []string{"dbo.orders"})
	if !errors.Is(err, errCaptureExists) {
		t.Fatalf("err = %v, want errCaptureExists", err)
	}
	if got := f.count("sp_cdc_enable_table"); got != 0 {
		t.Fatalf("sp_cdc_enable_table ran %d times; nothing may be altered", got)
	}
}

func TestCreatePublicationSecondTableExistingCaptureAltersNothing(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{
		{vals: []any{int64(0)}}, // capture 1 does not exist
		{vals: []any{int64(1)}}, // capture 2 already exists
	}}
	w := testWriter(f)
	err := w.CreatePublication(unscopedCtx(), "piperoom", []string{"dbo.orders", "dbo.customers"})
	if !errors.Is(err, errCaptureExists) {
		t.Fatalf("err = %v, want errCaptureExists", err)
	}
	if got := f.count("sp_cdc_enable_table"); got != 0 {
		t.Fatalf("sp_cdc_enable_table ran %d times; nothing may be altered", got)
	}
	// Both capture checks ran before any enable could.
	if got := f.count("capture_instance = @p1"); got != 2 {
		t.Fatalf("capture checks = %d, want 2", got)
	}
}

func TestCreateCanaryTableAndInsert(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{{vals: []any{false}}}} // not yet tracked by CDC
		w := testWriter(f)
		if err := w.CreateCanaryTable(unscopedCtx(), "dbo.canary"); err != nil {
			t.Fatal(err)
		}
		if len(f.sqls) != 3 {
			t.Fatalf("ran %d statements, want 3: %q", len(f.sqls), f.sqls)
		}
		create := f.sqls[0]
		if !strings.Contains(create, "[dbo].[canary]") {
			t.Fatalf("create table not quoted: %q", create)
		}
		if !strings.Contains(strings.ToUpper(create), "IF OBJECT_ID") || !strings.Contains(strings.ToUpper(create), "PRIMARY KEY") {
			t.Fatalf("create table shape wrong: %q", create)
		}
		if !strings.Contains(strings.ToUpper(f.sqls[2]), "SP_CDC_ENABLE_TABLE") {
			t.Fatalf("CDC not enabled on the canary table: %q", f.sqls[2])
		}
	})

	t.Run("insert binds the nonce exactly", func(t *testing.T) {
		const nonce = "deadbeefcafef00ddeadbeefcafef00d"
		f := &fakeDB{}
		w := testWriter(f)
		if err := w.InsertCanary(unscopedCtx(), "dbo.canary", nonce); err != nil {
			t.Fatal(err)
		}
		ins := f.sqls[0]
		if strings.Contains(ins, nonce) {
			t.Fatalf("nonce interpolated into SQL: %q", ins)
		}
		if !strings.Contains(ins, "@p1") {
			t.Fatalf("nonce not bound: %q", ins)
		}
		if !strings.Contains(ins, "[dbo].[canary]") {
			t.Fatalf("insert target not quoted: %q", ins)
		}
		if len(f.args[0]) != 1 || f.args[0][0] != nonce {
			t.Fatalf("nonce not bound exactly: %v", f.args[0])
		}
	})
}

func TestCreateReadRoleReturnsOnlySecretName(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{{vals: []any{int64(0)}}}} // login does not exist
	st := &memStore{}
	w := &SetupWriter{db: f, timeout: callTimeout, secret: st}
	got, err := w.CreateReadRole(unscopedCtx(), "piperoom_capture", []string{"dbo.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "mssql_piperoom_capture" {
		t.Fatalf("secret name = %q, want mssql_piperoom_capture", got)
	}
	if st.value == "" {
		t.Fatal("password was not stored")
	}
	if got == st.value {
		t.Fatal("the returned name must not be the password")
	}

	// The password reaches the server only as the quoted literal of the CREATE
	// LOGIN statement; no other statement may carry it.
	var createLogin string
	for i, q := range f.sqls {
		if strings.Contains(strings.ToUpper(q), "CREATE LOGIN") {
			createLogin = q
			if len(f.args[i]) != 0 {
				t.Fatalf("CREATE LOGIN must not bind the password as an arg: %v", f.args[i])
			}
			if !strings.Contains(q, "'"+st.value+"'") {
				t.Fatalf("CREATE LOGIN does not embed the password literal: %q", q)
			}
			if strings.Contains(strings.ToUpper(q), "PASSWORD = @P1") {
				t.Fatalf("CREATE LOGIN still binds the password as @p1: %q", q)
			}
			continue
		}
		if strings.Contains(q, st.value) {
			t.Fatalf("password leaked into another SQL statement: %q", q)
		}
	}
	if createLogin == "" {
		t.Fatal("no CREATE LOGIN ran")
	}
	joined := strings.Join(f.sqls, "\n")
	for _, want := range []string{
		"CREATE LOGIN [piperoom_capture] WITH PASSWORD = '",
		"CREATE USER [piperoom_capture] FOR LOGIN [piperoom_capture]",
		"GRANT SELECT ON [dbo].[orders] TO [piperoom_capture]",
		"GRANT SELECT ON SCHEMA::cdc TO [piperoom_capture]",
		"GRANT VIEW DATABASE STATE TO [piperoom_capture]",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in:\n%s", want, joined)
		}
	}
}

func TestCreateReadRoleLoginErrorKeepsPasswordOut(t *testing.T) {
	// The driver echoes the statement text, which now embeds the password
	// literal; the returned error must be generic and carry none of it.
	f := &fakeDB{
		qrows: []*fakeRow{{vals: []any{int64(0)}}}, // login does not exist
		execs: []error{errors.New("mssql: Incorrect syntax near 'CREATE LOGIN [x] WITH PASSWORD = 'abc123def''")},
	}
	st := &memStore{}
	w := &SetupWriter{db: f, timeout: callTimeout, secret: st}
	_, err := w.CreateReadRole(unscopedCtx(), "piperoom_capture", []string{"dbo.orders"})
	if err == nil {
		t.Fatal("want an error when CREATE LOGIN fails")
	}
	if !errors.Is(err, errCreateLogin) {
		t.Fatalf("err = %v, want errCreateLogin", err)
	}
	for _, bad := range []string{"abc123def", "PASSWORD = '"} {
		if strings.Contains(err.Error(), bad) {
			t.Fatalf("error leaks the statement text %q: %q", bad, err)
		}
	}
}

func TestCreateReadRoleExistingIsError(t *testing.T) {
	f := &fakeDB{qrows: []*fakeRow{{vals: []any{int64(1)}}}} // login already exists
	st := &memStore{}
	w := &SetupWriter{db: f, timeout: callTimeout, secret: st}
	if _, err := w.CreateReadRole(unscopedCtx(), "piperoom_capture", []string{"dbo.orders"}); !errors.Is(err, errRoleExists) {
		t.Fatalf("err = %v, want errRoleExists", err)
	}
	if f.count("CREATE LOGIN") != 0 {
		t.Fatal("must not create a login over an existing role")
	}
	if st.value != "" {
		t.Fatalf("generated a password for an existing role: %q", st.value)
	}
}

func TestCreateReadRoleSecretPutFailureDropsLoginAndUser(t *testing.T) {
	putErr := errors.New("secret store down")
	f := &fakeDB{qrows: []*fakeRow{{vals: []any{int64(0)}}}} // login does not exist
	w := &SetupWriter{db: f, timeout: callTimeout, secret: &failStore{err: putErr}}
	_, err := w.CreateReadRole(unscopedCtx(), "piperoom_capture", []string{"dbo.orders"})
	if err == nil {
		t.Fatal("want an error when the secret store fails")
	}
	if !errors.Is(err, putErr) {
		t.Fatalf("err = %v, want it to wrap the Put error %v", err, putErr)
	}
	if got := f.count("DROP USER [piperoom_capture]"); got != 1 {
		t.Fatalf("DROP USER ran %d times, want 1", got)
	}
	if got := f.count("DROP LOGIN [piperoom_capture]"); got != 1 {
		t.Fatalf("DROP LOGIN ran %d times, want 1", got)
	}
	// The password must not leak into any statement other than the CREATE LOGIN
	// literal, so the two rollback statements interpolate only the login name.
	for _, q := range f.sqls {
		if strings.Contains(strings.ToUpper(q), "DROP") && strings.Contains(q, "PASSWORD") {
			t.Fatalf("rollback statement embeds a password clause: %q", q)
		}
	}
}

func TestAlterSystemNotSupported(t *testing.T) {
	f := &fakeDB{}
	w := testWriter(f)
	if _, err := w.AlterSystem(context.Background(), "wal_level", "logical"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want clients.ErrNotSupported", err)
	}
	if len(f.sqls) != 0 {
		t.Fatalf("AlterSystem ran %d statements; it must do no I/O", len(f.sqls))
	}
}

func TestSetupWriterRefusesBadIdentifiers(t *testing.T) {
	cases := []struct {
		name string
		call func(w *SetupWriter) error
	}{
		{"publication a;DROP", func(w *SetupWriter) error {
			return w.CreatePublication(unscopedCtx(), "a;DROP", []string{"dbo.orders"})
		}},
		{"publication a]b", func(w *SetupWriter) error {
			return w.CreatePublication(unscopedCtx(), "a]b", []string{"dbo.orders"})
		}},
		{"publication 1abc", func(w *SetupWriter) error {
			return w.CreatePublication(unscopedCtx(), "1abc", []string{"dbo.orders"})
		}},
		{"empty publication", func(w *SetupWriter) error {
			return w.CreatePublication(unscopedCtx(), "", []string{"dbo.orders"})
		}},
		{"table schema.ta ble", func(w *SetupWriter) error {
			return w.CreateCanaryTable(unscopedCtx(), "schema.ta ble")
		}},
		{"table a;DROP", func(w *SetupWriter) error {
			return w.CreatePublication(unscopedCtx(), "pub", []string{"dbo.a;DROP"})
		}},
		{"role a;DROP", func(w *SetupWriter) error {
			_, err := w.CreateReadRole(unscopedCtx(), "a;DROP", []string{"dbo.orders"})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDB{}
			w := testWriter(f)
			if err := tc.call(w); err == nil {
				t.Fatal("want an error for a bad identifier")
			}
			if len(f.sqls) != 0 {
				t.Fatalf("ran %d statements for bad input", len(f.sqls))
			}
		})
	}
}

func TestSetupWriterScopeFailsClosed(t *testing.T) {
	t.Run("no scope", func(t *testing.T) {
		f := &fakeDB{}
		w := testWriter(f)
		if err := w.CreateCanaryTable(context.Background(), "dbo.canary"); !errors.Is(err, tools.ErrNoScope) {
			t.Fatalf("err = %v, want tools.ErrNoScope", err)
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d statements without a scope", len(f.sqls))
		}
	})

	t.Run("table outside the pipeline", func(t *testing.T) {
		f := &fakeDB{}
		w := testWriter(f)
		ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "p1", SourceTables: []string{"dbo.orders"}})
		if err := w.CreatePublication(ctx, "pub", []string{"dbo.payroll"}); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("err = %v, want tools.ErrOutOfScope", err)
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d statements for an out-of-scope table", len(f.sqls))
		}
	})
}

func TestCanaryWritesRespectTableScope(t *testing.T) {
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "p1", SourceTables: []string{"dbo.orders"}})

	t.Run("create outside the pipeline is refused", func(t *testing.T) {
		f := &fakeDB{}
		w := testWriter(f)
		if err := w.CreateCanaryTable(ctx, "dbo.payroll"); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("err = %v, want tools.ErrOutOfScope", err)
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d statements for an out-of-scope canary table", len(f.sqls))
		}
	})

	t.Run("insert outside the pipeline is refused", func(t *testing.T) {
		f := &fakeDB{}
		w := testWriter(f)
		if err := w.InsertCanary(ctx, "dbo.payroll", "deadbeefdeadbeefdeadbeefdeadbeef"); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("err = %v, want tools.ErrOutOfScope", err)
		}
		if len(f.sqls) != 0 {
			t.Fatalf("ran %d statements for an out-of-scope canary table", len(f.sqls))
		}
	})

	t.Run("in-scope target proceeds", func(t *testing.T) {
		f := &fakeDB{qrows: []*fakeRow{{vals: []any{true}}}} // already tracked by CDC
		w := testWriter(f)
		if err := w.CreateCanaryTable(ctx, "dbo.orders"); err != nil {
			t.Fatal(err)
		}
		if len(f.sqls) == 0 {
			t.Fatal("no statement ran for an in-scope canary table")
		}
		if err := w.InsertCanary(ctx, "dbo.orders", "deadbeefdeadbeefdeadbeefdeadbeef"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSetupWriterTimeout(t *testing.T) {
	t.Run("blocking call hits the deadline", func(t *testing.T) {
		w := &SetupWriter{db: blockingDB{}, timeout: 20 * time.Millisecond}
		start := time.Now()
		err := w.InsertCanary(unscopedCtx(), "dbo.canary", "deadbeef")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("deadline test took too long")
		}
	})

	t.Run("connect error is scrubbed of the DSN", func(t *testing.T) {
		dsn := "sqlserver://sa:S3cr3t!@db.corp:1433?database=orders"
		raw := errors.New("dial tcp db.corp:1433: connect: connection refused (" + dsn + ")")
		got := redactDSN(raw, dsn)
		if got == nil {
			t.Fatal("redactDSN returned nil")
		}
		if strings.Contains(got.Error(), dsn) || strings.Contains(got.Error(), "S3cr3t!") {
			t.Fatalf("DSN leaked: %q", got)
		}
	})
}
