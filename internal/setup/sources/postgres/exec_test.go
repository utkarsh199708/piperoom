package postgres

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakeExec is the write pool the executing setup writer runs against: it
// records the statements and answers QueryRow from scan.
type fakeExec struct {
	stmts   []string
	args    [][]any
	queried []string
	// execErr answers the statement with an error, nil for success.
	execErr func(sql string) error
	// scan fills one QueryRow result; nil leaves the destinations untouched.
	scan func(dest ...any) error
}

func (f *fakeExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.stmts = append(f.stmts, sql)
	f.args = append(f.args, args)
	if f.execErr != nil {
		if err := f.execErr(sql); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakeExec) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.queried = append(f.queried, sql)
	_ = args
	return fakeRow{scan: f.scan}
}

type fakeRow struct{ scan func(dest ...any) error }

func (r fakeRow) Scan(dest ...any) error {
	if r.scan == nil {
		return nil
	}
	return r.scan(dest...)
}

// memStore is a SecretStore that keeps the values in the test.
type memStore struct {
	names  []string
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
	m.names = append(m.names, name)
	m.values[name] = value
	return nil
}

func newSetupWriter(db queryExecer, opts ...SetupWriterOption) *SetupWriter {
	o := newSetupWriterOptions(opts)
	return &SetupWriter{db: db, timeout: o.timeout, secret: o.secret}
}

func unscoped() context.Context { return tools.WithUnscoped(context.Background()) }

func TestSetupWriterIsAClientSetupSourceWriter(t *testing.T) {
	var _ clients.SetupSourceWriter = (*SetupWriter)(nil)
}

func TestSetupWriterCreatePublication(t *testing.T) {
	db := &fakeExec{}
	w := newSetupWriter(db)
	if err := w.CreatePublication(unscoped(), "dbz_publication", []string{"public.orders"}); err != nil {
		t.Fatalf("CreatePublication: %v", err)
	}
	want, err := CreatePublicationSQL("dbz_publication", []string{"public.orders"})
	if err != nil {
		t.Fatalf("CreatePublicationSQL: %v", err)
	}
	if len(db.stmts) != 1 || db.stmts[0] != want {
		t.Fatalf("statements = %q, want [%q]", db.stmts, want)
	}

	// An existing publication is an error, never altered.
	db2 := &fakeExec{execErr: func(string) error { return &pgconn.PgError{Code: "42710"} }}
	if err := newSetupWriter(db2).CreatePublication(unscoped(), "p", []string{"public.orders"}); !errors.Is(err, errPublicationExists) {
		t.Fatalf("duplicate publication: %v, want errPublicationExists", err)
	}
}

func TestSetupWriterCreateCanaryTableAndInsert(t *testing.T) {
	db := &fakeExec{}
	w := newSetupWriter(db)
	if err := w.CreateCanaryTable(unscoped(), "public.c"); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	nonce := "0123456789abcdef0123456789abcdef"
	if err := w.InsertCanary(unscoped(), "public.c", nonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	create, _ := CanaryTableSQL("public.c")
	insert, _ := InsertCanarySQL("public.c")
	if len(db.stmts) != 2 || db.stmts[0] != create || db.stmts[1] != insert {
		t.Fatalf("statements = %q", db.stmts)
	}
	if len(db.args[1]) != 1 || db.args[1][0] != nonce {
		t.Fatalf("insert args = %v, want the nonce", db.args[1])
	}

	// A bad nonce and a bad table are refused before any I/O.
	before := len(db.stmts)
	if err := w.InsertCanary(unscoped(), "public.c", "not-a-nonce"); !errors.Is(err, ErrParams) {
		t.Fatalf("bad nonce: %v, want ErrParams", err)
	}
	if err := w.CreateCanaryTable(unscoped(), "c"); err == nil {
		t.Fatal("a table without a schema was accepted")
	}
	if len(db.stmts) != before {
		t.Fatalf("a refusal reached the pool: %q", db.stmts[before:])
	}
}

var rolePasswordRe = regexp.MustCompile(`PASSWORD '([0-9a-f]{48})'`)

func TestSetupWriterCreateReadRole(t *testing.T) {
	db := &fakeExec{}
	store := &memStore{}
	w := newSetupWriter(db, WithSetupSecretStore(store))
	name, err := w.CreateReadRole(unscoped(), "piperoom_ro", []string{"public.orders", "public.customers"})
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	if name != readPasswordSecretName {
		t.Fatalf("secret name = %q, want %q", name, readPasswordSecretName)
	}
	if len(db.stmts) != 3 {
		t.Fatalf("statements = %q, want CREATE ROLE and two grants", db.stmts)
	}
	if !strings.HasPrefix(db.stmts[0], `CREATE ROLE "piperoom_ro" LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '`) {
		t.Fatalf("create role = %q", db.stmts[0])
	}
	m := rolePasswordRe.FindStringSubmatch(db.stmts[0])
	if m == nil {
		t.Fatalf("create role carries no generated password: %q", db.stmts[0])
	}
	grants, _ := ReadRoleGrants("piperoom_ro", []string{"public.orders", "public.customers"})
	if db.stmts[1] != grants[0] || db.stmts[2] != grants[1] {
		t.Fatalf("grants = %q, want %q", db.stmts[1:], grants)
	}
	if len(store.names) != 1 || store.names[0] != readPasswordSecretName || store.values[readPasswordSecretName] != m[1] {
		t.Fatalf("store = %v / %q; want the generated password under %q", store.names, store.values, readPasswordSecretName)
	}
	if strings.Contains(name, m[1]) {
		t.Fatal("the return value carries the password")
	}

	// An existing role is never re-passworded: no grant runs, nothing is
	// stored, and the error carries no password.
	db2 := &fakeExec{execErr: func(sql string) error {
		if strings.HasPrefix(sql, "CREATE ROLE") {
			return &pgconn.PgError{Code: "42710"}
		}
		return nil
	}}
	store2 := &memStore{}
	if _, err := newSetupWriter(db2, WithSetupSecretStore(store2)).CreateReadRole(unscoped(), "piperoom_ro", []string{"public.orders"}); !errors.Is(err, errRoleExists) {
		t.Fatalf("existing role: %v, want errRoleExists", err)
	}
	if len(db2.stmts) != 1 || len(store2.names) != 0 {
		t.Fatalf("existing role changed something: %q, stored %v", db2.stmts, store2.names)
	}
	// A generic CREATE ROLE failure is reported without the statement text.
	db3 := &fakeExec{execErr: func(sql string) error {
		if strings.HasPrefix(sql, "CREATE ROLE") {
			return &pgconn.PgError{Code: "42501"}
		}
		return nil
	}}
	_, err = newSetupWriter(db3, WithSetupSecretStore(&memStore{})).CreateReadRole(unscoped(), "piperoom_ro", []string{"public.orders"})
	if !errors.Is(err, errCreateRole) {
		t.Fatalf("generic failure: %v, want errCreateRole", err)
	}
	if m := rolePasswordRe.FindStringSubmatch(db3.stmts[0]); m != nil && strings.Contains(err.Error(), m[1]) {
		t.Fatalf("the error carries the password: %v", err)
	}
}

func TestSetupWriterCreateReadRoleNeedsASecretStore(t *testing.T) {
	db := &fakeExec{}
	_, err := newSetupWriter(db).CreateReadRole(unscoped(), "piperoom_ro", []string{"public.orders"})
	if err == nil || !strings.Contains(err.Error(), "secret store") {
		t.Fatalf("CreateReadRole without a store: %v", err)
	}
	if len(db.stmts) != 0 {
		t.Fatalf("something ran before the refusal: %q", db.stmts)
	}
}

func TestSetupWriterCreateReadRoleDropsTheRoleWhenTheStoreFails(t *testing.T) {
	db := &fakeExec{}
	store := &memStore{err: errors.New("disk full")}
	_, err := newSetupWriter(db, WithSetupSecretStore(store)).CreateReadRole(unscoped(), "piperoom_ro", []string{"public.orders"})
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("store failure: %v", err)
	}
	last := db.stmts[len(db.stmts)-1]
	if last != `DROP ROLE "piperoom_ro"` {
		t.Fatalf("the role was left behind with a lost password: %q", db.stmts)
	}
}

func TestSetupWriterAlterSystem(t *testing.T) {
	db := &fakeExec{scan: func(dest ...any) error {
		*dest[0].(*bool) = true
		return nil
	}}
	w := newSetupWriter(db)
	pending, err := w.AlterSystem(unscoped(), "max_wal_senders", "10")
	if err != nil || !pending {
		t.Fatalf("AlterSystem: %v (pending %v)", err, pending)
	}
	if len(db.stmts) != 1 || db.stmts[0] != "ALTER SYSTEM SET max_wal_senders = '10'" {
		t.Fatalf("statements = %q", db.stmts)
	}
	if len(db.queried) != 1 || !strings.Contains(db.queried[0], "pending_restart") {
		t.Fatalf("queries = %q", db.queried)
	}

	// The allow-list and the meaning are decided before the pool is touched.
	before := len(db.stmts)
	for _, tc := range []struct{ name, value string }{
		{"shared_buffers", "1GB"},
		{"wal_level", "minimal"},
		{"wal_sender_timeout", "0"},
	} {
		if _, err := w.AlterSystem(unscoped(), tc.name, tc.value); !errors.Is(err, ErrParams) {
			t.Fatalf("%s=%s: %v, want ErrParams", tc.name, tc.value, err)
		}
	}
	if len(db.stmts) != before {
		t.Fatalf("a refusal reached the pool: %q", db.stmts[before:])
	}
}

func TestSetupWriterScopeFailsClosed(t *testing.T) {
	db := &fakeExec{}
	w := newSetupWriter(db, WithSetupSecretStore(&memStore{}))
	calls := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{"CreatePublication", func(ctx context.Context) error {
			return w.CreatePublication(ctx, "p", []string{"public.orders"})
		}},
		{"CreateCanaryTable", func(ctx context.Context) error { return w.CreateCanaryTable(ctx, "public.c") }},
		{"InsertCanary", func(ctx context.Context) error {
			return w.InsertCanary(ctx, "public.c", "0123456789abcdef0123456789abcdef")
		}},
		{"CreateReadRole", func(ctx context.Context) error {
			_, err := w.CreateReadRole(ctx, "piperoom_ro", []string{"public.orders"})
			return err
		}},
	}
	for _, c := range calls {
		// No scope at all.
		if err := c.run(context.Background()); !errors.Is(err, tools.ErrNoScope) {
			t.Fatalf("%s without a scope: %v, want ErrNoScope", c.name, err)
		}
		// A pipeline that does not list the table.
		p := owners.Pipeline{ID: "p1", SourceTables: []string{"public.other"}}
		if err := c.run(tools.WithPipeline(context.Background(), p)); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("%s out of scope: %v, want ErrOutOfScope", c.name, err)
		}
	}
	// AlterSystem names no table, so only the missing scope refuses it.
	if _, err := w.AlterSystem(context.Background(), "wal_level", "logical"); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("AlterSystem without a scope: %v, want ErrNoScope", err)
	}
	if len(db.stmts) != 0 {
		t.Fatalf("a scoped refusal reached the pool: %q", db.stmts)
	}
}

func TestSetupWriterRefusalsBeforeIO(t *testing.T) {
	db := &fakeExec{}
	w := newSetupWriter(db, WithSetupSecretStore(&memStore{}))
	for _, run := range []func() error{
		func() error { return w.CreatePublication(unscoped(), "1bad", []string{"public.orders"}) },
		func() error { return w.CreatePublication(unscoped(), "p", nil) },
		func() error { return w.CreateCanaryTable(unscoped(), "c") },
		func() error { return w.InsertCanary(unscoped(), "public.c", "zz") },
		func() error { _, err := w.CreateReadRole(unscoped(), "public", []string{"public.orders"}); return err },
		func() error { _, err := w.CreateReadRole(unscoped(), "pg_x", []string{"public.orders"}); return err },
	} {
		if err := run(); err == nil {
			t.Fatal("a bad argument was accepted")
		}
	}
	if len(db.stmts) != 0 {
		t.Fatalf("a refusal reached the pool: %q", db.stmts)
	}

	var nilWriter *SetupWriter
	if err := nilWriter.CreateCanaryTable(unscoped(), "public.c"); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil writer: %v, want ErrNoWriter", err)
	}
	if _, err := NewSetupWriter(context.Background(), "  "); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("empty DSN: %v, want ErrNoWriter", err)
	}
}
