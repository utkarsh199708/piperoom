package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// fakeWriter records the setup writer's calls. It is not the runner: it only
// proves what the driver asked for, and never sees a credential.
type fakeWriter struct {
	calls     []string
	pubName   string
	pubTables []string
	canary    string
	table     string
	nonce     string
	role      string
	roleTable []string
	secret    string
	sysName   string
	sysValue  string
	pending   bool
	err       error
}

func (f *fakeWriter) CreatePublication(_ context.Context, name string, tables []string) error {
	f.calls = append(f.calls, "create_publication")
	f.pubName, f.pubTables = name, tables
	return f.err
}

func (f *fakeWriter) CreateCanaryTable(_ context.Context, table string) error {
	f.calls = append(f.calls, "create_canary_table")
	f.canary = table
	return f.err
}

func (f *fakeWriter) InsertCanary(_ context.Context, table, nonce string) error {
	f.calls = append(f.calls, "insert_canary")
	f.table, f.nonce = table, nonce
	return f.err
}

func (f *fakeWriter) CreateReadRole(_ context.Context, role string, tables []string) (string, error) {
	f.calls = append(f.calls, "create_read_role")
	f.role, f.roleTable = role, tables
	if f.err != nil {
		return "", f.err
	}
	if f.secret == "" {
		return "postgres_read_password", nil
	}
	return f.secret, nil
}

func (f *fakeWriter) AlterSystem(_ context.Context, name, value string) (bool, error) {
	f.calls = append(f.calls, "alter_system")
	f.sysName, f.sysValue = name, value
	return f.pending, f.err
}

// Only the filtered publication exists (no FOR ALL TABLES form). This
// assertion pins that *Writer is a clients.SetupSourceWriter.
var _ clients.SetupSourceWriter = (*Writer)(nil)

func newTestWriter(t *testing.T, f clients.SetupSourceWriter) *Writer {
	t.Helper()
	w, err := NewWriter(f)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w
}

func TestCreatePublication(t *testing.T) {
	f := &fakeWriter{}
	w := newTestWriter(t, f)
	if err := w.CreatePublication(context.Background(), "dbz_publication", []string{"public.orders"}); err != nil {
		t.Fatalf("filtered: %v", err)
	}
	if f.pubName != "dbz_publication" || len(f.pubTables) != 1 || f.pubTables[0] != "public.orders" {
		t.Fatalf("call = %q %v", f.pubName, f.pubTables)
	}
	for _, tc := range []struct {
		name   string
		pub    string
		tables []string
	}{
		{"no tables", "p", nil},
		{"bad name", "1bad", []string{"public.orders"}},
		{"bad table", "p", []string{"orders"}},
	} {
		f.calls = nil
		if err := w.CreatePublication(context.Background(), tc.pub, tc.tables); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
		if len(f.calls) != 0 {
			t.Errorf("%s: the writer was called before the refusal: %v", tc.name, f.calls)
		}
	}
}

func TestCreateCanaryTableAndInsert(t *testing.T) {
	f := &fakeWriter{}
	w := newTestWriter(t, f)
	if err := w.CreateCanaryTable(context.Background(), "public.piperoom_canary"); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	if f.canary != "public.piperoom_canary" {
		t.Fatalf("table = %q", f.canary)
	}
	nonce := "0123456789abcdef0123456789abcdef"
	if err := w.InsertCanary(context.Background(), "public.piperoom_canary", nonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	if f.table != "public.piperoom_canary" || f.nonce != nonce {
		t.Fatalf("insert = %q %q", f.table, f.nonce)
	}

	// A bad table is refused by both methods before any I/O.
	for _, table := range []string{"piperoom_canary", "public;drop.piperoom_canary", "public.c.x", ""} {
		f.calls = nil
		if err := w.CreateCanaryTable(context.Background(), table); err == nil {
			t.Errorf("CreateCanaryTable(%q) accepted", table)
		}
		if err := w.InsertCanary(context.Background(), table, nonce); err == nil {
			t.Errorf("InsertCanary(%q) accepted", table)
		}
		if len(f.calls) != 0 {
			t.Errorf("%q: the writer was called before the refusal: %v", table, f.calls)
		}
	}

	// A nonce is 16 random bytes in lower-case hex, nothing else.
	for _, bad := range []string{"0123", "0123456789ABCDEF0123456789ABCDEF", "zz23456789abcdef0123456789abcdef", "", nonce + "00"} {
		f.calls = nil
		if err := w.InsertCanary(context.Background(), "public.c", bad); err == nil {
			t.Errorf("nonce %q accepted", bad)
		}
		if len(f.calls) != 0 {
			t.Errorf("nonce %q reached the writer", bad)
		}
	}
}

func TestCreateReadRole(t *testing.T) {
	f := &fakeWriter{}
	w := newTestWriter(t, f)
	name, err := w.CreateReadRole(context.Background(), "piperoom_ro", []string{"public.orders", "public.customers"})
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	if name != "postgres_read_password" {
		t.Fatalf("secret name = %q", name)
	}
	if strings.Contains(name, "password=") || f.role != "piperoom_ro" || len(f.roleTable) != 2 {
		t.Fatalf("call = %q %v, name %q", f.role, f.roleTable, name)
	}

	for _, tc := range []struct {
		name   string
		role   string
		tables []string
	}{
		{"public", "public", []string{"public.orders"}},
		{"pg_ role", "pg_read_all_stats", []string{"public.orders"}},
		{"bad role", "ro;drop", []string{"public.orders"}},
		{"no tables", "piperoom_ro", nil},
		{"bad table", "piperoom_ro", []string{"orders"}},
	} {
		f.calls = nil
		if _, err := w.CreateReadRole(context.Background(), tc.role, tc.tables); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
		if len(f.calls) != 0 {
			t.Errorf("%s: the writer was called before the refusal: %v", tc.name, f.calls)
		}
	}

	// The runner's answer is the only source of the name; an empty one is a
	// protocol error, never a password.
	f2 := &fakeWriter{secret: "named"}
	w2 := newTestWriter(t, f2)
	if got, _ := w2.CreateReadRole(context.Background(), "piperoom_ro", []string{"public.orders"}); got != "named" {
		t.Fatalf("secret name = %q, want named", got)
	}
	f3 := &fakeWriter{secret: " "}
	w3 := newTestWriter(t, f3)
	if _, err := w3.CreateReadRole(context.Background(), "piperoom_ro", []string{"public.orders"}); err == nil {
		t.Fatal("an empty secret name was accepted")
	}
}

func TestAlterSystemAllowList(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"wal_level", "logical"},
		{"wal_level", "replica"},
		{"max_replication_slots", "10"},
		{"max_wal_senders", "10"},
		{"max_slot_wal_keep_size", "-1"},
		{"max_slot_wal_keep_size", "10GB"},
		{"wal_sender_timeout", "30s"},
		{"wal_sender_timeout", "5min"},
	} {
		f := &fakeWriter{pending: true}
		w := newTestWriter(t, f)
		pending, err := w.AlterSystem(context.Background(), tc.name, tc.value)
		if err != nil || !pending {
			t.Fatalf("%s=%s: %v (pending %v)", tc.name, tc.value, err, pending)
		}
		if f.sysName != tc.name || f.sysValue != tc.value {
			t.Fatalf("%s: call = %q %q", tc.name, f.sysName, f.sysValue)
		}
	}

	for _, name := range []string{"", "rds.logical_replication", "password_encryption", "wal_level ", "WAL_LEVEL", "wal_level;drop", "shared_buffers"} {
		f := &fakeWriter{}
		w := newTestWriter(t, f)
		if _, err := w.AlterSystem(context.Background(), name, "logical"); !errors.Is(err, ErrParams) {
			t.Errorf("%q: %v, want ErrParams", name, err)
		}
		if len(f.calls) != 0 {
			t.Errorf("%q reached the writer", name)
		}
	}

	// The allow-list is decided before the writer is even present.
	var nilWriter *Writer
	if _, err := nilWriter.AlterSystem(context.Background(), "shared_buffers", "1"); !errors.Is(err, ErrParams) {
		t.Fatalf("nil writer, off-list name: %v, want ErrParams", err)
	}

	for _, value := range []string{"", "logical;drop", "lo gical'", strings.Repeat("x", 65)} {
		f := &fakeWriter{}
		w := newTestWriter(t, f)
		if _, err := w.AlterSystem(context.Background(), "wal_level", value); !errors.Is(err, ErrParams) {
			t.Errorf("value %q: %v, want ErrParams", value, err)
		}
		if len(f.calls) != 0 {
			t.Errorf("value %q reached the writer", value)
		}
	}
}

// TestAlterSystemRefusesValuesPostgresCannotUse pins the meaning of each
// allow-listed parameter, not just the shape of its value: a name on the list
// with a value Postgres refuses to start with would turn a maintenance window
// into an outage.
func TestAlterSystemRefusesValuesPostgresCannotUse(t *testing.T) {
	tests := []struct{ name, value string }{
		{"wal_level", "minimal"},
		{"wal_level", "logical "},
		{"wal_level", "on"},
		{"max_wal_senders", "0"},
		{"max_wal_senders", "-1"},
		{"max_wal_senders", "1.5"},
		{"max_wal_senders", "ten"},
		{"max_replication_slots", "0"},
		{"max_slot_wal_keep_size", "1024"}, // bare number: its unit would be a guess
		{"max_slot_wal_keep_size", "10gb"},
		{"max_slot_wal_keep_size", "0GB"},
		{"wal_sender_timeout", "0"},
		{"wal_sender_timeout", "0s"},
		{"wal_sender_timeout", "fast"},
		{"wal_sender_timeout", "-30s"},
	}
	for _, tc := range tests {
		f := &fakeWriter{}
		w := newTestWriter(t, f)
		if _, err := w.AlterSystem(context.Background(), tc.name, tc.value); !errors.Is(err, ErrParams) {
			t.Errorf("%s = %q: %v, want ErrParams", tc.name, tc.value, err)
		}
		if len(f.calls) != 0 {
			t.Errorf("%s = %q reached the writer", tc.name, tc.value)
		}
	}
}

func TestNilWriter(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("NewWriter(nil) = %v", err)
	}
	var w *Writer
	if err := w.CreateCanaryTable(context.Background(), "public.c"); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil CreateCanaryTable = %v", err)
	}
	if _, err := w.CreateReadRole(context.Background(), "ro", []string{"public.orders"}); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil CreateReadRole = %v", err)
	}
}

func TestReadRoleGrants(t *testing.T) {
	stmts, err := ReadRoleGrants("piperoom_ro", []string{"public.orders", "public.customers"})
	if err != nil {
		t.Fatalf("ReadRoleGrants: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("got %d statements: %v", len(stmts), stmts)
	}
	all := strings.Join(stmts, "\n")
	if !strings.HasPrefix(stmts[0], "GRANT SELECT ON TABLE ") ||
		!strings.Contains(stmts[0], `"public"."orders"`) || !strings.Contains(stmts[0], `"public"."customers"`) ||
		!strings.HasSuffix(stmts[0], ` TO "piperoom_ro"`) {
		t.Fatalf("select grant = %q", stmts[0])
	}
	if stmts[1] != `GRANT "pg_read_all_stats" TO "piperoom_ro"` {
		t.Fatalf("stats grant = %q", stmts[1])
	}
	for _, forbidden := range []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE", "ALL TABLES", "pg_monitor", "GRANT OPTION", "PUBLIC"} {
		if strings.Contains(all, forbidden) {
			t.Fatalf("grants contain %s: %v", forbidden, stmts)
		}
	}
	if _, err := ReadRoleGrants("public", []string{"public.orders"}); err == nil {
		t.Fatal("a grant to PUBLIC was accepted")
	}
	if _, err := ReadRoleGrants("ro", []string{"public.orders;drop"}); err == nil {
		t.Fatal("an injected table was accepted")
	}
}

func TestSQLBuilders(t *testing.T) {
	if _, err := CanaryTableSQL("public.c"); err != nil {
		t.Fatalf("CanaryTableSQL: %v", err)
	}
	sql, err := CanaryTableSQL("public.c")
	if err != nil || !strings.Contains(sql, `CREATE TABLE IF NOT EXISTS "public"."c"`) {
		t.Fatalf("canary DDL = %q, %v", sql, err)
	}
	if ins, err := InsertCanarySQL("public.c"); err != nil || ins != `INSERT INTO "public"."c" (nonce) VALUES ($1)` {
		t.Fatalf("insert SQL = %q, %v", ins, err)
	}
	if pub, err := CreatePublicationSQL("p", []string{"public.orders"}); err != nil || pub != `CREATE PUBLICATION "p" FOR TABLE "public"."orders"` {
		t.Fatalf("publication SQL = %q, %v", pub, err)
	}
	if _, err := CreatePublicationSQL("p", nil); err == nil {
		t.Fatal("a publication with no tables was accepted")
	}
}
