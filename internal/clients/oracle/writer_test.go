package oracle

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// unscopedCtx is the explicit opt-out of CLI and dev callers.
func unscopedCtx() context.Context { return tools.WithUnscoped(context.Background()) }

// pipelineCtx scopes a call to the "orders" pipeline whose source table is
// APP.ORDERS.
func pipelineCtx() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{
		ID:           "orders",
		SourceTables: []string{"APP.ORDERS"},
	})
}

// writeCall is one recorded statement and its bind args.
type writeCall struct {
	stmt string
	args []any
}

// fakeResult is a minimal sql.Result for ExecContext.
type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeResult) RowsAffected() (int64, error) { return 1, nil }

// fakeWriteDB records every exec and query, answers queries with canned rows
// in order, and honours the call's context like *sql.DB does.
type fakeWriteDB struct {
	execs     []writeCall
	queries   []writeCall
	responses []sqlRows
	queryErr  error
	execErr   error
}

func (f *fakeWriteDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	f.execs = append(f.execs, writeCall{stmt: query, args: args})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if f.execErr != nil {
		return nil, f.execErr
	}
	return fakeResult{}, nil
}

func (f *fakeWriteDB) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	f.queries = append(f.queries, writeCall{stmt: query, args: args})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	if len(f.responses) == 0 {
		return nil, errors.New("fakeWriteDB: no response for query: " + query)
	}
	r := f.responses[0]
	f.responses = f.responses[1:]
	return r, nil
}

// fakeSecretStore records every stored secret; it never returns a value.
type fakeSecretStore struct {
	names  []string
	values []string
	err    error
}

func (s *fakeSecretStore) Store(_ context.Context, name, value string) error {
	s.names = append(s.names, name)
	s.values = append(s.values, value)
	return s.err
}

func testWriter(f *fakeWriteDB) *Writer {
	return &Writer{dbx: f, target: "oracle test"}
}

func TestNewSetupWriterNeedsWriteMode(t *testing.T) {
	if _, err := NewWriter(context.Background(), ""); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("empty DSN: err = %v, want ErrNoWriter", err)
	}
	if _, err := NewWriter(context.Background(), "   "); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("blank DSN: err = %v, want ErrNoWriter", err)
	}
	if _, err := NewWriter(context.Background(), "oracle://user@host:1521/XE"); !errors.Is(err, errNoPassword) {
		t.Fatalf("DSN without password: err = %v, want errNoPassword", err)
	}
}

func TestCreatePublicationSupplementalLogging(t *testing.T) {
	f := &fakeWriteDB{}
	w := testWriter(f)
	if err := w.CreatePublication(unscopedCtx(), "dbz", []string{"APP.ORDERS", "APP.USERS"}); err != nil {
		t.Fatalf("CreatePublication: %v", err)
	}
	if len(f.execs) != 2 {
		t.Fatalf("execs = %d, want 2", len(f.execs))
	}
	want := []string{
		`ALTER TABLE "APP"."ORDERS" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`,
		`ALTER TABLE "APP"."USERS" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`,
	}
	for i, s := range f.execs {
		if s.stmt != want[i] {
			t.Errorf("exec[%d] = %q, want %q", i, s.stmt, want[i])
		}
	}
	if len(f.queries) != 0 {
		t.Fatalf("queries = %d, want 0", len(f.queries))
	}
}

func TestCreateCanaryAndInsert(t *testing.T) {
	f := &fakeWriteDB{responses: []sqlRows{rows()}} // canary table does not exist yet
	w := testWriter(f)

	if err := w.CreateCanaryTable(unscopedCtx(), "APP.CANARY"); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	if err := w.InsertCanary(unscopedCtx(), "APP.CANARY", "deadbeef00cafe01"); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}

	if len(f.queries) != 1 || !strings.Contains(f.queries[0].stmt, "FROM ALL_TABLES") {
		t.Fatalf("existence check = %+v, want one ALL_TABLES query", f.queries)
	}
	if len(f.queries[0].args) != 2 || f.queries[0].args[0] != "APP" || f.queries[0].args[1] != "CANARY" {
		t.Fatalf("existence check args = %v, want [APP CANARY]", f.queries[0].args)
	}

	if len(f.execs) != 3 {
		t.Fatalf("execs = %d, want 3 (create, supplemental logging, insert)", len(f.execs))
	}
	if !strings.HasPrefix(f.execs[0].stmt, `CREATE TABLE "APP"."CANARY" `) ||
		!strings.Contains(f.execs[0].stmt, `"NONCE" VARCHAR2(64) NOT NULL`) {
		t.Fatalf("create = %q, want a canary CREATE TABLE with a NONCE column", f.execs[0].stmt)
	}
	if f.execs[1].stmt != `ALTER TABLE "APP"."CANARY" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` {
		t.Fatalf("supplemental = %q", f.execs[1].stmt)
	}
	// The nonce must be bound, never spliced into the statement.
	if !strings.Contains(f.execs[2].stmt, `VALUES (:1)`) || strings.Contains(f.execs[2].stmt, "deadbeef") {
		t.Fatalf("insert = %q, want a :1 bound statement without the nonce inline", f.execs[2].stmt)
	}
	if len(f.execs[2].args) != 1 || f.execs[2].args[0] != "deadbeef00cafe01" {
		t.Fatalf("insert args = %v, want the nonce bound exactly", f.execs[2].args)
	}
}

func TestCreateCanaryTableExistingRepairsLogging(t *testing.T) {
	t.Run("already logged is ready without writes", func(t *testing.T) {
		f := &fakeWriteDB{responses: []sqlRows{
			rows([]string{"1"}),                            // table exists
			rows([]string{"ALL COLUMN LOGGING", "ALWAYS"}), // logging present
		}}
		w := testWriter(f)
		if err := w.CreateCanaryTable(unscopedCtx(), "APP.CANARY"); err != nil {
			t.Fatalf("CreateCanaryTable: %v", err)
		}
		if len(f.execs) != 0 {
			t.Fatalf("execs = %d, want 0 (already logged)", len(f.execs))
		}
		if len(f.queries) != 2 {
			t.Fatalf("queries = %d, want 2 (existence, logging)", len(f.queries))
		}
	})

	t.Run("missing logging is repaired and confirmed", func(t *testing.T) {
		f := &fakeWriteDB{responses: []sqlRows{
			rows([]string{"1"}), // table exists
			rows(),              // no supplemental logging
			rows([]string{"ALL COLUMN LOGGING", "ALWAYS"}), // after repair
		}}
		w := testWriter(f)
		if err := w.CreateCanaryTable(unscopedCtx(), "APP.CANARY"); err != nil {
			t.Fatalf("CreateCanaryTable: %v", err)
		}
		if len(f.execs) != 1 || f.execs[0].stmt != `ALTER TABLE "APP"."CANARY" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` {
			t.Fatalf("execs = %+v, want one supplemental logging ALTER", f.execs)
		}
	})

	t.Run("unrepairable logging is an error", func(t *testing.T) {
		f := &fakeWriteDB{responses: []sqlRows{
			rows([]string{"1"}), // table exists
			rows(),              // no supplemental logging
		}}
		f.execErr = errors.New("ORA-32588: cannot alter")
		w := testWriter(f)
		if err := w.CreateCanaryTable(unscopedCtx(), "APP.CANARY"); err == nil {
			t.Fatal("CreateCanaryTable succeeded, want error")
		}
		if len(f.execs) != 1 {
			t.Fatalf("execs = %d, want 1 attempted repair", len(f.execs))
		}
	})
}

func TestCanaryTableScope(t *testing.T) {
	pipeline := owners.Pipeline{ID: "orders", SourceTables: []string{"APP.ORDERS"}}
	ctx := tools.WithPipeline(context.Background(), pipeline)

	t.Run("create refused outside source tables", func(t *testing.T) {
		f := &fakeWriteDB{responses: []sqlRows{rows()}}
		w := testWriter(f)
		if err := w.CreateCanaryTable(ctx, "APP.CANARY"); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("CreateCanaryTable: err = %v, want tools.ErrOutOfScope", err)
		}
		if len(f.execs) != 0 || len(f.queries) != 0 {
			t.Fatalf("out-of-scope canary reached the database: execs %d, queries %d", len(f.execs), len(f.queries))
		}
	})

	t.Run("insert refused outside source tables", func(t *testing.T) {
		f := &fakeWriteDB{}
		w := testWriter(f)
		if err := w.InsertCanary(ctx, "APP.CANARY", "nonce"); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("InsertCanary: err = %v, want tools.ErrOutOfScope", err)
		}
		if len(f.execs) != 0 || len(f.queries) != 0 {
			t.Fatalf("out-of-scope canary reached the database: execs %d, queries %d", len(f.execs), len(f.queries))
		}
	})

	t.Run("canary within source tables is allowed", func(t *testing.T) {
		canaryPipeline := owners.Pipeline{ID: "orders", SourceTables: []string{"APP.ORDERS", "APP.CANARY"}}
		f := &fakeWriteDB{responses: []sqlRows{rows()}} // does not exist yet
		w := testWriter(f)
		if err := w.CreateCanaryTable(tools.WithPipeline(context.Background(), canaryPipeline), "APP.CANARY"); err != nil {
			t.Fatalf("CreateCanaryTable in-scope: %v", err)
		}
		if len(f.execs) != 2 {
			t.Fatalf("execs = %d, want 2 (create + supplemental logging)", len(f.execs))
		}
	})
}

func TestCreateReadRoleReturnsOnlySecretName(t *testing.T) {
	f := &fakeWriteDB{responses: []sqlRows{rows()}} // role does not exist
	store := &fakeSecretStore{}
	w := &Writer{dbx: f, target: "oracle test", secrets: store}

	got, err := w.CreateReadRole(unscopedCtx(), "C##LOG_MINER", []string{"APP.ORDERS"})
	if err != nil {
		t.Fatalf("CreateReadRole: %v", err)
	}
	if got != readPasswordSecretName {
		t.Fatalf("secret name = %q, want %q", got, readPasswordSecretName)
	}
	if len(store.names) != 1 || store.names[0] != readPasswordSecretName {
		t.Fatalf("stored names = %v, want [%s]", store.names, readPasswordSecretName)
	}
	if len(store.values) != 1 || len(store.values[0]) != 32 {
		t.Fatalf("stored password = %d chars, want 32 hex chars", len(store.values[0]))
	}

	// The password is in exactly one statement (the CREATE USER); nowhere else,
	// and never in a return value or error.
	pw := store.values[0]
	passwordStmts := 0
	for _, e := range f.execs {
		if strings.Contains(e.stmt, pw) {
			passwordStmts++
		}
	}
	if passwordStmts != 1 {
		t.Fatalf("password appears in %d statements, want exactly 1", passwordStmts)
	}
	if !strings.HasPrefix(f.execs[0].stmt, `CREATE USER "C##LOG_MINER" IDENTIFIED BY "`) {
		t.Fatalf("CREATE USER = %q, want a quoted role and quoted password", f.execs[0].stmt)
	}
	sawTableGrant := false
	for _, e := range f.execs {
		if strings.Contains(e.stmt, `GRANT SELECT ON "APP"."ORDERS" TO "C##LOG_MINER"`) {
			sawTableGrant = true
		}
	}
	if !sawTableGrant {
		t.Fatalf("no GRANT SELECT ON the source table; execs: %+v", f.execs)
	}
	// The Debezium LogMiner grants are issued verbatim.
	for _, want := range []string{"GRANT LOGMINING TO", "GRANT SELECT ON V_$LOGMNR_CONTENTS TO", "GRANT SELECT_CATALOG_ROLE TO"} {
		found := false
		for _, e := range f.execs {
			if strings.Contains(e.stmt, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing LogMiner grant %q; execs: %+v", want, f.execs)
		}
	}
}

func TestCreateReadRoleExistingIsError(t *testing.T) {
	f := &fakeWriteDB{responses: []sqlRows{rows([]string{"1"})}} // role exists
	store := &fakeSecretStore{}
	w := &Writer{dbx: f, target: "oracle test", secrets: store}

	_, err := w.CreateReadRole(unscopedCtx(), "C##LOG_MINER", []string{"APP.ORDERS"})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing role: err = %v, want already-exists", err)
	}
	if len(f.execs) != 0 || len(f.queries) != 1 {
		t.Fatalf("existing role reached the database: execs %d, queries %d (want 1 existence query only)", len(f.execs), len(f.queries))
	}
	if len(store.names) != 0 {
		t.Fatalf("existing role stored a password: %v", store.names)
	}
}

func TestAlterSystemAllowList(t *testing.T) {
	for _, name := range []string{"processes", "wal_level", "", "x; DROP", "archive log"} {
		f := &fakeWriteDB{}
		w := testWriter(f)
		if _, err := w.AlterSystem(unscopedCtx(), name, "x"); err == nil {
			t.Errorf("AlterSystem(%q) succeeded, want refusal", name)
		}
		if len(f.execs) != 0 || len(f.queries) != 0 {
			t.Errorf("AlterSystem(%q) reached the database: execs %d, queries %d", name, len(f.execs), len(f.queries))
		}
	}

	f := &fakeWriteDB{}
	w := testWriter(f)
	pending, err := w.AlterSystem(unscopedCtx(), "archive_log", "")
	if err != nil {
		t.Fatalf("AlterSystem(archive_log): %v", err)
	}
	if !pending {
		t.Fatal("archive_log: pendingRestart = false, want true")
	}
	if len(f.execs) != 1 || f.execs[0].stmt != "ALTER DATABASE ARCHIVELOG" {
		t.Fatalf("execs = %+v, want one ALTER DATABASE ARCHIVELOG", f.execs)
	}
}

func TestSetupWriterRefusesBadIdentifiers(t *testing.T) {
	for _, name := range []string{`a"b`, `a;DROP`, `1abc`, ``, `schema.ta ble`} {
		f := &fakeWriteDB{}
		w := testWriter(f)
		if err := w.CreatePublication(unscopedCtx(), "dbz", []string{name}); err == nil {
			t.Errorf("CreatePublication(%q) succeeded, want refusal", name)
		}
		if len(f.execs) != 0 || len(f.queries) != 0 {
			t.Errorf("CreatePublication(%q) reached the database: execs %d, queries %d", name, len(f.execs), len(f.queries))
		}
	}
}

func TestSetupWriterScopeFailsClosed(t *testing.T) {
	bg := context.Background()
	// The canary table must be one of the pipeline's source tables (P1), so
	// the scope-gate test declares it alongside APP.ORDERS; otherwise the
	// canary calls below would fail closed with ErrOutOfScope, not exercise
	// the gate this test is about.
	pipeline := owners.Pipeline{ID: "orders", SourceTables: []string{"APP.ORDERS", "APP.CANARY"}}
	cases := []struct {
		name string
		ctx  context.Context
		ok   bool
	}{
		{"no scope", bg, false},
		{"pipeline id only", tools.WithPipelineID(bg, "orders"), false},
		{"unscoped", tools.WithUnscoped(bg), true},
		{"whole pipeline", tools.WithPipeline(bg, pipeline), true},
	}
	calls := []struct {
		name      string
		responses int
		run       func(ctx context.Context, w *Writer) error
	}{
		{"CreatePublication", 0, func(ctx context.Context, w *Writer) error {
			return w.CreatePublication(ctx, "dbz", []string{"APP.ORDERS"})
		}},
		{"CreateCanaryTable", 1, func(ctx context.Context, w *Writer) error {
			return w.CreateCanaryTable(ctx, "APP.CANARY")
		}},
		{"InsertCanary", 0, func(ctx context.Context, w *Writer) error {
			return w.InsertCanary(ctx, "APP.CANARY", "nonce")
		}},
		{"CreateReadRole", 1, func(ctx context.Context, w *Writer) error {
			_, err := w.CreateReadRole(ctx, "C##LOG_MINER", []string{"APP.ORDERS"})
			return err
		}},
		{"AlterSystem", 0, func(ctx context.Context, w *Writer) error {
			_, err := w.AlterSystem(ctx, "archive_log", "")
			return err
		}},
	}
	for _, c := range calls {
		for _, tc := range cases {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				resps := make([]sqlRows, c.responses)
				for i := range resps {
					resps[i] = rows() // empty: the object does not exist yet
				}
				f := &fakeWriteDB{responses: resps}
				w := &Writer{dbx: f, target: "oracle test", secrets: &fakeSecretStore{}}
				err := c.run(tc.ctx, w)
				if tc.ok {
					if errors.Is(err, tools.ErrNoScope) {
						t.Fatalf("refused with a scope: %v", err)
					}
					return
				}
				if !errors.Is(err, tools.ErrNoScope) {
					t.Fatalf("error %v, want tools.ErrNoScope", err)
				}
				if len(f.execs) != 0 || len(f.queries) != 0 {
					t.Fatalf("no scope reached the database: execs %d, queries %d", len(f.execs), len(f.queries))
				}
			})
		}
	}
}

func TestSetupWriterTimeout(t *testing.T) {
	t.Run("deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		time.Sleep(time.Millisecond) // let the deadline pass
		f := &fakeWriteDB{}
		w := testWriter(f)
		err := w.CreatePublication(tools.WithUnscoped(ctx), "dbz", []string{"APP.ORDERS"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
	})
	t.Run("no writer without DSN", func(t *testing.T) {
		var w *Writer
		if err := w.CreatePublication(context.Background(), "dbz", []string{"APP.ORDERS"}); !errors.Is(err, ErrNoWriter) {
			t.Fatalf("nil writer: err = %v, want ErrNoWriter", err)
		}
	})
}
