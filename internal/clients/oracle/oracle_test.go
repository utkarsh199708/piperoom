package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// fakeRows implements sqlRows over a fixed table of string values so tests run
// without a database.
type fakeRows struct {
	cols [][]string
	pos  int
}

func (r *fakeRows) Next() bool {
	if r.pos < len(r.cols) {
		r.pos++
		return true
	}
	return false
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.pos == 0 || r.pos > len(r.cols) {
		return fmt.Errorf("fakeRows: Scan without a current row")
	}
	row := r.cols[r.pos-1]
	if len(row) < len(dest) {
		return fmt.Errorf("fakeRows: %d values, %d destinations", len(row), len(dest))
	}
	for i, d := range dest {
		switch dp := d.(type) {
		case *string:
			*dp = row[i]
		case *sql.NullString:
			*dp = sql.NullString{String: row[i], Valid: true}
		default:
			return fmt.Errorf("fakeRows: unsupported dest %T", d)
		}
	}
	return nil
}

func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Close() error { return nil }

func rows(values ...[]string) *fakeRows { return &fakeRows{cols: values} }

// fakeQuerier returns canned results in order and records every query text and
// bind args.
type fakeQuerier struct {
	responses []sqlRows
	queries   []string
	args      [][]any
	err       error
}

func (f *fakeQuerier) QueryContext(_ context.Context, query string, args ...any) (sqlRows, error) {
	f.queries = append(f.queries, query)
	f.args = append(f.args, args)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.responses) == 0 {
		return nil, fmt.Errorf("fakeQuerier: no response for query %d: %s", len(f.queries), query)
	}
	r := f.responses[0]
	f.responses = f.responses[1:]
	return r, nil
}

func testClient(q querier) *Client {
	return &Client{q: q, target: "oracle test"}
}

// dbRow is a canned V$DATABASE row in the order Database selects it.
func dbRow(cdb, name, container, logMode, forceLog string, sup ...string) []string {
	if len(sup) != 5 {
		panic("dbRow needs five supplemental logging flags")
	}
	return append([]string{name, cdb, container, logMode, forceLog}, sup...)
}

func TestDatabaseCDB(t *testing.T) {
	f := &fakeQuerier{responses: []sqlRows{
		rows(dbRow("YES", "ORCLCDB", "CDB$ROOT", "ARCHIVELOG", "YES", "YES", "YES", "YES", "YES", "NO")),
		rows([]string{"ORCLPDB1"}, []string{"ORCLPDB2"}),
		rows([]string{banner26ai}),
	}}
	c := testClient(f)

	got, err := c.Database(context.Background())
	if err != nil {
		t.Fatalf("Database: %v", err)
	}
	want := DatabaseInfo{
		Name:      "ORCLCDB",
		IsCDB:     true,
		Container: "CDB$ROOT",
		PDBs:      []string{"ORCLPDB1", "ORCLPDB2"},
		LogMode:   "ARCHIVELOG",
		ForceLog:  "YES",
		Supplemental: SupplementalLogging{
			Minimal: true, PrimaryKey: true, All: true, ForeignKey: true, UniqueIndex: false,
		},
		Version: banner26ai,
		Edition: "Free",
	}
	if !equalDB(got, want) {
		t.Fatalf("Database = %+v, want %+v", got, want)
	}
	if len(f.queries) != 3 {
		t.Fatalf("expected 3 queries, got %d", len(f.queries))
	}
	if !strings.Contains(f.queries[0], "FROM V$DATABASE") {
		t.Fatalf("first query is not the V$DATABASE read: %q", f.queries[0])
	}
}

func TestDatabaseNonCDB(t *testing.T) {
	f := &fakeQuerier{responses: []sqlRows{
		rows(dbRow("NO", "ORCL", "ORCL", "NOARCHIVELOG", "NO", "NO", "NO", "NO", "NO", "NO")),
		rows([]string{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production"}),
	}}
	c := testClient(f)

	got, err := c.Database(context.Background())
	if err != nil {
		t.Fatalf("Database: %v", err)
	}
	if got.IsCDB {
		t.Fatalf("IsCDB = true, want false")
	}
	if got.PDBs != nil {
		t.Fatalf("PDBs = %v, want nil for a non-CDB", got.PDBs)
	}
	if got.LogMode != "NOARCHIVELOG" {
		t.Fatalf("LogMode = %q, want NOARCHIVELOG", got.LogMode)
	}
	if got.Edition != "Enterprise Edition" {
		t.Fatalf("Edition = %q, want Enterprise Edition", got.Edition)
	}
	// A non-CDB must not issue the V$CONTAINERS query.
	if len(f.queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(f.queries))
	}
}

// TestDatabaseVersionBanner reads V$VERSION as the real releases return it:
// one row on 23ai and 26ai, a product row before the component rows on 12c to
// 19c, and no product row at all.
func TestDatabaseVersionBanner(t *testing.T) {
	cases := []struct {
		name        string
		banners     [][]string
		wantVersion string
		wantEdition string
		wantErr     bool
	}{
		{"23ai Free (23.9)", [][]string{{banner23ai}}, banner23ai, "Free", false},
		{"26ai Free (23.26.3)", [][]string{{banner26ai}}, banner26ai, "Free", false},
		{"19c with component rows", [][]string{
			{"Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production"},
			{"PL/SQL Release 19.0.0.0.0 - Production"},
			{"CORE\t19.0.0.0.0\tProduction"},
			{"TNS for Linux: Version 19.0.0.0.0 - Production"},
			{"NLSRTL Version 19.0.0.0.0 - Production"},
		}, "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production", "Enterprise Edition", false},
		{"component rows only", [][]string{
			{"PL/SQL Release 19.0.0.0.0 - Production"},
			{"NLSRTL Version 19.0.0.0.0 - Production"},
		}, "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeQuerier{responses: []sqlRows{
				rows(dbRow("NO", "FREE", "FREE", "ARCHIVELOG", "NO", "YES", "NO", "NO", "NO", "NO")),
				rows(tc.banners...),
			}}
			got, err := testClient(f).Database(context.Background())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Database = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Database: %v", err)
			}
			if got.Version != tc.wantVersion || got.Edition != tc.wantEdition {
				t.Fatalf("Version, Edition = %q, %q; want %q, %q", got.Version, got.Edition, tc.wantVersion, tc.wantEdition)
			}
			if got.LogMode != "ARCHIVELOG" || !got.Supplemental.Minimal {
				t.Fatalf("prerequisites lost: %+v", got)
			}
		})
	}
}

func TestDatabaseQueryError(t *testing.T) {
	f := &fakeQuerier{err: fmt.Errorf("connection lost")}
	c := testClient(f)
	if _, err := c.Database(context.Background()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestTableSupplementalLogging(t *testing.T) {
	f := &fakeQuerier{responses: []sqlRows{
		rows(
			[]string{"ALL COLUMN LOGGING", "ALWAYS"},
			[]string{"UNIQUE KEY LOGGING", "ALWAYS"},
		),
		rows(), // second table: no log groups
	}}
	c := testClient(f)

	got, err := c.TableSupplementalLogging(context.Background(), []string{"APP.ORDERS", "APP.USERS"})
	if err != nil {
		t.Fatalf("TableSupplementalLogging: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if !got[0].AllColumns || !got[0].Always {
		t.Fatalf("ORDERS = %+v, want all-columns always", got[0])
	}
	if got[1].AllColumns || got[1].Always {
		t.Fatalf("USERS = %+v, want none", got[1])
	}
	// The owner and table name must be bound, not spliced as identifiers.
	if !strings.Contains(f.queries[0], `OWNER = :1`) || !strings.Contains(f.queries[0], `TABLE_NAME = :2`) {
		t.Fatalf("table names not bound: %q", f.queries[0])
	}
	if len(f.args) < 1 || len(f.args[0]) != 2 || f.args[0][0] != "APP" || f.args[0][1] != "ORDERS" {
		t.Fatalf("table bind args = %v, want [APP ORDERS]", f.args[0])
	}
}

func TestTableSupplementalLoggingInvalidTable(t *testing.T) {
	c := testClient(&fakeQuerier{})
	if _, err := c.TableSupplementalLogging(context.Background(), []string{"APP"}); err == nil {
		t.Fatal("expected error for a non-owner.table name, got nil")
	}
	if _, err := c.TableSupplementalLogging(context.Background(), []string{"APP; DROP TABLE X"}); err == nil {
		t.Fatal("expected error for an injection attempt, got nil")
	}
}

func TestLogMinerGrants(t *testing.T) {
	f := &fakeQuerier{responses: []sqlRows{
		rows([]string{"LOG_MINER"}, []string{"SELECT_CATALOG_ROLE"}),
		rows([]string{"CREATE SESSION"}, []string{"SELECT ANY TABLE"}),
		rows(
			[]string{"LOGMINING", "NO"},
			[]string{"SELECT", "NO"},
		),
	}}
	c := testClient(f)

	got, err := c.LogMinerGrants(context.Background(), "C##LOGMINER", []string{"APP.ORDERS"})
	if err != nil {
		t.Fatalf("LogMinerGrants: %v", err)
	}
	if got.User != "C##LOGMINER" {
		t.Fatalf("User = %q", got.User)
	}
	if len(got.Roles) != 2 || got.Roles[0] != "LOG_MINER" {
		t.Fatalf("Roles = %v", got.Roles)
	}
	if len(got.SysPrivs) != 2 || got.SysPrivs[1] != "SELECT ANY TABLE" {
		t.Fatalf("SysPrivs = %v", got.SysPrivs)
	}
	if len(got.TablePrivs) != 2 || got.TablePrivs[0].Privilege != "LOGMINING" {
		t.Fatalf("TablePrivs = %v", got.TablePrivs)
	}
	if !strings.Contains(f.queries[0], `GRANTEE = :1`) {
		t.Fatalf("roles grantee not bound: %q", f.queries[0])
	}
	if !strings.Contains(f.queries[1], `GRANTEE = :1`) {
		t.Fatalf("system privilege grantee not bound: %q", f.queries[1])
	}
	if !strings.Contains(f.queries[2], `GRANTEE = :1`) ||
		!strings.Contains(f.queries[2], `OWNER = :2`) ||
		!strings.Contains(f.queries[2], `TABLE_NAME = :3`) {
		t.Fatalf("table privilege names not bound: %q", f.queries[2])
	}
	if len(f.args) < 1 || len(f.args[0]) != 1 || f.args[0][0] != "C##LOGMINER" {
		t.Fatalf("roles bind args = %v, want [C##LOGMINER]", f.args[0])
	}
	if len(f.args) < 3 || len(f.args[2]) != 3 ||
		f.args[2][0] != "C##LOGMINER" || f.args[2][1] != "APP" || f.args[2][2] != "ORDERS" {
		t.Fatalf("table privilege bind args = %v, want [C##LOGMINER APP ORDERS]", f.args[2])
	}
}

func TestLogMinerGrantsInvalidUser(t *testing.T) {
	c := testClient(&fakeQuerier{})
	if _, err := c.LogMinerGrants(context.Background(), "bad user", nil); err == nil {
		t.Fatal("expected error for an invalid user name, got nil")
	}
}

// ctxAwareRows fails Next once its context is done, mimicking how database/sql
// closes *sql.Rows when their query context is cancelled.
type ctxAwareRows struct {
	ctx  context.Context
	vals []string
	pos  int
}

func (r *ctxAwareRows) Next() bool {
	if r.ctx.Err() != nil {
		return false
	}
	if r.pos < len(r.vals) {
		r.pos++
		return true
	}
	return false
}

func (r *ctxAwareRows) Scan(dest ...any) error {
	if r.pos == 0 || r.pos > len(r.vals) {
		return fmt.Errorf("ctxAwareRows: Scan without a current row")
	}
	for _, d := range dest {
		switch dp := d.(type) {
		case *sql.NullString:
			*dp = sql.NullString{String: r.vals[r.pos-1], Valid: true}
		case *string:
			*dp = r.vals[r.pos-1]
		default:
			return fmt.Errorf("ctxAwareRows: unsupported dest %T", d)
		}
	}
	return nil
}

func (r *ctxAwareRows) Err() error {
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}
	return nil
}

func (r *ctxAwareRows) Close() error { return nil }

// ctxCapturingQuerier returns rows bound to the query context so a test can
// observe whether the context is still live during iteration.
type ctxCapturingQuerier struct {
	vals []string
}

func (f *ctxCapturingQuerier) QueryContext(ctx context.Context, _ string, _ ...any) (sqlRows, error) {
	return &ctxAwareRows{ctx: ctx, vals: f.vals}, nil
}

// TestQueryKeepsContextAliveUntilRowsConsumed guards the regression where
// query() cancelled its context as soon as it returned the rows, so every
// subsequent rows.Next() reported false under a done context.
func TestQueryKeepsContextAliveUntilRowsConsumed(t *testing.T) {
	c := testClient(&ctxCapturingQuerier{vals: []string{"a", "b", "c"}})
	got, err := c.stringColumn(context.Background(), "SELECT 1 FROM DUAL", "test", nil)
	if err != nil {
		t.Fatalf("stringColumn: %v", err)
	}
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("got %v, want [a b c]", got)
	}
}

// TestNewParseConfigErrorHidesPassword guards the DSN parse error path: the
// returned error must not contain the password even when url.Parse fails.
func TestNewParseConfigErrorHidesPassword(t *testing.T) {
	const dsn = "oracle://logminer:s3cret@host:99x/FREEPDB1"
	_, err := New(context.Background(), dsn)
	if err == nil {
		t.Fatal("expected error for a bad port, got nil")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("parse config error leaks password: %v", err)
	}
}

func TestTargetOf(t *testing.T) {
	cases := []struct {
		dsn  string
		want string
	}{
		{"oracle://logminer:secret@db.example:1521/FREEPDB1", "db.example:1521/FREEPDB1"},
		{"oracle://logminer:secret@localhost:1521/XE", "localhost:1521/XE"},
		{"oracle://logminer:secret@localhost/ORCL", "localhost/ORCL"},
		{"not a url", "oracle"},
		{"%zz", "oracle"},
	}
	for _, tc := range cases {
		if got := targetOf(tc.dsn); got != tc.want {
			t.Errorf("targetOf(%q) = %q, want %q", tc.dsn, got, tc.want)
		}
	}
}

// equalDB compares DatabaseInfo field by field so a test failure names the
// field that differs.
func equalDB(a, b DatabaseInfo) bool {
	if a.Name != b.Name || a.IsCDB != b.IsCDB || a.Container != b.Container ||
		a.LogMode != b.LogMode || a.ForceLog != b.ForceLog ||
		a.Supplemental != b.Supplemental || a.Version != b.Version || a.Edition != b.Edition {
		return false
	}
	if len(a.PDBs) != len(b.PDBs) {
		return false
	}
	for i := range a.PDBs {
		if a.PDBs[i] != b.PDBs[i] {
			return false
		}
	}
	return true
}
