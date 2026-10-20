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

// fakeQuerier returns canned results in order and records every query text.
type fakeQuerier struct {
	responses []sqlRows
	queries   []string
	err       error
}

func (f *fakeQuerier) QueryContext(_ context.Context, query string, _ ...any) (sqlRows, error) {
	f.queries = append(f.queries, query)
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

func testClient(f *fakeQuerier) *Client {
	return &Client{q: f, target: "oracle test"}
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
		rows(
			[]string{"Oracle Database 23ai Free Edition Release 23.0.0.0.0 - Production"},
			[]string{"NLSRTL Version 23.0.0.0.0 - Production"},
		),
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
		Version: "Oracle Database 23ai Free Edition Release 23.0.0.0.0 - Production",
		Edition: "Free Edition",
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
	// The table name must be quoted in the emitted SQL.
	if !strings.Contains(f.queries[0], `OWNER = "APP"`) || !strings.Contains(f.queries[0], `TABLE_NAME = "ORDERS"`) {
		t.Fatalf("table not quoted: %q", f.queries[0])
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
	if !strings.Contains(f.queries[2], `GRANTEE = "C##LOGMINER"`) {
		t.Fatalf("grantee not quoted: %q", f.queries[2])
	}
}

func TestLogMinerGrantsInvalidUser(t *testing.T) {
	c := testClient(&fakeQuerier{})
	if _, err := c.LogMinerGrants(context.Background(), "bad user", nil); err == nil {
		t.Fatal("expected error for an invalid user name, got nil")
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
