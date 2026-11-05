package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// migRows is one canned result set; Scan fills *string, *bool and *time.Time.
type migRows struct {
	pgx.Rows
	rows [][]any
	i    int
	err  error
}

func (r *migRows) Next() bool { r.i++; return r.err == nil && r.i <= len(r.rows) }
func (r *migRows) Close()     {}
func (r *migRows) Err() error { return r.err }
func (r *migRows) Scan(dest ...any) error {
	for i, v := range r.rows[r.i-1] {
		switch d := dest[i].(type) {
		case *string:
			*d = v.(string)
		case *bool:
			*d = v.(bool)
		case *time.Time:
			*d = v.(time.Time)
		default:
			return fmt.Errorf("unexpected destination %T", dest[i])
		}
	}
	return nil
}

// migResult answers one statement: rows, or an error from Query.
type migResult struct {
	rows [][]any
	err  error
}

// migQuerier answers statements in order and records them.
type migQuerier struct {
	fakeQuerier
	results []migResult
}

func (q *migQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	_, _ = q.fakeQuerier.Query(ctx, sql, args...)
	n := len(q.sqls) - 1
	if n >= len(q.results) {
		return nil, fmt.Errorf("unexpected statement %d: %s", n, sql)
	}
	r := q.results[n]
	if r.err != nil {
		return nil, r.err
	}
	return &migRows{rows: r.rows}, nil
}

var migSince = time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)

func TestRecentMigrationsFlywayAndLiquibase(t *testing.T) {
	at := func(m int) time.Time { return migSince.Add(time.Duration(m) * time.Minute) }
	q := &migQuerier{results: []migResult{
		{rows: [][]any{
			{"app", "databasechangelog", true},
			{"public", "flyway_schema_history", true},
		}},
		{rows: [][]any{ // liquibase, app schema
			{"create-orders-7", "addColumn tableName=orders", "dana", at(20), true},
		}},
		{rows: [][]any{ // flyway, public schema
			{"2.4", "widen amount", "deployer", at(30), true},
			{"2.3", "add index", "deployer", at(10), false},
		}},
	}}
	got, err := testDB(q).RecentMigrations(context.Background(), []string{"public", "app"}, migSince, 200)
	if err != nil {
		t.Fatalf("RecentMigrations: %v", err)
	}
	if len(got.Migrations) != 3 {
		t.Fatalf("migrations = %+v, want 3", got.Migrations)
	}
	var order []string
	for _, m := range got.Migrations {
		order = append(order, m.Source+":"+m.Version)
	}
	if want := "flyway:2.4,liquibase:create-orders-7,flyway:2.3"; strings.Join(order, ",") != want {
		t.Errorf("order = %v, want newest first %s", order, want)
	}
	if m := got.Migrations[0]; m.Table != "public.flyway_schema_history" || m.InstalledBy != "deployer" || !m.Success {
		t.Errorf("flyway row = %+v", m)
	}
	if got.Migrations[2].Success {
		t.Error("a failed Flyway migration must keep success=false")
	}
	if len(got.Tables) != 2 || got.Tables[0].State != HistoryRead || got.Tables[1].State != HistoryRead {
		t.Errorf("tables = %+v", got.Tables)
	}

	// Parameters only in the lookup; the read names the catalog's table, quoted.
	if args := q.args[0]; fmt.Sprint(args[1]) != "[public app]" {
		t.Errorf("lookup args = %v", args)
	}
	for _, want := range []string{`FROM "app"."databasechangelog"`, `FROM "public"."flyway_schema_history"`} {
		if !strings.Contains(strings.Join(q.sqls, "\n"), want) {
			t.Errorf("no statement reads %s", want)
		}
	}
	// Metadata only: never the script, its checksum or a whole row (E6).
	for _, s := range q.sqls {
		for _, banned := range []string{`\bscript\b`, `\bchecksum\b`, `\bmd5sum\b`, `\bfilename\b`, `SELECT \*`, `\bcomments\b`} {
			if regexp.MustCompile(banned).MatchString(s) {
				t.Errorf("statement selects %q: %s", banned, s)
			}
		}
		if !strings.HasPrefix(strings.TrimSpace(s), "SELECT") {
			t.Errorf("not a read: %s", s)
		}
	}
}

func TestRecentMigrationsNoHistoryTable(t *testing.T) {
	q := &migQuerier{results: []migResult{{}}}
	got, err := testDB(q).RecentMigrations(context.Background(), []string{"public"}, migSince, 50)
	if err != nil {
		t.Fatalf("no history table must not be an error: %v", err)
	}
	if len(got.Migrations) != 0 || len(got.Tables) != 0 || got.Migrations == nil {
		t.Errorf("got %+v, want empty non-nil", got)
	}
	if len(q.sqls) != 1 {
		t.Errorf("%d statements, want the catalog lookup only", len(q.sqls))
	}
}

func TestRecentMigrationsUnreadable(t *testing.T) {
	// No privilege by the catalog: the table is not read at all.
	q := &migQuerier{results: []migResult{{rows: [][]any{{"public", "flyway_schema_history", false}}}}}
	got, err := testDB(q).RecentMigrations(context.Background(), []string{"public"}, migSince, 50)
	if err != nil {
		t.Fatalf("RecentMigrations: %v", err)
	}
	if len(got.Tables) != 1 || got.Tables[0].State != HistoryUnreadable || len(q.sqls) != 1 {
		t.Errorf("tables = %+v after %d statements", got.Tables, len(q.sqls))
	}

	// A privilege error at read time (a column-level grant) is the same fact.
	q = &migQuerier{results: []migResult{
		{rows: [][]any{{"public", "databasechangelog", true}}},
		{err: &pgconn.PgError{Code: "42501", Message: "permission denied for table databasechangelog"}},
	}}
	got, err = testDB(q).RecentMigrations(context.Background(), []string{"public"}, migSince, 50)
	if err != nil || len(got.Tables) != 1 || got.Tables[0].State != HistoryUnreadable {
		t.Errorf("read-time 42501: %+v, %v", got.Tables, err)
	}
}

func TestRecentMigrationsUnknownShapeAndBadSchemaName(t *testing.T) {
	q := &migQuerier{results: []migResult{
		{rows: [][]any{
			{"Odd-Schema", "flyway_schema_history", true}, // outside rule 6: never quoted into SQL
			{"public", "flyway_schema_history", true},
		}},
		{err: &pgconn.PgError{Code: "42703", Message: `column "installed_rank" does not exist`}},
	}}
	got, err := testDB(q).RecentMigrations(context.Background(), []string{"public", "Odd-Schema"}, migSince, 50)
	if err != nil {
		t.Fatalf("RecentMigrations: %v", err)
	}
	if len(got.Tables) != 2 || got.Tables[0].State != HistoryUnknownShape || got.Tables[1].State != HistoryUnknownShape {
		t.Errorf("tables = %+v", got.Tables)
	}
	if len(q.sqls) != 2 || strings.Contains(q.sqls[1], "Odd-Schema") {
		t.Errorf("statements = %q", q.sqls)
	}
}

func TestRecentMigrationsErrors(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name    string
		schemas []string
		since   time.Time
		limit   int
	}{
		{"limit 0", []string{"public"}, migSince, 0},
		{"limit over cap", []string{"public"}, migSince, MaxMigrations + 1},
		{"zero since", []string{"public"}, time.Time{}, 10},
		{"empty schema", []string{""}, migSince, 10},
		{"NUL schema", []string{"pub\x00lic"}, migSince, 10},
		{"long schema", []string{strings.Repeat("s", 64)}, migSince, 10},
		{"too many schemas", make([]string, maxMigrationSchemas+1), migSince, 10},
	} {
		q := &migQuerier{}
		if _, err := testDB(q).RecentMigrations(ctx, c.schemas, c.since, c.limit); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: err = %v, want ErrBadInput", c.name, err)
		}
		if len(q.sqls) != 0 {
			t.Errorf("%s: %d statements before refusing", c.name, len(q.sqls))
		}
	}

	// No schema: nothing to look up, no I/O.
	q := &migQuerier{}
	if got, err := testDB(q).RecentMigrations(ctx, nil, migSince, 10); err != nil || len(q.sqls) != 0 || got.Migrations == nil {
		t.Errorf("no schemas: %+v %v after %d statements", got, err, len(q.sqls))
	}

	// A backend error is wrapped with its SQLSTATE, never the server's text.
	const secret = "row value zoe@example.com"
	q = &migQuerier{results: []migResult{
		{rows: [][]any{{"public", "flyway_schema_history", true}}},
		{err: &pgconn.PgError{Code: "22P02", Message: secret}},
	}}
	_, err := testDB(q).RecentMigrations(ctx, []string{"public"}, migSince, 10)
	if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "22P02") {
		t.Errorf("backend error = %v", err)
	}

	// A timeout keeps its cause.
	q = &migQuerier{results: []migResult{{err: context.DeadlineExceeded}}}
	if _, err := testDB(q).RecentMigrations(ctx, []string{"public"}, migSince, 10); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout = %v", err)
	}

	// Rows over the limit across tables are cut, newest kept.
	at := func(m int) time.Time { return migSince.Add(time.Duration(m) * time.Minute) }
	q = &migQuerier{results: []migResult{
		{rows: [][]any{{"a", "flyway_schema_history", true}, {"b", "flyway_schema_history", true}}},
		{rows: [][]any{{"1", "x", "u", at(1), true}, {"2", "y", "u", at(3), true}}},
		{rows: [][]any{{"3", "z", "u", at(2), true}}},
	}}
	got, err := testDB(q).RecentMigrations(ctx, []string{"a", "b"}, migSince, 2)
	if err != nil || len(got.Migrations) != 2 || got.Migrations[0].Version != "2" || got.Migrations[1].Version != "3" {
		t.Errorf("limit across tables: %+v, %v", got.Migrations, err)
	}
	if q.args[1][1] != 2 {
		t.Errorf("per-table LIMIT = %v, want the call's limit", q.args[1][1])
	}
	if !q.args[1][0].(time.Time).Equal(migSince) {
		t.Errorf("since arg = %v", q.args[1][0])
	}
	if q.args[0][0].([]string)[0] != flywayHistoryTable {
		t.Errorf("lookup names = %v", q.args[0][0])
	}
	if got.Migrations[0].Source != clients.ChangeSourceFlyway || got.Migrations[0].Table != "a.flyway_schema_history" {
		t.Errorf("row = %+v", got.Migrations[0])
	}
}
