package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The age-independent read: no age filter, walsenders and this backend left
// out, no query text or user selected, identity only on request.
func TestOpenTransactionsSQL(t *testing.T) {
	q := &fakeQuerier{}
	got, err := testDB(q).OpenTransactions(context.Background(), []string{"public.orders"}, false)
	if err != nil {
		t.Fatal(err)
	}
	// fakeRow puts 3 in the first int; fakeRows has no rows.
	if got.Open != 3 || got.Oldest != nil || got.OldestWriter != nil {
		t.Fatalf("got %+v", got)
	}
	if len(q.sqls) != 2 {
		t.Fatalf("%d statements, want 2 (counts, oldest)", len(q.sqls))
	}
	for i, sql := range q.sqls {
		for _, want := range []string{"a.pid <> pg_backend_pid()", "a.backend_type IS DISTINCT FROM 'walsender'", "COALESCE(a.application_name, '') = $3", "backend_xid"} {
			if !strings.Contains(sql, want) {
				t.Errorf("statement %d lacks %q", i, want)
			}
		}
		// No age threshold: a young writer must be counted.
		for _, banned := range []string{"make_interval", "query", "usename", "client_addr"} {
			if strings.Contains(sql, banned) {
				t.Errorf("statement %d contains %q:\n%s", i, banned, sql)
			}
		}
		args := q.args[i]
		if len(args) < 3 || args[2] != readerAppName {
			t.Errorf("statement %d args = %v, want $3 = %q", i, args, readerAppName)
		}
	}
	if !strings.Contains(q.sqls[0], "pg_prepared_xacts") {
		t.Errorf("counts statement does not count prepared transactions")
	}
	if args := q.args[1]; len(args) != 4 || args[3] != false {
		t.Errorf("oldest statement args = %v, want $4 = false", args)
	}
	if !strings.Contains(q.sqls[1], "CASE WHEN on_tables OR $4::bool THEN COALESCE(application_name, '') ELSE '' END") {
		t.Errorf("application_name is not limited to sessions on the tables:\n%s", q.sqls[1])
	}
}

func TestOpenTransactionsErrors(t *testing.T) {
	down := errors.New("dial tcp: connection refused")
	cases := []struct {
		name   string
		err    error
		tables []string
		want   string
	}{
		{"target down", down, nil, "postgres: count open transactions"},
		{"timeout", context.DeadlineExceeded, nil, "postgres: count open transactions"},
		{"bad table", nil, []string{"orders; DROP TABLE x"}, "table must be"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := &fakeQuerier{err: c.err}
			_, err := testDB(q).OpenTransactions(context.Background(), c.tables, true)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to contain %q", err, c.want)
			}
			if c.err != nil && !errors.Is(err, c.err) {
				t.Fatalf("err = %v does not wrap %v", err, c.err)
			}
			if c.tables != nil && len(q.sqls) != 0 {
				t.Fatalf("a bad table name reached SQL: %q", q.sqls)
			}
		})
	}
}
