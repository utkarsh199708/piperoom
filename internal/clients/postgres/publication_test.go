package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
)

// assign copies one scripted column into a Scan destination.
func assign(dest, v any) error {
	switch d := dest.(type) {
	case *string:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("scripted %T into *string", v)
		}
		*d = s
	case **string: // a nullable text column
		if v == nil {
			*d = nil
			return nil
		}
		s := v.(string)
		*d = &s
	case *bool:
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("scripted %T into *bool", v)
		}
		*d = b
	case *int:
		n, ok := v.(int)
		if !ok {
			return fmt.Errorf("scripted %T into *int", v)
		}
		*d = n
	default:
		return fmt.Errorf("scripted scan into %T is not supported", dest)
	}
	return nil
}

func assignAll(dest, vals []any) error {
	if len(dest) != len(vals) {
		return fmt.Errorf("scan wants %d columns, the script has %d", len(dest), len(vals))
	}
	for i := range dest {
		if err := assign(dest[i], vals[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}

type scriptedRows struct {
	pgx.Rows
	rows [][]any
	i    int
}

func (r *scriptedRows) Next() bool { r.i++; return r.i <= len(r.rows) }
func (r *scriptedRows) Scan(dest ...any) error {
	return assignAll(dest, r.rows[r.i-1])
}
func (r *scriptedRows) Close()     {}
func (r *scriptedRows) Err() error { return nil }

type scriptedRow struct {
	vals []any
	err  error
}

func (r scriptedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assignAll(dest, r.vals)
}

// pubQuerier answers the publication statements from a script and remembers
// the SQL text and arguments of every call.
type pubQuerier struct {
	exists     bool
	srcRows    [][]any // columns of sourcePrivileges
	memberRows [][]any // columns of publicationMembers
	other      int
	info       []any // columns of PublicationInfo; nil = no such publication
	err        error
	sqls       []string
	args       [][]any
}

func (q *pubQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if q.err != nil {
		return nil, q.err
	}
	switch {
	case strings.Contains(sql, "FROM (VALUES"):
		return &scriptedRows{rows: q.srcRows}, nil
	case strings.Contains(sql, "JOIN pg_namespace n ON n.nspname = pt.schemaname"):
		return &scriptedRows{rows: q.memberRows}, nil
	}
	return nil, fmt.Errorf("unscripted query: %s", sql)
}

func (q *pubQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if q.err != nil {
		return scriptedRow{err: q.err}
	}
	switch {
	case strings.Contains(sql, "SELECT EXISTS"):
		return scriptedRow{vals: []any{q.exists}}
	case strings.Contains(sql, "count(*)"):
		return scriptedRow{vals: []any{q.other}}
	case strings.Contains(sql, "FROM pg_publication p"):
		if q.info == nil {
			return scriptedRow{err: pgx.ErrNoRows}
		}
		return scriptedRow{vals: q.info}
	}
	return scriptedRow{err: fmt.Errorf("unscripted query row: %s", sql)}
}

// sqlWith returns the first recorded statement that contains s.
func (q *pubQuerier) sqlWith(t *testing.T, s string) (string, []any) {
	t.Helper()
	for i, sql := range q.sqls {
		if strings.Contains(sql, s) {
			return sql, q.args[i]
		}
	}
	t.Fatalf("no statement contains %q: %v", s, q.sqls)
	return "", nil
}

func TestPublicationSourceTablesOwnerAndMembership(t *testing.T) {
	cases := []struct {
		name       string
		row        []any // schema, table, in_publication, selectable, owner, role_member
		wantOwner  string
		wantIsOwn  bool
		wantMember bool
	}{
		// The connector-auth break: the table moved to orders_owner, the role
		// debezium is neither it nor a member.
		{"not owner, not member", []any{"public", "orders", true, true, "orders_owner", false}, "orders_owner", false, false},
		{"member of the owner", []any{"public", "orders", true, true, "orders_owner", true}, "orders_owner", false, true},
		{"is the owner", []any{"public", "orders", true, true, "debezium", true}, "debezium", true, true},
		// A table that does not exist has no owner, and no ownership.
		{"missing table", []any{"public", "gone", false, false, "", false}, "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &pubQuerier{exists: true, srcRows: [][]any{tc.row}}
			got, other, err := testDB(q).PublicationSourceTables(context.Background(), "dbz_publication", "debezium", []string{"public." + tc.row[1].(string)})
			if err != nil {
				t.Fatal(err)
			}
			if other != 0 || len(got) != 1 {
				t.Fatalf("got %+v, other %d", got, other)
			}
			g := got[0]
			if g.Owner != tc.wantOwner || g.RoleIsOwner != tc.wantIsOwn || g.RoleMemberOfOwner != tc.wantMember {
				t.Fatalf("got owner=%q is_owner=%v member=%v, want %q %v %v", g.Owner, g.RoleIsOwner, g.RoleMemberOfOwner, tc.wantOwner, tc.wantIsOwn, tc.wantMember)
			}
			if g.InPublication != tc.row[2].(bool) || g.Selectable != tc.row[3].(bool) {
				t.Fatalf("existing columns changed: %+v", g)
			}
		})
	}
}

func TestPublicationSourceTablesSQLIsParameterised(t *testing.T) {
	q := &pubQuerier{exists: true, srcRows: [][]any{
		{"public", "orders", true, true, "o", false},
		{"sales", "items", true, true, "o", false},
	}}
	_, _, err := testDB(q).PublicationSourceTables(context.Background(), "dbz_publication", "debezium", []string{"public.orders", "sales.items"})
	if err != nil {
		t.Fatal(err)
	}
	sql, args := q.sqlWith(t, "FROM (VALUES")
	for _, want := range []string{"pg_get_userbyid(c.relowner)", "FROM pg_roles WHERE rolname = $2::name", "pg_has_role($2::name, c.relowner, 'USAGE')"} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL lacks %q:\n%s", want, sql)
		}
	}
	// Names and tables reach the server only as arguments, in this order.
	if fmt.Sprint(args) != "[dbz_publication debezium public orders sales items]" {
		t.Errorf("args = %v", args)
	}
	for _, lit := range []string{"dbz_publication", "debezium", "orders", "sales", "items"} {
		if strings.Contains(sql, lit) {
			t.Errorf("SQL carries the value %q", lit)
		}
	}
	if !strings.Contains(sql, "(1, $3::name, $4::name), (2, $5::name, $6::name)") {
		t.Errorf("VALUES numbering changed:\n%s", sql)
	}
	// The count of other members still comes in SQL, not by reading names.
	q.sqlWith(t, "count(*)")
}

func TestPublicationTablesMembersCarryOwners(t *testing.T) {
	q := &pubQuerier{
		exists: true,
		memberRows: [][]any{
			{"public", "orders", true, "orders_owner", false},
			{"public", "payments", false, "pay_owner", false},
		},
		srcRows: [][]any{{"public", "orders", true, true, "orders_owner", false}},
	}
	got, err := testDB(q).PublicationTables(context.Background(), "dbz_publication", "debezium", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %+v", got)
	}
	// The listed source table first, then the other member; neither is owned by the role.
	if got[0].Table != "orders" || got[0].Owner != "orders_owner" || got[0].RoleIsOwner || got[0].RoleMemberOfOwner {
		t.Errorf("source table = %+v", got[0])
	}
	if got[1].Table != "payments" || got[1].Owner != "pay_owner" || got[1].Selectable {
		t.Errorf("member = %+v", got[1])
	}
	sql, args := q.sqlWith(t, "JOIN pg_namespace n ON n.nspname = pt.schemaname")
	if !strings.Contains(sql, "FROM pg_roles WHERE rolname = $2::name") || !strings.Contains(sql, "pg_has_role($2::name, c.relowner, 'USAGE')") || fmt.Sprint(args) != "[dbz_publication debezium]" {
		t.Errorf("members query:\n%s\nargs %v", sql, args)
	}
}

func TestPublicationTablesMemberIsOwnedByRole(t *testing.T) {
	q := &pubQuerier{
		exists:     true,
		memberRows: [][]any{{"public", "orders", true, "debezium", true}},
	}
	got, err := testDB(q).PublicationTables(context.Background(), "p", "debezium", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].RoleIsOwner || !got[0].RoleMemberOfOwner || got[0].Owner != "debezium" {
		t.Fatalf("got %+v", got)
	}
}

func TestPublicationInfo(t *testing.T) {
	cases := []struct {
		name string
		info []any // owner, role name (nil = no role), replication, superuser, member
		want PublicationInfo
	}{
		{
			"role owns neither: the connector-auth break",
			[]any{"orders_owner", "debezium", true, false, false},
			PublicationInfo{Publication: "dbz_publication", Owner: "orders_owner", Role: "debezium", RoleExists: true, RoleReplication: true},
		},
		{
			"role owns the publication",
			[]any{"debezium", "debezium", true, false, true},
			PublicationInfo{Publication: "dbz_publication", Owner: "debezium", Role: "debezium", RoleExists: true, RoleReplication: true, RoleIsOwner: true, RoleMemberOfOwner: true},
		},
		{
			"no REPLICATION attribute",
			[]any{"dba", "debezium", false, false, false},
			PublicationInfo{Publication: "dbz_publication", Owner: "dba", Role: "debezium", RoleExists: true},
		},
		{
			"superuser",
			[]any{"dba", "debezium", false, true, true},
			PublicationInfo{Publication: "dbz_publication", Owner: "dba", Role: "debezium", RoleExists: true, RoleSuperuser: true, RoleMemberOfOwner: true},
		},
		{
			"role was dropped",
			[]any{"dba", nil, false, false, false},
			PublicationInfo{Publication: "dbz_publication", Owner: "dba", Role: "debezium"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &pubQuerier{info: tc.info}
			got, err := testDB(q).PublicationInfo(context.Background(), "dbz_publication", "debezium")
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got  %+v\nwant %+v", got, tc.want)
			}
			sql, args := q.sqlWith(t, "FROM pg_publication p")
			if fmt.Sprint(args) != "[dbz_publication debezium]" {
				t.Errorf("args = %v", args)
			}
			for _, lit := range []string{"dbz_publication", "debezium"} {
				if strings.Contains(sql, lit) {
					t.Errorf("SQL carries the value %q", lit)
				}
			}
			// Catalog reads only.
			for _, bad := range []string{"INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "GRANT"} {
				if strings.Contains(strings.ToUpper(sql), bad) {
					t.Errorf("SQL contains %s", bad)
				}
			}
		})
	}
}

func TestPublicationInfoErrors(t *testing.T) {
	t.Run("missing publication", func(t *testing.T) {
		_, err := testDB(&pubQuerier{}).PublicationInfo(context.Background(), "nope", "debezium")
		if !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("database error", func(t *testing.T) {
		boom := errors.New("connection refused")
		_, err := testDB(&pubQuerier{err: boom}).PublicationInfo(context.Background(), "p", "r")
		if !errors.Is(err, boom) || errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), `"r"`) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("sources: missing publication", func(t *testing.T) {
		_, _, err := testDB(&pubQuerier{exists: false}).PublicationSourceTables(context.Background(), "nope", "r", []string{"public.t"})
		if !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("sources: database error", func(t *testing.T) {
		boom := errors.New("permission denied for pg_publication")
		_, _, err := testDB(&pubQuerier{err: boom}).PublicationSourceTables(context.Background(), "p", "r", []string{"public.t"})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("sources: bad table name never reaches SQL", func(t *testing.T) {
		q := &pubQuerier{exists: true}
		_, _, err := testDB(q).PublicationSourceTables(context.Background(), "p", "r", []string{"public.orders; DROP TABLE x"})
		if err == nil {
			t.Fatal("want an error for a bad table name")
		}
		for _, sql := range q.sqls {
			if strings.Contains(sql, "DROP") {
				t.Fatalf("statement carries the table name: %s", sql)
			}
		}
	})
}
