package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// ownersQuerier answers the role and publication rows by statement text and
// the table query with prepared rows.
type ownersQuerier struct {
	fakeQuerier
	role     func(dest ...any) error
	pub      func(dest ...any) error
	rows     [][]any
	deadline int
}

func (q *ownersQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if _, ok := ctx.Deadline(); ok {
		q.deadline++
	}
	if q.err != nil {
		return nil, q.err
	}
	return &scriptedRows{rows: q.rows}, nil
}

func (q *ownersQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if _, ok := ctx.Deadline(); ok {
		q.deadline++
	}
	if strings.Contains(sql, "pg_publication p") {
		return scanRow(q.pub)
	}
	return scanRow(q.role)
}

func TestObjectOwners(t *testing.T) {
	q := &ownersQuerier{
		role: func(dest ...any) error {
			*dest[0].(*bool), *dest[1].(*bool) = false, true
			return nil
		},
		pub: func(dest ...any) error {
			*dest[0].(*bool), *dest[1].(*string) = false, "orders_owner"
			*dest[2].(*bool), *dest[3].(*bool) = false, false
			return nil
		},
		rows: [][]any{
			{int64(2), false, "", false, false, false},
			{int64(1), true, "orders_owner", false, false, true},
		},
	}
	got, err := testDB(q).ObjectOwners(context.Background(), "piperoom", "dbz_publication", []string{"public.orders", "public.gone"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.RoleExists || got.Superuser || !got.Replication {
		t.Errorf("role = %+v", got)
	}
	if p := got.Publication; !p.Exists || p.Owner != "orders_owner" || p.Name != "dbz_publication" || p.RoleIsOwner {
		t.Errorf("publication = %+v", p)
	}
	if len(got.Tables) != 2 || got.Tables[0].Table != "public.orders" || got.Tables[0].Owner != "orders_owner" || !got.Tables[0].InPublication {
		t.Errorf("tables = %+v", got.Tables)
	}
	if g := got.Tables[1]; g.Table != "public.gone" || g.Exists {
		t.Errorf("missing table = %+v", g)
	}
	if q.deadline != 3 {
		t.Errorf("%d of 3 statements had a deadline", q.deadline)
	}
	// Names travel as parameters only.
	last := q.args[2]
	if last[0] != "dbz_publication" || last[1] != "piperoom" {
		t.Errorf("table query args = %v", last)
	}
	if strings.Contains(q.sqls[2], "orders") || !strings.Contains(q.sqls[2], "pg_has_role(r.oid, c.relowner, 'USAGE')") {
		t.Errorf("table SQL = %s", q.sqls[2])
	}
}

func TestObjectOwnersMissingRoleAndPublication(t *testing.T) {
	q := &ownersQuerier{
		role: func(...any) error { return pgx.ErrNoRows },
		pub:  func(...any) error { return pgx.ErrNoRows },
		rows: [][]any{{int64(1), true, "orders_owner", false, false, false}},
	}
	got, err := testDB(q).ObjectOwners(context.Background(), "gone", "dbz_publication", []string{"public.orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got.RoleExists || got.Publication.Exists || got.Tables[0].Owner != "orders_owner" {
		t.Errorf("got %+v", got)
	}
}

func TestObjectOwnersErrors(t *testing.T) {
	boom := errors.New("connection refused")
	ok := func(dest ...any) error { return nil }
	for name, q := range map[string]*ownersQuerier{
		"role read":  {role: func(...any) error { return boom }, pub: ok},
		"pub read":   {role: ok, pub: func(...any) error { return boom }},
		"table read": {role: ok, pub: ok, fakeQuerier: fakeQuerier{err: boom}},
	} {
		if _, err := testDB(q).ObjectOwners(context.Background(), "r", "p", []string{"public.orders"}); !errors.Is(err, boom) {
			t.Errorf("%s: err = %v, want the wrapped read error", name, err)
		}
	}
	q := &ownersQuerier{role: ok, pub: ok, rows: [][]any{{int64(7), true, "x", false, false, false}}}
	if _, err := testDB(q).ObjectOwners(context.Background(), "r", "p", []string{"public.orders"}); err == nil {
		t.Error("a row for a table that was not asked for was accepted")
	}
}

func TestObjectOwnersRefusesBadInputBeforeIO(t *testing.T) {
	for _, c := range []struct{ role, pub, table string }{
		{"", "p", "public.orders"},
		{"r", "", "public.orders"},
		{"r", "p", `public.orders"; DROP TABLE x; --`},
		{"r", "p", "orders"},
	} {
		q := &ownersQuerier{}
		if _, err := testDB(q).ObjectOwners(context.Background(), c.role, c.pub, []string{c.table}); err == nil {
			t.Errorf("%+v accepted", c)
		}
		if len(q.sqls) != 0 {
			t.Errorf("%+v: read before validating: %v", c, q.sqls)
		}
	}
}
