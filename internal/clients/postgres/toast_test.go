package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// scriptedRows returns fixed rows; Scan assigns each value to its pointer.
type scriptedRows struct {
	fakeRows
	rows [][]any
	i    int
}

func (r *scriptedRows) Next() bool { r.i++; return r.i <= len(r.rows) }
func (r *scriptedRows) Scan(dest ...any) error {
	for k, v := range r.rows[r.i-1] {
		reflect.ValueOf(dest[k]).Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

type scriptedQuerier struct {
	fakeQuerier
	rows [][]any
}

func (q *scriptedQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if _, err := q.fakeQuerier.Query(ctx, sql, args...); err != nil {
		return nil, err
	}
	return &scriptedRows{rows: q.rows}, nil
}

func toastCtx(tables ...string) context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", SourceTables: tables})
}

// toastRow is one result row: pos, found, relreplident, toast bytes,
// attname, type, attstorage, avg_width.
func toastRow(pos int64, found bool, ident string, toast int64, col, typ, storage string, avg int) []any {
	return []any{pos, found, ident, toast, col, typ, storage, avg}
}

func TestToastColumnsGroupsRows(t *testing.T) {
	q := &scriptedQuerier{rows: [][]any{
		toastRow(1, true, "d", 8192, "notes", "text", "x", 18),
		toastRow(1, true, "d", 8192, "doc", "jsonb", "x", -1),
		toastRow(2, true, "f", 0, "", "", "", -1), // no toastable column
		toastRow(3, false, "", 0, "", "", "", -1), // not found
	}}
	d := testDB(q)
	ctx := toastCtx("public.orders", "public.items", "public.gone")
	got, cut, err := d.ToastColumns(ctx, []string{"public.orders", "public.items", "public.gone"})
	if err != nil || cut {
		t.Fatalf("err %v cut %v", err, cut)
	}
	if len(got) != 3 {
		t.Fatalf("got %+v", got)
	}
	o := got[0]
	if !o.Found || o.ReplicaIdentity != "DEFAULT" || o.ToastBytes != 8192 || len(o.Columns) != 2 ||
		o.Columns[0] != (ToastColumn{Name: "notes", Type: "text", Storage: "x", AvgWidth: 18}) || o.Columns[1].AvgWidth != -1 {
		t.Errorf("orders = %+v", o)
	}
	if !got[1].Found || got[1].ReplicaIdentity != "FULL" || len(got[1].Columns) != 0 {
		t.Errorf("items = %+v", got[1])
	}
	if got[2].Found || got[2].Table != "public.gone" {
		t.Errorf("gone = %+v", got[2])
	}

	sql := q.sqls[0]
	for _, want := range []string{"FROM unnest($1::text[], $2::text[]) WITH ORDINALITY", "pg_attribute", "attstorage IN ('x', 'e')", "max(s.avg_width) FROM pg_stats", "pg_relation_size(c.reltoastrelid)", "LIMIT $3"} {
		if !strings.Contains(sql, want) {
			t.Errorf("sql lacks %q", want)
		}
	}
	// Catalog only: never a value statistic, never the table itself.
	for _, bad := range []string{"most_common_vals", "histogram_bounds", "SELECT *", "\"public\""} {
		if strings.Contains(sql, bad) {
			t.Errorf("sql reads %q", bad)
		}
	}
	a := q.args[0]
	if !reflect.DeepEqual(a[0], []string{"public", "public", "public"}) || !reflect.DeepEqual(a[1], []string{"orders", "items", "gone"}) || a[2] != MaxToastColumns+1 {
		t.Errorf("args = %#v", a)
	}
}

func TestToastColumnsRefusesBeforeIO(t *testing.T) {
	many := make([]string, MaxToastTables+1)
	for i := range many {
		many[i] = "public.t"
	}
	for _, c := range []struct {
		name   string
		ctx    context.Context
		tables []string
		want   error
	}{
		{"bad identifier", toastCtx("public.orders"), []string{"public.orders; drop"}, tools.ErrBadInput},
		{"not schema.table", toastCtx("public.orders"), []string{"orders"}, tools.ErrBadInput},
		{"too many", tools.WithUnscoped(context.Background()), many, tools.ErrBadInput},
		{"other pipeline's table", toastCtx("public.orders"), []string{"public.payments"}, tools.ErrOutOfScope},
		{"no scope", context.Background(), []string{"public.orders"}, tools.ErrNoScope},
		{"id-only scope", tools.WithPipelineID(context.Background(), "orders"), []string{"public.orders"}, tools.ErrNoScope},
	} {
		t.Run(c.name, func(t *testing.T) {
			q := &fakeQuerier{}
			_, _, err := testDB(q).ToastColumns(c.ctx, c.tables)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if len(q.sqls) != 0 {
				t.Fatalf("ran %d statements", len(q.sqls))
			}
		})
	}
}

func TestToastColumnsCutAndErrors(t *testing.T) {
	rows := make([][]any, MaxToastColumns+1)
	for i := range rows {
		rows[i] = toastRow(1, true, "d", 0, "c", "text", "x", 10)
	}
	got, cut, err := testDB(&scriptedQuerier{rows: rows}).ToastColumns(toastCtx("public.wide"), []string{"public.wide"})
	if err != nil || !cut || len(got[0].Columns) != MaxToastColumns {
		t.Fatalf("err %v cut %v columns %d", err, cut, len(got[0].Columns))
	}

	cause := errors.New("connection refused")
	_, _, err = testDB(&fakeQuerier{err: cause}).ToastColumns(toastCtx("public.orders"), []string{"public.orders"})
	if !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), "postgres: toast columns: ") {
		t.Fatalf("err = %v", err)
	}
}
