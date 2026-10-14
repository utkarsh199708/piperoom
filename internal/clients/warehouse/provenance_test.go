package warehouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/tools"
)

type fakeRows struct {
	pgx.Rows // unused methods panic: the tests only drive Next/Close/Err
}

func (fakeRows) Next() bool { return false }
func (fakeRows) Close()     {}
func (fakeRows) Err() error { return nil }

func (fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

type fakeRow struct{ err error }

func (r fakeRow) Scan(dest ...any) error {
	if r.err == nil {
		if p, ok := dest[0].(*int); ok {
			*p = 3
		}
	}
	return r.err
}

// fakeQuerier answers every statement with no rows (or err) and remembers them.
type fakeQuerier struct {
	err  error
	sqls []string
	args [][]any
}

func (f *fakeQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.sqls, f.args = append(f.sqls, sql), append(f.args, args)
	if f.err != nil {
		return nil, f.err
	}
	return fakeRows{}, nil
}

func (f *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.sqls, f.args = append(f.sqls, sql), append(f.args, args)
	return fakeRow{err: f.err}
}

func testDB(q querier) *DB { return &DB{q: q, target: "warehouse orders@wh.internal:5432"} }

func recordCtx() (context.Context, *tools.Recorder) {
	rec := tools.NewRecorder()
	return tools.WithRecorder(context.Background(), rec), rec
}

func TestSQLTextCollapsesWhitespaceKeepsPlaceholders(t *testing.T) {
	got := sqlText("\n\t\tSELECT  a,\n  b\n\tFROM t\n\tWHERE x = $1   AND y = ANY($2::text[])\n")
	want := "SELECT a, b FROM t WHERE x = $1 AND y = ANY($2::text[])"
	if got != want {
		t.Fatalf("sqlText = %q, want %q", got, want)
	}
}

func TestRedactArgsTable(t *testing.T) {
	when := time.Date(2026, 10, 14, 5, 0, 0, 0, time.FixedZone("x", 2*3600))
	long := strings.Repeat("a", 100)
	cases := []struct {
		name string
		args []any
		want []string
	}{
		{"name", []any{"debezium_orders"}, []string{"$1=debezium_orders"}},
		{"string slice", []any{[]string{"a", "b"}}, []string{"$1=[a,b]"}},
		{"empty slice", []any{[]string{}}, []string{"$1=[]"}},
		{"time", []any{when}, []string{"$1=2026-10-14T03:00:00Z"}},
		{"duration", []any{90 * time.Second}, []string{"$1=1m30s"}},
		{"numbers", []any{42, int64(7), 1.5}, []string{"$1=42", "$2=7", "$3=1.5"}},
		{"long string", []any{long}, []string{"$1=" + strings.Repeat("a", 64) + "…"}},
		{"exec mode skipped", []any{pgx.QueryExecModeDescribeExec, "pk-1"}, []string{"$1=pk-1"}},
		{"bytes never shown", []any{[]byte("secret")}, []string{"$1=<6 bytes>"}},
		{"nil", []any{nil}, []string{"$1=NULL"}},
		{"none", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactArgs(tc.args)
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCutParamKeepsRuneBoundary(t *testing.T) {
	got := cutParam(strings.Repeat("é", 40)) // 80 bytes
	if !strings.HasSuffix(got, "…") || strings.ContainsRune(got, '�') {
		t.Fatalf("cut = %q", got)
	}
}

func TestWarehouseFreshnessRecordsTarget(t *testing.T) {
	d := testDB(&fakeQuerier{})
	ctx, rec := recordCtx()
	if _, err := d.Freshness(ctx, "analytics.orders", "updated_at"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	c := calls[0]
	if c.Kind != tools.CallSQL || c.Target != "warehouse orders@wh.internal:5432" || c.Text != `SELECT max("updated_at") FROM "analytics"."orders"` {
		t.Fatalf("call = %+v", c)
	}
}

func TestWarehouseErrorStillRecords(t *testing.T) {
	d := testDB(&fakeQuerier{err: errors.New("connection refused")})
	ctx, rec := recordCtx()
	if _, err := d.Freshness(ctx, "analytics.orders", "updated_at"); err == nil {
		t.Fatal("want error")
	}
	if _, _, err := d.Row(ctx, "analytics.orders", "id", "o-1"); err == nil {
		t.Fatal("want error")
	}
	if n := len(rec.Calls()); n != 2 {
		t.Fatalf("got %d calls, want 2", n)
	}
}

func TestWarehouseRowKeepsPK(t *testing.T) {
	d := testDB(&fakeQuerier{})
	ctx, rec := recordCtx()
	if _, _, err := d.Row(ctx, "analytics.orders", "id", "o-1001"); err != nil {
		t.Fatal(err)
	}
	if p := rec.Calls()[0].ParamsRedacted; len(p) != 1 || p[0] != "$1=o-1001" {
		t.Fatalf("params = %v", p)
	}
}
