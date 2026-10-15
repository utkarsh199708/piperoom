package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// deadlineQuerier is fakeQuerier that also remembers whether each statement ran
// under a deadline (the statement timeout).
type deadlineQuerier struct {
	fakeQuerier
	deadlines []time.Duration
}

func (q *deadlineQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if dl, ok := ctx.Deadline(); ok {
		q.deadlines = append(q.deadlines, time.Until(dl))
	} else {
		q.deadlines = append(q.deadlines, 0)
	}
	return q.fakeQuerier.Query(ctx, sql, args...)
}

func TestTableActivityQuery(t *testing.T) {
	q := &deadlineQuerier{}
	d := testDB(q)
	if _, err := d.TableCounters(context.Background(), []string{"public.orders", "public.order_items"}); err != nil {
		t.Fatal(err)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("%d statements", len(q.sqls))
	}
	sql := q.sqls[0]
	for _, want := range []string{"FROM pg_stat_user_tables", "n_tup_ins", "n_tup_upd", "n_tup_del", "n_live_tup", "last_autovacuum", "last_autoanalyze", "= ANY($1::text[])"} {
		if !strings.Contains(sql, want) {
			t.Errorf("sql lacks %s: %s", want, sql)
		}
	}
	if strings.Contains(sql, "orders") {
		t.Errorf("table names must be parameters, not SQL text: %s", sql)
	}
	args := q.args[0]
	got, ok := args[0].([]string)
	if len(args) != 1 || !ok || strings.Join(got, ",") != "public.orders,public.order_items" {
		t.Errorf("args = %#v", args)
	}
	if dl := q.deadlines[0]; dl <= 0 || dl > 10*time.Second {
		t.Errorf("statement must run under the 10 s deadline, got %v", dl)
	}
}

func TestTableActivityRejectsBadInput(t *testing.T) {
	for name, tables := range map[string][]string{
		"no schema": {"orders"},
		"injection": {"public.orders; drop table x"},
		"quoted":    {`public."orders"`},
	} {
		t.Run(name, func(t *testing.T) {
			q := &fakeQuerier{}
			if _, err := testDB(q).TableCounters(context.Background(), tables); err == nil {
				t.Fatal("want error")
			}
			if len(q.sqls) != 0 {
				t.Fatalf("ran %d statements for bad input", len(q.sqls))
			}
		})
	}
}

func TestTableActivityNoTablesRunsNothing(t *testing.T) {
	q := &fakeQuerier{}
	got, err := testDB(q).TableCounters(context.Background(), nil)
	if err != nil || len(got) != 0 || len(q.sqls) != 0 {
		t.Fatalf("got %v, %v, %d statements", got, err, len(q.sqls))
	}
}

func TestTableActivityQueryError(t *testing.T) {
	boom := errors.New("connection refused")
	q := &fakeQuerier{err: boom}
	_, err := testDB(q).TableCounters(context.Background(), []string{"public.orders"})
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "postgres: table activity") {
		t.Fatalf("err = %v", err)
	}
}
