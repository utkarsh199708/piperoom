package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestEstimatedRows(t *testing.T) {
	q := &addStateQuerier{row: []any{int64(10_000_000)}}
	n, err := testDB(q).EstimatedRows(context.Background(), "public.orders")
	if err != nil || n != 10_000_000 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	if !q.deadline {
		t.Fatal("the read must run with a deadline")
	}
	if a := q.args[0]; a[0] != "public" || a[1] != "orders" || strings.Contains(q.sqls[0], "orders") {
		t.Fatalf("identifiers must travel as parameters: %v\n%s", a, q.sqls[0])
	}

	// Never analysed.
	q = &addStateQuerier{row: []any{int64(-1)}}
	if n, err := testDB(q).EstimatedRows(context.Background(), "public.orders"); err != nil || n != -1 {
		t.Fatalf("never analysed: %d %v", n, err)
	}
	// Missing table.
	q = &addStateQuerier{}
	if _, err := testDB(q).EstimatedRows(context.Background(), "public.gone"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestEstimatedRowsRefusesBadIdentifiersBeforeIO(t *testing.T) {
	for _, tbl := range []string{"orders", `public."orders"`, "public.orders;drop", "a.b.c", ""} {
		q := &addStateQuerier{}
		if _, err := testDB(q).EstimatedRows(context.Background(), tbl); err == nil {
			t.Errorf("%q: no error", tbl)
		}
		if len(q.sqls) != 0 {
			t.Errorf("%q: queried before the identifier check", tbl)
		}
	}
}
