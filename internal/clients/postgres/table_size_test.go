package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestTableSize(t *testing.T) {
	q := &addStateQuerier{row: []any{int64(8 << 20), int64(1 << 20)}}
	ts, err := testDB(q).TableSize(context.Background(), "public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if ts.TableBytes != 8<<20 || ts.IndexBytes != 1<<20 || ts.TotalBytes != 9<<20 {
		t.Fatalf("size = %+v", ts)
	}
	if !q.deadline {
		t.Fatal("the read must run with a deadline")
	}
	if a := q.args[0]; a[0] != "public" || a[1] != "orders" || strings.Contains(q.sqls[0], "orders") {
		t.Fatalf("identifiers must travel as parameters: %v\n%s", a, q.sqls[0])
	}
	// pg_partition_tree is empty for an ordinary table; the table's own oid
	// must be in the sum or every ordinary table would read as 0 bytes.
	if !strings.Contains(q.sqls[0], "UNION") {
		t.Fatalf("the query must sum the table itself too: %s", q.sqls[0])
	}
}

func TestTableSizeMissingRelation(t *testing.T) {
	q := &addStateQuerier{}
	if _, err := testDB(q).TableSize(context.Background(), "public.gone"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestTableSizeRefusesBadIdentifiersBeforeIO(t *testing.T) {
	for _, tbl := range []string{"orders", `public."orders"`, "public.orders;drop", "a.b.c", ""} {
		q := &addStateQuerier{}
		if _, err := testDB(q).TableSize(context.Background(), tbl); err == nil {
			t.Errorf("%q: no error", tbl)
		}
		if len(q.sqls) != 0 {
			t.Errorf("%q: queried before the identifier check", tbl)
		}
	}
}
