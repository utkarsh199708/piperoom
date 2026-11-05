package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
)

// addStateQuerier answers PublicationAddState's one QueryRow with row (nil:
// no rows) and records the arguments and whether ctx had a deadline.
type addStateQuerier struct {
	fakeQuerier
	row      []any
	deadline bool
}

func (q *addStateQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	_, q.deadline = ctx.Deadline()
	q.sqls, q.args = append(q.sqls, sql), append(q.args, args)
	if q.row == nil {
		return fakeRow{err: pgx.ErrNoRows}
	}
	return scanRow(func(dest ...any) error {
		for i, v := range q.row {
			reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
		}
		return nil
	})
}

func TestPublicationAddStateReadsOneRow(t *testing.T) {
	q := &addStateQuerier{row: []any{false, []string{"public.customers"}, 2, true, "r", "d", true, false, int64(1200)}}
	st, err := testDB(q).PublicationAddState(context.Background(), "dbz_publication", "public.orders", []string{"public.orders", "public.customers"})
	if err != nil {
		t.Fatal(err)
	}
	want := PublicationAddState{Members: []string{"public.customers"}, OtherMembers: 2, TableExists: true, RelKind: "r", ReplicaIdentity: "d", HasPrimaryKey: true, EstimatedRows: 1200}
	if !reflect.DeepEqual(st, want) {
		t.Fatalf("state = %+v, want %+v", st, want)
	}
	if !q.deadline {
		t.Fatal("the read must run with a deadline")
	}
	args := q.args[0]
	if args[0] != "dbz_publication" || args[1] != "public" || args[2] != "orders" {
		t.Fatalf("identifiers must travel as parameters, got %v", args[:3])
	}
	if !reflect.DeepEqual(args[3], []string{"public", "public"}) || !reflect.DeepEqual(args[4], []string{"orders", "customers"}) {
		t.Fatalf("visible tables = %v %v", args[3], args[4])
	}
	if strings.Contains(q.sqls[0], "orders") || strings.Contains(q.sqls[0], "dbz_publication") {
		t.Fatal("no input may be built into the statement")
	}
}

func TestPublicationAddStateNoVisibleSendsEmptyArrays(t *testing.T) {
	q := &addStateQuerier{row: []any{false, []string{}, 5, false, "", "", false, false, int64(-1)}}
	st, err := testDB(q).PublicationAddState(context.Background(), "dbz_publication", "public.orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.TableExists || st.OtherMembers != 5 || st.EstimatedRows != -1 {
		t.Fatalf("state = %+v", st)
	}
	if a, ok := q.args[0][3].([]string); !ok || a == nil {
		t.Fatalf("visible schemas must be a non-nil array, got %#v", q.args[0][3])
	}
}

func TestPublicationAddStateMissingPublication(t *testing.T) {
	_, err := testDB(&addStateQuerier{}).PublicationAddState(context.Background(), "dbz_publication", "public.orders", nil)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestPublicationAddStateRefusesBadIdentifiersBeforeIO(t *testing.T) {
	cases := []struct {
		name, pub, table string
		visible          []string
	}{
		{"publication injection", `dbz"; DROP PUBLICATION x; --`, "public.orders", nil},
		{"publication blank", "", "public.orders", nil},
		{"table injection", "dbz", "public.orders; DROP TABLE x", nil},
		{"table quoted", "dbz", `public."orders"`, nil},
		{"table no schema", "dbz", "orders", nil},
		{"visible injection", "dbz", "public.orders", []string{"public.x' OR '1'='1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &addStateQuerier{}
			if _, err := testDB(q).PublicationAddState(context.Background(), tc.pub, tc.table, tc.visible); err == nil {
				t.Fatal("expected a refusal")
			}
			if len(q.sqls) != 0 {
				t.Fatal("a refused call must not reach the database")
			}
		})
	}
}

func TestErrAlreadyPublishedIsTheContractSentinel(t *testing.T) {
	tx := &fakeTx{scan: pubRow(false, "public.orders")}
	w, _ := testWriter(tx)
	err := w.AddPublicationTables(unscopedCtx(), "dbz_publication", []string{"public.orders"})
	if !errors.Is(err, clients.ErrAlreadyPublished) {
		t.Fatalf("err = %v, want clients.ErrAlreadyPublished", err)
	}
}
