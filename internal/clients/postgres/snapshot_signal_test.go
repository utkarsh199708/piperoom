package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestSnapshotSignalStateReadsOneRow(t *testing.T) {
	q := &addStateQuerier{row: []any{true, true, true, true, []bool{true, false}, []int64{5000, 0}}}
	st, err := testDB(q).SnapshotSignalState(context.Background(), "dbz_publication", "debezium.signal", []string{"public.orders", "public.gone"})
	if err != nil {
		t.Fatal(err)
	}
	want := SnapshotSignalState{
		PublicationExists: true, SignalExists: true, SignalColumns: true, SignalPublished: true,
		Tables: []SnapshotTableState{{Table: "public.orders", Exists: true, EstimatedRows: 5000}, {Table: "public.gone", EstimatedRows: -1}},
	}
	if !reflect.DeepEqual(st, want) {
		t.Fatalf("state = %+v, want %+v", st, want)
	}
	if !q.deadline {
		t.Fatal("the read must run with a deadline")
	}
	args := q.args[0]
	if args[0] != "dbz_publication" || args[1] != "debezium" || args[2] != "signal" {
		t.Fatalf("identifiers must travel as parameters, got %v", args[:3])
	}
	if !reflect.DeepEqual(args[3], []string{"public", "public"}) || !reflect.DeepEqual(args[4], []string{"orders", "gone"}) {
		t.Fatalf("tables = %v %v", args[3], args[4])
	}
	for _, s := range []string{"orders", "dbz_publication", "debezium"} {
		if strings.Contains(q.sqls[0], s) {
			t.Fatalf("input %q built into the statement", s)
		}
	}
}

func TestSnapshotSignalStateRefusesBadIdentifiersBeforeIO(t *testing.T) {
	cases := []struct {
		pub, sig string
		tables   []string
	}{
		{`dbz"; DROP TABLE x; --`, "public.sig", []string{"public.orders"}},
		{"dbz_publication", "sig", []string{"public.orders"}},
		{"dbz_publication", "public.sig;x", []string{"public.orders"}},
		{"dbz_publication", "public.sig", []string{"public.orders.x"}},
		{"dbz_publication", "public.sig", []string{`public."orders"`}},
	}
	for _, c := range cases {
		q := &addStateQuerier{}
		if _, err := testDB(q).SnapshotSignalState(context.Background(), c.pub, c.sig, c.tables); err == nil {
			t.Errorf("%q %q %q: no error", c.pub, c.sig, c.tables)
		}
		if len(q.sqls) != 0 {
			t.Errorf("%q %q %q: queried before the identifier check", c.pub, c.sig, c.tables)
		}
	}
}

func TestSnapshotSignalStateShortArraysAreAnError(t *testing.T) {
	q := &addStateQuerier{row: []any{true, true, true, true, []bool{true}, []int64{1}}}
	if _, err := testDB(q).SnapshotSignalState(context.Background(), "p", "public.sig", []string{"public.a", "public.b"}); err == nil {
		t.Fatal("mismatched arrays accepted")
	}
}

func TestSnapshotSignalStateQueryError(t *testing.T) {
	// addStateQuerier with no row answers pgx.ErrNoRows: the statement always
	// returns one row, so any error is a read failure, wrapped.
	_, err := testDB(&addStateQuerier{}).SnapshotSignalState(context.Background(), "p", "public.sig", []string{"public.a"})
	if err == nil || !strings.Contains(err.Error(), "read signal table public.sig") || errors.Unwrap(err) == nil {
		t.Fatalf("err = %v", err)
	}
}
