package pgconv

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

type fakeTx struct {
	pgx.Tx
	rollbacks int
}

func (t *fakeTx) Rollback(context.Context) error { t.rollbacks++; return nil }

type fakeRows struct {
	pgx.Rows
	left   int
	closed bool
}

func (r *fakeRows) Next() bool { r.left--; return r.left >= 0 }
func (r *fakeRows) Close()     { r.closed = true }

type fakeRow struct{ err error }

func (r fakeRow) Scan(...any) error { return r.err }

// The transaction ends exactly once, whether the caller reads to the end,
// closes early, or both, so the connection goes back to the pool.
func TestTxRowsEndOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		next  int // Next calls before Close
		close bool
	}{
		{"read to end then close", 3, true},
		{"read to end, no close", 3, false},
		{"close early", 1, true},
		{"close without reading", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{}
			fr := &fakeRows{left: 2}
			r := &txRows{Rows: fr, ctx: context.Background(), tx: tx}
			for i := 0; i < tc.next; i++ {
				r.Next()
			}
			if tc.close {
				r.Close()
				r.Close()
			}
			want := 0
			if tc.close || tc.next > 2 {
				want = 1
			}
			if tx.rollbacks != want || (want == 1 && !fr.closed) {
				t.Fatalf("rollbacks = %d (closed %v), want %d", tx.rollbacks, fr.closed, want)
			}
		})
	}
}

func TestTxRowScanEnds(t *testing.T) {
	tx := &fakeTx{}
	r := &txRow{Row: fakeRow{}, ctx: context.Background(), tx: tx}
	if err := r.Scan(); err != nil || tx.rollbacks != 1 {
		t.Fatalf("err = %v, rollbacks = %d; want nil and 1", err, tx.rollbacks)
	}
}
