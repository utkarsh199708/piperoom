package wiring

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
)

// countingWarehouse is a warehouse with the X2a range reads and the X1
// window count; its WarehouseReader methods are never called.
type countingWarehouse struct {
	clients.Warehouse
	calls int
}

func (w *countingWarehouse) RangeColumn(context.Context, string, string) (postgres.RangeColumn, error) {
	w.calls++
	return postgres.RangeColumn{Type: "int8", Kind: "integer"}, nil
}

func (w *countingWarehouse) CountRange(context.Context, string, string, string, string) (int64, error) {
	w.calls++
	return 7, nil
}

func (w *countingWarehouse) CountBetween(context.Context, string, string, time.Time, time.Time) (int64, error) {
	w.calls++
	return 9, nil
}

// pg_resnapshot_rows gets a sink view that forwards only the counts, and a
// warehouse without them answers ErrNotSupported (unverifiable).
func TestResnapshotSinkView(t *testing.T) {
	if sinkView(nil) != nil {
		t.Fatal("no warehouse must be a nil sink")
	}
	ctx := context.Background()
	wh := &countingWarehouse{}
	v := sinkView(wh)
	if _, err := v.RangeColumn(ctx, "a.b", "id"); err != nil {
		t.Fatal(err)
	}
	if n, err := v.CountRange(ctx, "a.b", "id", "1", "2"); err != nil || n != 7 {
		t.Fatalf("CountRange = %d, %v", n, err)
	}
	if n, err := v.CountBetween(ctx, "a.b", "ts", time.Now(), time.Now()); err != nil || n != 9 || wh.calls != 3 {
		t.Fatalf("CountBetween = %d, %v (calls %d)", n, err, wh.calls)
	}

	bare := sinkView(struct{ clients.Warehouse }{})
	if _, err := bare.RangeColumn(ctx, "a.b", "id"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("RangeColumn on a bare warehouse = %v", err)
	}
	if _, err := bare.CountRange(ctx, "a.b", "id", "1", ""); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CountRange on a bare warehouse = %v", err)
	}
	if _, err := bare.CountBetween(ctx, "a.b", "ts", time.Now(), time.Now()); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CountBetween on a bare warehouse = %v", err)
	}
}
