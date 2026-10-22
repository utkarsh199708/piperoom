package clients_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
)

func TestReconcileSampleSize(t *testing.T) {
	tests := []struct{ n, want int }{
		{-1, 0},
		{0, 0},
		{1, 1},
		{50, 50},
		{clients.ReconcileMaxSample, clients.ReconcileMaxSample},
		{clients.ReconcileMaxSample + 1, clients.ReconcileMaxSample},
		{500, 200},
	}
	for _, tt := range tests {
		if got := clients.ReconcileSampleSize(tt.n); got != tt.want {
			t.Errorf("ReconcileSampleSize(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestReconcileCaps(t *testing.T) {
	// The ticket fixes these numbers; implementations and the watcher share them.
	if clients.ReconcileMaxSample != 200 || clients.ReconcileMaxKeys != 1000 || clients.ReconcileTimeout != 10*time.Second {
		t.Fatalf("caps changed: sample %d, keys %d, timeout %s", clients.ReconcileMaxSample, clients.ReconcileMaxKeys, clients.ReconcileTimeout)
	}
}

// errBadInput stands in for tools.ErrBadInput (clients must not import tools).
var errBadInput = errors.New("bad input")

// fakeReconciler proves the shape can be implemented and asserted without a
// local adapter: it is what X1a's clients and X1b's watcher fakes look like.
type fakeReconciler struct {
	keys  []string
	times map[string]map[string]time.Time
	calls int
}

var _ clients.Reconciler = (*fakeReconciler)(nil)

func (f *fakeReconciler) CountBetween(_ context.Context, _, _ string, from, to time.Time) (int64, error) {
	if to.Before(from) {
		return 0, fmt.Errorf("fake: window: %w", errBadInput)
	}
	f.calls++
	return int64(len(f.keys)), nil
}

func (f *fakeReconciler) SampleKeys(_ context.Context, _, _, _ string, _ time.Time, n int) ([]string, error) {
	size := clients.ReconcileSampleSize(n)
	if size == 0 {
		return nil, fmt.Errorf("fake: n %d: %w", n, errBadInput)
	}
	f.calls++
	return f.keys[:min(size, len(f.keys))], nil
}

func (f *fakeReconciler) RowTimes(_ context.Context, _, _ string, keys []string, tsColumns []string) (map[string]map[string]time.Time, error) {
	if len(keys) > clients.ReconcileMaxKeys || len(tsColumns) == 0 {
		return nil, fmt.Errorf("fake: %d keys, %d columns: %w", len(keys), len(tsColumns), errBadInput)
	}
	out := map[string]map[string]time.Time{}
	if len(keys) == 0 {
		return out, nil
	}
	f.calls++
	for _, k := range keys {
		if row, ok := f.times[k]; ok {
			out[k] = row
		}
	}
	return out, nil
}

// The reader is optional: a client without it still satisfies the existing
// interfaces, and callers find it by type assertion.
func TestReconcilerIsAnOptionalAssertion(t *testing.T) {
	var w clients.WarehouseReader = struct{ clients.WarehouseReader }{}
	if _, ok := w.(clients.Reconciler); ok {
		t.Fatal("a plain WarehouseReader must not be a Reconciler")
	}
	var v any = &fakeReconciler{}
	if _, ok := v.(clients.Reconciler); !ok {
		t.Fatal("fake does not satisfy clients.Reconciler")
	}
}

func TestFakeReconcilerHonoursCaps(t *testing.T) {
	keys := make([]string, 500)
	for i := range keys {
		keys[i] = fmt.Sprint(i)
	}
	f := &fakeReconciler{keys: keys, times: map[string]map[string]time.Time{"1": {"updated_at": time.Unix(0, 0).UTC()}}}
	ctx := context.Background()

	got, err := f.SampleKeys(ctx, "public.orders", "id", "updated_at", time.Time{}, 500)
	if err != nil || len(got) != clients.ReconcileMaxSample {
		t.Fatalf("SampleKeys(500) = %d keys, %v; want %d", len(got), err, clients.ReconcileMaxSample)
	}
	if _, err := f.SampleKeys(ctx, "public.orders", "id", "updated_at", time.Time{}, 0); !errors.Is(err, errBadInput) {
		t.Fatalf("SampleKeys(0) err = %v, want bad input", err)
	}

	before := f.calls
	if _, err := f.RowTimes(ctx, "public.orders", "id", make([]string, clients.ReconcileMaxKeys+1), []string{"updated_at"}); !errors.Is(err, errBadInput) {
		t.Fatalf("RowTimes(1001 keys) err = %v, want bad input", err)
	}
	if f.calls != before {
		t.Fatal("RowTimes over the cap did I/O")
	}
	rows, err := f.RowTimes(ctx, "public.orders", "id", []string{"1", "2"}, []string{"updated_at"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rows["2"]; ok || len(rows) != 1 {
		t.Fatalf("RowTimes: a key with no row must be absent, got %v", rows)
	}
}
