package topology

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// keyedWarehouse reports the sink's primary key like the warehouse client.
type keyedWarehouse struct {
	*fakeWarehouse
	col      string
	err      error
	rowCalls int
}

func (w *keyedWarehouse) PrimaryKeyColumn(context.Context, string) (string, error) {
	return w.col, w.err
}

func (w *keyedWarehouse) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	w.rowCalls++
	return w.fakeWarehouse.Row(ctx, table, pk, key)
}

func traceWithSink(t *testing.T, wh *keyedWarehouse) (string, map[string]string) {
	t.Helper()
	b := NewBuilder(
		testOwners(),
		&fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}}},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": 7}}},
		wh,
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	for _, s := range stages {
		if s.Layer == "warehouse" {
			return s.Status, s.Facts
		}
	}
	t.Fatalf("no warehouse stage in %+v", stages)
	return "", nil
}

// Source PK (id), sink PK (tenant_id, id): the sink row for id=7 may be
// another tenant's, so the warehouse stage is unknown and no row is read (#537).
func TestTraceCompositeSinkKeyIsUnknown(t *testing.T) {
	wh := &keyedWarehouse{
		fakeWarehouse: &fakeWarehouse{rows: map[string]map[string]any{"analytics.orders|7": {"tenant_id": 2, "id": 7}}},
		err:           fmt.Errorf("warehouse: composite: %w", clients.ErrNotSupported),
	}
	status, facts := traceWithSink(t, wh)
	if status != "unknown" || facts["error"] == "" {
		t.Fatalf("warehouse = %s %v, want unknown with a reason", status, facts)
	}
	if wh.rowCalls != 0 {
		t.Fatalf("read %d sink row(s) by a partial key", wh.rowCalls)
	}
}

func TestTraceSinkKeyColumnDiffersIsUnknown(t *testing.T) {
	wh := &keyedWarehouse{fakeWarehouse: &fakeWarehouse{}, col: "order_id"}
	status, _ := traceWithSink(t, wh)
	if status != "unknown" || wh.rowCalls != 0 {
		t.Fatalf("status %s, rowCalls %d; want unknown, 0", status, wh.rowCalls)
	}
}

func TestTraceSinkKeyMatchingReadsRow(t *testing.T) {
	wh := &keyedWarehouse{fakeWarehouse: &fakeWarehouse{}, col: "ID"}
	traceWithSink(t, wh)
	if wh.rowCalls != 1 {
		t.Fatalf("rowCalls = %d, want 1", wh.rowCalls)
	}
}

// A failed or empty sink-key lookup leaves the key unknown: the stage is
// unknown and no sink row is read by the source key (#537 review).
func TestTraceSinkKeyLookupFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		col  string
		err  error
	}{
		{"timeout", "", context.DeadlineExceeded},
		{"permission denied", "", fmt.Errorf("warehouse: permission denied for table orders")},
		{"table missing", "", fmt.Errorf("warehouse: %w", clients.ErrNotFound)},
		{"empty column", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wh := &keyedWarehouse{
				fakeWarehouse: &fakeWarehouse{rows: map[string]map[string]any{"analytics.orders|7": {"tenant_id": 2, "id": 7}}},
				col:           tc.col,
				err:           tc.err,
			}
			status, facts := traceWithSink(t, wh)
			if status != "unknown" || !strings.Contains(facts["error"], "sink key unknown") {
				t.Fatalf("warehouse = %s %v, want unknown with 'sink key unknown'", status, facts)
			}
			if wh.rowCalls != 0 {
				t.Fatalf("read %d sink row(s) with an unknown sink key", wh.rowCalls)
			}
		})
	}
}
