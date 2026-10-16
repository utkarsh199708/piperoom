package topology

import (
	"context"
	"fmt"
	"testing"

	"piperoom/internal/clients"
)

// compositeSource reports a composite primary key, as the postgres client
// does for PRIMARY KEY (tenant_id, id).
type compositeSource struct {
	*fakeSource
	rowCalls int
}

func (s *compositeSource) PrimaryKeyColumn(context.Context, string) (string, error) {
	return "", fmt.Errorf("postgres: primary key of public.orders is composite (2 columns): %w", clients.ErrNotSupported)
}

func (s *compositeSource) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	s.rowCalls++
	return s.fakeSource.Row(ctx, table, pk, key)
}

// With a composite key, a lookup by one value reads another row (tenant 42's
// order instead of order 42): the trace reads nothing and judges no stage
// (#483).
func TestTraceCompositeKeyIsUnknown(t *testing.T) {
	src := &compositeSource{fakeSource: &fakeSource{rows: map[string]map[string]any{
		"public.orders|42": {"tenant_id": 42, "id": 7},
	}}}
	b := NewBuilder(
		testOwners(),
		&fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}}},
		&fakeKafka{},
		src,
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "42")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if len(stages) != 5 {
		t.Fatalf("stages = %+v, want 5", stages)
	}
	for _, s := range stages {
		if s.Status != "unknown" || s.Facts["error"] != "composite primary key: the trace needs every key column" {
			t.Fatalf("stage %s = %s %v, want unknown with the composite-key reason", s.Layer, s.Status, s.Facts)
		}
	}
	if src.rowCalls != 0 {
		t.Fatalf("read %d row(s) by a partial key", src.rowCalls)
	}
}
