package topology

import (
	"context"
	"fmt"
	"strings"
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

// keyErrSource answers the key lookup with err, and counts row reads.
type keyErrSource struct {
	*fakeSource
	col      string
	err      error
	rowCalls int
}

func (s *keyErrSource) PrimaryKeyColumn(context.Context, string) (string, error) { return s.col, s.err }

func (s *keyErrSource) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	s.rowCalls++
	return s.fakeSource.Row(ctx, table, pk, key)
}

// The trace fails closed on the key: a table without a primary key (the
// Postgres client's ErrNotFound), a failed lookup, or an empty answer leaves
// every stage unknown and reads no row by "id". The raw lookup error never
// reaches the trace.
func TestTraceKeyLookupFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, col, want string
		err             error
	}{
		{"no primary key", "", "no primary key", fmt.Errorf("postgres: primary key of public.orders: %w", clients.ErrNotFound)},
		{"lookup failed", "", "lookup failed", fmt.Errorf("dial tcp db.internal:5432 user=svc: connection refused")},
		{"empty answer", "", "no primary key", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &keyErrSource{fakeSource: &fakeSource{rows: map[string]map[string]any{"public.orders|42": {"id": 42}}}, col: tc.col, err: tc.err}
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
				t.Fatalf("stages = %+v", stages)
			}
			for _, s := range stages {
				if s.Status != "unknown" || !strings.Contains(s.Facts["error"], tc.want) || strings.Contains(s.Facts["error"], "db.internal") {
					t.Fatalf("stage %s = %s %v, want unknown (%s)", s.Layer, s.Status, s.Facts, tc.want)
				}
			}
			if src.rowCalls != 0 {
				t.Fatalf("read %d row(s) without a known key", src.rowCalls)
			}
		})
	}
}

// A source client without a key lookup at all is the only case that reads
// by "id".
func TestTraceWithoutKeyLookupReadsByID(t *testing.T) {
	var _ pkColumner = (*fakeSource)(nil)
	b := &builder{src: struct{ clients.SourceDB }{&fakeSource{}}}
	col, err := b.pkColumn(context.Background(), "public.orders")
	if err != nil || col != "id" {
		t.Fatalf("pkColumn without a key lookup = %q %v, want id", col, err)
	}
}
