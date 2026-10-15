package topology

import (
	"context"
	"errors"
	"testing"
	"time"
)

// With Connect unreachable the trace cannot ask which connector streams the
// table, and used to read any table of the source database (and its key in
// the sink): a row-existence oracle from POST /api/rooms/{id}/trace (QA
// sweep, "trace leaves pipeline scope"). The table must then be one of the
// pipeline's source tables.
func TestTraceConnectDownStaysInScope(t *testing.T) {
	src := &fakeSource{rows: map[string]map[string]any{
		"public.orders|7":  {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)},
		"public.payroll|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)},
	}}
	b := NewBuilder(
		testOwners(),
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("EOF")}},
		&fakeKafka{},
		src,
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.payroll", "7")
	if !errors.Is(err, ErrBadInput) {
		t.Fatalf("out-of-pipeline table with Connect down: stages %+v, err %v; want ErrBadInput", stages, err)
	}
	if _, err := b.Trace(context.Background(), "orders", "public.orders", "7"); err != nil {
		t.Fatalf("the pipeline's own table must still trace: %v", err)
	}
}
