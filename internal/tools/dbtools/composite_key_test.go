package dbtools

import (
	"errors"
	"fmt"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// A composite primary key cannot be looked up by one value: pg_row and
// warehouse_row refuse instead of returning whichever row shares the first
// key column (#483).
func TestRowToolsRefuseCompositeKey(t *testing.T) {
	composite := fmt.Errorf("postgres: primary key is composite (2 columns): %w", clients.ErrNotSupported)

	src := &pkSource{fakeSource: testDeps().Src.(*fakeSource), pkErr: composite}
	d := testDeps()
	d.Src = src
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	pg, _ := reg.Get("pg_row")
	if _, err := runTool(t, pg, `{"pipeline_id":"orders","table":"public.orders","pk_column":"tenant_id","key":"42"}`); !errors.Is(err, tools.ErrBadInput) || src.rowCalls != 0 {
		t.Fatalf("pg_row: err = %v, row calls %d; want ErrBadInput and no read", err, src.rowCalls)
	}

	wh := &pkWarehouse{fakeWarehouse: testDeps().Wh.(*fakeWarehouse), pkErr: composite}
	d = testDeps()
	d.Wh = wh
	if _, err := runTool(t, whRowTool(t, d), `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"tenant_id","key":"42"}`); !errors.Is(err, tools.ErrBadInput) || wh.rowCalls != 0 {
		t.Fatalf("warehouse_row: err = %v, row calls %d; want ErrBadInput and no read", err, wh.rowCalls)
	}
}
