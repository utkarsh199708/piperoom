package dbtools

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/tools"
)

// pkWarehouse is a fakeWarehouse that reports the table's primary key, as the
// Postgres warehouse client does.
type pkWarehouse struct {
	*fakeWarehouse
	pk    string
	pkErr error
}

func (w *pkWarehouse) PrimaryKeyColumn(context.Context, string) (string, error) { return w.pk, w.pkErr }

func whRowTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("warehouse_row")
	return tt
}

// warehouse_row reads by primary key only. A lookup by another column (an
// email, a tax id) is an existence oracle for personal data in the sink and
// puts the probed value in the room log; pg_row was closed in #333.
func TestWarehouseRowRequiresThePrimaryKey(t *testing.T) {
	wh := &pkWarehouse{fakeWarehouse: testDeps().Wh.(*fakeWarehouse), pk: "id"}
	d := testDeps()
	d.Wh = wh
	tt := whRowTool(t, d)

	for _, col := range []string{"email", "customer_id", "loaded_at"} {
		_, err := runTool(t, tt, `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"`+col+`","key":"7"}`)
		if !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("pk_column %s: err = %v, want ErrBadInput", col, err)
		}
	}
	if wh.rowCalls != 0 {
		t.Fatalf("read %d row(s) by a non-key column", wh.rowCalls)
	}
	if _, err := runTool(t, tt, `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`); err != nil || wh.rowCalls != 1 {
		t.Fatalf("by primary key: err = %v, row calls %d", err, wh.rowCalls)
	}

	wh.pk, wh.pkErr = "", errors.New("warehouse: primary key of analytics.orders: not found")
	if _, err := runTool(t, tt, `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`); err == nil || wh.rowCalls != 1 {
		t.Fatalf("key lookup failed: err = %v, row calls %d; want an error and no read", err, wh.rowCalls)
	}
}
