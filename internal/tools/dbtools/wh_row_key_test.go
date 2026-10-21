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
	// rowPK is the pk_column the last Row call received.
	rowPK string
}

func (w *pkWarehouse) Row(ctx context.Context, table, pkColumn, key string) (map[string]any, bool, error) {
	w.rowPK = pkColumn
	return w.fakeWarehouse.Row(ctx, table, pkColumn, key)
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

// Snowflake folds unquoted identifiers to upper case, so the agent may pass
// the key as "ID" (the name Snowflake reports) while the client returns "id"
// (#441). Postgres stays case-sensitive: "ID" and "id" are two columns there.
func TestWarehouseRowPKCaseByKind(t *testing.T) {
	for _, tc := range []struct {
		kind, col string
		ok        bool
	}{
		{"snowflake", "ID", true},
		{"snowflake", "id", true},
		{"snowflake", "Email", false},
		{"databricks", "ID", true},
		{"databricks", "id", true},
		{"databricks", "Email", false},
		{"clickhouse", "id", true},
		{"clickhouse", "ID", false},
		{"bigquery", "id", true},
		{"bigquery", "ID", false},
		{"postgres", "id", true},
		{"postgres", "ID", false},
		{"", "ID", false},
	} {
		wh := &pkWarehouse{fakeWarehouse: testDeps().Wh.(*fakeWarehouse), pk: "id"}
		d := testDeps()
		d.Wh, d.WarehouseKind = wh, tc.kind
		_, err := runTool(t, whRowTool(t, d), `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"`+tc.col+`","key":"7"}`)
		if tc.ok && (err != nil || wh.rowCalls != 1) {
			t.Errorf("%q %s: err = %v, row calls %d, want a read", tc.kind, tc.col, err, wh.rowCalls)
		}
		if tc.ok && wh.rowPK != "id" {
			t.Errorf("%q %s: Row got pk_column %q, want the canonical \"id\"", tc.kind, tc.col, wh.rowPK)
		}
		if !tc.ok && (!errors.Is(err, tools.ErrBadInput) || wh.rowCalls != 0) {
			t.Errorf("%q %s: err = %v, row calls %d, want ErrBadInput and no read", tc.kind, tc.col, err, wh.rowCalls)
		}
	}
}
