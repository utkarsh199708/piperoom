package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools"
)

// leakyWarehouse answers Freshness the way the old Postgres client failed on
// a non-timestamp column: its error quotes max(col). It records the column.
type leakyWarehouse struct {
	*fakeWarehouse
	values map[string]string // column -> its max() value
	cols   []string
}

func (w *leakyWarehouse) Freshness(_ context.Context, table, col string) (time.Time, error) {
	w.cols = append(w.cols, col)
	if v, ok := w.values[col]; ok {
		return time.Time{}, fmt.Errorf("warehouse: freshness of %s: can't scan into dest[0] (col: max): invalid PostgreSQL date/time value %q", table, v)
	}
	return time.Now().Add(-time.Minute), nil
}

// #564: warehouse_freshness reads only the pipeline's sink_ts_column. Any
// other column of an in-scope sink table is refused before the warehouse is
// asked, so no value of it reaches the error, Summary or Data.
func TestWarehouseFreshnessOnlySinkTSColumn(t *testing.T) {
	values := map[string]string{
		"email":  "zoe.secret@example.com", // text
		"card":   "4111111111111111",       // card-like
		"amount": "99.5",                   // numeric
	}
	for _, col := range []string{"email", "card", "amount", "id", "UPDATED_AT"} {
		t.Run(col, func(t *testing.T) {
			d := testDeps()
			wh := &leakyWarehouse{fakeWarehouse: d.Wh.(*fakeWarehouse), values: values}
			d.Wh = wh
			reg := tools.NewRegistry()
			if err := Register(reg, d); err != nil {
				t.Fatal(err)
			}
			tt, _ := reg.Get("warehouse_freshness")
			in := fmt.Sprintf(`{"table":"analytics.orders","ts_column":%q}`, col)
			r, err := tt.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(in))
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
			if len(wh.cols) != 0 {
				t.Fatalf("warehouse was asked for max(%v)", wh.cols)
			}
			data, _ := json.Marshal(r.Data)
			for _, out := range []string{err.Error(), r.Summary, string(data)} {
				if v := values[col]; v != "" && strings.Contains(out, v) {
					t.Fatalf("value %q leaked: %s", v, out)
				}
			}
			if !strings.Contains(err.Error(), `"updated_at"`) {
				t.Fatalf("error should name the allowed column: %v", err)
			}
		})
	}
}

func TestWarehouseFreshnessSinkTSColumnWorks(t *testing.T) {
	for _, tc := range []struct {
		kind, col string
	}{{"postgres", "updated_at"}, {"snowflake", "UPDATED_AT"}} {
		t.Run(tc.kind, func(t *testing.T) {
			d := testDeps()
			d.WarehouseKind = tc.kind
			wh := &leakyWarehouse{fakeWarehouse: d.Wh.(*fakeWarehouse)}
			d.Wh = wh
			reg := tools.NewRegistry()
			if err := Register(reg, d); err != nil {
				t.Fatal(err)
			}
			tt, _ := reg.Get("warehouse_freshness")
			table := "analytics.orders"
			if tc.kind == "snowflake" {
				d.Owners.Pipelines[0].SinkTables = []string{"dwh.analytics.orders"}
				table = "dwh.analytics.orders"
			}
			in := fmt.Sprintf(`{"table":%q,"ts_column":%q}`, table, tc.col)
			if _, err := tt.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(in)); err != nil {
				t.Fatalf("sink_ts_column refused: %v", err)
			}
			// The canonical owners.yaml spelling goes to the warehouse.
			if len(wh.cols) != 1 || wh.cols[0] != "updated_at" {
				t.Fatalf("warehouse asked for %v, want [updated_at]", wh.cols)
			}
		})
	}
}

// A pipeline with no sink_ts_column cannot be read at all, rather than
// falling back to whatever column the agent names.
func TestWarehouseFreshnessNoSinkTSColumn(t *testing.T) {
	d := testDeps()
	d.Owners.Pipelines[0].SinkTSColumn = ""
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("warehouse_freshness")
	_, err := tt.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"table":"analytics.orders","ts_column":"email"}`))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v, want ErrBadInput", err)
	}
}
