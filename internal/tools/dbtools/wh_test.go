package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools"
)

func TestValidWarehouseTable(t *testing.T) {
	cases := []struct {
		kind, name string
		want       bool
	}{
		{"postgres", "analytics.orders", true},
		{"", "analytics.orders", true},
		{"postgres", "dwh.analytics.orders", false}, // 3 parts are Snowflake only
		{"", "dwh.analytics.orders", false},
		{"postgres", "analytics.order$1", false},
		{"snowflake", "analytics.orders", true},
		{"snowflake", "dwh.analytics.orders", true},
		{"Snowflake", "dwh.analytics.orders", true},
		{"snowflake", "analytics.order$1", true},
		{"snowflake", "orders", false},
		{"snowflake", "a.b.c.d", false},
		{"snowflake", "analytics.orders;", false},
		{"snowflake", `analytics."orders"`, false},
		{"snowflake", "a..b", false},
		{"snowflake", "$a.b", false},
		{"snowflake", "", false},
		// Databricks and BigQuery address a table by catalog/project + schema
		// + table, so a three-part name is valid on them too (KAN-460).
		{"databricks", "analytics.orders", true},
		{"databricks", "main.analytics.orders", true},
		{"databricks", "main.analytics.orders.extra", false},
		{"bigquery", "analytics.orders", true},
		{"bigquery", "my-project.analytics.orders", true},
		{"bigquery", "a..b", false},
		// The two kinds that keep two parts.
		{"clickhouse", "analytics.orders", true},
		{"clickhouse", "dwh.analytics.orders", false},
		{"postgres", "dwh.analytics.orders", false},
	}
	for _, tc := range cases {
		d := Deps{WarehouseKind: tc.kind}
		if got := d.validWarehouseTable(tc.name); got != tc.want {
			t.Errorf("kind %q validWarehouseTable(%q) = %v, want %v", tc.kind, tc.name, got, tc.want)
		}
	}
}

func TestValidWarehouseColumn(t *testing.T) {
	if (Deps{}).validWarehouseColumn("loaded$at") {
		t.Error("postgres must reject '$' in a column")
	}
	if !(Deps{WarehouseKind: "snowflake"}).validWarehouseColumn("loaded$at") {
		t.Error("snowflake must accept '$' in a column")
	}
	if (Deps{WarehouseKind: "snowflake"}).validWarehouseColumn("x; drop") {
		t.Error("snowflake must reject injection")
	}
}

// A three-part table on the Postgres warehouse is bad input for the agent, not
// a tool failure.
func TestWarehouseToolsPostgresThreePartIsBadInput(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	for name, in := range map[string]string{
		"warehouse_freshness": `{"table":"dwh.analytics.orders","ts_column":"updated_at"}`,
		"warehouse_row":       `{"pipeline_id":"orders","table":"dwh.analytics.orders","pk_column":"id","key":"7"}`,
	} {
		tt, _ := reg.Get(name)
		if _, err := tt.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(in)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s: err = %v, want ErrBadInput", name, err)
		}
	}
}

// A newest value ahead of the clock must not read as fresh (QA sweep H5):
// within a minute it is skew and reads as 0s; beyond that the tool says the
// value is in the future and gives no staleness.
func TestWarehouseFreshnessFutureTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ahead      time.Duration
		wantFuture bool
	}{
		{"skew", 30 * time.Second, false},
		{"future", 3 * time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testDeps()
			d.Wh.(*fakeWarehouse).fresh = time.Now().Add(tc.ahead)
			reg := tools.NewRegistry()
			if err := Register(reg, d); err != nil {
				t.Fatal(err)
			}
			tt, _ := reg.Get("warehouse_freshness")
			r, err := tt.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"table":"analytics.orders","ts_column":"updated_at"}`))
			if err != nil {
				t.Fatal(err)
			}
			data := r.Data.(map[string]any)
			_, hasStale := data["stale_for"]
			if tc.wantFuture {
				if !strings.Contains(r.Summary, "IN THE FUTURE") || hasStale || data["in_future"] != true {
					t.Fatalf("summary %q data %v: want the future warning and no stale_for", r.Summary, data)
				}
				return
			}
			if !strings.Contains(r.Summary, "stale for 0s") || data["stale_for"] != "0s" {
				t.Fatalf("summary %q data %v: want stale for 0s", r.Summary, data)
			}
		})
	}
}

// A Postgres 'infinity' max arrives as a year-9999 stand-in (#422). The tool
// must not show that made-up time or an age computed from it.
func TestWarehouseFreshnessInfinity(t *testing.T) {
	d := testDeps()
	d.Wh.(*fakeWarehouse).fresh = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("warehouse_freshness")
	r, err := tt.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"table":"analytics.orders","ts_column":"updated_at"}`))
	if err != nil {
		t.Fatal(err)
	}
	const want = "newest=infinity (a sentinel or bad value in updated_at): staleness cannot be judged"
	data := r.Data.(map[string]any)
	if r.Summary != want || data["newest"] != "infinity" || data["in_future"] != true {
		t.Fatalf("summary %q data %v", r.Summary, data)
	}
	for _, k := range []string{"ahead_by", "stale_for"} {
		if _, ok := data[k]; ok {
			t.Errorf("data has %s: %v", k, data)
		}
	}
}
