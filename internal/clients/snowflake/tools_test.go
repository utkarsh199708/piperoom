package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
)

// These tests drive the real dbtools warehouse tools against a Snowflake DB
// whose driver is a fake querier, to show the tools work through the
// clients.Warehouse interface with Snowflake's SQL and its upper-case folding.

const ownersYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders, dwh.analytics.orders_daily]
    sink_ts_column: updated_at
layers:
  warehouse: meera
`

func snowflakeTool(t *testing.T, fq *fakeQuerier, name string) tools.Tool {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(ownersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	var wh clients.Warehouse = &DB{q: fq}
	reg := tools.NewRegistry()
	if err := dbtools.Register(reg, dbtools.Deps{Wh: wh, Owners: o}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tt, ok := reg.Get(name)
	if !ok {
		t.Fatalf("tool %s not registered", name)
	}
	return tt
}

func TestWarehouseFreshnessToolOnSnowflake(t *testing.T) {
	// A TIMESTAMP_LTZ value in a non-UTC session zone, two minutes old.
	ts := time.Now().Add(-2 * time.Minute).In(time.FixedZone("PST", -8*3600))
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"MAX"}, data: [][]any{{&ts}}}}
	tt := snowflakeTool(t, fq, "warehouse_freshness")

	res, err := tt.Run(context.Background(), json.RawMessage(`{"table":"analytics.orders","ts_column":"updated_at"}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := `SELECT MAX("UPDATED_AT") FROM "ANALYTICS"."ORDERS"`; fq.lastQuery != want {
		t.Errorf("query = %q, want %q", fq.lastQuery, want)
	}
	data := res.Data.(map[string]any)
	newest, _ := data["newest"].(string)
	if !strings.HasSuffix(newest, "Z") {
		t.Errorf("newest = %q, want a UTC (Z) timestamp", newest)
	}
	if !strings.HasPrefix(res.Summary, "newest=") || !strings.Contains(res.Summary, "stale for") {
		t.Errorf("summary = %q", res.Summary)
	}
	// Two minutes old in any zone; a leaked zone offset would show as hours.
	if stale, _ := data["stale_for"].(string); stale != "2m" {
		t.Errorf("stale_for = %q, want 2m", stale)
	}
}

func TestWarehouseFreshnessToolThreePartTable(t *testing.T) {
	ts := time.Now().Add(-time.Hour)
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"MAX"}, data: [][]any{{&ts}}}}
	tt := snowflakeTool(t, fq, "warehouse_freshness")

	if _, err := tt.Run(context.Background(), json.RawMessage(`{"table":"dwh.analytics.orders_daily","ts_column":"updated_at"}`)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := `SELECT MAX("UPDATED_AT") FROM "DWH"."ANALYTICS"."ORDERS_DAILY"`; fq.lastQuery != want {
		t.Errorf("query = %q, want %q", fq.lastQuery, want)
	}
}

func TestWarehouseFreshnessToolEmptyAndMissing(t *testing.T) {
	var nilTS *time.Time
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"MAX"}, data: [][]any{{nilTS}}}}
	res, err := snowflakeTool(t, fq, "warehouse_freshness").Run(context.Background(), json.RawMessage(`{"table":"analytics.orders","ts_column":"updated_at"}`))
	if err != nil || res.Summary != "no rows (empty table)" {
		t.Fatalf("empty table: summary=%q err=%v", res.Summary, err)
	}

	fq = &fakeQuerier{err: &gosnowflake.SnowflakeError{Number: errObjectDoesNotExist, SQLState: "42S02"}}
	_, err = snowflakeTool(t, fq, "warehouse_freshness").Run(context.Background(), json.RawMessage(`{"table":"analytics.gone","ts_column":"updated_at"}`))
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing table: err = %v, want wrapping clients.ErrNotFound", err)
	}
}

func TestWarehouseToolsRefuseInjectionBeforeDriver(t *testing.T) {
	fq := &fakeQuerier{}
	tt := snowflakeTool(t, fq, "warehouse_freshness")
	for _, in := range []string{
		`{"table":"analytics.orders; DROP TABLE x","ts_column":"updated_at"}`,
		`{"table":"analytics.orders","ts_column":"updated_at) FROM t --"}`,
		`{"table":"a.b.c.d","ts_column":"updated_at"}`,
		`{"table":"analytics.\"orders\"","ts_column":"updated_at"}`,
	} {
		if _, err := tt.Run(context.Background(), json.RawMessage(in)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("input %s: err = %v, want ErrBadInput", in, err)
		}
	}
	if fq.lastQuery != "" {
		t.Fatalf("a query reached the driver: %q", fq.lastQuery)
	}
}

func TestWarehouseRowToolOnSnowflake(t *testing.T) {
	// Snowflake returns upper-case column names; the tool must still find the
	// timestamp columns and keep everything else out of Data.
	fq := &fakeQuerier{rows: &fakeRows{
		cols: []string{"ID", "AMOUNT", "UPDATED_AT"},
		data: [][]any{{"7", "19.99", time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}},
	}}
	tt := snowflakeTool(t, fq, "warehouse_row")
	res, err := tt.Run(context.Background(), json.RawMessage(`{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := `SELECT * FROM "ANALYTICS"."ORDERS" WHERE "ID" = ? LIMIT 1`; fq.lastQuery != want {
		t.Errorf("query = %q, want %q", fq.lastQuery, want)
	}
	if len(fq.lastArgs) != 1 || fq.lastArgs[0] != "7" {
		t.Errorf("args = %v, want the key bound as the only argument", fq.lastArgs)
	}
	data := res.Data.(map[string]any)
	if _, ok := data["updated_at"]; !ok {
		t.Errorf("data = %v, want updated_at kept", data)
	}
	if _, ok := data["amount"]; ok {
		t.Errorf("data = %v, want non-timestamp columns dropped", data)
	}
}
