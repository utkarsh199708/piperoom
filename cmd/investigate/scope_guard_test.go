package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/tools"
)

// TestEveryReadToolRefusesWithoutRoomScope is the registry-wide guard for
// finding F11: after the real wiring, every read tool except runbook_lookup
// (a static library with no customer data) refuses a context with no room
// pipeline and no opt-out, and reaches no client. A new read tool must be added
// here with a valid-looking input, or the test fails.
func TestEveryReadToolRefusesWithoutRoomScope(t *testing.T) {
	reg, _, f := wireFakes(t)
	inputs := map[string]string{
		"pg_replication_slots":     `{}`,
		"pg_streaming_liveness":    `{"pipeline_id":"orders"}`,
		"pg_recent_ddl":            `{"since":"2026-10-11T00:00:00Z"}`,
		"pg_long_transactions":     `{}`,
		"pg_row":                   `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"1"}`,
		"pg_publication_tables":    `{"pipeline_id":"orders"}`,
		"pg_connector_scope_gap":   `{"pipeline_id":"orders"}`,
		"pg_toast_risk":            `{"pipeline_id":"orders"}`,
		"source_settings":          `{}`,
		"source_table_replication": `{"pipeline_id":"orders"}`,
		"source_role_grants":       `{"pipeline_id":"orders"}`,
		"source_blocking_sessions": `{}`,
		"warehouse_freshness":      `{"table":"analytics.orders","ts_column":"updated_at"}`,
		"warehouse_row":            `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"1"}`,
		"warehouse_load_errors":    `{"table":"analytics.orders"}`,
		"reconcile":                `{"table":"public.orders","since":"2026-10-11T00:00:00Z","until":"2026-10-11T01:00:00Z"}`,
		"pipeline_map":             `{"pipeline_id":"orders"}`,
		"trace_record":             `{"pipeline_id":"orders","table":"public.orders","key":"1"}`,
		"connect_list_connectors":  `{}`,
		"connect_status":           `{"connector":"pg-orders-connector"}`,
		"connect_config":           `{"connector":"pg-orders-connector"}`,
		"schema_registry_subjects": `{}`,
		"schema_registry_versions": `{"subject":"pg.public.orders-value"}`,
		"schema_registry_diff":     `{"subject":"pg.public.orders-value","from_version":1}`,
		"kafka_topic_offsets":      `{"topic":"pg.public.orders"}`,
		"kafka_list_groups":        `{}`,
		"kafka_consumer_lag":       `{"group":"order-sync"}`,
		"kafka_find_by_key":        `{"topic":"pg.public.orders","key":"1","since":"2026-10-11T00:00:00Z"}`,
		"kafka_topic_config":       `{"pipeline_id":"orders"}`,
		"debezium_metrics":         `{"connector":"pg-orders-connector"}`,
		"dlq_peek":                 `{"connector":"pg-orders-sink"}`,
		"what_changed":             `{}`,

		// Q4b
		"connect_validate_config": `{"class":"io.debezium.connector.postgresql.PostgresConnector","config":{"topic.prefix":"pg"}}`,

		// X6b
		"debezium_snapshot_progress": `{"connector":"pg-orders-connector"}`,

		// Q3e
		"connect_worker_metrics": `{"connector":"pg-orders-connector"}`,
		"connect_worker_logs":    `{"connector":"pg-orders-connector"}`,

		// Q6b
		"net_probe": `{"target":"source"}`,
	}
	ran := 0
	for _, tl := range reg.All() {
		if tl.Kind() != tools.KindRead || tl.Name() == "runbook_lookup" {
			continue
		}
		in, ok := inputs[tl.Name()]
		if !ok {
			t.Errorf("read tool %s has no input in this guard test: add it, and make sure it calls ScopeFor", tl.Name())
			continue
		}
		ran++
		_, err := tl.Run(context.Background(), json.RawMessage(in))
		if !errors.Is(err, tools.ErrNoScope) {
			t.Errorf("%s with context.Background(): err = %v, want ErrNoScope", tl.Name(), err)
		}
	}
	if ran != len(inputs) {
		t.Errorf("ran %d tools, the table lists %d: a listed tool is not registered", ran, len(inputs))
	}
	if f.slotsCalls.Load() != 0 || f.statusCalls.Load() != 0 || f.findByKeyCalls.Load() != 0 {
		t.Errorf("a client was called by a refused tool: slots=%d status=%d find=%d", f.slotsCalls.Load(), f.statusCalls.Load(), f.findByKeyCalls.Load())
	}
}
