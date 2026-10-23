package evals

// W4k (KAN-495): the reads run 39994729845 made against six scenarios that no
// recording covered now resolve, the dead-slot and slot-inactive recordings are
// what the real tools return for the state they describe, and
// consumer-rebalance-storm's budget is not 16.

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/logtools"
)

// w4kMiss is one tool call a scenario had no recording for.
type w4kMiss struct{ tool, input string }

// w4kMisses are the distinct misses of each scenario in run 39994729845, from
// reviews/evals-2026-10-23/misses.json (W4-0, PR #943): 75 misses in all, 24,
// 7, 7, 9, 15 and 13 per scenario, a call counted once per run it was made in.
// The clickhouse-readonly-profile and clickhouse-sink-table-dropped rows are
// that baseline's; the W4a re-run (40028984141) had 5 misses in each, and its
// transcripts are not in this table.
var w4kMisses = map[string][]w4kMiss{
	"clickhouse-kafka-engine-stalled": {
		{"pg_recent_ddl", `{"since":"2024-01-01T00:00:00Z"}`},
		{"reconcile", `{"since":"2026-10-20T07:45:00Z","table":"public.orders"}`},
		{"similar_incidents", `{"query":"analytics.orders stale; ClickHouse Kafka engine consumer reported an exception; order-sync lag 0"}`},
		{"schema_registry_versions", `{"subject":"pg.public.orders-value"}`},
		{"schema_registry_subjects", `{"prefix":"pg.public.orders"}`},
		{"kafka_throughput", `{"window":"1h"}`},
		{"connect_config", `{"connector":"pg-orders-connector"}`},
		{"pg_recent_ddl", `{"since":"2026-10-20T06:00:00Z","table":"public.orders"}`},
		{"kafka_dup_keys", `{"max_scan":20000,"since":"2026-10-20T08:00:00Z","topic":"pg.public.orders"}`},
		{"pg_recent_ddl", `{"since":"2025-01-01T00:00:00Z","table":"public.orders"}`},
		{"similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders stale, ClickHouse Kafka engine consumer reported an exception, lag 0"}`},
		{"schema_registry_subjects", `{"include_deleted":true,"prefix":"pg.public.orders"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":100,"since":"2026-10-20T07:50:00Z"}`},
		{"similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders stale, ClickHouse Kafka engine consumer reported an exception, LOAD_FAILED, order-sync lag 0"}`},
		{"pg_recent_ddl", `{"since":"2026-10-20T07:00:00Z"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","since":"2026-10-20T07:45:00Z"}`},
	},
	"dead-slot-wal-no-context": {
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":50,"since":"4h"}`},
	},
	"slot-inactive-wal-growth": {
		{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		{"source_blocking_sessions", `{}`},
	},
	"consumer-rebalance-storm": {
		{"kafka_throughput", `{"window":"1h"}`},
		{"source_blocking_sessions", `{"older_than_minutes":5}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders","window_seconds":8}`},
		{"kafka_throughput", `{"window":"30m"}`},
	},
	"clickhouse-readonly-profile": {
		{"pg_recent_ddl", `{"since":"2024-01-01T00:00:00Z","table":"public.orders"}`},
		{"pg_replication_slots", `{}`},
		{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		{"similar_incidents", `{"query":"analytics.orders unreadable clickhouse readonly=1 Cannot execute query in readonly mode code 164"}`},
		{"net_probe", `{"target":"warehouse"}`},
		{"similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders unreadable clickhouse readonly=1 Cannot execute query in readonly mode code 164"}`},
		{"warehouse_row", `{"key":"1","pipeline_id":"orders","pk_column":"id","table":"analytics.orders"}`},
		{"similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders unreadable clickhouse readonly=1 code 164 Cannot execute query in readonly mode"}`},
	},
	"clickhouse-sink-table-dropped": {
		{"pg_recent_ddl", `{"since":"2024-01-01T00:00:00Z","table":"public.orders"}`},
		{"similar_incidents", `{"query":"analytics.orders stale 50m"}`},
		{"pg_replication_slots", `{}`},
		{"net_probe", `{"target":"warehouse"}`},
		{"warehouse_row", `{"key":"1","pipeline_id":"orders","pk_column":"id","table":"analytics.orders"}`},
		{"pg_recent_ddl", `{"since":"2025-01-01T00:00:00Z","table":"public.orders"}`},
		{"similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders stale; ClickHouse INSERT exception code 60 table not found; order-sync lag 0"}`},
		{"similar_incidents", `{"pipeline_id":"orders","query":"analytics.orders stale; ClickHouse INSERT exception code 60 unknown table; warehouse table not found; consumer lag 0"}`},
	},
}

// TestW4kMissesResolve: every call W4-0 lists as a miss is answered by a
// recording now, and a read off the standard path still is a miss.
func TestW4kMissesResolve(t *testing.T) {
	for name, misses := range w4kMisses {
		t.Run(name, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + name + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			rp, err := newScenarioReplayer(sc)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range misses {
				rec, ok := rp.lookup(m.tool, json.RawMessage(m.input))
				if !ok {
					t.Errorf("%s %s: no recording covers it", m.tool, m.input)
					continue
				}
				t.Logf("%s %s -> recording with input %v", m.tool, m.input, rec.Input)
			}
			// Off the standard path: a target the scenario does not probe, a
			// connector it does not have, a window nobody recorded, a tool it
			// never records.
			for _, m := range []w4kMiss{
				{"net_probe", `{"target":"kafka"}`},
				{"connect_source_offsets", `{"connector":"other-connector"}`},
				{"kafka_throughput", `{"window":"2h"}`},
				{"kafka_acls", `{}`},
			} {
				if _, ok := rp.lookup(m.tool, json.RawMessage(m.input)); ok {
					t.Errorf("%s %s is answered; a read off the standard path must stay a miss", m.tool, m.input)
				}
			}
		})
	}
}

// TestW4kMissCounts keeps the table above to the 75 misses W4-0 counted.
func TestW4kMissCounts(t *testing.T) {
	want := map[string]int{
		"clickhouse-kafka-engine-stalled": 16, "dead-slot-wal-no-context": 3, "slot-inactive-wal-growth": 3,
		"consumer-rebalance-storm": 4, "clickhouse-readonly-profile": 8, "clickhouse-sink-table-dropped": 8,
	}
	if len(w4kMisses) != len(want) {
		t.Fatalf("%d scenarios, want %d", len(w4kMisses), len(want))
	}
	for name, n := range want {
		if got := len(w4kMisses[name]); got != n {
			t.Errorf("%s: %d distinct misses, want %d", name, got, n)
		}
	}
}

// w4kRegistry is the real tools the recorded stacks run on.
func w4kRegistry(t *testing.T, w *w4kWorld) *tools.Registry {
	t.Helper()
	o := w4kOwners()
	calls := 0
	conn, src := w4kConnect{w: w}, w4kSource{w: w, calls: &calls}
	reg := tools.NewRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: conn, Owners: o}); err != nil {
		t.Fatal(err)
	}
	if err := dbtools.Register(reg, dbtools.Deps{Src: src, Connect: conn, Owners: o}); err != nil {
		t.Fatal(err)
	}
	if err := logtools.Register(reg, logtools.Deps{Owners: o}); err != nil {
		t.Fatal(err)
	}
	return reg
}

// w4kCheck runs the real tool and compares its summary and data with the
// scenario's recording for the same call in phase.
func w4kCheck(t *testing.T, sc Scenario, reg *tools.Registry, phase, tool, call string) {
	t.Helper()
	rp, err := newScenarioReplayer(sc)
	if err != nil {
		t.Fatal(err)
	}
	rp.phase = phase
	rec, ok := rp.lookup(tool, json.RawMessage(call))
	if !ok {
		t.Fatalf("%s %s: no recording", tool, call)
	}
	tl, ok := reg.Get(tool)
	if !ok {
		t.Fatalf("%s is not registered", tool)
	}
	res, err := tl.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(call))
	if err != nil {
		t.Fatalf("%s %s: %v", tool, call, err)
	}
	ob := rec.obs[0]
	if ob.err != "" {
		t.Fatalf("%s %s: the recording is an error: %s", tool, call, ob.err)
	}
	if res.Summary != ob.summary {
		t.Errorf("%s %s: summary differs from the recording\nreal tool: %q\nrecording: %q", tool, call, res.Summary, ob.summary)
	}
	if len(ob.data) == 0 {
		return // connect_worker_logs: its Data echoes the call's own since and until
	}
	got, want := w4kJSON(t, res.Data), w4kJSON(t, json.RawMessage(ob.data))
	// elapsed_ms is the wall time of the two slot reads and the wait between
	// them: the window plus a few ms, never the same twice.
	if g, ok := got["elapsed_ms"].(float64); ok {
		if g < 5000 || g > 8000 {
			t.Errorf("%s %s: elapsed_ms = %v, want the 5 s window plus a few ms", tool, call, g)
		}
		delete(got, "elapsed_ms")
		delete(want, "elapsed_ms")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s %s: data differs from the recording\nreal tool: %v\nrecording: %v", tool, call, got, want)
	}
}

// w4kJSON round-trips v through encoding/json, so a Go value and a YAML value
// compare the way the room log sees them. A list is wrapped in an object.
func w4kJSON(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if m, ok := out.(map[string]any); ok {
		return m
	}
	return map[string]any{"value": out}
}

// TestW4kRecordingsAreRealToolOutput: the connect_source_offsets,
// pg_streaming_liveness, source_blocking_sessions and connect_worker_logs
// recordings of dead-slot-wal-no-context and slot-inactive-wal-growth equal
// what the production code returns over the stack they describe. Each liveness
// check waits its real 5 s window, so the subtests run in parallel.
func TestW4kRecordingsAreRealToolOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("pg_streaming_liveness waits its 5 s window")
	}
	cases := []struct {
		scenario, phase string
		world           func() *w4kWorld
		calls           [][2]string
	}{
		{"dead-slot-wal-no-context", "", w4kDeadSlot, [][2]string{
			{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
			{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
			{"connect_worker_logs", `{"connector":"pg-orders-connector","level":"INFO","limit":50,"since":"4h"}`},
		}},
		{"slot-inactive-wal-growth", "broken", w4kSlotInactive, [][2]string{
			{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
			{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
			{"source_blocking_sessions", `{}`},
		}},
		{"slot-inactive-wal-growth", "recovered", w4kSlotRecovered, [][2]string{
			{"connect_source_offsets", `{"connector":"pg-orders-connector"}`},
			{"pg_streaming_liveness", `{"pipeline_id":"orders"}`},
		}},
	}
	for _, c := range cases {
		t.Run(c.scenario+"/"+c.phase, func(t *testing.T) {
			t.Parallel()
			sc, err := LoadScenario("scenarios/" + c.scenario + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			reg := w4kRegistry(t, c.world())
			for _, call := range c.calls {
				w4kCheck(t, sc, reg, c.phase, call[0], call[1])
			}
		})
	}
}

// TestW4kOffsetsHoldTheSlotPosition: the stored offset is at the slot's
// confirmed_flush_lsn (after the second read in the recovered state, where the
// slot moved), so the connector still holds its position: the check the prompt
// asks for before a resume.
func TestW4kOffsetsHoldTheSlotPosition(t *testing.T) {
	for name, w := range map[string]*w4kWorld{"dead-slot": w4kDeadSlot(), "slot-inactive": w4kSlotInactive(), "recovered": w4kSlotRecovered()} {
		slot := w.slotReads[len(w.slotReads)-1][0]
		got, err := w.offsets[0].Offset["lsn"].(json.Number).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if want := w4kLSNPos(slot.ConfirmedFlushLSN); uint64(got) != want {
			t.Errorf("%s: stored lsn %d, slot confirmed_flush_lsn %s = %d", name, got, slot.ConfirmedFlushLSN, want)
		}
	}
}

// TestW4kRebalanceStormBudget: the budget is unchanged (14) or, only after a
// live run with misses 0 passes at 15 calls, 15. It is never 16, which would be
// a failing run plus margin.
func TestW4kRebalanceStormBudget(t *testing.T) {
	sc, err := LoadScenario("scenarios/consumer-rebalance-storm.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := sc.Expect.MaxToolCalls; got != 14 && got != 15 {
		t.Errorf("max_tool_calls = %d, want 14 (unchanged) or 15, never 16", got)
	}
	b, err := os.ReadFile("scenarios/consumer-rebalance-storm.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"W4k (KAN-495)", "3 cycles", "Never 16"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the budget derivation comment lacks %q", want)
		}
	}
}
