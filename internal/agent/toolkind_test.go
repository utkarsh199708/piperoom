package agent

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// kindTestTools are read tools of both kinds and of neither table entry,
// in registration order.
var kindTestTools = []string{
	"pg_replication_slots", "warehouse_freshness", "pipeline_map", "trace_record", "reconcile",
	"connect_status", "schema_registry_diff", "kafka_topic_offsets", "dlq_peek", "debezium_metrics",
	"tls_cert_expiry", "dag_runs", "task_log_tail", "dbt_run_results", "dbt_model_sql",
	"runbook_lookup", "similar_incidents", "what_changed", "brand_new_tool",
}

func kindTestRegistry(t *testing.T) (*tools.Registry, map[string]*recordingTool) {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	byName := map[string]*recordingTool{}
	for _, n := range kindTestTools {
		rt := &recordingTool{name: n, schema: testSchema(), result: tools.Result{Summary: "ok"}}
		byName[n] = rt
		if err := reg.Register(rt); err != nil {
			t.Fatal(err)
		}
	}
	return reg, byName
}

func names(reg *tools.Registry) []string {
	var out []string
	for _, t := range reg.All() {
		out = append(out, t.Name())
	}
	return out
}

var (
	cdcPrefixesBanned   = []string{"dag_", "dbt_", "task_"}
	batchPrefixesBanned = []string{"kafka_", "pg_", "connect_", "schema_registry_", "debezium_", "dlq_"}
)

func hasPrefixIn(name string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func TestToolsForKind(t *testing.T) {
	reg, _ := kindTestRegistry(t)
	wantCDC := []string{
		"pg_replication_slots", "warehouse_freshness", "pipeline_map", "trace_record", "reconcile",
		"connect_status", "schema_registry_diff", "kafka_topic_offsets", "dlq_peek", "debezium_metrics",
		"tls_cert_expiry", "runbook_lookup", "similar_incidents", "what_changed", "brand_new_tool",
	}
	wantBatch := []string{
		"warehouse_freshness", "pipeline_map", "dag_runs", "task_log_tail", "dbt_run_results", "dbt_model_sql",
		"runbook_lookup", "similar_incidents", "what_changed",
	}
	for _, tt := range []struct {
		kind string
		want []string
	}{
		{"", wantCDC}, // rooms before P4 and unknown pipelines read as cdc
		{events.PipelineKindCDC, wantCDC},
		{events.PipelineKindBatch, wantBatch},
	} {
		got, err := ToolsForKind(reg, tt.kind)
		if err != nil {
			t.Fatalf("kind %q: %v", tt.kind, err)
		}
		if !got.ReadOnly() {
			t.Fatalf("kind %q: the filtered registry is not read-only", tt.kind)
		}
		if g := names(got); !slices.Equal(g, tt.want) {
			t.Fatalf("kind %q:\n got %v\nwant %v", tt.kind, g, tt.want)
		}
		banned := cdcPrefixesBanned
		if tt.kind == events.PipelineKindBatch {
			banned = append(slices.Clone(batchPrefixesBanned), "trace_record", "reconcile", "tls_cert_expiry", "brand_new_tool")
		}
		for _, n := range names(got) {
			if hasPrefixIn(n, banned) || slices.Contains(banned, n) {
				t.Fatalf("kind %q offers %s", tt.kind, n)
			}
		}
	}
	// The source registry is left whole: one set serves rooms of both kinds.
	if g := names(reg); !slices.Equal(g, kindTestTools) {
		t.Fatalf("ToolsForKind changed its input: %v", g)
	}
}

func TestToolsForKindRefusesUnknownKind(t *testing.T) {
	reg, _ := kindTestRegistry(t)
	for _, k := range []string{"CDC", "Batch", "stream", " cdc"} {
		if _, err := ToolsForKind(reg, k); err == nil {
			t.Fatalf("kind %q accepted", k)
		}
	}
	if got, err := ToolsForKind(nil, ""); err != nil || len(got.All()) != 0 {
		t.Fatalf("nil registry: %v, %v", got, err)
	}
}

func TestToolPipelineKindsKnown(t *testing.T) {
	for _, tt := range []struct {
		name  string
		kinds []string
		known bool
	}{
		{"kafka_consumer_lag", []string{"cdc"}, true},
		{"pg_row", []string{"cdc"}, true},
		{"connect_config", []string{"cdc"}, true},
		{"trace_record", []string{"cdc"}, true},
		{"dag_schedule_gap", []string{"batch"}, true},
		{"task_instances", []string{"batch"}, true},
		{"dbt_model_lineage", []string{"batch"}, true},
		{"warehouse_row", []string{"cdc", "batch"}, true},
		{"what_changed", []string{"cdc", "batch"}, true},
		{"something_else", []string{"cdc"}, false},
	} {
		kinds, known := ToolPipelineKinds(tt.name)
		if !slices.Equal(kinds, tt.kinds) || known != tt.known {
			t.Errorf("%s: %v %v, want %v %v", tt.name, kinds, known, tt.kinds, tt.known)
		}
	}
}

// toolsCapture is a model that records the read tools of each request and
// calls one tool once, then ends the turn.
type toolsCapture struct {
	mu    sync.Mutex
	call  string
	tools [][]string
}

func (m *toolsCapture) Turn(_ context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ns []string
	for _, tp := range p.Tools {
		if tp.OfTool != nil {
			ns = append(ns, tp.OfTool.Name)
		}
	}
	m.tools = append(m.tools, ns)
	if len(m.tools) == 1 && m.call != "" {
		return anthropic.BetaMessage{
			StopReason: anthropic.BetaStopReasonToolUse,
			Content: []anthropic.BetaContentBlockUnion{{
				Type: "tool_use", ID: "tu_1", Name: m.call, Input: []byte(`{}`),
			}},
		}, nil
	}
	return anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: "done"}},
	}, nil
}

// The runner's tool list for a room is derived from Alert.PipelineKind, and a
// tool of the other kind cannot run even when the model names it.
func TestRunnerOffersOnlyTheRoomKindTools(t *testing.T) {
	for _, tt := range []struct {
		kind   string
		call   string // a tool of the other kind the model tries
		banned []string
	}{
		{events.PipelineKindBatch, "kafka_topic_offsets", batchPrefixesBanned},
		{"", "dag_runs", cdcPrefixesBanned},
		{events.PipelineKindCDC, "dbt_run_results", cdcPrefixesBanned},
	} {
		reg, byName := kindTestRegistry(t)
		m := &toolsCapture{call: tt.call}
		r, err := newRunnerWith(Alert{Title: "t", PipelineID: "p", PipelineKind: tt.kind}, Deps{
			Tools: reg, Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: &fakeSink{}, Logger: discardLogger(),
		}, Options{MaxTurns: 3}, m)
		if err != nil {
			t.Fatalf("kind %q: %v", tt.kind, err)
		}
		_ = r.run(context.Background())
		if len(m.tools) == 0 {
			t.Fatalf("kind %q: no model call", tt.kind)
		}
		first := m.tools[0]
		for _, n := range first {
			if hasPrefixIn(n, tt.banned) {
				t.Fatalf("kind %q: the request offers %s: %v", tt.kind, n, first)
			}
		}
		if !slices.Contains(first, "warehouse_freshness") || !slices.Contains(first, "runbook_lookup") {
			t.Fatalf("kind %q: shared tools missing: %v", tt.kind, first)
		}
		if got := byName[tt.call].inputsAsJSON(); len(got) != 0 {
			t.Fatalf("kind %q: %s ran %d times; a tool of the other kind must never run", tt.kind, tt.call, len(got))
		}
	}
}

func TestRunnerRefusesUnknownPipelineKind(t *testing.T) {
	reg, _ := kindTestRegistry(t)
	_, err := newRunnerWith(Alert{Title: "t", PipelineID: "p", PipelineKind: "stream"}, Deps{
		Tools: reg, Inbox: &fakeInbox{}, Sink: &fakeSink{}, Logger: discardLogger(),
	}, Options{}, &toolsCapture{})
	if err == nil || !strings.Contains(err.Error(), "PipelineKind") {
		t.Fatalf("err = %v", err)
	}
}

// The first look reads with the room kind's tools only.
func TestTriagePassOffersOnlyTheRoomKindTools(t *testing.T) {
	for _, kind := range []string{events.PipelineKindBatch, events.PipelineKindCDC} {
		m := &funcModel{fn: func(_ context.Context, i int, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
			return anthropic.BetaMessage{}, errors.New("stop after the pick request")
		}}
		var defs []*recordingTool
		for _, n := range kindTestTools {
			defs = append(defs, &recordingTool{name: n, schema: testSchema()})
		}
		f := newTriageFixture(t, m, defs...)
		f.run(t, Alert{Title: "t", PipelineID: "p", PipelineKind: kind})
		calls := m.calls()
		if len(calls) == 0 {
			t.Fatalf("kind %s: no pick request", kind)
		}
		banned := cdcPrefixesBanned
		if kind == events.PipelineKindBatch {
			banned = batchPrefixesBanned
		}
		for _, tp := range calls[0].Tools {
			if tp.OfTool != nil && hasPrefixIn(tp.OfTool.Name, banned) {
				t.Fatalf("kind %s: the pick request offers %s", kind, tp.OfTool.Name)
			}
		}
	}
}
