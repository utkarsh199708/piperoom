package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// funcModel answers each request with fn and records the requests.
type funcModel struct {
	mu   sync.Mutex
	reqs []anthropic.BetaMessageNewParams
	fn   func(ctx context.Context, i int, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error)
}

func (m *funcModel) Turn(ctx context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.mu.Lock()
	i := len(m.reqs)
	m.reqs = append(m.reqs, p)
	m.mu.Unlock()
	return m.fn(ctx, i, p)
}

func (m *funcModel) calls() []anthropic.BetaMessageNewParams {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.reqs)
}

// verdictMessage is a structured verdict answer with usage.
func verdictMessage(t *testing.T, v map[string]any) anthropic.BetaMessage {
	t.Helper()
	b, _ := json.Marshal(v)
	return mustMessage(t, fmt.Sprintf(`{"stop_reason":"end_turn","content":[{"type":"text","text":%q}],"usage":{"input_tokens":50,"output_tokens":20}}`, b))
}

// propSchema is a strict object schema with the given string properties, the
// first required ones listed in required.
func propSchema(props []string, required ...string) map[string]any {
	p := map[string]any{}
	for _, n := range props {
		p[n] = map[string]any{"type": "string"}
	}
	return map[string]any{"type": "object", "properties": p, "additionalProperties": false, "required": required}
}

// triageFixture is a registry of recording tools plus an endpoint on m.
type triageFixture struct {
	ep    *Endpoint
	sink  *fakeSink
	reg   *tools.Registry
	tools map[string]*recordingTool
}

func newTriageFixture(t *testing.T, m model, toolDefs ...*recordingTool) *triageFixture {
	t.Helper()
	reg := tools.NewRegistry()
	f := &triageFixture{ep: testEndpoint(m, 0), sink: &fakeSink{}, reg: reg, tools: map[string]*recordingTool{}}
	f.ep.triagePass = triagePassConfig{on: true, model: "claude-haiku-5-5", timeout: 5 * time.Second}
	for _, td := range toolDefs {
		if err := reg.Register(td); err != nil {
			t.Fatalf("register %s: %v", td.name, err)
		}
		f.tools[td.name] = td
	}
	return f
}

func (f *triageFixture) run(t *testing.T, alert Alert) TriageResult {
	t.Helper()
	if alert.RoomID == "" {
		alert.RoomID = "r_1"
	}
	if alert.PipelineID == "" {
		alert.PipelineID = "orders"
	}
	res, err := f.ep.Triage(context.Background(), alert, Deps{Tools: f.reg, Sink: f.sink, Logger: discardLogger()}, Options{})
	if err != nil {
		t.Fatalf("Triage: %v", err)
	}
	return res
}

// triagePayload returns the one agent.triage the sink holds.
func (f *triageFixture) triagePayload(t *testing.T) events.AgentTriagePayload {
	t.Helper()
	evs := f.sink.ofType(events.AgentTriage)
	if len(evs) != 1 {
		t.Fatalf("agent.triage events = %d, want exactly 1", len(evs))
	}
	var p events.AgentTriagePayload
	if err := json.Unmarshal(evs[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.EvidenceRefs == nil {
		t.Fatal("evidence_refs is null; the payload says never null")
	}
	return p
}

func (f *triageFixture) usageRoles(t *testing.T) []string {
	t.Helper()
	var roles []string
	for _, e := range f.sink.ofType(events.AgentUsage) {
		var u events.AgentUsagePayload
		if err := json.Unmarshal(e.payload, &u); err != nil {
			t.Fatal(err)
		}
		roles = append(roles, u.Role)
	}
	return roles
}

func watcherRaw(t *testing.T, sig map[string]any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"source": "watcher", "pipeline": "orders", "severity": "sev2",
		"at": "2026-10-14T03:12:00Z", "reason": "opened", "signals": []any{sig},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func connectorDownSignal() map[string]any {
	return map[string]any{
		"signal": "connector_down", "severity": "sev2", "since": "2026-10-14T03:06:00Z", "firing_for_seconds": 360,
		"connectors": []any{map[string]any{"name": "orders-source", "state": "RUNNING", "tasks": []any{
			map[string]any{"id": 0, "state": "FAILED", "trace": "org.apache.kafka.connect.errors.DataException: MISSING_UNION_BRANCH password=hunter2secret\n\tat x.y.Z"},
		}}},
	}
}

func TestTriagePassUsesSignalTable(t *testing.T) {
	m := &funcModel{fn: func(_ context.Context, i int, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		return verdictMessage(t, map[string]any{
			"headline":        "orders: task 0 FAILED on MISSING_UNION_BRANCH after DDL on public.orders",
			"suspected_layer": "kafka-connect", "confidence": "medium",
			"next_check":     "schema_registry_diff for orders-value",
			"based_on_steps": []string{"s_forged", stepOf(t, p, 0), stepOf(t, p, 0)},
		}), nil
	}}
	f := newTriageFixture(t, m,
		&recordingTool{name: "connect_status", schema: propSchema([]string{"connector"}, "connector"), result: tools.Result{Summary: "orders-source RUNNING; task 0 FAILED: MISSING_UNION_BRANCH"}},
		&recordingTool{name: "pg_recent_ddl", schema: propSchema([]string{"since", "table"}, "since"), result: tools.Result{Summary: "ALTER TABLE public.orders ADD COLUMN note text at 03:01"}},
		&recordingTool{name: "kafka_consumer_lag", schema: propSchema([]string{"group"}, "group")},
	)
	fixed := time.Date(2026, 10, 14, 3, 12, 0, 0, time.UTC)
	p := &triagePass{ep: f.ep, cfg: f.ep.triagePass, alert: Alert{RoomID: "r_1", PipelineID: "orders", Title: "orders: connector down", Raw: watcherRaw(t, connectorDownSignal())},
		deps: Deps{Tools: f.reg, Sink: f.sink, Logger: discardLogger()}, opts: Options{}.withDefaults(), now: func() time.Time { return fixed }}
	res := p.run(context.Background())

	calls := m.calls()
	if len(calls) != 1 {
		t.Fatalf("model calls = %d, want 1 (the verdict; the table picks the reads)", len(calls))
	}
	if calls[0].Model != "claude-haiku-5-5" || calls[0].MaxTokens != triageVerdictMaxTokens || len(calls[0].Tools) != 0 {
		t.Fatalf("verdict request: model %q max_tokens %d tools %d", calls[0].Model, calls[0].MaxTokens, len(calls[0].Tools))
	}
	if got := f.tools["connect_status"].inputsAsJSON(); len(got) != 1 || got[0] != `{"connector":"orders-source"}` {
		t.Fatalf("connect_status inputs = %v", got)
	}
	if got := f.tools["pg_recent_ddl"].inputsAsJSON(); len(got) != 1 || got[0] != `{"since":"2026-10-13T21:12:00Z"}` {
		t.Fatalf("pg_recent_ddl inputs = %v", got)
	}
	if n := len(f.tools["kafka_consumer_lag"].inputsAsJSON()); n != 0 {
		t.Fatalf("kafka_consumer_lag ran %d times; not in the connector_down row", n)
	}
	pl := f.triagePayload(t)
	if pl.Status != "ok" || pl.SuspectedLayer != "kafka-connect" || pl.Confidence != "medium" || pl.Model != "claude-haiku-5-5" {
		t.Fatalf("payload = %+v", pl)
	}
	if len(res.Steps) != 2 || res.Steps[0].Tool != "connect_status" || res.Steps[1].Tool != "pg_recent_ddl" {
		t.Fatalf("steps = %+v", res.Steps)
	}
	if want := []string{res.Steps[0].StepID}; !slices.Equal(pl.EvidenceRefs, want) {
		t.Fatalf("evidence_refs = %v, want %v (own steps only, no duplicates)", pl.EvidenceRefs, want)
	}
	if res.Payload.Headline != pl.Headline {
		t.Fatal("the returned payload differs from the emitted one")
	}
	if got := len(f.sink.ofType(events.AgentToolResult)); got != 2 {
		t.Fatalf("tool_result events = %d, want 2", got)
	}
	if roles := f.usageRoles(t); !slices.Equal(roles, []string{events.UsageRoleFirstLook}) {
		t.Fatalf("usage roles = %v", roles)
	}
	// The verdict request reads the wrapped reads, with step ids.
	body := messagesJSON(calls[0].Messages)
	for _, want := range []string{`tool_output tool=\"connect_status\" step=\"` + res.Steps[0].StepID, "MISSING_UNION_BRANCH"} {
		if !strings.Contains(body, want) {
			t.Fatalf("verdict request misses %q: %s", want, body)
		}
	}
}

// stepOf finds the n-th step id in a verdict request.
func stepOf(t *testing.T, p anthropic.BetaMessageNewParams, n int) string {
	t.Helper()
	body := messagesJSON(p.Messages)
	parts := strings.Split(body, `step=\"`)
	if len(parts) <= n+1 {
		return "s_none"
	}
	return parts[n+1][:strings.Index(parts[n+1], `\"`)]
}

func TestTriagePassTableForEverySignal(t *testing.T) {
	all := []string{"connect_status", "connect_list_connectors", "pg_recent_ddl", "pg_replication_slots", "pg_long_transactions",
		"dlq_peek", "kafka_consumer_lag", "kafka_topic_offsets", "kafka_topic_config", "connect_config"}
	tests := []struct {
		sig  map[string]any
		want []string
	}{
		{map[string]any{"signal": "slot_inactive", "slot": "debezium_orders"}, []string{"connect_list_connectors", "pg_replication_slots"}},
		{map[string]any{"signal": "slot_wal", "slot": "debezium_orders"}, []string{"pg_long_transactions", "pg_replication_slots"}},
		{map[string]any{"signal": "dlq_growth", "topic": "dlq.orders", "writers": []any{"orders-sink"}}, []string{"dlq_peek", "connect_status"}},
		{map[string]any{"signal": "dlq_growth", "topic": "dlq.orders", "writers": []any{}}, []string{"dlq_peek", "connect_list_connectors"}},
		{map[string]any{"signal": "consumer_lag", "group": "order-sync"}, []string{"kafka_consumer_lag", "connect_list_connectors"}},
		{map[string]any{"signal": "group_unstable", "group": "order-sync"}, []string{"kafka_consumer_lag", "connect_list_connectors"}},
		{map[string]any{"signal": "offset_below_log_start", "topic": "pg.public.orders"}, []string{"kafka_topic_offsets", "kafka_topic_config"}},
		{map[string]any{"signal": "config_risk", "risks": []any{map[string]any{"connector": "orders-sink", "code": "tolerance_all_no_dlq"}}}, []string{"connect_config", "connect_status"}},
	}
	for _, tt := range tests {
		t.Run(tt.sig["signal"].(string), func(t *testing.T) {
			var defs []*recordingTool
			for _, n := range all {
				defs = append(defs, &recordingTool{name: n, schema: map[string]any{"type": "object"}, result: tools.Result{Summary: n + " ok"}})
			}
			f := newTriageFixture(t, &funcModel{fn: func(context.Context, int, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
				return verdictMessage(t, map[string]any{"headline": "h", "suspected_layer": "", "confidence": "low", "next_check": "", "based_on_steps": []string{}}), nil
			}}, defs...)
			res := f.run(t, Alert{Title: "t", Raw: watcherRaw(t, tt.sig)})
			var got []string
			for _, s := range res.Steps {
				got = append(got, s.Tool)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("reads = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTriagePassModelPicksAtMostTwoTools(t *testing.T) {
	pick := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"tool_use","id":"tu_1","name":"connect_status","input":{"connector":"a","why":"look at a"}},
		{"type":"tool_use","id":"tu_2","name":"connect_status","input":{"connector":"b"}},
		{"type":"tool_use","id":"tu_3","name":"connect_status","input":{"connector":"c"}}],
		"usage":{"input_tokens":100,"output_tokens":30}}`)
	m := &funcModel{fn: func(_ context.Context, i int, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		if i == 0 {
			return pick, nil
		}
		return verdictMessage(t, map[string]any{"headline": "connector a FAILED", "suspected_layer": "kafka-connect", "confidence": "high", "next_check": "connect_config a", "based_on_steps": []string{}}), nil
	}}
	f := newTriageFixture(t, m, &recordingTool{name: "connect_status", schema: propSchema([]string{"connector"}, "connector"), result: tools.Result{Summary: "FAILED"}})
	res := f.run(t, Alert{Title: "orders stale", Raw: json.RawMessage(`{"description":"from grafana"}`)})

	calls := m.calls()
	if len(calls) != 2 {
		t.Fatalf("model calls = %d, want 2 (pick + verdict)", len(calls))
	}
	pickReq := calls[0]
	if pickReq.MaxTokens != triagePickMaxTokens || pickReq.System[0].Text != triagePrompt() || len(pickReq.Tools) != 1 {
		t.Fatalf("pick request: max_tokens %d tools %d", pickReq.MaxTokens, len(pickReq.Tools))
	}
	if pickReq.OutputConfig.Effort != "low" {
		t.Fatalf("pick effort = %q", pickReq.OutputConfig.Effort)
	}
	if got := f.tools["connect_status"].inputsAsJSON(); len(got) != 2 {
		t.Fatalf("reads run = %v, want the first 2", got)
	}
	if len(res.Steps) != 2 || res.Payload.Status != "ok" {
		t.Fatalf("result = %+v", res)
	}
	if roles := f.usageRoles(t); !slices.Equal(roles, []string{events.UsageRoleFirstLook, events.UsageRoleFirstLook}) {
		t.Fatalf("usage roles = %v", roles)
	}
	if n := len(f.sink.ofType(events.AgentHypothesis)) + len(f.sink.ofType(events.ProposalCreated)); n != 0 {
		t.Fatalf("the pass wrote %d hypothesis/proposal events; it never may", n)
	}
	// The pick turn never offers room tools.
	for _, tp := range pickReq.Tools {
		if tp.OfTool != nil && slices.Contains(roomToolNames(), tp.OfTool.Name) {
			t.Fatalf("pick turn offers room tool %s", tp.OfTool.Name)
		}
	}
}

func TestTriagePassTimeoutEmitsFailed(t *testing.T) {
	m := &funcModel{fn: func(ctx context.Context, _ int, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		<-ctx.Done()
		return anthropic.BetaMessage{}, ctx.Err()
	}}
	f := newTriageFixture(t, m, &recordingTool{name: "connect_status", schema: propSchema([]string{"connector"}, "connector")})
	f.ep.triagePass.timeout = 50 * time.Millisecond
	start := time.Now()
	res := f.run(t, Alert{Title: "orders stale"})
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("pass took %s; the deadline is 50ms", took)
	}
	pl := f.triagePayload(t)
	if pl.Status != "failed" || pl.Reason != triageReasonTimeout || pl.Headline != "" {
		t.Fatalf("payload = %+v", pl)
	}
	if res.Payload.Reason != triageReasonTimeout {
		t.Fatalf("returned = %+v", res.Payload)
	}
}

func TestTriagePassBudgetEmitsFailed(t *testing.T) {
	m := &funcModel{fn: func(context.Context, int, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		t.Error("the model was called on a spent budget")
		return anthropic.BetaMessage{}, nil
	}}
	f := newTriageFixture(t, m, &recordingTool{name: "connect_status", schema: propSchema([]string{"connector"}, "connector")})
	b := f.ep.budgetWithLimit("r_1", 0)
	b.limit = 100
	b.record(usageTokens{input: 100}, true)
	f.run(t, Alert{Title: "orders stale"})
	if pl := f.triagePayload(t); pl.Status != "failed" || pl.Reason != triageReasonBudget {
		t.Fatalf("payload = %+v", pl)
	}
	if roles := f.usageRoles(t); len(roles) != 0 {
		t.Fatalf("usage for a request never sent: %v", roles)
	}
}

func TestTriageHeadlineIsCappedAndPlain(t *testing.T) {
	long := "orders: connector down\n\n## <b>bold</b> " + strings.Repeat("very long words ", 40) + " password=hunter2secret"
	m := &funcModel{fn: func(context.Context, int, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		return verdictMessage(t, map[string]any{
			"headline": long, "suspected_layer": "the-network", "confidence": "high",
			"next_check": strings.Repeat("check ", 60), "based_on_steps": []string{"s_not_ours"},
		}), nil
	}}
	// Every read fails, so a "high" from the model cannot stand.
	f := newTriageFixture(t, m, &recordingTool{name: "connect_status", schema: propSchema([]string{"connector"}, "connector"), err: errors.New("connect down")})
	f.run(t, Alert{Title: "t", Raw: watcherRaw(t, connectorDownSignal())})
	pl := f.triagePayload(t)
	if pl.Status != "ok" {
		t.Fatalf("payload = %+v", pl)
	}
	if n := utf8.RuneCountInString(pl.Headline); n > triageHeadlineRunes {
		t.Fatalf("headline is %d runes", n)
	}
	if n := utf8.RuneCountInString(pl.NextCheck); n > triageNextCheckRunes {
		t.Fatalf("next_check is %d runes", n)
	}
	if strings.ContainsAny(pl.Headline, "\n\r") || strings.Contains(pl.Headline, "  ") {
		t.Fatalf("headline is not one line: %q", pl.Headline)
	}
	if pl.SuspectedLayer != "" || pl.Confidence != "low" || len(pl.EvidenceRefs) != 0 {
		t.Fatalf("payload = %+v; want no layer, low, no refs", pl)
	}
	if got := plainLine("a b password=hunter2secret", 200); strings.Contains(got, "hunter2secret") {
		t.Fatalf("plainLine kept a secret: %q", got)
	}
}

func TestTriagePassFailures(t *testing.T) {
	refusal := mustMessage(t, `{"stop_reason":"refusal","content":[],"usage":{"input_tokens":5,"output_tokens":1}}`)
	tests := []struct {
		name   string
		answer func() (anthropic.BetaMessage, error)
		off    bool
		noTool bool
		want   string
		calls  int
	}{
		{"off", nil, true, false, triageReasonOff, 0},
		{"no tools", nil, false, true, triageReasonNoTools, 0},
		{"refused", func() (anthropic.BetaMessage, error) { return refusal, nil }, false, false, triageReasonRefused, 1},
		{"model error", func() (anthropic.BetaMessage, error) { return anthropic.BetaMessage{}, errors.New("400 bad model") }, false, false, triageReasonModelError, 1},
		{"bad json", func() (anthropic.BetaMessage, error) {
			return mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"not json"}]}`), nil
		}, false, false, triageReasonModelError, 1},
		{"empty headline", func() (anthropic.BetaMessage, error) {
			return verdictMessage(t, map[string]any{"headline": "  ", "suspected_layer": "", "confidence": "low", "next_check": "", "based_on_steps": []string{}}), nil
		}, false, false, triageReasonModelError, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &funcModel{fn: func(context.Context, int, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
				if tt.answer == nil {
					t.Error("model called")
					return anthropic.BetaMessage{}, nil
				}
				return tt.answer()
			}}
			var defs []*recordingTool
			if !tt.noTool {
				defs = append(defs, &recordingTool{name: "connect_status", schema: propSchema([]string{"connector"}, "connector"), result: tools.Result{Summary: "ok"}})
			}
			f := newTriageFixture(t, m, defs...)
			f.ep.triagePass.on = !tt.off
			res := f.run(t, Alert{Title: "t", Raw: watcherRaw(t, connectorDownSignal())})
			pl := f.triagePayload(t)
			if pl.Status != "failed" || pl.Reason != tt.want || pl.Headline != "" {
				t.Fatalf("payload = %+v, want failed/%s", pl, tt.want)
			}
			if res.Steps == nil {
				t.Fatal("steps is nil")
			}
			if got := len(m.calls()); got != tt.calls {
				t.Fatalf("model calls = %d, want %d", got, tt.calls)
			}
		})
	}
}

func TestTriagePassCancelledContextEmitsNothing(t *testing.T) {
	f := newTriageFixture(t, &funcModel{fn: func(context.Context, int, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		return anthropic.BetaMessage{}, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.ep.Triage(ctx, Alert{RoomID: "r_1"}, Deps{Tools: f.reg, Sink: f.sink}, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n := len(f.sink.events); n != 0 {
		t.Fatalf("emitted %d events", n)
	}
}

func TestTriagePassSettingsDefaults(t *testing.T) {
	got := triagePassSettings(EndpointConfig{TriagePass: true})
	if !got.on || got.model != "claude-haiku-5-5" || got.timeout != 40*time.Second {
		t.Fatalf("defaults = %+v", got)
	}
	if got := triagePassSettings(EndpointConfig{TriagePassTimeout: time.Hour}); got.timeout != 90*time.Second {
		t.Fatalf("timeout = %s, want the 90s cap", got.timeout)
	}
}
