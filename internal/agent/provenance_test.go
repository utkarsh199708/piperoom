package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// callingTool records the given calls under its context, like a client would,
// and returns res and err.
type callingTool struct {
	name  string
	calls []tools.Call
	res   tools.Result
	err   error
}

func (t *callingTool) Name() string                { return t.name }
func (t *callingTool) Description() string         { return "calling test tool" }
func (t *callingTool) InputSchema() map[string]any { return testSchema("connector") }
func (t *callingTool) Kind() tools.Kind            { return tools.KindRead }
func (t *callingTool) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	for _, c := range t.calls {
		tools.Record(ctx, c)
	}
	return t.res, t.err
}

func provRunner(t *testing.T, tool tools.Tool) (*runner, *fakeSink) {
	t.Helper()
	reg := tools.NewRegistry()
	if err := reg.Register(tool); err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	r, err := newRunner(Alert{}, Deps{Tools: reg, Inbox: &fakeInbox{}, Sink: sink, Logger: discardLogger()}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	r.toolList = buildToolList(r.deps.Tools, newRoomToolsWith(r.deps, r.proposals, r.steps))
	return r, sink
}

func lastToolResult(t *testing.T, sink *fakeSink) events.AgentToolResultPayload {
	t.Helper()
	evs := sink.ofType(events.AgentToolResult)
	if len(evs) == 0 {
		t.Fatal("no agent.tool_result emitted")
	}
	var p events.AgentToolResultPayload
	if err := json.Unmarshal(evs[len(evs)-1].payload, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

var provInput = json.RawMessage(`{"why":"check","connector":"c"}`)

func TestReadToolProvenanceEmitted(t *testing.T) {
	at := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	tool := &callingTool{
		name:  "connect_status",
		calls: []tools.Call{{Kind: tools.CallHTTP, Target: "http://connect:8083", Text: "GET /connectors/c/status", At: at, TookMS: 12}},
		res:   tools.Result{Summary: "ok", Provenance: []tools.Call{{Kind: tools.CallSQL, Text: "SELECT 1", At: at}}},
	}
	r, sink := provRunner(t, tool)
	r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: provInput})
	p := lastToolResult(t, sink)
	if len(p.Provenance) != 2 {
		t.Fatalf("provenance = %+v, want 2 calls", p.Provenance)
	}
	if p.Provenance[0].Text != "SELECT 1" || p.Provenance[1].Text != "GET /connectors/c/status" {
		t.Fatalf("tool-set call must come first, then recorded: %+v", p.Provenance)
	}
	if got := p.Provenance[1]; got.Kind != tools.CallHTTP || got.TookMS != 12 || !got.At.Equal(at) || got.Target != "http://connect:8083" {
		t.Fatalf("call = %+v", got)
	}
}

func TestErroredReadToolKeepsProvenance(t *testing.T) {
	tool := &callingTool{
		name:  "connect_status",
		calls: []tools.Call{{Kind: tools.CallHTTP, Text: "GET /connectors/c/status"}},
		err:   errors.New("connect: status 500"),
	}
	r, sink := provRunner(t, tool)
	res := r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: provInput})
	if !res.isError {
		t.Fatal("want an error result")
	}
	p := lastToolResult(t, sink)
	if !p.IsError || len(p.Provenance) != 1 || p.Provenance[0].Text != "GET /connectors/c/status" {
		t.Fatalf("payload = %+v", p)
	}
}

func TestProvenanceRedacted(t *testing.T) {
	tool := &callingTool{
		name: "connect_status",
		calls: []tools.Call{{
			Kind: tools.CallSQL, Target: "postgres://app:hunter2secret@db:5432/orders",
			Text: "SELECT 1 -- password=hunter2secret", ParamsRedacted: []string{"password=hunter2secret"},
		}},
		res: tools.Result{Summary: "ok"},
	}
	r, sink := provRunner(t, tool)
	r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: provInput})
	b, _ := json.Marshal(lastToolResult(t, sink).Provenance)
	if strings.Contains(string(b), "hunter2secret") {
		t.Fatalf("secret in provenance: %s", b)
	}
}

func TestProvenanceCapped(t *testing.T) {
	var calls []tools.Call
	for i := 0; i < tools.MaxProvenanceCalls+3; i++ {
		calls = append(calls, tools.Call{Kind: tools.CallHTTP, Text: strings.Repeat("é", tools.MaxCallTextBytes)})
	}
	r, sink := provRunner(t, &callingTool{name: "connect_status", calls: calls, res: tools.Result{Summary: "ok"}})
	r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: provInput})
	p := lastToolResult(t, sink)
	if len(p.Provenance) != tools.MaxProvenanceCalls {
		t.Fatalf("got %d calls, want %d", len(p.Provenance), tools.MaxProvenanceCalls)
	}
	if len(p.Provenance[0].Text) > tools.MaxCallTextBytes {
		t.Fatalf("text is %d bytes, over the cap", len(p.Provenance[0].Text))
	}
}

func TestReadToolWithoutCallsHasNoProvenance(t *testing.T) {
	r, sink := provRunner(t, &callingTool{name: "connect_status", res: tools.Result{Summary: "ok"}})
	r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: provInput})
	if p := lastToolResult(t, sink); p.Provenance != nil {
		t.Fatalf("provenance = %+v, want none", p.Provenance)
	}
}

func TestProvenanceNeverReachesTheModel(t *testing.T) {
	tool := &callingTool{
		name:  "connect_status",
		calls: []tools.Call{{Kind: tools.CallHTTP, Text: "GET /connectors/SENTINEL-PATH/status"}},
		res:   tools.Result{Summary: "ok"},
	}
	r, _ := provRunner(t, tool)
	res := r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: provInput})
	if strings.Contains(res.content, "SENTINEL-PATH") {
		t.Fatalf("provenance reached the model: %s", res.content)
	}
}

func TestVerifyStepKeepsProvenance(t *testing.T) {
	tool := &callingTool{
		name:  "connect_status",
		calls: []tools.Call{{Kind: tools.CallHTTP, Text: "GET /connectors/c/status"}},
		res:   tools.Result{Summary: "ok"},
	}
	r, sink := provRunner(t, tool)
	if _, err := r.runStep(context.Background(), "verify", "connect_status", json.RawMessage(`{"connector":"c"}`)); err != nil {
		t.Fatal(err)
	}
	if p := lastToolResult(t, sink); len(p.Provenance) != 1 {
		t.Fatalf("provenance = %+v", p.Provenance)
	}
}

// ---- step ledger and update_hypothesis checks ----

const (
	stepA = "s_01J0000000000000000000000A"
	stepB = "s_01J0000000000000000000000B"
)

func hypoRun(t *testing.T, r *runner, steps []string, conf string) toolResult {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"text": "the slot fell behind", "based_on_steps": steps, "confidence": conf})
	res := r.runRoomTool(context.Background(), toolUse{id: "tu_h", name: "update_hypothesis", input: b})
	return res
}

func hypoRunner(t *testing.T) (*runner, *fakeSink) {
	r, sink := provRunner(t, &callingTool{name: "connect_status", res: tools.Result{Summary: "ok"}})
	r.steps.add(stepA, "connect_status", "task 0 FAILED", false)
	r.steps.add(stepB, "pg_replication_slots", "error: timeout", true)
	return r, sink
}

func TestHypothesisAcceptsKnownStep(t *testing.T) {
	r, sink := hypoRunner(t)
	res := hypoRun(t, r, []string{stepA}, "high")
	if res.isError {
		t.Fatalf("refused: %s", res.content)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 1 {
		t.Fatalf("%d hypotheses emitted, want 1", n)
	}
}

func TestHypothesisRefusesUnknownStep(t *testing.T) {
	r, sink := hypoRunner(t)
	res := hypoRun(t, r, []string{"s_01J0000000000000000000000Z"}, "low")
	if !res.isError || !strings.Contains(res.content, "is not a step of this run (known: "+stepA+", "+stepB+")") {
		t.Fatalf("content = %s", res.content)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 0 {
		t.Fatalf("refused hypothesis was emitted")
	}
}

func TestHypothesisRefusesErroredStep(t *testing.T) {
	r, _ := hypoRunner(t)
	res := hypoRun(t, r, []string{stepA, stepB}, "medium")
	if !res.isError || !strings.Contains(res.content, stepB+" errored; it supports nothing") {
		t.Fatalf("content = %s", res.content)
	}
}

func TestHypothesisRefusesMalformedID(t *testing.T) {
	r, _ := hypoRunner(t)
	for _, id := range []string{"3", "s_short", "p_01J0000000000000000000000A", strings.ToLower(stepA), "s_01J0000000000000000000000A "} {
		res := hypoRun(t, r, []string{id}, "low")
		if !res.isError || !strings.Contains(res.content, "is not a step id; cite the step= attribute of a tool_output") {
			t.Fatalf("id %q: content = %s", id, res.content)
		}
	}
}

func TestHypothesisNeedsStepsAtMediumHigh(t *testing.T) {
	r, _ := hypoRunner(t)
	for _, conf := range []string{"medium", "high"} {
		res := hypoRun(t, r, []string{}, conf)
		if !res.isError || !strings.Contains(res.content, "medium or high confidence needs at least one supporting step") {
			t.Fatalf("%s: content = %s", conf, res.content)
		}
	}
}

func TestHypothesisLowWithoutSteps(t *testing.T) {
	r, sink := hypoRunner(t)
	res := hypoRun(t, r, []string{}, "low")
	if res.isError {
		t.Fatalf("refused: %s", res.content)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 1 {
		t.Fatalf("%d hypotheses emitted, want 1", n)
	}
}

func TestHypothesisKnownListIsTheNewestEight(t *testing.T) {
	r, _ := hypoRunner(t)
	var ids []string
	for i := 0; i < 12; i++ {
		id := newStepID()
		ids = append(ids, id)
		r.steps.add(id, "t", "x", false)
	}
	res := hypoRun(t, r, []string{"s_01J0000000000000000000000Z"}, "low")
	if strings.Contains(res.content, ids[3]) || !strings.Contains(res.content, ids[4]) || !strings.Contains(res.content, ids[11]) {
		t.Fatalf("known list is not the 8 newest: %s", res.content)
	}
}

func TestStepsAreLedgeredByEveryPath(t *testing.T) {
	// A successful read, an errored read and a validation failure each open a
	// step the hypothesis may (or may not) cite.
	okTool := &callingTool{name: "connect_status", res: tools.Result{Summary: "ok"}}
	r, sink := provRunner(t, okTool)
	r.runReadTool(context.Background(), toolUse{id: "1", name: "connect_status", input: provInput})
	okID := lastToolResult(t, sink).StepID
	if f, ok := r.steps.get(okID); !ok || f.isError || f.summary != "ok" {
		t.Fatalf("ok step = %+v, %v", f, ok)
	}
	r.runReadTool(context.Background(), toolUse{id: "2", name: "connect_status", input: json.RawMessage(`{"why":"x"}`)})
	badID := lastToolResult(t, sink).StepID
	if f, ok := r.steps.get(badID); !ok || !f.isError {
		t.Fatalf("invalid-input step = %+v, %v", f, ok)
	}
	if _, err := r.runStep(context.Background(), "verify", "connect_status", json.RawMessage(`{"connector":"c"}`)); err != nil {
		t.Fatal(err)
	}
	if id := lastToolResult(t, sink).StepID; id == "" {
		t.Fatal("no step id")
	} else if _, ok := r.steps.get(id); !ok {
		t.Fatal("verify step not in the ledger")
	}
}

func TestNilLedgerSkipsChecks(t *testing.T) {
	sink := &fakeSink{}
	tool := newRoomToolsWith(Deps{Sink: sink}, nil, nil)[0]
	b, _ := json.Marshal(map[string]any{"text": "x", "based_on_steps": []string{}, "confidence": "high"})
	if _, err := tool.Run(context.Background(), b); err != nil {
		t.Fatalf("a harness without a ledger must not be checked: %v", err)
	}
}

func TestStepLedgerCutsLongSummaryOnRuneBoundary(t *testing.T) {
	l := newStepLedger()
	l.add(stepA, "t", strings.Repeat("é", maxLedgerSummaryBytes), false)
	f, _ := l.get(stepA)
	if len(f.summary) > maxLedgerSummaryBytes || strings.ContainsRune(f.summary, '�') {
		t.Fatalf("summary is %d bytes", len(f.summary))
	}
}
