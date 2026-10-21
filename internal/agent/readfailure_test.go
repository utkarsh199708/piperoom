package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// KAN-408: a failed read is classified, stamped with a stable fact, and an
// unresolved unreachable gap refuses a healthy verdict.

func TestClassifyReadFailure(t *testing.T) {
	for _, tt := range []struct {
		name   string
		tool   string
		err    error
		class  readFailureClass
		reason string
	}{
		{"connection refused typed", "schema_registry_diff",
			fmt.Errorf("schemareg: GET /subjects: %w", syscall.ECONNREFUSED), readFailureUnreachable, "connection refused"},
		{"connection refused text", "schema_registry_diff",
			errors.New("schemareg: GET /subjects: dial tcp 127.0.0.1:8081: connectex: No connection could be made because the target machine actively refused it."),
			readFailureUnreachable, "connection refused"},
		{"timeout deadline", "schema_registry_versions",
			fmt.Errorf("schemareg: GET /subjects/x/versions: %w", context.DeadlineExceeded), readFailureUnreachable, "timeout"},
		{"timeout i/o", "connect_status",
			errors.New("connect: GET /connectors: Get \"http://localhost:8083\": i/o timeout"), readFailureUnreachable, "timeout"},
		{"dns", "schema_registry_versions",
			&net.DNSError{Err: "no such host", Name: "schema-registry"}, readFailureUnreachable, "dns"},
		{"dns text", "schema_registry_versions",
			errors.New("schemareg: lookup schema-registry: no such host"), readFailureUnreachable, "dns"},
		{"auth 401", "schema_registry_versions",
			errors.New("schemareg: GET /subjects: status 401: Unauthorized"), readFailureAuth, "401"},
		{"auth 403", "connect_status",
			errors.New("connect: GET /connectors: status 403: Forbidden"), readFailureAuth, "403"},
		{"not found typed", "schema_registry_versions",
			fmt.Errorf("schemareg: GET /subjects/x/versions/latest: %w", clients.ErrNotFound), readFailureNotFound, "not found"},
		{"not found 404", "schema_registry_versions",
			errors.New("schemareg: GET /subjects/x/versions/latest: status 404: not found"), readFailureNotFound, "404"},
		{"bad input is not a component fact", "connect_status",
			fmt.Errorf("%w: connector is required", tools.ErrBadInput), readFailureOther, ""},
		{"out of scope is not a component fact", "pg_row",
			fmt.Errorf("%w: table is outside pipeline orders", tools.ErrOutOfScope), readFailureOther, ""},
		{"unrelated error", "pipeline_map", errors.New("pipeline map: unknown pipeline"), readFailureOther, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := classifyReadFailure(tt.tool, tt.err)
			if f.class != tt.class || f.reason != tt.reason {
				t.Fatalf("classifyReadFailure = (%v, %q), want (%v, %q)", f.class, f.reason, tt.class, tt.reason)
			}
			if f.component != componentForTool(tt.tool) {
				t.Fatalf("component = %q, want %q", f.component, componentForTool(tt.tool))
			}
		})
	}
}

// A fact the A2/A3 tools emit themselves is reused unchanged, not classified
// from the text and not prefixed twice; the triage seeds parse it back.
func TestParseFailureFactRoundTrip(t *testing.T) {
	raw := "error: unreachable: schema-registry (connection refused): schemareg: GET /subjects: dial tcp"
	f, ok := parseFailureFact("schema_registry_diff", raw)
	if !ok || f.class != readFailureUnreachable || f.component != "schema-registry" || f.reason != "connection refused" {
		t.Fatalf("parseFailureFact = %+v, %v", f, ok)
	}
	if got, want := failedReadSummary(f, raw), raw; got != want {
		t.Fatalf("failedReadSummary doubled the fact:\n got %q\nwant %q", got, want)
	}
	if f := classifyReadFailure("schema_registry_diff", errors.New(raw)); f.component != "schema-registry" || f.class != readFailureUnreachable {
		t.Fatalf("classifyReadFailure did not reuse the emitted fact: %+v", f)
	}
	if _, ok := parseFailureFact("schema_registry_diff", "error: bad input"); ok {
		t.Fatal("parseFailureFact accepted text with no fact")
	}
}

func TestReadFailuresTracker(t *testing.T) {
	gaps := newReadFailures()
	if _, ok := gaps.unreachable(); ok {
		t.Fatal("a fresh tracker has an open gap")
	}
	gaps.record(readFailure{class: readFailureUnreachable, tool: "schema_registry_diff", component: "schema-registry", reason: "connection refused", stepID: "s_1"})
	if f, ok := gaps.unreachable(); !ok || f.stepID != "s_1" {
		t.Fatalf("unreachable = %+v, %v; want the open gap", f, ok)
	}
	// Auth and not-found are findings, not outages: they never block.
	auth := readFailure{class: readFailureAuth, tool: "schema_registry_versions", component: "schema-registry", reason: "401"}
	gaps.record(auth)
	if f, _ := gaps.unreachable(); f.stepID != "s_1" {
		t.Fatalf("auth replaced the unreachable gap: %+v", f)
	}
	// A successful read of the same tool resolves it as a blip.
	if f, ok := gaps.succeeded("kafka_consumer_lag"); ok {
		t.Fatalf("a success of another tool resolved %+v", f)
	}
	f, ok := gaps.succeeded("schema_registry_diff")
	if !ok || f.stepID != "s_1" {
		t.Fatalf("succeeded = %+v, %v; want the earlier failure", f, ok)
	}
	if _, ok := gaps.unreachable(); ok {
		t.Fatal("the resolved gap still blocks a healthy verdict")
	}
	if got, want := blipSummary(f, "v3 BC"), "blip: unreachable: schema-registry (connection refused) at s_1; this read now answers: v3 BC"; got != want {
		t.Fatalf("blipSummary = %q, want %q", got, want)
	}
}

func TestReadFailuresSeedTriage(t *testing.T) {
	gaps := newReadFailures()
	gaps.seedTriage(&TriageResult{Steps: []TriageStep{
		{StepID: "s_1", Tool: "schema_registry_diff", Summary: "error: unreachable: schema-registry (connection refused): dial tcp", IsError: true},
		{StepID: "s_2", Tool: "connect_status", Summary: "task 0 FAILED", IsError: false},
	}})
	f, ok := gaps.unreachable()
	if !ok || f.stepID != "s_1" || f.component != "schema-registry" {
		t.Fatalf("a first-look unreachable read was not seeded: %+v, %v", f, ok)
	}
	// A triage error without the fact (an old recording) is not guessed at.
	gaps2 := newReadFailures()
	gaps2.seedTriage(&TriageResult{Steps: []TriageStep{
		{StepID: "s_1", Tool: "schema_registry_diff", Summary: "error: connection refused", IsError: true},
	}})
	if _, ok := gaps2.unreachable(); ok {
		t.Fatal("seeded a gap from an unclassified summary")
	}
}

func TestHealthyClaim(t *testing.T) {
	for _, tt := range []struct {
		text string
		want bool
	}{
		{"the pipeline is healthy", true},
		{"the pipeline is healthy now and there is nothing to fix", true},
		{"nothing needs fixing", true},
		{"there are no issues", true},
		{"Kafka is fine", true},
		{"the consumer caught up", true},
		{"the pipeline is not healthy", false},
		{"the registry is unreachable; no data loss yet, risk rising", false},
		{"no data loss yet and the risk is rising", false},
		{"healthy except the registry is unreachable", false},
		{"it would be healthy if the registry answered", false},
		{"is the pipeline healthy?", false},
		{"the registry is unreachable and I cannot say the pipeline is healthy", false},
	} {
		if got := healthyClaim(tt.text); got != tt.want {
			t.Errorf("healthyClaim(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

// A healthy verdict is refused while a component is unreachable, names the
// evidence and what to check, and passes once the read succeeds; auth and
// not-found gaps never block (KAN-408, TC1-TC3).
func TestHealthyVerdictBlockedWhileUnreachable(t *testing.T) {
	gaps := newReadFailures()
	gaps.record(readFailure{class: readFailureUnreachable, tool: "schema_registry_diff",
		component: "schema-registry", reason: "connection refused", stepID: "s_1"})
	sink := &fakeSink{}
	room := newRoomToolsWithGaps(Deps{Sink: sink}, nil, nil, gaps)
	upd := room[0]
	hyp := json.RawMessage(`{"text":"the pipeline is healthy","based_on_steps":[],"confidence":"low"}`)

	_, err := upd.Run(context.Background(), hyp)
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("healthy verdict while unreachable: err = %v, want ErrBadInput", err)
	}
	for _, want := range []string{"blocked", "schema-registry is unreachable", "unreachable: schema-registry (connection refused)", "at s_1", "what to check"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err.Error(), want)
		}
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 0 {
		t.Fatalf("refused hypothesis was recorded %d time(s)", n)
	}

	// "no data loss yet, risk rising" is the verdict the lead review allows.
	ok := json.RawMessage(`{"text":"schema-registry is unreachable; no data loss yet, risk rising","based_on_steps":[],"confidence":"low"}`)
	if _, err := upd.Run(context.Background(), ok); err != nil {
		t.Fatalf("evidence-gap hypothesis refused: %v", err)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 1 {
		t.Fatalf("hypotheses = %d, want 1", n)
	}

	// post_message is guarded the same way, while the gap is still open.
	msg := room[2]
	_, err = msg.Run(context.Background(), json.RawMessage(`{"text":"All clear, the pipeline is healthy"}`))
	if err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("healthy post_message while unreachable: err = %v", err)
	}

	// A successful read of the same tool clears the gap: healthy is allowed.
	gaps.succeeded("schema_registry_diff")
	if _, err := upd.Run(context.Background(), hyp); err != nil {
		t.Fatalf("healthy verdict after a successful read: %v", err)
	}
	if n := len(sink.ofType(events.AgentHypothesis)); n != 2 {
		t.Fatalf("hypotheses = %d, want 2", n)
	}
}

// Auth and not-found failures are reported as facts but are not outages, so a
// healthy verdict still passes (KAN-408, TC2).
func TestAuthAndNotFoundDoNotBlockHealthy(t *testing.T) {
	for _, class := range []readFailureClass{readFailureAuth, readFailureNotFound} {
		gaps := newReadFailures()
		gaps.record(readFailure{class: class, tool: "schema_registry_versions", component: "schema-registry", reason: "401", stepID: "s_1"})
		sink := &fakeSink{}
		upd := newRoomToolsWithGaps(Deps{Sink: sink}, nil, nil, gaps)[0]
		_, err := upd.Run(context.Background(), json.RawMessage(`{"text":"the pipeline is healthy","based_on_steps":[],"confidence":"low"}`))
		if err != nil {
			t.Fatalf("class %v blocked a healthy verdict: %v", class, err)
		}
	}
}

// outcomeTool returns a scripted outcome per call, so one test can run a read
// that fails and a later read that succeeds.
type outcomeTool struct {
	name    string
	outcome []struct {
		res tools.Result
		err error
	}
	mu    sync.Mutex
	calls int
}

func (t *outcomeTool) Name() string                { return t.name }
func (t *outcomeTool) Description() string         { return "scripted outcome tool" }
func (t *outcomeTool) InputSchema() map[string]any { return testSchema() }
func (t *outcomeTool) Kind() tools.Kind            { return tools.KindRead }
func (t *outcomeTool) Run(_ context.Context, _ json.RawMessage) (tools.Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	i := t.calls
	t.calls++
	if i < len(t.outcome) {
		return t.outcome[i].res, t.outcome[i].err
	}
	return tools.Result{Summary: "ok"}, nil
}

// The runner stamps the fact on a failed read, refuses the healthy verdict
// that follows, then reports the later successful read as a blip and accepts
// the healthy verdict (KAN-408, TC1-TC3) — no model call.
func TestRunUnreachableBlocksHealthyThenBlip(t *testing.T) {
	refused := fmt.Errorf("schemareg: GET /subjects/orders-value/versions/latest: %w", syscall.ECONNREFUSED)
	tool := &outcomeTool{name: "schema_registry_diff", outcome: []struct {
		res tools.Result
		err error
	}{
		{err: refused},
		{res: tools.Result{Summary: "pg.public.orders-value v3 BACKWARD"}},
	}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu1","name":"schema_registry_diff","input":{}}]}`),
		mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu2","name":"update_hypothesis","input":{"text":"the pipeline is healthy","based_on_steps":[],"confidence":"low"}}]}`),
		mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu3","name":"schema_registry_diff","input":{}}]}`),
		mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu4","name":"update_hypothesis","input":{"text":"the pipeline is healthy","based_on_steps":[],"confidence":"low"}}]}`),
	}}
	sink := &fakeSink{}
	r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, tool, Options{MaxTurns: 6})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	results := sink.ofType(events.AgentToolResult)
	if len(results) != 2 {
		t.Fatalf("read results = %d, want 2", len(results))
	}
	var failed, blip events.AgentToolResultPayload
	if err := json.Unmarshal(results[0].payload, &failed); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(results[1].payload, &blip); err != nil {
		t.Fatal(err)
	}
	if !failed.IsError || !strings.HasPrefix(failed.Summary, "error: unreachable: schema-registry (connection refused): ") {
		t.Fatalf("failed read summary = %q", failed.Summary)
	}
	if !strings.Contains(blip.Summary, "blip: unreachable: schema-registry (connection refused)") || !strings.Contains(blip.Summary, "now answers") {
		t.Fatalf("successful read after a failure was not reported as a blip: %q", blip.Summary)
	}

	// The healthy verdict after the failure was refused; the one after the
	// successful read was recorded.
	hypotheses := sink.ofType(events.AgentHypothesis)
	if len(hypotheses) != 1 {
		t.Fatalf("hypotheses = %d, want exactly the post-success one", len(hypotheses))
	}
	var gapErr string
	for _, call := range model.calls {
		for _, b := range call[len(call)-1].Content {
			if b.OfToolResult == nil || b.OfToolResult.ToolUseID != "tu2" {
				continue
			}
			js, _ := json.Marshal(b.OfToolResult)
			gapErr = string(js)
		}
	}
	if !strings.Contains(gapErr, `"is_error":true`) || !strings.Contains(gapErr, "blocked") {
		t.Fatalf("healthy verdict before the re-read was not refused: %s", gapErr)
	}
	// The model saw the blip fact before it recorded the accepted hypothesis.
	var sawBlip bool
	for _, call := range model.calls {
		js := messagesJSON(call)
		if strings.Contains(js, "blip: unreachable: schema-registry") {
			sawBlip = true
		}
	}
	if !sawBlip {
		t.Fatal("the model never saw the blip fact")
	}
}
