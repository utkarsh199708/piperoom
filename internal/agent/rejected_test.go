package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// V1t: a tool call the agent refuses before running it carries
// AgentToolResultPayload.Rejected on its agent.tool_result; the text the
// model gets back is unchanged.

// toolResultEvents decodes every agent.tool_result the sink holds.
func toolResultEvents(t *testing.T, sink *fakeSink) []events.AgentToolResultPayload {
	t.Helper()
	var out []events.AgentToolResultPayload
	for _, e := range sink.ofType(events.AgentToolResult) {
		var p events.AgentToolResultPayload
		if err := json.Unmarshal(e.payload, &p); err != nil {
			t.Fatalf("decode tool_result: %v", err)
		}
		out = append(out, p)
	}
	return out
}

// onlyResult returns the single tool_result the sink holds.
func onlyResult(t *testing.T, sink *fakeSink) events.AgentToolResultPayload {
	t.Helper()
	got := toolResultEvents(t, sink)
	if len(got) != 1 {
		t.Fatalf("tool_result events = %d, want 1", len(got))
	}
	return got[0]
}

// assertStepPaired checks a refused call still has its step.started and a
// tool_call with the same step and tool_use ids, like any other step.
func assertStepPaired(t *testing.T, sink *fakeSink, res events.AgentToolResultPayload) {
	t.Helper()
	if res.StepID == "" {
		t.Fatal("refused call has no step id")
	}
	starts := sink.ofType(events.AgentStepStart)
	calls := sink.ofType(events.AgentToolCall)
	if len(starts) != 1 || len(calls) != 1 {
		t.Fatalf("step.started = %d, tool_call = %d, want 1 and 1", len(starts), len(calls))
	}
	var c events.AgentToolCallPayload
	if err := json.Unmarshal(calls[0].payload, &c); err != nil {
		t.Fatalf("decode tool_call: %v", err)
	}
	if c.StepID != res.StepID || c.ToolUseID != res.ToolUseID || c.Tool != res.Tool {
		t.Fatalf("tool_call %+v does not pair with tool_result %+v", c, res)
	}
}

// assertModelText checks the exact tool_result text the model is sent and
// that the rejection reason is not in it (the field is on the event only).
func assertModelText(t *testing.T, got toolResult, want string) {
	t.Helper()
	if !got.isError {
		t.Fatal("model tool_result is_error = false, want true")
	}
	if got.content != want {
		t.Fatalf("model text changed:\n got %s\nwant %s", got.content, want)
	}
	for _, s := range []string{"rejected", events.ToolRejectedUnknownTool, events.ToolRejectedInvalidJSON, events.ToolRejectedInvalidInput} {
		if strings.Contains(got.content, s) {
			t.Fatalf("model text carries %q: %s", s, got.content)
		}
	}
}

func readRunner(t *testing.T, sink *fakeSink, rec *recordingTool) *runner {
	t.Helper()
	r := newTestRunner(t, &scriptedModel{}, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 3})
	// The tool list run builds, so a test can call one tool directly.
	r.toolList = buildToolList(r.deps.Tools, roomToolsForKind(newRoomToolsWithCauses(r.deps, r.proposals, r.steps, r.gaps, r.causes), r.kind, r.deps.Actions))
	return r
}

func TestRejectedUnknownTool(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"no_such_tool","input":{"why":"probe"}}
		]}`),
	}}
	sink := &fakeSink{}
	r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 3})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	res := onlyResult(t, sink)
	if res.Rejected != events.ToolRejectedUnknownTool || !res.IsError {
		t.Fatalf("rejected = %q is_error = %v, want %q and true", res.Rejected, res.IsError, events.ToolRejectedUnknownTool)
	}
	assertStepPaired(t, sink, res)
	if n := len(rec.inputsAsJSON()); n != 0 {
		t.Fatalf("a tool ran %d times", n)
	}
	// The whole next request: the tool_result block carries the same text as
	// before V1t and nothing of the event's new field.
	req := messagesJSON(model.calls[len(model.calls)-1])
	want, _ := json.Marshal(wrapToolOutput("no_such_tool", res.StepID, "error: unknown tool no_such_tool"))
	if !strings.Contains(req, string(want)) {
		t.Fatalf("request lacks the unchanged tool_result text %s:\n%s", want, req)
	}
	for _, s := range []string{`"rejected"`, events.ToolRejectedUnknownTool} {
		if strings.Contains(req, s) {
			t.Fatalf("request carries %q: %s", s, req)
		}
	}
}

func TestRejectedInvalidJSON(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	sink := &fakeSink{}
	r := readRunner(t, sink, rec)
	raw := json.RawMessage(`{"connector":`)
	got := r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: raw})

	res := onlyResult(t, sink)
	if res.Rejected != events.ToolRejectedInvalidJSON || !res.IsError {
		t.Fatalf("rejected = %q is_error = %v, want %q and true", res.Rejected, res.IsError, events.ToolRejectedInvalidJSON)
	}
	assertStepPaired(t, sink, res)
	if n := len(rec.inputsAsJSON()); n != 0 {
		t.Fatalf("a tool ran %d times", n)
	}
	_, _, decErr := stripWhy(raw)
	assertModelText(t, got, wrapToolOutput("connect_status", res.StepID, "error: invalid tool input: "+decErr.Error()))
}

func TestRejectedInvalidInput(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"schema type", `{"connector":5}`},
		{"missing required", `{"why":"look"}`},
		{"unknown property", `{"connector":"c","topic":"t"}`},
		{"not an object", `["c"]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
			sink := &fakeSink{}
			r := readRunner(t, sink, rec)
			got := r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: json.RawMessage(c.input)})

			res := onlyResult(t, sink)
			if res.Rejected != events.ToolRejectedInvalidInput || !res.IsError {
				t.Fatalf("rejected = %q is_error = %v, want %q and true", res.Rejected, res.IsError, events.ToolRejectedInvalidInput)
			}
			assertStepPaired(t, sink, res)
			if n := len(rec.inputsAsJSON()); n != 0 {
				t.Fatalf("a tool ran %d times", n)
			}
			// The text is the one the model got before V1t.
			var msg string
			if cleaned, _, err := stripWhy(json.RawMessage(c.input)); err != nil {
				msg = err.Error()
			} else {
				msg = validateInput(testSchema("connector"), cleaned).Error()
			}
			assertModelText(t, got, wrapToolOutput("connect_status", res.StepID, "error: invalid tool input: "+msg))
		})
	}
}

func TestRejectedRoomToolInput(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		reason string
	}{
		{"schema type", `{"text":5}`, events.ToolRejectedInvalidInput},
		{"missing required", `{"reply_to":"c_1"}`, events.ToolRejectedInvalidInput},
		{"not JSON", `{"text":"hi"`, events.ToolRejectedInvalidJSON},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
			sink := &fakeSink{}
			r := readRunner(t, sink, rec)
			raw := json.RawMessage(c.input)
			got := r.runRoomTool(context.Background(), toolUse{id: "tu_p", name: "post_message", input: raw})

			res := onlyResult(t, sink)
			if res.Rejected != c.reason || !res.IsError || res.Tool != "post_message" || res.ToolUseID != "tu_p" {
				t.Fatalf("tool_result = %+v, want post_message tu_p rejected %q", res, c.reason)
			}
			assertStepPaired(t, sink, res)
			if n := len(sink.ofType(events.AgentMessage)); n != 0 {
				t.Fatalf("a refused post_message posted %d messages", n)
			}
			if c.reason == events.ToolRejectedInvalidJSON {
				// Text that is not JSON cannot be the event's JSON input.
				var call events.AgentToolCallPayload
				_ = json.Unmarshal(sink.ofType(events.AgentToolCall)[0].payload, &call)
				if string(call.Input) != "null" {
					t.Fatalf("tool_call input = %s, want null", call.Input)
				}
			}
			// The room tool's text has no step, exactly as before V1t, and
			// the refused step is not in the ledger the model can cite.
			schemaErr := validateInput(r.toolList.byName["post_message"].schema, raw)
			assertModelText(t, got, wrapToolOutput("post_message", "", "error: invalid input: "+schemaErr.Error()))
			if _, ok := r.steps.get(res.StepID); ok {
				t.Fatalf("refused room step %s is in the step ledger", res.StepID)
			}
		})
	}
}

// A room tool whose own check in Run refuses the input judged what the model
// concluded, not how it called the tool: no rejection, and (as before V1t) no
// tool_result event.
func TestRoomToolRunRefusalIsNotRejected(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	sink := &fakeSink{}
	r := readRunner(t, sink, rec)
	got := r.runRoomTool(context.Background(), toolUse{id: "tu_p", name: "post_message", input: json.RawMessage(`{"text":""}`)})
	if !got.isError {
		t.Fatal("empty post_message text was accepted")
	}
	if n := len(sink.ofType(events.AgentToolResult)); n != 0 {
		t.Fatalf("tool_result events = %d, want 0", n)
	}
}

func TestToolThatRanAndFailedIsNotRejected(t *testing.T) {
	for _, runErr := range []error{
		errors.New("connect: dial tcp: connection refused"),
		fmt.Errorf("%w: connector not found", tools.ErrBadInput),
	} {
		t.Run(runErr.Error(), func(t *testing.T) {
			rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), err: runErr}
			sink := &fakeSink{}
			r := readRunner(t, sink, rec)
			got := r.runReadTool(context.Background(), toolUse{id: "tu1", name: "connect_status", input: json.RawMessage(`{"connector":"c"}`)})
			if !got.isError {
				t.Fatal("failed tool returned is_error = false")
			}
			if n := len(rec.inputsAsJSON()); n != 1 {
				t.Fatalf("tool ran %d times, want 1", n)
			}
			res := onlyResult(t, sink)
			if !res.IsError || res.Rejected != "" {
				t.Fatalf("is_error = %v rejected = %q, want true and empty", res.IsError, res.Rejected)
			}
			raw := sink.ofType(events.AgentToolResult)[0].payload
			if strings.Contains(string(raw), `"rejected"`) {
				t.Fatalf("event carries rejected: %s", raw)
			}
		})
	}
}
