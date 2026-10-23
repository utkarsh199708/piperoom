package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func TestSkipsRecords(t *testing.T) {
	for _, tt := range []struct {
		name string
		tool string
		data any
		want bool
	}{
		{"tolerance all", "connect_config", map[string]string{"errors.tolerance": "all"}, true},
		{"tolerance all, spaced and cased", "connect_config", map[string]any{"errors.tolerance": " ALL "}, true},
		{"debezium skip", "connect_config", map[string]string{"event.processing.failure.handling.mode": "skip"}, true},
		{"debezium warn", "connect_config", map[string]string{"event.processing.failure.handling.mode": "warn"}, true},
		{"tolerance none", "connect_config", map[string]string{"errors.tolerance": "none"}, false},
		{"debezium fail", "connect_config", map[string]string{"event.processing.failure.handling.mode": "fail"}, false},
		{"no error keys", "connect_config", map[string]string{"connector.class": "x"}, false},
		{"other tool", "connect_status", map[string]string{"errors.tolerance": "all"}, false},
		{"nil data", "connect_config", nil, false},
		{"not an object", "connect_config", []string{"errors.tolerance", "all"}, false},
		{"not a string", "connect_config", map[string]any{"errors.tolerance": 1}, false},
		{"cannot encode", "connect_config", map[string]any{"errors.tolerance": func() {}}, false},
	} {
		if got := skipsRecords(tt.tool, tt.data); got != tt.want {
			t.Errorf("%s: skipsRecords = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestSkipRuleNoteIsFixedText(t *testing.T) {
	for _, want := range []string{"[agent rule]", "RUNNING task skip records", "connect_worker_metrics", "connect_worker_logs"} {
		if !strings.Contains(skipRuleNote, want) {
			t.Errorf("skip rule note lacks %q: %s", want, skipRuleNote)
		}
	}
	if strings.ContainsAny(skipRuleNote, "%<>") {
		t.Errorf("skip rule note looks formatted or nests markup: %s", skipRuleNote)
	}
}

// newRunnerWithTools is newTestRunner with several read tools.
func newRunnerWithTools(t *testing.T, model model, sink Sink, ts ...tools.Tool) *runner {
	t.Helper()
	reg := tools.NewRegistry()
	for _, tl := range ts {
		if err := reg.Register(tl); err != nil {
			t.Fatalf("register tool: %v", err)
		}
	}
	r, err := newRunner(Alert{Title: "orders stale", PipelineID: "orders", PipelineMap: "pg -> connect -> topic -> consumer -> warehouse"}, Deps{
		Tools:  reg,
		Inbox:  &fakeInbox{waitErr: context.Canceled},
		Sink:   sink,
		Logger: discardLogger(),
	}, Options{MaxTurns: 3})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	r.model = model
	return r
}

// firstToolResultText runs a room whose model calls tool once with input and
// returns the text of the tool_result the model then reads.
func firstToolResultText(t *testing.T, sink Sink, tool, input string, ts ...tools.Tool) string {
	t.Helper()
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"`+tool+`","input":`+input+`}
		]}`),
	}}
	r := newRunnerWithTools(t, viaSDK(model), sink, ts...)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) < 2 {
		t.Fatalf("model calls = %d", len(model.calls))
	}
	var content string
	for _, b := range lastMessage(model.calls).Content {
		if b.OfToolResult == nil {
			continue
		}
		js, err := json.Marshal(b.OfToolResult.Content)
		if err != nil {
			t.Fatal(err)
		}
		var blocks []struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(js, &blocks); err != nil {
			t.Fatal(err)
		}
		for _, blk := range blocks {
			content += blk.Text
		}
	}
	return content
}

func readTool(name string, res tools.Result) *recordingTool {
	return &recordingTool{name: name, schema: testSchema("connector"), result: res}
}

// TestSkipRuleReachesTheModel: a connect_config read showing errors.tolerance
// =all gets the skip rule after its wrapper when the room offers the worker
// tools, and nothing otherwise; the room log keeps the tool's own summary.
func TestSkipRuleReachesTheModel(t *testing.T) {
	cfg := func(tol string) *recordingTool {
		return readTool("connect_config", tools.Result{
			Summary: "errors.tolerance: " + tol,
			Data:    map[string]string{"errors.tolerance": tol},
		})
	}
	metrics := readTool("connect_worker_metrics", tools.Result{Summary: "m"})
	logs := readTool("connect_worker_logs", tools.Result{Summary: "l"})
	for _, tt := range []struct {
		name  string
		tools []tools.Tool
		want  bool
	}{
		{"skipping, worker tools offered", []tools.Tool{cfg("all"), metrics, logs}, true},
		{"failing, worker tools offered", []tools.Tool{cfg("none"), metrics, logs}, false},
		{"skipping, no worker logs tool", []tools.Tool{cfg("all"), metrics}, false},
		{"skipping, no worker tools", []tools.Tool{cfg("all")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := &fakeSink{}
			content := firstToolResultText(t, sink, "connect_config", `{"connector":"pg-orders-connector"}`, tt.tools...)
			if got := strings.Contains(content, skipRuleNote); got != tt.want {
				t.Fatalf("rule in tool_result = %v, want %v; content: %s", got, tt.want, content)
			}
			if tt.want && !strings.Contains(content, "</tool_output>\n"+skipRuleNote) {
				t.Fatalf("the rule must follow the untrusted wrapper, got: %s", content)
			}
			evs := sink.ofType(events.AgentToolResult)
			if len(evs) != 1 {
				t.Fatalf("tool result events = %d, want 1", len(evs))
			}
			var p events.AgentToolResultPayload
			if err := json.Unmarshal(evs[0].payload, &p); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(p.Summary, "agent rule") {
				t.Errorf("tool_result summary carries the rule: %q", p.Summary)
			}
		})
	}
}
