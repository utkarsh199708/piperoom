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

// TestPausedConnectorReads pins which tool payloads count as a paused
// connector: only a connector row (a name and a state) of a connector-state
// read, in either state that stops data without a failure to diagnose.
func TestPausedConnectorReads(t *testing.T) {
	for _, tt := range []struct {
		name string
		tool string
		data any
		want bool
	}{
		{"status PAUSED", "connect_status", map[string]any{"name": "pg-orders-connector", "state": "PAUSED"}, true},
		{"status STOPPED", "connect_status", map[string]any{"name": "pg-orders-connector", "state": "STOPPED"}, true},
		{"status lower case", "connect_status", map[string]any{"name": "pg-orders-connector", "state": " paused "}, true},
		{"status RUNNING", "connect_status", map[string]any{"name": "pg-orders-connector", "state": "RUNNING"}, false},
		{"status FAILED", "connect_status", map[string]any{"name": "pg-orders-connector", "state": "FAILED"}, false},
		{"list one paused", "connect_list_connectors", []any{
			map[string]any{"name": "orders-sink", "state": "RUNNING"},
			map[string]any{"name": "pg-orders-connector", "state": "PAUSED"},
		}, true},
		{"list all running", "connect_list_connectors", []any{
			map[string]any{"name": "orders-sink", "state": "RUNNING"},
		}, false},
		{"task paused under a running connector", "connect_status", map[string]any{
			"name": "pg-orders-connector", "state": "RUNNING",
			"tasks": []any{map[string]any{"id": float64(0), "state": "PAUSED"}},
		}, false},
		{"other tool with a state", "pg_replication_slots", map[string]any{"name": "slot", "state": "PAUSED"}, false},
		{"nil data", "connect_status", nil, false},
		{"malformed data", "connect_status", make(chan int), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := pausedConnector(tt.tool, tt.data); got != tt.want {
				t.Fatalf("pausedConnector(%s, %#v) = %v, want %v", tt.tool, tt.data, got, tt.want)
			}
		})
	}
}

// TestPauseRuleNoteWording pins the rule the model reads beside the evidence
// (KAN-406): the pause is the incident unless a recorded person's pause
// explains it, the connector layer is named, and "source quiet" needs the
// source's own newest write to be old.
func TestPauseRuleNoteWording(t *testing.T) {
	for _, want := range []string{
		"A PAUSED or STOPPED connector is the incident",
		"not a deliberate pause",
		"a maintenance window",
		"a connect_pause_connector this room executed",
		"a change record naming who and when",
		"name the connector as the incident in the kafka-connect layer",
		"ask who paused it and when",
		"propose connect_resume_connector after the resume checks",
		`before saying "source quiet"`,
	} {
		if !strings.Contains(pauseRuleNote, want) {
			t.Errorf("pause rule note lacks %q: %s", want, pauseRuleNote)
		}
	}
	// The note is fixed agent text: it must not interpolate a connector name
	// or any tool text that could forge a rule.
	if strings.Contains(pauseRuleNote, "%") {
		t.Errorf("pause rule note looks formatted: %s", pauseRuleNote)
	}
}

// TestPauseRuleReachesTheModel drives a connect_status read through the loop:
// a PAUSED connector's tool_result carries the rule after the untrusted
// wrapper, while the room event's summary stays pure evidence and a RUNNING
// connector gets no rule.
func TestPauseRuleReachesTheModel(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state string
		want  bool
	}{
		{"paused", "PAUSED", true},
		{"stopped", "STOPPED", true},
		{"running", "RUNNING", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingTool{
				name:   "connect_status",
				schema: testSchema("connector"),
				result: tools.Result{
					Summary: "pg-orders-connector (source): " + tt.state,
					Data:    map[string]any{"name": "pg-orders-connector", "state": tt.state},
				},
			}
			model := &scriptedModel{turns: []anthropic.BetaMessage{
				mustMessage(t, `{"stop_reason":"tool_use","content":[
					{"type":"tool_use","id":"tu1","name":"connect_status","input":{"connector":"pg-orders-connector"}}
				]}`),
			}}
			sink := &fakeSink{}
			r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 3})
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
				// Decode the text blocks so the wrapper's angle brackets are
				// readable: a plain JSON search sees them escaped as \u003c.
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
			got := strings.Contains(content, "name the connector as the incident in the kafka-connect layer")
			if got != tt.want {
				t.Fatalf("state %s: rule in tool_result = %v, want %v; content: %s", tt.state, got, tt.want, content)
			}
			if tt.want && !strings.Contains(content, "</tool_output>\n[agent rule]") {
				t.Fatalf("state %s: the rule must follow the untrusted wrapper, got: %s", tt.state, content)
			}

			// The rule is guidance, not evidence: the room log's summary stays
			// the tool's own words.
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

// TestPauseRuleNoteIsNotFormatted guards the note's shape after a future
// edit: it is a constant, so no tool text can reach it.
func TestPauseRuleNoteHasNoToolPlaceholders(t *testing.T) {
	if strings.Contains(pauseRuleNote, "<tool_output") || strings.Contains(pauseRuleNote, "</tool_output>") {
		t.Fatalf("pause rule note nests a tool wrapper: %s", pauseRuleNote)
	}
}
