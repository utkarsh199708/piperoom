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

// triageAnchorModel stands in for a model that trusts a triage layer stated as
// a suspect: when the opening message names the triage's layer, it adopts that
// layer without a read (the anchoring KAN-402 measured). Otherwise it reads
// connect_status and reports the cause the read shows.
type triageAnchorModel struct {
	t      *testing.T
	layer  string
	turn   int
	opened string // the first request's messages, for assertions
}

func (m *triageAnchorModel) Turn(_ context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	switch m.turn {
	case 0:
		m.turn++
		m.opened = messagesJSON(p.Messages)
		if m.layer != "" && strings.Contains(m.opened, "First look suspects: "+m.layer) {
			return mustMessage(m.t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu_anchor","name":"update_hypothesis",
				"input":{"text":"`+m.layer+` is at fault","based_on_steps":["s_triage01"],"confidence":"high"}}]}`), nil
		}
		return mustMessage(m.t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu_read","name":"connect_status",
			"input":{"connector":"orders-source"}}]}`), nil
	case 1:
		m.turn++
		return mustMessage(m.t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu_cause","name":"update_hypothesis",
			"input":{"text":"kafka-connect: task 0 FAILED on orders-source","based_on_steps":["s_triage01"],"confidence":"high"}}]}`), nil
	}
	return mustMessage(m.t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`), nil
}

// triageWithLayer is a first look whose verdict names layer and whose one read
// shows the real fault in kafka-connect.
func triageWithLayer(layer string) *TriageResult {
	return &TriageResult{
		Payload: events.AgentTriagePayload{
			Status: "ok", Headline: "orders-source: task 0 FAILED", SuspectedLayer: layer,
			Confidence: "medium", EvidenceRefs: []string{"s_triage01"},
		},
		Steps: []TriageStep{{StepID: "s_triage01", Tool: "connect_status", Summary: "orders-source RUNNING; task 0 FAILED: MISSING_UNION_BRANCH"}},
	}
}

// TestRunDoesNotAnchorOnTriageLayer pins KAN-402's core behaviour: whether the
// triage named the wrong layer or none, the full run's first message carries
// the triage reads but not the layer as a stated suspect, and the
// investigation reaches the cause the read shows. The fake model adopts the
// layer only when the message states it, so a regression that puts "First look
// suspects" back makes the first hypothesis name the wrong layer.
func TestRunDoesNotAnchorOnTriageLayer(t *testing.T) {
	tests := []struct {
		name  string
		layer string
	}{
		{"wrong triage layer", "schema-registry"},
		{"no triage layer", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingTool{
				name: "connect_status", schema: testSchema("connector"),
				result: tools.Result{Summary: "orders-source RUNNING; task 0 FAILED: MISSING_UNION_BRANCH"},
			}
			reg := tools.NewRegistry()
			if err := reg.Register(rec); err != nil {
				t.Fatal(err)
			}
			m := &triageAnchorModel{t: t, layer: tt.layer}
			sink := &fakeSink{}
			r, err := newRunnerWith(
				Alert{Title: "orders stale", PipelineID: "orders", Triage: triageWithLayer(tt.layer)},
				Deps{Tools: reg, Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger()},
				Options{MaxTurns: 4}, m,
			)
			if err != nil {
				t.Fatalf("newRunnerWith: %v", err)
			}
			if err := r.run(context.Background()); err != nil {
				t.Fatalf("run: %v", err)
			}

			// The triage evidence reaches the run; the layer does not.
			for _, want := range []string{"s_triage01", "MISSING_UNION_BRANCH", "The first look is a prior, not evidence"} {
				if !strings.Contains(m.opened, want) {
					t.Fatalf("the first message misses %q:\n%s", want, m.opened)
				}
			}
			if strings.Contains(m.opened, "First look suspects") {
				t.Fatalf("the triage layer is stated as a suspect:\n%s", m.opened)
			}
			if tt.layer != "" && strings.Contains(m.opened, tt.layer) {
				t.Fatalf("the triage layer %q reached the first message:\n%s", tt.layer, m.opened)
			}

			hyps := sink.ofType(events.AgentHypothesis)
			if len(hyps) == 0 {
				t.Fatal("no hypothesis was emitted")
			}
			var first events.AgentHypothesisPayload
			if err := json.Unmarshal(hyps[0].payload, &first); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(first.Text, "kafka-connect") {
				t.Fatalf("the first hypothesis followed the triage layer instead of the read: %+v", first)
			}
		})
	}
}
