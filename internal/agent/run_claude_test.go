//go:build claude

package agent

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent/fake"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// TestClaudeReachesSchemaChangeHypothesis runs the real Claude loop against
// fake tools that replay the dev-stack schema-change scenario. It needs
// ANTHROPIC_API_KEY and the `claude` build tag:
//
//	go test -tags claude ./internal/agent -run TestClaudeReachesSchemaChangeHypothesis
func TestClaudeReachesSchemaChangeHypothesis(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}

	reg := tools.NewRegistry()
	if err := fake.Register(reg); err != nil {
		t.Fatalf("register fake tools: %v", err)
	}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}

	r, err := newRunner(Alert{
		Title:       "orders stale",
		PipelineID:  "orders",
		PipelineMap: `postgres: public.orders -> connect: pg-orders-connector -> topic: pg.public.orders -> consumer: order-sync -> warehouse: analytics.orders`,
	}, Deps{
		Tools:  reg,
		Inbox:  inbox,
		Sink:   sink,
		Logger: discardLogger(),
	}, Options{MaxTurns: 12})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	if err := r.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, e := range sink.ofType(events.AgentHypothesis) {
		var p events.AgentHypothesisPayload
		if err := json.Unmarshal(e.payload, &p); err != nil {
			continue
		}
		lower := strings.ToLower(p.Text)
		if strings.Contains(lower, "amount") &&
			(strings.Contains(lower, "decimal") || strings.Contains(lower, "bytes") || strings.Contains(lower, "type")) {
			t.Logf("hypothesis: %q (confidence %s)", p.Text, p.Confidence)
			return
		}
	}
	t.Fatalf("no hypothesis named the amount type change; hypotheses: %d", len(sink.ofType(events.AgentHypothesis)))
}
