package openai_test

// The tool names and count of the production registry against the chat
// completions limits (design 7.1). An external test package: wiring imports
// the agent, which V6 makes import this adapter.

import (
	"regexp"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
)

// toolName is the documented rule of OpenAI and most compatible hosts.
var toolName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func TestRegistryFitsChatCompletionsLimits(t *testing.T) {
	m, err := openai.New(openai.Options{Flavor: openai.FlavorOllama, Retry: &retry.Policy{}})
	if err != nil {
		t.Fatal(err)
	}
	caps := m.Capabilities("any")
	for _, set := range []struct {
		kind string
		p    owners.Pipeline
	}{
		{"cdc", owners.Pipeline{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, Connectors: []string{"orders-c"}}},
		{"batch", owners.Pipeline{ID: "rev", Kind: events.PipelineKindBatch, DAGs: []string{"revenue_daily"}, Models: []string{"fct_revenue"}}},
	} {
		t.Run(set.kind, func(t *testing.T) {
			reg, areg := tools.NewReadOnlyRegistry(), actions.NewRegistry()
			if err := wiring.RegisterTools(reg, areg, &wiring.Wired{Owners: &owners.Owners{Pipelines: []owners.Pipeline{set.p}}}); err != nil {
				t.Fatalf("RegisterTools: %v", err)
			}
			var names []string
			for _, tl := range reg.All() {
				names = append(names, tl.Name())
			}
			names = append(names, agent.RoomToolNames()...)
			if len(names) < 10 {
				t.Fatalf("only %d tools registered; the test no longer sees the production registry", len(names))
			}
			if caps.MaxTools > 0 && len(names) > caps.MaxTools {
				t.Errorf("%d tools in one request, the API takes %d", len(names), caps.MaxTools)
			}
			seen := map[string]bool{}
			for _, n := range names {
				if !toolName.MatchString(n) || len(n) > caps.ToolNameMax {
					t.Errorf("tool name %q breaks ^[a-zA-Z0-9_-]{1,%d}$", n, caps.ToolNameMax)
				}
				if seen[n] {
					t.Errorf("tool name %q twice", n)
				}
				seen[n] = true
			}
			t.Logf("%s: %d tools", set.kind, len(names))
		})
	}
}
