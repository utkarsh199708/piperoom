package owners_test

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// TestProvisionalSatisfiesCheckScope pins "Provisional yields a pipeline every
// Q CheckScope accepts for the request's names": the room scopes its tools and
// actions to the provisional pipeline, and each name in the request passes.
func TestProvisionalSatisfiesCheckScope(t *testing.T) {
	p, err := owners.Provisional(events.SetupRequest{
		PipelineID:  "orders",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders", "public.items"},
		Connectors:  []string{"orders-connector"},
	})
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	ctx := tools.WithPipeline(context.Background(), p)
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		t.Fatalf("ScopeFor: %v", err)
	}
	if sc.Kind() != events.PipelineKindCDC {
		t.Fatalf("Kind = %q, want cdc", sc.Kind())
	}
	for _, table := range []string{"public.orders", "public.items"} {
		if err := sc.CheckSourceTable(table); err != nil {
			t.Errorf("CheckSourceTable(%q): %v", table, err)
		}
	}
	if err := sc.CheckTopic("pg.public.orders"); err != nil {
		t.Errorf("CheckTopic: %v", err)
	}
	if err := sc.CheckConnector("orders-connector"); err != nil {
		t.Errorf("CheckConnector: %v", err)
	}
	// A name outside the request is refused, not silently accepted.
	if err := sc.CheckSourceTable("public.other"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("CheckSourceTable(other) = %v, want ErrOutOfScope", err)
	}
}
