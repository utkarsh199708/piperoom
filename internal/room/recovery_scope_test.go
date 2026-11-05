package room

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// X2c: an action result reaches the agent with the action.executed time, the
// time a recovery step's Verify counts from (agent.HumanInput.At).
func TestActionResultCarriesEventTime(t *testing.T) {
	at := time.Date(2026, 10, 22, 9, 30, 0, 0, time.UTC)
	raw, _ := json.Marshal(events.ActionExecutedPayload{ProposalID: "p", StepIndex: 1, OK: true})
	in := inputFor(events.Event{Seq: 9, At: at, Type: events.ActionExecuted, Actor: executorActor, Payload: raw})
	if in == nil || in.Kind != "action_result" || !in.At.Equal(at) || in.Seq != 9 {
		t.Fatalf("input %+v, want an action_result at %v", in, at)
	}
}

// X2c: the agent's run ctx carries the room's whole pipeline from owners.yaml,
// so a recovery action's Verify reads in the room's scope (actions refuse an
// id-only scope); a pipeline owners.yaml does not know gets its id only.
func TestAgentRunCtxCarriesRoomPipeline(t *testing.T) {
	own := loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline)
	var mu sync.Mutex
	got := map[string]owners.Pipeline{}
	full := map[string]bool{}
	seen := make(chan struct{}, 4)
	svc := kindService(t, own, "", func(ctx context.Context, a agent.Alert, _ agent.Deps, _ agent.Options) error {
		p, ok := tools.PipelineFrom(ctx)
		mu.Lock()
		got[a.PipelineID], full[a.PipelineID] = p, ok
		mu.Unlock()
		seen <- struct{}{}
		<-ctx.Done()
		return nil
	})
	for _, pid := range []string{"orders", "ghost"} {
		raw := json.RawMessage(`{"title":"t","pipeline_id":"` + pid + `"}`)
		if _, _, err := svc.CreateRoomFromAlert(context.Background(), "t", "sev2", pid, raw); err != nil {
			t.Fatalf("CreateRoomFromAlert(%s): %v", pid, err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-seen:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 2 agent runs started", i)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if p := got["orders"]; !full["orders"] || p.ID != "orders" || !slices.Equal(p.SourceTables, []string{"public.orders"}) {
		t.Fatalf("orders run ctx pipeline = %+v (full %v), want the owners.yaml pipeline", p, full["orders"])
	}
	if full["ghost"] {
		t.Fatalf("an unknown pipeline's run carries a full pipeline %+v", got["ghost"])
	}
}
