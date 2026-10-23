package room_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// X7b: OnActionResult gets the step and its result (ok, proposal, index)
// with the untrusted output cleared, alone (no OnActionExecuted set).
func TestOnActionResultGetsTheResult(t *testing.T) {
	for _, fail := range []bool{false, true} {
		act := &fakeAction{name: "connector_config_set", layer: "postgres", fail: fail}
		c := &clock{t: time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)}
		st := memstore.NewWithClock(c.Now)
		reg := actions.NewRegistry()
		if err := reg.Register(act); err != nil {
			t.Fatal(err)
		}
		type call struct {
			pipeline string
			step     events.ProposalStep
			res      events.ActionExecutedPayload
		}
		var mu sync.Mutex
		var calls []call
		svc := room.New(room.Deps{
			Store: st, Actions: reg, Users: room.DefaultUsers(), Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now,
			Owners: &owners.Owners{Pipelines: []owners.Pipeline{
				{ID: "orders", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders"}},
			}},
			OnActionResult: func(_ context.Context, pipelineID string, step events.ProposalStep, res events.ActionExecutedPayload) {
				mu.Lock()
				calls = append(calls, call{pipelineID, step, res})
				mu.Unlock()
			},
		})
		roomID := createRoom(t, svc, "arjun").ID
		step := events.ProposalStep{Action: "connector_config_set", Layer: "postgres", Params: json.RawMessage(`{"connector":"pg-orders"}`)}
		pid := propose(t, svc, roomID, step)
		if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(calls) == 1 })
		mu.Lock()
		got := calls[0]
		mu.Unlock()
		if got.pipeline != "orders" || got.step.Action != step.Action || got.res.ProposalID != pid || got.res.StepIndex != 0 ||
			got.res.OK == fail || got.res.Output != "" {
			t.Fatalf("fail=%v: call = %+v", fail, got)
		}
		svc.Close()
	}
}
