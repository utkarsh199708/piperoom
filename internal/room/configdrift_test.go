package room_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

type execCall struct {
	pipeline string
	step     events.ProposalStep
}

// driftSvc is newTestService with the OnActionExecuted hook recorded and a
// pipeline that lists its connector.
func driftSvc(t *testing.T, act ...actions.Action) (*room.Service, *memstore.Store, func() []execCall) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	reg := actions.NewRegistry()
	for _, a := range act {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var calls []execCall
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{
			{ID: "orders", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders"}},
		}},
		OnActionExecuted: func(_ context.Context, pipelineID string, step events.ProposalStep) {
			mu.Lock()
			calls = append(calls, execCall{pipelineID, step})
			mu.Unlock()
		},
	})
	t.Cleanup(svc.Close)
	return svc, st, func() []execCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]execCall(nil), calls...)
	}
}

// X3b TC4 (room side): every action.executed reaches the hook with the
// room's pipeline and the step that ran, params included.
func TestOnActionExecutedGetsTheStep(t *testing.T) {
	act := &fakeAction{name: "connector_config_set", layer: "postgres"}
	svc, st, calls := driftSvc(t, act)
	roomID := createRoom(t, svc, "arjun").ID
	s1 := events.ProposalStep{Action: "connector_config_set", Layer: "postgres", Params: json.RawMessage(`{"connector":"pg-orders","key":"tasks.max"}`)}
	s2 := events.ProposalStep{Action: "connector_config_set", Layer: "postgres", Params: json.RawMessage(`{"connector":"pg-other"}`)}
	pid := propose(t, svc, roomID, s1, s2)
	approveAndWaitExecuted(t, svc, st, pid)
	waitFor(t, 5*time.Second, func() bool { return len(calls()) == 2 })
	got := calls()
	// Each call runs on its own goroutine: the order is not fixed.
	slices.SortFunc(got, func(a, b execCall) int { return strings.Compare(string(a.step.Params), string(b.step.Params)) })
	for i, want := range []events.ProposalStep{s1, s2} {
		if got[i].pipeline != "orders" || got[i].step.Action != want.Action || string(got[i].step.Params) != string(want.Params) {
			t.Fatalf("call %d = %+v (%s), want orders and %s", i, got[i], got[i].step.Params, want.Params)
		}
	}
}

// A failed step is an action.executed too: the hook still runs (the step
// may have written before it failed).
func TestOnActionExecutedFailedStep(t *testing.T) {
	act := &fakeAction{name: "connector_config_set", layer: "postgres", fail: true}
	svc, st, calls := driftSvc(t, act)
	roomID := createRoom(t, svc, "arjun").ID
	pid := propose(t, svc, roomID, events.ProposalStep{Action: "connector_config_set", Layer: "postgres", Params: json.RawMessage(`{"connector":"pg-orders"}`)})
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "failed"
	})
	waitFor(t, 5*time.Second, func() bool { return len(calls()) == 1 })
}

// X3b TC5 (room side): accept-config is 404 for an unknown pipeline or a
// connector that is not the pipeline's, before the role check; then only a
// kafka-connect layer owner passes.
func TestAuthorizeAcceptConfig(t *testing.T) {
	svc, _, _ := driftSvc(t)
	arjun, priya := mustUser(t, svc, "arjun"), mustUser(t, svc, "priya")
	if err := svc.AuthorizeAcceptConfig(arjun, "orders", "pg-orders"); err != nil {
		t.Fatalf("kafka-connect owner: %v", err)
	}
	wantCode(t, svc.AuthorizeAcceptConfig(priya, "orders", "pg-orders"), "not_layer_owner")
	wantCode(t, svc.AuthorizeAcceptConfig(priya, "nope", "pg-orders"), "not_found")
	wantCode(t, svc.AuthorizeAcceptConfig(priya, "orders", "pg-other"), "not_found")
	wantCode(t, svc.AuthorizeAcceptConfig(arjun, "orders", ""), "not_found")
}
