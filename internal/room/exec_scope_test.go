package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
)

// scopeSeen is what one Validate or Execute call found in its ctx.
type scopeSeen struct {
	call     string // "validate" or "execute"
	pipeline owners.Pipeline
	whole    bool // tools.PipelineFrom: the whole pipeline the write clients need
	scope    tools.Scope
	scopeErr error // tools.ScopeFor(ctx, nil), the write clients' own resolution
	deadline bool
}

// scopeAction records the pipeline scope of every Validate and Execute ctx.
// Its Execute fails closed like the Postgres and Kafka write clients
// (internal/clients/postgres/writer.go, internal/clients/kafka/writer.go): a
// write needs the room's whole pipeline in ctx, or the explicit opt-out.
type scopeAction struct {
	fakeAction
	smu  sync.Mutex
	seen []scopeSeen
}

func (a *scopeAction) record(ctx context.Context, call string) {
	p, whole := tools.PipelineFrom(ctx)
	sc, err := tools.ScopeFor(ctx, nil)
	_, deadline := ctx.Deadline()
	a.smu.Lock()
	a.seen = append(a.seen, scopeSeen{call: call, pipeline: p, whole: whole, scope: sc, scopeErr: err, deadline: deadline})
	a.smu.Unlock()
}

func (a *scopeAction) Validate(ctx context.Context, _ json.RawMessage) error {
	a.record(ctx, "validate")
	return nil
}

func (a *scopeAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	a.record(ctx, "execute")
	if err := writeScope(ctx); err != nil {
		return "", err
	}
	return a.fakeAction.Execute(ctx, params)
}

// count is the number of calls recorded so far.
func (a *scopeAction) count() int {
	a.smu.Lock()
	defer a.smu.Unlock()
	return len(a.seen)
}

// since returns the calls recorded after the first n.
func (a *scopeAction) since(n int) []scopeSeen {
	a.smu.Lock()
	defer a.smu.Unlock()
	return append([]scopeSeen(nil), a.seen[n:]...)
}

// writeScope is the write clients' fail-closed rule: the whole pipeline in
// ctx, or the explicit tools.WithUnscoped opt-out; anything else is
// tools.ErrNoScope.
func writeScope(ctx context.Context) error {
	if _, ok := tools.PipelineFrom(ctx); ok {
		return nil
	}
	if sc, err := tools.ScopeFor(ctx, nil); err == nil && sc.Unscoped() {
		return nil
	}
	return fmt.Errorf("fake writer: %w", tools.ErrNoScope)
}

func approveAndWaitExecuted(t *testing.T, svc *room.Service, st *memstore.Store, pid string) {
	t.Helper()
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && (p.Status == "executed" || p.Status == "failed")
	})
	if p, _ := st.GetProposal(context.Background(), pid); p.Status != "executed" {
		t.Fatalf("proposal status %q, want executed", p.Status)
	}
}

// checkRoomScope fails unless every call saw the room's whole pipeline p,
// resolved without an owners config (as the write clients resolve it) and
// never unscoped.
func checkRoomScope(t *testing.T, seen []scopeSeen, p owners.Pipeline) {
	t.Helper()
	for i, s := range seen {
		if !s.whole || s.pipeline.ID != p.ID || !slices.Equal(s.pipeline.SourceTables, p.SourceTables) {
			t.Fatalf("call %d (%s): ctx pipeline %+v (whole=%v), want the room's pipeline %+v", i, s.call, s.pipeline, s.whole, p)
		}
		if s.scopeErr != nil || s.scope.Unscoped() {
			t.Fatalf("call %d (%s): ScopeFor = unscoped %v, err %v; want the room's pipeline", i, s.call, s.scope.Unscoped(), s.scopeErr)
		}
		if err := s.scope.CheckSourceTable(p.SourceTables[0]); err != nil {
			t.Fatalf("call %d (%s): the room's own table is refused: %v", i, s.call, err)
		}
		if err := s.scope.CheckSourceTable("public.not_this_pipeline"); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("call %d (%s): another table = %v, want ErrOutOfScope", i, s.call, err)
		}
	}
}

// Every Validate and Execute the executor runs carries the room's whole
// pipeline (board row P3-0): the Postgres and Kafka write clients refuse a
// write without it. Execute keeps its step deadline.
func TestExecutorValidateAndExecuteCarryTheRoomPipeline(t *testing.T) {
	act := &scopeAction{fakeAction: fakeAction{name: "scoped_write", layer: "postgres"}}
	svc, st, _ := newTestService(t, act)
	roomID := createRoom(t, svc, "arjun").ID
	step := events.ProposalStep{Action: "scoped_write", Layer: "postgres", Params: json.RawMessage(`{}`)}
	pid := propose(t, svc, roomID, step, step)
	created := act.count() // CreateProposal's own Validate calls

	approveAndWaitExecuted(t, svc, st, pid)

	got := act.since(created)
	var calls []string
	for _, s := range got {
		calls = append(calls, s.call)
	}
	if want := []string{"validate", "execute", "validate", "execute"}; !slices.Equal(calls, want) {
		t.Fatalf("executor calls %v, want %v (a re-validation right before each Execute)", calls, want)
	}
	checkRoomScope(t, got, testOwners().Pipelines[0])
	for i, s := range got {
		if s.call == "execute" && !s.deadline {
			t.Fatalf("call %d: Execute ctx has no deadline, want the step timeout", i)
		}
	}
	n := 0
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !p.OK {
			t.Fatalf("step %d failed: %q", p.StepIndex, p.Output)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("%d action.executed events, want 2", n)
	}
}

// The scope is the room's own pipeline, not one the service picked once:
// rooms on two pipelines each run under theirs.
func TestExecutorScopeIsEachRoomsOwnPipeline(t *testing.T) {
	act := &scopeAction{fakeAction: fakeAction{name: "scoped_write", layer: "postgres"}}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	c := &clock{t: time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	own := &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", SourceTables: []string{"public.orders"}},
		{ID: "payments", SourceTables: []string{"public.payments"}},
	}}
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Owners: own,
		Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now,
	})
	t.Cleanup(svc.Close)

	for _, p := range own.Pipelines {
		snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "Incident", "sev2", p.ID)
		if err != nil {
			t.Fatalf("create room on %s: %v", p.ID, err)
		}
		pid := propose(t, svc, snap.ID, events.ProposalStep{Action: "scoped_write", Layer: "postgres", Params: json.RawMessage(`{}`)})
		before := act.count()
		approveAndWaitExecuted(t, svc, st, pid)
		got := act.since(before)
		if len(got) != 2 {
			t.Fatalf("pipeline %s: %d executor calls, want Validate and Execute", p.ID, len(got))
		}
		checkRoomScope(t, got, p)
	}
}
