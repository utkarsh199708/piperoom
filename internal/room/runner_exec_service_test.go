package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// TestRunnerResumeRecordsStepOnce: a done result the runner reports after a
// dropped request finishes the step and emits one action.executed with its
// receipt; a second Resume for the same op changes nothing.
func TestRunnerResumeRecordsStepOnce(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	arjun, ok := svc.LookupUser("arjun")
	if !ok {
		t.Fatal("arjun missing")
	}
	snap, err := svc.CreateRoom(ctx, arjun, "Setup", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}

	pid := "p_setup"
	steps := json.RawMessage(`[{"action":"compose_up","layer":"environment","params":{}}]`)
	if err := st.CreateProposal(ctx, store.ProposalRow{
		ProposalID: pid, RoomID: snap.ID, Title: "bring up the stack",
		Status: "open", Steps: steps, ParamsHash: "h",
	}); err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if err := st.StartProposalStep(ctx, pid, 0); err != nil {
		t.Fatalf("start step: %v", err)
	}

	creator := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	if err := svc.AttachRunner(ctx, "CODE1234", room.Runner{
		Tenant: "", RoomID: snap.ID, Creator: creator, Label: "laptop",
		Token: "tok-a", TokenExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	w := &captureWriter{}
	if !svc.BindRunnerWriter("", snap.ID, "arjun", "tok-a", w) {
		t.Fatal("bind writer refused")
	}

	ectx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	req := stepReq("op-1", pid, 0)
	go func() {
		_, err := svc.RunnerExecutor().Execute(ectx, snap.ID, req)
		done <- err
	}()
	waitOp(t, w, 1)
	cancel()
	if err := <-done; err == nil {
		t.Fatal("Execute returned a result after cancellation")
	}

	res := protocol.Resume{InFlight: []protocol.OpStatus{{
		ID: "op-1", ProposalID: pid, StepIndex: req.StepIndex, State: protocol.StateDone,
		Result: &protocol.OpResult{
			ID: "op-1", OK: true, Summary: "stack up",
			Receipt: events.SetupReceipt{Kind: "exec", Target: "host", OutMAC: "mac", At: time.Now()},
		},
	}}}
	svc.RunnerResume(ctx, "", snap.ID, "arjun", "tok-a", res)
	svc.RunnerResume(ctx, "", snap.ID, "arjun", "tok-a", res)

	// The step is terminal: a second finish is an order violation.
	if err := st.FinishProposalStep(ctx, pid, 0, "done", "again"); !errors.Is(err, store.ErrStepOrder) {
		t.Fatalf("step finished twice or never: %v", err)
	}

	evs, _, err := svc.Events(ctx, snap.ID, 0, 200)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var executed []events.ActionExecutedPayload
	for _, e := range evs {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err == nil && p.ProposalID == pid && p.StepIndex == 0 {
			executed = append(executed, p)
		}
	}
	if len(executed) != 1 {
		t.Fatalf("action.executed for the step = %d, want exactly 1", len(executed))
	}
	got := executed[0]
	if !got.OK || got.Output != "stack up" || got.Receipt == nil || got.Receipt.OutMAC != "mac" {
		t.Fatalf("recorded outcome = %+v", got)
	}
	if got.OutcomeUnknown {
		t.Fatal("a done result was recorded as unknown")
	}
}
