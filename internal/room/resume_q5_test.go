package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// recapService is a room service over st whose agent runs hand their deps to
// the test and then wait for the room to end.
func recapService(t *testing.T, st *memstore.Store, c *clock, act actions.Action) (*room.Service, chan agent.Deps) {
	t.Helper()
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	deps := make(chan agent.Deps, 8)
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: lifecycleUsers(t), Owners: testOwners(), Now: c.Now, WriteHandoff: fakeWrite,
		Run: func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
			select {
			case deps <- d:
			case <-ctx.Done():
			}
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	return svc, deps
}

func nextDeps(t *testing.T, ch chan agent.Deps) agent.Deps {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no agent run started")
		return agent.Deps{}
	}
}

// drainUntil drains the run's inbox until pred holds for the inputs so far.
func drainUntil(t *testing.T, d agent.Deps, pred func([]agent.HumanInput) bool) []agent.HumanInput {
	t.Helper()
	var got []agent.HumanInput
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		in, err := d.Inbox.Drain(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, in...)
		if pred(got) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("inbox never matched; got %+v", got)
	return nil
}

// TestExecutedAfterRestartWakesAgentWithRecap (Q5 T1): the run that made a
// proposal is gone (server restart); the proposal is approved and executes.
// The room starts a run for the result, and that run's first input is the
// server's recap of the room: the hypothesis, the step summaries, the
// proposal with its steps and status, and the hand-off. The action_result
// follows as today.
func TestExecutedAfterRestartWakesAgentWithRecap(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	act := &fakeAction{name: "connect_restart", layer: "kafka-connect"}

	svc1, deps1 := recapService(t, st, c, act)
	snap, err := svc1.CreateRoom(ctx, mustUser(t, svc1, "arjun"), "orders sink down", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	d := nextDeps(t, deps1)
	emit := func(typ events.Type, p any) {
		t.Helper()
		if err := d.Sink.Emit(ctx, typ, p); err != nil {
			t.Fatalf("emit %s: %v", typ, err)
		}
	}
	emit(events.AgentStepStart, events.AgentStepStartPayload{StepID: "s_1"})
	emit(events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", ToolUseID: "tu_1", Tool: "connect_status", Summary: "task 0 FAILED: schema mismatch"})
	emit(events.AgentHypothesis, events.AgentHypothesisPayload{Text: "task 0 failed on the new schema", BasedOnSteps: []string{"s_1"}, Confidence: "high"})
	pid, err := d.Proposals.Create(ctx, events.ProposalCreatedPayload{
		Title: "restart the connector", Risk: "the connector restarts", Rollback: "set the connector back",
		Steps:          []events.ProposalStep{{Action: "connect_restart", Layer: "kafka-connect", Params: json.RawMessage(`{"name":"orders-sink"}`), Verify: "task 0 RUNNING"}},
		RequiredLayers: []string{"kafka-connect"},
	})
	if err != nil {
		t.Fatal(err)
	}
	svc1.Close() // the restart: run 1 is gone

	svc2, deps2 := recapService(t, st, c, act)
	if err := svc2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	hash := proposalHash(t, svc2, pid)
	if _, err := svc2.ApproveProposal(ctx, mustUser(t, svc2, "arjun"), pid, "kafka-connect", hash); err != nil {
		t.Fatal(err)
	}
	d2 := nextDeps(t, deps2)
	got := drainUntil(t, d2, func(in []agent.HumanInput) bool {
		for _, x := range in {
			if x.Kind == "action_result" {
				return true
			}
		}
		return false
	})
	first := got[0]
	if first.Kind != "comment" || first.Actor.Kind != events.ActorSystem || first.Actor.ID != "system" {
		t.Fatalf("first input = %+v, want the system recap", first)
	}
	var rc room.Recap
	if err := json.Unmarshal([]byte(first.Text), &rc); err != nil {
		t.Fatalf("recap is not JSON: %v\n%s", err, first.Text)
	}
	if rc.Kind != room.RecapKind || rc.Version != 1 || rc.RoomID != snap.ID {
		t.Fatalf("recap header = %+v", rc)
	}
	if rc.Hypothesis == nil || rc.Hypothesis.Text != "task 0 failed on the new schema" || rc.Hypothesis.Confidence != "high" {
		t.Fatalf("hypothesis = %+v", rc.Hypothesis)
	}
	if len(rc.Steps) != 1 || rc.Steps[0].StepID != "s_1" || rc.Steps[0].Tool != "connect_status" || !strings.Contains(rc.Steps[0].Summary, "FAILED") {
		t.Fatalf("steps = %+v", rc.Steps)
	}
	if len(rc.Proposals) != 1 {
		t.Fatalf("proposals = %+v", rc.Proposals)
	}
	p := rc.Proposals[0]
	if p.ProposalID != pid || !p.ByAgent || len(p.Steps) != 1 || p.Steps[0].Action != "connect_restart" || p.Steps[0].Verify != "task 0 RUNNING" {
		t.Fatalf("proposal = %+v", p)
	}
	// The recap is built when the run starts; the step may or may not have
	// been recorded by then, but never a verification.
	if p.Verified {
		t.Fatalf("proposal = %+v, want not verified", p)
	}
	var res agent.HumanInput
	for _, x := range got {
		if x.Kind == "action_result" {
			res = x
		}
	}
	if res.ProposalID != pid || res.StepIndex == nil || *res.StepIndex != 0 || res.OK == nil || !*res.OK {
		t.Fatalf("action_result = %+v", res)
	}
}

// TestFirstRunHasNoRecap (Q5 T1): a room's first run starts from the alert;
// it gets no recap.
func TestFirstRunHasNoRecap(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc, deps := recapService(t, st, c, &fakeAction{name: "connect_restart", layer: "kafka-connect"})
	if _, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "orders", "sev2", "orders"); err != nil {
		t.Fatal(err)
	}
	d := nextDeps(t, deps)
	in, err := d.Inbox.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range in {
		if strings.Contains(x.Text, room.RecapKind) {
			t.Fatalf("first run got a recap: %+v", x)
		}
	}
}

// TestResumedRunGetsRecap (Q5 T1): after a restart, a steer resumes the room
// and the new run reads the recap before the steer.
func TestResumedRunGetsRecap(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	act := &fakeAction{name: "connect_restart", layer: "kafka-connect"}
	svc1, deps1 := recapService(t, st, c, act)
	snap, err := svc1.CreateRoom(ctx, mustUser(t, svc1, "arjun"), "orders", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	d := nextDeps(t, deps1)
	if err := d.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{Text: "slot inactive", Confidence: "medium"}); err != nil {
		t.Fatal(err)
	}
	svc1.Close()

	svc2, deps2 := recapService(t, st, c, act)
	if err := svc2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := svc2.Steer(ctx, mustUser(t, svc2, "arjun"), snap.ID, "check the slot again"); err != nil {
		t.Fatal(err)
	}
	got := drainUntil(t, nextDeps(t, deps2), func(in []agent.HumanInput) bool { return len(in) >= 2 })
	var rc room.Recap
	if err := json.Unmarshal([]byte(got[0].Text), &rc); err != nil || rc.Hypothesis == nil || rc.Hypothesis.Text != "slot inactive" {
		t.Fatalf("first input = %+v (err %v), want the recap", got[0], err)
	}
	if got[1].Kind != "steer" {
		t.Fatalf("second input = %+v, want the steer", got[1])
	}
}
