package demo

import (
	"context"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// scriptedInbox hands out one batch per Wait, then closes idle (every batch
// has been handled) and blocks until ctx ends. Only the runner goroutine
// touches batches and next.
type scriptedInbox struct {
	batches [][]agent.HumanInput
	next    []agent.HumanInput
	idle    chan struct{}
}

func (in *scriptedInbox) Wait(ctx context.Context) error {
	if len(in.batches) == 0 {
		close(in.idle)
		<-ctx.Done()
		return ctx.Err()
	}
	in.next, in.batches = in.batches[0], in.batches[1:]
	return nil
}

func (in *scriptedInbox) Drain(context.Context) ([]agent.HumanInput, error) {
	out := in.next
	in.next = nil
	return out, nil
}

func approval(pid, layer string) agent.HumanInput {
	ok := true
	return agent.HumanInput{Kind: "approval_result", ProposalID: pid, Layer: layer, Approved: &ok, Text: "approved"}
}

func stepDone(pid string, i int) agent.HumanInput {
	ok := true
	return agent.HumanInput{Kind: "action_result", ProposalID: pid, StepIndex: &i, OK: &ok, Text: "done"}
}

func statesAfterBatches(t *testing.T, batches ...[]agent.HumanInput) []string {
	t.Helper()
	sink := &recordingSink{}
	ctx, cancel := context.WithCancel(context.Background())
	inbox := &scriptedInbox{batches: batches, idle: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fix := newFixState("p_1", 2, "schema-registry", "kafka-connect")
		_ = waitForInput(ctx, agent.Deps{Inbox: inbox}, func(typ events.Type, p any) error {
			return sink.Emit(ctx, typ, p)
		}, fix)
	}()
	<-inbox.idle
	cancel()
	<-done
	var states []string
	for i, typ := range sink.types {
		if typ == events.AgentStatus {
			states = append(states, sink.pays[i].(events.AgentStatusPayload).State)
		}
	}
	return states
}

// QA sweep 2026-10-14 (QA9 #13): the last approval and the step results of the
// execution it started arrive in one batch. The status says the fix ran; it
// is never "waiting_approval" again.
func TestDemoNoWaitingApprovalAfterExecution(t *testing.T) {
	got := statesAfterBatches(t,
		[]agent.HumanInput{approval("p_1", "schema-registry")},
		[]agent.HumanInput{approval("p_1", "kafka-connect"), stepDone("p_1", 0), stepDone("p_1", 1)},
	)
	if len(got) != 1 || got[0] != "idle" {
		t.Fatalf("statuses = %v, want [idle]", got)
	}
}

func TestDemoStatusFollowsTheFix(t *testing.T) {
	got := statesAfterBatches(t,
		[]agent.HumanInput{approval("p_1", "schema-registry")},
		[]agent.HumanInput{approval("p_1", "kafka-connect")},
		[]agent.HumanInput{stepDone("p_1", 0)},
		[]agent.HumanInput{stepDone("p_1", 1)},
	)
	want := []string{"working", "idle"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
}
