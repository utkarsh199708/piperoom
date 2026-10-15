package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

type fakeAction struct {
	name    string
	scoped  bool
	execErr error
	ran     *int
}

func (f fakeAction) ranPtr() *int                   { return f.ran }
func (f fakeAction) Name() string                   { return f.name }
func (f fakeAction) Layer() string                  { return "connect" }
func (f fakeAction) Describe() string               { return "fake" }
func (f fakeAction) ParamsSchema() map[string]any   { return map[string]any{"type": "object"} }
func (f fakeAction) Validate(json.RawMessage) error { return nil }
func (f fakeAction) Execute(context.Context, json.RawMessage) (string, error) {
	*f.ran++
	return "done", f.execErr
}

type scopedAction struct{ fakeAction }

func (scopedAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }

func approveFixture(t *testing.T, acts ...actions.Action) (*approveProposals, *stdinInbox) {
	t.Helper()
	reg := actions.NewRegistry()
	for _, a := range acts {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	in := &stdinInbox{notify: make(chan struct{}, 1), eof: true}
	return &approveProposals{acts: reg, pipe: owners.Pipeline{ID: "orders"}, havePipe: true, inbox: in, out: &bytes.Buffer{}, ctx: context.Background()}, in
}

func waitInputs(t *testing.T, in *stdinInbox, want int) []agent.HumanInput {
	t.Helper()
	var got []agent.HumanInput
	deadline := time.After(5 * time.Second)
	for len(got) < want {
		d, _ := in.Drain(context.Background())
		got = append(got, d...)
		if len(got) >= want {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("got %d inputs, want %d: %+v", len(got), want, got)
		case <-time.After(5 * time.Millisecond):
		}
	}
	return got
}

func TestApproveProposalsApprovesAndExecutes(t *testing.T) {
	ran := 0
	p, in := approveFixture(t, scopedAction{fakeAction{name: "do_a", ran: &ran}}, scopedAction{fakeAction{name: "do_b", ran: &ran}})
	id, err := p.Create(context.Background(), events.ProposalCreatedPayload{
		RequiredLayers: []string{"connect", "schema"},
		Steps:          []events.ProposalStep{{Action: "do_a"}, {Action: "do_b"}},
	})
	if err != nil || id != "p_1" {
		t.Fatalf("Create = %q, %v", id, err)
	}
	got := waitInputs(t, in, 4)
	if got[0].Kind != "approval_result" || got[1].Kind != "approval_result" || got[0].Approved == nil || !*got[0].Approved {
		t.Errorf("approvals = %+v", got[:2])
	}
	for i, h := range got[2:] {
		if h.Kind != "action_result" || h.OK == nil || !*h.OK || h.StepIndex == nil || *h.StepIndex != i || h.ProposalID != id {
			t.Errorf("action result %d = %+v", i, h)
		}
	}
	if ran != 2 {
		t.Errorf("executed %d steps, want 2", ran)
	}
}

func TestApproveProposalsStopsAtFirstFailure(t *testing.T) {
	ran := 0
	p, in := approveFixture(t,
		scopedAction{fakeAction{name: "bad", ran: &ran, execErr: errors.New("boom")}},
		scopedAction{fakeAction{name: "never", ran: &ran}})
	_, _ = p.Create(context.Background(), events.ProposalCreatedPayload{
		RequiredLayers: []string{"connect"},
		Steps:          []events.ProposalStep{{Action: "bad"}, {Action: "never"}},
	})
	got := waitInputs(t, in, 2)
	if r := got[1]; r.OK == nil || *r.OK {
		t.Errorf("failed step result = %+v, want ok=false", r)
	}
	time.Sleep(50 * time.Millisecond)
	if ran != 1 {
		t.Errorf("executed %d steps, want 1 (stop at first failure)", ran)
	}
}

func TestApproveProposalsRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		act      actions.Action
		step     string
		havePipe bool
	}{
		"unknown action": {act: scopedAction{fakeAction{name: "x", ran: new(int)}}, step: "nope", havePipe: true},
		"not scoped":     {act: fakeAction{name: "x", ran: new(int)}, step: "x", havePipe: true},
		"no pipeline":    {act: scopedAction{fakeAction{name: "x", ran: new(int)}}, step: "x", havePipe: false},
	} {
		t.Run(name, func(t *testing.T) {
			p, in := approveFixture(t, tc.act)
			p.havePipe = tc.havePipe
			_, _ = p.Create(context.Background(), events.ProposalCreatedPayload{Steps: []events.ProposalStep{{Action: tc.step}}})
			got := waitInputs(t, in, 1)
			if got[0].OK == nil || *got[0].OK {
				t.Errorf("result = %+v, want ok=false", got[0])
			}
			if n := *tc.act.(interface{ ranPtr() *int }).ranPtr(); n != 0 {
				t.Errorf("action executed %d times", n)
			}
		})
	}
}

// The inbox must stay open while a proposal executes, even at stdin EOF.
func TestInboxHeldDuringExecution(t *testing.T) {
	in := &stdinInbox{notify: make(chan struct{}, 1), eof: true}
	in.hold()
	done := make(chan error, 1)
	go func() { done <- in.Wait(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("Wait returned %v while held", err)
	case <-time.After(50 * time.Millisecond):
	}
	in.push(agent.HumanInput{Kind: "action_result"})
	if err := <-done; err != nil {
		t.Fatalf("Wait = %v, want nil once an input is pending", err)
	}
	_, _ = in.Drain(context.Background())
	in.release()
	if err := in.Wait(context.Background()); !errors.Is(err, agent.ErrInboxClosed) {
		t.Fatalf("Wait after release = %v, want ErrInboxClosed", err)
	}
}
