package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// classedFake is a fakeAction of another class, with the Verifier and
// Preview the registry requires of it.
type classedFake struct {
	fakeAction
	class string
}

func (a *classedFake) Class() string { return a.class }
func (a *classedFake) Verify(context.Context, json.RawMessage, time.Time) (actions.VerifyResult, error) {
	return actions.VerifyResult{}, nil
}
func (a *classedFake) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	return actions.PreviewResult{}, nil
}

// X6c (room side): proposal.created.class comes from the registry, never
// from the proposer; a proposal mixing recovery and performance steps is a
// 400 validation and nothing is stored; fix steps join either class.
func TestProposalClassFromRegistry(t *testing.T) {
	fix := &fakeAction{name: "connect_restart_task", layer: "kafka-connect"}
	rec := &classedFake{fakeAction{name: "kafka_replay_range", layer: "kafka"}, actions.ClassRecovery}
	perf := &classedFake{fakeAction{name: "connect_tune_throughput", layer: "kafka-connect"}, actions.ClassPerformance}
	svc, st, _ := driftSvc(t, fix, rec, perf)
	roomID := createRoom(t, svc, "arjun").ID
	step := func(action, layer string) events.ProposalStep {
		return events.ProposalStep{Action: action, Layer: layer, Params: json.RawMessage(`{"connector":"pg-orders"}`), Description: "d"}
	}
	fixStep := step("connect_restart_task", "kafka-connect")
	recStep := step("kafka_replay_range", "kafka")
	perfStep := step("connect_tune_throughput", "kafka-connect")

	cases := []struct {
		name  string
		steps []events.ProposalStep
		class string
	}{
		{"fix only", []events.ProposalStep{fixStep}, ""},
		{"fix and performance", []events.ProposalStep{fixStep, perfStep}, events.ActionClassPerformance},
		{"recovery and fix", []events.ProposalStep{recStep, fixStep}, events.ActionClassRecovery},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A class the proposer claims is overwritten.
			pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
				Title: "fix", Steps: tc.steps, Risk: "low", Rollback: "undo", Class: "recovery-claimed",
			}, events.AgentActor)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if got := createdClass(t, st, roomID, pid); got != tc.class {
				t.Fatalf("class = %q, want %q", got, tc.class)
			}
		})
	}

	before := countProposals(t, st, roomID)
	_, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "replay and tune", Steps: []events.ProposalStep{recStep, fixStep, perfStep}, Risk: "low", Rollback: "undo",
	}, events.AgentActor)
	var re *room.Error
	if !errors.As(err, &re) || re.Status != 400 || re.Code != "validation" || !strings.Contains(err.Error(), "cannot mix recovery and performance") {
		t.Fatalf("err = %v, want 400 validation", err)
	}
	if n := countProposals(t, st, roomID); n != before {
		t.Fatalf("a refused proposal was stored: %d proposals, want %d", n, before)
	}
}

func createdClass(t *testing.T, st interface {
	Since(context.Context, string, int64, int) ([]events.Event, error)
}, roomID, pid string) string {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		if e.Type != events.ProposalCreated {
			continue
		}
		p, err := events.Decode[events.ProposalCreatedPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		if p.ProposalID == pid {
			return p.Class
		}
	}
	t.Fatalf("no proposal.created for %s", pid)
	return ""
}

func countProposals(t *testing.T, st interface {
	Since(context.Context, string, int64, int) ([]events.Event, error)
}, roomID string) int {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs {
		if e.Type == events.ProposalCreated {
			n++
		}
	}
	return n
}
