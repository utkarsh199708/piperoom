package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/room/demo"
	"piperoom/internal/store/memstore"
)

// previewAction is a fakeAction that also implements actions.Preview.
type previewAction struct {
	fakeAction
	res actions.PreviewResult
	err error
}

func (a *previewAction) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	return a.res, a.err
}

func previewSvc(t *testing.T, acts ...actions.Action) *room.Service {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	reg := actions.NewRegistry()
	for _, a := range acts {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	svc := room.New(room.Deps{
		Store: memstore.NewWithClock(c.Now), Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now, Topology: demo.Builder{},
	})
	t.Cleanup(svc.Close)
	return svc
}

func propose(t *testing.T, svc *room.Service, roomID string, steps ...events.ProposalStep) string {
	t.Helper()
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix", Steps: steps, Risk: "low", Rollback: "undo",
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	return pid
}

func TestProposalGetsOnePreviewWithDownstream(t *testing.T) {
	act := &previewAction{
		fakeAction: fakeAction{name: "connect_restart_task", layer: "kafka-connect"},
		res:        actions.PreviewResult{Target: "pg-orders-connector", Current: "task 0 FAILED", Expected: "task 0 RUNNING", Reversible: true},
	}
	plain := &fakeAction{name: "postgres_action", layer: "postgres"}
	svc := previewSvc(t, act, plain)
	roomID := createRoom(t, svc, "arjun").ID
	pid := propose(t, svc, roomID,
		events.ProposalStep{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)},
		events.ProposalStep{Action: "postgres_action", Layer: "postgres", Params: json.RawMessage(`{}`)},
	)
	waitFor(t, 5*time.Second, func() bool {
		return len(eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed)) > 0
	})
	time.Sleep(200 * time.Millisecond)
	got := eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed)
	if len(got) != 1 || got[0].ProposalID != pid || len(got[0].Steps) != 2 {
		t.Fatalf("previewed events = %+v, want exactly one for %s with 2 steps", got, pid)
	}
	pv := got[0].Steps[0].Preview
	if pv == nil || pv.Current != "task 0 FAILED" {
		t.Fatalf("step 0 preview = %+v", got[0].Steps[0])
	}
	want := []string{"topic:pg.public.orders", "group:order-sync", "wh:analytics.orders"}
	if len(pv.Downstream) != len(want) {
		t.Fatalf("downstream = %v, want %v", pv.Downstream, want)
	}
	for i := range want {
		if pv.Downstream[i] != want[i] {
			t.Fatalf("downstream = %v, want %v", pv.Downstream, want)
		}
	}
	if e := got[0].Steps[1]; e.Preview != nil || e.Error != "no preview for this action" {
		t.Fatalf("step 1 = %+v, want the no-preview error", e)
	}
	// The system wrote it, and the snapshot carries it.
	for _, ev := range roomEvents(t, svc, roomID) {
		if ev.Type == events.ProposalPreviewed && ev.Actor.Kind != events.ActorSystem {
			t.Fatalf("actor = %+v, want system", ev.Actor)
		}
	}
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.OpenProposals) != 1 || snap.OpenProposals[0].Preview == nil {
		t.Fatalf("snapshot proposals = %+v, want the preview folded in", snap.OpenProposals)
	}
}

func TestPreviewErrorIsStoredRedactedAndApprovalStillWorks(t *testing.T) {
	act := &previewAction{
		fakeAction: fakeAction{name: "postgres_action", layer: "postgres"},
		err:        errors.New("dial postgres://u:hunter2secret@db:5432/orders failed"),
	}
	svc := previewSvc(t, act)
	roomID := createRoom(t, svc, "arjun").ID
	pid := propose(t, svc, roomID, events.ProposalStep{Action: "postgres_action", Layer: "postgres", Params: json.RawMessage(`{}`)})
	waitFor(t, 5*time.Second, func() bool {
		return len(eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed)) > 0
	})
	got := eventsOf[events.ProposalPreviewedPayload](t, svc, roomID, events.ProposalPreviewed)[0]
	if got.Steps[0].Preview != nil || got.Steps[0].Error == "" {
		t.Fatalf("step = %+v, want an error and no preview", got.Steps[0])
	}
	if contains(got.Steps[0].Error, "hunter2secret") {
		t.Fatalf("error leaks a secret: %q", got.Steps[0].Error)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve after a failed preview: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestApplyProposalPreviewed(t *testing.T) {
	sys := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
	created := ev(events.ProposalCreated, 1, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "fix", RequiredLayers: []string{"postgres"},
		Steps: []events.ProposalStep{{Action: "a", Layer: "postgres"}},
	})
	prev := func(seq int64, cur string) events.Event {
		return ev(events.ProposalPreviewed, seq, sys, events.ProposalPreviewedPayload{
			ProposalID: "p_1", Steps: []events.StepPreview{{StepIndex: 0, Preview: &events.PreviewResult{Current: cur}}},
		})
	}
	cases := []struct {
		name string
		evs  []events.Event
		want string // Current of step 0, "" for no preview
	}{
		{"after created", []events.Event{created, prev(2, "one")}, "one"},
		{"before created waits", []events.Event{prev(1, "early"), created}, "early"},
		{"second replaces first", []events.Event{created, prev(2, "one"), prev(3, "two")}, "two"},
		{"unknown proposal stays out", []events.Event{prev(1, "x")}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := room.NewSnapshot()
			for _, e := range c.evs {
				s = room.Apply(s, e)
			}
			var got string
			if len(s.OpenProposals) == 1 && s.OpenProposals[0].Preview != nil {
				got = s.OpenProposals[0].Preview.Steps[0].Preview.Current
			}
			if got != c.want {
				t.Fatalf("preview current = %q, want %q", got, c.want)
			}
		})
	}
}

func TestCreateProposalTruncatesVerify(t *testing.T) {
	act := &fakeAction{name: "postgres_action", layer: "postgres"}
	svc := previewSvc(t, act)
	roomID := createRoom(t, svc, "arjun").ID
	long := make([]byte, 2000)
	for i := range long {
		long[i] = 'v'
	}
	propose(t, svc, roomID, events.ProposalStep{Action: "postgres_action", Layer: "postgres", Params: json.RawMessage(`{}`), Verify: string(long)})
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(snap.OpenProposals[0].Steps[0].Verify); n > 510 {
		t.Fatalf("verify is %d bytes, want it capped near 500", n)
	}
}
