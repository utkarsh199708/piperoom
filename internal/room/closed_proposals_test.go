package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// A closed room's snapshot has no open proposals, whatever withdrawals the
// log holds: a room closed before the close withdrew its proposals (Q1) has
// none (QA X1).
func TestClosedRoomFoldHasNoOpenProposals(t *testing.T) {
	arjun := human("arjun", "Arjun")
	s := room.Apply(room.NewSnapshot(), ev(events.RoomCreated, 1, arjun, events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders"}))
	s = room.Apply(s, ev(events.ProposalCreated, 2, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "fix", RequiredLayers: []string{"kafka-connect"},
		Steps: []events.ProposalStep{{Action: "connect_restart_task", Layer: "kafka-connect"}},
	}))
	if len(s.OpenProposals) != 1 {
		t.Fatalf("open proposals before close = %d, want 1", len(s.OpenProposals))
	}
	before := s
	s = room.Apply(s, ev(events.RoomClosed, 3, arjun, events.RoomClosedPayload{Resolution: "fixed"}))
	if len(s.OpenProposals) != 0 {
		t.Fatalf("closed room open_proposals = %+v, want none", s.OpenProposals)
	}
	if len(before.OpenProposals) != 1 {
		t.Fatal("Apply mutated its input")
	}
	// Events after the close do not bring one back.
	s = room.Apply(s, ev(events.ProposalCreated, 4, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: "p_2", Title: "late", RequiredLayers: []string{"kafka"},
	}))
	s = room.Apply(s, ev(events.ProposalPreviewed, 5, events.AgentActor, events.ProposalPreviewedPayload{ProposalID: "p_1"}))
	if len(s.OpenProposals) != 0 {
		t.Fatalf("after late events open_proposals = %+v, want none", s.OpenProposals)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"open_proposals":[]`) {
		t.Fatalf("snapshot JSON: %s", raw)
	}
}

// A room closed before Q1 (room.closed with no withdrawals after it): the
// snapshot lists nothing and approve/reject answer room_closed.
func TestPreQ1ClosedRoomProposals(t *testing.T) {
	ctx := context.Background()
	e := newLifecycleEnv(t, connectConfig())
	roomID := createRoom(t, e.svc, "arjun").ID
	pid := e.propose(t, roomID, events.AgentActor, events.ProposalCreatedPayload{Steps: []events.ProposalStep{step("connect_config", "kafka-connect", "double")}})
	hash := proposalHash(t, e.svc, pid)
	e.svc.Close()

	app(t, e.st, e.clock, roomID, events.RoomClosed, pmHuman("arjun", "Arjun"), events.RoomClosedPayload{Resolution: "fixed"})
	svc := room.New(room.Deps{Store: e.st, Users: lifecycleUsers(t), Owners: testOwners(), Run: fakeRun, Now: e.clock.Now})
	t.Cleanup(svc.Close)
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Status != "closed" || len(snap.OpenProposals) != 0 {
		t.Fatalf("status %q, open_proposals %+v; want closed with none", snap.Status, snap.OpenProposals)
	}
	_, err = svc.ApproveProposal(ctx, mustUser(t, svc, "arjun"), pid, "kafka-connect", hash)
	wantCode(t, err, "room_closed")
	_, err = svc.RejectProposal(ctx, mustUser(t, svc, "arjun"), pid, "kafka-connect", "no", hash)
	wantCode(t, err, "room_closed")
}
