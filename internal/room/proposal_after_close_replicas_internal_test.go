package room

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// closingCoord is a shared store whose CreateProposal is the moment another
// replica closes the room: the room.closed lands between this replica's check
// and its proposal.created append.
type closingCoord struct {
	fakeCoord
	t *testing.T
}

func (c *closingCoord) CreateProposal(ctx context.Context, p store.ProposalRow) error {
	if _, err := c.Store.Append(ctx, p.RoomID, events.RoomClosed, events.Actor{Kind: events.ActorHuman, ID: "kabir", Name: "Kabir"}, events.RoomClosedPayload{Resolution: "done"}); err != nil {
		c.t.Fatalf("close on the other replica: %v", err)
	}
	return c.Store.CreateProposal(ctx, p)
}

// QA R7: a proposal.created must not land after room.closed written by another
// replica: the append is refused and the row removed.
func TestCreateProposalRefusedWhenAnotherReplicaClosedTheRoom(t *testing.T) {
	ctx := context.Background()
	ms := memstore.New()
	reg := actions.NewRegistry()
	if err := reg.Register(&previewStub{name: "connect_restart_task"}); err != nil {
		t.Fatal(err)
	}
	run := func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		<-ctx.Done()
		return nil
	}
	owns := &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}}
	// The room is opened on a single-process service, then seen from a replica
	// of a shared store.
	setup := New(Deps{Store: ms, Actions: reg, Users: DefaultUsers(), Owners: owns, Run: run})
	u, _ := setup.users.Get("arjun")
	snap, err := setup.CreateRoom(ctx, u, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	setup.Close()
	cc := &closingCoord{fakeCoord: fakeCoord{Store: ms}, t: t}
	svc := New(Deps{Store: cc, Actions: reg, Users: DefaultUsers(), Owners: owns, Run: run, ReplicaID: "replica-b"})
	t.Cleanup(svc.Close)
	if _, err := svc.GetRoom(ctx, snap.ID); err != nil {
		t.Fatal(err)
	}
	_, err = svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "Fix", Risk: "low", Rollback: "restart again", RequiredLayers: []string{"kafka-connect"},
		Steps: []events.ProposalStep{{Action: "connect_restart_task", Layer: "kafka-connect", Description: "restart"}},
	}, events.AgentActor)
	var re *Error
	if !errors.As(err, &re) || re.Code != codeRoomClosed {
		t.Fatalf("CreateProposal err = %v, want room_closed", err)
	}
	evs, err := ms.Since(ctx, snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	closedAt := int64(0)
	for _, e := range evs {
		switch e.Type {
		case events.RoomClosed:
			closedAt = e.Seq
		case events.ProposalCreated:
			t.Fatalf("proposal.created written at seq %d (closed at %d)", e.Seq, closedAt)
		}
	}
	if closedAt == 0 {
		t.Fatal("room.closed missing from the log")
	}
}
