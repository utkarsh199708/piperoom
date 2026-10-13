package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// roomWithChildren opens a room as arjun and gives it one row in every child
// table: a suggestion, a proposal with an approval and an executed step, and a
// pending hand-off. It returns the room id and the proposal id.
func roomWithChildren(t *testing.T, svc *room.Service, st *memstore.Store) (roomID, proposalID string) {
	t.Helper()
	ctx := context.Background()
	roomID = createRoom(t, svc, "arjun").ID
	if _, err := svc.Steer(ctx, mustUser(t, svc, "kabir"), roomID, "look at the slot"); err != nil {
		t.Fatalf("suggest: %v", err)
	}
	pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title:          "fix",
		Steps:          []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
		RequiredLayers: []string{"postgres"},
		Risk:           "low", Rollback: "undo",
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("proposal: %v", err)
	}
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(ctx, pid)
		return err == nil && p.Status == "executed"
	})
	if _, err := svc.CreateHandoff(ctx, mustUser(t, svc, "arjun"), roomID, "priya", false); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	return roomID, pid
}

func TestPurgeExpiredDeletesOnlyOldClosedRooms(t *testing.T) {
	svc, st, c := newTestService(t, &fakeAction{name: "pg_act", layer: "postgres"})
	ctx := context.Background()
	day := 24 * time.Hour

	// A: closed long ago. B: closed recently. C: still open, but opened as long
	// ago as A. D: closed long ago too, to prove more than one room goes.
	aID, aPid := roomWithChildren(t, svc, st)
	dID := createRoom(t, svc, "arjun").ID
	cID := createRoom(t, svc, "arjun").ID
	bID, bPid := roomWithChildren(t, svc, st)
	aEvents := len(allEvents(t, st, aID))
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), aID, "old"); err != nil {
		t.Fatalf("close A: %v", err)
	}
	aEvents++ // the room.closed event
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), dID, "old"); err != nil {
		t.Fatalf("close D: %v", err)
	}

	c.Advance(20 * day)
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), bID, "recent"); err != nil {
		t.Fatalf("close B: %v", err)
	}
	c.Advance(20 * day) // now: A and D closed 40 days ago, B 20 days ago

	// Warm the cache so eviction is exercised.
	for _, id := range []string{aID, bID, cID, dID} {
		if _, err := svc.GetRoom(ctx, id); err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
	}

	res, err := svc.PurgeExpired(ctx, c.Now().Add(-30*day))
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(res.RoomIDs) != 2 {
		t.Fatalf("want 2 rooms purged, got %v", res.RoomIDs)
	}
	if res.Proposals != 1 || res.Approvals != 1 || res.Steps != 1 || res.Suggestions != 1 || res.Handoffs != 1 {
		t.Fatalf("child counts wrong: %+v", res)
	}
	if res.Events < int64(aEvents) {
		t.Fatalf("want at least %d events deleted, got %d", aEvents, res.Events)
	}

	// A and D are gone everywhere: room, log and every child row.
	for _, id := range []string{aID, dID} {
		if _, err := svc.GetRoom(ctx, id); err == nil {
			t.Fatalf("room %s still readable", id)
		}
		if _, err := st.Since(ctx, id, 0, 10); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("events of %s should be gone, got %v", id, err)
		}
	}
	if _, err := st.GetProposal(ctx, aPid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("proposal of purged room still there: %v", err)
	}
	if st.ProposalStepStatus(aPid, 0) != "" {
		t.Fatalf("proposal step of purged room still there")
	}
	if err := st.RecordApproval(ctx, store.ApprovalRow{ProposalID: aPid, Layer: "postgres", Decision: "approved"}); err != nil {
		t.Fatalf("approval row of purged room should be gone (insert must succeed): %v", err)
	}

	// B (closed recently) and C (open) are untouched, children included.
	for _, id := range []string{bID, cID} {
		if _, err := svc.GetRoom(ctx, id); err != nil {
			t.Fatalf("room %s must be kept: %v", id, err)
		}
	}
	if _, err := st.GetProposal(ctx, bPid); err != nil {
		t.Fatalf("recent room's proposal must be kept: %v", err)
	}
	list, err := svc.ListRooms(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("want 2 rooms listed, got %d (%v)", len(list), err)
	}

	// Nothing left to do: a second run deletes nothing.
	res, err = svc.PurgeExpired(ctx, c.Now().Add(-30*day))
	if err != nil || len(res.RoomIDs) != 0 {
		t.Fatalf("second purge: %+v %v", res, err)
	}
}

func TestPurgeExpiredNeverTouchesOpenRooms(t *testing.T) {
	svc, st, c := newTestService(t)
	ctx := context.Background()
	id := createRoom(t, svc, "arjun").ID
	c.Advance(1000 * 24 * time.Hour)
	res, err := svc.PurgeExpired(ctx, c.Now())
	if err != nil || len(res.RoomIDs) != 0 {
		t.Fatalf("open room must be kept: %+v %v", res, err)
	}
	if _, err := st.GetRoom(ctx, id); err != nil {
		t.Fatalf("open room gone: %v", err)
	}
}

func TestPurgeExpiredBoundaryIsStrictlyOlder(t *testing.T) {
	svc, _, c := newTestService(t)
	ctx := context.Background()
	id := createRoom(t, svc, "arjun").ID
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), id, "x"); err != nil {
		t.Fatal(err)
	}
	closedAt := c.Now()
	// cutoff == closed_at: not older, kept.
	if res, err := svc.PurgeExpired(ctx, closedAt); err != nil || len(res.RoomIDs) != 0 {
		t.Fatalf("room closed exactly at the cutoff must be kept: %+v %v", res, err)
	}
	if res, err := svc.PurgeExpired(ctx, closedAt.Add(time.Nanosecond)); err != nil || len(res.RoomIDs) != 1 {
		t.Fatalf("room closed before the cutoff must go: %+v %v", res, err)
	}
}

// noPurgeStore hides the Purger method of the embedded store.
type noPurgeStore struct{ store.Store }

func TestPurgeExpiredNeedsAPurger(t *testing.T) {
	svc := room.New(room.Deps{Store: noPurgeStore{memstore.New()}, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	_, err := svc.PurgeExpired(context.Background(), time.Now())
	if !errors.Is(err, room.ErrRetentionUnsupported) {
		t.Fatalf("want ErrRetentionUnsupported, got %v", err)
	}
}
