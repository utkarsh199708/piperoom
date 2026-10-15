package room_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// lostWriteStore is a memstore whose rooms-row close and proposals-row
// "rejected" status writes fail while their switch is on: the side-table
// writes that follow room.closed and approval.rejected.
type lostWriteStore struct {
	*memstore.Store
	mu         sync.Mutex
	failClose  bool
	failReject bool
}

func (f *lostWriteStore) set(close, reject bool) {
	f.mu.Lock()
	f.failClose, f.failReject = close, reject
	f.mu.Unlock()
}

func (f *lostWriteStore) CloseRoom(ctx context.Context, id string, closedAt time.Time) error {
	f.mu.Lock()
	fail := f.failClose
	f.mu.Unlock()
	if fail {
		return errSideWrite
	}
	return f.Store.CloseRoom(ctx, id, closedAt)
}

func (f *lostWriteStore) SetProposalStatus(ctx context.Context, proposalID, status string) error {
	f.mu.Lock()
	fail := f.failReject && status == "rejected"
	f.mu.Unlock()
	if fail {
		return errSideWrite
	}
	return f.Store.SetProposalStatus(ctx, proposalID, status)
}

// PurgeClosed is promoted from memstore; the assertion keeps the retention
// path under test.
var _ store.Purger = (*lostWriteStore)(nil)

func newLostWriteService(t *testing.T, st *lostWriteStore, c *clock) *room.Service {
	t.Helper()
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{
		&fakeAction{name: "pg_act", layer: "postgres"},
		&fakeAction{name: "kafka_act", layer: "kafka"},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register action: %v", err)
		}
	}
	svc := room.New(room.Deps{
		Store:        st,
		Actions:      reg,
		Users:        room.DefaultUsers(),
		Owners:       testOwners(),
		Run:          fakeRun,
		WriteHandoff: fakeWrite,
		Now:          c.Now,
	})
	t.Cleanup(svc.Close)
	return svc
}

func newLostWriteStore() (*lostWriteStore, *clock) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	return &lostWriteStore{Store: memstore.NewWithClock(c.Now)}, c
}

// closeWithLostRow closes a new room while the rows write fails, and checks
// the close still happened and the row missed it.
func closeWithLostRow(t *testing.T, svc *room.Service, st *lostWriteStore) (string, time.Time) {
	t.Helper()
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	st.set(true, false)
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), roomID, "fixed"); err != nil {
		t.Fatalf("close with a failing row write: %v", err)
	}
	st.set(false, false)
	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil || snap.Status != "closed" || snap.ClosedAt == nil {
		t.Fatalf("snapshot after close = %+v (%v), want closed", snap, err)
	}
	if row, _ := st.Store.GetRoom(ctx, roomID); row.Status != "open" {
		t.Fatalf("row status = %q, want open (the write failed)", row.Status)
	}
	return roomID, *snap.ClosedAt
}

func wantClosedRow(t *testing.T, st *lostWriteStore, roomID string, at time.Time) {
	t.Helper()
	row, err := st.Store.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	if row.Status != "closed" || row.ClosedAt == nil || !row.ClosedAt.Equal(at) {
		t.Fatalf("row = %s closed_at %v, want closed at %v (room.closed's time)", row.Status, row.ClosedAt, at)
	}
}

// A room whose rows close was lost is closed in the row when any service
// loads it from the log, with room.closed's own time.
func TestLostCloseRowRepairedOnLoad(t *testing.T) {
	st, c := newLostWriteStore()
	svc := newLostWriteService(t, st, c)
	roomID, closedAt := closeWithLostRow(t, svc, st)

	svc2 := newLostWriteService(t, st, c)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	wantClosedRow(t, st, roomID, closedAt)
}

// A room that stays cached is repaired by the retention pass, and purged by
// the same pass once its retention is over; a failed repair is retried.
func TestLostCloseRowRepairedByRetention(t *testing.T) {
	ctx := context.Background()
	st, c := newLostWriteStore()
	svc := newLostWriteService(t, st, c)
	roomID, _ := closeWithLostRow(t, svc, st)
	other, _ := closeWithLostRow(t, svc, st)

	// The repair itself fails: nothing is purged, the row stays open.
	st.set(true, false)
	c.Advance(40 * 24 * time.Hour)
	res, err := svc.PurgeByPolicy(ctx, c.Now(), room.RetentionPolicy{GlobalDays: 30})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(res.RoomIDs) != 0 {
		t.Fatalf("purged %v with the rows still open", res.RoomIDs)
	}
	st.set(false, false)

	// The next pass repairs both rows and purges both rooms.
	res, err = svc.PurgeByPolicy(ctx, c.Now(), room.RetentionPolicy{GlobalDays: 30})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if len(res.RoomIDs) != 2 {
		t.Fatalf("purged %v, want %s and %s", res.RoomIDs, roomID, other)
	}
	_, err = svc.GetRoom(ctx, roomID)
	wantCode(t, err, "not_found")
}

// The repair only happens when the row says open and the log says closed: an
// open room keeps an open row through load and retention.
func TestOpenRoomRowUntouched(t *testing.T) {
	ctx := context.Background()
	st, c := newLostWriteStore()
	svc := newLostWriteService(t, st, c)
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.PurgeByPolicy(ctx, c.Now(), room.RetentionPolicy{GlobalDays: 1}); err != nil {
		t.Fatalf("purge: %v", err)
	}
	svc2 := newLostWriteService(t, st, c)
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	if row, _ := st.Store.GetRoom(ctx, roomID); row.Status != "open" || row.ClosedAt != nil {
		t.Fatalf("row = %s %v, want open", row.Status, row.ClosedAt)
	}
}

// twoLayerProposal opens a room with a proposal that needs postgres (priya)
// and kafka (arjun).
func twoLayerProposal(t *testing.T, svc *room.Service) (roomID, pid, hash string) {
	t.Helper()
	roomID = createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "kafka_act", Layer: "kafka", Params: json.RawMessage("{}")},
		},
		Risk: "low", Rollback: "undo",
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	return roomID, pid, proposalHash(t, svc, pid)
}

func countEventType(t *testing.T, st *lostWriteStore, roomID string, typ events.Type) int {
	t.Helper()
	evs, err := st.Store.Since(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	n := 0
	for _, e := range evs {
		if e.Type == typ {
			n++
		}
	}
	return n
}

// A rejection whose "rejected" row write was lost still decides the proposal:
// the log says rejected, so a later approval or rejection of another layer is
// already_decided and logs nothing, here and after a restart, and the row is
// repaired once the store takes the write.
func TestLostRejectedStatusRefusesApproval(t *testing.T) {
	ctx := context.Background()
	st, c := newLostWriteStore()
	svc := newLostWriteService(t, st, c)
	roomID, pid, hash := twoLayerProposal(t, svc)
	arjun, priya := mustUser(t, svc, "arjun"), mustUser(t, svc, "priya")

	// Every rejected-status write fails, the repair's too: the refusal must
	// not depend on it.
	st.set(false, true)
	if _, err := svc.RejectProposal(ctx, priya, pid, "postgres", "too risky", hash); err != nil {
		t.Fatalf("reject with a failing status write: %v", err)
	}
	if p, _ := st.Store.GetProposal(ctx, pid); p.Status != "open" {
		t.Fatalf("row status = %q, want open (the write failed)", p.Status)
	}
	_, err := svc.ApproveProposal(ctx, arjun, pid, "kafka", hash)
	wantCode(t, err, "already_decided")
	_, err = svc.RejectProposal(ctx, arjun, pid, "kafka", "also no", hash)
	wantCode(t, err, "already_decided")
	if n := countEventType(t, st, roomID, events.ApprovalGiven); n != 0 {
		t.Fatalf("approval.given logged %d times for a rejected proposal", n)
	}
	if n := countEventType(t, st, roomID, events.ApprovalRejected); n != 1 {
		t.Fatalf("approval.rejected logged %d times, want 1", n)
	}
	if apps, _ := st.Store.ListApprovals(ctx, pid); len(apps) != 1 {
		t.Fatalf("approval rows = %+v, want only the rejection", apps)
	}

	// A restarted service folds the same answer from the log; with the store
	// healthy again the refusal also repairs the row.
	svc2 := newLostWriteService(t, st, c)
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	_, err = svc2.ApproveProposal(ctx, arjun, pid, "kafka", hash)
	wantCode(t, err, "already_decided")
	if p, _ := st.Store.GetProposal(ctx, pid); p.Status != "open" {
		t.Fatalf("row status = %q, want open while the write still fails", p.Status)
	}
	st.set(false, false)
	_, err = svc2.ApproveProposal(ctx, arjun, pid, "kafka", hash)
	wantCode(t, err, "already_decided")
	if p, _ := st.Store.GetProposal(ctx, pid); p.Status != "rejected" {
		t.Fatalf("row status after repair = %q, want rejected", p.Status)
	}
	if n := countEventType(t, st, roomID, events.ApprovalGiven); n != 0 {
		t.Fatalf("approval.given logged %d times for a rejected proposal", n)
	}
}

// Without a lost write nothing changes: the other layer's approval of an open
// proposal still lands.
func TestOpenProposalStillApproves(t *testing.T) {
	st, c := newLostWriteStore()
	svc := newLostWriteService(t, st, c)
	_, pid, hash := twoLayerProposal(t, svc)
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", hash); err != nil {
		t.Fatalf("approve: %v", err)
	}
}
