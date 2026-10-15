package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

var errSideWrite = errors.New("simulated side-table failure")

// sideFailStore is a memstore whose writes after an event can fail: the
// side-table writes (driver, handoff status, void) while failSide is set, and
// the next appends of each type counted in failAppend.
type sideFailStore struct {
	*memstore.Store
	mu         sync.Mutex
	failSide   bool
	failAppend map[events.Type]int
}

func newSideFailStore() *sideFailStore {
	return &sideFailStore{Store: memstore.New(), failAppend: map[events.Type]int{}}
}

func (f *sideFailStore) setFailSide(on bool) {
	f.mu.Lock()
	f.failSide = on
	f.mu.Unlock()
}

func (f *sideFailStore) failNext(typ events.Type) {
	f.mu.Lock()
	f.failAppend[typ]++
	f.mu.Unlock()
}

func (f *sideFailStore) sideErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSide {
		return errSideWrite
	}
	return nil
}

func (f *sideFailStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	f.mu.Lock()
	n := f.failAppend[typ]
	if n > 0 {
		f.failAppend[typ] = n - 1
	}
	f.mu.Unlock()
	if n > 0 {
		return events.Event{}, fmt.Errorf("simulated append failure for %s", typ)
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

func (f *sideFailStore) UpdateDriver(ctx context.Context, id, driverID, driverName string, leaseUntil time.Time) error {
	if err := f.sideErr(); err != nil {
		return err
	}
	return f.Store.UpdateDriver(ctx, id, driverID, driverName, leaseUntil)
}

func (f *sideFailStore) SetHandoffStatus(ctx context.Context, handoffID, status string) error {
	if err := f.sideErr(); err != nil {
		return err
	}
	return f.Store.SetHandoffStatus(ctx, handoffID, status)
}

func (f *sideFailStore) VoidHandoffs(ctx context.Context, roomID, exceptHandoffID string) error {
	if err := f.sideErr(); err != nil {
		return err
	}
	return f.Store.VoidHandoffs(ctx, roomID, exceptHandoffID)
}

// handoffTo opens a room driven by arjun and creates his hand-off to toID.
func handoffTo(t *testing.T, svc *room.Service, toID string) (roomID, handoffID string) {
	t.Helper()
	roomID = createRoom(t, svc, "arjun").ID
	ev, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, toID, false)
	if err != nil {
		t.Fatalf("create handoff: %v", err)
	}
	p, err := events.Decode[events.HandoffCreatedPayload](ev)
	if err != nil {
		t.Fatalf("decode handoff.created: %v", err)
	}
	return roomID, p.HandoffID
}

func wantDriver(t *testing.T, svc *room.Service, roomID, id string) {
	t.Helper()
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if snap.Driver == nil || snap.Driver.ID != id {
		t.Fatalf("driver = %+v, want %s", snap.Driver, id)
	}
}

func handoffStatus(t *testing.T, st *sideFailStore, handoffID string) string {
	t.Helper()
	h, err := st.Store.GetHandoff(context.Background(), handoffID)
	if err != nil {
		t.Fatalf("get handoff: %v", err)
	}
	return h.Status
}

// Once control.granted is logged the accept has happened: failed side-table
// writes after it are logged, not answered with a 500, and handoff.accepted is
// still emitted. The handoffs row that missed its write is reconciled from the
// log, so a repeat is already_decided, never stale_handoff, here and after a
// restart.
func TestAcceptHandoffSurvivesFailedSideWrites(t *testing.T) {
	ctx := context.Background()
	st := newSideFailStore()
	svc, _, _ := guardSetup(t, st)
	roomID, hid := handoffTo(t, svc, "priya")
	priya := mustUser(t, svc, "priya")

	st.setFailSide(true)
	e, err := svc.AcceptHandoff(ctx, priya, hid)
	if err != nil {
		t.Fatalf("accept with failing side writes: %v", err)
	}
	if e.Type != events.HandoffAccepted {
		t.Fatalf("accept returned %s, want handoff.accepted", e.Type)
	}
	wantDriver(t, svc, roomID, "priya")
	if n := countType(t, st.Store, roomID, events.HandoffAccepted); n != 1 {
		t.Fatalf("handoff.accepted logged %d times, want 1", n)
	}
	if got := handoffStatus(t, st, hid); got != "pending" {
		t.Fatalf("row status = %q, want pending (the write failed)", got)
	}

	// The client's retry: decided, not stale, and nothing more is logged.
	_, err = svc.AcceptHandoff(ctx, priya, hid)
	wantCode(t, err, "already_decided")
	if n := countType(t, st.Store, roomID, events.HandoffAccepted); n != 1 {
		t.Fatalf("retry logged handoff.accepted again: %d", n)
	}

	// After a restart the log alone gives the same answer, and the row is
	// brought in line once the store takes writes again.
	st.setFailSide(false)
	svc2, _, _ := guardSetup(t, st)
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	wantDriver(t, svc2, roomID, "priya")
	_, err = svc2.AcceptHandoff(ctx, priya, hid)
	wantCode(t, err, "already_decided")
	if got := handoffStatus(t, st, hid); got != "accepted" {
		t.Fatalf("row status after reconcile = %q, want accepted", got)
	}
}

// An accept whose handoff.accepted append fails after control.granted landed
// returns the error, and the recipient's retry finishes the hand-off instead
// of answering stale_handoff, also on a restarted service.
func TestAcceptHandoffRetryFinishesHalfDoneAccept(t *testing.T) {
	ctx := context.Background()
	st := newSideFailStore()
	svc, _, _ := guardSetup(t, st)
	roomID, hid := handoffTo(t, svc, "priya")
	priya := mustUser(t, svc, "priya")

	st.failNext(events.HandoffAccepted)
	if _, err := svc.AcceptHandoff(ctx, priya, hid); err == nil {
		t.Fatal("want the failed handoff.accepted append returned")
	}
	wantDriver(t, svc, roomID, "priya")
	if n := countType(t, st.Store, roomID, events.HandoffAccepted); n != 0 {
		t.Fatalf("handoff.accepted logged %d times, want 0", n)
	}
	if got := handoffStatus(t, st, hid); got != "pending" {
		t.Fatalf("row status = %q, want pending until handoff.accepted is logged", got)
	}

	svc2, _, _ := guardSetup(t, st)
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	e, err := svc2.AcceptHandoff(ctx, priya, hid)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	p, err := events.Decode[events.HandoffAcceptedPayload](e)
	if err != nil || e.Type != events.HandoffAccepted || p.HandoffID != hid {
		t.Fatalf("retry returned %s %+v (%v), want handoff.accepted for %s", e.Type, p, err, hid)
	}
	if n := countType(t, st.Store, roomID, events.ControlGranted); n != 2 {
		t.Fatalf("control.granted logged %d times, want 2 (creator, accept): the retry must not move the role again", n)
	}
	if got := handoffStatus(t, st, hid); got != "accepted" {
		t.Fatalf("row status = %q, want accepted", got)
	}
	_, err = svc2.AcceptHandoff(ctx, priya, hid)
	wantCode(t, err, "already_decided")
}

// A recipient who got the role by a direct grant, not by accepting, still
// finds the pending hand-off stale: only the recipient's own control.granted
// after handoff.created is a half-done accept.
func TestAcceptHandoffAfterDirectGrantStaysStale(t *testing.T) {
	ctx := context.Background()
	st := newSideFailStore()
	svc, _, _ := guardSetup(t, st)
	roomID, hid := handoffTo(t, svc, "priya")
	if _, err := svc.GrantControl(ctx, mustUser(t, svc, "arjun"), roomID, "priya"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	_, err := svc.AcceptHandoff(ctx, mustUser(t, svc, "priya"), hid)
	wantCode(t, err, "stale_handoff")
	if n := countType(t, st.Store, roomID, events.HandoffAccepted); n != 0 {
		t.Fatalf("handoff.accepted logged %d times, want 0", n)
	}
}

// room.created is logged before the creator's control.granted: if the grant
// fails, the room still opens (agent and hook included) without a driver,
// instead of a 500 the client would answer by opening a second room.
func TestCreateRoomSurvivesFailedGrant(t *testing.T) {
	ctx := context.Background()
	st := newSideFailStore()
	svc, runs, hooks := guardSetup(t, st)
	st.failNext(events.ControlGranted)
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "Incident", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if snap.Driver != nil {
		t.Fatalf("driver = %+v, want none", snap.Driver)
	}
	if rooms, _ := svc.ListRooms(ctx); len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	waitFor(t, 5*time.Second, func() bool { return runs.Load() == 1 && hooks.Load() == 1 })
	// With no driver anyone may take control.
	if _, err := svc.GrantControl(ctx, mustUser(t, svc, "kabir"), snap.ID, "kabir"); err != nil {
		t.Fatalf("take control: %v", err)
	}
	wantDriver(t, svc, snap.ID, "kabir")
}

// The unknown-pipeline note after room.created is a note: if it fails, the
// alert room still opens and its agent starts.
func TestAlertRoomSurvivesFailedUnknownPipelineNote(t *testing.T) {
	ctx := context.Background()
	st := newSideFailStore()
	svc, runs, hooks := guardSetup(t, st)
	st.failNext(events.AgentMessage)
	snap, created, err := svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2",
		PipelineID: "not_in_owners", DedupKey: "generic:k", Raw: json.RawMessage(`{}`),
	})
	if err != nil || !created {
		t.Fatalf("open alert room: created=%v err=%v", created, err)
	}
	if n := countType(t, st.Store, snap.ID, events.RoomCreated); n != 1 {
		t.Fatalf("room.created logged %d times, want 1", n)
	}
	waitFor(t, 5*time.Second, func() bool { return runs.Load() == 1 && hooks.Load() == 1 })
	// The same alert joins the room it opened.
	again, created, err := svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2",
		PipelineID: "not_in_owners", DedupKey: "generic:k", Raw: json.RawMessage(`{}`),
	})
	if err != nil || created || again.ID != snap.ID {
		t.Fatalf("repeat: id=%s created=%v err=%v, want a join of %s", again.ID, created, err, snap.ID)
	}
}

// A close racing the last approval never drops the approved proposal: it
// either runs or is recorded as failed, and every executor goroutine stops.
func TestCloseRacingApprovalRecordsProposal(t *testing.T) {
	ctx := context.Background()
	act := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, st, _ := newTestService(t, act)
	arjun, priya := mustUser(t, svc, "arjun"), mustUser(t, svc, "priya")
	var approved []string
	for i := 0; i < 20; i++ {
		roomID := createRoom(t, svc, "arjun").ID
		pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
			Risk: "low", Rollback: "revert the change", Title: "fix",
			Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
		}, events.AgentActor)
		if err != nil {
			t.Fatalf("create proposal: %v", err)
		}
		hash := proposalHash(t, svc, pid)
		var wg sync.WaitGroup
		var approveErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, approveErr = svc.ApproveProposal(ctx, priya, pid, "postgres", hash)
		}()
		go func() {
			defer wg.Done()
			if _, err := svc.CloseRoom(ctx, arjun, roomID, "done"); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
		wg.Wait()
		if approveErr == nil {
			approved = append(approved, pid)
		}
	}
	waitFor(t, 5*time.Second, func() bool {
		for _, pid := range approved {
			if p, err := st.GetProposal(ctx, pid); err != nil || p.Status == "open" {
				return false
			}
		}
		return true
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not stop the executors within 5s")
	}
}
