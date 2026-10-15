package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Tests for the cross-room passes that must see every room, the alert key
// lock's context, and the Load sweeps that keep their store calls off the
// room locks.

// A room another caller is loading when the open-room limit runs is waited
// for, not skipped: the pass warns it too.
func TestEnforceOpenLimitCountsRoomMidLoad(t *testing.T) {
	c := &testClock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
	st := &gatedStore{Store: memstore.NewWithClock(c.Now), entered: make(chan string, 64), loads: map[string]int{}}
	deps := Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun, Now: c.Now,
		Limits: config.Limits{MaxOpenDays: 14}}
	first := New(deps)
	ids := createRooms(t, first, 2)
	first.Close()
	svc := New(deps)
	t.Cleanup(svc.Close)
	a := ids[0]
	ctx := context.Background()
	c.Advance(13 * 24 * time.Hour)

	base := st.loadsOf(a)
	release := st.arm(a, "since")
	t.Cleanup(release)
	loaded := make(chan error, 1)
	go func() {
		_, err := svc.GetRoom(ctx, a)
		loaded <- err
	}()
	st.waitHeld(t, "since")

	type result struct {
		res OpenLimitResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := svc.EnforceOpenLimit(ctx)
		done <- result{res, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("the pass returned while room A was still loading: %+v %v", r.res, r.err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case r := <-done:
		if r.err != nil || r.res.Warned != 2 || r.res.Failed != 0 {
			t.Fatalf("pass = %+v, %v; want both rooms warned", r.res, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pass did not finish after the load")
	}
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
	if n := st.loadsOf(a) - base; n != 1 {
		t.Fatalf("%d rebuilds of room A, want 1 (the pass shares the load in flight)", n)
	}
}

// The wait for an in-flight load ends with the pass's context.
func TestEnforceOpenLimitWaitIsBoundedByContext(t *testing.T) {
	st := newGatedStore()
	first := New(Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun})
	ids := createRooms(t, first, 1)
	first.Close()
	svc := New(Deps{Store: st, Users: DefaultUsers(), Owners: tenantOwners(), Run: idleRun,
		Limits: config.Limits{MaxOpenDays: 14}})
	t.Cleanup(svc.Close)
	release := st.arm(ids[0], "since")
	t.Cleanup(release)
	go func() { _, _ = svc.GetRoom(context.Background(), ids[0]) }()
	st.waitHeld(t, "since")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	within(t, time.Second, "EnforceOpenLimit", func() error {
		if _, err := svc.EnforceOpenLimit(ctx); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("err = %v, want the deadline", err)
		}
		return nil
	})
}

// sameStripeKey returns a key other than key on key's alert lock stripe.
func sameStripeKey(t *testing.T, key string) string {
	t.Helper()
	stripe := func(k string) uint32 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(k))
		return h.Sum32() % alertLockStripes
	}
	want := stripe(key)
	for i := range 100000 {
		if k := fmt.Sprintf("other-%d", i); stripe(k) == want {
			return k
		}
	}
	t.Fatal("no key on the same stripe")
	return ""
}

// Waiting for an alert key's in-process lock ends with the caller's context,
// for the same key and for another key on the same stripe; the holder keeps
// its lock, and the stripe is free again once it lets go.
func TestLockAlertKeyRespectsContext(t *testing.T) {
	svc := New(Deps{Store: memstore.New()})
	t.Cleanup(svc.Close)
	unlock, err := svc.lockAlertKey(context.Background(), "orders\x00k1")
	if err != nil {
		t.Fatal(err)
	}
	other := sameStripeKey(t, "orders\x00k1")
	for _, key := range []string{"orders\x00k1", other} {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		within(t, time.Second, "lockAlertKey("+key+")", func() error {
			if u, err := svc.lockAlertKey(ctx, key); !errors.Is(err, context.DeadlineExceeded) {
				if u != nil {
					u()
				}
				return fmt.Errorf("err = %v, want the deadline", err)
			}
			return nil
		})
		cancel()
	}
	// An already cancelled context never hangs on a held stripe.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	within(t, time.Second, "lockAlertKey(cancelled)", func() error {
		if _, err := svc.lockAlertKey(ctx, "orders\x00k1"); !errors.Is(err, context.Canceled) {
			return fmt.Errorf("err = %v, want canceled", err)
		}
		return nil
	})

	unlock()
	within(t, time.Second, "lockAlertKey after unlock", func() error {
		u, err := svc.lockAlertKey(context.Background(), other)
		if err == nil {
			u()
		}
		return err
	})
}

// An alert whose key's stripe is held elsewhere gives up when its request's
// context ends, instead of waiting for the slow open.
func TestOpenAlertRoomGivesUpOnBusyStripe(t *testing.T) {
	svc := newLockService(t, memstore.New())
	key := alertIndexKey("orders", "generic:k1")
	unlock, err := svc.lockAlertKey(context.Background(), sameStripeKey(t, key))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	within(t, time.Second, "OpenAlertRoom", func() error {
		if _, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{
			Source: alertin.Generic, Status: alertin.StatusFiring, Title: "Lag", Severity: "sev2",
			PipelineID: "orders", DedupKey: "generic:k1", Raw: []byte(`{"title":"Lag"}`),
		}); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("err = %v, want the deadline", err)
		}
		return nil
	})
}

// lockWatchStore is a memstore that records every watched call made while
// some cached room's lock is held (in these tests only the code under test
// takes room locks, so a held lock is its own).
type lockWatchStore struct {
	*memstore.Store
	svc   atomic.Pointer[Service]
	mu    sync.Mutex
	under []string
	calls map[string]int
}

func (w *lockWatchStore) check(op string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.calls == nil {
		w.calls = map[string]int{}
	}
	w.calls[op]++
	s := w.svc.Load()
	if s == nil {
		return
	}
	for id, st := range s.cachedRooms() {
		if !st.mu.TryLock() {
			w.under = append(w.under, op+" "+id)
			continue
		}
		st.mu.Unlock()
	}
}

func (w *lockWatchStore) report() (under []string, calls map[string]int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]int{}
	for k, v := range w.calls {
		out[k] = v
	}
	return append([]string(nil), w.under...), out
}

func (w *lockWatchStore) GetProposal(ctx context.Context, id string) (store.ProposalRow, error) {
	w.check("GetProposal")
	return w.Store.GetProposal(ctx, id)
}

func (w *lockWatchStore) FinishProposalStep(ctx context.Context, id string, i int, status, output string) error {
	w.check("FinishProposalStep")
	return w.Store.FinishProposalStep(ctx, id, i, status, output)
}

func (w *lockWatchStore) SetProposalStatus(ctx context.Context, id, status string) error {
	w.check("SetProposalStatus")
	return w.Store.SetProposalStatus(ctx, id, status)
}

func (w *lockWatchStore) GetShadowRun(ctx context.Context, roomID string) (store.ShadowRun, error) {
	w.check("GetShadowRun")
	return w.Store.GetShadowRun(ctx, roomID)
}

func (w *lockWatchStore) CreateShadowRun(ctx context.Context, r store.ShadowRun) error {
	w.check("CreateShadowRun")
	return w.Store.CreateShadowRun(ctx, r)
}

func (w *lockWatchStore) UpdateShadowRun(ctx context.Context, r store.ShadowRun) error {
	w.check("UpdateShadowRun")
	return w.Store.UpdateShadowRun(ctx, r)
}

func (w *lockWatchStore) MarkRevealed(ctx context.Context, roomID string, at time.Time) (bool, error) {
	w.check("MarkRevealed")
	return w.Store.MarkRevealed(ctx, roomID, at)
}

// Load interrupts a stale proposal with its store reads and writes made
// outside the room's lock; only the action.executed append is under it.
func TestLoadInterruptsStaleProposalsOffTheRoomLock(t *testing.T) {
	ctx := context.Background()
	w := &lockWatchStore{Store: memstore.New()}
	reg := actions.NewRegistry()
	if err := reg.Register(&countAction{}); err != nil {
		t.Fatal(err)
	}
	deps := Deps{Store: w, Actions: reg, Users: DefaultUsers(), Run: idleRun,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}}}
	first := New(deps)
	arjun, _ := first.LookupUser("arjun")
	snap, err := first.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := first.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{{Action: "count_act", Layer: "postgres", Params: json.RawMessage(`{}`)}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	// Approved and claimed before the crash, written straight to the store.
	prior := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	if _, err := w.Append(ctx, snap.ID, events.ApprovalGiven, prior, events.ApprovalGivenPayload{ProposalID: pid, Layer: "postgres"}); err != nil {
		t.Fatal(err)
	}
	if err := w.StartProposalStep(ctx, pid, 0); err != nil {
		t.Fatal(err)
	}

	svc := New(deps)
	t.Cleanup(svc.Close)
	w.svc.Store(svc)
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	under, calls := w.report()
	if len(under) != 0 {
		t.Fatalf("store calls under a room lock: %v", under)
	}
	if calls["GetProposal"] == 0 || calls["FinishProposalStep"] == 0 || calls["SetProposalStatus"] == 0 {
		t.Fatalf("the sweep made no store calls: %v", calls)
	}
	if row, err := w.GetProposal(ctx, pid); err != nil || row.Status != "failed" {
		t.Fatalf("proposal after Load: %+v %v", row, err)
	}
	if got, _ := svc.GetRoom(ctx, snap.ID); len(got.OpenProposals) != 0 {
		t.Fatalf("proposal still open in the snapshot: %+v", got.OpenProposals)
	}

	// The check under the lock: a proposal decided between the read and the
	// lock is left alone.
	st, err := svc.lockRoom(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	last := st.snap.LastSeq
	got := svc.interruptStaleProposalsLocked(ctx, snap.ID, st, []staleProposal{{p: Proposal{ProposalID: pid}}})
	if len(got) != 0 || st.snap.LastSeq != last {
		t.Fatalf("a decided proposal was interrupted again: %+v (seq %d -> %d)", got, last, st.snap.LastSeq)
	}
	svc.unlockRoom(st)
}

// Load's shadow sweep reads and writes the run rows of rooms that need no
// reveal with no room lock held.
func TestShadowSweepOffTheRoomLock(t *testing.T) {
	ctx := context.Background()
	w := &lockWatchStore{Store: memstore.New()}
	system := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
	mk := func(id string, closed bool) {
		t.Helper()
		if err := w.CreateRoom(ctx, store.Room{ID: id, Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append(ctx, id, events.RoomCreated, system,
			events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders", AlertSource: "generic", Shadow: true}); err != nil {
			t.Fatal(err)
		}
		if closed {
			if _, err := w.Append(ctx, id, events.RoomClosed, system, events.RoomClosedPayload{Resolution: "fixed"}); err != nil {
				t.Fatal(err)
			}
			if err := w.CloseRoom(ctx, id, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	mk("r_running", false)
	mk("r_norow", false)
	mk("r_half", true)
	now := time.Now().UTC()
	if err := w.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r_running", Status: store.ShadowRunning, StartedAt: &now}); err != nil {
		t.Fatal(err)
	}
	// Revealed in the log, revealed_at not yet written (a crash in between).
	if err := w.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r_half", Status: store.ShadowHypothesised, FinishedAt: &now}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(ctx, "r_half", events.ShadowRevealed, system, events.ShadowRevealedPayload{Status: store.ShadowHypothesised}); err != nil {
		t.Fatal(err)
	}

	svc := New(Deps{Store: w, Users: DefaultUsers(), Run: idleRun,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}}})
	t.Cleanup(svc.Close)
	w.svc.Store(svc)
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	under, calls := w.report()
	if len(under) != 0 {
		t.Fatalf("store calls under a room lock: %v", under)
	}
	if calls["UpdateShadowRun"] == 0 || calls["CreateShadowRun"] == 0 || calls["MarkRevealed"] == 0 {
		t.Fatalf("the sweep made no row writes: %v", calls)
	}
	for _, id := range []string{"r_running", "r_norow"} {
		if r, err := w.Store.GetShadowRun(ctx, id); err != nil || r.Status != store.ShadowInterrupted {
			t.Fatalf("%s after Load: %+v %v", id, r, err)
		}
	}
	if r, _ := w.Store.GetShadowRun(ctx, "r_half"); r.RevealedAt == nil {
		t.Fatal("r_half: revealed_at not set")
	}
}

// A run that is active in this process is not marked interrupted by the
// sweep: the check is made under the room's lock.
func TestShadowSweepLeavesActiveRun(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	svc := New(Deps{Store: mem})
	t.Cleanup(svc.Close)
	now := time.Now().UTC()
	if err := mem.CreateRoom(ctx, store.Room{ID: "r1", Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := mem.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r1", Status: store.ShadowRunning, StartedAt: &now}); err != nil {
		t.Fatal(err)
	}
	st := &roomState{snap: NewSnapshot(), shadowActive: true}
	st.snap.ID, st.snap.Status, st.snap.Shadow = "r1", "open", true
	st.publish()
	svc.sweepShadowRoom(ctx, "r1", st, now)
	if r, _ := mem.GetShadowRun(ctx, "r1"); r.Status != store.ShadowRunning {
		t.Fatalf("active run marked %s", r.Status)
	}
}
