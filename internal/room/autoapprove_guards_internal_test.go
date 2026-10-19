package room

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// The fire path's guards one by one (LEAD-REVIEW on #629): each test fails
// when its guard is removed. Plus the cancel rule once the policy started,
// and the once-per-proposal logging of the policy loop.

// aaLogs records every log line as "message key=value ...", so a test can
// count how often the loop said something.
type aaLogs struct {
	mu    sync.Mutex
	lines []string
}

func (l *aaLogs) Enabled(context.Context, slog.Level) bool { return true }
func (l *aaLogs) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" " + a.Key + "=" + a.Value.String())
		return true
	})
	l.mu.Lock()
	l.lines = append(l.lines, b.String())
	l.mu.Unlock()
	return nil
}
func (l *aaLogs) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *aaLogs) WithGroup(string) slog.Handler      { return l }

// count is how many lines contain every one of subs.
func (l *aaLogs) count(subs ...string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
outer:
	for _, line := range l.lines {
		for _, s := range subs {
			if !strings.Contains(line, s) {
				continue outer
			}
		}
		n++
	}
	return n
}

func (r *aaRig) autoNotes() int {
	r.svc.autoSkipMu.Lock()
	defer r.svc.autoSkipMu.Unlock()
	return len(r.svc.autoSkip)
}

// Review item 1 (and cancellable's policyN == 0): the policy approved one
// layer of two and stopped. A cancel then is 409 not_scheduled and undoes
// nothing.
func TestAutoApproveCancelRefusedOncePolicyStarted(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")
	c.Advance(aaDelay)
	st := r.svc.lockCachedRoom(roomID)
	row, err := ms.GetProposal(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.svc.appendApprovalLocked(ctx, st, row, "kafka-connect", approver{actor: systemActor, detail: events.GrantPolicy}); err != nil {
		t.Fatal(err)
	}
	r.svc.unlockRoom(st)

	for _, who := range []string{"arjun", "kabir"} { // the driver, the owner of the pending layer
		if _, err := r.svc.CancelProposal(ctx, r.user(who), pid, "too late"); !isCode(err, "not_scheduled") {
			t.Fatalf("cancel by %s after the policy started = %v, want 409 not_scheduled", who, err)
		}
	}
	if n := len(logOf(t, ms, roomID, events.ProposalCancelled)); n != 0 {
		t.Fatalf("%d proposal.cancelled, want 0", n)
	}
	got := policyApprovals(t, ms, roomID)
	if len(got) != 1 || got[0].Layer != "kafka-connect" {
		t.Fatalf("policy approvals = %+v, want the one kafka-connect approval kept", got)
	}
	apps, err := ms.ListApprovals(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 1 || apps[0].Layer != "kafka-connect" || apps[0].Decision != "approved" {
		t.Fatalf("approvals rows = %+v, want the kafka-connect row kept", apps)
	}
	if p := r.proposal(roomID, pid); p.Approvals["kafka-connect"] != "approved" || p.Approvals["consumer"] != "pending" {
		t.Fatalf("approvals = %v", p.Approvals)
	}
}

// Review item 2a (the due guard): a stale replica's fire before run_at
// approves nothing.
func TestAutoApproveFireBeforeRunAtApprovesNothing(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	setup := newAARig(t, ms, c, acts, aaOpts{})
	roomID := setup.open()
	pid := setup.propose(roomID, "", "connect_restart_task")
	setup.svc.Close()

	fc := &aaCoord{fakeCoord: &fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}}
	stale := New(Deps{Store: fc, Actions: acts.reg, Users: DefaultUsers(), Owners: aaOwners(), Run: idleRun, Now: c.Now,
		ReplicaID: "replica-a", AutoApproveDelay: aaDelay})
	t.Cleanup(stale.Close)
	mustRoom(t, stale, roomID)
	c.Advance(aaDelay - time.Millisecond)
	stale.firePolicy(ctx, roomID, pid)
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals before run_at, want 0", n)
	}
	if fc.claimCount() != 0 {
		t.Fatal("handed to the executor before run_at")
	}
	c.Advance(time.Millisecond)
	stale.firePolicy(ctx, roomID, pid)
	if n := len(policyApprovals(t, ms, roomID)); n != 1 {
		t.Fatalf("%d policy approvals at run_at, want 1", n)
	}
}

// Review item 2b (the policy guard): a restart loads an auto_approve: list
// that no longer covers the step. Nothing is approved and the refusal is
// logged once, however many checks run.
func TestAutoApprovePolicyNarrowedAtRestart(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	a := newAARig(t, ms, c, acts, aaOpts{})
	roomID := a.open()
	pid := a.propose(roomID, "", "connect_restart_task")
	a.svc.Close()

	logs := &aaLogs{}
	b := newAARig(t, ms, c, acts, aaOpts{own: aaOwners("consumer_restart"), log: slog.New(logs)})
	if p := b.proposal(roomID, pid); p.Scheduled == nil {
		t.Fatal("countdown lost at restart")
	}
	c.Advance(aaDelay)
	for i := 0; i < 3; i++ {
		b.tick()
	}
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 || acts.restart.count() != 0 {
		t.Fatalf("%d approvals, %d runs under a list that no longer covers the step", n, acts.restart.count())
	}
	if n := logs.count("does not approve", "proposal="+pid, "no longer covers"); n != 1 {
		t.Fatalf("refusal logged %d times over 3 checks, want 1:\n%s", n, strings.Join(logs.lines, "\n"))
	}
}

// The preview half of the same guard: a later proposal.previewed that
// fails replaces the good one, and the policy no longer approves.
func TestAutoApprovePreviewTurnedBad(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	logs := &aaLogs{}
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{log: slog.New(logs)})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task")
	emitEvent(t, r.svc, roomID, systemActor, events.ProposalPreviewed, events.ProposalPreviewedPayload{
		ProposalID: pid, Steps: []events.StepPreview{{StepIndex: 0, Error: "connect down"}}, At: aaT0,
	})
	c.Advance(aaDelay)
	r.tick()
	r.tick()
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals after a failing preview, want 0", n)
	}
	if n := logs.count("does not approve", "preview"); n != 1 {
		t.Fatalf("refusal logged %d times, want 1", n)
	}
}

// Review item 2c (the layer guard), two ways. First as it can happen: a
// person approved a layer on another replica after this replica's cache was
// read; the conditional append catches up and nothing is approved. Then the
// guard on its own: a snapshot that shows a person's approval on a layer
// while the countdown still reads pending (a state the fold does not
// produce) gets no policy approval.
func TestAutoApprovePersonDecidedLayerInBetween(t *testing.T) {
	ctx := context.Background()
	t.Run("on another replica", func(t *testing.T) {
		c := &testClock{t: aaT0}
		ms := memstore.NewWithClock(c.Now)
		acts := newAAActions(t)
		setup := newAARig(t, ms, c, acts, aaOpts{})
		roomID := setup.open()
		pid := setup.propose(roomID, "", "connect_restart_task", "consumer_restart")
		hash := setup.proposal(roomID, pid).ParamsHash
		setup.svc.Close()

		fc := &aaCoord{fakeCoord: &fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}}
		mk := func(id string) *Service {
			svc := New(Deps{Store: fc, Actions: acts.reg, Users: DefaultUsers(), Owners: aaOwners(), Run: idleRun, Now: c.Now,
				ReplicaID: id, AutoApproveDelay: aaDelay})
			t.Cleanup(svc.Close)
			mustRoom(t, svc, roomID)
			return svc
		}
		stale, other := mk("replica-a"), mk("replica-b")
		kabir, _ := other.LookupUser("kabir")
		if _, err := other.ApproveProposal(ctx, kabir, pid, "consumer", hash); err != nil {
			t.Fatal(err)
		}
		c.Advance(aaDelay)
		stale.firePolicy(ctx, roomID, pid) // its cache misses kabir's approval
		if n := len(policyApprovals(t, ms, roomID)); n != 0 {
			t.Fatalf("%d policy approvals after a person decided a layer, want 0", n)
		}
		apps, err := ms.ListApprovals(ctx, pid)
		if err != nil {
			t.Fatal(err)
		}
		if len(apps) != 1 || apps[0].Layer != "consumer" {
			t.Fatalf("approvals rows = %+v, want kabir's only", apps)
		}
	})
	t.Run("guard alone", func(t *testing.T) {
		c := &testClock{t: aaT0}
		ms := memstore.NewWithClock(c.Now)
		logs := &aaLogs{}
		r := newAARig(t, ms, c, newAAActions(t), aaOpts{log: slog.New(logs)})
		roomID := r.open()
		pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")
		st := r.svc.lockCachedRoom(roomID)
		st.snap.OpenProposals = cloneProposals(st.snap.OpenProposals)
		for i := range st.snap.OpenProposals {
			if st.snap.OpenProposals[i].ProposalID == pid {
				st.snap.OpenProposals[i].Approvals["consumer"] = "approved"
			}
		}
		st.changed = true
		r.svc.unlockRoom(st)
		c.Advance(aaDelay)
		r.tick()
		r.tick()
		if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
			t.Fatalf("%d approvals with a layer a person approved, want 0", n)
		}
		if n := logs.count("does not approve", "approved by a person"); n != 1 {
			t.Fatalf("refusal logged %d times, want 1", n)
		}
		// Any other decision on a layer is refused the same way.
		st = r.svc.lockCachedRoom(roomID)
		st.snap.OpenProposals = cloneProposals(st.snap.OpenProposals)
		st.snap.OpenProposals[0].Approvals["consumer"] = "rejected"
		_, err := r.svc.fireCheckLocked(st, pid)
		var refused *policyRefusal
		if !errors.As(err, &refused) || !strings.Contains(refused.why, "decided by a person") {
			t.Fatalf("fire check with a rejected layer = %v", err)
		}
		r.svc.unlockRoom(st)
	})
}

// The schedule rule wants every layer pending: a proposal a person already
// decided on gets no countdown.
func TestAutoApproveScheduleNeedsEveryLayerPending(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")
	st := r.svc.lockCachedRoom(roomID)
	defer r.svc.unlockRoom(st)
	saved := st.snap
	st.snap.OpenProposals = cloneProposals(saved.OpenProposals)
	st.snap.OpenProposals[0].sched = schedState{} // as before proposal.scheduled
	if err := r.svc.scheduleCheckLocked(st, pid); err != nil {
		t.Fatalf("schedule check with every layer pending = %v, want nil", err)
	}
	st.snap.OpenProposals[0].Approvals["consumer"] = "approved"
	if err := r.svc.scheduleCheckLocked(st, pid); !errors.Is(err, errNotAutoApprovable) {
		t.Fatalf("schedule check with a decided layer = %v, want refused", err)
	}
	st.snap = saved
}

// Review item 3: an approvals row with no approval.given behind it (a lost
// compensation) refuses every policy append. The loop logs it once, not
// once a second; the note is dropped when the proposal leaves the open list
// (item 5).
func TestAutoApproveOrphanRowLoggedOnce(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	logs := &aaLogs{}
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{log: slog.New(logs)})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")
	hash := r.proposal(roomID, pid).ParamsHash
	if err := ms.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "kafka-connect", Decision: "approved", ParamsHash: hash, IdentityMethod: "dev"}); err != nil {
		t.Fatal(err)
	}
	c.Advance(aaDelay)
	for i := 0; i < 5; i++ {
		r.tick()
	}
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals, want 0", n)
	}
	if n := logs.count("decided elsewhere", "proposal="+pid); n != 1 {
		t.Fatalf("logged %d times over 5 checks, want 1", n)
	}
	if r.autoNotes() != 1 {
		t.Fatalf("notes = %d, want 1", r.autoNotes())
	}
	// kabir rejects: the proposal leaves the open list and its note goes.
	if _, err := r.svc.RejectProposal(ctx, r.user("kabir"), pid, "consumer", "not now", hash); err != nil {
		t.Fatal(err)
	}
	r.tick()
	if r.autoNotes() != 0 {
		t.Fatalf("notes after the proposal left the open list = %d, want 0", r.autoNotes())
	}
}

// failApprovals fails RecordApproval while fail is set: a store that is down.
type failApprovals struct {
	*memstore.Store
	fail atomic.Bool
}

func (f *failApprovals) RecordApproval(ctx context.Context, a store.ApprovalRow) error {
	if f.fail.Load() {
		return errors.New("db down")
	}
	return f.Store.RecordApproval(ctx, a)
}

// Review item 3, store errors: a store that keeps failing is logged once;
// once it answers again the next check approves. Closing the room drops the
// notes of its proposals (item 5).
func TestAutoApproveStoreErrorLoggedOnce(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: aaT0}
	fs := &failApprovals{Store: memstore.NewWithClock(c.Now)}
	logs := &aaLogs{}
	r := newAARig(t, fs, c, newAAActions(t), aaOpts{log: slog.New(logs)})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task")
	fs.fail.Store(true)
	c.Advance(aaDelay)
	for i := 0; i < 5; i++ {
		r.tick()
	}
	if n := logs.count("store call failed", "proposal="+pid, "db down"); n != 1 {
		t.Fatalf("store failure logged %d times over 5 checks, want 1", n)
	}
	if n := len(logOf(t, fs, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals while the store fails, want 0", n)
	}
	fs.fail.Store(false)
	r.tick()
	if n := len(policyApprovals(t, fs, roomID)); n != 1 {
		t.Fatalf("%d policy approvals once the store answers, want 1", n)
	}

	// A second proposal whose refusal is noted, then the room closes.
	logs2 := &aaLogs{}
	r2 := newAARig(t, memstore.NewWithClock(c.Now), c, newAAActions(t), aaOpts{log: slog.New(logs2)})
	room2 := r2.open()
	pid2 := r2.propose(room2, "", "connect_restart_task")
	emitEvent(t, r2.svc, room2, systemActor, events.ProposalPreviewed, events.ProposalPreviewedPayload{
		ProposalID: pid2, Steps: []events.StepPreview{{StepIndex: 0, Error: "connect down"}}, At: c.Now(),
	})
	c.Advance(aaDelay)
	r2.tick()
	if r2.autoNotes() != 1 {
		t.Fatalf("notes = %d, want 1", r2.autoNotes())
	}
	if _, err := r2.svc.CloseRoom(ctx, r2.user("arjun"), room2, "fixed by hand"); err != nil {
		t.Fatal(err)
	}
	r2.tick()
	if r2.autoNotes() != 0 {
		t.Fatalf("notes after the room closed = %d, want 0", r2.autoNotes())
	}
}
