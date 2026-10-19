package room

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// P2b-4b: a countdown that will never fire leaves the caller's view once
// run_at is StaleCountdownGrace in the past. The log, the published snapshot
// and mode.auto_approve do not change, the proposal stays approvable by hand,
// and a countdown that is still cancellable in the log is cancelled whether
// the view shows it or not.

// viewOf is RoomFor for arjun (the driver, owner of kafka-connect).
func (r *aaRig) viewOf(roomID string) RoomSnapshot {
	r.t.Helper()
	v, err := r.svc.RoomFor(context.Background(), r.user("arjun"), roomID)
	if err != nil {
		r.t.Fatal(err)
	}
	return v
}

func viewProposal(t *testing.T, v RoomSnapshot, pid string) Proposal {
	t.Helper()
	for _, p := range v.OpenProposals {
		if p.ProposalID == pid {
			return p
		}
	}
	t.Fatalf("proposal %s not in the view", pid)
	return Proposal{}
}

// viewKeys returns the JSON member names of the view and of its first open
// proposal.
func viewKeys(t *testing.T, v RoomSnapshot) (top, prop []string) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	var ps []map[string]json.RawMessage
	if err := json.Unmarshal(m["open_proposals"], &ps); err != nil || len(ps) == 0 {
		t.Fatalf("open_proposals = %s (%v)", m["open_proposals"], err)
	}
	for k := range m {
		top = append(top, k)
	}
	for k := range ps[0] {
		prop = append(prop, k)
	}
	slices.Sort(top)
	slices.Sort(prop)
	return top, prop
}

func eventCount(t *testing.T, st store.Store, roomID string) int {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return len(evs)
}

func TestStaleCountdownGraceValue(t *testing.T) {
	// It must outlast a crashed lease holder's takeover plus two loop ticks
	// (docs/design/replicas.md), and stay short enough that a dead countdown
	// does not linger.
	if floor := DefaultLeaseTTL + DefaultTakeoverScan + 2*AutoApproveCheckEvery; StaleCountdownGrace <= floor {
		t.Fatalf("grace %v does not outlast lease TTL + takeover scan + 2 ticks (%v)", StaleCountdownGrace, floor)
	}
	if StaleCountdownGrace > time.Minute {
		t.Fatalf("grace %v is over a minute", StaleCountdownGrace)
	}
}

// The fire check refuses the countdown (the preview turned bad after it was
// scheduled): shown before run_at and within the grace, hidden after it, a
// person approves and runs it.
func TestStaleCountdownHiddenWhenPolicyRefuses(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	ctx := context.Background()
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task")
	runAt := aaT0.Add(aaDelay)

	// Before run_at.
	before := r.viewOf(roomID)
	if sc := viewProposal(t, before, pid).Scheduled; sc == nil || !sc.RunAt.Equal(runAt) {
		t.Fatalf("scheduled before run_at = %+v, want run_at %v", sc, runAt)
	}
	topBefore, propBefore := viewKeys(t, before)
	if !slices.Contains(propBefore, "scheduled") {
		t.Fatalf("proposal members before run_at = %v, want scheduled", propBefore)
	}

	emitEvent(t, r.svc, roomID, systemActor, events.ProposalPreviewed, events.ProposalPreviewedPayload{
		ProposalID: pid, Steps: []events.StepPreview{{StepIndex: 0, Error: "connect down"}}, At: aaT0,
	})

	// Just after run_at: the policy refuses, the countdown is still shown.
	c.Advance(aaDelay + time.Second)
	r.tick()
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals after the policy refused, want 0", n)
	}
	if sc := viewProposal(t, r.viewOf(roomID), pid).Scheduled; sc == nil {
		t.Fatal("scheduled gone 1s after run_at, within the grace")
	}
	// Exactly run_at plus the grace: still shown (more than the grace hides).
	c.Advance(StaleCountdownGrace - time.Second)
	if sc := viewProposal(t, r.viewOf(roomID), pid).Scheduled; sc == nil {
		t.Fatal("scheduled gone at run_at + grace")
	}

	// Past the grace: hidden from the view only.
	logLen := eventCount(t, ms, roomID)
	c.Advance(time.Millisecond)
	after := r.viewOf(roomID)
	if sc := viewProposal(t, after, pid).Scheduled; sc != nil {
		t.Fatalf("scheduled past the grace = %+v, want none", sc)
	}
	topAfter, propAfter := viewKeys(t, after)
	wantProp := slices.DeleteFunc(slices.Clone(propBefore), func(k string) bool { return k == "scheduled" })
	if !slices.Equal(topAfter, topBefore) || !slices.Equal(propAfter, wantProp) {
		t.Fatalf("view members changed beyond scheduled:\n top %v -> %v\n proposal %v -> %v", topBefore, topAfter, propBefore, propAfter)
	}
	if !slices.Equal(after.Mode.AutoApprove, before.Mode.AutoApprove) || len(after.Mode.AutoApprove) == 0 {
		t.Fatalf("mode.auto_approve = %v, was %v", after.Mode.AutoApprove, before.Mode.AutoApprove)
	}
	if p, _ := openProposal(mustRoom(t, r.svc, roomID), pid); p.Scheduled == nil || !p.sched.pending() {
		t.Fatalf("published snapshot lost the countdown: %+v", p.Scheduled)
	}
	if n := eventCount(t, ms, roomID); n != logLen {
		t.Fatalf("log grew from %d to %d by hiding the countdown", logLen, n)
	}

	// Still open and approvable by hand.
	p := viewProposal(t, r.viewOf(roomID), pid)
	if p.Approvals["kafka-connect"] != "pending" {
		t.Fatalf("approvals = %v, want pending", p.Approvals)
	}
	ev, err := r.svc.ApproveProposal(ctx, r.user("arjun"), pid, "kafka-connect", p.ParamsHash)
	if err != nil {
		t.Fatalf("manual approve of a hidden countdown: %v", err)
	}
	if ap, _ := events.Decode[events.ApprovalGivenPayload](ev); ap.Grant == events.GrantPolicy {
		t.Fatalf("manual approval has grant %q", ap.Grant)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.acts.restart.count() == 1 }, "manual approval did not run the step")
	if n := len(policyApprovals(t, ms, roomID)); n != 0 {
		t.Fatalf("%d policy approvals, want 0", n)
	}
}

// A countdown hidden past the grace is still cancellable in the log, so the
// cancel is taken whether the policy on this replica could still fire it (the
// loop was stuck) or refuses it (the preview turned bad): another replica's
// config decides that, not this one's. The policy then never fires it, and a
// second cancel answers 409 not_scheduled.
func TestStaleCountdownCancelAlwaysTaken(t *testing.T) {
	for _, refuses := range []bool{false, true} {
		name := "loop stuck"
		if refuses {
			name = "policy refuses"
		}
		t.Run(name, func(t *testing.T) {
			c := &testClock{t: aaT0}
			ms := memstore.NewWithClock(c.Now)
			r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
			ctx := context.Background()
			roomID := r.open()
			pid := r.propose(roomID, "", "connect_restart_task")
			if refuses {
				emitEvent(t, r.svc, roomID, systemActor, events.ProposalPreviewed, events.ProposalPreviewedPayload{
					ProposalID: pid, Steps: []events.StepPreview{{StepIndex: 0, Error: "connect down"}}, At: aaT0,
				})
			}

			c.Advance(aaDelay + StaleCountdownGrace + time.Second)
			if refuses {
				r.tick() // refused: nothing approved
			}
			if sc := viewProposal(t, r.viewOf(roomID), pid).Scheduled; sc != nil {
				t.Fatalf("scheduled past the grace = %+v, want none", sc)
			}
			ev, err := r.svc.CancelProposal(ctx, r.user("arjun"), pid, "stop it")
			if err != nil {
				t.Fatalf("cancel of a hidden but cancellable countdown = %v, want taken", err)
			}
			if ev.Type != events.ProposalCancelled {
				t.Fatalf("cancel event = %s", ev.Type)
			}
			r.tick()
			if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 || r.acts.restart.count() != 0 {
				t.Fatalf("%d approvals, %d runs after the cancel, want 0", n, r.acts.restart.count())
			}
			if p := viewProposal(t, r.viewOf(roomID), pid); p.Scheduled != nil || p.Approvals["kafka-connect"] != "pending" {
				t.Fatalf("after the cancel: scheduled %+v, approvals %v; want none, pending", p.Scheduled, p.Approvals)
			}
			if _, err := r.svc.CancelProposal(ctx, r.user("arjun"), pid, ""); !isCode(err, "not_scheduled") {
				t.Fatalf("second cancel = %v, want 409 not_scheduled", err)
			}
		})
	}
}

// A fire within the grace and a late fire past it both approve and run: the
// grace changes the view, never the policy.
func TestStaleCountdownFireStillRuns(t *testing.T) {
	for _, late := range []time.Duration{StaleCountdownGrace / 2, StaleCountdownGrace + time.Minute} {
		t.Run(late.String(), func(t *testing.T) {
			c := &testClock{t: aaT0}
			ms := memstore.NewWithClock(c.Now)
			r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
			roomID := r.open()
			pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")
			c.Advance(aaDelay + late)
			r.tick()
			if n := len(policyApprovals(t, ms, roomID)); n != 2 {
				t.Fatalf("%d policy approvals, want 2", n)
			}
			waitForCond(t, 5*time.Second, func() bool {
				return r.acts.restart.count() == 1 && r.acts.consumer.count() == 1
			}, "the steps never ran after the policy approvals")
			if _, ok := openProposal(mustRoom(t, r.svc, roomID), pid); ok {
				if sc := viewProposal(t, r.viewOf(roomID), pid).Scheduled; sc != nil {
					t.Fatalf("scheduled after the fire = %+v", sc)
				}
			}
		})
	}
}
