package room

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Auto-approve policy (P2b-4): proposal.scheduled after a passing preview,
// the cancellable countdown, policy approvals at run_at, and the fire path's
// re-checks across restarts and replicas. Every test drives the clock and
// the loop's ticker by hand.

var aaT0 = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

const aaDelay = 2 * time.Second

// aaAct is a fake action with a preview the test sets: a preview error, a
// dry run that says no, or none at all (noPreview).
type aaAct struct {
	name, layer string
	previewErr  error
	dryRunNo    bool
	mu          sync.Mutex
	runs        int
}

func (a *aaAct) Name() string                                      { return a.name }
func (a *aaAct) Layer() string                                     { return a.layer }
func (a *aaAct) Describe() string                                  { return a.name }
func (a *aaAct) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (a *aaAct) Validate(context.Context, json.RawMessage) error   { return nil }
func (a *aaAct) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *aaAct) Execute(context.Context, json.RawMessage) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs++
	return "done", nil
}
func (a *aaAct) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	if a.previewErr != nil {
		return actions.PreviewResult{}, a.previewErr
	}
	res := actions.PreviewResult{Target: a.name, Current: "failed", Expected: "running"}
	if a.dryRunNo {
		res.DryRun = &events.DryRunResult{OK: false, Source: "connect config validate", Text: "no"}
	}
	return res, nil
}

func (a *aaAct) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

// aaNoPreview is an action without actions.Preview: its step's preview is
// the error "no preview for this action", so the policy never schedules it.
type aaNoPreview struct{ aaAct }

func (*aaNoPreview) Preview() {} // shadows the embedded method: not actions.Preview

// aaActions builds the registry: two policy actions on two layers, one with
// a failing preview, one whose dry run says no, one with no preview and a
// schema action.
type aaActions struct {
	restart, consumer, badPreview, dryNo, schema *aaAct
	noPreview                                    *aaNoPreview
	reg                                          *actions.Registry
}

func newAAActions(t *testing.T) *aaActions {
	t.Helper()
	a := &aaActions{
		restart:    &aaAct{name: "connect_restart_task", layer: "kafka-connect"},
		consumer:   &aaAct{name: "consumer_restart", layer: "consumer"},
		badPreview: &aaAct{name: "connect_bad_preview", layer: "kafka-connect", previewErr: errors.New("connect down")},
		dryNo:      &aaAct{name: "connect_dry_no", layer: "kafka-connect", dryRunNo: true},
		schema:     &aaAct{name: "schema_set_compat", layer: "schema-registry"},
		noPreview:  &aaNoPreview{aaAct{name: "connect_no_preview", layer: "kafka-connect"}},
		reg:        actions.NewRegistry(),
	}
	for _, act := range []actions.Action{a.restart, a.consumer, a.badPreview, a.dryNo, a.schema, a.noPreview} {
		if err := a.reg.Register(act); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

// aaOwners lists every fake action on orders' auto_approve: list; the
// layers' owners come from DefaultUsers (users.yaml layers).
func aaOwners(list ...string) *owners.Owners {
	if list == nil {
		list = []string{"connect_restart_task", "consumer_restart", "connect_bad_preview", "connect_dry_no", "schema_set_compat", "connect_no_preview"}
	}
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}, AutoApprove: list}}}
}

type aaRig struct {
	t     *testing.T
	svc   *Service
	st    store.Store
	clock *testClock
	acts  *aaActions
	ticks chan time.Time
	done  chan struct{}
	stops atomic.Int32
}

type aaOpts struct {
	own      *owners.Owners
	writeOff bool
	delay    time.Duration
	solo     string
}

func newAARig(t *testing.T, st store.Store, c *testClock, acts *aaActions, o aaOpts) *aaRig {
	t.Helper()
	if o.own == nil {
		o.own = aaOwners()
	}
	if o.delay == 0 {
		o.delay = aaDelay
	} else if o.delay < 0 {
		o.delay = 0
	}
	r := &aaRig{t: t, st: st, clock: c, acts: acts, ticks: make(chan time.Time), done: make(chan struct{})}
	r.svc = New(Deps{
		Store: st, Actions: acts.reg, Users: DefaultUsers(), Owners: o.own, Run: idleRun, Now: c.Now,
		WriteModeOff: o.writeOff, AutoApproveDelay: o.delay, SoloUser: o.solo,
		AutoApproveTicker: func(time.Duration) (<-chan time.Time, func()) {
			return r.ticks, func() { r.stops.Add(1) }
		},
	})
	r.svc.autoTickDone = func() { r.done <- struct{}{} }
	t.Cleanup(r.svc.Close)
	if err := r.svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

// tick runs one check of the policy loop and returns when it has finished.
func (r *aaRig) tick() {
	r.t.Helper()
	select {
	case r.ticks <- time.Time{}:
	case <-time.After(5 * time.Second):
		r.t.Fatal("policy loop not running")
	}
	<-r.done
}

func (r *aaRig) user(id string) User {
	r.t.Helper()
	u, ok := r.svc.LookupUser(id)
	if !ok {
		r.t.Fatalf("no user %s", id)
	}
	return u
}

// open opens a room driven by arjun.
func (r *aaRig) open() string {
	r.t.Helper()
	snap, err := r.svc.CreateRoom(context.Background(), r.user("arjun"), "Incident", "sev2", "orders")
	if err != nil {
		r.t.Fatal(err)
	}
	return snap.ID
}

// propose creates an agent proposal with one step per action and waits for
// its preview to be folded (and, when it qualifies, its countdown).
func (r *aaRig) propose(roomID, origin string, acts ...string) string {
	r.t.Helper()
	var steps []events.ProposalStep
	for _, name := range acts {
		a, ok := r.acts.reg.Get(name)
		if !ok {
			r.t.Fatalf("no action %s", name)
		}
		steps = append(steps, events.ProposalStep{Action: name, Layer: a.Layer(), Params: json.RawMessage(`{}`)})
	}
	pid, err := r.svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "restart again", Steps: steps, Origin: origin,
	}, events.AgentActor)
	if err != nil {
		r.t.Fatal(err)
	}
	waitForCond(r.t, 5*time.Second, func() bool {
		p, ok := openProposal(mustRoom(r.t, r.svc, roomID), pid)
		return ok && p.Preview != nil
	}, "preview never folded")
	// The countdown is appended under the same lock hold as the preview.
	return pid
}

func (r *aaRig) proposal(roomID, pid string) Proposal {
	r.t.Helper()
	p, ok := openProposal(mustRoom(r.t, r.svc, roomID), pid)
	if !ok {
		r.t.Fatalf("proposal %s not open", pid)
	}
	return p
}

// logOf returns the room's events of the given types.
func logOf(t *testing.T, st store.Store, roomID string, types ...events.Type) []events.Event {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []events.Event
	for _, e := range evs {
		if slices.Contains(types, e.Type) {
			out = append(out, e)
		}
	}
	return out
}

func policyApprovals(t *testing.T, st store.Store, roomID string) []events.ApprovalGivenPayload {
	t.Helper()
	var out []events.ApprovalGivenPayload
	for _, e := range logOf(t, st, roomID, events.ApprovalGiven) {
		p, err := events.Decode[events.ApprovalGivenPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		if p.Grant == events.GrantPolicy {
			if e.Actor != systemActor || p.Identity != nil || p.Policy != events.PolicyAutoApprove {
				t.Fatalf("policy approval %+v by %+v: want the system actor, no identity, policy auto_approve", p, e.Actor)
			}
			out = append(out, p)
		}
	}
	return out
}

func TestAutoApproveRunsAfterDelayWithPolicyGrant(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")

	sched := logOf(t, ms, roomID, events.ProposalScheduled, events.ProposalPreviewed)
	if len(sched) != 2 || sched[0].Type != events.ProposalPreviewed || sched[1].Type != events.ProposalScheduled {
		t.Fatalf("log = %v, want proposal.previewed then proposal.scheduled", sched)
	}
	if sched[1].Actor != systemActor {
		t.Fatalf("proposal.scheduled actor = %+v, want system", sched[1].Actor)
	}
	sp, _ := events.Decode[events.ProposalScheduledPayload](sched[1])
	want := aaT0.Add(aaDelay)
	if sp.ProposalID != pid || !sp.RunAt.Equal(want) || !sp.CancelUntil.Equal(sp.RunAt) || sp.Policy != events.PolicyAutoApprove {
		t.Fatalf("proposal.scheduled = %+v, want run_at = cancel_until = %v, policy auto_approve", sp, want)
	}
	p := r.proposal(roomID, pid)
	if p.Scheduled == nil || !p.Scheduled.RunAt.Equal(want) || !p.Scheduled.CancelUntil.Equal(want) {
		t.Fatalf("snapshot scheduled = %+v", p.Scheduled)
	}

	// One second in: not due.
	c.Advance(time.Second)
	r.tick()
	if n := len(policyApprovals(t, ms, roomID)); n != 0 {
		t.Fatalf("%d policy approvals before run_at", n)
	}
	// At run_at: one policy approval per layer in required order, then the run.
	c.Advance(time.Second)
	r.tick()
	got := policyApprovals(t, ms, roomID)
	if len(got) != 2 || got[0].Layer != "kafka-connect" || got[1].Layer != "consumer" {
		t.Fatalf("policy approvals = %+v, want kafka-connect then consumer", got)
	}
	for _, a := range got {
		if a.ParamsHash != p.ParamsHash || a.ProposalID != pid {
			t.Fatalf("approval %+v: want the proposal's params_hash", a)
		}
	}
	waitForCond(t, 5*time.Second, func() bool {
		return len(logOf(t, ms, roomID, events.ActionExecuted)) == 2
	}, "the steps never ran after the policy approvals")
	if r.acts.restart.count() != 1 || r.acts.consumer.count() != 1 {
		t.Fatalf("runs = %d, %d; want 1 each", r.acts.restart.count(), r.acts.consumer.count())
	}
	if n := r.svc.ApprovalsByGrant()[events.GrantPolicy]; n != 2 {
		t.Fatalf("piperoom_approvals_total{grant=policy} = %d, want 2", n)
	}
	// Later checks do nothing more.
	c.Advance(time.Minute)
	r.tick()
	if n := len(policyApprovals(t, ms, roomID)); n != 2 {
		t.Fatalf("%d policy approvals after a later check, want 2", n)
	}
}

func TestAutoApproveNeverScheduled(t *testing.T) {
	cases := []struct {
		name   string
		opts   aaOpts
		origin string
		acts   []string
	}{
		{name: "action not on the list", opts: aaOpts{own: aaOwners("consumer_restart")}, acts: []string{"connect_restart_task", "consumer_restart"}},
		{name: "empty list", opts: aaOpts{own: aaOwners([]string{}...)}, acts: []string{"consumer_restart"}},
		{name: "failed preview", acts: []string{"connect_bad_preview"}},
		{name: "dry run says no", acts: []string{"connect_dry_no"}},
		{name: "no preview", acts: []string{"connect_no_preview"}},
		{name: "schema action", acts: []string{"schema_set_compat"}},
		{name: "write mode off", opts: aaOpts{writeOff: true}, acts: []string{"connect_restart_task"}},
		{name: "delay 0", opts: aaOpts{delay: -1}, acts: []string{"connect_restart_task"}},
		{name: "driver-authored", origin: events.ProposalOriginDriver, acts: []string{"connect_restart_task"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &testClock{t: aaT0}
			ms := memstore.NewWithClock(c.Now)
			r := newAARig(t, ms, c, newAAActions(t), tc.opts)
			roomID := r.open()
			pid := r.propose(roomID, tc.origin, tc.acts...)
			if n := len(logOf(t, ms, roomID, events.ProposalScheduled)); n != 0 {
				t.Fatalf("%d proposal.scheduled, want 0", n)
			}
			if p := r.proposal(roomID, pid); p.Scheduled != nil {
				t.Fatalf("snapshot scheduled = %+v, want none", p.Scheduled)
			}
			c.Advance(time.Hour)
			if r.svc.autoDelay > 0 {
				r.tick()
			}
			if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
				t.Fatalf("%d approvals, want 0", n)
			}
		})
	}
}

// TestAutoApproveNotForShadowRoom: a shadow room has no proposals in its log
// (the agent's go to the shadow log), and the schedule rule refuses one
// anyway.
func TestAutoApproveNotForShadowRoom(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task")
	// The countdown exists for the live room; a shadow room with the same
	// proposal would not get one.
	st := r.svc.lockCachedRoom(roomID)
	defer r.svc.unlockRoom(st)
	saved := st.snap
	st.snap.Shadow = true
	st.snap.OpenProposals = cloneProposals(saved.OpenProposals)
	for i := range st.snap.OpenProposals {
		st.snap.OpenProposals[i].sched = schedState{}
	}
	if err := r.svc.scheduleCheckLocked(st, pid); !errors.Is(err, errNotAutoApprovable) {
		t.Fatalf("schedule check in a shadow room = %v, want refused", err)
	}
	st.snap = saved
}

func TestAutoApproveCancelBeforeFire(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	ctx := context.Background()
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")

	// priya owns neither layer and does not drive.
	if _, err := r.svc.CancelProposal(ctx, r.user("priya"), pid, ""); !isCode(err, codeNotLayerOwner) {
		t.Fatalf("cancel by a non-owner = %v, want 403 not_layer_owner", err)
	}
	if _, err := r.svc.CancelProposal(ctx, r.user("arjun"), pid, strings.Repeat("x", 501)); !isCode(err, codeValidation) {
		t.Fatalf("cancel with a long reason = %v, want 400", err)
	}
	c.Advance(time.Second)
	// kabir owns consumer, a required layer, and does not drive.
	ev, err := r.svc.CancelProposal(ctx, r.user("kabir"), pid, "want to look first")
	if err != nil {
		t.Fatal(err)
	}
	cp, _ := events.Decode[events.ProposalCancelledPayload](ev)
	if ev.Type != events.ProposalCancelled || ev.Actor.ID != "kabir" || cp.By.ID != "kabir" || cp.ProposalID != pid || cp.Reason != "want to look first" {
		t.Fatalf("cancel event = %+v %+v", ev, cp)
	}
	if p := r.proposal(roomID, pid); p.Scheduled != nil {
		t.Fatalf("scheduled after cancel = %+v", p.Scheduled)
	}
	if _, err := r.svc.CancelProposal(ctx, r.user("arjun"), pid, ""); !isCode(err, "not_scheduled") {
		t.Fatalf("second cancel = %v, want 409 not_scheduled", err)
	}
	c.Advance(time.Minute)
	r.tick()
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals after cancel, want 0", n)
	}
	p := r.proposal(roomID, pid)
	if p.Approvals["kafka-connect"] != "pending" || p.Approvals["consumer"] != "pending" {
		t.Fatalf("approvals = %v, want pending for manual approval", p.Approvals)
	}
	// Still approvable by hand.
	if _, err := r.svc.ApproveProposal(ctx, r.user("arjun"), pid, "kafka-connect", p.ParamsHash); err != nil {
		t.Fatal(err)
	}

	// A proposal with no countdown: not_scheduled.
	pid2 := r.propose(roomID, "", "connect_bad_preview")
	if _, err := r.svc.CancelProposal(ctx, r.user("arjun"), pid2, ""); !isCode(err, "not_scheduled") {
		t.Fatalf("cancel of an unscheduled proposal = %v, want 409 not_scheduled", err)
	}
}

func TestAutoApproveCancelAfterPolicyRan(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task")
	c.Advance(aaDelay)
	r.tick()
	if _, err := r.svc.CancelProposal(context.Background(), r.user("arjun"), pid, ""); !isCode(err, "not_scheduled") {
		t.Fatalf("cancel after the policy ran = %v, want 409 not_scheduled", err)
	}
}

// TestAutoApproveStoppedByPersonsApproval: a person's approve (or
// approve-all) before run_at stops the countdown without proposal.cancelled.
func TestAutoApproveStoppedByPersonsApproval(t *testing.T) {
	for _, all := range []bool{false, true} {
		name := "approve"
		if all {
			name = "approve-all"
		}
		t.Run(name, func(t *testing.T) {
			c := &testClock{t: aaT0}
			ms := memstore.NewWithClock(c.Now)
			r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
			ctx := context.Background()
			roomID := r.open()
			var pid string
			if all {
				// arjun owns kafka-connect, the one required layer.
				pid = r.propose(roomID, "", "connect_restart_task")
				if _, err := r.svc.ApproveAllProposal(ctx, r.user("arjun"), pid, r.proposal(roomID, pid).ParamsHash, ""); err != nil {
					t.Fatal(err)
				}
				waitForCond(t, 5*time.Second, func() bool { return r.acts.restart.count() == 1 }, "approve-all did not run the step")
			} else {
				pid = r.propose(roomID, "", "connect_restart_task", "consumer_restart")
				if _, err := r.svc.ApproveProposal(ctx, r.user("kabir"), pid, "consumer", r.proposal(roomID, pid).ParamsHash); err != nil {
					t.Fatal(err)
				}
				if p := r.proposal(roomID, pid); p.Scheduled != nil || p.sched.pending() {
					t.Fatalf("after a person's approval: scheduled %+v, pending %v", p.Scheduled, p.sched.pending())
				}
			}
			c.Advance(time.Minute)
			r.tick()
			if n := len(policyApprovals(t, ms, roomID)); n != 0 {
				t.Fatalf("%d policy approvals after a person's approval, want 0", n)
			}
			if n := len(logOf(t, ms, roomID, events.ProposalCancelled)); n != 0 {
				t.Fatalf("%d proposal.cancelled, want 0", n)
			}
			if !all {
				if p := r.proposal(roomID, pid); p.Approvals["kafka-connect"] != "pending" {
					t.Fatalf("kafka-connect = %q, want pending for its owner", p.Approvals["kafka-connect"])
				}
			}
		})
	}
}

// hashStore changes the stored proposal under the countdown: GetProposal
// answers with other steps (or another hash) than proposal.created froze.
type hashStore struct {
	*memstore.Store
	mu     sync.Mutex
	change func(*store.ProposalRow)
}

func (h *hashStore) GetProposal(ctx context.Context, id string) (store.ProposalRow, error) {
	row, err := h.Store.GetProposal(ctx, id)
	h.mu.Lock()
	defer h.mu.Unlock()
	if err == nil && h.change != nil {
		h.change(&row)
	}
	return row, err
}

func TestAutoApproveHashChangedBeforeFire(t *testing.T) {
	for name, change := range map[string]func(*store.ProposalRow){
		"hash": func(r *store.ProposalRow) { r.ParamsHash = strings.Repeat("0", 64) },
		"steps": func(r *store.ProposalRow) {
			r.Steps = json.RawMessage(`[{"action":"connect_restart_task","layer":"kafka-connect","params":{"task":9}}]`)
		},
		"status": func(r *store.ProposalRow) { r.Status = "rejected" },
	} {
		t.Run(name, func(t *testing.T) {
			c := &testClock{t: aaT0}
			hs := &hashStore{Store: memstore.NewWithClock(c.Now)}
			r := newAARig(t, hs, c, newAAActions(t), aaOpts{})
			roomID := r.open()
			pid := r.propose(roomID, "", "connect_restart_task")
			hs.mu.Lock()
			hs.change = change
			hs.mu.Unlock()
			c.Advance(aaDelay)
			r.tick()
			r.tick()
			if n := len(logOf(t, hs, roomID, events.ApprovalGiven)); n != 0 {
				t.Fatalf("%d approvals with a changed proposal, want 0", n)
			}
			if r.acts.restart.count() != 0 {
				t.Fatal("the step ran")
			}
			if _, ok := openProposal(mustRoom(t, r.svc, roomID), pid); !ok {
				t.Fatal("proposal no longer open")
			}
		})
	}
}

// TestAutoApproveSurvivesRestart: the countdown is in the log, so a new
// process over the same store fires it, once.
func TestAutoApproveSurvivesRestart(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	a := newAARig(t, ms, c, acts, aaOpts{})
	roomID := a.open()
	pid := a.propose(roomID, "", "connect_restart_task", "consumer_restart")
	a.svc.Close()
	if n := a.stops.Load(); n != 1 {
		t.Fatalf("ticker stopped %d times at Close, want 1", n)
	}

	b := newAARig(t, ms, c, acts, aaOpts{})
	if p := b.proposal(roomID, pid); p.Scheduled == nil {
		t.Fatal("the restarted process lost the countdown")
	}
	c.Advance(aaDelay)
	b.tick()
	b.tick()
	if n := len(policyApprovals(t, ms, roomID)); n != 2 {
		t.Fatalf("%d policy approvals after restart, want 2", n)
	}
	waitForCond(t, 5*time.Second, func() bool { return acts.restart.count() == 1 && acts.consumer.count() == 1 }, "steps did not run once")

	// Restarted again after the fire: nothing more.
	b.svc.Close()
	d := newAARig(t, ms, c, acts, aaOpts{})
	c.Advance(time.Minute)
	d.tick()
	if n := len(policyApprovals(t, ms, roomID)); n != 2 {
		t.Fatalf("%d policy approvals after a second restart, want 2", n)
	}
}

// TestAutoApprovePartialFireResumes: a process that stopped after the first
// policy approval leaves the rest to the next check, which approves only the
// layers still pending.
func TestAutoApprovePartialFireResumes(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	r := newAARig(t, ms, c, acts, aaOpts{})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task", "consumer_restart")
	c.Advance(aaDelay)
	// The first layer, as a fire that stopped there would leave it.
	st := r.svc.lockCachedRoom(roomID)
	row, err := ms.GetProposal(context.Background(), pid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.svc.appendApprovalLocked(context.Background(), st, row, "kafka-connect", approver{actor: systemActor, detail: events.GrantPolicy}); err != nil {
		t.Fatal(err)
	}
	r.svc.unlockRoom(st)
	if p := r.proposal(roomID, pid); p.Scheduled != nil || !p.sched.pending() {
		t.Fatalf("after one policy approval: scheduled %+v, pending %v; want hidden and pending", p.Scheduled, p.sched.pending())
	}
	r.tick()
	got := policyApprovals(t, ms, roomID)
	if len(got) != 2 || got[1].Layer != "consumer" {
		t.Fatalf("policy approvals = %+v", got)
	}
	waitForCond(t, 5*time.Second, func() bool { return acts.consumer.count() == 1 }, "steps did not run")
}

func TestAutoApproveWriteModeOffApprovesButBlocks(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	a := newAARig(t, ms, c, acts, aaOpts{})
	roomID := a.open()
	pid := a.propose(roomID, "", "connect_restart_task")
	a.svc.Close()

	// The countdown was started with write mode on; this process has it off:
	// the policy still approves (P0 item 1) and nothing runs.
	b := newAARig(t, ms, c, acts, aaOpts{writeOff: true})
	c.Advance(aaDelay)
	b.tick()
	if n := len(policyApprovals(t, ms, roomID)); n != 1 {
		t.Fatalf("%d policy approvals with write mode off, want 1", n)
	}
	view, err := b.svc.RoomFor(context.Background(), b.user("arjun"), roomID)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := openProposal(view, pid)
	if !ok || p.Blocked != BlockedWriteModeOff || p.Scheduled != nil {
		t.Fatalf("view proposal = %+v (open %v), want blocked write_mode_off, not scheduled", p, ok)
	}
	if n := len(logOf(t, ms, roomID, events.ActionExecuted)); n != 0 || acts.restart.count() != 0 {
		t.Fatalf("%d action.executed, %d runs with write mode off", n, acts.restart.count())
	}
	row, _ := ms.GetProposal(context.Background(), pid)
	if row.Status != store.ProposalStatusBlocked {
		t.Fatalf("row status = %q, want blocked", row.Status)
	}
}

// aaCoord is fakeCoord (a memstore claiming to be shared) with the executor
// claim counted: it always answers "claimed elsewhere", so the test sees how
// many replicas handed the proposal to an executor without running one.
type aaCoord struct {
	*fakeCoord
	mu     sync.Mutex
	claims int
}

func (f *aaCoord) ClaimProposal(context.Context, string, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	return false, nil
}

func (f *aaCoord) AppendIfLast(ctx context.Context, roomID string, expectSeq int64, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fakeCoord.AppendIfLast(ctx, roomID, expectSeq, typ, actor, payload)
}

func (f *aaCoord) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims
}

// TestAutoApproveTwoReplicasFireOnce: two replicas over one store both find
// the countdown due; each layer is approved once and the proposal goes to
// one executor claim. A replica whose cache is behind fires nothing.
func TestAutoApproveTwoReplicasFireOnce(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	setup := newAARig(t, ms, c, acts, aaOpts{})
	roomID := setup.open()
	pid := setup.propose(roomID, "", "connect_restart_task", "consumer_restart")
	setup.svc.Close()

	fc := &aaCoord{fakeCoord: &fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}}
	mk := func(id string) *Service {
		svc := New(Deps{Store: fc, Actions: acts.reg, Users: DefaultUsers(), Owners: aaOwners(), Run: idleRun, Now: c.Now,
			ReplicaID: id, AutoApproveDelay: aaDelay})
		t.Cleanup(svc.Close)
		mustRoom(t, svc, roomID)
		return svc
	}
	r1, r2, late := mk("replica-a"), mk("replica-b"), mk("replica-c")
	c.Advance(aaDelay)

	var wg sync.WaitGroup
	for _, svc := range []*Service{r1, r2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.checkSchedules(ctx)
		}()
	}
	wg.Wait()
	// Whichever lost the race may have left a layer to the next check.
	r1.checkSchedules(ctx)
	r2.checkSchedules(ctx)

	got := policyApprovals(t, ms, roomID)
	if len(got) != 2 || got[0].Layer != "kafka-connect" || got[1].Layer != "consumer" {
		t.Fatalf("policy approvals = %+v, want one per layer in order", got)
	}
	if n := fc.claimCount(); n != 1 {
		t.Fatalf("executor claims = %d, want 1", n)
	}
	// late's cache still shows the countdown pending; its fire is refused by
	// the approvals rows (one decision per layer).
	late.firePolicy(ctx, roomID, pid)
	if n := len(policyApprovals(t, ms, roomID)); n != 2 || fc.claimCount() != 1 {
		t.Fatalf("after a stale replica's fire: %d approvals, %d claims; want 2 and 1", n, fc.claimCount())
	}

	// A room whose agent lease another replica holds is left to it.
	setup2 := newAARig(t, ms, c, acts, aaOpts{})
	room2 := setup2.open()
	setup2.propose(room2, "", "connect_restart_task")
	setup2.svc.Close()
	fc.leases = map[string]store.RoomLease{room2: {RoomID: room2, Owner: "replica-b"}}
	mustRoom(t, r1, room2)
	c.Advance(aaDelay)
	r1.checkSchedules(ctx)
	if n := len(policyApprovals(t, ms, room2)); n != 0 {
		t.Fatalf("replica-a fired a room replica-b holds: %d approvals", n)
	}
}

// TestAutoApproveCancelRacesFire: a cancel logged by another replica after
// this one's cache was read wins: the conditional append of the policy
// approval catches up, sees proposal.cancelled and approves nothing.
func TestAutoApproveCancelRacesFire(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	acts := newAAActions(t)
	setup := newAARig(t, ms, c, acts, aaOpts{})
	roomID := setup.open()
	pid := setup.propose(roomID, "", "connect_restart_task")
	setup.svc.Close()

	fc := &aaCoord{fakeCoord: &fakeCoord{Store: ms, leases: map[string]store.RoomLease{}}}
	svc := New(Deps{Store: fc, Actions: acts.reg, Users: DefaultUsers(), Owners: aaOwners(), Run: idleRun, Now: c.Now,
		ReplicaID: "replica-a", AutoApproveDelay: aaDelay})
	t.Cleanup(svc.Close)
	mustRoom(t, svc, roomID)
	arjun, _ := svc.LookupUser("arjun")
	if _, err := ms.Append(ctx, roomID, events.ProposalCancelled, svc.humanActor(arjun), events.ProposalCancelledPayload{
		ProposalID: pid, By: svc.humanActor(arjun),
	}); err != nil {
		t.Fatal(err)
	}
	c.Advance(aaDelay)
	svc.firePolicy(ctx, roomID, pid) // its cache is one event behind
	if n := len(logOf(t, ms, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals after another replica's cancel, want 0", n)
	}
	apps, err := ms.ListApprovals(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 0 {
		t.Fatalf("approvals rows = %+v, want the refused one removed", apps)
	}
}

func TestAutoApproveModeAndSnapshot(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	own := aaOwners("connect_restart_task", "schema_set_compat", "consumer_restart")
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{own: own, solo: ""})
	roomID := r.open()
	pid := r.propose(roomID, "", "connect_restart_task")

	view, err := r.svc.RoomFor(context.Background(), r.user("arjun"), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Mode == nil || view.Mode.Solo || !slices.Equal(view.Mode.AutoApprove, []string{"connect_restart_task", "consumer_restart"}) {
		t.Fatalf("mode = %+v, want solo false and auto_approve without the schema entry", view.Mode)
	}
	b, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		OpenProposals []struct {
			ProposalID string          `json:"proposal_id"`
			Scheduled  json.RawMessage `json:"scheduled"`
		} `json:"open_proposals"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.OpenProposals) != 1 || decoded.OpenProposals[0].ProposalID != pid {
		t.Fatalf("open_proposals = %s", b)
	}
	runAt := aaT0.Add(aaDelay).Format(time.RFC3339Nano)
	if want := `{"run_at":"` + runAt + `","cancel_until":"` + runAt + `"}`; string(decoded.OpenProposals[0].Scheduled) != want {
		t.Fatalf("scheduled = %s, want %s", decoded.OpenProposals[0].Scheduled, want)
	}

	// Policy off: auto_approve is [] whatever the file says.
	off := newAARig(t, memstore.NewWithClock(c.Now), c, newAAActions(t), aaOpts{own: own, delay: -1})
	offRoom := off.open()
	v2, err := off.svc.RoomFor(context.Background(), off.user("arjun"), offRoom)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Mode.AutoApprove == nil || len(v2.Mode.AutoApprove) != 0 {
		t.Fatalf("auto_approve with the policy off = %#v, want []", v2.Mode.AutoApprove)
	}
}

func TestModeSoloDerived(t *testing.T) {
	users, err := ParseUsers([]byte(`users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka]
  - id: omni
    name: Omni
    layers: [postgres, kafka-connect, schema-registry, kafka, consumer, warehouse]
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		user, solo string
		want       bool
	}{
		{user: "arjun", want: false},
		{user: "omni", want: true}, // every cdc layer
		{user: "arjun", solo: "arjun", want: true},
	} {
		svc := New(Deps{Store: memstore.New(), Users: users, Owners: aaOwners(), Run: idleRun, SoloUser: tc.solo})
		u, _ := svc.LookupUser(tc.user)
		if got := svc.soloFor(u, "orders"); got != tc.want {
			t.Errorf("soloFor(%s, solo=%q) = %v, want %v", tc.user, tc.solo, got, tc.want)
		}
		svc.Close()
	}
	// A batch pipeline's layers are orchestrator, transform, warehouse:
	// omni owns warehouse only of those.
	batch := &owners.Owners{Pipelines: []owners.Pipeline{{ID: "nightly", Kind: events.PipelineKindBatch}}}
	svc := New(Deps{Store: memstore.New(), Users: users, Owners: batch, Run: idleRun})
	defer svc.Close()
	u, _ := svc.LookupUser("omni")
	if svc.soloFor(u, "nightly") {
		t.Error("soloFor on a batch pipeline with no orchestrator/transform rights = true")
	}
}

func TestApplyScheduleFold(t *testing.T) {
	at := aaT0.Add(time.Minute)
	ev := func(seq int64, typ events.Type, actor events.Actor, p any) events.Event {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		return events.Event{Seq: seq, RoomID: "r", Type: typ, Actor: actor, Payload: b, At: aaT0}
	}
	person := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	base := Apply(NewSnapshot(), ev(1, events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: "p1", Title: "t", RequiredLayers: []string{"kafka-connect", "consumer"},
		Steps: []events.ProposalStep{{Action: "a", Layer: "kafka-connect"}, {Action: "b", Layer: "consumer"}},
	}))
	sched := Apply(base, ev(2, events.ProposalScheduled, systemActor, events.ProposalScheduledPayload{ProposalID: "p1", RunAt: at, CancelUntil: at, Policy: events.PolicyAutoApprove}))
	if p := sched.OpenProposals[0]; p.Scheduled == nil || !p.sched.cancellable() {
		t.Fatalf("after proposal.scheduled: %+v", p)
	}
	if base.OpenProposals[0].Scheduled != nil {
		t.Fatal("Apply mutated its input")
	}
	// A second proposal.scheduled is ignored (at most once per proposal).
	again := Apply(sched, ev(3, events.ProposalScheduled, systemActor, events.ProposalScheduledPayload{ProposalID: "p1", RunAt: at.Add(time.Hour)}))
	if !again.OpenProposals[0].Scheduled.RunAt.Equal(at) {
		t.Fatal("second proposal.scheduled replaced the first")
	}

	cancelled := Apply(sched, ev(3, events.ProposalCancelled, person, events.ProposalCancelledPayload{ProposalID: "p1", By: person}))
	if p := cancelled.OpenProposals[0]; p.Scheduled != nil || p.sched.pending() {
		t.Fatalf("after cancel: %+v", p)
	}
	human := Apply(sched, ev(3, events.ApprovalGiven, person, events.ApprovalGivenPayload{ProposalID: "p1", Layer: "consumer", Grant: events.GrantOwner}))
	if p := human.OpenProposals[0]; p.Scheduled != nil || p.sched.pending() {
		t.Fatalf("after a person's approval: %+v", p)
	}
	// A human actor claiming grant policy is a person's approval.
	forged := Apply(sched, ev(3, events.ApprovalGiven, person, events.ApprovalGivenPayload{ProposalID: "p1", Layer: "consumer", Grant: events.GrantPolicy}))
	if forged.OpenProposals[0].sched.pending() {
		t.Fatal("a human approval with grant policy kept the countdown")
	}
	policy := Apply(sched, ev(3, events.ApprovalGiven, systemActor, events.ApprovalGivenPayload{ProposalID: "p1", Layer: "kafka-connect", Grant: events.GrantPolicy, Policy: events.PolicyAutoApprove}))
	if p := policy.OpenProposals[0]; p.Scheduled != nil || !p.sched.pending() || p.sched.cancellable() {
		t.Fatalf("after a policy approval: %+v", p)
	}
	// A proposal.scheduled after a decision gives no countdown.
	decided := Apply(base, ev(2, events.ApprovalGiven, person, events.ApprovalGivenPayload{ProposalID: "p1", Layer: "consumer"}))
	late := Apply(decided, ev(3, events.ProposalScheduled, systemActor, events.ProposalScheduledPayload{ProposalID: "p1", RunAt: at}))
	if p := late.OpenProposals[0]; p.Scheduled != nil || p.sched.pending() {
		t.Fatalf("scheduled after a decision: %+v", p)
	}
}

func TestAutoApproveLoopNotStartedWhenOff(t *testing.T) {
	made := 0
	svc := New(Deps{Store: memstore.New(), Users: DefaultUsers(), Owners: aaOwners(), Run: idleRun,
		AutoApproveTicker: func(time.Duration) (<-chan time.Time, func()) {
			made++
			return make(chan time.Time), func() {}
		}})
	if err := svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	svc.Close()
	if made != 0 {
		t.Fatalf("ticker made %d times with the policy off", made)
	}
}

func isCode(err error, code string) bool {
	var re *Error
	return errors.As(err, &re) && re.Code == code
}
