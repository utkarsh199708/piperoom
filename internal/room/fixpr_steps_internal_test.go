package room

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/store/memstore"
)

// Q7k: Layered and Deferred steps (docs/design/fix-pr.md §7). The fakes
// below stand in for repo_open_pull_request (internal/actions/fixpr): a
// Layered action whose layer the test sets, and a Deferred one that opens a
// pull request.

// q7Layered is a Layered action: Layer() is "" and LayerFor answers what the
// test set (or its error).
type q7Layered struct {
	aaAct
	lmu      sync.Mutex
	layerFor string
	layerErr error
	pipes    []string // the pipeline ids LayerFor was asked with
}

func (a *q7Layered) Layer() string { return "" }

func (a *q7Layered) LayerFor(p owners.Pipeline, _ json.RawMessage) (string, error) {
	a.lmu.Lock()
	defer a.lmu.Unlock()
	a.pipes = append(a.pipes, p.ID)
	return a.layerFor, a.layerErr
}

func (a *q7Layered) setLayer(l string) {
	a.lmu.Lock()
	defer a.lmu.Unlock()
	a.layerFor = l
}

// q7Deferred is a Layered, Deferred PullRequestOpener: a fix PR step.
type q7Deferred struct {
	q7Layered
	smu     sync.Mutex
	steps   []actions.StepRef
	opened  int
	openErr error
}

func (*q7Deferred) Deferred() {}

func (a *q7Deferred) ExecutePullRequest(ctx context.Context, _ json.RawMessage) (string, events.PullRequestRef, error) {
	a.smu.Lock()
	defer a.smu.Unlock()
	s, ok := actions.StepFrom(ctx)
	if !ok {
		return "", events.PullRequestRef{}, errors.New("no step in ctx")
	}
	a.steps = append(a.steps, s)
	if a.openErr != nil {
		return "", events.PullRequestRef{}, a.openErr
	}
	a.opened++
	n := a.opened
	return "opened pull request", events.PullRequestRef{
		Repo: "connect-config", Number: 10 + n, URL: "https://github.com/acme/connect-config/pull/1",
		Branch: "piperoom/" + s.RoomID + "-" + s.ProposalID + "-0", HeadSHA: strings.Repeat("a", 40),
	}, nil
}

func (a *q7Deferred) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	out, _, err := a.ExecutePullRequest(ctx, params)
	return out, err
}

func (a *q7Deferred) gotSteps() []actions.StepRef {
	a.smu.Lock()
	defer a.smu.Unlock()
	return slices.Clone(a.steps)
}

type q7Rig struct {
	*aaRig
	layered *q7Layered
	fixpr   *q7Deferred
}

// newQ7Rig is the auto-approve rig with the two Q7 fakes registered and, by
// default, every action (the fix PR one too) on orders' auto_approve: list:
// the room must refuse to schedule a Deferred step even when the list names
// it (owners Load refuses that list; this tests the second wall).
func newQ7Rig(t *testing.T, o aaOpts) *q7Rig {
	t.Helper()
	acts := newAAActions(t)
	r := &q7Rig{
		layered: &q7Layered{aaAct: aaAct{name: "repo_layered"}, layerFor: "kafka-connect"},
		fixpr:   &q7Deferred{q7Layered: q7Layered{aaAct: aaAct{name: "repo_open_pull_request"}, layerFor: "kafka-connect"}},
	}
	for _, a := range []actions.Action{r.layered, r.fixpr} {
		if err := acts.reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	if o.own == nil {
		o.own = aaOwners("connect_restart_task", "repo_open_pull_request", "repo_layered")
	}
	c := &testClock{t: aaT0}
	r.aaRig = newAARig(t, memstore.NewWithClock(c.Now), c, acts, o)
	return r
}

func (r *q7Rig) create(roomID string, steps ...events.ProposalStep) (string, error) {
	return r.svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix the image", Risk: "low", Rollback: "close the pull request", Steps: steps,
	}, events.AgentActor)
}

func prStep(layer string) events.ProposalStep {
	return events.ProposalStep{Action: "repo_open_pull_request", Layer: layer, Params: json.RawMessage(`{}`)}
}

func isValidation(err error) bool {
	var re *Error
	return errors.As(err, &re) && re.Code == "validation"
}

// The layer of a Layered step is LayerFor with the room's pipeline; the
// proposer's layer must equal it (a forged layer is refused), and a LayerFor
// error or an empty layer refuses the step.
func TestQ7kLayeredStepLayer(t *testing.T) {
	cases := []struct {
		name     string
		layerFor string
		layerErr error
		propose  string
		ok       bool
	}{
		{"derived layer", "kafka-connect", nil, "kafka-connect", true},
		{"forged layer: the proposer picks another approver", "kafka-connect", nil, "consumer", false},
		{"forged layer: empty", "kafka-connect", nil, "", false},
		{"LayerFor error", "", errors.New("path matches globs of two layers"), "kafka-connect", false},
		{"LayerFor answers no layer", "", nil, "", false},
		{"LayerFor answers an unknown layer", "nope", nil, "nope", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newQ7Rig(t, aaOpts{})
			r.layered.layerFor, r.layered.layerErr = tc.layerFor, tc.layerErr
			roomID := r.open()
			_, err := r.create(roomID, events.ProposalStep{Action: "repo_layered", Layer: tc.propose, Params: json.RawMessage(`{}`)})
			switch {
			case tc.ok && err != nil:
				t.Fatalf("create = %v, want ok", err)
			case !tc.ok && !isValidation(err):
				t.Fatalf("create = %v, want a validation error", err)
			}
			if tc.ok && !slices.Contains(r.layered.pipes, "orders") {
				t.Fatalf("LayerFor asked with pipelines %v, want the room's pipeline orders", r.layered.pipes)
			}
		})
	}
}

// A proposal with a Deferred step holds only Deferred steps, at most
// actions.MaxDeferredSteps.
func TestQ7kDeferredProposalShape(t *testing.T) {
	restart := events.ProposalStep{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{}`)}
	pr := prStep("kafka-connect")
	cases := []struct {
		name  string
		steps []events.ProposalStep
		ok    bool
		why   string
	}{
		{"one PR step", []events.ProposalStep{pr}, true, ""},
		{"three PR steps", []events.ProposalStep{pr, pr, pr}, true, ""},
		{"four PR steps", []events.ProposalStep{pr, pr, pr, pr}, false, "at most 3"},
		{"mixed: PR then live", []events.ProposalStep{pr, restart}, false, "only pull-request steps"},
		{"mixed: live then PR", []events.ProposalStep{restart, pr}, false, "only pull-request steps"},
		{"live only", []events.ProposalStep{restart, restart}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newQ7Rig(t, aaOpts{})
			roomID := r.open()
			_, err := r.create(roomID, tc.steps...)
			if tc.ok {
				if err != nil {
					t.Fatalf("create = %v, want ok", err)
				}
				return
			}
			if !isValidation(err) || !strings.Contains(err.Error(), tc.why) {
				t.Fatalf("create = %v, want a validation error naming %q", err, tc.why)
			}
		})
	}
}

// approveAll approves every required layer of an open proposal as its owner.
func (r *q7Rig) approveAll(roomID, pid string) {
	r.t.Helper()
	p := r.proposal(roomID, pid)
	for _, l := range p.RequiredLayers {
		if _, err := r.svc.ApproveProposal(context.Background(), r.ownerOf(l), pid, l, p.ParamsHash); err != nil {
			r.t.Fatalf("approve %s: %v", l, err)
		}
	}
}

func (r *q7Rig) ownerOf(layer string) User {
	for _, u := range r.svc.AllUsers() {
		if slices.Contains(u.Layers, layer) {
			return u
		}
	}
	r.t.Fatalf("no owner of %s", layer)
	return User{}
}

func executedOf(t *testing.T, r *q7Rig, roomID string, n int) []events.ActionExecutedPayload {
	t.Helper()
	waitForCond(t, 5*time.Second, func() bool {
		return len(logOf(t, r.st, roomID, events.ActionExecuted)) >= n
	}, "steps never executed")
	var out []events.ActionExecutedPayload
	for _, e := range logOf(t, r.st, roomID, events.ActionExecuted) {
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// The executor puts the StepRef in ctx, prefers ExecutePullRequest, and
// stores the PR on action.executed; the snapshot lists it room-level after
// the proposal left open_proposals.
func TestQ7kExecutorOpensPullRequest(t *testing.T) {
	r := newQ7Rig(t, aaOpts{})
	roomID := r.open()
	pid, err := r.create(roomID, prStep("kafka-connect"))
	if err != nil {
		t.Fatal(err)
	}
	r.waitPreview(roomID, pid)
	if p := r.proposal(roomID, pid); p.Scheduled != nil {
		t.Fatalf("a Deferred proposal was scheduled: %+v", p.Scheduled)
	}
	r.approveAll(roomID, pid)
	got := executedOf(t, r, roomID, 1)
	if !got[0].OK || got[0].PullRequest == nil || got[0].PullRequest.Number != 11 {
		t.Fatalf("action.executed = %+v, want ok with pull_request #11", got[0])
	}
	steps := r.fixpr.gotSteps()
	want := actions.StepRef{RoomID: roomID, ProposalID: pid, StepIndex: 0, Title: "fix the image", Approvals: []string{"kafka-connect: Arjun"}}
	if len(steps) != 1 || steps[0].RoomID != want.RoomID || steps[0].ProposalID != want.ProposalID ||
		steps[0].StepIndex != 0 || steps[0].Title != want.Title || !slices.Equal(steps[0].Approvals, want.Approvals) {
		t.Fatalf("StepRef = %+v, want %+v", steps, want)
	}
	waitForCond(t, 5*time.Second, func() bool { return len(mustRoom(t, r.svc, roomID).PullRequests) == 1 }, "no pull_requests entry")
	snap := mustRoom(t, r.svc, roomID)
	pr := snap.PullRequests[0]
	if pr.ProposalID != pid || pr.Number != 11 || pr.State != "open" || pr.Repo != "connect-config" || pr.HeadChanged || pr.MergedAt != nil {
		t.Fatalf("pull_requests[0] = %+v", pr)
	}
	if _, open := openProposal(snap, pid); open {
		t.Fatal("the executed proposal is still open")
	}
}

func (r *q7Rig) waitPreview(roomID, pid string) {
	r.t.Helper()
	waitForCond(r.t, 5*time.Second, func() bool {
		p, ok := openProposal(mustRoom(r.t, r.svc, roomID), pid)
		return ok && p.Preview != nil
	}, "preview never folded")
}

// The executor derives the layer again before Execute: a step approved as
// one layer whose layer is now another does not run.
func TestQ7kExecutorRefusesALayerThatMoved(t *testing.T) {
	r := newQ7Rig(t, aaOpts{})
	roomID := r.open()
	pid, err := r.create(roomID, prStep("kafka-connect"))
	if err != nil {
		t.Fatal(err)
	}
	r.waitPreview(roomID, pid)
	r.fixpr.setLayer("consumer") // owners.yaml moved the glob to another layer
	r.approveAll(roomID, pid)
	got := executedOf(t, r, roomID, 1)
	if got[0].OK || got[0].PullRequest != nil || !strings.Contains(got[0].Output, "layer check failed") {
		t.Fatalf("action.executed = %+v, want a failed layer check", got[0])
	}
	if n := len(r.fixpr.gotSteps()); n != 0 {
		t.Fatalf("ExecutePullRequest ran %d times", n)
	}
}

// A failed PR step stores no pull request.
func TestQ7kFailedOpenStoresNoPullRequest(t *testing.T) {
	r := newQ7Rig(t, aaOpts{})
	r.fixpr.openErr = errors.New("the base branch changed")
	roomID := r.open()
	pid, err := r.create(roomID, prStep("kafka-connect"))
	if err != nil {
		t.Fatal(err)
	}
	r.waitPreview(roomID, pid)
	r.approveAll(roomID, pid)
	got := executedOf(t, r, roomID, 1)
	if got[0].OK || got[0].PullRequest != nil {
		t.Fatalf("action.executed = %+v, want failed without pull_request", got[0])
	}
	if n := len(mustRoom(t, r.svc, roomID).PullRequests); n != 0 {
		t.Fatalf("%d pull_requests after a failed open", n)
	}
}

// A room holds at most 3 open fix PRs, counting the PR steps of open
// proposals that have not run yet.
func TestQ7kOpenFixPRCap(t *testing.T) {
	r := newQ7Rig(t, aaOpts{})
	roomID := r.open()
	pid, err := r.create(roomID, prStep("kafka-connect"), prStep("kafka-connect"))
	if err != nil {
		t.Fatal(err)
	}
	r.waitPreview(roomID, pid)
	r.approveAll(roomID, pid)
	executedOf(t, r, roomID, 2)
	waitForCond(t, 5*time.Second, func() bool { return len(mustRoom(t, r.svc, roomID).PullRequests) == 2 }, "two PRs not folded")
	// Two open PRs: one more PR step is allowed, then the room is full.
	if _, err := r.create(roomID, prStep("kafka-connect")); err != nil {
		t.Fatalf("third PR: %v", err)
	}
	_, err = r.create(roomID, prStep("kafka-connect"))
	if !isValidation(err) || !strings.Contains(err.Error(), "at most 3 open fix pull requests") {
		t.Fatalf("fourth PR = %v, want the open fix PR cap", err)
	}
	// A live fix is not capped.
	if _, err := r.create(roomID, events.ProposalStep{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("live fix: %v", err)
	}
}

// A policy never approves a Deferred step: no countdown although
// auto_approve: names it, and a countdown in the log (a forged or older one)
// is refused by the fire path.
func TestQ7kDeferredNeverPolicyApproved(t *testing.T) {
	r := newQ7Rig(t, aaOpts{})
	roomID := r.open()
	pid, err := r.create(roomID, prStep("kafka-connect"))
	if err != nil {
		t.Fatal(err)
	}
	r.waitPreview(roomID, pid)
	if n := len(logOf(t, r.st, roomID, events.ProposalScheduled)); n != 0 {
		t.Fatalf("%d proposal.scheduled for a Deferred proposal", n)
	}
	// A live proposal on the same list is scheduled: the policy is on.
	live, err := r.create(roomID, events.ProposalStep{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	r.waitPreview(roomID, live)
	if p := r.proposal(roomID, live); p.Scheduled == nil {
		t.Fatal("the live proposal was not scheduled: the policy is off in this rig")
	}
	// Attack: a countdown for the PR proposal lands in the log anyway.
	runAt := r.clock.Now().Add(aaDelay)
	if err := r.svc.emitAs(context.Background(), roomID, systemActor, events.ProposalScheduled, events.ProposalScheduledPayload{
		ProposalID: pid, RunAt: runAt, Policy: events.PolicyAutoApprove, CancelUntil: runAt,
	}); err != nil {
		t.Fatal(err)
	}
	r.clock.Advance(aaDelay)
	r.tick()
	for _, a := range policyApprovals(t, r.st, roomID) {
		if a.ProposalID == pid {
			t.Fatalf("policy approved the Deferred proposal: %+v", a)
		}
	}
	if n := len(r.fixpr.gotSteps()); n != 0 {
		t.Fatalf("the PR step ran %d times", n)
	}
}

// tieredDeferred is a Deferred action that claims T1 in a setup room.
type tieredDeferred struct{ q7Deferred }

func (*tieredDeferred) Tier(json.RawMessage, string) (string, error) { return plan.TierT1, nil }

// tieredLive is the same claim from an ordinary action.
type tieredLive struct{ aaAct }

func (*tieredLive) Tier(json.RawMessage, string) (string, error) { return plan.TierT1, nil }

// A Deferred step is never a setup T1 step, whatever its Tier says.
func TestQ7kDeferredNeverSetupT1(t *testing.T) {
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{
		&tieredDeferred{q7Deferred{q7Layered: q7Layered{aaAct: aaAct{name: "pr_t1"}, layerFor: "kafka-connect"}}},
		&tieredLive{aaAct{name: "live_t1", layer: "kafka-connect"}},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct{ action, want string }{{"pr_t1", plan.TierT2}, {"live_t1", plan.TierT1}}
	for _, tc := range cases {
		got, err := setupStepTier(reg, events.ProposalStep{Action: tc.action, Params: json.RawMessage(`{}`)}, "local")
		if err != nil || got != tc.want {
			t.Fatalf("setupStepTier(%s) = %q, %v; want %q", tc.action, got, err, tc.want)
		}
	}
}

// checkDeferredSteps and stepLayer are the rules; a table of their answers.
func TestQ7kRuleTable(t *testing.T) {
	live := &aaAct{name: "live", layer: "kafka-connect"}
	pr := &q7Deferred{q7Layered: q7Layered{aaAct: aaAct{name: "pr"}, layerFor: "consumer"}}
	cases := []struct {
		name string
		acts []actions.Action
		n    int
		ok   bool
	}{
		{"none", nil, 0, true},
		{"live", []actions.Action{live}, 0, true},
		{"pr", []actions.Action{pr}, 1, true},
		{"pr x3", []actions.Action{pr, pr, pr}, 3, true},
		{"pr x4", []actions.Action{pr, pr, pr, pr}, 4, false},
		{"pr + live", []actions.Action{pr, live}, 1, false},
	}
	for _, tc := range cases {
		n, err := checkDeferredSteps(tc.acts)
		if n != tc.n || (err == nil) != tc.ok {
			t.Errorf("%s: checkDeferredSteps = %d, %v; want %d, ok %v", tc.name, n, err, tc.n, tc.ok)
		}
	}
	s := New(Deps{Store: memstore.New(), Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}}})
	t.Cleanup(s.Close)
	for _, tc := range []struct {
		a    actions.Action
		pipe string
		want string
		ok   bool
	}{
		{live, "orders", "kafka-connect", true},
		{pr, "orders", "consumer", true},
		{pr, "unknown-pipeline", "consumer", true}, // LayerFor sees the id alone
		{&q7Layered{aaAct: aaAct{name: "empty"}}, "orders", "", false},
	} {
		got, err := s.stepLayer(nil, tc.pipe, tc.a, json.RawMessage(`{}`))
		if got != tc.want || (err == nil) != tc.ok {
			t.Errorf("stepLayer(%s, %s) = %q, %v; want %q, ok %v", tc.a.Name(), tc.pipe, got, err, tc.want, tc.ok)
		}
	}
}
