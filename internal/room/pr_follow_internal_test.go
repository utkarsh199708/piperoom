package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Q7e: the pull request follower. Every test drives the clock by hand and
// runs checks either through the loop's hand-fed ticker or by calling
// followPRs directly; nothing sleeps on the follower.

var prT0 = time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)

var wroteHead = strings.Repeat("a", 40)

// fakePRs is a forge reader whose answer the test sets.
type fakePRs struct {
	mu    sync.Mutex
	pr    clients.PullRequest
	errs  []error // returned first, one per call
	calls int
	repos []clients.RepoRef
}

func (f *fakePRs) PullRequest(ctx context.Context, repo clients.RepoRef, number int) (clients.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := ctx.Deadline(); !ok {
		return clients.PullRequest{}, errors.New("fake: no deadline")
	}
	f.calls++
	f.repos = append(f.repos, repo)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return clients.PullRequest{}, err
	}
	pr := f.pr
	pr.Number = number
	return pr, nil
}

func (f *fakePRs) set(fn func(pr *clients.PullRequest)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(&f.pr)
}

func (f *fakePRs) fail(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, errs...)
}

func (f *fakePRs) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func prOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", SourceTables: []string{"public.orders"},
		Repos: []owners.Repo{{
			ID: "connect-config", URL: "https://github.com/acme/connect-config", Base: "main",
			Paths: []owners.RepoPath{{Glob: "connectors/*.json", Layer: "kafka-connect"}},
		}},
	}}}
}

type prRig struct {
	t     *testing.T
	svc   *Service
	st    store.Store
	clock *testClock
	prs   *fakePRs
	fixpr *q7Deferred
	ticks chan time.Time
	done  chan struct{}
	stops atomic.Int32
}

// newPRRig builds a single-process service with the fix PR fake, the fake
// forge reader and a hand-fed follower ticker, and loads it (which starts
// the follower loop).
func newPRRig(t *testing.T) *prRig {
	t.Helper()
	c := &testClock{t: prT0}
	ms := memstore.NewWithClock(c.Now)
	r := &prRig{t: t, st: ms, clock: c, prs: &fakePRs{}, ticks: make(chan time.Time), done: make(chan struct{})}
	r.fixpr = &q7Deferred{q7Layered: q7Layered{aaAct: aaAct{name: "repo_open_pull_request"}, layerFor: "kafka-connect"}}
	reg := actions.NewRegistry()
	if err := reg.Register(r.fixpr); err != nil {
		t.Fatal(err)
	}
	r.svc = New(Deps{
		Store: ms, Actions: reg, Users: DefaultUsers(), Owners: prOwners(), Run: idleRun, Now: c.Now,
		PullRequests: r.prs,
		PRPollTicker: func(d time.Duration) (<-chan time.Time, func()) {
			if d != PRCheckEvery {
				t.Errorf("follower ticker period %v, want %v", d, PRCheckEvery)
			}
			return r.ticks, func() { r.stops.Add(1) }
		},
	})
	r.svc.prTickDone = func() { r.done <- struct{}{} }
	t.Cleanup(r.svc.Close)
	if err := r.svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

// tick runs one check of the follower loop and returns when it finished.
func (r *prRig) tick() {
	r.t.Helper()
	select {
	case r.ticks <- time.Time{}:
	case <-time.After(5 * time.Second):
		r.t.Fatal("follower loop not running")
	}
	<-r.done
}

// setNow sets the fake clock.
func (r *prRig) setNow(t time.Time) {
	r.clock.mu.Lock()
	defer r.clock.mu.Unlock()
	r.clock.t = t
}

// openPR opens a room, proposes and approves one fix PR step and waits until
// the snapshot follows its PR. The fake forge answers that PR as Piperoom
// wrote it: open, head at the written commit, no checks.
func (r *prRig) openPR() (roomID string, pr PullRequest) {
	r.t.Helper()
	ctx := context.Background()
	arjun, _ := r.svc.LookupUser("arjun")
	snap, err := r.svc.CreateRoom(ctx, arjun, "Incident", "sev2", "orders")
	if err != nil {
		r.t.Fatal(err)
	}
	roomID = snap.ID
	pid, err := r.svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "close the pull request",
		Steps: []events.ProposalStep{{Action: "repo_open_pull_request", Layer: "kafka-connect",
			Params: json.RawMessage(`{"repo":"connect-config","title":"connect image: add the plugin"}`)}},
	}, events.AgentActor)
	if err != nil {
		r.t.Fatal(err)
	}
	waitForCond(r.t, 5*time.Second, func() bool {
		p, ok := openProposal(mustRoom(r.t, r.svc, roomID), pid)
		return ok && p.Preview != nil
	}, "preview never folded")
	p, _ := openProposal(mustRoom(r.t, r.svc, roomID), pid)
	if _, err := r.svc.ApproveProposal(ctx, arjun, pid, "kafka-connect", p.ParamsHash); err != nil {
		r.t.Fatal(err)
	}
	waitForCond(r.t, 5*time.Second, func() bool { return len(mustRoom(r.t, r.svc, roomID).PullRequests) == 1 }, "PR never followed")
	pr = mustRoom(r.t, r.svc, roomID).PullRequests[0]
	r.prs.set(func(p *clients.PullRequest) {
		*p = clients.PullRequest{Repo: "connect-config", URL: pr.URL, State: clients.PRStateOpen,
			HeadRef: pr.Branch, HeadSHA: wroteHead, Checks: clients.PullRequestChecks{State: clients.ChecksNone}}
	})
	return roomID, pr
}

func (r *prRig) updates(roomID string) []events.PullRequestUpdatedPayload {
	r.t.Helper()
	var out []events.PullRequestUpdatedPayload
	for _, e := range logOf(r.t, r.st, roomID, events.PullRequestUpdated) {
		if e.Actor != systemActor {
			r.t.Fatalf("pull_request.updated actor %+v, want system", e.Actor)
		}
		p, err := events.Decode[events.PullRequestUpdatedPayload](e)
		if err != nil {
			r.t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// An event only when state, head, merge or checks changed; never a second
// one for the same state; a merge writes the pipeline change and the
// pr_merged input, then polling stops.
func TestQ7eFollowerChangeDetection(t *testing.T) {
	r := newPRRig(t)
	roomID, pr := r.openPR()

	r.tick() // due at once; nothing changed against the written PR
	if r.prs.count() != 1 || len(r.updates(roomID)) != 0 {
		t.Fatalf("calls %d, updates %d; want one poll and no event", r.prs.count(), len(r.updates(roomID)))
	}
	if got := r.prs.repos[0]; got != (clients.RepoRef{ID: "connect-config", Owner: "acme", Name: "connect-config", Base: "main"}) {
		t.Fatalf("polled repo %+v, want acme/connect-config @ main from owners.yaml", got)
	}
	r.tick() // not due: PRPollEvery has not passed
	if r.prs.count() != 1 {
		t.Fatalf("polled %d times before the period passed", r.prs.count())
	}

	steps := []struct {
		name   string
		change func(p *clients.PullRequest)
		events int
	}{
		{"checks start", func(p *clients.PullRequest) {
			p.Checks = clients.PullRequestChecks{State: clients.ChecksPending, Total: 2}
		}, 1},
		{"same state again", func(*clients.PullRequest) {}, 1},
		{"checks fail", func(p *clients.PullRequest) {
			p.Checks = clients.PullRequestChecks{State: clients.ChecksFailure, Total: 2, Failed: []string{"build\n"}}
		}, 2},
		{"reviewer pushes", func(p *clients.PullRequest) { p.HeadSHA = strings.Repeat("b", 40) }, 3},
		{"pushes again: head_changed stays true, checks unchanged", func(p *clients.PullRequest) { p.HeadSHA = strings.Repeat("c", 40) }, 3},
		{"merged", func(p *clients.PullRequest) {
			p.State, p.MergeSHA, p.MergedAt = clients.PRStateMerged, strings.Repeat("d", 40), prT0.Add(10*time.Minute)
		}, 4},
	}
	for _, s := range steps {
		r.prs.set(s.change)
		r.clock.Advance(PRPollEvery)
		r.tick()
		if n := len(r.updates(roomID)); n != s.events {
			t.Fatalf("%s: %d events, want %d", s.name, n, s.events)
		}
	}
	ups := r.updates(roomID)
	if ups[0].Checks.State != "pending" || ups[0].HeadChanged || ups[0].State != "open" || ups[0].HeadSHA != wroteHead || ups[0].Number != pr.Number {
		t.Fatalf("first update %+v", ups[0])
	}
	if f := ups[1].Checks.Failed; len(f) != 1 || f[0] != "build" {
		t.Fatalf("failed checks %q, want cleaned names", f)
	}
	if !ups[2].HeadChanged || ups[2].HeadSHA != wroteHead {
		t.Fatalf("push update %+v: want head_changed against the written commit, head_sha the written commit", ups[2])
	}
	m := ups[3]
	if m.State != "merged" || m.MergeSHA != strings.Repeat("d", 40) || !m.MergedAt.Equal(prT0.Add(10*time.Minute)) || !m.HeadChanged {
		t.Fatalf("merge update %+v", m)
	}

	snap := mustRoom(t, r.svc, roomID)
	got := snap.PullRequests[0]
	if got.State != "merged" || got.MergedAt == nil || !got.HeadChanged || got.UpdatedAt.IsZero() {
		t.Fatalf("snapshot PR %+v", got)
	}
	// The merge is a pipeline change.
	changes, err := r.st.(clients.ChangeReader).ListChanges(context.Background(), "orders", time.Time{}, time.Time{}, []string{clients.ChangePullRequest}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Object != fmt.Sprintf("connect-config#%d", pr.Number) ||
		changes[0].Summary != "connect image: add the plugin" || !strings.Contains(string(changes[0].After), strings.Repeat("d", 40)) {
		t.Fatalf("changes %+v", changes)
	}
	// The agent hears pr_merged once.
	in := inboxKinds(t, r.svc, roomID)
	if n := countKind(in, "pr_merged"); n != 1 {
		t.Fatalf("inbox %v: %d pr_merged, want 1", in, n)
	}
	// Merged: no more polls.
	calls := r.prs.count()
	r.clock.Advance(10 * PRPollEvery)
	r.tick()
	if r.prs.count() != calls {
		t.Fatal("a merged PR was polled again")
	}
}

func inboxKinds(t *testing.T, svc *Service, roomID string) []agent.HumanInput {
	t.Helper()
	st := svc.cached(roomID)
	if st == nil {
		t.Fatalf("room %s not cached", roomID)
	}
	items, err := st.inbox.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return items
}

func countKind(in []agent.HumanInput, kind string) int {
	n := 0
	for _, i := range in {
		if i.Kind == kind {
			n++
		}
	}
	return n
}

// A close sends pr_closed and stops polling; a closed room is not polled.
func TestQ7eClosedAndRoomClose(t *testing.T) {
	r := newPRRig(t)
	roomID, _ := r.openPR()
	r.prs.set(func(p *clients.PullRequest) { p.State = clients.PRStateClosed })
	r.tick()
	ups := r.updates(roomID)
	if len(ups) != 1 || ups[0].State != "closed" || ups[0].MergeSHA != "" {
		t.Fatalf("updates %+v, want one closed", ups)
	}
	in := inboxKinds(t, r.svc, roomID)
	if countKind(in, "pr_closed") != 1 || countKind(in, "pr_merged") != 0 {
		t.Fatalf("inbox %+v, want one pr_closed", in)
	}
	for _, i := range in {
		if i.Kind == "pr_closed" && (i.StepIndex == nil || *i.StepIndex != 0 || i.ProposalID == "" || !strings.Contains(i.Text, "not applied")) {
			t.Fatalf("pr_closed input %+v", i)
		}
	}
	calls := r.prs.count()
	r.clock.Advance(PRPollEvery)
	r.tick()
	if r.prs.count() != calls {
		t.Fatal("a closed PR was polled again")
	}

	// A second PR in another room stops being followed when its room closes.
	room2, _ := r.openPR()
	arjun, _ := r.svc.LookupUser("arjun")
	if _, err := r.svc.CloseRoom(context.Background(), arjun, room2, "resolved"); err != nil {
		t.Fatal(err)
	}
	calls = r.prs.count()
	r.clock.Advance(PRPollEvery)
	r.tick()
	if r.prs.count() != calls {
		t.Fatal("a PR of a closed room was polled")
	}
}

// A rate-limit answer stops every poll until its reset; an unknown reset
// backs off like an error.
func TestQ7eRateLimitWaitsForReset(t *testing.T) {
	r := newPRRig(t)
	roomID, _ := r.openPR()
	reset := prT0.Add(5 * time.Minute)
	r.prs.fail(&PRRateLimitError{Reset: reset, Err: errors.New("403")})
	r.tick()
	if r.prs.count() != 1 {
		t.Fatalf("calls %d, want 1", r.prs.count())
	}
	for _, d := range []time.Duration{PRPollEvery, 4 * time.Minute} {
		r.setNow(prT0.Add(d))
		r.tick()
		if r.prs.count() != 1 {
			t.Fatalf("polled at +%v during the rate limit", d)
		}
	}
	r.setNow(reset)
	r.prs.set(func(p *clients.PullRequest) {
		p.Checks = clients.PullRequestChecks{State: clients.ChecksSuccess, Total: 1}
	})
	r.tick()
	if r.prs.count() != 2 || len(r.updates(roomID)) != 1 {
		t.Fatalf("after the reset: calls %d, updates %d; want 2 and 1", r.prs.count(), len(r.updates(roomID)))
	}
}

// Errors back off doubling from PRPollEvery up to prMaxBackoff, and a
// success resets the period.
func TestQ7eErrorBackoff(t *testing.T) {
	r := newPRRig(t)
	r.openPR()
	down := errors.New("forge: GET /repos: status 502")
	r.prs.fail(down, down, down, down, down, down, down)
	r.tick() // +0: error 1, next in 60s
	waits := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute}
	at := prT0
	for i, w := range waits {
		r.setNow(at.Add(w - time.Second))
		r.tick()
		if r.prs.count() != i+1 {
			t.Fatalf("polled %v after error %d, before its %v back-off", w-time.Second, i+1, w)
		}
		at = at.Add(w)
		r.setNow(at)
		r.tick()
		if r.prs.count() != i+2 {
			t.Fatalf("not polled %v after error %d", w, i+1)
		}
	}
	// The last call failed (7 errors queued); the next succeeds after the
	// capped wait, then the period is back to PRPollEvery.
	at = at.Add(prMaxBackoff)
	r.setNow(at)
	r.tick()
	r.setNow(at.Add(PRPollEvery))
	r.tick()
	if want := len(waits) + 3; r.prs.count() != want {
		t.Fatalf("calls %d, want %d", r.prs.count(), want)
	}
}

// A forge answer for another branch, or a state the room does not know, is
// an error: no event.
func TestQ7eRefusesAnotherPR(t *testing.T) {
	r := newPRRig(t)
	roomID, _ := r.openPR()
	r.prs.set(func(p *clients.PullRequest) { p.HeadRef, p.State = "feature/x", clients.PRStateMerged })
	r.tick()
	r.prs.set(func(p *clients.PullRequest) { p.HeadRef = ""; p.State = "draft" })
	r.clock.Advance(PRPollEvery)
	r.tick()
	if n := len(r.updates(roomID)); n != 0 {
		t.Fatalf("%d events for a PR that is not the step's", n)
	}
}

// Close stops the loop: the ticker is stopped and no tick is read.
func TestQ7eLoopStopsOnClose(t *testing.T) {
	r := newPRRig(t)
	r.svc.Close()
	if n := r.stops.Load(); n != 1 {
		t.Fatalf("ticker stopped %d times, want 1", n)
	}
	select {
	case r.ticks <- time.Time{}:
		t.Fatal("the loop read a tick after Close")
	case <-time.After(50 * time.Millisecond):
	}
}

// No forge reader: no loop at all.
func TestQ7eNoForgeNoLoop(t *testing.T) {
	made := 0
	svc := New(Deps{Store: memstore.New(), Users: DefaultUsers(), Owners: prOwners(),
		PRPollTicker: func(time.Duration) (<-chan time.Time, func()) { made++; return nil, func() {} }})
	t.Cleanup(svc.Close)
	if err := svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if made != 0 {
		t.Fatalf("follower ticker made %d times without a forge", made)
	}
}

// prCoord is fakeCoord with the cluster-wide lock the follower takes on a
// room without a lease.
type prCoord struct {
	fakeCoord
	mu     sync.Mutex
	locked map[string]bool
}

func (c *prCoord) LockKey(ctx context.Context, key string) (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.locked[key] {
		return nil, fmt.Errorf("fake: %s is locked: %w", key, context.DeadlineExceeded)
	}
	c.locked[key] = true
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.locked, key)
	}, nil
}

// One poller per room across replicas: a room another replica holds is left
// to it, a room this replica holds is polled, and a room without a lease is
// polled only by the replica that takes its lock; the conditional append
// writes one event when two replicas saw the same change.
func TestQ7eLeaseHolderOnly(t *testing.T) {
	ctx := context.Background()
	r := newPRRig(t)
	heldElsewhere, _ := r.openPR()
	heldHere, _ := r.openPR()
	unheld, _ := r.openPR()
	r.svc.Close()
	r.prs.set(func(p *clients.PullRequest) {
		// One answer for three PRs on three branches: no head branch.
		p.HeadRef, p.Checks = "", clients.PullRequestChecks{State: clients.ChecksPending, Total: 1}
	})

	pc := &prCoord{fakeCoord: fakeCoord{Store: r.st.(*memstore.Store), leases: map[string]store.RoomLease{
		heldElsewhere: {RoomID: heldElsewhere, Owner: "replica-b"},
		heldHere:      {RoomID: heldHere, Owner: "replica-a"},
	}}, locked: map[string]bool{}}
	replica := func(id string, prs PullRequestReader) *Service {
		svc := New(Deps{Store: pc, Users: DefaultUsers(), Owners: prOwners(), Now: r.clock.Now, ReplicaID: id, PullRequests: prs, Run: idleRun})
		t.Cleanup(svc.Close)
		for _, room := range []string{heldElsewhere, heldHere, unheld} {
			mustRoom(t, svc, room)
		}
		return svc
	}
	a := replica("replica-a", r.prs)
	// replica-c caches the rooms now, before replica-a writes anything.
	otherPRs := &fakePRs{pr: r.prs.pr}
	c := replica("replica-c", otherPRs)
	a.followPRs(ctx)
	if n := len(r.updates(heldElsewhere)); n != 0 {
		t.Fatalf("room held by another replica: %d events", n)
	}
	for _, id := range []string{heldHere, unheld} {
		if n := len(r.updates(id)); n != 1 {
			t.Fatalf("room %s: %d events, want 1", id, n)
		}
	}
	if r.prs.count() != 2 {
		t.Fatalf("replica-a polled %d PRs, want 2", r.prs.count())
	}

	// Another replica holds the unleased room's lock: replica-c skips it.
	unlock, err := pc.LockKey(ctx, "pr-follow:"+unheld)
	if err != nil {
		t.Fatal(err)
	}
	c.followPRs(ctx)
	if otherPRs.count() != 0 {
		t.Fatalf("replica-c polled %d PRs: one is another replica's, one locked", otherPRs.count())
	}
	unlock()
	// The lock is free, replica-c's cached snapshot is behind: it polls,
	// sees the same change, and its checked append writes nothing more.
	c.followPRs(ctx)
	if otherPRs.count() != 1 {
		t.Fatalf("replica-c polled %d PRs, want 1", otherPRs.count())
	}
	if n := len(r.updates(unheld)); n != 1 {
		t.Fatalf("unheld room: %d events after two replicas saw the change, want 1", n)
	}
	if got := mustRoom(t, c, unheld).PullRequests[0].Checks.State; got != clients.ChecksPending {
		t.Fatalf("replica-c did not catch up on the conflict: checks %q", got)
	}
}

// pull_request.updated yields pr_merged or pr_closed (which wake an idle
// agent) and nothing for an open PR.
func TestQ7ePRInputs(t *testing.T) {
	merged := prT0.Add(2 * time.Minute)
	ref := events.PullRequestRef{Repo: "connect-config", Number: 12, URL: "u", Branch: "b", HeadSHA: wroteHead}
	cases := []struct {
		name string
		p    events.PullRequestUpdatedPayload
		kind string
		text string
	}{
		{"open", events.PullRequestUpdatedPayload{ProposalID: "p_1", PullRequestRef: ref, State: "open"}, "", ""},
		{"merged", events.PullRequestUpdatedPayload{ProposalID: "p_1", PullRequestRef: ref, State: "merged", MergeSHA: "1a2b3c4d5e", MergedAt: merged},
			"pr_merged", "PR #12 in connect-config merged at 14:02 (merge 1a2b3c4); edited after approval: no"},
		{"merged, edited", events.PullRequestUpdatedPayload{ProposalID: "p_1", PullRequestRef: ref, State: "merged", HeadChanged: true, MergedAt: merged},
			"pr_merged", "edited after approval: yes"},
		{"closed", events.PullRequestUpdatedPayload{ProposalID: "p_1", StepIndex: 2, PullRequestRef: ref, State: "closed"}, "pr_closed", "not applied"},
	}
	for _, tc := range cases {
		b, err := json.Marshal(tc.p)
		if err != nil {
			t.Fatal(err)
		}
		e := events.Event{Seq: 9, Type: events.PullRequestUpdated, Actor: systemActor, At: prT0, Payload: b}
		in := inputFor(e)
		if tc.kind == "" {
			if in != nil || startsAgent(RoomSnapshot{}, e) {
				t.Fatalf("%s: input %+v, want none", tc.name, in)
			}
			continue
		}
		if in == nil || in.Kind != tc.kind || !strings.Contains(in.Text, tc.text) || in.ProposalID != "p_1" ||
			in.StepIndex == nil || *in.StepIndex != tc.p.StepIndex || !startsAgent(RoomSnapshot{}, e) {
			t.Fatalf("%s: input %+v", tc.name, in)
		}
		if tc.kind == "pr_merged" && !in.At.Equal(merged) {
			t.Fatalf("%s: At %v, want the merge time", tc.name, in.At)
		}
	}
}

// Hand-off lines name every open fix PR the summary does not.
func TestQ7eOpenPRLines(t *testing.T) {
	snap := RoomSnapshot{PullRequests: []PullRequest{
		{Repo: "connect-config", Number: 12, URL: "https://github.com/acme/connect-config/pull/12", State: "open", HeadChanged: true},
		{Repo: "connect-config", Number: 13, URL: "https://github.com/acme/connect-config/pull/13", State: "open"},
		{Repo: "connect-config", Number: 9, URL: "https://github.com/acme/connect-config/pull/9", State: "merged"},
	}}
	got := openPRLines(snap, []string{"watch https://github.com/acme/connect-config/pull/13 until it merges"})
	if len(got) != 2 || !strings.Contains(got[1], "#12") || !strings.Contains(got[1], "edited after approval") {
		t.Fatalf("open lines %q", got)
	}
}

// The snapshot keeps at most 20 PRs, dropping finished ones first.
func TestQ7eSnapshotCap(t *testing.T) {
	var prs []PullRequest
	for i := 0; i < maxSnapshotPullRequests; i++ {
		prs = addPullRequest(prs, events.ActionExecutedPayload{ProposalID: fmt.Sprintf("p_%d", i), OK: true,
			PullRequest: &events.PullRequestRef{Repo: "r", Number: i}}, prT0)
	}
	// The oldest is still open, the second oldest merged.
	prs = applyPRUpdate(prs, events.PullRequestUpdatedPayload{ProposalID: "p_1", State: "merged"}, prT0)
	prs = addPullRequest(prs, events.ActionExecutedPayload{ProposalID: "p_new", OK: true,
		PullRequest: &events.PullRequestRef{Repo: "r", Number: 99}}, prT0)
	if len(prs) != maxSnapshotPullRequests || prs[0].ProposalID != "p_new" {
		t.Fatalf("len %d, first %s", len(prs), prs[0].ProposalID)
	}
	if _, ok := findPR(prs, "p_1", 0); ok {
		t.Fatal("the merged PR was kept over an open one")
	}
	if _, ok := findPR(prs, "p_0", 0); !ok {
		t.Fatal("the oldest open PR was dropped")
	}
	// A repeated action.executed changes nothing.
	again := addPullRequest(prs, events.ActionExecutedPayload{ProposalID: "p_new", OK: true,
		PullRequest: &events.PullRequestRef{Repo: "r", Number: 99}}, prT0)
	if len(again) != len(prs) {
		t.Fatal("a repeated action.executed added a PR")
	}
}
