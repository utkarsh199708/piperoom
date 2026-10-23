package room

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
)

// Review of #934 (fix-PR Major 2): the pr_merged input is drained by the run
// that waits for the change to apply, and the inbox is in memory. A new run
// (after a restart, a lease move or a retry) is told again about every fix
// PR that merged or closed and has no agent.verification yet.

// mergeAndRestart merges the rig's PR, closes the first service and loads a
// second one on the same store: the restarted server. The room is paused
// (no run) until someone steers.
func mergeAndRestart(t *testing.T) (r *prRig, roomID string, pr PullRequest, svc2 *Service) {
	t.Helper()
	r = newPRRig(t)
	roomID, pr = r.openPR()
	r.prs.set(func(p *clients.PullRequest) {
		p.State, p.MergeSHA, p.MergedAt = clients.PRStateMerged, strings.Repeat("d", 40), prT0.Add(2*time.Minute)
	})
	r.tick()
	if ups := r.updates(roomID); len(ups) != 1 || ups[0].State != "merged" {
		t.Fatalf("updates %+v, want one merge", ups)
	}
	r.svc.Close()

	svc2 = New(Deps{Store: r.st, Actions: r.svc.actions, Users: DefaultUsers(), Owners: prOwners(), Run: idleRun, Now: r.clock.Now})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r, roomID, pr, svc2
}

func steer(t *testing.T, svc *Service, roomID string) {
	t.Helper()
	arjun, _ := svc.LookupUser("arjun")
	if _, err := svc.Steer(context.Background(), arjun, roomID, "carry on"); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, 5*time.Second, func() bool {
		st := svc.cached(roomID)
		if st == nil {
			return false
		}
		svc.lockState(st)
		defer svc.unlockRoom(st)
		return st.agentCancel != nil
	}, "steer did not start the agent")
}

// stopRun ends the room's run the way a lost run does (the run's ctx ends);
// the next steer starts a new one.
func stopRun(t *testing.T, svc *Service, roomID string) {
	t.Helper()
	st := svc.cached(roomID)
	svc.lockState(st)
	run, cancel := st.agentRun, st.agentCancel
	svc.unlockRoom(st)
	svc.releaseAgent(st, run, cancel)
}

func TestPRMergedReplayedToANewRun(t *testing.T) {
	_, roomID, pr, svc2 := mergeAndRestart(t)

	steer(t, svc2, roomID)
	in := inboxKinds(t, svc2, roomID)
	if n := countKind(in, "pr_merged"); n != 1 {
		t.Fatalf("new run's inbox %+v: %d pr_merged, want 1", in, n)
	}
	var got agent.HumanInput
	for _, i := range in {
		if i.Kind == "pr_merged" {
			got = i
		}
	}
	want := "PR #" + strconv.Itoa(pr.Number) + " in connect-config merged at 14:02 (merge ddddddd); edited after approval: no"
	if got.ProposalID != pr.ProposalID || got.StepIndex == nil || *got.StepIndex != pr.StepIndex ||
		got.Text != want || !got.At.Equal(prT0.Add(2*time.Minute)) || got.Seq == 0 {
		t.Fatalf("replayed input %+v, want the original pr_merged (%q)", got, want)
	}

	// Another new run before any verdict: told again.
	stopRun(t, svc2, roomID)
	steer(t, svc2, roomID)
	if n := countKind(inboxKinds(t, svc2, roomID), "pr_merged"); n != 1 {
		t.Fatalf("second new run: %d pr_merged, want 1", n)
	}

	// The verdict is written: a later run is not told again.
	st := svc2.cached(roomID)
	svc2.lockState(st)
	_, err := svc2.emitLocked(context.Background(), st, roomID, events.AgentVerification, events.AgentActor, events.AgentVerificationPayload{
		ProposalID: pr.ProposalID, Verdict: "applied", Outcome: "",
	})
	svc2.unlockRoom(st)
	if err != nil {
		t.Fatal(err)
	}
	stopRun(t, svc2, roomID)
	steer(t, svc2, roomID)
	if n := countKind(inboxKinds(t, svc2, roomID), "pr_merged"); n != 0 {
		t.Fatalf("run after the verdict: %d pr_merged, want 0", n)
	}
}

// The pr_merged that starts a run is not queued twice.
func TestPRReplayDedupesTheLiveInput(t *testing.T) {
	r := newPRRig(t)
	roomID, _ := r.openPR()
	stopRun(t, r.svc, roomID) // no run: the merge starts one (startsAgent)
	r.prs.set(func(p *clients.PullRequest) {
		p.State, p.MergeSHA, p.MergedAt = clients.PRStateMerged, strings.Repeat("d", 40), prT0.Add(2*time.Minute)
	})
	r.tick()
	waitForCond(t, 5*time.Second, func() bool {
		st := r.svc.cached(roomID)
		r.svc.lockState(st)
		defer r.svc.unlockRoom(st)
		return st.agentCancel != nil
	}, "the merge did not start the agent")
	if n := countKind(inboxKinds(t, r.svc, roomID), "pr_merged"); n != 1 {
		t.Fatalf("%d pr_merged, want 1", n)
	}
}

// A closed PR is replayed as pr_closed; an open one never is.
func TestPRReplayInputsFromTheSnapshot(t *testing.T) {
	r := newPRRig(t)
	roomID, pr := r.openPR()
	if got := prReplayInputs(mustRoom(t, r.svc, roomID)); len(got) != 0 {
		t.Fatalf("open PR replays %+v", got)
	}
	r.prs.set(func(p *clients.PullRequest) { p.State = clients.PRStateClosed })
	r.tick()
	got := prReplayInputs(mustRoom(t, r.svc, roomID))
	if len(got) != 1 || got[0].Kind != "pr_closed" || got[0].ProposalID != pr.ProposalID {
		t.Fatalf("closed PR replays %+v, want one pr_closed", got)
	}
	// The snapshot rebuilt from the log (a restart) holds the same.
	l, err := r.svc.rebuild(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if got := prReplayInputs(l.snap); len(got) != 1 || got[0].Kind != "pr_closed" {
		t.Fatalf("rebuilt replays %+v", got)
	}
}
