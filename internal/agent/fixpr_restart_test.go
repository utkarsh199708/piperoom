package agent

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Review fixes of Q7f: restart, partial execution, the pull_request kind.

// originProposals is a room Proposals that also reads a stored proposal's
// origin (ProposalOriginReader).
type originProposals struct {
	stepsProposals
	origin string
}

func (p *originProposals) ProposalOrigin(context.Context, string) (string, error) {
	return p.origin, nil
}

func noteHas(out actionOutcome, s string) bool {
	for _, n := range out.notes {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}

const mergedText = "PR #12 in connect-config merged at 09:00 (merge 1a2b3c4); edited after approval: no"

// A restart (or lease move) during the apply wait loses the run's state; the
// room sends pr_merged again on run start, and the new run rebuilds the
// proposal from the room's stored steps, waits and writes a verdict. Who
// wrote it comes from its origin, never from whether the run has a record.
func TestPRMergedAfterRestartWritesVerdict(t *testing.T) {
	steps := []events.ProposalStep{prStep(pluginApplied)}
	merged := newTickClock().Now()
	in := HumanInput{Kind: "pr_merged", At: merged, ProposalID: "p_1", StepIndex: intp(0), Text: mergedText}

	// Run 1: the merge starts a wait, then the run ends before it finishes.
	sink1 := &fakeSink{}
	r1, _ := newPRRunner(t, sink1, []tools.Tool{prPluginsTool(1 << 30)}, steps)
	r1.clk = &blockClock{tickClock: *newTickClock()}
	ctx, cancel := context.WithCancel(context.Background())
	r1.handleActionResults(ctx, []HumanInput{in})
	cancel()
	r1.applies.stop()
	if n := len(prVerifications(t, sink1)); n != 0 {
		t.Fatalf("run 1 wrote %d verifications", n)
	}

	for name, tc := range map[string]struct {
		proposals  Proposals
		origin     string
		wantOrigin string
		wantHead   string
		noHead     string
	}{
		"origin agent from the room": {
			proposals: &originProposals{stepsProposals: stepsProposals{steps: steps}}, wantOrigin: events.ProposalOriginAgent, noHead: "driver's fix"},
		"origin driver on the input": {
			proposals: &stepsProposals{steps: steps}, origin: events.ProposalOriginDriver, wantOrigin: events.ProposalOriginDriver, wantHead: `proposal "p_1" is the driver's fix, not yours.`},
		"origin driver from the room": {
			proposals: &originProposals{stepsProposals: stepsProposals{steps: steps}, origin: events.ProposalOriginDriver}, wantOrigin: events.ProposalOriginDriver, wantHead: "driver's fix, not yours"},
		"origin unknown": {
			proposals: &stepsProposals{steps: steps}, wantHead: "picked up again after an agent restart", noHead: "driver's fix"},
	} {
		t.Run(name, func(t *testing.T) {
			// Run 2: no record of p_1.
			sink := &fakeSink{}
			r, _ := newPRRunner(t, sink, []tools.Tool{prPluginsTool(1)}, nil)
			r.deps.Proposals = tc.proposals
			again := in
			again.Origin = tc.origin
			out := r.handleActionResults(context.Background(), []HumanInput{again})
			if !noteHas(out, "not verified yet") {
				t.Fatalf("merge outcome %+v", out)
			}
			res := awaitApplied(t, r)
			v := prVerifications(t, sink)
			if len(v) != 1 || v[0].ProposalID != "p_1" || len(v[0].Checks) == 0 || v[0].Checks[0].Name != "applied_when" || !v[0].Checks[0].OK {
				t.Fatalf("verifications %+v", v)
			}
			if tc.wantHead != "" && !noteHas(res, tc.wantHead) {
				t.Errorf("notes %q lack %q", res.notes, tc.wantHead)
			}
			if tc.noHead != "" && noteHas(res, tc.noHead) {
				t.Errorf("notes %q say %q", res.notes, tc.noHead)
			}
			if got := r.deferred["p_1"].origin; got != tc.wantOrigin {
				t.Errorf("origin %q, want %q", got, tc.wantOrigin)
			}
			if _, stored := r.driverVerdicts["p_1"]; stored != (tc.wantOrigin == events.ProposalOriginDriver) {
				t.Errorf("driver verdict stored %v for origin %q", stored, tc.wantOrigin)
			}
		})
	}
}

// Without a steps reader the restarted run still waits (the default settle
// for an unknown applied_when) and writes a verdict that says so.
func TestPRMergedAfterRestartStepsUnreadable(t *testing.T) {
	sink := &fakeSink{}
	r, clk := newPRRunner(t, sink, nil, nil)
	merged := clk.Now()
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "pr_merged", At: merged, ProposalID: "p_9", StepIndex: intp(1), Text: mergedText}})
	res := awaitApplied(t, r)
	if got := clk.Now().Sub(merged); got != defaultApplySettle {
		t.Errorf("waited %s, want the default settle %s", got, defaultApplySettle)
	}
	if v := prVerifications(t, sink); len(v) != 1 || v[0].ProposalID != "p_9" {
		t.Fatalf("verifications %+v", v)
	}
	if !noteHas(res, "stored steps were not readable") || !res.needsModel {
		t.Errorf("outcome %+v", res)
	}
}

// A rebuilt proposal with a PR step this run never heard of: an honest
// non-final verdict once the merged one is seen, then the full verification
// when the other one merges.
func TestPRMergedAfterRestartOtherStepUnheard(t *testing.T) {
	steps := []events.ProposalStep{prStep(pluginApplied), prStep(pluginApplied)}
	sink := &fakeSink{}
	r, clk := newPRRunner(t, sink, []tools.Tool{prPluginsTool(1)}, nil)
	r.deps.Proposals = &stepsProposals{steps: steps}
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "pr_merged", At: clk.Now(), ProposalID: "p_1", StepIndex: intp(0), Text: mergedText}})
	res := awaitApplied(t, r)
	v := prVerifications(t, sink)
	if len(v) != 1 || v[0].Recovered || v[0].Outcome != "" || !strings.Contains(v[0].Verdict, "does not know whether the pull request of step(s) 2 opened") {
		t.Fatalf("interim verifications %+v", v)
	}
	if !res.broken || r.deferred["p_1"].final {
		t.Fatalf("interim outcome %+v, final %v", res, r.deferred["p_1"].final)
	}
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "pr_merged", At: clk.Now(), ProposalID: "p_1", StepIndex: intp(1), Text: "PR #13 in connect-config merged at 09:05 (merge 2b3c4d5); edited after approval: no"}})
	awaitApplied(t, r)
	v = prVerifications(t, sink)
	if len(v) != 2 || len(v[1].Checks) < 2 || v[1].Checks[0].Name != "applied_when" || v[1].Checks[1].Name != "applied_when" {
		t.Fatalf("final verifications %+v", v)
	}
	if !r.deferred["p_1"].final {
		t.Error("the proposal is not final after every step was seen")
	}
}

// Step 1 opens PR #12, step 2 fails: once #12 merges and is seen, the
// proposal gets a final not_applied verdict instead of waiting for step 2.
func TestPartlyExecutedPRProposalGetsVerdict(t *testing.T) {
	steps := []events.ProposalStep{prStep(pluginApplied), prStep(pluginApplied)}
	sink := &fakeSink{}
	r, clk := newPRRunner(t, sink, []tools.Tool{prPluginsTool(1)}, steps)
	ok, failed := true, false
	r.handleActionResults(context.Background(), []HumanInput{
		{Kind: "action_result", OK: &ok, ProposalID: "p_1", StepIndex: intp(0), Text: "opened pull request #12"},
		{Kind: "action_result", OK: &failed, ProposalID: "p_1", StepIndex: intp(1), Text: "base moved"},
	})
	if n := len(prVerifications(t, sink)); n != 0 {
		t.Fatalf("%d verifications before the merge", n)
	}
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "pr_merged", At: clk.Now(), ProposalID: "p_1", StepIndex: intp(0), Text: mergedText}})
	res := awaitApplied(t, r)
	v := prVerifications(t, sink)
	if len(v) != 1 || v[0].Outcome != events.DeferredNotApplied || v[0].Recovered {
		t.Fatalf("verifications %+v", v)
	}
	if !strings.Contains(v[0].Verdict, "step 2 did not succeed") {
		t.Errorf("verdict %q", v[0].Verdict)
	}
	var names []string
	for _, c := range v[0].Checks {
		names = append(names, c.Name+":"+strconv.FormatBool(c.OK))
	}
	if strings.Join(names, ",") != "applied_when:true,step_result:false" {
		t.Errorf("checks %v", names)
	}
	if !res.broken || !r.deferred["p_1"].final {
		t.Errorf("outcome %+v", res)
	}
	if rec, _ := r.proposals.get("p_1"); !rec.verified || rec.recovered {
		t.Errorf("record %+v", rec)
	}
}

// An unknown outcome after the opened PR was already merged and seen: the
// verdict is written at that result.
func TestPartlyExecutedUnknownAfterApply(t *testing.T) {
	steps := []events.ProposalStep{prStep(pluginApplied), prStep(pluginApplied)}
	sink := &fakeSink{}
	r, clk := newPRRunner(t, sink, []tools.Tool{prPluginsTool(1)}, steps)
	ok := true
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "action_result", OK: &ok, ProposalID: "p_1", StepIndex: intp(0)}})
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "pr_merged", At: clk.Now(), ProposalID: "p_1", StepIndex: intp(0)}})
	awaitApplied(t, r)
	if n := len(prVerifications(t, sink)); n != 0 {
		t.Fatalf("%d verifications while step 2 is open", n)
	}
	out := r.handleActionResults(context.Background(), []HumanInput{{Kind: "action_result", ProposalID: "p_1", StepIndex: intp(1)}})
	if v := prVerifications(t, sink); len(v) != 1 || v[0].Outcome != events.DeferredNotApplied {
		t.Fatalf("verifications %+v", v)
	}
	if !out.broken || !noteHas(out, "did not succeed") {
		t.Errorf("outcome %+v", out)
	}
}

// The driver's fix failed at step 2 after step 1 opened a PR: the failure's
// verdict is final, so the later merge checks nothing again.
func TestDriverFailedPRProposalFinal(t *testing.T) {
	steps := []events.ProposalStep{prStep(pluginApplied), prStep(pluginApplied)}
	sink := &fakeSink{}
	r, clk := newPRRunner(t, sink, []tools.Tool{prPluginsTool(1)}, nil)
	r.deps.Proposals = &stepsProposals{steps: steps}
	ok, failed := true, false
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "proposal_executed", Origin: events.ProposalOriginDriver, ProposalID: "p_d1", StepsOK: []*bool{&ok, &failed}}})
	r.handleActionResults(context.Background(), []HumanInput{{Kind: "pr_merged", At: clk.Now(), ProposalID: "p_d1", StepIndex: intp(0)}})
	if r.applies.active() {
		t.Fatal("a merge of a failed driver proposal started a wait")
	}
	if v := prVerifications(t, sink); len(v) != 1 {
		t.Fatalf("verifications %+v", v)
	}
}

// A pull_request what-changed entry never shows a merged change in the
// system: the room writes one on every fix-PR merge.
func TestAppliedChangeIgnoresPullRequestKind(t *testing.T) {
	at := time.Date(2026, 10, 22, 9, 1, 0, 0, time.UTC)
	wc := &fakeVerifyTool{name: "what_changed", schema: map[string]any{"type": "object"}, runFn: func() (tools.Result, error) {
		return tools.Result{Summary: "x", Data: map[string]any{"changes": []map[string]any{{"kind": "pull_request", "at": at}}}}, nil
	}}
	r, clk := newPRRunner(t, &fakeSink{}, []tools.Tool{wc}, nil)
	job := applyJob{id: "p_1", mergedAt: clk.Now(), deadline: clk.Now().Add(time.Hour)}
	job.when, job.known = appliedWhenOf(json.RawMessage(`{"applied_when":{"kind":"change","kinds":["pull_request","deploy"]}}`))
	seen, detail, err := r.readApplied(context.Background(), job)
	if seen || err != nil {
		t.Fatalf("seen %v (%s), err %v", seen, detail, err)
	}
}
