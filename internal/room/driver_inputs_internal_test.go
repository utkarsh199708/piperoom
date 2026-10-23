package room

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store/memstore"
)

// driverInputRig folds a proposal and its step results the way every fold
// path does (Apply, then inputsFor on the folded room) and collects the agent
// inputs each event carries.
type driverInputRig struct {
	t    *testing.T
	snap RoomSnapshot
	seq  int64
	got  []agent.HumanInput
}

func (r *driverInputRig) fold(typ events.Type, actor events.Actor, payload any) {
	r.t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		r.t.Fatal(err)
	}
	r.seq++
	e := events.Event{Seq: r.seq, RoomID: "r1", Type: typ, Actor: actor, At: time.Unix(r.seq, 0).UTC(), Payload: raw}
	r.snap = Apply(r.snap, e)
	r.got = append(r.got, inputsFor(r.snap, e)...)
}

func (r *driverInputRig) of(kind string) []agent.HumanInput {
	var out []agent.HumanInput
	for _, in := range r.got {
		if in.Kind == kind {
			out = append(out, in)
		}
	}
	return out
}

var arjunActor = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}

func threeStepProposal(origin string) events.ProposalCreatedPayload {
	step := func(a string) events.ProposalStep {
		return events.ProposalStep{Action: a, Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"orders-src"}`)}
	}
	return events.ProposalCreatedPayload{
		ProposalID: "pr_1", Title: "restart the connector", Risk: "a short gap", Rollback: "none needed, restart again",
		Steps:          []events.ProposalStep{step("connect_pause_connector"), step("connect_update_connector_config"), step("connect_restart_task")},
		RequiredLayers: []string{"kafka-connect"}, Origin: origin,
	}
}

func bools(v ...*bool) []*bool { return v }

func stepsOKString(s []*bool) string {
	parts := make([]string, len(s))
	for i, b := range s {
		switch {
		case b == nil:
			parts[i] = "nil"
		case *b:
			parts[i] = "true"
		default:
			parts[i] = "false"
		}
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// TestDriverProposalInputs pins the P3c agent inputs of a driver-authored
// proposal: one proposal_created when it is created, one proposal_executed
// with the action.executed that ends its run (the last step, or the first
// failed or unknown step), StepsOK one entry per step with a step that never
// ran as false. The per-step action_result inputs stay. An agent proposal
// (origin "agent" or empty, as before P3) yields neither.
func TestDriverProposalInputs(t *testing.T) {
	type res struct {
		ok, unknown bool
	}
	ok, fail, unknown := res{ok: true}, res{}, res{unknown: true}
	cases := []struct {
		name      string
		origin    string
		steps     []res
		wantExec  bool
		wantSteps []*bool
	}{
		{"driver, all ok", events.ProposalOriginDriver, []res{ok, ok, ok}, true, bools(ptr(true), ptr(true), ptr(true))},
		{"driver, step 2 of 3 fails", events.ProposalOriginDriver, []res{ok, fail}, true, bools(ptr(true), ptr(false), ptr(false))},
		{"driver, step 1 fails", events.ProposalOriginDriver, []res{fail}, true, bools(ptr(false), ptr(false), ptr(false))},
		{"driver, step 2 outcome unknown", events.ProposalOriginDriver, []res{ok, unknown}, true, bools(ptr(true), nil, ptr(false))},
		{"driver, last step outcome unknown", events.ProposalOriginDriver, []res{ok, ok, unknown}, true, bools(ptr(true), ptr(true), nil)},
		{"driver, still running", events.ProposalOriginDriver, []res{ok, ok}, false, nil},
		{"agent", events.ProposalOriginAgent, []res{ok, ok, ok}, false, nil},
		{"agent, before P3 (no origin)", "", []res{ok, fail}, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &driverInputRig{t: t, snap: NewSnapshot()}
			actor := events.AgentActor
			if tc.origin == events.ProposalOriginDriver {
				actor = arjunActor
			}
			r.fold(events.ProposalCreated, actor, threeStepProposal(tc.origin))
			for i, s := range tc.steps {
				r.fold(events.ActionExecuted, executorActor, events.ActionExecutedPayload{
					ProposalID: "pr_1", StepIndex: i, OK: s.ok, OutcomeUnknown: s.unknown, Output: "done",
				})
			}
			if n := len(r.of("action_result")); n != len(tc.steps) {
				t.Fatalf("%d action_result inputs, want %d (one per step run)", n, len(tc.steps))
			}
			created := r.of("proposal_created")
			executed := r.of("proposal_executed")
			if tc.origin != events.ProposalOriginDriver {
				if len(created)+len(executed) != 0 {
					t.Fatalf("agent proposal produced driver inputs: %+v", r.got)
				}
				return
			}
			if len(created) != 1 {
				t.Fatalf("%d proposal_created inputs, want 1", len(created))
			}
			c := created[0]
			if c.Seq != 1 || c.ProposalID != "pr_1" || c.Origin != events.ProposalOriginDriver || c.Actor.ID != "arjun" || c.At.IsZero() {
				t.Fatalf("proposal_created fields: %+v", c)
			}
			for _, want := range []string{"restart the connector", "connect_update_connector_config", "kafka-connect", `{"connector":"orders-src"}`, "a short gap", "none needed, restart again"} {
				if !strings.Contains(c.Text, want) {
					t.Fatalf("proposal_created text lacks %q:\n%s", want, c.Text)
				}
			}
			if !tc.wantExec {
				if len(executed) != 0 {
					t.Fatalf("proposal_executed before the run ended: %+v", executed)
				}
				return
			}
			if len(executed) != 1 {
				t.Fatalf("%d proposal_executed inputs, want 1", len(executed))
			}
			x := executed[0]
			if x.ProposalID != "pr_1" || x.Origin != events.ProposalOriginDriver || x.Seq != r.seq || x.OK != nil || x.StepIndex != nil {
				t.Fatalf("proposal_executed fields: %+v", x)
			}
			if got, want := stepsOKString(x.StepsOK), stepsOKString(tc.wantSteps); got != want {
				t.Fatalf("StepsOK = %s, want %s", got, want)
			}
			// It follows the last step's action_result, in log order.
			if last := r.got[len(r.got)-1]; last.Kind != "proposal_executed" {
				t.Fatalf("last input is %q, want proposal_executed after the action_result", last.Kind)
			}
		})
	}
}

// TestDriverProposalExecutedOnceAndPure: the input comes only with the event
// that ended the run (a later event yields none), and Apply leaves the
// snapshot it was given untouched (the outcomes are copy-on-write).
func TestDriverProposalExecutedOnceAndPure(t *testing.T) {
	r := &driverInputRig{t: t, snap: NewSnapshot()}
	r.fold(events.ProposalCreated, arjunActor, threeStepProposal(events.ProposalOriginDriver))
	r.fold(events.ActionExecuted, executorActor, events.ActionExecutedPayload{ProposalID: "pr_1", StepIndex: 0, OK: true})
	before := r.snap
	r.fold(events.ActionExecuted, executorActor, events.ActionExecutedPayload{ProposalID: "pr_1", StepIndex: 1, OK: true})
	if got := before.OpenProposals[0].outcomes; len(got) != 3 || got[1] != outcomeNotRun {
		t.Fatalf("Apply mutated the previous snapshot's outcomes: %v", got)
	}
	r.fold(events.ActionExecuted, executorActor, events.ActionExecutedPayload{ProposalID: "pr_1", StepIndex: 2, OK: true})
	r.fold(events.SteerSent, arjunActor, events.SteerSentPayload{Text: "verify it"})
	// A stray result for the decided proposal (never logged in practice) does
	// not send it again.
	r.fold(events.ActionExecuted, executorActor, events.ActionExecutedPayload{ProposalID: "pr_1", StepIndex: 2, OK: true})
	if n := len(r.of("proposal_executed")); n != 1 {
		t.Fatalf("%d proposal_executed inputs, want 1", n)
	}
}

// TestDriverProposalInputsSurviveRebuild: a room rebuilt from the log (a
// restart, or another replica taking the agent lease) keeps the same driver
// inputs pending as the replica that folded the events live, because both
// derive them from the folded room.
func TestDriverProposalInputsSurviveRebuild(t *testing.T) {
	ctx := context.Background()
	ms := memstore.New()
	svc := New(Deps{Store: ms, Users: DefaultUsers(), Run: idleRun})
	t.Cleanup(svc.Close)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	live := &roomState{snap: NewSnapshot()}
	appendEv := func(typ events.Type, actor events.Actor, payload any) {
		t.Helper()
		if _, err := ms.Append(ctx, snap.ID, typ, actor, payload); err != nil {
			t.Fatal(err)
		}
	}
	appendEv(events.ProposalCreated, arjunActor, threeStepProposal(events.ProposalOriginDriver))
	appendEv(events.ActionExecuted, executorActor, events.ActionExecutedPayload{ProposalID: "pr_1", StepIndex: 0, OK: true})
	appendEv(events.ActionExecuted, executorActor, events.ActionExecutedPayload{ProposalID: "pr_1", StepIndex: 1, OK: false})
	all, err := ms.Since(ctx, snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range all {
		live.snap = Apply(live.snap, e)
		trackPending(live, e)
	}
	l, err := svc.rebuild(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	kinds := func(ins []agent.HumanInput) string {
		var k []string
		for _, in := range ins {
			k = append(k, in.Kind)
			if in.Kind == "proposal_executed" {
				k = append(k, stepsOKString(in.StepsOK))
			}
		}
		return strings.Join(k, " ")
	}
	want := "proposal_created action_result action_result proposal_executed [true,false,false]"
	if got := kinds(live.pending); !strings.HasSuffix(got, want) {
		t.Fatalf("live pending = %q, want it to end with %q", got, want)
	}
	if got, live := kinds(l.pending), kinds(live.pending); got != live {
		t.Fatalf("rebuilt pending = %q, live = %q", got, live)
	}
}

// TestAutoApproveNeverSchedulesRouteDriverProposal: a proposal authored
// through the driver route (CreateDriverProposal, which forces origin
// "driver") gets no countdown and no policy approval, even for an action on
// the auto-approve list with a passing preview, while the same action proposed
// by the agent is scheduled.
func TestAutoApproveNeverSchedulesRouteDriverProposal(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomID := r.open()
	a, _ := r.acts.reg.Get("connect_restart_task")
	ev, err := r.svc.CreateDriverProposal(context.Background(), r.user("arjun"), roomID, events.ProposalCreatedPayload{
		Title: "restart", Risk: "low", Rollback: "restart again", Origin: events.ProposalOriginAgent,
		Steps: []events.ProposalStep{{Action: "connect_restart_task", Layer: a.Layer(), Params: json.RawMessage(`{}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := events.Decode[events.ProposalCreatedPayload](ev)
	if err != nil || p.Origin != events.ProposalOriginDriver {
		t.Fatalf("route proposal origin = %q (%v), want driver", p.Origin, err)
	}
	waitForCond(t, 5*time.Second, func() bool {
		op, ok := openProposal(mustRoom(t, r.svc, roomID), p.ProposalID)
		return ok && op.Preview != nil
	}, "preview never folded")
	// The agent's identical proposal is scheduled: the policy is on.
	agentPID := r.propose(roomID, "", "connect_restart_task")
	if r.proposal(roomID, agentPID).Scheduled == nil {
		t.Fatal("control: the agent's proposal got no countdown")
	}
	if got := r.proposal(roomID, p.ProposalID); got.Scheduled != nil {
		t.Fatalf("driver proposal scheduled: %+v", got.Scheduled)
	}
	for _, e := range logOf(t, ms, roomID, events.ProposalScheduled) {
		if sp, _ := events.Decode[events.ProposalScheduledPayload](e); sp.ProposalID == p.ProposalID {
			t.Fatal("proposal.scheduled logged for the driver's proposal")
		}
	}
	c.Advance(time.Hour)
	r.tick()
	for _, e := range logOf(t, ms, roomID, events.ApprovalGiven) {
		if ap, _ := events.Decode[events.ApprovalGivenPayload](e); ap.ProposalID == p.ProposalID {
			t.Fatalf("driver proposal approved by %+v", e.Actor)
		}
	}
}

// The runner type-asserts Deps.Proposals to agent.ProposalStepsReader (P3e,
// internal/agent/driverproposal.go); the room passes proposalCreator by value.
var (
	_ agent.ProposalStepsReader = (*proposalCreator)(nil)
	_ agent.ProposalStepsReader = proposalCreator{}
)

// TestProposalCreatorProposalSteps: the room's agent.Proposals returns the
// stored steps of a proposal in its own room, and a not-found error for an
// unknown id or another room's proposal.
func TestProposalCreatorProposalSteps(t *testing.T) {
	c := &testClock{t: aaT0}
	ms := memstore.NewWithClock(c.Now)
	r := newAARig(t, ms, c, newAAActions(t), aaOpts{})
	roomA, roomB := r.open(), r.open()
	pid := r.propose(roomA, "", "connect_restart_task", "consumer_restart")
	ctx := context.Background()

	var reader any = proposalCreator{s: r.svc, roomID: roomA, run: 1}
	pr, ok := reader.(agent.ProposalStepsReader)
	if !ok {
		t.Fatal("proposalCreator does not implement ProposalSteps")
	}
	steps, err := pr.ProposalSteps(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[0].Action != "connect_restart_task" || steps[1].Action != "consumer_restart" || steps[0].Layer == "" {
		t.Fatalf("steps = %+v", steps)
	}
	for name, tc := range map[string]struct {
		room, pid string
	}{
		"unknown id":         {roomA, "p_missing"},
		"another room's one": {roomB, pid},
	} {
		_, err := proposalCreator{s: r.svc, roomID: tc.room, run: 1}.ProposalSteps(ctx, tc.pid)
		var re *Error
		if !errors.As(err, &re) || re.Code != codeNotFound {
			t.Fatalf("%s: err = %v, want not_found", name, err)
		}
	}
}

// TestDriverProposalTextBounded: the proposal_created Text is at most
// maxDriverProposalText bytes, valid UTF-8, and says it was cut; a small
// proposal is rendered whole.
func TestDriverProposalTextBounded(t *testing.T) {
	small := threeStepProposal(events.ProposalOriginDriver)
	if got := driverProposalText(small); strings.Contains(got, "truncated") || !strings.Contains(got, "rollback: none needed") {
		t.Fatalf("small proposal text = %q", got)
	}
	big := threeStepProposal(events.ProposalOriginDriver)
	big.Risk = strings.Repeat("é", 1<<19) // 1 MB of two-byte runes
	got := driverProposalText(big)
	if len(got) > maxDriverProposalText {
		t.Fatalf("len = %d, want <= %d", len(got), maxDriverProposalText)
	}
	if !utf8.ValidString(got) || !strings.HasSuffix(got, driverProposalTruncated) || !strings.HasPrefix(got, "title: restart the connector") {
		t.Fatalf("bounded text: valid=%v suffix=%v prefix=%q", utf8.ValidString(got), strings.HasSuffix(got, driverProposalTruncated), got[:40])
	}
}
