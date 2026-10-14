package evals

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// The executor follows the room's rules: these runs try to make the stack
// "recover" in ways the room never allows.

// twoStepFix proposes schema_set_compatibility then connect_restart_task on
// the seed scenario, through the real propose_fix tool.
func twoStepFix(t *testing.T) string {
	t.Helper()
	fix, err := json.Marshal(map[string]any{
		"title": "allow v2 and restart", "risk": "medium", "rollback": "set BACKWARD again",
		"steps": []map[string]any{
			{"action": "schema_set_compatibility", "params": `{"subject":"pg.public.orders-value","level":"NONE"}`, "description": "d"},
			{"action": "connect_restart_task", "params": `{"connector":"pg-orders-connector","task":0}`, "description": "d"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(fix)
}

func seedPhased(inputs ...Input) Scenario {
	sc := loadSeed()
	sc.Phases = []string{"broken", "compat-loosened", "recovered"}
	sc.Transitions = []Transition{
		{On: TransitionOn{Action: "schema_set_compatibility", OK: okp(true)}, To: "compat-loosened"},
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
	}
	sc.Inputs = inputs
	return sc
}

func actionInput(action string, ok bool, text string) Input {
	return Input{Kind: "action_result", Action: action, OK: okp(ok), Text: text}
}

// The executor stops at the first step that fails: a later step of that
// proposal never runs, so a later input for it is skipped and nothing
// recovers.
func TestFailedStepStopsTheProposal(t *testing.T) {
	sc := seedPhased(
		actionInput("schema_set_compatibility", false, "schemareg: PUT /config/pg.public.orders-value: 503"),
		actionInput("connect_restart_task", true, "restarted pg-orders-connector task 0 (RUNNING)"),
	)
	script := append(goodSchemaChangeScript()[:4], step{proposeFix: twoStepFix(t)}, step{idle: true}, step{idle: true})
	r, tr := evalOnce(t, sc, script)
	if got := strings.Join(r.PhasesVisited, ","); got != "broken" {
		t.Fatalf("phases = %s, want broken only", got)
	}
	msgs := strings.Join(messages(tr), "\n")
	if strings.Contains(msgs, "step 1:") {
		t.Errorf("the step after the failed one ran:\n%s", msgs)
	}
	if len(r.UnexecutedInputs) != 1 || !strings.HasPrefix(r.UnexecutedInputs[0], "inputs[1]:") {
		t.Errorf("unexecuted = %v", r.UnexecutedInputs)
	}
}

// Naming an earlier step runs the rest of the proposal too, as the room's
// executor does, so the last step's result (and agent.Run's verify) comes.
func TestNamingAnEarlierStepRunsTheRest(t *testing.T) {
	sc := seedPhased(actionInput("schema_set_compatibility", true, "pg.public.orders-value compatibility: BACKWARD → NONE"))
	script := append(goodSchemaChangeScript()[:4], step{proposeFix: twoStepFix(t)}, step{idle: true})
	r, tr := evalOnce(t, sc, script)
	if got := strings.Join(r.PhasesVisited, ","); got != "broken,compat-loosened,recovered" {
		t.Fatalf("phases = %s", got)
	}
	msgs := strings.Join(messages(tr), "\n")
	for _, want := range []string{"ack: proposal p_1 step 0: pg.public.orders-value compatibility", "ack: proposal p_1 step 1: succeeded"} {
		if !strings.Contains(msgs, want) {
			t.Errorf("inputs lack %q:\n%s", want, msgs)
		}
	}
}

// A step with params other than the scripted ones is the wrong fix: it
// fails with a not-found result and the stack does not move.
func TestWrongParamsDoNotRecover(t *testing.T) {
	sc := loadScenario(t, "scenarios/heldout/ledger-producer-timeout-recovery.yaml")
	r, tr := evalOnce(t, sc, []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
		{hypothesis: ledgerHypothesis},
		{proposeFix: proposeFixJSON(t, "connect_restart_task", `{"connector":"pg-ledger-connector","task":7}`)},
		{idle: true},
		{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
		{message: "recovered: task 0 is RUNNING."},
	})
	if r.Pass || r.VerifiedAfterFix {
		t.Fatalf("wrong task passed: pass=%v verified=%v failures=%v", r.Pass, r.VerifiedAfterFix, r.Failures)
	}
	if got := strings.Join(r.PhasesVisited, ","); got != "broken" {
		t.Errorf("phases = %s", got)
	}
	if msgs := strings.Join(messages(tr), "\n"); !strings.Contains(msgs, `step 0: actions: connect_restart_task {"connector":"pg-ledger-connector","task":7}: not found`) {
		t.Errorf("wrong-params result:\n%s", msgs)
	}
	mismatch := false
	for _, m := range tr.Replay.Marks {
		if m.Kind == markExecuted && m.ParamsMismatch && m.OK != nil && !*m.OK {
			mismatch = true
		}
		if m.Kind == markTransition {
			t.Errorf("a transition fired: %+v", m)
		}
	}
	if !mismatch {
		t.Errorf("marks = %+v", tr.Replay.Marks)
	}
}

// A proposal the room refuses at creation (out of the pipeline's scope)
// never runs.
func TestRefusedProposalNeverRuns(t *testing.T) {
	sc := seedPhased(actionInput("connect_restart_task", true, "restarted"))
	sc.Resources.Connectors = append(sc.Resources.Connectors, "billing-connector")
	script := append(goodSchemaChangeScript()[:4],
		proposal(ps("connect_restart_task", `{"connector":"billing-connector","task":0}`)),
		step{idle: true})
	r, tr := evalOnce(t, sc, script)
	if r.Proposals != 1 || len(r.BadSteps) == 0 {
		t.Fatalf("the out-of-scope proposal was not created as expected: proposals=%d bad=%v", r.Proposals, r.BadSteps)
	}
	if got := strings.Join(r.PhasesVisited, ","); got != "broken" {
		t.Errorf("phases = %s", got)
	}
	if len(messages(tr)) != 0 || len(r.UnexecutedInputs) != 1 {
		t.Errorf("messages = %q unexecuted = %v", messages(tr), r.UnexecutedInputs)
	}
}

// With two open proposals sharing an action, the input binds to the newest.
func TestInputBindsToNewestProposal(t *testing.T) {
	sc := seedPhased(actionInput("connect_restart_task", true, "restarted pg-orders-connector task 0 (RUNNING)"))
	restart := proposeFixJSON(t, "connect_restart_task", `{"connector":"pg-orders-connector","task":0}`)
	script := append(goodSchemaChangeScript()[:4], step{proposeFix: restart}, step{proposeFix: restart}, step{idle: true})
	_, tr := evalOnce(t, sc, script)
	msgs := strings.Join(messages(tr), "\n")
	if !strings.Contains(msgs, "ack: proposal p_2 step 0: restarted") || strings.Contains(msgs, "proposal p_1") {
		t.Errorf("not bound to the newest proposal:\n%s", msgs)
	}
}

// A 2-step proposal named by two inputs runs once: the approvals come once
// and the second input has nothing left to run.
func TestApprovalsOncePerProposal(t *testing.T) {
	sc := seedPhased(
		actionInput("schema_set_compatibility", true, "BACKWARD → NONE"),
		actionInput("connect_restart_task", true, "restarted"),
	)
	script := append(goodSchemaChangeScript()[:4], step{proposeFix: twoStepFix(t)}, step{idle: true}, step{idle: true})
	r, tr := evalOnce(t, sc, script)
	approvals := 0
	for _, m := range messages(tr) {
		if strings.Contains(m, "approved proposal p_1") {
			approvals++
		}
	}
	if approvals != 2 { // one per layer, once
		t.Errorf("approvals = %d, want 2:\n%s", approvals, strings.Join(messages(tr), "\n"))
	}
	if len(r.UnexecutedInputs) != 1 || !strings.HasPrefix(r.UnexecutedInputs[0], "inputs[1]:") {
		t.Errorf("unexecuted = %v", r.UnexecutedInputs)
	}
}

// Reads after the first executed step are the re-check: budgeted by
// max_verify_calls and kept out of the investigation's tool_calls.
func TestVerifyCallsBudget(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	check := step{tool: "pg_replication_slots", input: map[string]any{}}
	r, _ := evalOnce(t, sc, resumeScript(t, check, check, step{message: recoveredMessage}))
	if !r.Pass || r.VerifyCalls != 2 || r.ToolCalls != 2 {
		t.Fatalf("pass=%v verify=%d tools=%d failures=%v", r.Pass, r.VerifyCalls, r.ToolCalls, r.Failures)
	}
	many := []step{}
	for i := 0; i < defaultMaxVerifyCalls+1; i++ {
		many = append(many, check)
	}
	r, _ = evalOnce(t, sc, resumeScript(t, many...))
	if r.Pass || !strings.Contains(strings.Join(r.Failures, ";"), "over verify budget: 7 > 6") {
		t.Fatalf("failures = %v", r.Failures)
	}
	sc.Expect.MaxVerifyCalls = 7
	if r, _ = evalOnce(t, sc, resumeScript(t, many...)); !r.Pass {
		t.Errorf("max_verify_calls 7 not applied: %v", r.Failures)
	}
}

// A transition with from fires only in that phase.
func TestTransitionFrom(t *testing.T) {
	rp, err := newPhasedReplayer(twoPhaseRecordings(), []string{"broken", "recovered", "worse"}, []Transition{
		{On: TransitionOn{Action: "connect_resume_connector", OK: okp(true)}, To: "recovered"},
		{On: TransitionOn{AfterCalls: &AfterCalls{Tool: "kafka_consumer_lag", N: 1}}, To: "worse", From: "broken"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rp.executed("connect_resume_connector", okp(true))
	serveSummary(t, rp, "kafka_consumer_lag", `{"group":"g"}`)
	if got := rp.currentPhase(); got != "recovered" {
		t.Fatalf("time passing undid the fix: phase %s", got)
	}
}

// A serve in the new phase is marked Changed only when its answer differs
// from what the phase the run left gives; a call that phase could not answer
// counts as changed, and before any transition nothing is.
func TestServeMarksChanged(t *testing.T) {
	recs := append(twoPhaseRecordings(),
		Recording{Tool: "pg_replication_slots", Summary: "slot active=false"},
		Recording{Tool: "pg_replication_slots", Phase: "recovered", Summary: "slot active=false"},
		Recording{Tool: "pg_recent_ddl", Phase: "broken", Summary: "no DDL"},
		Recording{Tool: "pg_long_transactions", Phase: "recovered", Summary: "(no long transactions)"})
	rp, err := newPhasedReplayer(recs, []string{"broken", "recovered"}, []Transition{
		{On: TransitionOn{Action: "connect_resume_connector", OK: okp(true)}, To: "recovered"},
	})
	if err != nil {
		t.Fatal(err)
	}
	serveSummary(t, rp, "pg_recent_ddl", `{}`)
	rp.executed("connect_resume_connector", okp(true))
	serveSummary(t, rp, "pg_replication_slots", `{}`)
	serveSummary(t, rp, "connect_status", `{"connector":"c"}`)
	serveSummary(t, rp, "pg_long_transactions", `{}`)
	var changed []bool
	for _, m := range rp.trace().Marks {
		if m.Kind == markServe {
			changed = append(changed, m.Changed)
		}
	}
	if fmt.Sprint(changed) != "[false false true true]" {
		t.Errorf("changed = %v, want [false false true true]", changed)
	}
}

// An action transition with from fires only in that phase.
func TestActionTransitionFrom(t *testing.T) {
	rp, err := newPhasedReplayer(twoPhaseRecordings(), []string{"broken", "recovered", "worse"}, []Transition{
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "worse", From: "recovered"},
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered", From: "broken"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rp.executed("connect_restart_task", okp(true))
	if got := rp.currentPhase(); got != "recovered" {
		t.Fatalf("phase = %s, want recovered (the first transition is only for recovered)", got)
	}
	rp.executed("connect_restart_task", okp(true))
	if got := rp.currentPhase(); got != "worse" {
		t.Fatalf("phase = %s, want worse", got)
	}
}

// Approvals come once per layer, however many steps the layer has.
func TestApprovalOncePerLayer(t *testing.T) {
	sc := seedPhased(actionInput("connect_restart_connector", true, "restarted failed tasks"))
	fix, err := json.Marshal(map[string]any{
		"title": "restart task 0 and failed tasks", "risk": "low", "rollback": "restarts change no config; nothing to undo",
		"steps": []map[string]any{
			{"action": "connect_restart_task", "params": `{"connector":"pg-orders-connector","task":0}`, "description": "d"},
			{"action": "connect_restart_connector", "params": `{"connector":"pg-orders-connector"}`, "description": "d"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r, tr := evalOnce(t, sc, append(goodSchemaChangeScript()[:4], step{proposeFix: string(fix)}, step{idle: true}))
	if r.Proposals != 1 {
		t.Fatalf("proposals = %d", r.Proposals)
	}
	approvals := 0
	for _, m := range messages(tr) {
		if strings.Contains(m, `layer "kafka-connect" approved`) {
			approvals++
		}
	}
	if approvals != 1 {
		t.Errorf("kafka-connect approvals = %d, want 1:\n%s", approvals, strings.Join(messages(tr), "\n"))
	}
}
