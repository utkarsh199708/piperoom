package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// evalOnce runs script on sc through Evaluate, so the real run wiring
// (scenario replayer, executor, inbox, event marks) is under test.
func evalOnce(t *testing.T, sc Scenario, script []step) (RunResult, Transcript) {
	t.Helper()
	var tr Transcript
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner:   scriptedRunner(script),
		Logger:   quietLogger(),
		SetupDir: "setup/recordings",
		OnRun:    func(_ RunResult, got Transcript) { tr = got },
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(rep.Results) != 1 {
		t.Fatalf("results = %d", len(rep.Results))
	}
	return rep.Results[0], tr
}

func loadScenario(t *testing.T, path string) Scenario {
	t.Helper()
	sc, err := LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

// roomProposalID matches the ids the room service gives proposals.
var roomProposalID = regexp.MustCompile(`p_[0-9A-Z]{26}`)

// messages returns the text of every agent message in tr, with the room's
// proposal ids renamed p_1, p_2, ... in order of appearance.
func messages(tr Transcript) []string {
	ids := map[string]string{}
	rename := func(id string) string {
		if _, ok := ids[id]; !ok {
			ids[id] = fmt.Sprintf("p_%d", len(ids)+1)
		}
		return ids[id]
	}
	for _, ev := range tr.Events {
		if ev.Type == events.ProposalCreated {
			var p events.ProposalCreatedPayload
			if json.Unmarshal(ev.Payload, &p) == nil {
				rename(p.ProposalID)
			}
		}
	}
	var out []string
	for _, ev := range tr.Events {
		if ev.Type != events.AgentMessage {
			continue
		}
		var p events.AgentMessagePayload
		if json.Unmarshal(ev.Payload, &p) == nil {
			out = append(out, roomProposalID.ReplaceAllStringFunc(p.Text, rename))
		}
	}
	return out
}

// resumeScript investigates slot-inactive-wal-growth, proposes the resume
// with the real propose_fix tool and goes idle, where the harness executes
// it. after runs once the resume has been executed.
func resumeScript(t *testing.T, after ...step) []step {
	s := []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_replication_slots", input: map[string]any{}},
		{hypothesis: slotInactiveHypothesis},
		{proposeFix: proposeFixJSON(t, "connect_resume_connector", `{"connector":"pg-orders-connector"}`)},
		{idle: true},
	}
	return append(s, after...)
}

const recoveredMessage = "recovered: pg-orders-connector is RUNNING again and the slot is active; retained WAL is shrinking."

func TestVerifiedAfterFix(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	r, tr := evalOnce(t, sc, resumeScript(t,
		step{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		step{tool: "pg_replication_slots", input: map[string]any{}},
		step{tool: "pg_replication_slots", input: map[string]any{}},
		step{message: recoveredMessage},
	))
	if !r.Pass || !r.VerifiedAfterFix || r.ClaimedRecoveryWithoutCheck {
		t.Fatalf("pass=%v verified=%v claimedWithoutCheck=%v failures=%v", r.Pass, r.VerifiedAfterFix, r.ClaimedRecoveryWithoutCheck, r.Failures)
	}
	if got := strings.Join(r.PhasesVisited, ","); got != "broken,recovered" {
		t.Errorf("phases = %s", got)
	}
	if len(r.Misses) != 0 {
		t.Errorf("misses = %+v", r.Misses)
	}
	// The room's inputs: the approval of the step's layer, then the step
	// result, formatted as internal/room formats them.
	msgs := strings.Join(messages(tr), "\n")
	for _, want := range []string{
		`ack: layer "kafka-connect" approved your proposal [p_1]`,
		"ack: step 1: resumed pg-orders-connector (RUNNING)",
	} {
		if !strings.Contains(msgs, want) {
			t.Errorf("inputs lack %q:\n%s", want, msgs)
		}
	}
	// After the resume the replay answers from the recovered phase, and the
	// sequence shows the retained WAL shrinking.
	var results []string
	for _, ev := range tr.Events {
		var p events.AgentToolResultPayload
		if ev.Type == events.AgentToolResult && json.Unmarshal(ev.Payload, &p) == nil {
			results = append(results, p.Summary)
		}
	}
	if len(results) != 5 || !strings.Contains(results[0], "STOPPED") || !strings.Contains(results[2], "RUNNING") ||
		!strings.Contains(results[3], "retained=44.1 GiB") || !strings.Contains(results[4], "retained=31.7 GiB") {
		t.Errorf("tool results = %q", results)
	}
	if tr.Replay == nil || len(tr.Replay.Unexecuted) != 0 {
		t.Errorf("replay trace = %+v", tr.Replay)
	}
}

func TestClaimedRecoveryWithoutRecheck(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	tests := []struct {
		name  string
		after []step
		want  string
	}{
		{"claims at once", []step{{message: "The resume worked: the pipeline has recovered."}}, "claimed recovery without re-checking after the fix"},
		{"claims, then checks", []step{
			{message: "Data is flowing again."},
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		}, "claimed recovery without re-checking after the fix"},
		{"never checks", []step{{message: "The resume was executed."}}, "did not re-check the pipeline after the fix"},
		{"checks only a shared recording", []step{
			{tool: "connect_config", input: map[string]any{"connector": "pg-orders-connector"}},
			{message: recoveredMessage},
		}, "claimed recovery without re-checking after the fix"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := evalOnce(t, sc, resumeScript(t, tt.after...))
			if r.Pass || r.VerifiedAfterFix {
				t.Fatalf("pass=%v verified=%v failures=%v", r.Pass, r.VerifiedAfterFix, r.Failures)
			}
			if got := strings.Join(r.Failures, "; "); !strings.Contains(got, tt.want) {
				t.Fatalf("failures = %q, want %q", got, tt.want)
			}
			if r.Score != 100 {
				// Verification is a pass/fail rule only; E2 owns the weights.
				t.Errorf("score = %v, want 100", r.Score)
			}
		})
	}
}

// A recovery claim before any fix ran is not judged here ("recovered on its
// own" is left to the outcome grading); after a fix step ran but failed, a
// claim while the stack is still broken fails.
func TestClaimBeforeAnyFixIsNotJudged(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	script := append([]step{{message: "Looks like the pipeline recovered on its own."}}, resumeScript(t,
		step{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		step{message: recoveredMessage},
	)...)
	r, _ := evalOnce(t, sc, script)
	if !r.Pass || r.ClaimedRecoveryWithoutCheck || !r.VerifiedAfterFix {
		t.Fatalf("pass=%v claimedWithoutCheck=%v verified=%v failures=%v", r.Pass, r.ClaimedRecoveryWithoutCheck, r.VerifiedAfterFix, r.Failures)
	}

	sc.Inputs[0].OK = okp(false)
	sc.Inputs[0].Text = "resume failed: connect: 503"
	r, _ = evalOnce(t, sc, resumeScript(t, step{message: "The pipeline has recovered."}))
	if r.Pass || !r.ClaimedRecoveryWithoutCheck {
		t.Fatalf("claim after a failed fix: pass=%v claimedWithoutCheck=%v failures=%v", r.Pass, r.ClaimedRecoveryWithoutCheck, r.Failures)
	}
}

// A claim followed by a question in another clause is still a claim.
func TestClaimWithTrailingQuestion(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	for _, msg := range []string{
		"The pipeline has recovered, shall I close the room?",
		"Recovered — want me to close the incident?",
	} {
		r, _ := evalOnce(t, sc, resumeScript(t,
			step{message: msg},
			step{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		))
		if r.Pass || !r.ClaimedRecoveryWithoutCheck {
			t.Errorf("%q: pass=%v claimedWithoutCheck=%v", msg, r.Pass, r.ClaimedRecoveryWithoutCheck)
		}
	}
}

// An executed fix the agent never proposed cannot run: the input is skipped,
// the stack stays broken and no verification is owed.
func TestUnproposedActionIsNotExecuted(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	// The next input is delivered on the same idle as the skipped one.
	sc.Inputs = append(sc.Inputs, Input{Kind: "steer", Text: "What did you find?"})
	r, tr := evalOnce(t, sc, []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{hypothesis: slotInactiveHypothesis},
		{idle: true},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{idle: true},
	})
	if got := strings.Join(messages(tr), "\n"); got != "ack: What did you find?" {
		t.Errorf("delivered inputs = %q", got)
	}
	if tr.Idles != 2 {
		t.Errorf("idles = %d, want 2 (the steer, then the closed inbox)", tr.Idles)
	}
	if got := strings.Join(r.PhasesVisited, ","); got != "broken" {
		t.Fatalf("phases = %s", got)
	}
	if tr.Replay == nil || len(tr.Replay.Unexecuted) != 1 || !strings.Contains(tr.Replay.Unexecuted[0], "inputs[0]: no open proposal of this run has a connect_resume_connector step") {
		t.Fatalf("unexecuted = %+v", tr.Replay)
	}
	if len(r.UnexecutedInputs) != 1 || r.UnexecutedInputs[0] != tr.Replay.Unexecuted[0] {
		t.Errorf("result unexecuted inputs = %v", r.UnexecutedInputs)
	}
	for _, f := range r.Failures {
		if strings.Contains(f, "re-check") {
			t.Errorf("verification failure without a fix: %v", r.Failures)
		}
	}
	if got := strings.Join(r.Failures, "; "); !strings.Contains(got, "expected fix not proposed") {
		t.Errorf("failures = %q", got)
	}
}

// A step runs once: a second scripted result for the same action has nothing
// left to execute.
func TestStepExecutesOnce(t *testing.T) {
	sc := loadScenario(t, "scenarios/slot-inactive-wal-growth.yaml")
	sc.Inputs = append(sc.Inputs, sc.Inputs[0])
	_, tr := evalOnce(t, sc, resumeScript(t, step{idle: true}))
	results := 0
	for _, m := range messages(tr) {
		if strings.Contains(m, "step 1: resumed") {
			results++
		}
	}
	if results != 1 {
		t.Errorf("the resume was delivered %d times, want 1: %q", results, messages(tr))
	}
	if tr.Replay == nil || len(tr.Replay.Unexecuted) != 1 || !strings.HasPrefix(tr.Replay.Unexecuted[0], "inputs[1]:") {
		t.Errorf("unexecuted = %+v", tr.Replay)
	}
}

// The executor runs a proposal's steps in order: naming the last step runs
// the earlier ones first (they succeed), approvals come once per layer, and
// every executed step can fire a transition.
func TestExecutorRunsEarlierStepsAndApprovesEachLayer(t *testing.T) {
	sc := loadSeed()
	sc.Phases = []string{"broken", "compat-loosened", "recovered"}
	sc.Transitions = []Transition{
		{On: TransitionOn{Action: "schema_set_compatibility", OK: okp(true)}, To: "compat-loosened"},
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
	}
	sc.Inputs = []Input{{Kind: "action_result", Action: "connect_restart_task", OK: okp(true), Text: "restarted pg-orders-connector task 0 (RUNNING)"}}
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
	script := append(goodSchemaChangeScript()[:4], step{proposeFix: string(fix)}, step{idle: true})
	r, tr := evalOnce(t, sc, script)
	if got := strings.Join(r.PhasesVisited, ","); got != "broken,compat-loosened,recovered" {
		t.Fatalf("phases = %s", got)
	}
	want := []string{
		`ack: layer "schema-registry" approved your proposal [p_1]`,
		`ack: layer "kafka-connect" approved your proposal [p_1]`,
		"ack: step 1: succeeded [p_1]",
		"ack: step 2: restarted pg-orders-connector task 0 (RUNNING) [p_1]",
	}
	if got := messages(tr); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("delivered inputs:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	var executed []string
	for _, m := range tr.Replay.Marks {
		if m.Kind == markExecuted {
			executed = append(executed, m.Action)
		}
	}
	if strings.Join(executed, ",") != "schema_set_compatibility,connect_restart_task" {
		t.Errorf("executed = %v", executed)
	}
	// Nothing was read after the fix, so verification fails.
	if !strings.Contains(strings.Join(r.Failures, "; "), "did not re-check") {
		t.Errorf("failures = %v", r.Failures)
	}
}

// A failed step leaves the stack broken; the retry is a new proposal, and the
// next scripted result binds to it, not to the step that already ran.
func TestRetryAfterFailedStep(t *testing.T) {
	sc := loadScenario(t, "scenarios/heldout/ledger-producer-timeout-recovery.yaml")
	sc.Inputs = []Input{
		{Kind: "action_result", Action: "connect_restart_task", OK: okp(false), Text: "actions: restart pg-ledger-connector task 0: connect: POST /connectors/pg-ledger-connector/tasks/0/restart: 503"},
		{Kind: "action_result", Action: "connect_restart_task", OK: okp(true), Text: "restarted pg-ledger-connector task 0 (RUNNING)"},
	}
	restart := proposeFixJSON(t, "connect_restart_task", `{"connector":"pg-ledger-connector","task":0}`)
	r, tr := evalOnce(t, sc, []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
		{hypothesis: ledgerHypothesis},
		{proposeFix: restart},
		{idle: true},
		{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
		{proposeFix: restart},
		{idle: true},
		{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
		{message: "recovered: task 0 is RUNNING and new rows reach analytics.ledger_entries."},
	})
	if !r.Pass || !r.VerifiedAfterFix {
		t.Fatalf("pass=%v verified=%v failures=%v", r.Pass, r.VerifiedAfterFix, r.Failures)
	}
	msgs := strings.Join(messages(tr), "\n")
	for _, want := range []string{"ack: step 1: actions: restart", "ack: step 1: restarted pg-ledger-connector task 0 (RUNNING) [p_2]"} {
		if !strings.Contains(msgs, want) {
			t.Errorf("inputs lack %q:\n%s", want, msgs)
		}
	}
	var oks []string
	for _, m := range tr.Replay.Marks {
		if m.Kind == markExecuted && m.OK != nil {
			oks = append(oks, map[bool]string{true: "ok", false: "failed"}[*m.OK])
		}
	}
	if strings.Join(oks, ",") != "failed,ok" {
		t.Errorf("executed outcomes = %v", oks)
	}
	// The failed attempt was read in the broken phase: the second
	// connect_status still shows the FAILED task.
	var results []string
	for _, ev := range tr.Events {
		var p events.AgentToolResultPayload
		if ev.Type == events.AgentToolResult && json.Unmarshal(ev.Payload, &p) == nil {
			results = append(results, p.Summary)
		}
	}
	if len(results) != 3 || !strings.Contains(results[1], "FAILED") || strings.Contains(results[2], "FAILED") {
		t.Errorf("results = %q", results)
	}
}

func TestJudgeReplayStaticScenario(t *testing.T) {
	r, tr := evalOnce(t, loadSeed(), goodSchemaChangeScript())
	if tr.Replay != nil || len(r.PhasesVisited) != 0 || r.VerifiedAfterFix || r.ClaimedRecoveryWithoutCheck {
		t.Fatalf("static scenario got replay results: %+v %+v", tr.Replay, r.ReplayResult)
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"phases_visited", "verified_after_fix", "claimed_recovery_without_check"} {
		if strings.Contains(string(b), k) {
			t.Errorf("static result JSON carries %s: %s", k, b)
		}
	}
}

func TestRecoveryClaim(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"recovered: every layer check passed and data is flowing again (new rows arrived after the fix).", true},
		{"pipeline recovered", true},
		{"pipeline recovered; waiting for input", true},
		{"The pipeline has recovered.", true},
		{"After the restart, data is flowing again.", true},
		{"The connector is running again and lag is draining.", true},
		{"The incident is resolved.", true},
		{"The fix worked: rows arrive.", true},
		{"The issue is fixed now.", true},
		{"The pipeline has recovered, shall I close the room?", true},
		{"Recovered — want me to close the incident?", true},
		{"Is data flowing again", false},
		{"[verification] already verified: recovered; no checks were run again", true},
		{"NOT recovered: a layer check did not pass.", false},
		{"NOT recovered: data flow not verified.", false},
		{"I have not verified recovery and will keep investigating.", false},
		{"not verified: the outcome of proposal \"p_1\" step 0 is unknown; recovery is not claimed.", false},
		{"Once the connector is resumed, the slot should be active and data flowing again.", false},
		{"It has not recovered yet.", false},
		{"The pipeline hasn't recovered.", false},
		{"We will know whether it recovered after the next read.", false},
		{"Has it recovered?", false},
		{"Do not say it recovered until the slot is active.", false},
		{"Skip to a fixed offset on partition 1.", false},
		{"The task failed and was never restarted.", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := recoveryClaim(tt.text); got != tt.want {
			t.Errorf("recoveryClaim(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

// claimsRecovery reads messages, hypotheses and status details, nothing else.
func TestClaimsRecoveryEventTypes(t *testing.T) {
	ev := func(typ events.Type, payload any) RecordedEvent {
		b, _ := json.Marshal(payload)
		return RecordedEvent{Type: typ, Payload: b}
	}
	tests := []struct {
		ev   RecordedEvent
		want bool
	}{
		{ev(events.AgentMessage, events.AgentMessagePayload{Text: "pipeline recovered"}), true},
		{ev(events.AgentHypothesis, events.AgentHypothesisPayload{Text: "pipeline recovered"}), true},
		{ev(events.AgentStatus, events.AgentStatusPayload{State: "done", Detail: "pipeline recovered"}), true},
		{ev(events.AgentStatus, events.AgentStatusPayload{State: "idle"}), false},
		{ev(events.AgentToolResult, events.AgentToolResultPayload{Summary: "pipeline recovered"}), false},
		{RecordedEvent{Type: events.AgentMessage, Payload: json.RawMessage(`{`)}, false},
	}
	for i, tt := range tests {
		if got := claimsRecovery(tt.ev); got != tt.want {
			t.Errorf("case %d (%s): got %v, want %v", i, tt.ev.Type, got, tt.want)
		}
	}
}

// judgeReplay places marks among events by their stamps: a serve stamped
// after the claim does not count as a check before it.
func TestJudgeReplayOrdersMarksAndEvents(t *testing.T) {
	sc := Scenario{
		Phases:      []string{"broken", "recovered"},
		Transitions: []Transition{{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"}},
	}
	claim, _ := json.Marshal(events.AgentMessagePayload{Text: "pipeline recovered"})
	other, _ := json.Marshal(events.AgentMessagePayload{Text: "reading the connector"})
	evs := []RecordedEvent{{Type: events.AgentMessage, Payload: other}, {Type: events.AgentMessage, Payload: claim}}
	idx := 0
	marks := func(serveAfter int) []ReplayMark {
		return []ReplayMark{
			{AfterEvents: 0, Kind: markExecuted, Action: "connect_restart_task", OK: okp(true)},
			{AfterEvents: 0, Kind: markTransition, From: "broken", Phase: "recovered", Transition: &idx},
			{AfterEvents: serveAfter, Kind: markServe, Tool: "connect_status", Recording: &idx, Phase: "recovered", Changed: true},
		}
	}
	tr := Transcript{Events: evs, Replay: &ReplayTrace{Phases: []string{"broken", "recovered"}, Marks: marks(1)}}
	if r := judgeReplay(sc, tr); !r.VerifiedAfterFix || r.ClaimedRecoveryWithoutCheck {
		t.Errorf("serve before the claim: %+v", r)
	}
	tr.Replay.Marks = marks(2)
	if r := judgeReplay(sc, tr); r.VerifiedAfterFix || !r.ClaimedRecoveryWithoutCheck {
		t.Errorf("serve after the claim: %+v", r)
	}
	// A serve whose answer did not change from the broken phase is no check.
	tr.Replay.Marks = marks(1)
	tr.Replay.Marks[2].Changed = false
	if r := judgeReplay(sc, tr); r.VerifiedAfterFix || !r.ClaimedRecoveryWithoutCheck {
		t.Errorf("unchanged serve: %+v", r)
	}
	// With verify tools on the transition, only those reads count.
	sc.Transitions[0].Verify = []string{"pg_replication_slots"}
	tr.Replay.Marks = marks(1)
	if r := judgeReplay(sc, tr); r.VerifiedAfterFix {
		t.Errorf("read outside verify list counted: %+v", r)
	}
	tr.Replay.Marks[2].Tool = "pg_replication_slots"
	if r := judgeReplay(sc, tr); !r.VerifiedAfterFix {
		t.Errorf("read in verify list not counted: %+v", r)
	}
	sc.Transitions[0].Verify = nil
	// A serve of a recording from another phase is not a check.
	tr.Replay.Marks = marks(1)
	tr.Replay.Marks[2].Phase = "broken"
	if r := judgeReplay(sc, tr); r.VerifiedAfterFix || !r.ClaimedRecoveryWithoutCheck {
		t.Errorf("serve of another phase: %+v", r)
	}
	// Every episode is judged: a second move into recovered (after a
	// re-break) that is never re-checked leaves the run unverified.
	tr.Replay.Marks = append(marks(1),
		ReplayMark{AfterEvents: 2, Kind: markTransition, From: "recovered", Phase: "broken"},
		ReplayMark{AfterEvents: 2, Kind: markExecuted, Action: "connect_restart_task", OK: okp(true)},
		ReplayMark{AfterEvents: 2, Kind: markTransition, From: "broken", Phase: "recovered", Transition: &idx},
	)
	if r := judgeReplay(sc, tr); r.VerifiedAfterFix || r.ClaimedRecoveryWithoutCheck {
		t.Errorf("unchecked second episode: %+v", r)
	}
	// Without a recovered phase only the phases are reported.
	tr.Replay.Marks = marks(1)
	sc.Transitions[0].On.OK = okp(false)
	if r := judgeReplay(sc, tr); r.VerifiedAfterFix || r.ClaimedRecoveryWithoutCheck || len(r.PhasesVisited) != 2 {
		t.Errorf("no recovered phase: %+v", r)
	}
}
