package evals

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func okp(v bool) *bool { return &v }

// serveSummary serves one call and returns its summary or error text, "" for
// a miss.
func serveSummary(t *testing.T, rp *replayer, tool, input string) string {
	t.Helper()
	ob, ok := rp.serve(tool, json.RawMessage(input))
	if !ok {
		return ""
	}
	if ob.err != "" {
		return "error: " + ob.err
	}
	return ob.summary
}

func twoPhaseRecordings() []Recording {
	return []Recording{
		{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "c STOPPED"},
		{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Phase: "recovered", Summary: "c RUNNING"},
		{Tool: "connect_status", Summary: "any connector"},
		{Tool: "kafka_consumer_lag", Input: map[string]any{"group": "g"}, Summary: "lag broken"},
		{Tool: "kafka_consumer_lag", Input: map[string]any{"group": "g"}, Phase: "recovered", Summary: "lag recovered"},
	}
}

func TestPhaseTransitionOnExecutedAction(t *testing.T) {
	rp, err := newPhasedReplayer(twoPhaseRecordings(), []string{"broken", "recovered"}, []Transition{
		{On: TransitionOn{Action: "connect_resume_connector", OK: okp(true)}, To: "recovered"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := serveSummary(t, rp, "connect_status", `{"connector":"c"}`); got != "c STOPPED" {
		t.Fatalf("start phase: %q", got)
	}
	// A failed step, an unknown outcome and another action leave the phase.
	rp.executed("connect_resume_connector", okp(false))
	rp.executed("connect_resume_connector", nil)
	rp.executed("connect_restart_task", okp(true))
	if got := serveSummary(t, rp, "connect_status", `{"connector":"c"}`); got != "c STOPPED" {
		t.Fatalf("after a failed resume: %q", got)
	}
	rp.executed("connect_resume_connector", okp(true))
	if got := serveSummary(t, rp, "connect_status", `{"connector":"c"}`); got != "c RUNNING" {
		t.Fatalf("after the resume: %q", got)
	}
	// A recording without a phase answers in every phase, and the
	// recovered recording never shadows a more specific shared one.
	if got := serveSummary(t, rp, "connect_status", `{"connector":"other"}`); got != "any connector" {
		t.Fatalf("shared wildcard in recovered: %q", got)
	}
	tr := rp.trace()
	if tr == nil || strings.Join(tr.Phases, ",") != "broken,recovered" {
		t.Fatalf("phases = %+v", tr)
	}
	var kinds []string
	for _, m := range tr.Marks {
		kinds = append(kinds, m.Kind)
	}
	// Executed steps are all marked; only the recovered serve is a serve
	// mark (the others answered from recordings without a phase).
	if got := strings.Join(kinds, ","); got != "executed,executed,executed,executed,transition,serve" {
		t.Fatalf("marks = %s", got)
	}
}

func TestPhaseTransitionOnFailedAction(t *testing.T) {
	recs := append(twoPhaseRecordings(), Recording{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Phase: "degraded", Summary: "c FAILED again"})
	rp, err := newPhasedReplayer(recs, []string{"broken", "recovered", "degraded"}, []Transition{
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(false)}, To: "degraded"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rp.executed("connect_restart_task", okp(false))
	if got := serveSummary(t, rp, "connect_status", `{"connector":"c"}`); got != "c FAILED again" {
		t.Fatalf("after the failed restart: %q", got)
	}
	// The retry works.
	rp.executed("connect_restart_task", okp(true))
	if got := serveSummary(t, rp, "connect_status", `{"connector":"c"}`); got != "c RUNNING" {
		t.Fatalf("after the retry: %q", got)
	}
	if got := strings.Join(rp.trace().Phases, ","); got != "broken,degraded,recovered" {
		t.Fatalf("phases = %s", got)
	}
}

// Transitions are checked in file order and only the first match fires.
func TestPhaseFirstMatchingTransitionWins(t *testing.T) {
	rp, err := newPhasedReplayer(twoPhaseRecordings(), []string{"broken", "recovered", "other"}, []Transition{
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
		{On: TransitionOn{Action: "connect_restart_task"}, To: "other"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rp.executed("connect_restart_task", okp(true))
	if got := rp.currentPhase(); got != "recovered" {
		t.Fatalf("ok: phase = %q, want recovered", got)
	}
	rp.executed("connect_restart_task", okp(false))
	if got := rp.currentPhase(); got != "other" {
		t.Fatalf("failed: phase = %q, want other", got)
	}
}

func TestPhaseTransitionAnyOutcome(t *testing.T) {
	rp, err := newPhasedReplayer(twoPhaseRecordings(), []string{"broken", "recovered"}, []Transition{
		{On: TransitionOn{Action: "connect_restart_task"}, To: "recovered"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rp.executed("connect_restart_task", nil) // unknown outcome matches "any"
	if got := rp.currentPhase(); got != "recovered" {
		t.Fatalf("phase = %q", got)
	}
}

func TestPhaseTransitionAfterCalls(t *testing.T) {
	rp, err := newPhasedReplayer(twoPhaseRecordings(), []string{"broken", "recovered"}, []Transition{
		{On: TransitionOn{AfterCalls: &AfterCalls{Tool: "kafka_consumer_lag", N: 3}}, To: "recovered"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A miss counts as a call: time passes whatever the agent asks.
	if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"other"}`); got != "" {
		t.Fatalf("miss answered: %q", got)
	}
	// Other tools do not count.
	serveSummary(t, rp, "connect_status", `{"connector":"c"}`)
	if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"g"}`); got != "lag broken" {
		t.Fatalf("call 2: %q", got)
	}
	// The n-th call is still answered in the old phase.
	if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"g"}`); got != "lag broken" {
		t.Fatalf("call 3: %q", got)
	}
	if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"g"}`); got != "lag recovered" {
		t.Fatalf("call 4: %q", got)
	}
	if got := serveSummary(t, rp, "connect_status", `{"connector":"c"}`); got != "c RUNNING" {
		t.Fatalf("connect_status after the transition: %q", got)
	}
	if n := len(rp.Misses()); n != 1 {
		t.Fatalf("misses = %d, want 1", n)
	}
}

func TestSequenceAdvancesAndLastRepeats(t *testing.T) {
	rp, err := newReplayer([]Recording{
		{Tool: "kafka_consumer_lag", Input: map[string]any{"group": "a"}, Sequence: []Observation{
			{Summary: "lag 1"}, {Error: "kafka: group a: coordinator not available"}, {Summary: "lag 3", Data: map[string]any{"total_lag": 3}},
		}},
		{Tool: "kafka_consumer_lag", Input: map[string]any{"group": "b"}, Sequence: []Observation{{Summary: "b1"}, {Summary: "b2"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// lookup peeks and never advances.
	for i := 0; i < 3; i++ {
		if rec, ok := rp.lookup("kafka_consumer_lag", json.RawMessage(`{"group":"a"}`)); !ok || len(rec.obs) != 3 {
			t.Fatalf("lookup = %+v, %v", rec, ok)
		}
	}
	want := []string{"lag 1", "error: kafka: group a: coordinator not available", "lag 3", "lag 3", "lag 3"}
	for i, w := range want {
		if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"a"}`); got != w {
			t.Fatalf("call %d: %q, want %q", i+1, got, w)
		}
		// The other recording keeps its own position.
		if i == 1 {
			if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"b"}`); got != "b1" {
				t.Fatalf("b call 1: %q", got)
			}
		}
	}
	if got := serveSummary(t, rp, "kafka_consumer_lag", `{"group":"b"}`); got != "b2" {
		t.Fatalf("b call 2: %q", got)
	}
	ob, _ := rp.serve("kafka_consumer_lag", json.RawMessage(`{"group":"a"}`))
	if string(ob.data) != `{"total_lag":3}` {
		t.Fatalf("data of the repeated observation = %s", ob.data)
	}
	// A static scenario without phases leaves no trace.
	if tr := rp.trace(); tr != nil {
		t.Fatalf("trace = %+v, want nil", tr)
	}
}

func TestPhaseSpecificity(t *testing.T) {
	recs := []Recording{
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t", "key": "1"}, Summary: "exact shared"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t"}, Phase: "recovered", Summary: "topic recovered"},
		{Tool: "kafka_find_by_key", Input: map[string]any{"topic": "t"}, Summary: "topic shared"},
		{Tool: "kafka_find_by_key", Phase: "broken", Summary: "wildcard broken"},
	}
	rp, err := newPhasedReplayer(recs, []string{"broken", "recovered"}, []Transition{
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct{ phase, input, want string }{
		{"broken", `{"topic":"t","key":"1"}`, "exact shared"},
		{"broken", `{"topic":"t","key":"2"}`, "topic shared"},
		{"broken", `{"topic":"u","key":"2"}`, "wildcard broken"},
		{"recovered", `{"topic":"t","key":"1"}`, "exact shared"}, // more fields still win
		{"recovered", `{"topic":"t","key":"2"}`, "topic recovered"},
		{"recovered", `{"topic":"u","key":"2"}`, ""}, // the broken wildcard is gone
	}
	for _, tt := range tests {
		if tt.phase == "recovered" && rp.currentPhase() != "recovered" {
			rp.executed("connect_restart_task", okp(true))
		}
		rec, ok := rp.lookup("kafka_find_by_key", json.RawMessage(tt.input))
		got := ""
		if ok {
			got = rec.Summary
		}
		if got != tt.want {
			t.Errorf("%s %s: got %q, want %q", tt.phase, tt.input, got, tt.want)
		}
	}
}

// TestReplayConcurrentCalls runs many tool calls and executed steps at once
// (go test -race): every sequence observation is handed out exactly once,
// after_calls fires once, and the phase ends where it must.
func TestReplayConcurrentCalls(t *testing.T) {
	const calls = 200
	seq := make([]Observation, calls)
	for i := range seq {
		seq[i] = Observation{Summary: fmt.Sprintf("obs %d", i)}
	}
	recs := []Recording{
		{Tool: "kafka_consumer_lag", Input: map[string]any{"group": "g"}, Sequence: seq},
		{Tool: "connect_status", Summary: "shared"},
		{Tool: "connect_status", Phase: "late", Summary: "late"},
	}
	rp, err := newPhasedReplayer(recs, []string{"start", "late", "fixed"}, []Transition{
		{On: TransitionOn{AfterCalls: &AfterCalls{Tool: "kafka_consumer_lag", N: calls / 2}}, To: "late"},
		{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "fixed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var count int
	rp.events = func() int { count++; return count }
	var wg sync.WaitGroup
	got := make(chan string, calls)
	for i := 0; i < calls; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			ob, ok := rp.serve("kafka_consumer_lag", json.RawMessage(`{"group":"g"}`))
			if ok {
				got <- ob.summary
			}
		}()
		go func() {
			defer wg.Done()
			rp.serve("connect_status", json.RawMessage(`{"connector":"c"}`))
		}()
	}
	wg.Wait()
	close(got)
	seen := map[string]bool{}
	for s := range got {
		if seen[s] {
			t.Fatalf("observation %q handed out twice", s)
		}
		seen[s] = true
	}
	if len(seen) != calls {
		t.Fatalf("%d distinct observations, want %d", len(seen), calls)
	}
	if p := rp.currentPhase(); p != "late" {
		t.Fatalf("phase = %q, want late", p)
	}
	var execWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		execWG.Add(1)
		go func() { defer execWG.Done(); rp.executed("connect_restart_task", okp(true)) }()
	}
	execWG.Wait()
	// after_calls fired once, at its n-th call: later calls of the tool do
	// not move the run back.
	rp.serve("kafka_consumer_lag", json.RawMessage(`{"group":"g"}`))
	tr := rp.trace()
	if strings.Join(tr.Phases, ",") != "start,late,fixed" {
		t.Fatalf("phases = %v", tr.Phases)
	}
	transitions := 0
	for _, m := range tr.Marks {
		if m.Kind == markTransition {
			transitions++
		}
	}
	if transitions != 2 {
		t.Fatalf("%d transitions, want 2 (after_calls once, the action once)", transitions)
	}
}

// A result that appears only in a sequence element is visible through
// recordingResults, the one place that enumerates what a recording shows. E2's
// fact check (factRecorded) must read recordings through it: here the facts
// f_consumer_stuck and f_group_stable exist only in a sequence.
func TestRecordingResultsReadsSequenceElements(t *testing.T) {
	for _, tt := range []struct{ file, tool, want string }{
		{"scenarios/heldout/users-compat-none-consumer.yaml", "kafka_consumer_lag", "user-profile-sync: Empty, 0 members, total lag 10127"},
		{"scenarios/heldout/shipments-lag-burst-healthy.yaml", "kafka_consumer_lag", "shipment-tracker: Stable, 3 members, total lag 12904"},
		{"scenarios/consumer-poison-pill.yaml", "kafka_consumer_lag", "order-sync: PreparingRebalance, 1 members, total lag 14774"},
	} {
		sc, err := LoadScenario(tt.file)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range sc.Recordings {
			if r.Tool != tt.tool || r.Error != "" {
				continue
			}
			if strings.Contains(r.Summary, tt.want) {
				t.Fatalf("%s: %q is in a top-level summary; the test needs it only in a sequence", tt.file, tt.want)
			}
			for _, o := range recordingResults(r) {
				if o.Error == "" && strings.Contains(o.Summary, tt.want) {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("%s: no %s result shows %q", tt.file, tt.tool, tt.want)
		}
	}
	plain := Recording{Tool: "t", Summary: "s", Data: 1, Truncated: true, Error: "e"}
	if got := recordingResults(plain); len(got) != 1 || got[0] != (Observation{Summary: "s", Data: 1, Truncated: true, Error: "e"}) {
		t.Errorf("plain recording results = %+v", got)
	}
	if got := recordingResults(Recording{Sequence: []Observation{}}); len(got) != 0 {
		t.Errorf("empty sequence results = %+v", got)
	}
}

func TestNewReplayerRejectsEmptySequence(t *testing.T) {
	_, err := newReplayer([]Recording{{Tool: "x", Sequence: []Observation{}}})
	if err == nil || !strings.Contains(err.Error(), "sequence is empty") {
		t.Fatalf("err = %v", err)
	}
	_, err = newReplayer([]Recording{{Tool: "x", Sequence: []Observation{{Summary: "a", Data: map[string]any{"f": func() {}}}}}})
	if err == nil {
		t.Fatal("unmarshalable sequence data accepted")
	}
}

func TestValidatePhases(t *testing.T) {
	tools := map[string][]string{"connect_status": {"connector"}, "kafka_consumer_lag": {"group"}}
	acts := []string{"connect_restart_task", "connect_resume_connector"}
	base := func() Scenario {
		return Scenario{
			Name:     "x",
			Pipeline: pipelineOrders(),
			Alert:    Alert{Title: "t"},
			Phases:   []string{"broken", "recovered"},
			Transitions: []Transition{
				{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
			},
			Recordings: []Recording{
				{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Summary: "stopped", Data: map[string]any{"state": "STOPPED"}},
				{Tool: "connect_status", Input: map[string]any{"connector": "c"}, Phase: "recovered", Summary: "running", Data: map[string]any{"state": "RUNNING"}},
				{Tool: "kafka_consumer_lag", Phase: "broken", Sequence: []Observation{{Summary: "1"}, {Summary: "2"}}},
			},
			Inputs: []Input{{Kind: "action_result", Action: "connect_restart_task", OK: okp(true), Text: "restarted"}},
			Expect: Expect{RootCause: [][]string{{"a"}}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*Scenario)
		want   string // "" = valid
	}{
		{"valid", func(*Scenario) {}, ""},
		{"static scenario", func(s *Scenario) {
			s.Phases, s.Transitions = nil, nil
			s.Recordings = s.Recordings[:1]
		}, ""},
		{"unknown phase on recording", func(s *Scenario) { s.Recordings[1].Phase = "fixed" }, `recordings[1] (connect_status): unknown phase "fixed"`},
		{"phase without phases", func(s *Scenario) { s.Phases, s.Transitions = nil, nil }, `phase "recovered", but the scenario declares no phases`},
		{"transition to unknown phase", func(s *Scenario) { s.Transitions[0].To = "healed" }, `transitions[0]: to unknown phase "healed"`},
		{"transitions without phases", func(s *Scenario) {
			s.Phases = nil
			s.Recordings = s.Recordings[:1]
		}, "transitions need phases"},
		{"duplicate phase", func(s *Scenario) { s.Phases = append(s.Phases, "broken") }, `phases[2]: duplicate phase "broken"`},
		{"blank phase", func(s *Scenario) { s.Phases = append(s.Phases, " ") }, "phases[2]: blank name"},
		{"empty sequence", func(s *Scenario) { s.Recordings[2].Sequence = []Observation{} }, "recordings[2] (kafka_consumer_lag): sequence is empty"},
		{"sequence plus summary", func(s *Scenario) { s.Recordings[2].Summary = "x" }, "has a sequence and a top-level summary"},
		{"sequence plus error", func(s *Scenario) { s.Recordings[2].Error = "x" }, "has a sequence and a top-level summary"},
		{"sequence item without result", func(s *Scenario) { s.Recordings[2].Sequence[1] = Observation{} }, "sequence[1]: needs summary or error"},
		{"sequence item bad data", func(s *Scenario) { s.Recordings[2].Sequence[0].Data = map[string]any{"f": func() {}} }, "sequence[0]: data does not convert"},
		{"on both", func(s *Scenario) {
			s.Transitions[0].On.AfterCalls = &AfterCalls{Tool: "connect_status", N: 1}
		}, "both action and after_calls"},
		{"on neither", func(s *Scenario) { s.Transitions[0].On = TransitionOn{} }, "on needs action or after_calls"},
		{"unknown on.action", func(s *Scenario) { s.Transitions[0].On.Action = "drop_slot" }, `on.action: unknown action "drop_slot"`},
		{"unknown after_calls tool", func(s *Scenario) {
			s.Transitions[0].On = TransitionOn{AfterCalls: &AfterCalls{Tool: "nope", N: 1}}
		}, `on.after_calls: unknown tool "nope"`},
		{"after_calls n zero", func(s *Scenario) {
			s.Transitions[0].On = TransitionOn{AfterCalls: &AfterCalls{Tool: "connect_status"}}
		}, "on.after_calls.n must be at least 1"},
		{"ok on after_calls", func(s *Scenario) {
			s.Transitions[0].On = TransitionOn{AfterCalls: &AfterCalls{Tool: "connect_status", N: 1}, OK: okp(true)}
		}, "on.ok applies only to on.action"},
		{"duplicate trigger", func(s *Scenario) {
			s.Transitions = append(s.Transitions, Transition{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "broken"})
		}, "transitions[1]: never fires: same trigger as transitions[0]"},
		{"shadowed by any outcome", func(s *Scenario) {
			s.Transitions = []Transition{
				{On: TransitionOn{Action: "connect_restart_task"}, To: "recovered"},
				{On: TransitionOn{Action: "connect_restart_task", OK: okp(false)}, To: "broken"},
			}
		}, "transitions[1]: never fires: transitions[0] matches connect_restart_task with any outcome first"},
		{"status of a later phase first", func(s *Scenario) {
			s.Recordings[0], s.Recordings[1] = s.Recordings[1], s.Recordings[0]
		}, `it is in phase "recovered", not the start phase "broken"`},
		{"ok without action", func(s *Scenario) { s.Inputs[0].Action = "" }, "inputs[0]: ok needs action"},
		{"action on a steer", func(s *Scenario) { s.Inputs[0].Kind = "steer" }, `inputs[0]: action applies only to kind action_result, not "steer"`},
		{"unknown input action", func(s *Scenario) { s.Inputs[0].Action = "drop_slot" }, `inputs[0]: unknown action "drop_slot"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := base()
			tt.mutate(&s)
			err := s.Validate(tools, acts)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestPhaseWarnings(t *testing.T) {
	s := Scenario{
		Phases: []string{"broken", "recovered", "orphan"},
		Transitions: []Transition{
			{On: TransitionOn{Action: "connect_restart_task", OK: okp(true)}, To: "recovered"},
			{On: TransitionOn{Action: "connect_resume_connector", OK: okp(true)}, To: "recovered"},
		},
		Inputs: []Input{
			{Kind: "action_result", Action: "connect_restart_task", OK: okp(true), Text: "ok"},
			{Kind: "action_result", Action: "connect_resume_connector", OK: okp(false), Text: "failed"},
		},
		Expect: Expect{Pending: map[string]any{"outcome": "healthy", "claims": nil}},
	}
	got := strings.Join(s.Warnings(), "\n")
	for _, want := range []string{
		`expect: claims, outcome accepted but not scored`,
		`phase "orphan" is unreachable`,
		"transitions[1]: no input executes connect_resume_connector with a matching ok",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("warnings lack %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "transitions[0]") || strings.Contains(got, `"recovered" is unreachable`) {
		t.Errorf("false warning:\n%s", got)
	}
	if w := (Scenario{}).Warnings(); len(w) != 0 {
		t.Errorf("static scenario warnings = %v", w)
	}
}

func TestParseScenarioReplayFields(t *testing.T) {
	s, err := ParseScenario([]byte(`
name: x
phases: [broken, recovered]
transitions:
  - on: {action: connect_restart_task, ok: true}
    to: recovered
  - on: {after_calls: {tool: kafka_consumer_lag, n: 3}}
    to: recovered
recordings:
  - tool: kafka_consumer_lag
    phase: broken
    sequence:
      - {summary: "lag 1204"}
      - {summary: "lag 1310", data: {total_lag: 1310}}
inputs:
  - {kind: action_result, action: connect_restart_task, ok: true, text: restarted}
expect:
  root_cause: [[a]]
  outcome: diagnose
  root_cause_layer: postgres
  expected_facts:
    - {id: f, tool: connect_status, match: [FAILED], required: true}
  claims:
    - {about: root_cause, facts: [f]}
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Phases) != 2 || s.Transitions[1].On.AfterCalls.N != 3 || *s.Transitions[0].On.OK != true {
		t.Errorf("phases/transitions = %+v %+v", s.Phases, s.Transitions)
	}
	if r := s.Recordings[0]; r.Phase != "broken" || len(r.Sequence) != 2 || r.Sequence[1].Summary != "lag 1310" {
		t.Errorf("recording = %+v", r)
	}
	if in := s.Inputs[0]; in.Action != "connect_restart_task" || in.OK == nil || !*in.OK {
		t.Errorf("input = %+v", in)
	}
	// Before E2's typed fields exist the shared-schema keys land in Pending;
	// after, they are typed fields and Pending is empty. Either way they
	// parse.
	if got := strings.Join(sortedKeys(s.Expect.Pending), ","); got != "" && got != "claims,expected_facts,outcome,root_cause_layer" {
		t.Errorf("pending expect keys = %s", got)
	}
	// Only the shared-schema keys are tolerated; any other unknown key in
	// expect, and unknown keys in the new blocks, still fail.
	for _, bad := range []string{
		"name: x\nexpect:\n  outcome_typo: healthy\n",
		"name: x\ntransitions:\n  - {on: {action: a, okay: true}, to: b}\n",
		"name: x\nrecordings:\n  - {tool: t, sequence: [{summary: a, sumary: b}]}\n",
		"name: x\ninputs:\n  - {kind: action_result, actoin: a, text: t}\n",
	} {
		if _, err := ParseScenario([]byte(bad)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	// An explicitly empty sequence parses and then fails validation.
	s, err = ParseScenario([]byte("name: x\nrecordings:\n  - {tool: connect_status, sequence: []}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(map[string][]string{"connect_status": {"connector"}}, nil); err == nil || !strings.Contains(err.Error(), "sequence is empty") {
		t.Errorf("empty sequence: %v", err)
	}
}
