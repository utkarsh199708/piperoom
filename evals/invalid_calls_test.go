package evals

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

// recEv builds one recorded event from the agent.
func recEv(t *testing.T, typ events.Type, payload any) RecordedEvent {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return RecordedEvent{Type: typ, Payload: b, Actor: events.Actor{Kind: events.ActorAgent}}
}

// callPair is a tool call and its result as the agent writes them.
func callPair(t *testing.T, step, tool string, isErr bool, rejected string) []RecordedEvent {
	return []RecordedEvent{
		recEv(t, events.AgentToolCall, events.AgentToolCallPayload{StepID: step, Tool: tool}),
		recEv(t, events.AgentToolResult, events.AgentToolResultPayload{StepID: step, Tool: tool, IsError: isErr, Rejected: rejected}),
	}
}

func cat(parts ...[]RecordedEvent) []RecordedEvent {
	var out []RecordedEvent
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func TestCheckToolCalls(t *testing.T) {
	system := recEv(t, events.AgentToolResult, events.AgentToolResultPayload{Tool: "x", IsError: true, Rejected: events.ToolRejectedUnknownTool})
	system.Actor = events.Actor{Kind: events.ActorSystem}
	// A transcript from before actors were recorded still counts.
	legacy := callPair(t, "s9", "nope", true, events.ToolRejectedUnknownTool)
	for i := range legacy {
		legacy[i].Actor = events.Actor{}
	}
	cases := []struct {
		name    string
		evs     []RecordedEvent
		invalid int
		all     int
		reasons map[string]int
	}{
		{name: "no events", evs: nil, invalid: 0, all: 0},
		{name: "a read that worked", evs: callPair(t, "s1", "connector_status", false, ""), invalid: 0, all: 1},
		{name: "a tool error that ran is not invalid", evs: callPair(t, "s1", "connector_status", true, ""), invalid: 0, all: 1},
		{name: "unknown tool", evs: callPair(t, "s1", "conector_status", true, events.ToolRejectedUnknownTool),
			invalid: 1, all: 1, reasons: map[string]int{"unknown_tool": 1}},
		{name: "invalid json", evs: callPair(t, "s1", "connector_status", true, events.ToolRejectedInvalidJSON),
			invalid: 1, all: 1, reasons: map[string]int{"invalid_json": 1}},
		{name: "invalid input", evs: callPair(t, "s1", "propose_fix", true, events.ToolRejectedInvalidInput),
			invalid: 1, all: 1, reasons: map[string]int{"invalid_input": 1}},
		{
			name: "each reason once, among good and errored calls",
			evs: cat(
				callPair(t, "s1", "connector_status", false, ""),
				callPair(t, "s2", "nope", true, events.ToolRejectedUnknownTool),
				callPair(t, "s3", "topic_offsets", true, events.ToolRejectedInvalidJSON),
				callPair(t, "s4", "topic_offsets", true, ""),
				callPair(t, "s5", "topic_offsets", true, events.ToolRejectedInvalidInput),
				callPair(t, "s6", "topic_offsets", true, events.ToolRejectedInvalidInput),
			),
			invalid: 4, all: 6,
			reasons: map[string]int{"unknown_tool": 1, "invalid_json": 1, "invalid_input": 2},
		},
		{name: "a system-written result is not the agent's", evs: []RecordedEvent{system}, invalid: 0, all: 0},
		{name: "transcript without actors", evs: legacy, invalid: 1, all: 1, reasons: map[string]int{"unknown_tool": 1}},
		{
			name: "a proposal Validate refusal is rejected_attempts, not invalid",
			evs:  []RecordedEvent{recEv(t, events.AgentMessage, events.AgentMessagePayload{Text: "proposal refused"})},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := checkToolCalls(tc.evs)
			if got.InvalidToolCalls == nil || *got.InvalidToolCalls != tc.invalid {
				t.Fatalf("invalid = %v, want %d", got.InvalidToolCalls, tc.invalid)
			}
			if got.AllToolCalls != tc.all {
				t.Errorf("all = %d, want %d", got.AllToolCalls, tc.all)
			}
			if !reflect.DeepEqual(got.InvalidToolCallReasons, tc.reasons) {
				t.Errorf("reasons = %v, want %v", got.InvalidToolCallReasons, tc.reasons)
			}
		})
	}
}

// TestInvalidCallsInRunResult checks the counter reaches the run's result
// through withReplayChecks and changes neither score nor pass.
func TestInvalidCallsInRunResult(t *testing.T) {
	sc := Scenario{Name: "s", Expect: Expect{Outcome: OutcomeHealthy}}
	good := Transcript{Events: callPair(t, "s1", "connector_status", false, "")}
	bad := Transcript{Events: cat(good.Events, callPair(t, "s2", "nope", true, events.ToolRejectedUnknownTool))}
	rg := withReplayChecks(sc, good, Judge(sc, good, nil, false))
	rb := withReplayChecks(sc, bad, Judge(sc, bad, nil, false))
	if rb.InvalidToolCalls == nil || *rb.InvalidToolCalls != 1 || rb.AllToolCalls != 2 {
		t.Fatalf("checks = %+v", rb.ToolCallChecks)
	}
	if rb.RejectedAttempts != 0 {
		t.Errorf("rejected_attempts = %d: an invalid call is not a rejected fix", rb.RejectedAttempts)
	}
	b, err := json.Marshal(rb)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"invalid_tool_calls":1`, `"invalid_tool_call_reasons":{"unknown_tool":1}`, `"all_tool_calls":2`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("run JSON lacks %s", want)
		}
	}
	// The rejected call costs what any extra tool call costs (efficiency),
	// nothing more: the counter itself does not touch the grade.
	again := Judge(sc, bad, nil, false)
	again = withReplayChecks(sc, bad, again)
	if again.Score != rb.Score || again.Pass != rb.Pass || rg.Pass != rb.Pass {
		t.Errorf("grade moved: good %v/%v bad %v/%v", rg.Pass, rg.Score, rb.Pass, rb.Score)
	}
}

func intp(n int) *int { return &n }

func checked(n, all int) ToolCallChecks {
	return ToolCallChecks{InvalidToolCalls: intp(n), AllToolCalls: all}
}

func TestSummaryInvalidCalls(t *testing.T) {
	a := []RunResult{
		{Scenario: "a", ToolCallsToRootCause: -1, ReplayResult: ReplayResult{ToolCallChecks: checked(1, 10)}},
		{Scenario: "a", ToolCallsToRootCause: -1, ReplayResult: ReplayResult{ToolCallChecks: checked(0, 10)}},
		// An infra run is in no count.
		infraResult("a", 3, "HTTP 401"),
	}
	b := []RunResult{{Scenario: "b", ToolCallsToRootCause: -1, ReplayResult: ReplayResult{ToolCallChecks: checked(3, 30)}}}
	sa, sb := summarize(a), summarize(b)
	if *sa.InvalidToolCalls != 1 || *sa.AllToolCalls != 20 || *sa.InvalidToolCallRate != 0.05 {
		t.Errorf("a = %d/%d %v", *sa.InvalidToolCalls, *sa.AllToolCalls, *sa.InvalidToolCallRate)
	}
	o := combine([]Summary{sa, sb})
	// Pooled over every tool call: 4/50, not the mean of 0.05 and 0.1.
	if *o.InvalidToolCalls != 4 || *o.AllToolCalls != 50 || *o.InvalidToolCallRate != 0.08 {
		t.Errorf("overall = %d/%d %v", *o.InvalidToolCalls, *o.AllToolCalls, *o.InvalidToolCallRate)
	}
	// A run without the counter (an older report) makes the total unknown,
	// not a low count.
	old := summarize(append(b, RunResult{Scenario: "b", ToolCallsToRootCause: -1}))
	if old.InvalidToolCalls != nil || old.InvalidToolCallRate != nil {
		t.Errorf("unchecked run counted: %v", *old.InvalidToolCalls)
	}
	if o := combine([]Summary{sa, old}); o.InvalidToolCalls != nil {
		t.Errorf("overall with an unchecked scenario = %d", *o.InvalidToolCalls)
	}
	// Zero is reported as zero, not omitted.
	z := summarize([]RunResult{{ToolCallsToRootCause: -1, ReplayResult: ReplayResult{ToolCallChecks: checked(0, 0)}}})
	js, _ := json.Marshal(z)
	if !strings.Contains(string(js), `"invalid_tool_calls":0`) || !strings.Contains(string(js), `"invalid_tool_call_rate":0`) {
		t.Errorf("zero summary JSON: %s", js)
	}
}

func TestReportPrintsInvalidCalls(t *testing.T) {
	sc := []Scenario{{Name: "a"}}
	res := RunResult{Scenario: "a", Run: 1, Pass: true, ToolCallsToRootCause: -1, ReplayResult: ReplayResult{ToolCallChecks: ToolCallChecks{
		InvalidToolCalls: intp(2), AllToolCalls: 8, InvalidToolCallReasons: map[string]int{"invalid_input": 1, "unknown_tool": 1},
	}}}
	rep := buildReport(Config{Runs: 1}, sc, []RunResult{res}, time.Time{})
	var b strings.Builder
	if err := rep.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"| misses | invalid tool calls | rejected fixes |",
		"| 0 | 2 (25.0%) | 0 |",
		"invalid tool calls: 2 of 8 tool calls (25.0%): invalid_input 1, unknown_tool 1",
		"not rejected fixes",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown lacks %q:\n%s", want, out)
		}
	}
	var js strings.Builder
	if err := rep.WriteJSON(&js); err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal([]byte(js.String()), &back); err != nil {
		t.Fatal(err)
	}
	if back.Overall.InvalidToolCalls == nil || *back.Overall.InvalidToolCalls != 2 || *back.Overall.InvalidToolCallRate != 0.25 ||
		back.Results[0].InvalidToolCalls == nil || *back.Results[0].InvalidToolCalls != 2 {
		t.Errorf("round trip lost the counter: %+v", back.Overall)
	}
}

// TestCompareInvalidCalls: per run over the shared scenarios; growth is
// flagged, an older baseline without the counter is unknown, never grown.
func TestCompareInvalidCalls(t *testing.T) {
	sum := func(name string, runs int, invalid *int) ScenarioSummary {
		s := Summary{Runs: runs, Passes: runs, PassRate: 1, MeanScore: 90, InvalidToolCalls: invalid}
		return ScenarioSummary{Name: name, Summary: s}
	}
	rep := func(runs int, ss ...ScenarioSummary) Report { return Report{Runs: runs, Scenarios: ss} }
	cases := []struct {
		name       string
		base, cand Report
		want       InvalidCallsComparison
	}{
		{
			name: "same per run: 3-run baseline, 1-run candidate",
			base: rep(3, sum("a", 3, intp(3))), cand: rep(1, sum("a", 1, intp(1))),
			want: InvalidCallsComparison{Known: true, Shared: 1, BasePerRun: 1, CandPerRun: 1},
		},
		{
			name: "grew",
			base: rep(3, sum("a", 3, intp(0)), sum("b", 3, intp(3))), cand: rep(3, sum("a", 3, intp(1)), sum("b", 3, intp(3))),
			want: InvalidCallsComparison{Known: true, Shared: 2, BasePerRun: 0.5, CandPerRun: 4.0 / 6, Grew: true},
		},
		{
			name: "fell",
			base: rep(1, sum("a", 1, intp(2))), cand: rep(1, sum("a", 1, intp(0))),
			want: InvalidCallsComparison{Known: true, Shared: 1, BasePerRun: 2, CandPerRun: 0},
		},
		{
			name: "only shared scenarios count",
			base: rep(1, sum("a", 1, intp(0)), sum("z", 1, intp(9))), cand: rep(1, sum("a", 1, intp(0)), sum("new", 1, intp(9))),
			want: InvalidCallsComparison{Known: true, Shared: 1},
		},
		{
			name: "older baseline without the counter",
			base: rep(1, sum("a", 1, nil)), cand: rep(3, sum("a", 3, intp(5))),
			want: InvalidCallsComparison{Shared: 1},
		},
		{
			name: "nothing shared",
			base: rep(1, sum("a", 1, intp(0))), cand: rep(1, sum("b", 1, intp(1))),
			want: InvalidCallsComparison{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Compare(tc.base, tc.cand, 5)
			if c.InvalidToolCalls != tc.want {
				t.Errorf("got %+v, want %+v", c.InvalidToolCalls, tc.want)
			}
			// A base scenario missing from the candidate is a regression
			// of its own; invalid calls never add one.
			missing := 0
			for _, r := range c.Rows {
				if r.Reason == "missing from candidate" {
					missing++
				}
			}
			if c.Regressions != missing {
				t.Errorf("regressions = %d: invalid calls are not a scenario regression", c.Regressions)
			}
		})
	}
}

// TestCompareOldBaselineFile loads the committed 1-run baseline, written
// before V9r: the comparison says unknown and does not flag growth.
func TestCompareOldBaselineFile(t *testing.T) {
	path := filepath.Join("baseline", "default.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skip("no committed baseline")
	}
	if strings.Contains(string(raw), `"invalid_tool_calls"`) {
		t.Skip("the committed baseline already carries the counter")
	}
	base, err := LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	if base.Overall.InvalidToolCalls != nil {
		t.Fatal("old baseline read as checked")
	}
	cand := base
	cand.Scenarios = append([]ScenarioSummary(nil), base.Scenarios...)
	for i := range cand.Scenarios {
		cand.Scenarios[i].InvalidToolCalls = intp(4)
	}
	c := Compare(base, cand, 5)
	if c.InvalidToolCalls.Known || c.InvalidToolCalls.Grew {
		t.Errorf("old baseline compared: %+v", c.InvalidToolCalls)
	}
	var b strings.Builder
	if err := c.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "invalid tool calls: unknown, not compared") {
		t.Errorf("markdown:\n%s", b.String())
	}
}
