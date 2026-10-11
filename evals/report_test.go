package evals

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rep(label string, runs int, scen map[string]Summary) Report {
	r := Report{Label: label, Runs: runs, Model: "m"}
	var ss []Summary
	for _, name := range []string{"a", "b", "c"} {
		if s, ok := scen[name]; ok {
			r.Scenarios = append(r.Scenarios, ScenarioSummary{Name: name, Summary: s})
			ss = append(ss, s)
		}
	}
	r.Overall = combine(ss)
	return r
}

func TestCompare(t *testing.T) {
	base := rep("main", 3, map[string]Summary{
		"a": {Runs: 3, Passes: 3, PassRate: 1, MeanScore: 95},
		"b": {Runs: 3, Passes: 2, PassRate: 2.0 / 3, MeanScore: 80},
		"c": {Runs: 3, Passes: 3, PassRate: 1, MeanScore: 90},
	})
	cand := rep("v2", 3, map[string]Summary{
		"a": {Runs: 3, Passes: 3, PassRate: 1, MeanScore: 88}, // score drop > 5
		"b": {Runs: 3, Passes: 3, PassRate: 1, MeanScore: 90}, // better
		// c missing
	})
	c := Compare(base, cand, DefaultScoreTolerance)
	byName := map[string]CompareRow{}
	for _, r := range c.Rows {
		byName[r.Scenario] = r
	}
	if r := byName["a"]; !r.Regressed || !strings.Contains(r.Reason, "score 95.0 → 88.0") {
		t.Errorf("a = %+v", r)
	}
	if r := byName["b"]; !r.Improved || r.Regressed {
		t.Errorf("b = %+v", r)
	}
	if r := byName["c"]; !r.Regressed || r.Reason != "missing from candidate" {
		t.Errorf("c = %+v", r)
	}
	if c.Regressions != 2 {
		t.Errorf("regressions = %d, want 2", c.Regressions)
	}

	var b bytes.Buffer
	if err := c.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"| a | 3/3 → 3/3 | 95.0 → 88.0 (-7.0)", "**REGRESSED**", "2 scenario(s) regressed"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("markdown lacks %q:\n%s", want, b.String())
		}
	}
}

func TestCompareSmallChangesAndNotes(t *testing.T) {
	base := rep("main", 1, map[string]Summary{"a": {Runs: 1, Passes: 1, PassRate: 1, MeanScore: 90}})
	cand := rep("v2", 1, map[string]Summary{
		"a": {Runs: 1, Passes: 1, PassRate: 1, MeanScore: 87},
		"b": {Runs: 1, Passes: 0, MeanScore: 10},
	})
	cand.Model = "other"
	c := Compare(base, cand, -1) // negative → default tolerance
	if c.Regressions != 0 || c.Tolerance != DefaultScoreTolerance {
		t.Errorf("small drop counted: %+v", c)
	}
	if c.Rows[1].Reason != "new scenario" || c.Rows[1].Regressed {
		t.Errorf("new scenario row = %+v", c.Rows[1])
	}
	notes := strings.Join(c.Notes, "\n")
	if !strings.Contains(notes, "fewer than 3 runs") || !strings.Contains(notes, "model differs") {
		t.Errorf("notes = %q", notes)
	}
	pass := rep("v3", 1, map[string]Summary{"a": {Runs: 1, Passes: 0, MeanScore: 89}})
	if c := Compare(base, pass, 5); c.Regressions != 1 {
		t.Error("pass-rate drop not a regression even within score tolerance")
	}
}

func TestSummarize(t *testing.T) {
	s := summarize([]RunResult{
		{Pass: true, Score: 100, RootCause: true, LayerNamed: true, ToolCalls: 4, ToolCallsToRootCause: 3, DurationMS: 10},
		{Score: 50, ToolCalls: 8, ToolCallsToRootCause: -1, DurationMS: 30, Error: "x", Misses: []Miss{{Tool: "t"}}},
	})
	if s.Runs != 2 || s.Passes != 1 || s.PassRate != 0.5 || s.MeanScore != 75 || s.RootCauseRate != 0.5 ||
		s.LayerRate != 0.5 || s.MeanToolCalls != 6 || s.MeanToolCallsToRootCause != 3 || s.Misses != 1 || s.Errors != 1 || s.MeanDurationMS != 20 {
		t.Errorf("summary = %+v", s)
	}
	if e := summarize(nil); e.MeanToolCallsToRootCause != -1 || e.Runs != 0 {
		t.Errorf("empty = %+v", e)
	}
}

func TestReportRoundTripAndMarkdown(t *testing.T) {
	r := buildReport(Config{Label: "x|y", Runs: 1}, []Scenario{{Name: "a"}, {Name: "b"}}, []RunResult{
		{Scenario: "a", Run: 1, Pass: true, Score: 100, ToolCallsToRootCause: 2},
		{Scenario: "b", Run: 1, Score: 20, ToolCallsToRootCause: -1, Failures: []string{"wrong root cause", "bad | pipe\nnewline"}},
	}, timeZero())
	if r.Overall.PassRate != 0.5 {
		t.Errorf("overall = %+v", r.Overall)
	}
	path := filepath.Join(t.TempDir(), "r.json")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WriteJSON(f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	back, err := LoadReport(path)
	if err != nil || back.Overall != r.Overall || len(back.Results) != 2 {
		t.Fatalf("round trip: %v %+v", err, back.Overall)
	}
	if _, err := LoadReport(filepath.Join(t.TempDir(), "none.json")); err == nil {
		t.Error("missing report accepted")
	}

	var b bytes.Buffer
	if err := r.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	md := b.String()
	for _, want := range []string{"## Agent eval: x\\|y", "| a | 1/1 | 100.0 |", "| **overall** | 1/2 |", "- b #1: wrong root cause; bad \\| pipe newline"} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown lacks %q:\n%s", want, md)
		}
	}
}
