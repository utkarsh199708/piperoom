package evals

import (
	"bytes"
	"strings"
	"testing"
)

// A failed run's markdown lines say what the agent concluded and proposed and
// which root_cause groups it missed, so a CI log explains "wrong root cause"
// without the JSON artifact.
func TestJudgeRecordsWhyARunFailed(t *testing.T) {
	sc := loadSeed()
	good := goodSchemaChangeScript()
	setCompat := ps("schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`)
	script := append(append([]step(nil), good[:3]...),
		step{hypothesis: "The consumer order-sync is stuck on a poison pill."}, proposal(setCompat))
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	if r.RootCause {
		t.Fatal("setup: the hypothesis should miss the root cause")
	}
	if len(r.MissingCauseGroups) == 0 || !strings.HasPrefix(r.MissingCauseGroups[0], "group ") || !strings.Contains(r.MissingCauseGroups[0], "[") {
		t.Errorf("missing groups = %q", r.MissingCauseGroups)
	}
	if len(r.ProposedActions) != 1 || r.ProposedActions[0] != "schema_set_compatibility" {
		t.Errorf("proposed actions = %q", r.ProposedActions)
	}

	// The passing run misses no group.
	ok := Judge(sc, transcriptOf(t, sc, good), nil, false)
	if len(ok.MissingCauseGroups) != 0 {
		t.Errorf("passing run lists missing groups: %q", ok.MissingCauseGroups)
	}
}

func TestMarkdownPrintsFailedRunDetailsRedactedAndCut(t *testing.T) {
	secret := "sk-ant-api03-" + strings.Repeat("Ab3x", 12) + "9Q"
	rep := Report{Results: []RunResult{
		{
			Scenario: "s1", Run: 1, Failures: []string{"wrong root cause"},
			FinalHypothesis:    "Connect failed because of " + secret + " " + strings.Repeat("x", 400),
			LastMessage:        "Task 0 is stuck; key " + secret + " " + strings.Repeat("y", 500),
			Confidence:         "high",
			ProposedActions:    []string{"connect_restart_task", "schema_register"},
			MissingCauseGroups: []string{"group 2: [missing, gone]"},
		},
		{Scenario: "s2", Run: 1, Pass: true, FinalHypothesis: "fine"},
		{Scenario: "s3", Run: 1, Failures: []string{"no hypothesis"}},
	}}
	var b bytes.Buffer
	if err := rep.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"- s1 #1: wrong root cause",
		"hypothesis (confidence high, first layer named false, root-cause layer named false): Connect failed because of",
		"proposed actions: connect_restart_task, schema_register",
		"missing group 2: [missing, gone]",
		"- s3 #1: no hypothesis",
		"last message: Task 0 is stuck; key ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, secret) {
		t.Errorf("secret reached the markdown:\n%s", out)
	}
	if strings.Contains(out, "fine") {
		t.Errorf("passing run printed:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "hypothesis (confidence") {
			// the text is cut at 300 runes (plus the ellipsis) and masked before that
			text := line[strings.Index(line, "): ")+3:]
			if n := len([]rune(text)); n > 301 {
				t.Errorf("hypothesis text is %d runes, want at most 301", n)
			}
		}
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "- last message: ") {
			text := line[strings.Index(line, "- last message: ")+len("- last message: "):]
			if n := len([]rune(text)); n > 401 {
				t.Errorf("last message text is %d runes, want at most 401", n)
			}
		}
	}
	// s3 has no hypothesis, no actions, no groups: nothing under it.
	if strings.Contains(out, "- s3 #1: no hypothesis\n  -") {
		t.Errorf("empty details printed under s3:\n%s", out)
	}
}

// TestMarkdownExplainsFailedHealthyVerdict: a healthy run that failed its
// verdict prints the fault term and the clause that tripped it, or that the
// text has no healthy phrase, so the job log alone diagnoses it.
func TestMarkdownExplainsFailedHealthyVerdict(t *testing.T) {
	rep := Report{Results: []RunResult{
		{
			Scenario: "h1", Run: 1, Outcome: OutcomeHealthy,
			Failures:        []string{"final message does not say the pipeline is healthy or that there is nothing to fix"},
			FinalHypothesis: "Pipeline orders is healthy now: warehouse 1m stale (s_01).",
			Confidence:      "low",
			LastMessage:     "Nothing to fix. The warehouse is 41m stale, which the DBA knows.",
		},
		{
			Scenario: "h2", Run: 1, Outcome: OutcomeHealthy,
			Failures:    []string{"final message does not say the pipeline is healthy or that there is nothing to fix"},
			LastMessage: "I checked the slot and the connector.",
		},
		{
			Scenario: "h3", Run: 1, Outcome: OutcomeHealthy, SaysHealthy: true,
			Failures:    []string{"proposed a fix on a healthy pipeline"},
			LastMessage: "All good.",
		},
	}}
	var b bytes.Buffer
	if err := rep.WriteMarkdown(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		`healthy verdict failed: last message: fault "stale" in "the warehouse is 41m stale"`,
		"healthy verdict failed: last message: no healthy phrase",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "hypothesis: fault") || strings.Contains(out, "hypothesis: no healthy") {
		t.Errorf("a healthy hypothesis got a note:\n%s", out)
	}
	if strings.Count(out, "healthy verdict failed") != 2 {
		t.Errorf("want the note under h1 and h2 only:\n%s", out)
	}
}
