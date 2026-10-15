package evals

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// rootCauseCorpus is the committed set of right and wrong root-cause wordings
// for the held-out scenarios whose term rows #332 will rewrite (#427). Reviewers
// kept finding wordings the rows misgraded, so each wording lives here and the
// rows change only after it is added. The rows are origin/main's; a text they
// grade wrong carries a gap and is asserted at the wrong grade, so a change that
// fixes it (or breaks a text that is not a gap) fails the test.
const rootCauseCorpusPath = heldoutDir + "/fixtures/rootcause_corpus.yaml"

type corpusEntry struct {
	Scenario      string `yaml:"scenario"`
	Text          string `yaml:"text"`
	WantRootCause bool   `yaml:"want_root_cause"` // the correct grade
	Source        string `yaml:"source"`
	Gap           string `yaml:"gap"` // set when the rows grade the text wrong; the reason
}

func loadRootCauseCorpus(t *testing.T) []corpusEntry {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(rootCauseCorpusPath))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Entries []corpusEntry `yaml:"entries"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatalf("%s: %v", rootCauseCorpusPath, err)
	}
	return doc.Entries
}

// withHypothesis returns the scenario's focused script with its hypothesis
// replaced by text, so a full run grades the same investigation with a
// different wording.
func withHypothesis(script []step, text string) []step {
	out := make([]step, len(script))
	copy(out, script)
	for i, s := range out {
		if s.hypothesis != "" {
			out[i].hypothesis = text
		}
	}
	return out
}

// corpusLayerTail names the layers a full run must state. The corpus texts
// are about the cause only, so a right answer gets this appended for the full
// run; it adds no term any forbidden row looks for.
var corpusLayerTail = map[string]string{
	"users-compat-none-consumer":       " The consumer user-profile-sync is the first failing layer; the root cause is in the schema-registry layer.",
	"ledger-producer-timeout-recovery": " The first failing layer is kafka-connect (the connector); the root cause is in the kafka layer, at the broker.",
	"payments-sink-dlq":                " The failing layer is kafka-connect: the sink connector.",
}

var corpusSources = map[string]bool{"#332": true, "r1": true, "r2": true, "r3": true, "r4": true}

func TestHeldoutRootCauseCorpusShape(t *testing.T) {
	entries := loadRootCauseCorpus(t)
	if len(entries) < 150 {
		t.Fatalf("%d corpus entries, want at least 150", len(entries))
	}
	seen := map[string]bool{}
	for i, e := range entries {
		if e.Text == "" || e.Scenario == "" {
			t.Errorf("entry %d: scenario and text are required: %+v", i, e)
		}
		if !corpusSources[e.Source] {
			t.Errorf("entry %d (%q): source %q is not one of #332, r1..r4", i, e.Text, e.Source)
		}
		if k := e.Scenario + "\x00" + e.Text; seen[k] {
			t.Errorf("entry %d: duplicate text %q", i, e.Text)
		} else {
			seen[k] = true
		}
	}
}

// TestHeldoutRootCauseCorpusGapReasons checks the raw document: yaml decodes a
// missing want_root_cause and `gap: ""` to zero values (a trap, and no gap), so
// both are checked before decoding.
func TestHeldoutRootCauseCorpusGapReasons(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash(rootCauseCorpusPath))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Entries []map[string]any `yaml:"entries"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	for i, e := range doc.Entries {
		if _, ok := e["want_root_cause"].(bool); !ok {
			t.Errorf("entry %d (%q): want_root_cause must be a boolean", i, e["text"])
		}
		if g, ok := e["gap"]; ok {
			if s, _ := g.(string); strings.TrimSpace(s) == "" {
				t.Errorf("entry %d (%q): gap needs a reason", i, e["text"])
			}
		}
	}
}

// Every corpus text is graded by rootCauseMatches against its scenario's
// expect. A gap entry is asserted at the grade the rows give today, the
// opposite of want_root_cause. A right answer without a gap also gets one full
// run, so a forbidden-story hit or a missing fact fails it, not only the term
// groups.
func TestHeldoutRootCauseCorpus(t *testing.T) {
	scripts := heldoutScripts(t)
	for i, e := range loadRootCauseCorpus(t) {
		t.Run(fmt.Sprintf("%03d/%s/%s", i, e.Scenario, e.Source), func(t *testing.T) {
			sc := loadScenario(t, filepath.Join(heldoutDir, e.Scenario+".yaml"))
			got := rootCauseMatches(sc.Expect, e.Text)
			if e.Gap != "" {
				if got == e.WantRootCause {
					t.Errorf("entry %d: gap fixed: drop gap and keep want (want %v, rows now grade %v) for %q", i, e.WantRootCause, got, e.Text)
				}
				return
			}
			if got != e.WantRootCause {
				t.Errorf("entry %d: rootCauseMatches = %v, want %v for %q", i, got, e.WantRootCause, e.Text)
			}
			if !e.WantRootCause {
				return
			}
			script, ok := scripts[e.Scenario]
			if !ok {
				t.Fatalf("no focused script for %s", e.Scenario)
			}
			r, _ := evalOnce(t, sc, withHypothesis(script, e.Text+corpusLayerTail[e.Scenario]))
			if !r.Pass || r.Score != 100 {
				t.Errorf("entry %d: full run of right answer %q: pass=%v score=%v failures=%v", i, e.Text, r.Pass, r.Score, r.Failures)
			}
		})
	}
}
