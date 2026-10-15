package evals

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// rootCauseCorpus is the committed set of right and wrong root-cause wordings
// for the held-out scenarios whose term rows #332 rewrote (#427). Reviewers kept
// finding wordings the rows misgraded, so each wording lives here and the rows
// change only after it is added.
const rootCauseCorpusPath = heldoutDir + "/fixtures/rootcause_corpus.yaml"

type corpusEntry struct {
	Scenario      string `yaml:"scenario"`
	Text          string `yaml:"text"`
	WantRootCause bool   `yaml:"want_root_cause"`
	MainWrong     bool   `yaml:"main_wrong"`
	Source        string `yaml:"source"`
	Gap           string `yaml:"gap"`
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
}

func TestHeldoutRootCauseCorpusShape(t *testing.T) {
	entries := loadRootCauseCorpus(t)
	if len(entries) < 60 {
		t.Fatalf("%d corpus entries, want at least 60", len(entries))
	}
	seen := map[string]bool{}
	for i, e := range entries {
		if e.Text == "" || e.Scenario == "" || e.Source == "" {
			t.Errorf("entry %d: scenario, text and source are required: %+v", i, e)
		}
		if e.MainWrong != (e.Gap != "") {
			t.Errorf("entry %d (%q): main_wrong and gap go together", i, e.Text)
		}
		if k := e.Scenario + "\x00" + e.Text; seen[k] {
			t.Errorf("entry %d: duplicate text %q", i, e.Text)
		} else {
			seen[k] = true
		}
	}
}

// Every corpus text is graded by rootCauseMatches against its scenario's
// expect. A text wanted as a right answer also gets one full run, so a
// forbidden-story hit or a missing fact fails it, not only the term groups.
func TestHeldoutRootCauseCorpus(t *testing.T) {
	scripts := heldoutScripts(t)
	for i, e := range loadRootCauseCorpus(t) {
		t.Run(e.Scenario+"/"+e.Source, func(t *testing.T) {
			sc := loadScenario(t, filepath.Join(heldoutDir, e.Scenario+".yaml"))
			if got := rootCauseMatches(sc.Expect, e.Text); got != e.WantRootCause {
				t.Errorf("entry %d: rootCauseMatches = %v, want %v for %q (%s)", i, got, e.WantRootCause, e.Text, e.Gap)
			}
			script, ok := scripts[e.Scenario]
			if !ok {
				t.Fatalf("no focused script for %s", e.Scenario)
			}
			if !e.WantRootCause || e.MainWrong {
				return
			}
			r, _ := evalOnce(t, sc, withHypothesis(script, e.Text+corpusLayerTail[e.Scenario]))
			if !r.Pass || r.Score != 100 {
				t.Errorf("entry %d: full run of right answer %q: pass=%v score=%v failures=%v", i, e.Text, r.Pass, r.Score, r.Failures)
			}
		})
	}
}
