package redact

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// scenarioFile holds the parts of an evals scenario that carry tool text.
type scenarioFile struct {
	Name  string `yaml:"name"`
	Alert struct {
		Title       string `yaml:"title"`
		PipelineMap string `yaml:"pipeline_map"`
	} `yaml:"alert"`
	Recordings []struct {
		Tool    string `yaml:"tool"`
		Summary string `yaml:"summary"`
		Data    any    `yaml:"data"`
		Error   string `yaml:"error"`
	} `yaml:"recordings"`
	Inputs []struct {
		Text string `yaml:"text"`
	} `yaml:"inputs"`
	Expect struct {
		RootCause         [][]string `yaml:"root_cause"`
		FirstFailingLayer string     `yaml:"first_failing_layer"`
		EvidenceTools     []string   `yaml:"evidence_tools"`
		Fix               [][]string `yaml:"fix"`
		ForbiddenActions  []string   `yaml:"forbidden_actions"`
	} `yaml:"expect"`
}

func (s scenarioFile) texts() []string {
	out := []string{s.Alert.Title, s.Alert.PipelineMap}
	for _, r := range s.Recordings {
		out = append(out, r.Summary, r.Error)
	}
	for _, in := range s.Inputs {
		out = append(out, in.Text)
	}
	return out
}

func (s scenarioFile) terms() []string {
	var out []string
	for _, g := range s.Expect.RootCause {
		out = append(out, g...)
	}
	for _, g := range s.Expect.Fix {
		out = append(out, g...)
	}
	out = append(out, s.Expect.EvidenceTools...)
	out = append(out, s.Expect.ForbiddenActions...)
	if s.Expect.FirstFailingLayer != "" {
		out = append(out, s.Expect.FirstFailingLayer)
	}
	return out
}

// evidencePatterns are kinds of evidence that must survive redaction in
// every scenario, counted per pattern before and after.
var evidencePatterns = map[string]*regexp.Regexp{
	"lsn":        regexp.MustCompile(`\b[0-9A-F]{1,8}/[0-9A-F]{1,8}\b`),
	"offset":     regexp.MustCompile(`[a-z0-9._-]+/[0-9]+@[0-9]+`),
	"sqlstate":   regexp.MustCompile(`\b[0-9A-Z]{5}\b`),
	"exception":  regexp.MustCompile(`\b(?:[a-z][a-z0-9_]*\.)+[A-Z][A-Za-z0-9_]*(?:Exception|Error)\b`),
	"timestamp":  regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z`),
	"schema_id":  regexp.MustCompile(`\b(?:id|version|versions) \[?[0-9]+`),
	"topic":      regexp.MustCompile(`\bpg\.public\.[a-z_]+(?:-(?:key|value))?`),
	"connector":  regexp.MustCompile(`\bpg-orders-connector\b`),
	"slot":       regexp.MustCompile(`\bdebezium_orders\b`),
	"partitions": regexp.MustCompile(`\b(?:committed|end|lag|offset)[ =][0-9]+`),
}

func TestScenarioFixturesKeepEvidence(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "evals", "scenarios", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no scenarios found under evals/scenarios")
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var sc scenarioFile
		if err := yaml.Unmarshal(b, &sc); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		t.Run(sc.Name, func(t *testing.T) {
			var before, after strings.Builder
			lines, changed := 0, 0
			for _, txt := range sc.texts() {
				if txt == "" {
					continue
				}
				out, _ := Text(txt)
				in, red := strings.Split(txt, "\n"), strings.Split(out, "\n")
				lines += len(in)
				if len(in) != len(red) {
					t.Errorf("line count changed: %q -> %q", txt, out)
					continue
				}
				for i := range in {
					if in[i] != red[i] {
						changed++
						t.Logf("changed line:\n  in:  %s\n  out: %s", in[i], red[i])
					}
				}
				before.WriteString(txt + "\n")
				after.WriteString(out + "\n")
			}
			for _, r := range sc.Recordings {
				if r.Data == nil {
					continue
				}
				if got := JSON(r.Data); !jsonEqual(got, r.Data) {
					t.Errorf("%s data changed: %v -> %v", r.Tool, r.Data, got)
				}
			}
			if pct := 100 * float64(changed) / float64(lines); pct >= 2 {
				t.Errorf("%d of %d lines changed (%.2f%%), want < 2%%", changed, lines, pct)
			}
			bl, al := strings.ToLower(before.String()), strings.ToLower(after.String())
			for _, term := range sc.terms() {
				term = strings.ToLower(term)
				if nb, na := strings.Count(bl, term), strings.Count(al, term); nb != na {
					t.Errorf("ground-truth term %q: %d occurrences before, %d after", term, nb, na)
				}
			}
			for name, re := range evidencePatterns {
				nb, na := len(re.FindAllString(before.String(), -1)), len(re.FindAllString(after.String(), -1))
				if nb != na {
					t.Errorf("evidence %s: %d before, %d after", name, nb, na)
				}
			}
			t.Logf("%s: %d lines, %d changed", sc.Name, lines, changed)
		})
	}
}

func jsonEqual(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k := range x {
			if !jsonEqual(x[k], y[k]) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !jsonEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	}
	return a == b
}
