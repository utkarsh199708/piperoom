package evals

// The supported-model list check (V12, docs/design/model-providers.md 5.3 and
// 5.4). ci.yml runs it as its own step: every row of
// internal/agent/llm/supported.json has a committed 3-run baseline that passes
// the gate against the current reference, and no row is past its sunset. It
// lives here (not in internal/agent/llm) because it reads the gate and the
// baseline folder, which are this package's.

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
)

const supportedListPath = "../internal/agent/llm/supported.json"

// checkSupported returns one problem per failure of the list raw against the
// baselines in dir on day today: a list that does not parse, a baseline that is
// missing, misnamed, of another model or too short, one that the gate refuses
// (or, for a row listed as supported, one that only reaches experimental), and
// a sunset that has passed. An empty list needs no reference.
func checkSupported(raw []byte, dir string, c GateConfig, today time.Time) []string {
	list, err := llm.ParseSupported(raw)
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	var ref *Report
	for _, m := range list {
		where := fmt.Sprintf("%s %s %s", m.Provider, m.Flavor, m.Model)
		where = strings.Join(strings.Fields(where), " ")
		if m.Sunset != "" {
			sunset, _ := time.Parse(time.DateOnly, m.Sunset) // ParseSupported checked it
			day := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
			if day.After(sunset) {
				problems = append(problems, fmt.Sprintf("%s: sunset %s has passed: remove the row or move the model", where, m.Sunset))
			}
		}
		// The baseline field is the file name or its path from the repository root.
		file := m.Baseline
		if strings.Contains(file, "/") {
			if path.Dir(file) != "evals/baseline" {
				problems = append(problems, fmt.Sprintf("%s: baseline %q is not under evals/baseline/", where, m.Baseline))
				continue
			}
			file = path.Base(file)
		}
		if want := BaselineName(m.Provider, m.Model); file != want {
			problems = append(problems, fmt.Sprintf("%s: baseline file is %s, want %s", where, file, want))
			continue
		}
		cand, err := LoadReport(filepath.Join(dir, file))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: no usable baseline %s: %v", where, file, err))
			continue
		}
		if cand.Provider != m.Provider || cand.Model != m.Model || cand.Flavor != m.Flavor {
			problems = append(problems, fmt.Sprintf("%s: %s is a report of %s %s %s", where, file, cand.Provider, cand.Flavor, cand.Model))
			continue
		}
		if ref == nil {
			r, err := loadReference(dir)
			if err != nil {
				return append(problems, fmt.Sprintf("%s: %v", where, err))
			}
			ref = &r
		}
		g, err := Gate(*ref, cand, c)
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("%s: %s cannot be gated against the reference (3 runs needed): %v", where, file, err))
		case g.Verdict == GateRefused:
			problems = append(problems, fmt.Sprintf("%s: %s is refused by the gate against the reference", where, file))
		case g.Verdict == GateExperimental && m.Status == llm.GateSupported:
			problems = append(problems, fmt.Sprintf("%s: listed as supported but %s only reaches experimental: change the row to experimental in the PR that moves the reference", where, file))
		}
	}
	return problems
}

func loadReference(dir string) (Report, error) {
	for _, name := range ReferenceBaselines {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return LoadReport(filepath.Join(dir, name))
		}
	}
	return Report{}, fmt.Errorf("no reference baseline (%s) in %s", strings.Join(ReferenceBaselines, " or "), dir)
}

// TestSupportedListGate is the check of the repository as committed: it passes
// while supported.json is empty and fails as soon as a row loses its baseline,
// its gate or its sunset date.
func TestSupportedListGate(t *testing.T) {
	raw, err := os.ReadFile(supportedListPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	problems := checkSupported(raw, "baseline", defaultGate(t), now)
	for _, p := range problems {
		t.Error(p)
	}
	if list, err := llm.ParseSupported(raw); err == nil {
		for _, m := range list {
			if m.Sunset == "" {
				continue
			}
			if sunset, _ := time.Parse(time.DateOnly, m.Sunset); now.After(sunset.AddDate(0, 0, -llm.SunsetWarnDays)) {
				t.Logf("%s %s: sunset %s is within %d days", m.Provider, m.Model, m.Sunset, llm.SunsetWarnDays)
			}
		}
	}
}

// writeFixtureBaseline writes a fixture report as provider/model's baseline.
func writeFixtureBaseline(t *testing.T, dir, file string, rep Report) {
	t.Helper()
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, file), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func fixtureRow(provider, flavor, model, status, extra string) string {
	f := ""
	if flavor != "" {
		f = fmt.Sprintf(`"flavor":%q,`, flavor)
	}
	return fmt.Sprintf(`{"models":[{"provider":%q,%s"model":%q,"status":%q,"baseline":%q,"gated_at":"2026-10-20"%s}]}`,
		provider, f, model, status, "evals/baseline/"+BaselineName(provider, model), extra)
}

func TestCheckSupportedFixtures(t *testing.T) {
	c := defaultGate(t)
	today := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	good := fxReport(3, fxSet(nil), 10, 0)
	named := func(r Report, provider, flavor, model string) Report {
		r.Provider, r.Flavor, r.Model = provider, flavor, model
		return r
	}
	// A candidate 2 of 3 runs short of the reference's root cause rate.
	weak := fxReport(3, fxSet(func(i int, s *fxScenario) { s.rc = 0; s.layer = 0; s.pass = 0 }), 10, 0)
	cases := []struct {
		name  string
		list  string
		files map[string]Report
		want  string // "" = no problem; else a substring of the one problem
	}{
		{"empty list needs nothing", `{"models":[]}`, nil, ""},
		{"row with a passing baseline", fixtureRow("openai", "groq", "m1", "supported", ""),
			map[string]Report{BaselineName("openai", "m1"): named(good, "openai", "groq", "m1")}, ""},
		{"row without a baseline file", fixtureRow("gemini", "", "m2", "supported", ""), nil, "no usable baseline"},
		{"baseline of another model", fixtureRow("gemini", "", "m2", "supported", ""),
			map[string]Report{BaselineName("gemini", "m2"): named(good, "gemini", "", "other")}, "is a report of"},
		{"baseline with 1 run", fixtureRow("gemini", "", "m2", "supported", ""),
			map[string]Report{BaselineName("gemini", "m2"): named(fxReport(1, fxSet(nil), 10, 0), "gemini", "", "m2")}, "cannot be gated"},
		{"baseline refused by the gate", fixtureRow("gemini", "", "m2", "experimental", ""),
			map[string]Report{BaselineName("gemini", "m2"): named(fxReport(3, fxSet(nil), 10, 7), "gemini", "", "m2")}, "refused by the gate"},
		{"supported row whose baseline is experimental", fixtureRow("gemini", "", "m2", "supported", ""),
			map[string]Report{BaselineName("gemini", "m2"): named(weak, "gemini", "", "m2")}, "only reaches experimental"},
		{"experimental row whose baseline is experimental", fixtureRow("gemini", "", "m2", "experimental", ""),
			map[string]Report{BaselineName("gemini", "m2"): named(weak, "gemini", "", "m2")}, ""},
		{"sunset not yet reached", fixtureRow("gemini", "", "m2", "supported", `,"sunset":"2026-10-23"`),
			map[string]Report{BaselineName("gemini", "m2"): named(good, "gemini", "", "m2")}, ""},
		{"sunset passed", fixtureRow("gemini", "", "m2", "supported", `,"sunset":"2026-10-22"`),
			map[string]Report{BaselineName("gemini", "m2"): named(good, "gemini", "", "m2")}, "sunset 2026-10-22 has passed"},
		{"baseline outside evals/baseline", strings.Replace(fixtureRow("gemini", "", "m2", "supported", ""), "evals/baseline/", "evals/", 1), nil, "not under evals/baseline"},
		{"misnamed baseline", strings.Replace(fixtureRow("gemini", "", "m2", "supported", ""), "gemini-m2.json", "m2.json", 1), nil, "baseline file is m2.json"},
		{"list that does not parse", `{"models":[{"provider":"anthropic","model":"x"}]}`, nil, "Claude is never gated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFixtureBaseline(t, dir, "default.json", good)
			for f, r := range tc.files {
				writeFixtureBaseline(t, dir, f, r)
			}
			got := checkSupported([]byte(tc.list), dir, c, today)
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("problems = %v, want none", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("problems = %v, want one containing %q", got, tc.want)
			}
		})
	}
	t.Run("rows without a reference baseline", func(t *testing.T) {
		dir := t.TempDir()
		writeFixtureBaseline(t, dir, BaselineName("gemini", "m2"), named(good, "gemini", "", "m2"))
		got := checkSupported([]byte(fixtureRow("gemini", "", "m2", "supported", "")), dir, c, today)
		if len(got) != 1 || !strings.Contains(got[0], "no reference baseline") {
			t.Fatalf("problems = %v", got)
		}
	})
}
