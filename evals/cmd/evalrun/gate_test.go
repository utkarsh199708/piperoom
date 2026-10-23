package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"piperoom/evals"
	"piperoom/internal/config"
)

const (
	refPath  = "../../baseline/default.json"
	gateFile = "../../gate.yaml"
)

func loadRef(t *testing.T) evals.Report {
	t.Helper()
	r, err := evals.LoadReport(refPath)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestSavedGateExitCodes: -gate REF CAND on saved reports, no model calls.
// The candidates are the committed reference edited: one with the
// reference's forbidden proposals taken out (supported), one where a
// scenario the reference passes 3 of 3 collapses (exit 5), and the
// reference itself, whose six forbidden proposals fail a hard gate (exit 4).
func TestSavedGateExitCodes(t *testing.T) {
	dir := t.TempDir()
	clean := loadRef(t)
	clean.Provider, clean.Model = "openai", "m:1"
	for i := range clean.Results {
		clean.Results[i].ForbiddenProposed = nil
	}
	collapsed := clean
	collapsed.Scenarios = append([]evals.ScenarioSummary(nil), clean.Scenarios...)
	hit := ""
	for i, s := range collapsed.Scenarios {
		if s.Passes == 3 {
			collapsed.Scenarios[i].Passes, hit = 0, s.Name
			break
		}
	}
	if hit == "" {
		t.Fatal("the reference passes no scenario 3 of 3")
	}
	cases := []struct {
		name string
		cand evals.Report
		code int
		out  string
	}{
		{"supported", clean, 0, "**Verdict: supported**"},
		{"collapse", collapsed, evals.ExitGateFloor, "collapsed: " + hit},
		{"reference's own forbidden proposals", loadRef(t), evals.ExitGateHard, "| forbidden actions | hard | 6 |"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cand := saveReport(t, dir, strings.ReplaceAll(tc.name, " ", "_")+".json", tc.cand)
			var out bytes.Buffer
			code, err := run(flags{gate: refPath, gateConfig: gateFile, args: []string{cand}}, &out, &out)
			if err != nil || code != tc.code || !strings.Contains(out.String(), tc.out) {
				t.Fatalf("code %d err %v, want %d and %q:\n%s", code, err, tc.code, tc.out, out.String())
			}
			for _, w := range []string{"SE_diff", "| root-cause rate | floor |", "| pass rate | reported |"} {
				if !strings.Contains(out.String(), w) {
					t.Fatalf("output lacks %q", w)
				}
			}
		})
	}
	var out bytes.Buffer
	if _, err := run(flags{gate: refPath, gateConfig: gateFile, args: []string{filepath.Join(dir, "supported.json")}}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Baseline file: `evals/baseline/openai-m_1.json`") {
		t.Fatalf("no baseline path:\n%s", out.String())
	}
}

// TestGateRefusals: a 1-run reference, a too-short candidate and a gate
// file that raises a safety gate are refused before any model call (no API
// key is set, and the error is not about one).
func TestGateRefusals(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	dir := t.TempDir()
	one := loadRef(t)
	one.Runs = 1
	onePath := saveReport(t, dir, "one.json", one)
	b, err := os.ReadFile(gateFile)
	if err != nil {
		t.Fatal(err)
	}
	unsafe := filepath.Join(dir, "gate.yaml")
	if err := os.WriteFile(unsafe, bytes.Replace(b, []byte("hard:\n"), []byte("hard:\n  max_injection_failures: 1\n"), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		f    flags
		want string
	}{
		{"1-run reference, live", flags{scenarios: "../../scenarios", gate: onePath, gateConfig: gateFile, runs: 3}, "the reference has 1 run(s) per scenario"},
		{"1-run reference, saved", flags{gate: onePath, gateConfig: gateFile, args: []string{refPath}}, "the reference has 1 run(s) per scenario"},
		{"1-run candidate, live", flags{scenarios: "../../scenarios", gate: refPath, gateConfig: gateFile, runs: 1}, "-runs 3 or more"},
		{"safety gate above zero", flags{gate: refPath, gateConfig: unsafe, args: []string{refPath}}, "max_injection_failures is 1"},
		{"models table with an unsafe file", flags{modelsTable: "../../baseline", gateConfig: unsafe}, "max_injection_failures is 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			code, err := run(tc.f, &out, &out)
			if code != 1 || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("code %d err %v, want %q", code, err, tc.want)
			}
		})
	}
}

func TestModelsTableFlag(t *testing.T) {
	var out bytes.Buffer
	code, err := run(flags{modelsTable: "../../baseline", gateConfig: gateFile}, &out, &out)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v", code, err)
	}
	rows := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(rows) < 3 || !strings.HasPrefix(rows[2], "| claude-opus-5-5 | anthropic |") || !strings.Contains(rows[2], "| reference |") {
		t.Fatalf("reference is not first:\n%s", out.String())
	}
}

// TestProviderFlags: bad combinations fail before any work; the defaults
// leave the config exactly as the environment set it.
func TestProviderFlags(t *testing.T) {
	cfg := config.Load()
	cfg.ModelProvider = "anthropic"
	got, err := withModelFlags(cfg, flags{model: "claude-sonnet-5"})
	if err != nil || !reflect.DeepEqual(got, cfg) {
		t.Fatalf("Claude run changed the config (err %v)", err)
	}
	got, err = withModelFlags(cfg, flags{provider: "openai", flavor: "Ollama", model: "qwen3:32b"})
	if err != nil || got.ModelProvider != "openai" || got.ModelFlavor != "ollama" || got.Model != "qwen3:32b" {
		t.Fatalf("openai flags: %+v %v", got.ModelProvider, err)
	}
	var rep evals.Report
	recordModel(&rep, got)
	if rep.Provider != "openai" || rep.Flavor != "ollama" || rep.PromptHashKind != evals.PromptKindUntapped {
		t.Fatalf("recorded %q %q %q", rep.Provider, rep.Flavor, rep.PromptHashKind)
	}
	rep = evals.Report{PromptHashes: []string{"sha256:07e292a36b50ff08"}}
	recordModel(&rep, cfg)
	if rep.Provider != "anthropic" || rep.Flavor != "" || rep.PromptHashKind != evals.PromptKindTap {
		t.Fatalf("recorded %q %q %q", rep.Provider, rep.Flavor, rep.PromptHashKind)
	}
	// V11: a run tapped in process records the neutral kind.
	rep = evals.Report{PromptHashes: []string{evals.NeutralPromptHash("system", nil)}}
	recordModel(&rep, cfg)
	if rep.PromptHashKind != evals.PromptKindNeutral {
		t.Fatalf("neutral kind %q", rep.PromptHashKind)
	}
	got, _ = withModelFlags(cfg, flags{provider: "openai"})
	rep = evals.Report{}
	recordModel(&rep, got)
	if rep.Flavor != "openai" {
		t.Fatalf("default flavor %q", rep.Flavor)
	}

	for _, tc := range []struct {
		f    flags
		want string
	}{
		{flags{flavor: "ollama"}, "-flavor is for provider openai only"},
		{flags{provider: "gemini", flavor: "ollama"}, "-flavor is for provider openai only"},
		{flags{provider: "mystery"}, `-provider "mystery"`},
		{flags{judgeProvider: "openai"}, "the evals judge runs on anthropic, bedrock, vertex only"},
		{flags{judgeProvider: "mystery"}, `-judge-provider "mystery"`},
	} {
		if _, err := withModelFlags(cfg, tc.f); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err %v, want %q", tc.f, err, tc.want)
		}
	}
	// Through run: refused before the key check and before any model call.
	t.Setenv("ANTHROPIC_API_KEY", "")
	var out bytes.Buffer
	if code, err := run(flags{scenarios: "../../scenarios", flavor: "ollama", runs: 1}, &out, &out); code != 1 || err == nil || !strings.Contains(err.Error(), "-flavor") {
		t.Fatalf("code %d err %v", code, err)
	}
}

func TestParseProviderFlags(t *testing.T) {
	f, err := parseFlags([]string{"-provider", "openai", "-flavor", "ollama", "-judge-provider", "anthropic", "-gate", "ref.json", "cand.json", "-models-table", "d"}, new(bytes.Buffer))
	if err != nil {
		t.Fatal(err)
	}
	if f.provider != "openai" || f.flavor != "ollama" || f.judgeProvider != "anthropic" || f.gate != "ref.json" || f.modelsTable != "d" ||
		f.gateConfig != evals.DefaultGateFile || len(f.args) != 1 || f.args[0] != "cand.json" {
		t.Fatalf("%+v", f)
	}
}
