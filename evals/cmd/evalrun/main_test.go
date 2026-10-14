package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/evals"
)

func saveReport(t *testing.T, dir, name string, r evals.Report) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := r.WriteJSON(f); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunCheck(t *testing.T) {
	var out bytes.Buffer
	code, err := run(flags{scenarios: "../../scenarios", check: true}, &out, &out)
	if err != nil || code != 0 || !strings.Contains(out.String(), "scenario(s) OK") {
		t.Fatalf("code %d err %v out %q", code, err, out.String())
	}
}

func TestRunUnknownScenario(t *testing.T) {
	var out bytes.Buffer
	if _, err := run(flags{scenarios: "../../scenarios", only: "nope", check: true}, &out, &out); err == nil {
		t.Fatal("unknown -only accepted")
	}
}

func TestRunNeedsAPIKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	var out bytes.Buffer
	_, err := run(flags{scenarios: "../../scenarios"}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "ANTHROPIC_API_KEY") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunCompare(t *testing.T) {
	dir := t.TempDir()
	sum := func(pass int) evals.ScenarioSummary {
		return evals.ScenarioSummary{Name: "a", Summary: evals.Summary{Runs: 3, Passes: pass, PassRate: float64(pass) / 3, MeanScore: 90}}
	}
	base := saveReport(t, dir, "base.json", evals.Report{Label: "main", Runs: 3, Scenarios: []evals.ScenarioSummary{sum(3)}})
	worse := saveReport(t, dir, "worse.json", evals.Report{Label: "v2", Runs: 3, Scenarios: []evals.ScenarioSummary{sum(1)}})

	var out bytes.Buffer
	code, err := run(flags{compare: base + "," + worse, failOnRegr: true, tolerance: 5}, &out, &out)
	if err != nil || code != exitRegressed || !strings.Contains(out.String(), "REGRESSED") {
		t.Fatalf("code %d err %v out:\n%s", code, err, out.String())
	}
	code, err = run(flags{compare: base + "," + worse, tolerance: 5}, &out, &out)
	if err != nil || code != 0 {
		t.Errorf("without -fail-on-regression: code %d err %v", code, err)
	}
	if _, err := run(flags{compare: base}, &out, &out); err == nil {
		t.Error("single report accepted")
	}
	if _, err := run(flags{compare: base + "," + filepath.Join(dir, "none.json")}, &out, &out); err == nil {
		t.Error("missing report accepted")
	}
}

func TestParseFlagsValidateAlias(t *testing.T) {
	for _, arg := range []string{"-validate", "-check"} {
		f, err := parseFlags([]string{arg, "-scenarios", "x"}, &bytes.Buffer{})
		if err != nil || !f.check || f.scenarios != "x" {
			t.Errorf("%s: f = %+v, err %v", arg, f, err)
		}
	}
	f, err := parseFlags(nil, &bytes.Buffer{})
	if err != nil || f.check || f.scenarios != "evals/scenarios" || f.runs != 1 {
		t.Errorf("defaults: f = %+v, err %v", f, err)
	}
	if _, err := parseFlags([]string{"-nope"}, &bytes.Buffer{}); err == nil {
		t.Error("unknown flag accepted")
	}
}

// The held-out set validates on its own and is never part of the default
// set.
func TestRunCheckHeldoutSet(t *testing.T) {
	var out, errOut bytes.Buffer
	code, err := run(flags{scenarios: "../../scenarios", set: "heldout", check: true}, &out, &errOut)
	if err != nil || code != 0 || !strings.Contains(out.String(), "6 scenario(s) OK") {
		t.Fatalf("code %d err %v out %q", code, err, out.String())
	}
	const held = "ledger-producer-timeout-recovery"
	if _, err := run(flags{scenarios: "../../scenarios", set: "heldout", only: held, check: true}, &out, &errOut); err != nil {
		t.Errorf("-set heldout -only %s: %v", held, err)
	}
	if _, err := run(flags{scenarios: "../../scenarios", only: held, check: true}, &out, &errOut); err == nil {
		t.Errorf("the default set has the held-out scenario %s", held)
	}
	if _, err := run(flags{scenarios: "../../scenarios", set: "nope", check: true}, &out, &errOut); err == nil || !strings.Contains(err.Error(), "unknown -set") {
		t.Errorf("unknown set: %v", err)
	}
	f, err := parseFlags(nil, &bytes.Buffer{})
	if err != nil || f.set != "default" {
		t.Errorf("default -set: f = %+v, err %v", f, err)
	}
	if f, err := parseFlags([]string{"-set", "heldout"}, &bytes.Buffer{}); err != nil || f.set != "heldout" {
		t.Errorf("-set heldout: f = %+v, err %v", f, err)
	}
}

// -validate prints scenario warnings to stderr and still passes.
func TestRunCheckPrintsWarnings(t *testing.T) {
	dir := t.TempDir()
	b, err := os.ReadFile("../../scenarios/heldout/ledger-producer-timeout-recovery.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// A third phase nothing leads to is a warning, not an error.
	b = bytes.Replace(b, []byte("phases: [broken, recovered]"), []byte("phases: [broken, recovered, orphan]"), 1)
	if err := os.WriteFile(filepath.Join(dir, "ledger.yaml"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code, err := run(flags{scenarios: dir, check: true}, &out, &errOut)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v", code, err)
	}
	if !strings.Contains(errOut.String(), `warning: ledger-producer-timeout-recovery: phase "orphan" is unreachable`) {
		t.Errorf("stderr = %q", errOut.String())
	}
}
