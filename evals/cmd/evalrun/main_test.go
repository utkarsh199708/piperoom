package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/evals"
	"piperoom/internal/owners"
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

// TestRunCheckRejectsFactTypo: -validate catches an expected fact whose terms
// no recording of its tool contains, so an author typo fails before any run.
func TestRunCheckRejectsFactTypo(t *testing.T) {
	b, err := os.ReadFile("../../scenarios/schema-change.yaml")
	if err != nil {
		t.Fatal(err)
	}
	good := `match: [incompatible, "409"]`
	if !bytes.Contains(b, []byte(good)) {
		t.Fatalf("seed no longer has %s", good)
	}
	dir := t.TempDir()
	typo := bytes.Replace(b, []byte(good), []byte(`match: [incompatable, "409"]`), 1)
	if err := os.WriteFile(filepath.Join(dir, "schema-change.yaml"), typo, 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code, err := run(flags{scenarios: dir, check: true}, &out, &out)
	if err == nil || code != 1 || !strings.Contains(err.Error(), `(f_schema_incompatible): no successful connect_status recording contains all of ["incompatable" "409"]`) {
		t.Fatalf("code %d err %v", code, err)
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

func TestRunAgreement(t *testing.T) {
	var out bytes.Buffer
	sum := filepath.Join(t.TempDir(), "sum.json")
	code, err := run(flags{agreement: "../../testdata/shadow-agreement", out: sum}, &out, &out)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v out %q", code, err, out.String())
	}
	for _, want := range []string{"room_agree_both", "both agree:", "with human grade"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("table lacks %q:\n%s", want, out.String())
		}
	}
	if b, err := os.ReadFile(sum); err != nil || !strings.Contains(string(b), `"agreement_with_human_grade"`) {
		t.Errorf("summary: %v %q", err, b)
	}
}

func TestRunAgreementRefusesRunningRoom(t *testing.T) {
	dir := t.TempDir()
	doc := `{"run":{"room_id":"r","status":"running"},"room":{}}`
	if err := os.WriteFile(filepath.Join(dir, "r.json"), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code, err := run(flags{agreement: dir}, &out, &out)
	if err == nil || code == 0 || !strings.Contains(err.Error(), "refusing") || out.Len() != 0 {
		t.Fatalf("code %d err %v out %q", code, err, out.String())
	}
}

func TestRunAgreementUnknownJudge(t *testing.T) {
	var out bytes.Buffer
	if _, err := run(flags{agreement: "../../testdata/shadow-agreement", judge: "bogus"}, &out, &out); err == nil {
		t.Error("unknown judge accepted")
	}
}

func TestParseFlagsCompareWithTwoPaths(t *testing.T) {
	f, err := parseFlags([]string{"-compare", "a.json", "b.json", "-fail-on-regression", "-tolerance", "2"}, &bytes.Buffer{})
	if err != nil || f.compare != "a.json" || len(f.args) != 1 || f.args[0] != "b.json" || !f.failOnRegr || f.tolerance != 2 {
		t.Fatalf("f = %+v, err %v: flags after the second path must still count", f, err)
	}
	if _, err := parseFlags([]string{"-compare", "a.json", "b.json", "-nope"}, &bytes.Buffer{}); err == nil {
		t.Error("an unknown flag after a path was accepted")
	}
}

func pairedReport(label string, passes ...bool) evals.Report {
	r := evals.Report{Label: label, Runs: len(passes), Scenarios: []evals.ScenarioSummary{{Name: "a", Summary: evals.Summary{Runs: len(passes)}}}}
	for i, p := range passes {
		r.Results = append(r.Results, evals.RunResult{Scenario: "a", Run: i + 1, Pass: p})
	}
	return r
}

func TestRunComparePaired(t *testing.T) {
	dir := t.TempDir()
	base := saveReport(t, dir, "a.json", pairedReport("main", true, false, false, true))
	cand := saveReport(t, dir, "b.json", pairedReport("v2", true, true, false, false))

	for name, f := range map[string]flags{
		"space form": {compare: base, args: []string{cand}, tolerance: 5},
		"comma form": {compare: base + "," + cand, tolerance: 5},
	} {
		var out bytes.Buffer
		code, err := run(f, &out, &out)
		if err != nil || code != 0 {
			t.Fatalf("%s: code %d err %v", name, code, err)
		}
		for _, want := range []string{"Paired comparison", "4 pair(s), 0 unmatched", "pass→fail 1, fail→pass 1", "Sample too small to call: 4 pair(s), need at least 20"} {
			if !strings.Contains(out.String(), want) {
				t.Fatalf("%s: output lacks %q:\n%s", name, want, out.String())
			}
		}
	}
	for name, f := range map[string]flags{
		"three paths":             {compare: base, args: []string{cand, cand}},
		"comma form and a path":   {compare: base + "," + cand, args: []string{cand}},
		"a path without -compare": {scenarios: "../../scenarios", check: true, args: []string{cand}},
	} {
		if _, err := run(f, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRunRefusesABadPriceTableBeforeAnyModelCall(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "test-key-not-used")
	var out bytes.Buffer
	_, err := run(flags{scenarios: "../../scenarios", prices: filepath.Join(t.TempDir(), "none.json")}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_MODEL_PRICES") {
		t.Fatalf("err = %v, want the price table refused", err)
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

// learnedTree builds a scenarios directory with one reviewed learned scenario
// and one pending one, both exported from the room fixtures.
func learnedTree(t *testing.T) (root, reviewed, pending string) {
	t.Helper()
	root = t.TempDir()
	seed, err := os.ReadFile("../../scenarios/schema-change.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "schema-change.yaml"), seed, 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load("../../../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, evals.LearnedDir)
	export := func(fixture string) (string, string) {
		raw, err := os.ReadFile(filepath.Join("../../testdata/rooms", fixture))
		if err != nil {
			t.Fatal(err)
		}
		res, path, err := evals.ExportLearned(raw, evals.LearnedOptions{Owners: own, Dir: dir, Validate: true})
		if err != nil {
			t.Fatal(err)
		}
		return res.Name, path
	}
	reviewed, path := export("orders-confirmed.json")
	if err := os.Rename(path, filepath.Join(dir, reviewed+".yaml")); err != nil { // the person's review
		t.Fatal(err)
	}
	pending, _ = export("orders-hard.json")
	return root, reviewed, pending
}

// TestLoadSetReplaysLearnedNeverPending: the default set replays learned/, and
// learned/pending/ is only returned for validation.
func TestLoadSetReplaysLearnedNeverPending(t *testing.T) {
	root, reviewed, pending := learnedTree(t)
	run, pend, err := loadSet(flags{scenarios: root}, root)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, sc := range run {
		names[sc.Name] = true
	}
	if !names["schema-change"] || !names[reviewed] || names[pending] || len(run) != 2 {
		t.Errorf("replayed %v: want schema-change and %s, never %s", names, reviewed, pending)
	}
	if len(pend) != 1 || pend[0].Name != pending {
		t.Errorf("pending = %+v", pend)
	}
	// The held-out set has no learned scenarios.
	if _, _, err := loadSet(flags{set: setHeldout}, filepath.Join(root, setHeldout)); err == nil {
		t.Error("an absent held-out directory must still be an error")
	}
}

// TestRunCheckValidatesLearnedAndPending: -check counts both and rejects a
// malformed pending file by name.
func TestRunCheckValidatesLearnedAndPending(t *testing.T) {
	root, _, _ := learnedTree(t)
	var out bytes.Buffer
	code, err := run(flags{scenarios: root, check: true}, &out, &out)
	if err != nil || code != 0 || !strings.Contains(out.String(), "3 scenario(s) OK") {
		t.Fatalf("code %d err %v out %q", code, err, out.String())
	}
	bad := filepath.Join(root, evals.LearnedDir, evals.PendingDir, "broken.yaml")
	if err := os.WriteFile(bad, []byte("name: broken\nunknown_field: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	code, err = run(flags{scenarios: root, check: true}, &out, &out)
	if err == nil || code != 1 || !strings.Contains(err.Error(), "broken.yaml") {
		t.Fatalf("code %d err %v", code, err)
	}
}
