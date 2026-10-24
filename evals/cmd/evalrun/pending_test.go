package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/evals"
)

// pendingA and pendingB were held out until U1-10; it landed (#1012), so they
// run now and only pendingC is pending.
const pendingA, pendingB = "setup/compose-postgres-clean", "setup/compose-privileged-refused"

// pendingC is held out until container-resolve-op (W4c, KAN-494, lead ruling R10;
// re-keyed by the KAN-526 ruling).
const pendingC = "setup/db-unreachable-from-connect"

// pendingCount is how many scenarios carry pending_harness.
const pendingCount = 1

// -list prints the scenarios a run replays, without the pending_harness ones.
func TestRunListExcludesPendingHarness(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code, err := run(flags{scenarios: "../../scenarios", list: true}, &stdout, &stderr)
	if err != nil || code != 0 {
		t.Fatalf("code %d err %v", code, err)
	}
	names := strings.Fields(stdout.String())
	all, err := evals.LoadScenarios("../../scenarios")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != len(all)-pendingCount {
		t.Errorf("%d names listed, want %d", len(names), len(all)-pendingCount)
	}
	listed := map[string]bool{}
	for _, n := range names {
		listed[n] = true
		if n == pendingC {
			t.Errorf("%s is listed but pending", n)
		}
	}
	if !listed[pendingA] || !listed[pendingB] {
		t.Errorf("%s and %s are no longer pending but not listed", pendingA, pendingB)
	}
	// -check still validates every scenario, the held-out ones included.
	stdout.Reset()
	if code, err = run(flags{scenarios: "../../scenarios", check: true}, &stdout, &stderr); err != nil || code != 0 || !strings.Contains(stdout.String(), fmt.Sprintf("%d scenario(s) OK", len(all))) {
		t.Errorf("-check: code %d err %v out %q", code, err, stdout.String())
	}
}

func refusingAPI(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("ANTHROPIC_BASE_URL", srv.URL)
	t.Setenv("ANTHROPIC_API_KEY", "test-key-not-real")
}

func readReport(t *testing.T, path string) evals.Report {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r evals.Report
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// Without -only a pending scenario is skipped and reported apart; with -only
// naming one it runs, labelled, and a baseline comparison leaves it out.
func TestRunSkipsPendingHarnessUnlessNamed(t *testing.T) {
	refusingAPI(t)
	dir := t.TempDir()

	var stdout, stderr bytes.Buffer
	out := filepath.Join(dir, "all.json")
	code, err := run(flags{scenarios: "../../scenarios", runs: 1, parallel: 1, judge: evals.JudgeTerms, noTap: true, out: out, maxCalls: evals.DefaultJudgeMaxCalls}, &stdout, &stderr)
	if err != nil || code != exitInfra {
		t.Fatalf("code %d err %v\n%s", code, err, stderr.String())
	}
	rep := readReport(t, out)
	if strings.Join(rep.PendingHarness, ",") != pendingC || len(rep.PendingHarnessRan) != 0 {
		t.Errorf("pending lists: skipped %v ran %v", rep.PendingHarness, rep.PendingHarnessRan)
	}
	for _, s := range rep.Scenarios {
		if s.Name == pendingC {
			t.Errorf("%s is in the report rows (so in the rates)", s.Name)
		}
	}
	if strings.Contains(stderr.String(), pendingC) || !strings.Contains(stdout.String(), "Pending harness (skipped, in no rate):") {
		t.Errorf("skipped scenarios ran or are not reported apart\nstdout:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	out = filepath.Join(dir, "only.json")
	code, err = run(flags{scenarios: "../../scenarios", only: pendingC, runs: 1, parallel: 1, judge: evals.JudgeTerms, noTap: true, out: out, maxCalls: evals.DefaultJudgeMaxCalls}, &stdout, &stderr)
	if err != nil || code != exitInfra {
		t.Fatalf("-only: code %d err %v\n%s", code, err, stderr.String())
	}
	rep = readReport(t, out)
	if !strings.Contains(stderr.String(), pendingC+" #1") || len(rep.PendingHarness) != 0 || strings.Join(rep.PendingHarnessRan, ",") != pendingC {
		t.Errorf("-only did not run the marked scenario: skipped %v ran %v\n%s", rep.PendingHarness, rep.PendingHarnessRan, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Pending harness (ran by -only, not compared with the baseline):** "+pendingC) {
		t.Errorf("run not labelled pending harness:\n%s", stdout.String())
	}
}

// A pending scenario is in neither side of the comparison: skipped it would read
// as missing from the candidate, run it could not regress an older baseline.
func TestCompareLeavesPendingHarnessOut(t *testing.T) {
	good := evals.Summary{Runs: 1, Passes: 1, PassRate: 1, MeanScore: 100}
	bad := evals.Summary{Runs: 1, Passes: 0, PassRate: 0, MeanScore: 40}
	base := evals.Report{Label: "main", Runs: 1, Scenarios: []evals.ScenarioSummary{
		{Name: "schema-change", Summary: good},
		{Name: pendingA, Summary: good},
		{Name: pendingB, Summary: good},
	}}
	f := flags{failOnRegr: true, tolerance: evals.DefaultScoreTolerance}

	skipped := evals.Report{Label: "cand", Runs: 1, PendingHarness: []string{pendingA, pendingB},
		Scenarios: []evals.ScenarioSummary{{Name: "schema-change", Summary: good}}}
	var out bytes.Buffer
	if code, err := compare(base, skipped, f, &out); err != nil || code != 0 {
		t.Errorf("skipped pair regressed the baseline: code %d err %v\n%s", code, err, out.String())
	}

	ran := evals.Report{Label: "cand", Runs: 1, PendingHarnessRan: []string{pendingA},
		Scenarios: []evals.ScenarioSummary{{Name: "schema-change", Summary: good}, {Name: pendingA, Summary: bad}}}
	// An -only run of schema-change and one pending scenario.
	onlyF := f
	onlyF.only = "schema-change," + pendingA
	out.Reset()
	if code, err := compare(base, ran, onlyF, &out); err != nil || code != 0 {
		t.Errorf("pending scenario gated the baseline: code %d err %v\n%s", code, err, out.String())
	}

	// Control: the same failure on a scenario that is not pending does regress.
	plain := evals.Report{Label: "cand", Runs: 1,
		Scenarios: []evals.ScenarioSummary{{Name: "schema-change", Summary: bad}}}
	out.Reset()
	if code, _ := compare(base, plain, f, &out); code != exitRegressed {
		t.Errorf("control: code %d, want regression\n%s", code, out.String())
	}
}

// -check refuses a marker whose row has landed.
func TestRunCheckRefusesALandedMarker(t *testing.T) {
	root := t.TempDir()
	scDir := filepath.Join(root, "scenarios")
	if err := os.MkdirAll(scDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile("../../scenarios/setup-compose-postgres-clean.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// The checked-in file lost its marker when U1-10 landed; the fixture puts
	// it back, so -check meets a marker whose row has landed.
	name := []byte("\nname: setup/compose-postgres-clean\n")
	if !bytes.Contains(src, name) {
		t.Fatal("the scenario file has no name line to mark")
	}
	src = bytes.Replace(src, name, append(name, "pending_harness: U1-10\n"...), 1)
	if err := os.WriteFile(filepath.Join(scDir, "setup-compose-postgres-clean.yaml"), src, 0o644); err != nil {
		t.Fatal(err)
	}
	// The refusal reads the repository root above the working directory, so run
	// from a fixture root that has a go.mod and a wiring package with SetupStack.
	wiring := filepath.Join(root, "internal", "wiring")
	if err := os.MkdirAll(wiring, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wiring, "w.go"), []byte("package wiring\n\nfunc SetupStack() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	var stdout, stderr bytes.Buffer
	_, err = run(flags{scenarios: scDir, check: true}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "pending_harness U1-10") || !strings.Contains(err.Error(), "remove the marker") {
		t.Errorf("err = %v, want the landed-marker refusal", err)
	}
}
