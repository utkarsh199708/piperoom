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

const pendingA, pendingB = "setup/compose-postgres-clean", "setup/compose-privileged-refused"

// pendingC was held out until W4c-resolver (W4c, KAN-494, lead ruling R10);
// KAN-526 landed nettools.ConnectResolveCheck and removed its marker, so it
// runs and is scored like any other scenario.
const pendingC = "setup/db-unreachable-from-connect"

// pendingCount is how many scenarios carry pending_harness.
const pendingCount = 2

// -list prints the scenarios a run replays, without the pending_harness pair.
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
	listedC := false
	for _, n := range names {
		if n == pendingA || n == pendingB {
			t.Errorf("%s is listed but pending", n)
		}
		listedC = listedC || n == pendingC
	}
	if !listedC {
		t.Errorf("%s is not listed, but its marker is gone (KAN-526)", pendingC)
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

// Without -only the pair is skipped and reported apart; with -only naming one it
// runs, labelled, and a baseline comparison leaves it out.
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
	if strings.Join(rep.PendingHarness, ",") != pendingA+","+pendingB || len(rep.PendingHarnessRan) != 0 {
		t.Errorf("pending lists: skipped %v ran %v", rep.PendingHarness, rep.PendingHarnessRan)
	}
	for _, s := range rep.Scenarios {
		if s.Name == pendingA || s.Name == pendingB {
			t.Errorf("%s is in the report rows (so in the rates)", s.Name)
		}
	}
	if strings.Contains(stderr.String(), pendingA) || !strings.Contains(stdout.String(), "Pending harness (skipped, in no rate):") {
		t.Errorf("skipped scenarios ran or are not reported apart\nstdout:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	out = filepath.Join(dir, "only.json")
	code, err = run(flags{scenarios: "../../scenarios", only: pendingA, runs: 1, parallel: 1, judge: evals.JudgeTerms, noTap: true, out: out, maxCalls: evals.DefaultJudgeMaxCalls}, &stdout, &stderr)
	if err != nil || code != exitInfra {
		t.Fatalf("-only: code %d err %v\n%s", code, err, stderr.String())
	}
	rep = readReport(t, out)
	if !strings.Contains(stderr.String(), pendingA+" #1") || len(rep.PendingHarness) != 0 || strings.Join(rep.PendingHarnessRan, ",") != pendingA {
		t.Errorf("-only did not run the marked scenario: skipped %v ran %v\n%s", rep.PendingHarness, rep.PendingHarnessRan, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Pending harness (ran by -only, not compared with the baseline):** "+pendingA) {
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
