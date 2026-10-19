package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/evals"
)

// The API refuses every request (an empty credit balance): evalrun prints why
// on the run line, stops starting runs, marks them infra and exits 3, and the
// -baseline comparison never calls it a regression.
func TestRunExits3OnAPIRefusal(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`)
	}))
	defer srv.Close()
	t.Setenv("ANTHROPIC_BASE_URL", srv.URL)
	t.Setenv("ANTHROPIC_API_KEY", "test-key-not-real")

	dir := t.TempDir()
	base := saveReport(t, dir, "base.json", evals.Report{Label: "main", Runs: 3, Scenarios: []evals.ScenarioSummary{
		{Name: "schema-change", Summary: evals.Summary{Runs: 3, Passes: 3, PassRate: 1, MeanScore: 95}},
	}})
	out := filepath.Join(dir, "cand.json")

	var stdout, stderr bytes.Buffer
	code, err := run(flags{
		scenarios: "../../scenarios", only: "schema-change", runs: 3, parallel: 1,
		judge: evals.JudgeTerms, noTap: true, baseline: base, failOnRegr: true, out: out,
		maxCalls: evals.DefaultJudgeMaxCalls,
	}, &stdout, &stderr)
	if err != nil || code != exitInfra {
		t.Fatalf("code %d err %v\nstdout:\n%s\nstderr:\n%s", code, err, stdout.String(), stderr.String())
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("%d model requests; the first refusal must stop new runs", n)
	}
	log := stderr.String()
	if got := strings.Count(log, "INFRA (not scored)"); got != 3 || !strings.Contains(log, "credit balance") {
		t.Errorf("run lines (%d infra):\n%s", got, log)
	}
	table := stdout.String()
	if strings.Contains(table, "REGRESSED") || !strings.Contains(table, "infra (3, not scored)") || !strings.Contains(table, "- schema-change #1:") {
		t.Errorf("table:\n%s", table)
	}
	if !strings.Contains(table, "error: ") || !strings.Contains(table, "credit balance") {
		t.Errorf("Failed runs lack the error:\n%s", table)
	}

	// The written report compares cleanly later, too.
	cand, err := evals.LoadReport(out)
	if err != nil || cand.InfraStop == "" {
		t.Fatalf("report: %v %+v", err, cand)
	}
	var cmp bytes.Buffer
	code, err = compare(mustLoad(t, base), cand, flags{failOnRegr: true, tolerance: evals.DefaultScoreTolerance}, &cmp)
	if err != nil || code != 0 || strings.Contains(cmp.String(), "REGRESSED") {
		t.Errorf("compare code %d err %v:\n%s", code, err, cmp.String())
	}
}

func mustLoad(t *testing.T, path string) evals.Report {
	t.Helper()
	r, err := evals.LoadReport(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
