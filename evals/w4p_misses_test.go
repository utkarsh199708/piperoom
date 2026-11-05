package evals

import (
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// W4p (KAN-499): the misses run 39994729845 reported in 21 failing scenarios.
// testdata/w4p_misses.json is W4-0's table for those scenarios (every miss of
// results[].misses, with how many runs hit it). The test replays each miss's
// tool and input against the scenario's recordings, the way the harness does
// at the start phase, and fails when no recording answers it.

type w4pMiss struct {
	Tool  string         `json:"tool"`
	Input map[string]any `json:"input"`
	Count int            `json:"count"`
}

type w4pFixture struct {
	RunID      int64 `json:"run_id"`
	HandedOver struct {
		To        string   `json:"to"`
		Scenarios []string `json:"scenarios"`
	} `json:"handed_over"`
	Scenarios map[string][]w4pMiss `json:"scenarios"`
}

func loadW4p(t *testing.T) w4pFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/w4p_misses.json")
	if err != nil {
		t.Fatal(err)
	}
	var f w4pFixture
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// w4pFile maps a scenario name to its file: the setup scenarios are named
// setup/<x> and live in setup-<x>.yaml.
func w4pFile(name string) string {
	return "scenarios/" + strings.ReplaceAll(name, "/", "-") + ".yaml"
}

func TestW4pMissesResolveToRecordings(t *testing.T) {
	f := loadW4p(t)
	if f.RunID != 39994729845 {
		t.Fatalf("fixture is for run %d, want 39994729845", f.RunID)
	}
	if len(f.Scenarios) != 21 {
		t.Fatalf("fixture has %d scenarios, want the 21 of W4p", len(f.Scenarios))
	}
	handed := map[string]bool{}
	for _, s := range f.HandedOver.Scenarios {
		handed[s] = true
	}
	var total, resolved, handedMisses int
	for name, misses := range f.Scenarios {
		for _, m := range misses {
			total += m.Count
		}
		if handed[name] {
			// W4c (KAN-494) records these two; W4p leaves them alone.
			for _, m := range misses {
				handedMisses += m.Count
			}
			continue
		}
		sc, err := LoadScenario(w4pFile(name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		r, err := newScenarioReplayer(sc)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, m := range misses {
			in, err := json.Marshal(m.Input)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if _, ok := r.lookup(m.Tool, in); !ok {
				t.Errorf("%s: no recording answers %s %s (%d run(s) missed it)", name, m.Tool, in, m.Count)
				continue
			}
			resolved += m.Count
		}
	}
	if total != 126 {
		t.Errorf("fixture lists %d misses, want 126", total)
	}
	if handedMisses != 15 || len(handed) != 2 {
		t.Errorf("handed over to W4c: %d misses in %d scenarios, want 15 in 2", handedMisses, len(handed))
	}
	if resolved != total-handedMisses {
		t.Errorf("%d of %d misses resolve (W4c's %d excluded)", resolved, total-handedMisses, handedMisses)
	}
}

// A recording that tells the agent to do a forbidden thing would make the
// wrong fix look right. The recordings that answer the misses of a W4p
// scenario name none of the scenario's forbidden actions.
func TestW4pAnswersNameNoForbiddenAction(t *testing.T) {
	f := loadW4p(t)
	handed := map[string]bool{}
	for _, s := range f.HandedOver.Scenarios {
		handed[s] = true
	}
	for name, misses := range f.Scenarios {
		if handed[name] {
			continue
		}
		sc, err := LoadScenario(w4pFile(name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		r, err := newScenarioReplayer(sc)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, m := range misses {
			in, _ := json.Marshal(m.Input)
			rr, ok := r.lookup(m.Tool, in)
			if !ok {
				continue // reported by TestW4pMissesResolveToRecordings
			}
			var text strings.Builder
			for _, o := range rr.obs {
				text.WriteString(o.summary + "\n" + o.err + "\n" + string(o.data) + "\n")
			}
			for _, a := range sc.Expect.ForbiddenActions {
				if strings.Contains(text.String(), a) {
					t.Errorf("%s: the answer to %s %s names the forbidden action %s", name, m.Tool, in, a)
				}
			}
		}
	}
}

// connect_worker_logs is recorded as the tool's own answer when no log reader
// is configured (the lead convention of the W4 rows): no log line is invented.
func TestW4pWorkerLogRecordingsAreNotConfigured(t *testing.T) {
	f := loadW4p(t)
	seen := 0
	for name := range f.Scenarios {
		if name == "setup/db-unreachable-from-connect" || name == "idle-wal-growth" {
			continue
		}
		sc, err := LoadScenario(w4pFile(name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for i, rec := range sc.Recordings {
			if rec.Tool != "connect_worker_logs" {
				continue
			}
			seen++
			if !strings.HasPrefix(rec.Summary, "Worker logs are not configured on this server (WORKER_LOGS_LOKI_URL and WORKER_LOGS_SELECTOR)") {
				t.Errorf("%s: recordings[%d]: connect_worker_logs is not the not-configured answer", name, i)
			}
		}
	}
	if seen == 0 {
		t.Fatal("no connect_worker_logs recording found in the W4p scenarios")
	}
}

var w4pReconRow = regexp.MustCompile(`(?m)^(\d\d:\d\d) \| (\d+) \| (\d+) \| (\d+) \| ([+-]\d+)$`)
var w4pMissing = regexp.MustCompile(`verdict: sink is missing (\d+) rows`)

// The reconcile tables W4p adds do their own arithmetic: the last column is
// sink minus source, and the verdict's missing count is what the buckets with
// a gap above the tool's 2% tolerance add up to.
func TestW4pReconcileRecordingsAddUp(t *testing.T) {
	checked := 0
	for _, c := range []struct {
		scenario string
		since    string
	}{
		{"blocked-slot", "2026-10-23T08:40:00Z"},
		{"tolerance-all-skips", "2026-10-23T09:00:00Z"},
		{"topic-retention-data-loss", "2026-10-11T01:00:00Z"},
	} {
		sc, err := LoadScenario(w4pFile(c.scenario))
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range sc.Recordings {
			if rec.Tool != "reconcile" || rec.Input["since"] != c.since {
				continue
			}
			checked++
			rows := w4pReconRow.FindAllStringSubmatch(rec.Summary, -1)
			if len(rows) < 2 || len(rows) > 12 {
				t.Fatalf("%s: %d bucket rows, want 2 to 12", c.scenario, len(rows))
			}
			var missing int
			for _, r := range rows {
				src, _ := strconv.Atoi(r[2])
				snk, _ := strconv.Atoi(r[4])
				diff, _ := strconv.Atoi(r[5])
				if snk-src != diff {
					t.Errorf("%s: bucket %s: sink-source is %d, the row says %d", c.scenario, r[1], snk-src, diff)
				}
				if gap := src - snk; gap >= 1 && float64(gap) > 0.02*float64(src) {
					missing += gap
				}
			}
			m := w4pMissing.FindStringSubmatch(rec.Summary)
			if m == nil {
				t.Errorf("%s: no 'sink is missing' verdict", c.scenario)
				continue
			}
			if want, _ := strconv.Atoi(m[1]); want != missing {
				t.Errorf("%s: verdict says %d missing rows, the buckets add up to %d", c.scenario, want, missing)
			}
		}
	}
	if checked != 3 {
		t.Errorf("checked %d reconcile recordings, want 3", checked)
	}
}
