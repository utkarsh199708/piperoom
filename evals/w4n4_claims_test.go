package evals

import (
	"fmt"
	"slices"
	"testing"
)

// W4n-4 (KAN-497; lead audit after R17) keeps consumer-skipped-range's
// root-cause claim backable from both sides of the replay. R17 added the
// replayed phase, where the traces reach the warehouse and no longer say
// "stopped at warehouse", so a recovery re-check cited by the final
// hypothesis lost the pre-replay facts. Claims are an OR (claimSupported: one
// cited step that established any listed fact backs the claim).

func w4n4Fact(t *testing.T, sc Scenario, id string) Fact {
	t.Helper()
	for _, f := range sc.Expect.ExpectedFacts {
		if f.ID == id {
			return f
		}
	}
	t.Fatalf("no expected fact %q", id)
	return Fact{}
}

// TestW4n4SkippedRangeFactsInBothPhases pins the facts against the recordings
// themselves: every skipped key's trace holds f_trace_behind_commit before and
// after the replay, f_trace_committed holds only before it, and the reads that
// found nothing establish none of the claim's facts.
func TestW4n4SkippedRangeFactsInBothPhases(t *testing.T) {
	sc := w4nScenario(t, "consumer-skipped-range")
	wantFacts := []string{"f_in_topic", "f_trace_committed", "f_trace_behind_commit"}
	if len(sc.Expect.Claims) != 1 || sc.Expect.Claims[0].About != ClaimRootCause || !slices.Equal(sc.Expect.Claims[0].Facts, wantFacts) {
		t.Fatalf("claims = %+v, want one root_cause claim over %v", sc.Expect.Claims, wantFacts)
	}
	inTopic := w4n4Fact(t, sc, "f_in_topic")
	if !slices.Equal(inTopic.Match, []string{"found 1 record"}) {
		t.Errorf("f_in_topic match = %q, want the count only (no key-specific offset)", inTopic.Match)
	}
	committed := w4n4Fact(t, sc, "f_trace_committed")
	behind := w4n4Fact(t, sc, "f_trace_behind_commit")

	pre := map[int]bool{}
	post := map[int]bool{}
	finds := map[int]bool{}
	for _, r := range sc.Recordings {
		key, _ := r.Input["key"].(string)
		var id int
		if _, err := fmt.Sscanf(key, "%d", &id); err != nil {
			id = 0
		}
		skipped := id >= 1301 && id <= 1340
		switch r.Tool {
		case "trace_record":
			if !skipped {
				continue
			}
			if r.Phase == "replayed" {
				post[id] = true
				if !factIn(behind, r.Summary) {
					t.Errorf("replayed trace of %d lacks f_trace_behind_commit:\n%s", id, r.Summary)
				}
				if factIn(committed, r.Summary) {
					t.Errorf("replayed trace of %d still matches f_trace_committed (the reason the third fact exists):\n%s", id, r.Summary)
				}
			} else {
				pre[id] = true
				if !factIn(behind, r.Summary) || !factIn(committed, r.Summary) {
					t.Errorf("skipped-phase trace of %d must match both facts:\n%s", id, r.Summary)
				}
			}
		case "kafka_find_by_key":
			if !skipped {
				continue
			}
			finds[id] = true
			if !factIn(inTopic, r.Summary) {
				t.Errorf("find_by_key %d lacks f_in_topic:\n%s", id, r.Summary)
			}
		}
	}
	for id := 1301; id <= 1340; id++ {
		if !pre[id] || !post[id] || !finds[id] {
			t.Errorf("key %d: skipped trace %v, replayed trace %v, find_by_key %v, want all recorded", id, pre[id], post[id], finds[id])
		}
	}

	// Reads that saw no skipped record establish none of the facts: the
	// recorded misses (999999 not found, not_seen trace) and a count of 0 or 2.
	for _, s := range []string{
		"not found: no record with key \"999999\" since 2026-10-22T06:40:00Z",
		"found 0 record(s) with key \"1301\"",
		"found 2 record(s) with key \"1301\"\n  partition 2, offset 433, 2026-10-22T08:51:10Z",
		"no record with key \"1301\" found since 2026-10-22T06:40:00Z in the scanned window",
	} {
		if factIn(inTopic, s) {
			t.Errorf("f_in_topic holds for %q", s)
		}
	}
	for _, r := range sc.Recordings {
		if r.Input != nil {
			continue
		}
		switch r.Tool {
		case "kafka_find_by_key":
			if factIn(inTopic, r.Summary) {
				t.Errorf("recorded miss establishes f_in_topic:\n%s", r.Summary)
			}
		case "trace_record":
			if factIn(behind, r.Summary) || factIn(committed, r.Summary) {
				t.Errorf("recorded not_seen trace establishes a trace fact:\n%s", r.Summary)
			}
		}
	}
	// A trace that stopped before the group's commit does not show a skip.
	notBehind := "postgres public.orders: seen updated_at=2026-10-22T08:51:10.233Z\n" +
		"kafka pg.public.orders: seen offset=433 partition=2\n" +
		"consumer order-sync: seen committed=433\n" +
		"warehouse analytics.orders: seen\n"
	if factIn(behind, notBehind) {
		t.Error("f_trace_behind_commit holds for a trace with committed=433")
	}
}

// TestW4n4SkippedRangeClaimBackedAfterReplay (end to end): the final
// hypothesis cites only a re-check after the replay and the root-cause claim
// is supported; a citation of reads that saw no skipped record is not.
func TestW4n4SkippedRangeClaimBackedAfterReplay(t *testing.T) {
	sc := w4nScenario(t, "consumer-skipped-range")
	var c recoveryCase
	for _, rc := range recoveryCases(t) {
		if rc.file == "consumer-skipped-range" {
			c = rc
		}
	}
	trace := func(key string) step {
		return step{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": key}}
	}
	find := func(key string) step {
		return step{tool: "kafka_find_by_key", input: map[string]any{"topic": "pg.public.orders", "key": key, "since": "2026-10-22T06:40:00Z"}}
	}
	good := hypotheses[c.file].good

	// run executes the standard reads, the fix and its scripted result, then
	// the given re-checks, and ends on a hypothesis citing only recheck[cite].
	run := func(replay bool, recheck []step, cite int) RunResult {
		script := slices.Clone(c.reads)
		script = append(script, step{hypothesis: good})
		if replay {
			script = append(script, step{proposeFix: c.fix})
			for range sc.Inputs {
				script = append(script, step{idle: true})
			}
		}
		base := len(script)
		script = append(script, recheck...)
		script = append(script, step{hypothesis: good, cite: []string{fmt.Sprintf("s_%d", base+cite)}}, step{idle: true})
		return runRecovery(t, sc, script)
	}
	claim := "root_cause: f_in_topic, f_trace_committed, f_trace_behind_commit"

	t.Run("post-replay trace backs the claim", func(t *testing.T) {
		r := run(true, []step{trace("1305")}, 0)
		if !slices.Contains(r.PhasesVisited, "replayed") {
			t.Fatalf("phases visited = %v, the replay did not happen", r.PhasesVisited)
		}
		if !slices.Equal(r.SupportedClaims, []string{claim}) || len(r.UnsupportedClaims) != 0 {
			t.Errorf("supported %v unsupported %v, want the claim supported", r.SupportedClaims, r.UnsupportedClaims)
		}
	})
	t.Run("pre-replay trace still backs the claim", func(t *testing.T) {
		r := run(false, []step{trace("1305")}, 0)
		if slices.Contains(r.PhasesVisited, "replayed") {
			t.Fatalf("phases visited = %v, want no replay", r.PhasesVisited)
		}
		if !slices.Equal(r.SupportedClaims, []string{claim}) {
			t.Errorf("supported %v unsupported %v", r.SupportedClaims, r.UnsupportedClaims)
		}
	})
	t.Run("post-replay find_by_key of any skipped key backs the claim", func(t *testing.T) {
		r := run(true, []step{find("1320")}, 0)
		if !slices.Equal(r.SupportedClaims, []string{claim}) {
			t.Errorf("supported %v unsupported %v", r.SupportedClaims, r.UnsupportedClaims)
		}
	})
	for _, tt := range []struct {
		name    string
		recheck []step
	}{
		{"post-replay trace that saw no record", []step{trace("999999")}},
		{"post-replay find_by_key that found nothing", []step{find("999999")}},
		{"post-replay warehouse_row", []step{{tool: "warehouse_row", input: map[string]any{"pipeline_id": "orders", "table": "analytics.orders", "pk_column": "id", "key": "1301"}}}},
		{"post-replay consumer lag", []step{{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}}}},
	} {
		t.Run(tt.name+" does not", func(t *testing.T) {
			r := run(true, tt.recheck, 0)
			if len(r.SupportedClaims) != 0 || !slices.Equal(r.UnsupportedClaims, []string{claim}) {
				t.Errorf("supported %v unsupported %v, want the claim unsupported", r.SupportedClaims, r.UnsupportedClaims)
			}
		})
	}
}
