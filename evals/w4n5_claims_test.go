package evals

import (
	"slices"
	"testing"
)

// W4n-5 (R20): debezium_metrics is optional in tolerance-all-skips, so a run
// that cites only connect_worker_metrics must still back the root-cause claim.
// Claims are an OR (claimSupported), so f_records_skipped sits in the claim and
// the connect_worker_metrics recording has to establish it.
func TestW4n5ToleranceClaimBackedByWorkerMetrics(t *testing.T) {
	sc := w4nScenario(t, "tolerance-all-skips")
	if len(sc.Expect.Claims) != 1 || sc.Expect.Claims[0].About != ClaimRootCause || !slices.Contains(sc.Expect.Claims[0].Facts, "f_records_skipped") {
		t.Fatalf("claims = %+v, want one root_cause claim that lists f_records_skipped", sc.Expect.Claims)
	}
	f := w4n4Fact(t, sc, "f_records_skipped")
	var found bool
	for _, r := range sc.Recordings {
		if r.Tool == "connect_worker_metrics" && r.Error == "" && factIn(f, r.Summary) {
			found = true
		}
	}
	if !found {
		t.Error("no connect_worker_metrics recording establishes f_records_skipped")
	}
}
