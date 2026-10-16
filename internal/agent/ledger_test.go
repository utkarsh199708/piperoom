package agent

import (
	"errors"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// TestStepLedgerCap: the ledger keeps the newest maxLedgerSteps steps plus the
// triage seeds (#447). A citation of an evicted step is refused with the
// re-read hint; recent steps and the seeds still back a hypothesis.
func TestStepLedgerCap(t *testing.T) {
	l := newStepLedger()
	const seed = "s_triage_read_1"
	l.seedTriage(&TriageResult{Steps: []TriageStep{{StepID: seed, Tool: "connect_status", Summary: "orders-sink FAILED"}}})
	var ids []string
	for range maxLedgerSteps + 50 {
		id := newStepID()
		ids = append(ids, id)
		l.addWithInput(id, "kafka_lag", `{"group":"orders-dw"}`, strings.Repeat("x", 100), false)
	}
	l.mu.Lock()
	nSteps, nOrder := len(l.steps), len(l.order)
	l.mu.Unlock()
	if nSteps != maxLedgerSteps+1 || nOrder != maxLedgerSteps+1 {
		t.Fatalf("ledger holds %d steps (%d ordered), want %d: the cap plus one seed", nSteps, nOrder, maxLedgerSteps+1)
	}

	tool := &updateHypothesisTool{steps: l}
	oldest := ids[0]
	err := tool.checkSteps(hypothesisInput{Text: "lag", BasedOnSteps: []string{oldest}, Confidence: "medium"})
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "is not a step of this run") {
		t.Fatalf("evicted step: err = %v, want the not-a-step refusal", err)
	}
	newest := ids[len(ids)-1]
	if err := tool.checkSteps(hypothesisInput{Text: "lag", BasedOnSteps: []string{newest, seed}, Confidence: "high"}); err != nil {
		t.Fatalf("newest step and triage seed: %v", err)
	}
	if got := tool.unbacked(hypothesisInput{Text: "orders-sink lag on orders-dw", BasedOnSteps: []string{newest, seed}}); len(got) != 0 {
		t.Errorf("unbacked = %v, want the seed summary and the step input to back every term", got)
	}
	if got := l.recent(1); len(got) != 1 || got[0] != newest {
		t.Errorf("recent(1) = %v, want [%s]", got, newest)
	}
}
