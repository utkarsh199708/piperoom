package main

import (
	"strings"
	"testing"
)

// A bad PIPEROOM_HYPOTHESIS_GUARD stops the run before any client opens, and
// the error names the variable.
func TestInvalidHypothesisGuardStopsRun(t *testing.T) {
	t.Setenv("PIPEROOM_HYPOTHESIS_GUARD", "strict")
	err := run(runOptions{Pipeline: "orders", FakeTools: true})
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_HYPOTHESIS_GUARD") {
		t.Fatalf("err = %v, want one naming PIPEROOM_HYPOTHESIS_GUARD", err)
	}
}
