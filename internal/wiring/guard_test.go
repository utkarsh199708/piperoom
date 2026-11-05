package wiring

import (
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

func TestWithHypothesisGuard(t *testing.T) {
	base := agent.Options{Model: "m", MaxTurns: 7}
	for in, want := range map[string]string{"": agent.GuardLog, " enforce ": agent.GuardEnforce, "off": agent.GuardOff, "log": agent.GuardLog} {
		got, err := WithHypothesisGuard(config.Config{HypothesisGuard: in}, base)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got.HypothesisGuard != want || got.Model != "m" || got.MaxTurns != 7 {
			t.Fatalf("%q: options = %+v, want mode %q and the rest kept", in, got, want)
		}
	}
}

// An invalid value stops the start, naming the variable.
func TestWithHypothesisGuardInvalid(t *testing.T) {
	t.Setenv("PIPEROOM_HYPOTHESIS_GUARD", "strict")
	_, err := WithHypothesisGuard(config.Load(), agent.Options{})
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_HYPOTHESIS_GUARD") {
		t.Fatalf("err = %v, want one naming PIPEROOM_HYPOTHESIS_GUARD", err)
	}
}
