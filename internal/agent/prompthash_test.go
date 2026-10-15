package agent

import "testing"

func TestPromptHashStable(t *testing.T) {
	a, b := PromptHash(), PromptHash()
	if a != b || len(a) != 64 {
		t.Fatalf("PromptHash = %q then %q, want the same 64 hex chars", a, b)
	}
}
