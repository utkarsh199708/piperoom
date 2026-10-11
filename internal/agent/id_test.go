package agent

import (
	"strings"
	"testing"
)

func TestNewULIDFormat(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newULID()
		if len(id) != 26 {
			t.Fatalf("ulid length = %d, want 26 (%q)", len(id), id)
		}
		for _, c := range id {
			if !strings.ContainsRune(crockford, c) {
				t.Fatalf("ulid contains invalid char %q in %q", c, id)
			}
		}
		if seen[id] {
			t.Fatalf("ulid collision: %q", id)
		}
		seen[id] = true
	}
}

func TestNewStepID(t *testing.T) {
	id := newStepID()
	if !strings.HasPrefix(id, "s_") {
		t.Fatalf("step id %q missing s_ prefix", id)
	}
	if len(id) != 28 { // "s_" + 26
		t.Fatalf("step id length = %d", len(id))
	}
}
