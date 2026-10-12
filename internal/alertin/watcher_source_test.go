package alertin

import "testing"

// The watcher opens rooms inside the process; it must never be a source that
// the HTTP layer routes, or an outside caller could post as the watcher.
func TestWatcherSourceIsNotRouted(t *testing.T) {
	if Watcher != "watcher" {
		t.Fatalf("Watcher = %q, want %q", Watcher, "watcher")
	}
	for _, s := range Sources {
		if s == Watcher {
			t.Fatal("Watcher must not be in Sources (no HTTP route)")
		}
	}
}
