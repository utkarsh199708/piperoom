package similarincidents

import "testing"

// TestRootIsTheLastClassCauseNotATrailingLogLine: a log line after the root
// must not become the root fingerprint (textnorm marks such lines Log).
func TestRootIsTheLastClassCauseNotATrailingLogLine(t *testing.T) {
	root := "Caused by: org.a.b.BarException: root cause here"
	plain, _, ok1 := analyze("org.a.b.FooException: outer\n" + root)
	withLog, _, ok2 := analyze("org.a.b.FooException: outer\n" + root + "\n[2026-10-11 09:41:07] INFO Worker stopped")
	only, ok3 := fingerprint(root)
	if !ok1 || !ok2 || !ok3 {
		t.Fatal("no fingerprint")
	}
	if plain != withLog {
		t.Errorf("a trailing log line changed the fingerprints: %+v vs %+v", plain, withLog)
	}
	if plain.Root != only.Root {
		t.Errorf("root = %s, want the fingerprint of the root cause alone %s", plain.Root, only.Root)
	}
}
