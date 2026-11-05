package replay

import (
	"path/filepath"
	"testing"
	"time"
)

// MustLoad loads a cassette for a test and fails the test on any error,
// ErrCredential included: a cassette with a credential-shaped string fails
// every test that loads it (board U13a "Done when"). A stale cassette is
// logged, not failed.
func MustLoad(tb testing.TB, path string) *Cassette {
	tb.Helper()
	c, err := Load(path)
	if err != nil {
		tb.Fatalf("replay: %v", err)
		return nil
	}
	if c.Stale(time.Now()) {
		tb.Logf("replay: warning: cassette %s was recorded %s, over %d days ago; re-record it (go test -tags manual)",
			filepath.Base(path), c.RecordedAt.Format("2006-01-02"), int(StaleAfter.Hours()/24))
	}
	return c
}

// Replay loads the cassette at path and returns its replayer; the test
// fails at cleanup if any request was unmatched.
func Replay(tb testing.TB, path string, s Scrubber) *Replayer {
	tb.Helper()
	r := NewReplayer(MustLoad(tb, path), s)
	tb.Cleanup(func() {
		if u := r.Unmatched(); len(u) > 0 {
			tb.Errorf("replay: %s: unmatched requests: %v", filepath.Base(path), u)
		}
	})
	return r
}
