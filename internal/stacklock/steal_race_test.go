package stacklock_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/stacklock"
)

// A fresh lock (younger than maxAge) must never be removed by Steal.
func TestStealNeverRemovesFreshLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stack.lock")
	var freshDeleted, rounds atomic.Int32
	for r := 0; r < 300; r++ {
		old, _ := json.Marshal(stacklock.Record{Owner: "stale", At: time.Now().Add(-2 * time.Hour)})
		_ = os.Remove(path)
		if err := os.WriteFile(path, old, 0o644); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for g := 0; g < 6; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				l := stacklock.New(path)
				owner := fmt.Sprintf("o%d-%d", r, g)
				_, _ = l.Steal(time.Hour)
				if rec, err := l.Acquire(owner); err == nil && rec.Owner == owner {
					time.Sleep(2 * time.Millisecond)
					cur, _ := l.Status()
					if cur == nil || cur.Owner != owner {
						freshDeleted.Add(1)
					}
					_ = l.Release(owner)
				}
			}(g)
		}
		wg.Wait()
		rounds.Add(1)
	}
	t.Logf("rounds=%d fresh locks lost by their holder=%d", rounds.Load(), freshDeleted.Load())
	if freshDeleted.Load() > 0 {
		t.Errorf("Steal removed a fresh lock (TOCTOU between Status and Remove)")
	}
}
