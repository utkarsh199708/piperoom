package airflow

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

// A Client built without New (zero logCeiling) must still stream the log,
// not read nothing and report it cut (nit from #593).
func TestZeroLogCeiling(t *testing.T) {
	const log = "line one\nline two\n"
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, log)
	})
	c.logCeiling = 0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tail, cut, err := c.TaskLogTailCut(ctx, "d", "r", "t", 1, 64)
	if err != nil || cut || tail != log {
		t.Fatalf("zero ceiling: tail %q, cut %v, err %v", tail, cut, err)
	}
	if got := (&Client{logCeiling: -1}).ceiling(); got != MaxLogStreamBytes {
		t.Fatalf("negative ceiling: got %d, want MaxLogStreamBytes", got)
	}
}

// The zero Client has no HTTP client either: every read is an error, never a panic.
func TestZeroClientNoPanic(t *testing.T) {
	var c Client
	ctx := context.Background()
	if _, err := c.DAGRuns(ctx, "d", 1); err == nil {
		t.Fatal("DAGRuns on a zero Client: want an error")
	}
	if _, _, err := c.TaskLogTailCut(ctx, "d", "r", "t", 1, 64); err == nil {
		t.Fatal("TaskLogTailCut on a zero Client: want an error")
	}
}
