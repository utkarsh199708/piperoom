package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
)

func TestReplayReaderNilClient(t *testing.T) {
	for _, r := range []*ReplayReader{nil, NewReplayReader(nil)} {
		if _, err := r.ReadRecords(context.Background(), "t", 0, 0, 1); !errors.Is(err, errNoReplayClient) {
			t.Fatalf("ReadRecords = %v, want errNoReplayClient", err)
		}
		if _, err := r.DLQOffsets(context.Background(), "t"); !errors.Is(err, errNoReplayClient) {
			t.Fatalf("DLQOffsets = %v, want errNoReplayClient", err)
		}
	}
}

// TestKFakeReplayReader reads offsets and whole records through the reader
// Client, with no Writer built (write mode off).
func TestKFakeReplayReader(t *testing.T) {
	addrs, raw, _ := kfWriterCluster(t, 1, "dlq")
	produceAt(t, raw, "dlq", 0, time.Now(), time.Now(), time.Now())
	rc, err := New(addrs, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Close)
	r := NewReplayReader(rc)
	ctx := context.Background() // reads need no scope: the action's CheckScope scopes them
	offs, err := r.DLQOffsets(ctx, "dlq", "missing")
	if err != nil {
		t.Fatalf("DLQOffsets: %v", err)
	}
	if _, ok := offs["missing"]; ok || len(offs["dlq"]) != 1 || offs["dlq"][0].End != 3 {
		t.Fatalf("DLQOffsets = %+v", offs)
	}
	recs, err := r.ReadRecords(ctx, "dlq", 0, 1, 10)
	if err != nil || len(recs) != 2 || recs[0].Offset != 1 || string(recs[1].Value) != "v" {
		t.Fatalf("ReadRecords = %+v, %v", recs, err)
	}
	var _ clients.RecordReader = r
}
