package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/store"
)

type fakeBackfiller struct {
	res   store.BackfillResult
	err   error
	block bool // wait for ctx, as a long backfill stopped by shutdown
	calls atomic.Int32
}

func (f *fakeBackfiller) BackfillByteSize(ctx context.Context, batch int) (store.BackfillResult, error) {
	f.calls.Add(1)
	if batch != store.BackfillBatch {
		return store.BackfillResult{}, errors.New("unexpected batch")
	}
	if f.block {
		<-ctx.Done()
		return f.res, ctx.Err()
	}
	return f.res, f.err
}

// TestStartByteSizeBackfill: the service re-reads its counters only when the
// backfill counted something; a skipped, failed or stopped run leaves them;
// nil (memstore) is a no-op; stop waits for the goroutine.
func TestStartByteSizeBackfill(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name       string
		bf         *fakeBackfiller
		wantReload bool
	}{
		{"counted rooms", &fakeBackfiller{res: store.BackfillResult{Rooms: 3}}, true},
		{"counted shadow runs", &fakeBackfiller{res: store.BackfillResult{ShadowRuns: 1}}, true},
		{"nothing to count", &fakeBackfiller{}, false},
		{"another replica", &fakeBackfiller{res: store.BackfillResult{Skipped: true}}, false},
		{"failed", &fakeBackfiller{res: store.BackfillResult{Rooms: 2}, err: errors.New("db down")}, false},
		{"stopped by shutdown", &fakeBackfiller{block: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reloads atomic.Int32
			stop := startByteSizeBackfill(context.Background(), tc.bf, func(context.Context) { reloads.Add(1) }, log)
			if !tc.bf.block {
				deadline := time.Now().Add(5 * time.Second)
				for tc.bf.calls.Load() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
			}
			stop() // waits for the goroutine, so reload has run if it will
			if got := reloads.Load() == 1; got != tc.wantReload {
				t.Fatalf("reloads = %d, want reload %v", reloads.Load(), tc.wantReload)
			}
		})
	}
	startByteSizeBackfill(context.Background(), nil, func(context.Context) { t.Fatal("reload with no backfiller") }, log)()
}
