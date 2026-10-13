package main

import (
	"context"
	"log/slog"
	"time"

	"piperoom/internal/store"
)

// byteSizeBackfiller is the part of *store.PG the start-up backfill needs.
type byteSizeBackfiller interface {
	BackfillByteSize(ctx context.Context, batch int) (store.BackfillResult, error)
}

// startByteSizeBackfill counts byte_size for rooms from before migration
// 00012 in the background (docs/design/retention.md section 5.2), so a large
// room table never delays the start, then has the service re-read its
// counters. Until a room is counted its byte cap is not enforced. bf is nil
// for the in-memory store. The returned stop func cancels and waits.
func startByteSizeBackfill(ctx context.Context, bf byteSizeBackfiller, reload func(context.Context), log *slog.Logger) (stop func()) {
	if bf == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		start := time.Now()
		res, err := bf.BackfillByteSize(ctx, store.BackfillBatch)
		attrs := []any{"rooms", res.Rooms, "shadow_runs", res.ShadowRuns, "took", time.Since(start).Round(time.Millisecond).String()}
		switch {
		case err != nil && ctx.Err() != nil:
			log.Info("byte_size backfill: stopped by shutdown; the rest is counted on the next start", attrs...)
		case err != nil:
			log.Error("byte_size backfill failed; uncounted rooms keep no byte cap until the next start", append(attrs, "err", err)...)
		case res.Skipped:
			log.Info("byte_size backfill: another replica holds the lock")
		case res.Rooms > 0 || res.ShadowRuns > 0:
			log.Info("byte_size backfill done", attrs...)
			reload(ctx)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
