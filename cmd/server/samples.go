package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/store"
)

// loopSamples is the leader loop of the watch_samples retention.
const loopSamples = "samples-retention"

// samplesPurger is the part of the sample store (store.SampleStore) the
// retention loop needs.
type samplesPurger interface {
	PurgeSamplesBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

// startSamplesRetention deletes watch_samples rows older than
// config.WatchSampleRetention (7 days, fixed) once now and then every
// `every`, like startChangesRetention. A nil purger starts nothing. Logs hold
// counts only.
func startSamplesRetention(ctx context.Context, p samplesPurger, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if p == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			runSamplesRetention(ctx, p, timeout, now, log)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// runSamplesRetention is one purge with its own deadline.
func runSamplesRetention(ctx context.Context, p samplesPurger, timeout time.Duration, now func() time.Time, log *slog.Logger) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	n, err := p.PurgeSamplesBefore(rctx, now().UTC().Add(-config.WatchSampleRetention))
	switch {
	case err != nil && ctx.Err() != nil:
		log.Info("samples retention: stopped by shutdown")
	case errors.Is(err, context.DeadlineExceeded):
		log.Warn("samples retention: run hit its deadline; the next run continues")
	case err != nil:
		log.Error("samples retention: purge failed", "err", err)
	case n > 0:
		log.Info("samples retention: deleted old samples", "rows", n)
	default:
		log.Debug("samples retention: nothing to delete")
	}
}

// startElectedSamplesRetention is startSamplesRetention on the leader of
// "samples-retention" only.
func startElectedSamplesRetention(ctx context.Context, el store.Elector, p samplesPurger, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if p == nil {
		return func() {}
	}
	return runElected(ctx, el, loopSamples, func(lctx context.Context) {
		stopTerm := startSamplesRetention(lctx, p, every, timeout, now, log)
		<-lctx.Done()
		stopTerm()
	})
}
