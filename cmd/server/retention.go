package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/store"
)

const (
	// retentionInterval is how often the purge runs after the start-up run.
	retentionInterval = 6 * time.Hour
	// retentionRunTimeout bounds one purge run, so a stuck database cannot
	// hold the goroutine forever. Rooms not reached are picked up next run.
	retentionRunTimeout = 15 * time.Minute
)

// expirer is the part of room.Service the retention loop needs.
type expirer interface {
	PurgeExpired(ctx context.Context, cutoff time.Time) (store.PurgeResult, error)
}

// startRetention deletes closed rooms older than days once now and then every
// `every`, until ctx ends or the returned stop func is called (stop waits for a
// run in flight). days <= 0 keeps everything and starts nothing. The first run
// happens in the goroutine, so a large backlog never delays start-up. Logs hold
// counts only, never room content.
func startRetention(ctx context.Context, svc expirer, days int, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if days <= 0 {
		log.Info("retention: off, closed rooms are kept forever")
		return func() {}
	}
	log.Info("retention: on", "days", days, "every", every.String())
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runRetention(ctx, svc, days, timeout, now, log)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runRetention(ctx, svc, days, timeout, now, log)
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// runRetention is one purge run with its own deadline.
func runRetention(ctx context.Context, svc expirer, days int, timeout time.Duration, now func() time.Time, log *slog.Logger) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cutoff := now().UTC().AddDate(0, 0, -days)
	res, err := svc.PurgeExpired(rctx, cutoff)
	attrs := []any{
		"days", days,
		"rooms", len(res.RoomIDs),
		"events", res.Events,
		"proposals", res.Proposals,
		"approvals", res.Approvals,
		"steps", res.Steps,
		"suggestions", res.Suggestions,
		"handoffs", res.Handoffs,
		"slack_threads", res.SlackThreads,
	}
	if err != nil {
		if ctx.Err() != nil {
			log.Info("retention: stopped by shutdown", attrs...)
			return
		}
		log.Error("retention: purge failed", append(attrs, "err", err)...)
		return
	}
	if len(res.RoomIDs) == 0 {
		log.Debug("retention: nothing to purge", "days", days)
		return
	}
	log.Info("retention: purged closed rooms", attrs...)
}
