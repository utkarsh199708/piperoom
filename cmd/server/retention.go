package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/room"
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
	PurgeByPolicy(ctx context.Context, now time.Time, p room.RetentionPolicy) (store.PurgeResult, error)
}

// startRetention deletes closed rooms by the policy (PIPEROOM_RETENTION_DAYS
// and each pipeline's retention_days) once now and then every `every`, until
// ctx ends or the returned stop func is called (stop waits for a run in
// flight). A policy that keeps everything starts nothing. The first run
// happens in the goroutine, so a large backlog never delays start-up. Logs
// hold counts only, never room content.
func startRetention(ctx context.Context, svc expirer, policy room.RetentionPolicy, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if policy.Off() {
		log.Info("retention: off, closed rooms are kept forever")
		return func() {}
	}
	log.Info("retention: on", "days", policy.GlobalDays, "pipelines", policy.PipelineDays, "every", every.String())
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runRetention(ctx, svc, policy, timeout, now, log)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runRetention(ctx, svc, policy, timeout, now, log)
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// runRetention is one purge run with its own deadline.
func runRetention(ctx context.Context, svc expirer, policy room.RetentionPolicy, timeout time.Duration, now func() time.Time, log *slog.Logger) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := svc.PurgeByPolicy(rctx, now().UTC(), policy)
	attrs := []any{
		"days", policy.GlobalDays,
		"rooms", len(res.RoomIDs),
		"events", res.Events,
		"proposals", res.Proposals,
		"approvals", res.Approvals,
		"steps", res.Steps,
		"suggestions", res.Suggestions,
		"handoffs", res.Handoffs,
		"slack_threads", res.SlackThreads,
		"shadow_events", res.ShadowEvents,
		"shadow_runs", res.ShadowRuns,
	}
	if err != nil {
		switch {
		case ctx.Err() != nil:
			log.Info("retention: stopped by shutdown", attrs...)
		case len(res.Failed) > 0:
			// Some rooms failed (each already logged at Error with its id); the
			// rest were handled. That is a partial success, not a failed run.
			log.Warn("retention: run finished with failed rooms",
				append(attrs, "purged", len(res.RoomIDs), "failed", len(res.Failed), "err", err)...)
		case errors.Is(err, context.DeadlineExceeded):
			// The run's own deadline fired. No room is to blame; the rooms not
			// reached are picked up by the next run.
			log.Warn("retention: run hit its deadline; remaining rooms wait for the next run", attrs...)
		default:
			log.Error("retention: purge failed", append(attrs, "err", err)...)
		}
		return
	}
	if len(res.RoomIDs) == 0 {
		log.Debug("retention: nothing to purge", "days", policy.GlobalDays)
		return
	}
	log.Info("retention: purged closed rooms", attrs...)
}

const (
	// openLimitInterval is how often the open-room limit looks at open rooms.
	// The warning comes 24 h ahead, so a few minutes of lateness do not matter.
	openLimitInterval = 10 * time.Minute
	// openLimitRunTimeout bounds one pass.
	openLimitRunTimeout = 5 * time.Minute
)

// openLimiter is the part of room.Service the open-room limit loop needs.
type openLimiter interface {
	EnforceOpenLimit(ctx context.Context) (room.OpenLimitResult, error)
}

// startOpenLimit runs the open-room limit (PIPEROOM_MAX_OPEN_DAYS) once now and
// then every `every`, like startRetention. days <= 0 starts nothing.
func startOpenLimit(ctx context.Context, svc openLimiter, days int, every, timeout time.Duration, log *slog.Logger) (stop func()) {
	if days <= 0 {
		log.Info("open-room limit: off, open rooms stay open until closed")
		return func() {}
	}
	log.Info("open-room limit: on", "days", days, "every", every.String())
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			runOpenLimit(ctx, svc, timeout, log)
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

// runOpenLimit is one pass with its own deadline. Counts only in the log.
func runOpenLimit(ctx context.Context, svc openLimiter, timeout time.Duration, log *slog.Logger) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := svc.EnforceOpenLimit(rctx)
	attrs := []any{"warned", res.Warned, "closed", res.Closed, "failed", res.Failed}
	switch {
	case err != nil && ctx.Err() != nil:
		log.Info("open-room limit: stopped by shutdown", attrs...)
	case err != nil:
		log.Error("open-room limit: pass failed", append(attrs, "err", err)...)
	case res.Failed > 0:
		log.Warn("open-room limit: some rooms failed; they are tried again next pass", attrs...)
	case res.Warned > 0 || res.Closed > 0:
		log.Info("open-room limit: pass done", attrs...)
	}
}
