package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/auth"
)

// startAuthPrune deletes dead login rows once now and then every `every`,
// until ctx ends or the returned stop func is called (stop waits for a run in
// flight):
//
//   - sessions revoked, or past expires_at, by more than auth.SessionGrace;
//   - login_states past expires_at.
//
// It is separate from startRetention on purpose: that one does nothing when
// PIPEROOM_RETENTION_DAYS is 0 or empty ("keep closed rooms forever"), but
// dead sessions and login states are not room history and must be pruned in
// every setup. A nil pruner is skipped; with both nil nothing starts. Logs hold
// counts only.
func startAuthPrune(ctx context.Context, sessions auth.SessionPruner, loginStates auth.LoginStatePruner, every, timeout time.Duration, now func() time.Time, log *slog.Logger) (stop func()) {
	if sessions == nil && loginStates == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runAuthPrune(ctx, sessions, loginStates, timeout, now, log)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runAuthPrune(ctx, sessions, loginStates, timeout, now, log)
			}
		}
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// runAuthPrune is one prune run with its own deadline. A failure of one table
// is logged and does not stop the other.
func runAuthPrune(ctx context.Context, sessions auth.SessionPruner, loginStates auth.LoginStatePruner, timeout time.Duration, now func() time.Time, log *slog.Logger) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	at := now().UTC()
	if sessions != nil {
		n, err := sessions.PruneSessions(rctx, at, auth.SessionGrace)
		logAuthPrune(ctx, log, "sessions", n, err)
	}
	if loginStates != nil {
		n, err := loginStates.PruneLoginStates(rctx, at)
		logAuthPrune(ctx, log, "login_states", n, err)
	}
}

func logAuthPrune(ctx context.Context, log *slog.Logger, table string, n int64, err error) {
	switch {
	case err != nil && ctx.Err() != nil:
		log.Info("auth prune: stopped by shutdown", "table", table)
	case errors.Is(err, context.DeadlineExceeded):
		log.Warn("auth prune: run hit its deadline; the next run continues", "table", table)
	case err != nil:
		log.Error("auth prune: failed", "table", table, "err", err)
	case n > 0:
		log.Info("auth prune: deleted dead rows", "table", table, "rows", n)
	default:
		log.Debug("auth prune: nothing to delete", "table", table)
	}
}

// authPruners returns the prune side of the two auth stores, or nil for a
// store that cannot prune (a test fake).
func authPruners(sessions auth.Store, loginStates auth.LoginStateStore) (auth.SessionPruner, auth.LoginStatePruner) {
	sp, _ := sessions.(auth.SessionPruner)
	lp, _ := loginStates.(auth.LoginStatePruner)
	return sp, lp
}
