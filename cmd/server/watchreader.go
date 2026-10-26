package main

import (
	"context"
	"log/slog"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/store"
)

// staleWatchFactor is how many watcher intervals old a status row may be
// before the hypothesis guard ignores it: a watcher that stopped must not
// leave a firing signal that refuses hypotheses for ever (H1).
const staleWatchFactor = 3

// freshWatchReader is agent.Deps.Watch: the watcher's status rows without the
// stale ones. It is nil when the watcher is off or the store holds no status
// rows, and the guard then reads no row.
func freshWatchReader(st store.Store, w config.Watch, log *slog.Logger) clients.WatchStatusReader {
	if w.Mode == config.WatchOff || w.Interval <= 0 {
		return nil
	}
	fs, ok := st.(store.FleetStore)
	if !ok {
		return nil
	}
	return &freshWatch{src: fs, maxAge: staleWatchFactor * w.Interval, now: time.Now, log: log}
}

// freshWatch drops rows whose CheckedAt is older than maxAge. A read error
// is logged (no row content) and turned into no rows: the guard must never
// refuse because a read failed.
type freshWatch struct {
	src    clients.WatchStatusReader
	maxAge time.Duration
	now    func() time.Time
	log    *slog.Logger
}

// WatchStatuses implements clients.WatchStatusReader.
func (f *freshWatch) WatchStatuses(ctx context.Context, pipelineIDs []string) ([]clients.WatchStatus, error) {
	rows, err := f.src.WatchStatuses(ctx, pipelineIDs)
	if err != nil {
		if f.log != nil {
			f.log.Warn("server: watch status read failed; the hypothesis guard sees no row", "err", err)
		}
		return nil, nil
	}
	cut := f.now().Add(-f.maxAge)
	out := make([]clients.WatchStatus, 0, len(rows))
	for _, r := range rows {
		if r.CheckedAt.Before(cut) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}
