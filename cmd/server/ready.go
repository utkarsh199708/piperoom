package main

import (
	"log/slog"
	"sync"
	"time"
)

// readyLogEvery limits readiness-failure logs: probes run every few seconds
// and a down database would otherwise flood the log.
const readyLogEvery = time.Minute

// readyLogger logs failed /readyz checks at Warn, at most once per interval.
type readyLogger struct {
	log   *slog.Logger
	every time.Duration
	now   func() time.Time

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func newReadyLogger(log *slog.Logger, every time.Duration, now func() time.Time) *readyLogger {
	return &readyLogger{log: log, every: every, now: now}
}

// notReady matches httpapi.Config.OnNotReady.
func (l *readyLogger) notReady(reason string, err error) {
	l.mu.Lock()
	t := l.now()
	if !l.last.IsZero() && t.Sub(l.last) < l.every {
		l.suppressed++
		l.mu.Unlock()
		return
	}
	suppressed := l.suppressed
	l.last, l.suppressed = t, 0
	l.mu.Unlock()
	l.log.Warn("readiness check failed", "check", "store", "reason", reason, "err", err, "suppressed", suppressed)
}
