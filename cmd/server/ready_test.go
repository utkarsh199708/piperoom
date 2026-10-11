package main

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestReadyLoggerRateLimit(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	l := newReadyLogger(log, time.Minute, func() time.Time { return now })
	boom := errors.New("connection refused")

	l.notReady("unreachable", boom)
	now = now.Add(10 * time.Second)
	l.notReady("unreachable", boom)
	if n := strings.Count(buf.String(), "readiness check failed"); n != 1 {
		t.Fatalf("two failures within a minute logged %d times:\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("not logged at Warn:\n%s", buf.String())
	}

	now = now.Add(time.Minute)
	l.notReady("timeout", boom)
	out := buf.String()
	if n := strings.Count(out, "readiness check failed"); n != 2 {
		t.Fatalf("failure after the interval logged %d times in total:\n%s", n, out)
	}
	if !strings.Contains(out, "suppressed=1") || !strings.Contains(out, "reason=timeout") {
		t.Fatalf("second line misses suppressed count or reason:\n%s", out)
	}
}
