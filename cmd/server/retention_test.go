package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/store"
)

type fakeExpirer struct {
	mu      sync.Mutex
	cutoffs []time.Time
	res     store.PurgeResult
	err     error
	block   chan struct{} // when set, PurgeExpired waits for it or ctx
}

func (f *fakeExpirer) PurgeExpired(ctx context.Context, cutoff time.Time) (store.PurgeResult, error) {
	f.mu.Lock()
	f.cutoffs = append(f.cutoffs, cutoff)
	f.mu.Unlock()
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return store.PurgeResult{}, ctx.Err()
		}
	}
	return f.res, f.err
}

func (f *fakeExpirer) calls() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.cutoffs...)
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestRetentionOffDoesNothing(t *testing.T) {
	f := &fakeExpirer{}
	stop := startRetention(context.Background(), f, 0, time.Millisecond, time.Second, time.Now, slog.New(slog.DiscardHandler))
	time.Sleep(20 * time.Millisecond)
	stop()
	if n := len(f.calls()); n != 0 {
		t.Fatalf("retention off must never purge, got %d calls", n)
	}
}

func TestRetentionRunsAtStartThenOnTicker(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	f := &fakeExpirer{}
	stop := startRetention(context.Background(), f, 30, 5*time.Millisecond, time.Second, func() time.Time { return now }, slog.New(slog.DiscardHandler))
	waitUntil(t, func() bool { return len(f.calls()) >= 3 }) // start-up run + two ticks
	stop()
	want := now.AddDate(0, 0, -30)
	for _, c := range f.calls() {
		if !c.Equal(want) {
			t.Fatalf("cutoff %v, want %v (30 days before now)", c, want)
		}
	}
	// stop really stops: no calls after it returns.
	n := len(f.calls())
	time.Sleep(30 * time.Millisecond)
	if len(f.calls()) != n {
		t.Fatalf("purge ran after stop")
	}
}

func TestRetentionStopInterruptsRunInFlight(t *testing.T) {
	f := &fakeExpirer{block: make(chan struct{})}
	stop := startRetention(context.Background(), f, 7, time.Hour, time.Minute, time.Now, slog.New(slog.DiscardHandler))
	waitUntil(t, func() bool { return len(f.calls()) == 1 })
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not cancel the run in flight")
	}
}

func TestRetentionRunTimeoutBoundsOneRun(t *testing.T) {
	f := &fakeExpirer{block: make(chan struct{})}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	start := time.Now()
	runRetention(context.Background(), f, 7, 20*time.Millisecond, time.Now, log)
	if time.Since(start) > 2*time.Second {
		t.Fatal("run was not bounded by its timeout")
	}
	if !strings.Contains(buf.String(), "retention: purge failed") {
		t.Fatalf("a timeout must be logged as a failure: %s", buf.String())
	}
}

// TestRetentionLogsCountsOnly: the log line has counts and no room id or text.
func TestRetentionLogsCountsOnly(t *testing.T) {
	f := &fakeExpirer{res: store.PurgeResult{RoomIDs: []string{"r_SECRETID"}, Events: 12, Proposals: 2, Approvals: 3}}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	runRetention(context.Background(), f, 30, time.Second, time.Now, log)
	out := buf.String()
	if !strings.Contains(out, "rooms=1") || !strings.Contains(out, "events=12") || !strings.Contains(out, "approvals=3") {
		t.Fatalf("counts missing: %s", out)
	}
	if strings.Contains(out, "r_SECRETID") {
		t.Fatalf("room ids must not be logged: %s", out)
	}
}

func TestRetentionErrorIsLoggedNotFatal(t *testing.T) {
	f := &fakeExpirer{err: errors.New("db down")}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	runRetention(context.Background(), f, 30, time.Second, time.Now, log)
	if !strings.Contains(buf.String(), "retention: purge failed") || !strings.Contains(buf.String(), "db down") {
		t.Fatalf("error not logged: %s", buf.String())
	}
}
