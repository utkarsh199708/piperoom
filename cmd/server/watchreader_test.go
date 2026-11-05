package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/store/memstore"
)

type fakeStatusReader struct {
	rows []clients.WatchStatus
	err  error
}

func (f fakeStatusReader) WatchStatuses(context.Context, []string) ([]clients.WatchStatus, error) {
	return f.rows, f.err
}

func TestFreshWatchDropsStaleRows(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	interval := time.Minute
	rows := []clients.WatchStatus{
		{PipelineID: "fresh", CheckedAt: now.Add(-2 * interval)},
		{PipelineID: "edge", CheckedAt: now.Add(-3 * interval)}, // exactly 3 x: still fresh
		{PipelineID: "stale", CheckedAt: now.Add(-3*interval - time.Second)},
	}
	f := &freshWatch{src: fakeStatusReader{rows: rows}, maxAge: staleWatchFactor * interval, now: func() time.Time { return now }}
	got, err := f.WatchStatuses(context.Background(), []string{"fresh", "edge", "stale"})
	if err != nil || len(got) != 2 || got[0].PipelineID != "fresh" || got[1].PipelineID != "edge" {
		t.Fatalf("got %+v, %v; want fresh and edge only", got, err)
	}
	// The window follows the interval: with a 10 s interval the same rows are stale.
	f.maxAge = staleWatchFactor * 10 * time.Second
	got, _ = f.WatchStatuses(context.Background(), nil)
	if len(got) != 0 {
		t.Fatalf("with a 10s interval got %+v, want no rows", got)
	}
}

func TestFreshWatchReadErrorGivesNoRows(t *testing.T) {
	f := &freshWatch{src: fakeStatusReader{err: errors.New("db down")}, maxAge: time.Minute, now: time.Now, log: slog.Default()}
	got, err := f.WatchStatuses(context.Background(), []string{"orders"})
	if err != nil || len(got) != 0 {
		t.Fatalf("got %+v, %v; want no rows and no error", got, err)
	}
}

func TestFreshWatchReaderWiring(t *testing.T) {
	st := memstore.New()
	if r := freshWatchReader(st, config.Watch{Mode: config.WatchOff, Interval: time.Minute}, nil); r != nil {
		t.Fatalf("watcher off: reader %v, want nil", r)
	}
	r := freshWatchReader(st, config.Watch{Mode: config.WatchRooms, Interval: 20 * time.Second}, nil)
	fw, ok := r.(*freshWatch)
	if !ok || fw.maxAge != time.Minute {
		t.Fatalf("reader = %#v, want a freshWatch with maxAge 1m (3 x 20s)", r)
	}
	// Through the real store: a fresh row passes, an old one does not.
	now := time.Now()
	for id, at := range map[string]time.Time{"fresh": now, "old": now.Add(-time.Hour)} {
		err := st.PutWatchStatus(context.Background(), clients.WatchStatus{PipelineID: id, CheckedAt: at})
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.WatchStatuses(context.Background(), []string{"fresh", "old"})
	if err != nil || len(got) != 1 || got[0].PipelineID != "fresh" {
		t.Fatalf("got %+v, %v; want only the fresh row", got, err)
	}
}

func TestAgentOptionsCarryTheGuardMode(t *testing.T) {
	for _, in := range []string{"", "off", "log", "enforce"} {
		cfg := config.Config{HypothesisGuard: in}
		mode, err := config.ParseHypothesisGuard(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if o := agentOptions(cfg, config.Limits{}, mode); o.HypothesisGuard != mode {
			t.Errorf("%q: live guard = %q, want %q", in, o.HypothesisGuard, mode)
		}
		if o := shadowAgentOptions(cfg, config.Shadow{}, config.Limits{}, mode); o.HypothesisGuard != mode {
			t.Errorf("%q: shadow guard = %q, want %q", in, o.HypothesisGuard, mode)
		}
	}
	if m, _ := config.ParseHypothesisGuard(config.Config{HypothesisGuard: "enforce"}); m != agent.GuardEnforce {
		t.Fatalf("enforce parsed as %q", m)
	}
	if _, err := config.ParseHypothesisGuard(config.Config{HypothesisGuard: "maybe"}); err == nil {
		t.Fatal("a bad value must be an error")
	}
}
