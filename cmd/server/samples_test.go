package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/store/memstore"
)

// X3a TC1: the retention job removes samples older than 7 days, not newer ones.
func TestSamplesRetentionRuns(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	st := memstore.New()
	ctx := context.Background()
	m := clients.WatchMetricSlotWALBytes
	for _, at := range []time.Time{now.Add(-config.WatchSampleRetention - time.Minute), now.Add(-config.WatchSampleRetention + time.Minute), now} {
		if err := st.AppendSample(ctx, "orders", m, at, 1); err != nil {
			t.Fatal(err)
		}
	}
	// The loop logs from its goroutine while the test reads: lockedBuffer.
	var buf lockedBuffer
	stop := startSamplesRetention(ctx, st, time.Hour, time.Second, func() time.Time { return now }, slog.New(slog.NewTextHandler(&buf, nil)))
	waitUntil(t, func() bool { return strings.Contains(buf.String(), "rows=1") })
	stop()
	got, err := st.Samples(ctx, "orders", m, time.Time{})
	if err != nil || len(got) != 2 || !got[0].At.Equal(now.Add(-config.WatchSampleRetention+time.Minute)) {
		t.Fatalf("after retention: %+v, %v", got, err)
	}
}

type failingPurger struct{}

func (failingPurger) PurgeSamplesBefore(context.Context, time.Time) (int64, error) {
	return 0, errors.New("db down")
}

func TestSamplesRetentionNilAndErrors(t *testing.T) {
	stop := startSamplesRetention(context.Background(), nil, time.Millisecond, time.Second, time.Now, slog.Default())
	stop() // a nil purger starts nothing
	var buf bytes.Buffer
	runSamplesRetention(context.Background(), failingPurger{}, time.Second, time.Now, slog.New(slog.NewTextHandler(&buf, nil)))
	if !strings.Contains(buf.String(), "samples retention: purge failed") || !strings.Contains(buf.String(), "db down") {
		t.Fatalf("error not logged: %s", buf.String())
	}
}
