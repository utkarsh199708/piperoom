package storetest

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/store"
)

// RunSamples checks the watch sample history (X3a, clients.SampleWriter and
// SampleReader, store.SampleStore): samples read back oldest first with
// at >= since; a repeated (pipeline, metric, at) is not a second row; series
// are kept apart by pipeline and metric; at most clients.MaxSamplesPerSeries,
// the newest; bad samples are refused; and the retention purge removes what
// is older than config.WatchSampleRetention and nothing newer. pipeline is a
// unique pipeline id for this run; s holds no other sample older than 7 days.
func RunSamples(t *testing.T, s store.SampleStore, pipeline string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	m := clients.WatchMetricSlotWALBytes

	// Written out of order; read back in order.
	for _, h := range []int{2, 0, 1} {
		if err := s.AppendSample(ctx, pipeline, m, now.Add(time.Duration(h)*time.Hour), float64(h)*1e9); err != nil {
			t.Fatalf("append %dh: %v", h, err)
		}
	}
	// The same at again is no new row and no error.
	if err := s.AppendSample(ctx, pipeline, m, now, 42); err != nil {
		t.Fatalf("append duplicate: %v", err)
	}
	if err := s.AppendSample(ctx, pipeline, clients.WatchMetricGroupLag, now, 7); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendSample(ctx, pipeline+"x", m, now, 9); err != nil {
		t.Fatal(err)
	}
	got, err := s.Samples(ctx, pipeline, m, now)
	if err != nil {
		t.Fatal(err)
	}
	want := []clients.SamplePoint{{At: now, Value: 0}, {At: now.Add(time.Hour), Value: 1e9}, {At: now.Add(2 * time.Hour), Value: 2e9}}
	if !samePoints(got, want) {
		t.Fatalf("samples = %+v, want %+v", got, want)
	}
	// since is inclusive.
	if got, _ := s.Samples(ctx, pipeline, m, now.Add(time.Hour)); len(got) != 2 || !got[0].At.Equal(now.Add(time.Hour)) {
		t.Fatalf("since 1h = %+v", got)
	}
	// None is an empty list, not an error.
	if got, err := s.Samples(ctx, pipeline+"none", m, now); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty series = %#v, %v", got, err)
	}

	// Refused: unknown metric (both sides), empty pipeline, zero time, NaN, Inf.
	if _, err := s.Samples(ctx, pipeline, "cpu", now); !errors.Is(err, store.ErrUnknownMetric) {
		t.Fatalf("unknown metric read: %v", err)
	}
	for name, bad := range map[string]func() error{
		"metric":   func() error { return s.AppendSample(ctx, pipeline, "cpu", now, 1) },
		"pipeline": func() error { return s.AppendSample(ctx, "", m, now, 1) },
		"at":       func() error { return s.AppendSample(ctx, pipeline, m, time.Time{}, 1) },
		"nan":      func() error { return s.AppendSample(ctx, pipeline, m, now.Add(5*time.Hour), math.NaN()) },
		"inf":      func() error { return s.AppendSample(ctx, pipeline, m, now.Add(5*time.Hour), math.Inf(1)) },
	} {
		if err := bad(); err == nil {
			t.Errorf("bad %s: no error", name)
		}
	}

	// Cap: the newest MaxSamplesPerSeries, oldest first.
	capPipe := pipeline + "cap"
	start := now.Add(-config.WatchSampleRetention)
	n := clients.MaxSamplesPerSeries + 5
	for i := range n {
		if err := s.AppendSample(ctx, capPipe, clients.WatchMetricDLQRecords, start.Add(time.Duration(i)*config.WatchSampleInterval), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err = s.Samples(ctx, capPipe, clients.WatchMetricDLQRecords, time.Time{})
	if err != nil || len(got) != clients.MaxSamplesPerSeries || got[0].Value != 5 || got[len(got)-1].Value != float64(n-1) {
		t.Fatalf("capped series: %d points, first %+v, err %v", len(got), first(got), err)
	}

	// Retention: older than 7 days goes, the rest stays.
	old := now.Add(-config.WatchSampleRetention - time.Minute)
	if err := s.AppendSample(ctx, pipeline, m, old, 1); err != nil {
		t.Fatal(err)
	}
	purged, err := s.PurgeSamplesBefore(ctx, now.Add(-config.WatchSampleRetention))
	// Exactly the one old row: the cap series starts on the cutoff itself.
	if err != nil || purged != 1 {
		t.Fatalf("purge = %d, %v", purged, err)
	}
	if got, _ := s.Samples(ctx, pipeline, m, time.Time{}); !samePoints(got, want) {
		t.Fatalf("after purge = %+v, want %+v", got, want)
	}
}

func samePoints(a, b []clients.SamplePoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].At.Equal(b[i].At) || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func first(p []clients.SamplePoint) any {
	if len(p) == 0 {
		return nil
	}
	return p[0]
}
