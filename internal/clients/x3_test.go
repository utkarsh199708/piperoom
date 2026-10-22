package clients_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
)

// fakeSamples is the smallest store the X3 implementers can build against
// (E5): it compiles against both interfaces and keeps their documented rules.
type fakeSamples struct {
	rows map[[2]string][]clients.SamplePoint
}

var (
	_ clients.SampleWriter = (*fakeSamples)(nil)
	_ clients.SampleReader = (*fakeSamples)(nil)
)

func (f *fakeSamples) AppendSample(_ context.Context, pipelineID, metric string, at time.Time, value float64) error {
	if pipelineID == "" || clients.WatchMetricUnit(metric) == "" || math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("fake: bad sample %q %q %v", pipelineID, metric, value)
	}
	k := [2]string{pipelineID, metric}
	if slices.ContainsFunc(f.rows[k], func(p clients.SamplePoint) bool { return p.At.Equal(at) }) {
		return nil
	}
	f.rows[k] = append(f.rows[k], clients.SamplePoint{At: at.UTC(), Value: value})
	sort.Slice(f.rows[k], func(i, j int) bool { return f.rows[k][i].At.Before(f.rows[k][j].At) })
	return nil
}

func (f *fakeSamples) Samples(_ context.Context, pipelineID, metric string, since time.Time) ([]clients.SamplePoint, error) {
	if clients.WatchMetricUnit(metric) == "" {
		return nil, errors.New("fake: unknown metric")
	}
	out := []clients.SamplePoint{}
	for _, p := range f.rows[[2]string{pipelineID, metric}] {
		if !p.At.Before(since) {
			out = append(out, p)
		}
	}
	if len(out) > clients.MaxSamplesPerSeries {
		out = out[len(out)-clients.MaxSamplesPerSeries:]
	}
	return out, nil
}

func TestWatchMetrics(t *testing.T) {
	want := map[string]string{
		"slot_wal_bytes": "bytes", "dlq_records": "records", "group_lag": "records",
		"db_size_bytes": "bytes", "topic_oldest_unread_age": "seconds",
	}
	if len(clients.WatchMetrics) != len(want) {
		t.Fatalf("WatchMetrics = %v; want the %d names of X3a", clients.WatchMetrics, len(want))
	}
	for _, m := range clients.WatchMetrics {
		if got := clients.WatchMetricUnit(m); got != want[m] {
			t.Errorf("WatchMetricUnit(%q) = %q; want %q", m, got, want[m])
		}
	}
	for _, bad := range []string{"", "SLOT_WAL_BYTES", "slot_wal", "lag"} {
		if u := clients.WatchMetricUnit(bad); u != "" {
			t.Errorf("WatchMetricUnit(%q) = %q; want \"\"", bad, u)
		}
	}
}

// The series cap is the retention window in sample periods, so a full
// 7-day trends answer is never cut.
func TestMaxSamplesPerSeriesMatchesRetention(t *testing.T) {
	if want := int(config.WatchSampleRetention / config.WatchSampleInterval); clients.MaxSamplesPerSeries != want {
		t.Errorf("MaxSamplesPerSeries = %d; want %d (retention / interval)", clients.MaxSamplesPerSeries, want)
	}
}

// E4, E5: a store built on the contract keeps order, dedups a repeated at,
// refuses bad input and caps a series at the boundary.
func TestFakeSampleStoreFollowsTheContract(t *testing.T) {
	ctx := context.Background()
	f := &fakeSamples{rows: map[[2]string][]clients.SamplePoint{}}
	t0 := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	n := clients.MaxSamplesPerSeries + 5
	for i := n - 1; i >= 0; i-- { // newest first: the reader must still sort
		if err := f.AppendSample(ctx, "orders", clients.WatchMetricSlotWALBytes, t0.Add(time.Duration(i)*15*time.Minute), float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.AppendSample(ctx, "orders", clients.WatchMetricSlotWALBytes, t0, 99); err != nil {
		t.Fatalf("repeat at: %v; want no error", err)
	}
	got, err := f.Samples(ctx, "orders", clients.WatchMetricSlotWALBytes, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != clients.MaxSamplesPerSeries || got[0].Value != 5 || got[len(got)-1].Value != float64(n-1) {
		t.Fatalf("got %d points from %v to %v; want the newest %d, oldest first", len(got), got[0], got[len(got)-1], clients.MaxSamplesPerSeries)
	}
	for _, bad := range []struct {
		p, m string
		v    float64
	}{{"", clients.WatchMetricGroupLag, 1}, {"orders", "nope", 1}, {"orders", clients.WatchMetricGroupLag, math.NaN()}, {"orders", clients.WatchMetricGroupLag, math.Inf(1)}} {
		if err := f.AppendSample(ctx, bad.p, bad.m, t0, bad.v); err == nil {
			t.Errorf("AppendSample(%q, %q, %v) = nil; want an error", bad.p, bad.m, bad.v)
		}
	}
	if none, err := f.Samples(ctx, "other", clients.WatchMetricGroupLag, t0); err != nil || none == nil || len(none) != 0 {
		t.Errorf("no samples = %#v, %v; want [], nil", none, err)
	}
}
