package watch

import (
	"math"
	"testing"
	"time"
)

// series builds samples every step from start, value f(elapsed).
func series(start time.Time, n int, step time.Duration, f func(el time.Duration) int64) []sample {
	out := make([]sample, 0, n)
	for i := range n {
		el := time.Duration(i) * step
		out = append(out, sample{at: start.Add(el), retained: f(el)})
	}
	return out
}

func TestSlopeWindows(t *testing.T) {
	// 6 h of samples every 5 min: flat for 5 h, then +600 MiB/h for the last hour.
	const perHour = 600 << 20
	samples := series(t0, 73, 5*time.Minute, func(el time.Duration) int64 {
		if el <= 5*time.Hour {
			return 100 << 20
		}
		return 100<<20 + int64(float64(perHour)*(el-5*time.Hour).Hours())
	})
	now := t0.Add(6 * time.Hour)

	fast, ok := slope(samples, now, fastWindow)
	if !ok {
		t.Fatal("1h slope: not ok")
	}
	if got := fast * 3600; math.Abs(got-perHour)/perHour > 0.01 {
		t.Fatalf("1h slope = %.0f B/h, want about %d", got, perHour)
	}
	slow, ok := slope(samples, now, slowWindow)
	if !ok {
		t.Fatal("6h slope: not ok")
	}
	// The 6 h line is flatter than the last hour but still positive.
	if !(slow > 0 && slow < fast/2) {
		t.Fatalf("6h slope %.1f B/s should be positive and well below the 1h slope %.1f B/s", slow, fast)
	}
}

func TestSlopeWarmUpIsUnknown(t *testing.T) {
	cases := []struct {
		name    string
		samples []sample
		wantOK  bool
	}{
		{"no samples", nil, false},
		{"two samples over an hour", series(t0, 2, time.Hour, func(time.Duration) int64 { return 1 }), false},
		{"three samples over 20 min", series(t0, 3, 10*time.Minute, func(time.Duration) int64 { return 1 }), false},
		{"many samples over 29 min", series(t0, 30, time.Minute, func(time.Duration) int64 { return 1 }), false},
		{"three samples over 30 min", series(t0, 3, 15*time.Minute, func(time.Duration) int64 { return 1 }), true},
	}
	for _, c := range cases {
		now := t0.Add(time.Hour)
		if len(c.samples) > 0 {
			now = c.samples[len(c.samples)-1].at
		}
		if _, ok := slope(c.samples, now, slowWindow); ok != c.wantOK {
			t.Errorf("%s: ok = %v, want %v", c.name, ok, c.wantOK)
		}
	}
}

func TestSlopeZeroAndNegative(t *testing.T) {
	flat := series(t0, 10, 5*time.Minute, func(time.Duration) int64 { return 500 })
	if r, ok := slope(flat, flat[9].at, slowWindow); !ok || r != 0 {
		t.Fatalf("flat: rate %v ok %v, want 0 true", r, ok)
	}
	down := series(t0, 10, 5*time.Minute, func(el time.Duration) int64 { return 10_000 - int64(el.Seconds()) })
	if r, ok := slope(down, down[9].at, slowWindow); !ok || r >= 0 {
		t.Fatalf("falling: rate %v ok %v, want negative true", r, ok)
	}
}

func TestSlopeIgnoresOldSamples(t *testing.T) {
	// Steep growth 5 h ago, flat in the last hour: the 1 h rate is zero.
	samples := series(t0, 73, 5*time.Minute, func(el time.Duration) int64 {
		if el < time.Hour {
			return int64(el.Seconds()) * 1000
		}
		return 3_600_000
	})
	now := t0.Add(6 * time.Hour)
	if r, ok := slope(samples, now, fastWindow); !ok || r != 0 {
		t.Fatalf("1h rate = %v ok %v, want 0 true", r, ok)
	}
	if r, ok := slope(samples, now, slowWindow); !ok || r <= 0 {
		t.Fatalf("6h rate = %v ok %v, want positive", r, ok)
	}
}

func TestTrimSamples(t *testing.T) {
	samples := series(t0, 100, 5*time.Minute, func(time.Duration) int64 { return 1 })
	now := samples[99].at
	got := trimSamples(samples, now)
	if first := got[0].at; first.Before(now.Add(-slowWindow)) {
		t.Fatalf("kept a sample from %v, older than 6 h before %v", first, now)
	}
	if len(got) < 70 || len(got) > 74 {
		t.Fatalf("kept %d samples, want about 73", len(got))
	}
}

func ptr[T any](v T) *T { return &v }

func TestComputeETA(t *testing.T) {
	const gib = 1 << 30
	perHour := func(b float64) float64 { return b / 3600 }
	cases := []struct {
		name      string
		slow      float64
		slowOK    bool
		retained  int64
		safe      *int64
		headroom  int64
		wantKnown bool
		wantHours float64
		wantBasis string
	}{
		{"safe_wal_size wins", perHour(gib), true, 2 * gib, ptr(int64(4 * gib)), 100 * gib, true, 4, basisSafeWAL},
		{"headroom when safe is NULL", perHour(gib), true, 2 * gib, nil, 10 * gib, true, 8, basisHeadroom},
		{"headroom already used up", perHour(gib), true, 12 * gib, nil, 10 * gib, true, 0, basisHeadroom},
		{"neither", perHour(gib), true, 2 * gib, nil, 0, false, 0, ""},
		{"warming up", perHour(gib), false, 2 * gib, ptr(int64(gib)), 0, false, 0, ""},
		{"safe_wal_size negative: already unreserved", perHour(gib), true, 2 * gib, ptr(int64(-5 * mib)), 10 * gib, false, 0, ""},
		{"safe_wal_size zero", perHour(gib), true, 2 * gib, ptr(int64(0)), 10 * gib, false, 0, ""},
		{"not growing", 0, true, 2 * gib, ptr(int64(gib)), 0, false, 0, ""},
		{"shrinking", -5, true, 2 * gib, ptr(int64(gib)), 0, false, 0, ""},
	}
	for _, c := range cases {
		e := computeETA(c.slow, c.slowOK, c.retained, c.safe, c.headroom)
		if e.Known != c.wantKnown {
			t.Errorf("%s: Known = %v (%s), want %v", c.name, e.Known, e.Reason, c.wantKnown)
			continue
		}
		if !c.wantKnown {
			if e.Reason == "" {
				t.Errorf("%s: no reason given", c.name)
			}
			continue
		}
		if math.Abs(e.D.Hours()-c.wantHours) > 0.01 || e.Basis != c.wantBasis {
			t.Errorf("%s: eta %.2fh basis %q, want %.2fh %q", c.name, e.D.Hours(), e.Basis, c.wantHours, c.wantBasis)
		}
	}
}
