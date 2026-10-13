package loadtest

import (
	"math"
	"sync"
	"testing"
	"time"
)

func durs(ms ...int) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, m := range ms {
		out[i] = time.Duration(m) * time.Millisecond
	}
	return out
}

func TestPercentileNearestRank(t *testing.T) {
	// 1..100 ms: nearest rank p is exactly p ms.
	hundred := make([]time.Duration, 100)
	for i := range hundred {
		hundred[i] = time.Duration(i+1) * time.Millisecond
	}
	tests := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{"empty", nil, 50, 0},
		{"single p0", durs(7), 0, 7 * time.Millisecond},
		{"single p50", durs(7), 50, 7 * time.Millisecond},
		{"single p100", durs(7), 100, 7 * time.Millisecond},
		{"negative p is min", durs(1, 2, 3), -5, time.Millisecond},
		{"NaN p is min", durs(1, 2, 3), math.NaN(), time.Millisecond},
		{"p above 100 is max", durs(1, 2, 3), 250, 3 * time.Millisecond},
		{"p50 of 1..100", hundred, 50, 50 * time.Millisecond},
		{"p95 of 1..100", hundred, 95, 95 * time.Millisecond},
		{"p99 of 1..100", hundred, 99, 99 * time.Millisecond},
		{"p99.9 of 1..100 is max", hundred, 99.9, 100 * time.Millisecond},
		{"p1 of 1..100", hundred, 1, time.Millisecond},
		// ceil(0.5*4)=2 -> second value; no interpolation between 2 and 3.
		{"p50 of four", durs(1, 2, 3, 4), 50, 2 * time.Millisecond},
		{"p51 of four", durs(1, 2, 3, 4), 51, 3 * time.Millisecond},
		// ceil(0.95*20)=19, not 20 (float 0.95*20 = 19.000000000000004).
		{"p95 of twenty", durs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20), 95, 19 * time.Millisecond},
		{"p99 of ten is max", durs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 99, 10 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Percentile(tc.sorted, tc.p); got != tc.want {
				t.Fatalf("Percentile(%v, %v) = %v, want %v", tc.sorted, tc.p, got, tc.want)
			}
		})
	}
}

func TestSummarizeDoesNotReorderInput(t *testing.T) {
	in := durs(30, 10, 20)
	d := Summarize(in)
	if in[0] != 30*time.Millisecond || in[1] != 10*time.Millisecond || in[2] != 20*time.Millisecond {
		t.Fatalf("Summarize changed its input: %v", in)
	}
	want := Dist{N: 3, MinMS: 10, P50MS: 20, P95MS: 30, P99MS: 30, MaxMS: 30, MeanMS: 20}
	if d != want {
		t.Fatalf("Summarize = %+v, want %+v", d, want)
	}
}

func TestSummarizeEmptyAndSubMillisecond(t *testing.T) {
	if d := Summarize(nil); d != (Dist{}) {
		t.Fatalf("Summarize(nil) = %+v, want zero", d)
	}
	d := Summarize([]time.Duration{1500 * time.Microsecond, 250 * time.Microsecond})
	if d.MinMS != 0.25 || d.MaxMS != 1.5 || d.P50MS != 0.25 || d.MeanMS != 0.875 {
		t.Fatalf("sub-millisecond summary = %+v", d)
	}
}

func TestRecorderLimitAndReset(t *testing.T) {
	r := NewRecorder(3)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r.Add(time.Duration(i) * time.Millisecond)
		}(i)
	}
	wg.Wait()
	s, dropped := r.Snapshot()
	if len(s) != 3 || dropped != 7 {
		t.Fatalf("kept %d dropped %d, want 3 and 7", len(s), dropped)
	}
	r.Reset()
	if s, dropped := r.Snapshot(); len(s) != 0 || dropped != 0 {
		t.Fatalf("after Reset: kept %d dropped %d", len(s), dropped)
	}
	if NewRecorder(0).limit != 1_000_000 {
		t.Fatal("NewRecorder(0) should use the default limit")
	}
}
