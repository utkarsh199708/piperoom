package loadtest

import (
	"math"
	"slices"
	"sync"
	"time"
)

// Percentile returns the nearest-rank percentile p (0..100) of sorted, which
// must be in ascending order. p <= 0 is the minimum and p >= 100 the maximum;
// an empty slice gives 0. Nearest rank never interpolates, so every reported
// value is a latency that was really observed.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if math.IsNaN(p) || p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}
	// Rank is ceil(p/100 * n), 1-based. The small epsilon keeps a product that
	// floating point puts a hair above a whole number (for example 7/100*100 =
	// 7.0000000000000009) from moving one rank up.
	rank := int(math.Ceil(p/100*float64(n) - 1e-9))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// Dist is the distribution of a set of durations, in milliseconds so the JSON
// output is readable without unit conversion.
type Dist struct {
	N      int     `json:"n"`
	MinMS  float64 `json:"min_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
	MeanMS float64 `json:"mean_ms"`
}

// Summarize computes the distribution of samples. It sorts a copy, so the
// caller's slice keeps its order.
func Summarize(samples []time.Duration) Dist {
	if len(samples) == 0 {
		return Dist{}
	}
	s := slices.Clone(samples)
	slices.Sort(s)
	var sum float64
	for _, d := range s {
		sum += float64(d)
	}
	return Dist{
		N:      len(s),
		MinMS:  ms(s[0]),
		P50MS:  ms(Percentile(s, 50)),
		P95MS:  ms(Percentile(s, 95)),
		P99MS:  ms(Percentile(s, 99)),
		MaxMS:  ms(s[len(s)-1]),
		MeanMS: round3(sum / float64(len(s)) / float64(time.Millisecond)),
	}
}

// ms converts d to milliseconds rounded to microseconds.
func ms(d time.Duration) float64 {
	return round3(float64(d) / float64(time.Millisecond))
}

func round3(f float64) float64 { return math.Round(f*1000) / 1000 }

// Recorder collects duration samples from many goroutines. It keeps at most
// limit samples (reservoir-free: later samples are counted but dropped), so a
// runaway run cannot exhaust memory; Dropped says how many were left out.
type Recorder struct {
	mu      sync.Mutex
	limit   int
	samples []time.Duration
	dropped int64
}

// NewRecorder returns a Recorder that keeps at most limit samples; limit <= 0
// means 1,000,000.
func NewRecorder(limit int) *Recorder {
	if limit <= 0 {
		limit = 1_000_000
	}
	return &Recorder{limit: limit}
}

// Add records one sample.
func (r *Recorder) Add(d time.Duration) {
	r.mu.Lock()
	if len(r.samples) < r.limit {
		r.samples = append(r.samples, d)
	} else {
		r.dropped++
	}
	r.mu.Unlock()
}

// Snapshot returns a copy of the samples and the dropped count.
func (r *Recorder) Snapshot() ([]time.Duration, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.samples), r.dropped
}

// Reset drops every sample.
func (r *Recorder) Reset() {
	r.mu.Lock()
	r.samples = r.samples[:0]
	r.dropped = 0
	r.mu.Unlock()
}

// Dist summarizes the samples recorded so far.
func (r *Recorder) Dist() Dist {
	s, _ := r.Snapshot()
	return Summarize(s)
}
