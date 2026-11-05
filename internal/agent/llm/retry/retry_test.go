package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"syscall"
	"testing"
	"time"
)

// recorder is a Policy.Sleep that records the waits and returns at once.
type recorder struct{ waits []time.Duration }

func (r *recorder) sleep(_ context.Context, d time.Duration) error {
	r.waits = append(r.waits, d)
	return nil
}

func TestDoRetriesTwiceWithBackoffThenStops(t *testing.T) {
	rec := &recorder{}
	p := Default()
	p.Sleep = rec.sleep
	p.Rand = func() float64 { return 0.5 } // no jitter
	calls := 0
	boom := errors.New("503")
	err := p.Do(context.Background(), func(context.Context) (Outcome, error) {
		calls++
		return Outcome{Retry: true}, boom
	})
	if !errors.Is(err, boom) || calls != 3 {
		t.Fatalf("err = %v, calls = %d; want the last error after 3 attempts", err, calls)
	}
	if fmt.Sprint(rec.waits) != fmt.Sprint([]time.Duration{time.Second, 4 * time.Second}) {
		t.Fatalf("waits = %v, want [1s 4s]", rec.waits)
	}
}

func TestDoJitterStaysWithinTwentyPercent(t *testing.T) {
	for _, r := range []float64{0, 0.999999} {
		rec := &recorder{}
		p := Default()
		p.Sleep, p.Rand = rec.sleep, func() float64 { return r }
		_ = p.Do(context.Background(), func(context.Context) (Outcome, error) { return Outcome{Retry: true}, errors.New("x") })
		for i, w := range rec.waits {
			base := DefaultBackoff[i]
			if w < time.Duration(float64(base)*0.8)-time.Millisecond || w > time.Duration(float64(base)*1.2)+time.Millisecond {
				t.Fatalf("rand %v: wait %d = %v, outside ±20%% of %v", r, i, w, base)
			}
		}
	}
}

func TestDoStopsOnSuccessOrUnretryable(t *testing.T) {
	p := Default()
	p.Sleep = (&recorder{}).sleep
	calls := 0
	if err := p.Do(context.Background(), func(context.Context) (Outcome, error) {
		calls++
		if calls == 2 {
			return Outcome{}, nil
		}
		return Outcome{Retry: true}, errors.New("503")
	}); err != nil || calls != 2 {
		t.Fatalf("err = %v calls = %d; want success on the second attempt", err, calls)
	}
	calls = 0
	quota := errors.New("insufficient_quota")
	if err := p.Do(context.Background(), func(context.Context) (Outcome, error) {
		calls++
		return Outcome{Retry: false}, quota
	}); !errors.Is(err, quota) || calls != 1 {
		t.Fatalf("err = %v calls = %d; an unretryable failure is sent once", err, calls)
	}
}

func TestDoZeroPolicyNeverRetries(t *testing.T) {
	calls := 0
	_ = Policy{}.Do(context.Background(), func(context.Context) (Outcome, error) {
		calls++
		return Outcome{Retry: true}, errors.New("503")
	})
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoRetryAfterHonouredAndCapped(t *testing.T) {
	for _, tc := range []struct {
		after, want time.Duration
	}{
		{2 * time.Second, 2 * time.Second},
		{time.Hour, MaxRetryAfter},
	} {
		rec := &recorder{}
		p := Policy{MaxRetries: 1, Sleep: rec.sleep}
		_ = p.Do(context.Background(), func(context.Context) (Outcome, error) {
			return Outcome{Retry: true, RetryAfter: tc.after}, errors.New("429")
		})
		if len(rec.waits) != 1 || rec.waits[0] != tc.want {
			t.Fatalf("Retry-After %v: waits = %v, want [%v]", tc.after, rec.waits, tc.want)
		}
	}
}

func TestDoNeverWaitsPastTheDeadline(t *testing.T) {
	rec := &recorder{}
	p := Policy{MaxRetries: 2, Sleep: rec.sleep}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	last := errors.New("429")
	err := p.Do(ctx, func(context.Context) (Outcome, error) {
		calls++
		return Outcome{Retry: true, RetryAfter: 30 * time.Second}, last
	})
	if !errors.Is(err, last) || calls != 1 || len(rec.waits) != 0 {
		t.Fatalf("err = %v calls = %d waits = %v; a wait past the deadline must return the last error at once", err, calls, rec.waits)
	}
}

func TestDoRealSleepHonoursCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{MaxRetries: 1, Backoff: []time.Duration{time.Hour}}
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	last := errors.New("503")
	if err := p.Do(ctx, func(context.Context) (Outcome, error) { return Outcome{Retry: true}, last }); !errors.Is(err, last) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("Do slept through a cancelled context")
	}
}

func TestRetryableStatus(t *testing.T) {
	want := map[int]bool{408: true, 429: true, 500: true, 502: true, 503: true, 504: true, 529: true}
	for s := 100; s < 600; s++ {
		if RetryableStatus(s) != want[s] {
			t.Errorf("RetryableStatus(%d) = %v", s, !want[s])
		}
	}
}

func TestConnReset(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{&url.Error{Op: "Post", URL: "x", Err: syscall.ECONNRESET}, true},
		{&url.Error{Op: "Post", URL: "x", Err: io.EOF}, true},
		{fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{&url.Error{Op: "Post", URL: "x", Err: context.DeadlineExceeded}, false},
		{context.Canceled, false},
		{syscall.ECONNREFUSED, false},
		{errors.New("tls: bad certificate"), false},
	} {
		if got := ConnReset(tc.err); got != tc.want {
			t.Errorf("ConnReset(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestAfter(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		h    http.Header
		want time.Duration
	}{
		{http.Header{}, 0},
		{http.Header{"Retry-After": {"7"}}, 7 * time.Second},
		{http.Header{"Retry-After": {"1.5"}}, 1500 * time.Millisecond},
		{http.Header{"Retry-After": {now.Add(90 * time.Second).Format(http.TimeFormat)}}, 90 * time.Second},
		{http.Header{"Retry-After": {now.Add(-time.Minute).Format(http.TimeFormat)}}, 0},
		{http.Header{"Retry-After": {"soon"}}, 0},
		{http.Header{"Retry-After": {"-3"}}, 0},
		{http.Header{"Retry-After-Ms": {"250"}, "Retry-After": {"9"}}, 250 * time.Millisecond},
	} {
		if got := After(tc.h, now); got != tc.want {
			t.Errorf("After(%v) = %v, want %v", tc.h, got, tc.want)
		}
	}
}
