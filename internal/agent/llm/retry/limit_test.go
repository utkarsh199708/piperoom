package retry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock drives a Limiter's now and sleep: sleeping advances the clock.
type fakeClock struct {
	mu    sync.Mutex
	t     time.Time
	waits []time.Duration
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waits = append(c.waits, d)
	c.t = c.t.Add(d)
	return nil
}

func newTestLimiter(l Limits, log *slog.Logger) (*Limiter, *fakeClock) {
	c := &fakeClock{t: time.Now()}
	lim := NewLimiter("https://api.example.test", l, log)
	lim.now, lim.sleep = c.now, c.sleep
	return lim, c
}

func TestNilLimiterLimitsNothing(t *testing.T) {
	var l *Limiter
	release, err := l.Acquire(context.Background(), 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	release(5)
	l.Observe(http.Header{"X-Ratelimit-Remaining-Tokens": {"0"}})
	if (*Limiters)(nil).For("x") != nil {
		t.Fatal("a nil set hands out a nil limiter")
	}
}

func TestConcurrencyBound(t *testing.T) {
	l := NewLimiter("e", Limits{MaxConcurrency: 2}, nil)
	var inFlight, peak atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := l.Acquire(context.Background(), 10)
			if err != nil {
				t.Error(err)
				return
			}
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			release(10)
		}()
	}
	wg.Wait()
	if peak.Load() > 2 {
		t.Fatalf("peak in flight = %d, want at most 2", peak.Load())
	}
}

func TestConcurrencyWaitHonoursContext(t *testing.T) {
	l := NewLimiter("e", Limits{MaxConcurrency: 1}, nil)
	release, _ := l.Acquire(context.Background(), 1)
	defer release(0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := l.Acquire(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context's deadline", err)
	}
}

func TestReleaseTwiceFreesOneSlot(t *testing.T) {
	l := NewLimiter("e", Limits{MaxConcurrency: 1}, nil)
	r1, _ := l.Acquire(context.Background(), 1)
	r1(0)
	r1(0) // must not free a slot it no longer holds
	r2, _ := l.Acquire(context.Background(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := l.Acquire(ctx, 1); err == nil {
		t.Fatal("a double release freed a second slot")
	}
	r2(0)
}

func TestTokenBucketWaitsForRefill(t *testing.T) {
	l, c := newTestLimiter(Limits{TPM: 60_000}, nil) // 1000 tokens a second
	release, err := l.Acquire(context.Background(), 50_000)
	if err != nil {
		t.Fatal(err)
	}
	release(50_000)
	// 10_000 left; 30_000 more needs 20 s of refill.
	if _, err := l.Acquire(context.Background(), 30_000); err != nil {
		t.Fatal(err)
	}
	if len(c.waits) != 1 || c.waits[0] != 20*time.Second {
		t.Fatalf("waits = %v, want [20s]", c.waits)
	}
}

func TestTokenBucketSettlesAgainstTheEstimate(t *testing.T) {
	l, c := newTestLimiter(Limits{TPM: 60_000}, nil)
	release, _ := l.Acquire(context.Background(), 60_000)
	release(1_000) // the estimate over-took 59_000
	if _, err := l.Acquire(context.Background(), 50_000); err != nil {
		t.Fatal(err)
	}
	if len(c.waits) != 0 {
		t.Fatalf("waits = %v; the settled tokens should have been given back", c.waits)
	}
}

func TestTokenBucketLargerThanBucketGoesWhenFull(t *testing.T) {
	l, c := newTestLimiter(Limits{TPM: 1_000}, nil)
	if _, err := l.Acquire(context.Background(), 5_000); err != nil {
		t.Fatal(err)
	}
	if len(c.waits) != 0 {
		t.Fatalf("a full bucket must let an oversized request through, waited %v", c.waits)
	}
}

func TestWaitPastDeadlineRefusesWithoutWaiting(t *testing.T) {
	l, c := newTestLimiter(Limits{TPM: 600, MaxConcurrency: 1}, nil) // 10 tokens a second
	r, _ := l.Acquire(context.Background(), 600)
	r(600)
	ctx, cancel := context.WithDeadline(context.Background(), c.now().Add(5*time.Second))
	defer cancel()
	if _, err := l.Acquire(ctx, 600); !errors.Is(err, ErrWaitPastDeadline) {
		t.Fatalf("err = %v, want ErrWaitPastDeadline", err)
	}
	if len(c.waits) != 0 {
		t.Fatalf("waited %v before refusing", c.waits)
	}
	// The slot was freed on refusal.
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if _, err := l.Acquire(ctx2, 0); err != nil {
		t.Fatalf("the refused Acquire kept its slot: %v", err)
	}
}

func TestObservePacesByServerHeaders(t *testing.T) {
	var logs bytes.Buffer
	l, c := newTestLimiter(Limits{}, slog.New(slog.NewTextHandler(&logs, nil)))
	l.Observe(http.Header{
		"X-Ratelimit-Remaining-Tokens": {"1000"},
		"X-Ratelimit-Reset-Tokens":     {"6m0s"},
	})
	// Enough left: no wait.
	r, _ := l.Acquire(context.Background(), 500)
	r(500)
	if len(c.waits) != 0 {
		t.Fatalf("waits = %v, want none", c.waits)
	}
	// 500 left after the first request; 2000 needs the window to reset (capped at 60 s).
	if _, err := l.Acquire(context.Background(), 2000); err != nil {
		t.Fatal(err)
	}
	if len(c.waits) != 1 || c.waits[0] != MaxRetryAfter {
		t.Fatalf("waits = %v, want [60s]", c.waits)
	}

	l.Observe(http.Header{"X-Ratelimit-Remaining-Requests": {"0"}, "X-Ratelimit-Reset-Requests": {"1.5s"}})
	if _, err := l.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if got := c.waits[len(c.waits)-1]; got != 1500*time.Millisecond {
		t.Fatalf("request window wait = %v, want 1.5s", got)
	}
}

func TestLongWaitLoggedOncePerEndpoint(t *testing.T) {
	var logs bytes.Buffer
	l, _ := newTestLimiter(Limits{TPM: 60}, slog.New(slog.NewTextHandler(&logs, nil))) // 1 token a second
	for range 3 {
		r, err := l.Acquire(context.Background(), 60)
		if err != nil {
			t.Fatal(err)
		}
		r(120) // used twice the estimate: the next one waits 120 s
	}
	if n := strings.Count(logs.String(), "waiting for the endpoint's rate limit"); n != 1 {
		t.Fatalf("long-wait log lines = %d, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "https://api.example.test") {
		t.Fatalf("the log names no endpoint: %s", logs.String())
	}
}

func TestResetDuration(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want time.Duration
		ok   bool
	}{
		{"6m0s", MaxRetryAfter, true},
		{"1s", time.Second, true},
		{"20ms", 20 * time.Millisecond, true},
		{"7.66s", 7660 * time.Millisecond, true},
		{"2", 2 * time.Second, true},
		{"", 0, false},
		{"soon", 0, false},
		{"-1s", 0, false},
	} {
		got, ok := ResetDuration(tc.v)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ResetDuration(%q) = %v %v, want %v %v", tc.v, got, ok, tc.want, tc.ok)
		}
	}
}

func TestLimitersShareOnePerEndpoint(t *testing.T) {
	s := NewLimiters(Limits{MaxConcurrency: 1}, nil)
	if s.For("https://a") != s.For("https://a") {
		t.Fatal("two roles on one endpoint got different limiters")
	}
	if s.For("https://a") == s.For("https://b") {
		t.Fatal("two endpoints share a limiter")
	}
}
