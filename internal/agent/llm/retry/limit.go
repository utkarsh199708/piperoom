package retry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrWaitPastDeadline is returned by Limiter.Acquire when the wait for a slot
// or for tokens would end past ctx's deadline: the adapter reports it as
// rate_limited without sending, rather than sending into a 429.
var ErrWaitPastDeadline = errors.New("retry: the endpoint's rate limit wait ends past the turn's deadline")

// longWait is the wait above which Limiter logs once per endpoint.
const longWait = 60 * time.Second

// Limits bounds one endpoint (design 7.4).
type Limits struct {
	// MaxConcurrency bounds requests in flight (PIPEROOM_MODEL_MAX_CONCURRENCY,
	// default PIPEROOM_MAX_AGENTS); 0 = no bound.
	MaxConcurrency int
	// TPM is a token bucket of this many tokens per minute
	// (PIPEROOM_MODEL_TPM); 0 = none.
	TPM int64
}

// Limiter bounds the requests to one provider endpoint, shared by every role
// on it: requests in flight, an optional tokens-per-minute bucket, and the
// pacing the server asks for in its x-ratelimit-* headers. A nil *Limiter
// limits nothing. It is safe for concurrent use.
type Limiter struct {
	name string // the endpoint as it may be logged (scheme://host)
	log  *slog.Logger
	sem  chan struct{}
	tpm  float64

	mu        sync.Mutex
	tokens    float64   // bucket level (may go negative: a request larger than the bucket is let through when it is full)
	refilled  time.Time // when tokens was last brought up to date
	remaining int64     // x-ratelimit-remaining-tokens, -1 = unknown
	tokReset  time.Time // when the server's token window resets
	reqLeft   int64     // x-ratelimit-remaining-requests, -1 = unknown
	reqReset  time.Time // when the server's request window resets
	warned    bool      // a long wait was logged

	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// NewLimiter returns the limiter of one endpoint. name is the endpoint as it
// may be logged (scheme://host, never a path or a key); log nil means
// slog.Default().
func NewLimiter(name string, l Limits, log *slog.Logger) *Limiter {
	if log == nil {
		log = slog.Default()
	}
	lim := &Limiter{name: name, log: log, remaining: -1, reqLeft: -1, now: time.Now, sleep: Sleep}
	if l.MaxConcurrency > 0 {
		lim.sem = make(chan struct{}, l.MaxConcurrency)
	}
	if l.TPM > 0 {
		lim.tpm = float64(l.TPM)
		lim.tokens = lim.tpm
	}
	return lim
}

// Acquire waits for a request slot, for estimate input tokens in the
// bucket, and for the server's window when its last headers said fewer
// tokens (or no requests) remain than this request needs. It returns
// ErrWaitPastDeadline, without waiting, when the wait would end past ctx's
// deadline, and ctx's error when ctx ends first. release must be called once
// with the tokens the request used (0 when it was not answered): it frees the
// slot and settles the bucket against the estimate.
func (l *Limiter) Acquire(ctx context.Context, estimate int64) (release func(used int64), err error) {
	if l == nil {
		return func(int64) {}, nil
	}
	if l.sem != nil {
		select {
		case l.sem <- struct{}{}:
		default:
			select {
			case l.sem <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	free := func() {
		if l.sem != nil {
			<-l.sem
		}
	}
	for {
		wait := l.reserve(estimate)
		if wait <= 0 {
			break
		}
		if dl, ok := ctx.Deadline(); ok && l.now().Add(wait).After(dl) {
			free()
			return nil, fmt.Errorf("%w (%s)", ErrWaitPastDeadline, wait.Round(time.Second))
		}
		l.noteWait(wait)
		if err := l.sleep(ctx, wait); err != nil {
			free()
			return nil, err
		}
	}
	var once sync.Once
	return func(used int64) {
		once.Do(func() {
			l.settle(estimate, used)
			free()
		})
	}, nil
}

// reserve takes estimate tokens and returns 0, or returns how long to wait
// before trying again.
func (l *Limiter) reserve(estimate int64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Before(l.reqReset) && l.reqLeft == 0 {
		return l.reqReset.Sub(now)
	}
	if now.Before(l.tokReset) && l.remaining >= 0 && l.remaining < estimate {
		return l.tokReset.Sub(now)
	}
	if l.tpm <= 0 {
		l.spendWindow(estimate)
		return 0
	}
	l.refill(now)
	need := float64(estimate)
	// A request larger than the whole bucket goes when the bucket is full,
	// or it would never go.
	if need > l.tpm {
		need = l.tpm
	}
	if l.tokens >= need {
		l.tokens -= float64(estimate)
		l.spendWindow(estimate)
		return 0
	}
	perSec := l.tpm / 60
	return time.Duration(math.Ceil((need-l.tokens)/perSec*1000)) * time.Millisecond
}

// spendWindow counts a request against the server's last reported window,
// so concurrent requests do not all trust the same remainder.
func (l *Limiter) spendWindow(estimate int64) {
	if l.remaining > 0 {
		l.remaining = max(0, l.remaining-estimate)
	}
	if l.reqLeft > 0 {
		l.reqLeft--
	}
}

func (l *Limiter) refill(now time.Time) {
	if !l.refilled.IsZero() {
		l.tokens = min(l.tpm, l.tokens+now.Sub(l.refilled).Seconds()*l.tpm/60)
	}
	l.refilled = now
}

// settle gives back what the estimate over-took, or takes what it
// under-took.
func (l *Limiter) settle(estimate, used int64) {
	if l.tpm <= 0 || used <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill(l.now())
	l.tokens = min(l.tpm, l.tokens+float64(estimate-used))
}

func (l *Limiter) noteWait(d time.Duration) {
	if d <= longWait {
		return
	}
	l.mu.Lock()
	first := !l.warned
	l.warned = true
	l.mu.Unlock()
	if first {
		l.log.Warn("llm: waiting for the endpoint's rate limit; raise the tier or PIPEROOM_MODEL_TPM", "endpoint", l.name, "wait", d.Round(time.Second))
	}
}

// Observe reads the server's x-ratelimit-remaining-tokens,
// x-ratelimit-reset-tokens, x-ratelimit-remaining-requests and
// x-ratelimit-reset-requests (OpenAI and most compatible hosts), so the next
// Acquire waits for the window when it would overdraw it. Missing or
// unreadable headers change nothing.
func (l *Limiter) Observe(h http.Header) {
	if l == nil || h == nil {
		return
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if n, ok := headerInt(h, "x-ratelimit-remaining-tokens"); ok {
		if d, ok := ResetDuration(h.Get("x-ratelimit-reset-tokens")); ok {
			l.remaining, l.tokReset = n, now.Add(d)
		}
	}
	if n, ok := headerInt(h, "x-ratelimit-remaining-requests"); ok {
		if d, ok := ResetDuration(h.Get("x-ratelimit-reset-requests")); ok {
			l.reqLeft, l.reqReset = n, now.Add(d)
		}
	}
}

func headerInt(h http.Header, name string) (int64, bool) {
	v := strings.TrimSpace(h.Get(name))
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// ResetDuration reads an x-ratelimit-reset-* value: a Go-style duration
// ("6m0s", "1.5s", "20ms", as OpenAI and Groq send it) or plain seconds. It
// caps the result at MaxRetryAfter.
func ResetDuration(v string) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		s, ferr := strconv.ParseFloat(v, 64)
		if ferr != nil {
			return 0, false
		}
		d = time.Duration(s * float64(time.Second))
	}
	if d < 0 {
		return 0, false
	}
	return min(d, MaxRetryAfter), true
}

// Limiters hands out one Limiter per endpoint, so every role on the same
// endpoint shares its bounds. It is safe for concurrent use.
type Limiters struct {
	limits Limits
	log    *slog.Logger
	mu     sync.Mutex
	by     map[string]*Limiter
}

// NewLimiters returns the set every endpoint's Limiter is taken from, each
// with limits l.
func NewLimiters(l Limits, log *slog.Logger) *Limiters {
	return &Limiters{limits: l, log: log, by: map[string]*Limiter{}}
}

// For returns endpoint's Limiter, creating it on first use. endpoint is the
// key and the logged name: scheme://host[:port], never a path or a key.
func (s *Limiters) For(endpoint string) *Limiter {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.by[endpoint]; ok {
		return l
	}
	l := NewLimiter(endpoint, s.limits, s.log)
	s.by[endpoint] = l
	return l
}
