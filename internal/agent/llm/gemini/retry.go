package gemini

// TODO(V3a-integration): replace with llm/retry. The shared retry policy and
// the per-endpoint rate limits are V3a's (internal/agent/llm/retry, design
// 7.4), built in parallel with this row. Until both land this file holds a
// minimal local copy behind two unexported seams, retrier and limiter, so the
// swap is local to New (m.retry, m.limit) and Turn's send closure.

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// attempt is one HTTP exchange's outcome as the retry policy sees it.
type attempt struct {
	status     int           // 0 when there was no HTTP answer
	retryable  bool          // the adapter's classification
	retryAfter time.Duration // the server's hint (Retry-After or RetryInfo); 0 = none
}

// retrier runs send until an attempt is final. It returns nil, or the
// context's error when the caller's deadline ended a wait (the last
// attempt's outcome stays with the caller's closure).
//
// TODO(V3a-integration): replace with llm/retry.
type retrier interface {
	do(ctx context.Context, send func(context.Context) attempt) error
}

// noRetry sends once (the conformance suite's adapters).
type noRetry struct{}

func (noRetry) do(ctx context.Context, send func(context.Context) attempt) error {
	send(ctx)
	return nil
}

// backoff is design 7.4: at most two retries, 1 s then 4 s with ±20%
// jitter, a server hint honoured up to 60 s, never past the deadline.
//
// TODO(V3a-integration): replace with llm/retry.
type backoff struct {
	delays   []time.Duration
	maxAfter time.Duration
	jitter   func(time.Duration) time.Duration
	sleep    func(context.Context, time.Duration) error
}

func newBackoff() *backoff {
	return &backoff{
		delays:   []time.Duration{time.Second, 4 * time.Second},
		maxAfter: 60 * time.Second,
		jitter: func(d time.Duration) time.Duration {
			return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
		},
		sleep: sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (b *backoff) do(ctx context.Context, send func(context.Context) attempt) error {
	for i := 0; ; i++ {
		a := send(ctx)
		if !a.retryable || i >= len(b.delays) {
			return nil
		}
		wait := b.jitter(b.delays[i])
		if a.retryAfter > 0 {
			wait = min(a.retryAfter, b.maxAfter)
		}
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= wait {
			// The retry could not finish before the deadline: stop with
			// the last answer instead of sleeping into it.
			return nil
		}
		if err := b.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// retryAfterHeader reads a Retry-After of whole seconds or an HTTP date.
func retryAfterHeader(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// limiter bounds requests in flight and tokens per minute for one endpoint.
//
// TODO(V3a-integration): replace with llm/retry (whose limiter is shared by
// every role on the endpoint; this one is per adapter instance).
type limiter interface {
	// acquire waits for a slot and for tokens (an estimate of the request's
	// input and output); release frees the slot.
	acquire(ctx context.Context, tokens int64) (release func(), err error)
}

type endpointLimiter struct {
	sem chan struct{} // nil = no concurrency bound

	mu       sync.Mutex
	tpm      int64 // 0 = no token budget
	tokens   float64
	last     time.Time
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
	log      *slog.Logger
	longOnce sync.Once
}

func newEndpointLimiter(concurrency, tpm int64, log *slog.Logger) *endpointLimiter {
	if log == nil {
		log = slog.Default()
	}
	l := &endpointLimiter{tpm: tpm, tokens: float64(tpm), now: time.Now, sleep: sleepCtx, log: log}
	l.last = l.now()
	if concurrency > 0 {
		l.sem = make(chan struct{}, concurrency)
	}
	return l
}

func (l *endpointLimiter) acquire(ctx context.Context, tokens int64) (func(), error) {
	if l.sem != nil {
		select {
		case l.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	release := func() {
		if l.sem != nil {
			<-l.sem
		}
	}
	if err := l.take(ctx, tokens); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// take draws tokens from the bucket (refilled at tpm per minute), waiting for
// a refill when it would overdraw; a request larger than the whole budget
// waits for a full bucket.
func (l *endpointLimiter) take(ctx context.Context, tokens int64) error {
	if l.tpm <= 0 {
		return nil
	}
	need := float64(min(tokens, l.tpm))
	for {
		l.mu.Lock()
		now := l.now()
		l.tokens = min(float64(l.tpm), l.tokens+now.Sub(l.last).Minutes()*float64(l.tpm))
		l.last = now
		if l.tokens >= need {
			l.tokens -= need
			l.mu.Unlock()
			return nil
		}
		wait := time.Duration((need - l.tokens) / float64(l.tpm) * float64(time.Minute))
		l.mu.Unlock()
		if wait > time.Minute {
			l.longOnce.Do(func() {
				l.log.Warn("llm/gemini: waiting over 60s for the tokens-per-minute budget (PIPEROOM_MODEL_TPM)")
			})
		}
		if err := l.sleep(ctx, wait); err != nil {
			return err
		}
	}
}
