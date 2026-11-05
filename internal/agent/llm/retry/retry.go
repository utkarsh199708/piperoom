// Package retry is the retry policy and the per-endpoint limits every
// non-Claude model adapter shares (design docs/design/model-providers.md
// section 7.4): the OpenAI-compatible adapter (V3a, V3b) and the Gemini
// adapter (V4) use it; the Bedrock Converse adapter (V5) sets the AWS SDK's
// retryer to the same bounds. The Anthropic adapter keeps its SDK's retries.
//
// The package knows HTTP statuses and headers, never a provider's body: the
// adapter decides what an attempt was (a quota 429 is the adapter's call) and
// tells Policy.Do through an Outcome.
//
// Typical use in an adapter's Turn:
//
//	err := policy.Do(ctx, func(ctx context.Context) (retry.Outcome, error) {
//		release, err := limiter.Acquire(ctx, estimate)
//		if err != nil {
//			return retry.Outcome{}, err
//		}
//		resp, err := client.Do(req)
//		if err != nil {
//			release(0)
//			return retry.Outcome{Retry: retry.ConnReset(err)}, err
//		}
//		limiter.Observe(resp.Header)
//		// read the capped body, classify ...
//		release(usedTokens)
//		return retry.Outcome{
//			Retry:      retry.RetryableStatus(resp.StatusCode) && !quota,
//			RetryAfter: retry.After(resp.Header, time.Now()),
//		}, classifiedErr
//	})
package retry

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// The bounds of design 7.4.
const (
	// DefaultMaxRetries is how many times a failed attempt is retried.
	DefaultMaxRetries = 2
	// DefaultJitter is the relative jitter on each backoff (±20%).
	DefaultJitter = 0.2
	// MaxRetryAfter caps a server's Retry-After.
	MaxRetryAfter = 60 * time.Second
)

// DefaultBackoff is the wait before the first and the second retry.
var DefaultBackoff = []time.Duration{1 * time.Second, 4 * time.Second}

// Policy is how many times, and after how long, a failed attempt is retried.
// The zero Policy never retries (what a conformance fixture wants); use
// Default for production.
type Policy struct {
	// MaxRetries is the number of retries after the first attempt.
	MaxRetries int
	// Backoff is the wait before retry i (the last entry repeats). Empty
	// means DefaultBackoff.
	Backoff []time.Duration
	// Jitter is the relative jitter applied to Backoff (0.2 = ±20%). It is
	// not applied to a server's Retry-After.
	Jitter float64
	// Sleep waits d or until ctx ends; nil is a real timer. Tests record
	// the waits through it.
	Sleep func(ctx context.Context, d time.Duration) error
	// Rand returns a number in [0, 1); nil is math/rand/v2.
	Rand func() float64
}

// Default is the policy of design 7.4: two retries, 1 s then 4 s, ±20%
// jitter, Retry-After honoured up to 60 s.
func Default() Policy {
	return Policy{MaxRetries: DefaultMaxRetries, Backoff: DefaultBackoff, Jitter: DefaultJitter}
}

// Outcome is what an attempt tells Do besides its error.
type Outcome struct {
	// Retry says the attempt failed in a way the policy may retry: a
	// RetryableStatus that is not a quota or billing answer, or a ConnReset
	// before any response byte. A response that had started is never
	// retried.
	Retry bool
	// RetryAfter is the server's wait (After), 0 when it sent none. It
	// replaces the backoff, capped at MaxRetryAfter.
	RetryAfter time.Duration
}

// Do runs attempt until it succeeds, it fails without Outcome.Retry, the
// retries are spent, or the next wait would end past ctx's deadline. It
// returns the last attempt's error (nil on success); a wait cut short by ctx
// returns the last attempt's error too, since that is what the caller
// classifies. It never waits past ctx's deadline.
func (p Policy) Do(ctx context.Context, attempt func(ctx context.Context) (Outcome, error)) error {
	for i := 0; ; i++ {
		out, err := attempt(ctx)
		if err == nil || !out.Retry || i >= p.MaxRetries || ctx.Err() != nil {
			return err
		}
		wait := p.wait(i, out.RetryAfter)
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= wait {
			return err
		}
		if serr := p.sleep(ctx, wait); serr != nil {
			return err
		}
	}
}

// wait is the pause before retry i+1.
func (p Policy) wait(i int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, MaxRetryAfter)
	}
	b := p.Backoff
	if len(b) == 0 {
		b = DefaultBackoff
	}
	d := b[min(i, len(b)-1)]
	if p.Jitter > 0 {
		r := p.Rand
		if r == nil {
			r = rand.Float64
		}
		d = time.Duration(float64(d) * (1 + p.Jitter*(2*r()-1)))
	}
	return d
}

func (p Policy) sleep(ctx context.Context, d time.Duration) error {
	if p.Sleep != nil {
		return p.Sleep(ctx, d)
	}
	return Sleep(ctx, d)
}

// Sleep waits d or until ctx ends, whichever is first; it returns ctx's error
// when ctx ended first.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// RetryableStatus reports the statuses design 7.4 retries: 408, 429, 500,
// 502, 503, 504 and 529. The adapter still refuses a 429 that means quota or
// billing (OpenAI insufficient_quota, Gemini RESOURCE_EXHAUSTED with a quota
// reason) before setting Outcome.Retry.
func RetryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout,
		529:
		return true
	}
	return false
}

// ConnReset reports a transport error that a retry may cure: the connection
// was reset or closed before any response byte (ECONNRESET, or EOF while
// waiting for the response headers). Pass only the error of the request
// itself (http.Client.Do), never one from reading a body that had started.
// A context error is never a reset.
func ConnReset(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// After reads a server's requested wait from h: retry-after-ms (OpenAI,
// Azure), else Retry-After as seconds or an HTTP date. It returns 0 when
// there is none or it cannot be read; it does not cap (Policy.Do does).
func After(h http.Header, now time.Time) time.Duration {
	if v := strings.TrimSpace(h.Get("retry-after-ms")); v != "" {
		if ms, err := strconv.ParseFloat(v, 64); err == nil && ms > 0 {
			return time.Duration(ms * float64(time.Millisecond))
		}
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if s, err := strconv.ParseFloat(v, 64); err == nil {
		if s <= 0 {
			return 0
		}
		return time.Duration(s * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
