package converse

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"piperoom/internal/agent/llm"
)

// The retry bounds of design 7.4, which every adapter shares: at most two
// retries, 1 s then 4 s with ±20% jitter, Retry-After honoured up to 60 s,
// never past the turn's deadline (the SDK's retry sleep ends with ctx).
const (
	maxAttempts   = 3 // the first try and two retries
	firstDelay    = 1 * time.Second
	laterDelay    = 4 * time.Second
	jitter        = 0.2
	maxRetryAfter = 60 * time.Second
)

// retryStatuses are the statuses design 7.4 retries.
var retryStatuses = map[int]bool{408: true, 429: true, 500: true, 502: true, 503: true, 504: true, 529: true}

// retryPolicy is the SDK retryer set to the design's bounds. attempts and
// delay are test hooks (Config).
type retryPolicy struct {
	attempts int
	delay    func(attempt int, err error) time.Duration
}

// retryer is the SDK's standard retryer with the design's statuses, schedule
// and attempts, and no client-side retry quota (the token bucket would make
// whether a turn retries depend on other rooms' failures).
func (p retryPolicy) retryer() aws.Retryer {
	attempts := p.attempts
	if attempts <= 0 {
		attempts = maxAttempts
	}
	delay := p.delay
	if delay == nil {
		delay = scheduleDelay
	}
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = attempts
		o.Retryables = []retry.IsErrorRetryable{retry.IsErrorRetryableFunc(retryable)}
		o.Backoff = retry.BackoffDelayerFunc(func(attempt int, err error) (time.Duration, error) {
			return delay(attempt, err), nil
		})
		o.RateLimiter = ratelimit.None
	})
}

// retryable says whether a failed attempt is retried (design 7.4): the
// statuses above, except a 429 that means a quota; a connection error before
// any answer; never the caller's cancellation or deadline, and never any
// other 4xx.
func retryable(err error) aws.Ternary {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return aws.FalseTernary
	}
	if status := httpStatus(err); status != 0 {
		if !retryStatuses[status] {
			return aws.FalseTernary
		}
		var me apiError
		if errors.As(err, &me) && Kind(status, me.ErrorCode(), me.ErrorMessage()) == llm.ErrBilling {
			return aws.FalseTernary
		}
		return aws.TrueTernary
	}
	if (retry.RetryableConnectionError{}).IsErrorRetryable(err) == aws.TrueTernary {
		return aws.TrueTernary
	}
	return aws.FalseTernary
}

// scheduleDelay is the wait before retry attempt (1 for the first retry):
// the answer's Retry-After when it has one, capped at 60 s, else 1 s then
// 4 s with ±20% jitter.
func scheduleDelay(attempt int, err error) time.Duration {
	if d, ok := retryAfter(err); ok {
		return d
	}
	base := firstDelay
	if attempt > 1 {
		base = laterDelay
	}
	return time.Duration(float64(base) * (1 - jitter + 2*jitter*rand.Float64()))
}

// retryAfter reads the answer's Retry-After in seconds, capped at 60 s.
func retryAfter(err error) (time.Duration, bool) {
	var re *awshttp.ResponseError
	if !errors.As(err, &re) || re.ResponseError == nil || re.Response == nil || re.Response.Response == nil {
		return 0, false
	}
	v := strings.TrimSpace(re.Response.Header.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	secs, perr := strconv.Atoi(v)
	if perr != nil || secs < 0 {
		return 0, false
	}
	d := time.Duration(secs) * time.Second
	if d > maxRetryAfter {
		d = maxRetryAfter
	}
	return d, true
}
