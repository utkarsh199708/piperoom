package slackbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
)

const (
	maxResponseBytes = 4 << 20
	attemptTimeout   = 10 * time.Second
	maxAttempts      = 5
	maxBackoff       = 30 * time.Second
	maxRetryAfter    = 120 * time.Second
	// minGap is Slack's "about one message per second per channel" guidance for
	// chat.postMessage (https://docs.slack.dev/apis/web-api/rate-limits).
	minGap = time.Second
)

// ErrRateLimited means Slack asked us to wait longer than we are willing to.
var ErrRateLimited = errors.New("slackbridge: rate limited by slack")

// APIError is a Slack Web API reply with ok=false. Code is Slack's error code
// (https://docs.slack.dev/reference/methods/chat.postMessage#errors).
type APIError struct{ Code string }

func (e *APIError) Error() string { return "slackbridge: chat.postMessage: slack error " + e.Code }

type textObj struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Verbatim bool   `json:"verbatim"`
}

type block struct {
	Type     string    `json:"type"` // "section" or "context"
	Text     *textObj  `json:"text,omitempty"`
	Elements []textObj `json:"elements,omitempty"`
}

// message is one chat.postMessage call. text is the notification fallback.
type message struct {
	Channel        string
	Text           string
	Blocks         []block
	ThreadTS       string
	ReplyBroadcast bool
}

type postResult struct{ Channel, TS string }

// client calls chat.postMessage, the only Slack method the bridge uses.
type client struct {
	hc    *http.Client
	base  string
	token netsec.Secret

	now   func() time.Time
	sleep func(context.Context, time.Duration) error

	mu   sync.Mutex
	next time.Time // earliest start of the next post, across all rooms
}

func newClient(hc *http.Client, base string, token netsec.Secret) *client {
	return &client{hc: hc, base: base, token: token, now: time.Now, sleep: sleepCtx}
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

// pace reserves the next send slot under the lock and sleeps outside it, so a
// room waiting for its slot never blocks another room's cancelled ctx.
func (c *client) pace(ctx context.Context) error {
	c.mu.Lock()
	now := c.now()
	start := c.next
	if start.Before(now) {
		start = now
	}
	c.next = start.Add(minGap)
	c.mu.Unlock()
	if d := start.Sub(now); d > 0 {
		return c.sleep(ctx, d)
	}
	return nil
}

// post sends m. Only HTTP 429, 5xx and Slack's own rate-limit codes are
// retried: those mean the message was not accepted. After a transport error
// or timeout it may have been, and chat.postMessage is not idempotent, so
// that error is returned as is.
func (c *client) post(ctx context.Context, m message) (postResult, error) {
	fields := map[string]any{
		"channel":         m.Channel,
		"text":            m.Text,
		"blocks":          m.Blocks,
		"reply_broadcast": m.ReplyBroadcast,
		"unfurl_links":    false,
		"unfurl_media":    false,
		"parse":           "none",
	}
	if m.ThreadTS != "" { // only replies carry it, and always the parent ts
		fields["thread_ts"] = m.ThreadTS
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return postResult{}, fmt.Errorf("slackbridge: encode message: %w", err)
	}
	for attempt := 1; ; attempt++ {
		if err := c.pace(ctx); err != nil {
			return postResult{}, fmt.Errorf("slackbridge: chat.postMessage: waiting for send slot: %w", err)
		}
		o := c.once(ctx, body)
		if !o.retry {
			return o.res, o.err
		}
		wait := backoff(attempt)
		if o.retryAfter != "" {
			if secs, err := strconv.Atoi(o.retryAfter); err == nil && secs > 0 {
				wait = time.Duration(secs) * time.Second
				if wait > maxRetryAfter {
					return postResult{}, fmt.Errorf("%w: retry-after %ds is too long: %w", ErrRateLimited, secs, o.err)
				}
			}
		}
		if attempt >= maxAttempts {
			return postResult{}, fmt.Errorf("slackbridge: chat.postMessage: gave up after %d attempts: %w", attempt, o.err)
		}
		if err := c.sleep(ctx, wait); err != nil {
			return postResult{}, fmt.Errorf("slackbridge: chat.postMessage: waiting to retry: %w", err)
		}
	}
}

func backoff(attempt int) time.Duration {
	d := time.Second << (attempt - 1)
	if d > maxBackoff || d <= 0 {
		return maxBackoff
	}
	return d
}

type outcome struct {
	res        postResult
	err        error
	retry      bool
	retryAfter string
}

func (c *client) once(ctx context.Context, body []byte) outcome {
	ctx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return outcome{err: fmt.Errorf("slackbridge: chat.postMessage: build request: %w", err)}
	}
	// Slack takes the token as a bearer header on JSON calls, never in the body.
	req.Header.Set("Authorization", "Bearer "+c.token.Reveal())
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.hc.Do(req)
	if err != nil {
		return outcome{err: fmt.Errorf("slackbridge: chat.postMessage: %w", err)}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return outcome{err: fmt.Errorf("slackbridge: chat.postMessage: read response: %w", err)}
	}
	ra := resp.Header.Get("Retry-After")
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return outcome{retry: true, retryAfter: ra, err: fmt.Errorf("slackbridge: chat.postMessage: HTTP 429: %w", ErrRateLimited)}
	case resp.StatusCode >= 500:
		return outcome{retry: true, retryAfter: ra, err: fmt.Errorf("slackbridge: chat.postMessage: HTTP %d", resp.StatusCode)}
	case resp.StatusCode != http.StatusOK:
		return outcome{err: fmt.Errorf("slackbridge: chat.postMessage: HTTP %d", resp.StatusCode)}
	}
	var out struct {
		OK      bool   `json:"ok"`
		Error   string `json:"error"`
		Channel string `json:"channel"`
		TS      string `json:"ts"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return outcome{err: fmt.Errorf("slackbridge: chat.postMessage: decode response: %w", err)}
	}
	if !out.OK {
		apiErr := &APIError{Code: out.Error}
		rl := out.Error == "ratelimited" || out.Error == "rate_limited"
		return outcome{err: apiErr, retry: rl, retryAfter: ra}
	}
	if out.TS == "" {
		return outcome{err: errors.New("slackbridge: chat.postMessage: ok reply without ts")}
	}
	return outcome{res: postResult{Channel: out.Channel, TS: out.TS}}
}
