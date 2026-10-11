// Package slackbridge mirrors each incident room into one Slack thread and
// turns human replies in that thread into room comments.
//
// Slack is a window onto the room, not a second control plane: replies become
// plain comments only. Steering, control and approvals stay in the room UI,
// where the driver and layer-owner checks live.
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
	"time"
)

const (
	maxResponseBytes = 4 << 20
	maxRetries       = 3
	maxRetryWait     = 30 * time.Second
)

// ErrSlack wraps an error reported by the Slack Web API (ok=false).
var ErrSlack = errors.New("slackbridge: slack api error")

// Poster posts messages to Slack. *Client implements it; tests use a fake.
type Poster interface {
	// Post sends text to channel. A non-empty threadTS makes it a thread reply.
	// It returns the message timestamp, which doubles as the thread id.
	Post(ctx context.Context, channel, threadTS, text string) (ts string, err error)
}

// Client is a minimal Slack Web API client (chat.postMessage only).
type Client struct {
	token   string
	baseURL string
	http    *http.Client
}

// NewClient builds a client. baseURL is "https://slack.com/api" in production.
// The token is never logged or included in errors.
func NewClient(token, baseURL string) *Client {
	return &Client{token: token, baseURL: baseURL, http: &http.Client{Timeout: 10 * time.Second}}
}

// Post implements Poster. It retries on HTTP 429 using Retry-After.
func (c *Client) Post(ctx context.Context, channel, threadTS, text string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"channel":      channel,
		"thread_ts":    threadTS,
		"text":         text,
		"unfurl_links": false,
		"unfurl_media": false,
	})
	if err != nil {
		return "", fmt.Errorf("slackbridge: marshal message: %w", err)
	}
	for attempt := 0; ; attempt++ {
		ts, wait, err := c.postOnce(ctx, body)
		if wait == 0 || attempt >= maxRetries-1 {
			return ts, err
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("slackbridge: waiting for rate limit: %w", ctx.Err())
		case <-time.After(wait):
		}
	}
}

// postOnce returns wait > 0 when Slack asked us to retry later.
func (c *Client) postOnce(ctx context.Context, body []byte) (string, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return "", 0, fmt.Errorf("slackbridge: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("slackbridge: chat.postMessage: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", 0, fmt.Errorf("slackbridge: read response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		secs, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		wait := time.Duration(secs) * time.Second
		if wait <= 0 {
			wait = time.Second
		}
		return "", min(wait, maxRetryWait), fmt.Errorf("slackbridge: rate limited (retry after %s)", wait)
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("slackbridge: chat.postMessage: HTTP %d", resp.StatusCode)
	}
	var out struct {
		OK    bool   `json:"ok"`
		TS    string `json:"ts"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", 0, fmt.Errorf("slackbridge: decode response: %w", err)
	}
	if !out.OK {
		return "", 0, fmt.Errorf("%w: %s", ErrSlack, out.Error)
	}
	return out.TS, 0, nil
}
