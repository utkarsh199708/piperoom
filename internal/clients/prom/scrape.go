package prom

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"piperoom/internal/tools"
)

// DefaultMaxBytes is the body cap of a scrape (AGENTS.md rule 7).
const DefaultMaxBytes = 4 << 20

// ErrTooLarge is wrapped by Scrape when the body exceeds the cap.
var ErrTooLarge = errors.New("prom: response too large")

// Client scrapes metrics endpoints with one *http.Client.
type Client struct {
	hc *http.Client
}

// NewClient returns a Client with a 10 s timeout that follows a redirect only
// within the same host, at most three times.
func NewClient() *Client {
	return &Client{hc: &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("prom: too many redirects")
			}
			if req.URL.Host != via[0].URL.Host {
				return errors.New("prom: redirect to another host refused")
			}
			return nil
		},
	}}
}

// Scrape GETs rawURL and parses the body. A body over maxBytes (0 =
// DefaultMaxBytes) is an error wrapping ErrTooLarge; a non-200 status is an
// error. The call is recorded as a provenance call with the host only: the URL
// path and query may carry a token.
func (c *Client) Scrape(ctx context.Context, rawURL string, maxBytes int64) (_ []Sample, err error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("prom: want an http(s) URL")
	}
	start := time.Now()
	defer func() {
		tools.Record(ctx, tools.Call{
			Kind: tools.CallHTTP, Target: u.Host, Text: "GET " + u.Path,
			At: start, TookMS: time.Since(start).Milliseconds(),
		})
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("prom: build request: %w", err)
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := c.hc.Do(req)
	if err != nil {
		// The client error embeds the URL; report the host only.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("prom: scrape %s: %w", u.Host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prom: scrape %s: status %d", u.Host, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("prom: scrape %s: read: %w", u.Host, err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: more than %d MB from %s", ErrTooLarge, maxBytes>>20, u.Host)
	}
	return Parse(bytes.NewReader(body))
}
