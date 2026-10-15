// Package notify delivers owner.paged outside the app (docs/api.md, Q-wave
// events: Pages; QA G2): one webhook POST per page, signed when a secret is
// set. The room service calls it after the event is written, on a goroutine of
// its own; a failed delivery is logged and never fails the page.
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Webhook headers and the event name (docs/api.md, Pages).
const (
	EventOwnerPaged = "owner.paged"
	HeaderEvent     = "X-Piperoom-Event"
	HeaderSignature = "X-Piperoom-Signature"
)

// DefaultTimeout bounds one POST attempt (docs/api.md: 5 s).
const DefaultTimeout = 5 * time.Second

// defaultRetryWait is the pause before the one retry.
const defaultRetryWait = time.Second

// maxResponseBytes caps how much of a response body is read (and thrown away)
// so the connection can be reused; nothing of it is kept or logged.
const maxResponseBytes = 64 << 10

// ErrDelivery is wrapped by every error Deliver returns. The error text names
// only the status code or the kind of failure, never the URL (it may carry a
// token), the body or the secret.
var ErrDelivery = errors.New("notify: webhook delivery failed")

// Owner is one paged person in the webhook body. Name falls back to the id;
// Email is set only when users.yaml or the IdP has one.
type Owner struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email,omitempty"`
}

// OwnerPaged is the webhook body of one owner.paged event (docs/api.md).
// Deliver sets Type and RoomURL; the caller fills the rest from the event.
type OwnerPaged struct {
	Type      string `json:"type"`
	RoomID    string `json:"room_id"`
	RoomTitle string `json:"room_title"`
	// TitleFlags are the room's title_flags (injectscan kinds of the title):
	// a flagged title is sent as it is, so the relay must see the flag before
	// it shows the title. [] when clean, never null. (Q5 T9)
	TitleFlags []string  `json:"title_flags"`
	RoomURL    string    `json:"room_url,omitempty"`
	PipelineID string    `json:"pipeline_id"`
	Layer      string    `json:"layer"`
	Owners     []Owner   `json:"owners"`
	Reason     string    `json:"reason"`
	ProposalID string    `json:"proposal_id"`
	Auto       bool      `json:"auto"`
	At         time.Time `json:"at"`
	Seq        int64     `json:"seq"`
}

// WebhookConfig configures a Webhook (config.Notify plus PIPEROOM_PUBLIC_URL).
type WebhookConfig struct {
	// URL is PIPEROOM_NOTIFY_WEBHOOK_URL, already checked by config.ParseNotify.
	URL string
	// Secret signs the body (X-Piperoom-Signature); "" = unsigned.
	Secret string
	// PublicURL is PIPEROOM_PUBLIC_URL; the body's room_url is PublicURL +
	// "/rooms/{id}", and is left out when PublicURL is "".
	PublicURL string
	// Timeout bounds one attempt; 0 = DefaultTimeout.
	Timeout time.Duration
	// RetryWait is the pause before the retry; 0 = one second.
	RetryWait time.Duration
	// Logger gets one line per failed attempt; nil = slog.Default().
	Logger *slog.Logger
}

// Webhook POSTs owner.paged bodies to one URL. It is safe for concurrent use.
type Webhook struct {
	url       string
	publicURL string
	secret    []byte
	client    *http.Client
	retryWait time.Duration
	log       *slog.Logger
}

// NewWebhook builds a Webhook with one *http.Client. The URL must be absolute
// http(s); config.ParseNotify has applied the stricter rules already.
func NewWebhook(c WebhookConfig) (*Webhook, error) {
	raw := strings.TrimSpace(c.URL)
	u, err := url.Parse(raw)
	// The error never quotes the URL: it may carry a token in its path.
	if raw == "" || err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("notify: webhook URL must be an absolute http or https URL")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	wait := c.RetryWait
	if wait <= 0 {
		wait = defaultRetryWait
	}
	log := c.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Webhook{
		url:       raw,
		publicURL: strings.TrimRight(strings.TrimSpace(c.PublicURL), "/"),
		secret:    []byte(c.Secret),
		client: &http.Client{
			Timeout: timeout,
			// A redirect would send the signed body somewhere the operator
			// did not configure; it counts as a failed delivery instead.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		retryWait: wait,
		log:       log,
	}, nil
}

// Signature is the X-Piperoom-Signature value of body: "sha256=" and the hex
// HMAC-SHA256 of the body with secret. A receiver recomputes it to verify.
func Signature(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

// Deliver POSTs p, retrying once on a 5xx answer or a network error (a
// timeout included). Any other answer outside 2xx fails at once. It returns
// nil on a 2xx; otherwise an error wrapping ErrDelivery. ctx bounds the whole
// delivery; each attempt also has the client timeout.
func (w *Webhook) Deliver(ctx context.Context, p OwnerPaged) error {
	p.Type = EventOwnerPaged
	if p.Owners == nil {
		p.Owners = []Owner{}
	}
	if p.TitleFlags == nil {
		p.TitleFlags = []string{}
	}
	p.RoomURL = ""
	if w.publicURL != "" && p.RoomID != "" {
		p.RoomURL = w.publicURL + "/rooms/" + url.PathEscape(p.RoomID)
	}
	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("%w: encode body: %w", ErrDelivery, err)
	}
	sig := ""
	if len(w.secret) > 0 {
		sig = Signature(w.secret, body)
	}
	const attempts = 2
	for attempt := 1; ; attempt++ {
		status, retry, err := w.post(ctx, body, sig)
		if err == nil {
			w.log.Debug("notify: webhook delivered", "room", p.RoomID, "layer", p.Layer, "status", status)
			return nil
		}
		// Status code and failure kind only: never the URL, body or secret.
		w.log.Warn("notify: webhook delivery failed", "room", p.RoomID, "layer", p.Layer, "seq", p.Seq,
			"attempt", attempt, "status", status, "err", err)
		if !retry || attempt >= attempts {
			return err
		}
		t := time.NewTimer(w.retryWait)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("%w: cancelled", ErrDelivery)
		case <-t.C:
		}
	}
}

// post makes one attempt. status is 0 when no answer came; retry says whether
// the failure is one the single retry is for.
func (w *Webhook) post(ctx context.Context, body []byte, sig string) (status int, retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return 0, false, fmt.Errorf("%w: build request", ErrDelivery)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderEvent, EventOwnerPaged)
	if sig != "" {
		req.Header.Set(HeaderSignature, sig)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		// The *url.Error text quotes the URL, so only its kind is kept.
		if ctx.Err() != nil {
			return 0, false, fmt.Errorf("%w: cancelled", ErrDelivery)
		}
		kind := "network error"
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			kind = "timeout"
		}
		return 0, true, fmt.Errorf("%w: %s", ErrDelivery, kind)
	}
	// Drain a bounded part so the connection can be reused, then close.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return resp.StatusCode, false, nil
	case resp.StatusCode >= 500:
		return resp.StatusCode, true, fmt.Errorf("%w: status %d", ErrDelivery, resp.StatusCode)
	default:
		return resp.StatusCode, false, fmt.Errorf("%w: status %d", ErrDelivery, resp.StatusCode)
	}
}
