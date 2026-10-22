package alertin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// ChangeWebhookPath is the route the room service mounts ChangeWebhook on
// (docs/api.md, External changes).
const ChangeWebhookPath = "/api/changes/webhook"

// Limits of the change webhook besides config.ChangesWebhookMaxBodyBytes and
// config.ChangesWebhookPerMinute.
const (
	// MaxChangeTitleRunes bounds the title after control characters are removed.
	MaxChangeTitleRunes = 300
	// MaxChangeFieldBytes bounds actor and the sender's id.
	MaxChangeFieldBytes = 128
	// MaxChangeAge is how old a change's at may be; a later one is refused.
	MaxChangeAge = 7 * 24 * time.Hour
	// DefaultChangeStoreTimeout bounds the store write.
	DefaultChangeStoreTimeout = 10 * time.Second
)

// ChangeWebhookConfig configures ChangeWebhook. Secret, Owners and Writer
// come from the caller (cmd/server); this package reads no environment.
type ChangeWebhookConfig struct {
	// Secret is CHANGES_WEBHOOK_SECRET: one secret or several separated by
	// commas while a sender rotates. Empty disables the route (every request 401).
	Secret string
	// Owners names the pipelines a change may be recorded on.
	Owners *owners.Owners
	// Writer stores the row (the room store).
	Writer clients.ChangeWriter
	Log    *slog.Logger

	PerMinute    int              // default config.ChangesWebhookPerMinute
	MaxSkew      time.Duration    // signature timestamp window both ways; default DefaultMaxSkew
	StoreTimeout time.Duration    // default DefaultChangeStoreTimeout
	Now          func() time.Time // default time.Now
}

// ChangeWebhook is the handler of POST /api/changes/webhook. It records a
// deploy, migration or vendor incident in the pipeline's change log; it opens
// no room and wakes no agent.
type ChangeWebhook struct {
	cfg     ChangeWebhookConfig
	secrets []string
	limit   *perMinuteLimiter
	warn    *throttledWarn
}

// NewChangeWebhook builds the handler. A nil Writer or Owners leaves the route
// answering 401 like an empty secret, so a half-configured server stores nothing.
func NewChangeWebhook(cfg ChangeWebhookConfig) *ChangeWebhook {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.PerMinute <= 0 {
		cfg.PerMinute = config.ChangesWebhookPerMinute
	}
	if cfg.MaxSkew <= 0 {
		cfg.MaxSkew = DefaultMaxSkew
	}
	if cfg.StoreTimeout <= 0 {
		cfg.StoreTimeout = DefaultChangeStoreTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	var secrets []string
	if cfg.Writer != nil && cfg.Owners != nil {
		for _, s := range strings.Split(cfg.Secret, ",") {
			if s = strings.TrimSpace(s); s != "" {
				secrets = append(secrets, s)
			}
		}
	}
	return &ChangeWebhook{
		cfg:     cfg,
		secrets: secrets,
		limit:   &perMinuteLimiter{max: cfg.PerMinute, seen: map[string][]time.Time{}},
		warn:    &throttledWarn{every: time.Minute, last: map[string]time.Time{}},
	}
}

// Enabled reports whether a secret is configured (for the start-up log line).
func (h *ChangeWebhook) Enabled() bool { return len(h.secrets) > 0 }

// ChangeRequest is the webhook body (docs/api.md). Every string is untrusted
// sender text.
type ChangeRequest struct {
	PipelineID string `json:"pipeline_id"`
	Kind       string `json:"kind"`
	At         string `json:"at"`
	Title      string `json:"title"`
	Service    string `json:"service"`
	URL        string `json:"url,omitempty"`
	Actor      string `json:"actor,omitempty"`
	Status     string `json:"status,omitempty"`
	ID         string `json:"id,omitempty"`
}

// ServeHTTP checks, in this order: secret configured (401), body size (413),
// signature and timestamp (401), JSON shape (400), pipeline (404), rate per
// pipeline (429), fields (400). Then it writes one row and answers
// 202 {"stored": bool}. Logs carry the pipeline, the kind and whether a row
// was stored, never sender text or a secret.
func (h *ChangeWebhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if len(h.secrets) == 0 {
		h.refused(r, "webhook is not configured")
		writeChangeError(w, http.StatusUnauthorized, "unauthenticated", "change authentication failed")
		return
	}
	limit := int64(config.ChangesWebhookMaxBodyBytes)
	if r.ContentLength > limit {
		writeChangeError(w, http.StatusRequestEntityTooLarge, "too_large", "change body too large")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeChangeError(w, http.StatusRequestEntityTooLarge, "too_large", "change body too large")
			return
		}
		writeChangeError(w, http.StatusBadRequest, "validation", "unreadable request body")
		return
	}
	now := h.cfg.Now()
	if err := verifyRotated(r.Header, body, h.secrets, now, h.cfg.MaxSkew); err != nil {
		h.refused(r, err.Error())
		writeChangeError(w, http.StatusUnauthorized, "unauthenticated", "change authentication failed")
		return
	}
	req, err := decodeChangeRequest(body)
	if err != nil {
		writeChangeError(w, http.StatusBadRequest, "validation", err.Error())
		return
	}
	if req.PipelineID == "" {
		writeChangeError(w, http.StatusBadRequest, "validation", "pipeline_id is required")
		return
	}
	if !ValidPipelineID(req.PipelineID) {
		writeChangeError(w, http.StatusNotFound, "not_found", "unknown pipeline")
		return
	}
	if _, ok := h.cfg.Owners.Pipeline(req.PipelineID); !ok {
		writeChangeError(w, http.StatusNotFound, "not_found", "unknown pipeline")
		return
	}
	if !h.limit.allow(req.PipelineID, now) {
		h.cfg.Log.Warn("changes: webhook rate limited", "pipeline", req.PipelineID)
		writeChangeError(w, http.StatusTooManyRequests, "rate_limited", "too many changes for this pipeline; retry in a minute")
		return
	}
	c, err := req.Change(now)
	if err != nil {
		writeChangeError(w, http.StatusBadRequest, "validation", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.StoreTimeout)
	defer cancel()
	stored, err := h.cfg.Writer.RecordChange(ctx, c)
	if err != nil {
		// A store error carries no sender text (NormalizeChange names the kind
		// only, which is one of three constants here).
		h.cfg.Log.Error("changes: webhook store", "pipeline", c.PipelineID, "kind", c.Kind, "err", err)
		writeChangeError(w, http.StatusInternalServerError, "internal_error", "could not store the change")
		return
	}
	h.cfg.Log.Info("changes: webhook", "pipeline", c.PipelineID, "kind", c.Kind, "stored", stored)
	writeChangeJSON(w, http.StatusAccepted, map[string]bool{"stored": stored})
}

// refused logs a refused request at Warn once a minute, the rest at Debug:
// anyone who can reach the port can cause them.
func (h *ChangeWebhook) refused(r *http.Request, reason string) {
	level := slog.LevelDebug
	if h.warn.due("refused", h.cfg.Now()) {
		level = slog.LevelWarn
	}
	h.cfg.Log.Log(r.Context(), level, "changes: webhook refused", "remote", r.RemoteAddr, "reason", reason)
}

// verifyRotated accepts the generic alert signature under any of secrets.
func verifyRotated(hd http.Header, body []byte, secrets []string, now time.Time, skew time.Duration) error {
	var err error
	for _, s := range secrets {
		if err = verifyGeneric(hd, body, s, now, skew); err == nil {
			return nil
		}
	}
	return err
}

// decodeChangeRequest reads exactly one JSON object with only the documented fields.
func decodeChangeRequest(body []byte) (ChangeRequest, error) {
	var req ChangeRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return ChangeRequest{}, errors.New("body must be one JSON object with the documented fields only")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return ChangeRequest{}, errors.New("body must be one JSON object")
	}
	return req, nil
}

// Change validates the request at now and returns the row to store. The error
// names the field, never its value. It refuses rather than fixes a value up,
// except that control characters are removed from the title.
func (q ChangeRequest) Change(now time.Time) (clients.PipelineChange, error) {
	switch q.Kind {
	case clients.ChangeDeploy, clients.ChangeMigration, clients.ChangeVendorIncident:
	case "":
		return clients.PipelineChange{}, errors.New("kind is required")
	default:
		return clients.PipelineChange{}, errors.New("kind must be deploy, migration or vendor_incident")
	}
	at, err := time.Parse(time.RFC3339, q.At)
	if err != nil {
		return clients.PipelineChange{}, errors.New("at must be an RFC 3339 time")
	}
	if at.After(now.Add(DefaultMaxSkew)) {
		return clients.PipelineChange{}, errors.New("at is more than 5 minutes in the future")
	}
	if at.Before(now.Add(-MaxChangeAge)) {
		return clients.PipelineChange{}, errors.New("at is older than 7 days")
	}
	title := strings.TrimSpace(cleanChangeText(q.Title))
	if title == "" {
		return clients.PipelineChange{}, errors.New("title is required")
	}
	if utf8.RuneCountInString(title) > MaxChangeTitleRunes {
		return clients.PipelineChange{}, fmt.Errorf("title is over %d characters", MaxChangeTitleRunes)
	}
	if strings.TrimSpace(q.Service) == "" {
		return clients.PipelineChange{}, errors.New("service is required")
	}
	for _, f := range []struct{ name, v string }{{"actor", q.Actor}, {"id", q.ID}} {
		if len(f.v) > MaxChangeFieldBytes {
			return clients.PipelineChange{}, fmt.Errorf("%s is over %d bytes", f.name, MaxChangeFieldBytes)
		}
		if strings.ContainsFunc(f.v, unicode.IsControl) {
			return clients.PipelineChange{}, fmt.Errorf("%s holds control characters", f.name)
		}
	}
	switch {
	case q.Status == "":
	case q.Kind != clients.ChangeVendorIncident:
		return clients.PipelineChange{}, errors.New("status is for vendor_incident only")
	case q.Status != clients.VendorInvestigating && q.Status != clients.VendorIdentified &&
		q.Status != clients.VendorMonitoring && q.Status != clients.VendorResolved:
		return clients.PipelineChange{}, errors.New("status must be investigating, identified, monitoring or resolved")
	}
	ext := clients.ExternalChange{URL: q.URL, Service: q.Service, Status: q.Status}
	if err := ext.Validate(); err != nil {
		// The contract's messages name the field and the rule, not the value.
		return clients.PipelineChange{}, errors.New(strings.TrimPrefix(err.Error(), "clients: external change "))
	}
	after, err := json.Marshal(ext)
	if err != nil {
		return clients.PipelineChange{}, fmt.Errorf("alertin: encode change: %w", err)
	}
	return clients.PipelineChange{
		PipelineID:  q.PipelineID,
		Kind:        q.Kind,
		Object:      q.Service,
		At:          at,
		Actor:       q.Actor,
		Source:      clients.ChangeSourceWebhook,
		Fingerprint: changeFingerprint(q.Kind, q.Service, q.ID, at, title),
		After:       after,
		Summary:     title,
	}, nil
}

// changeFingerprint is the dedup key (docs/api.md): kind, service and the
// sender's id when it sent one, else kind, service, at and title. The parts
// are JSON-encoded so no two different inputs join to the same bytes.
func changeFingerprint(kind, service, id string, at time.Time, title string) string {
	parts := []string{"id", kind, service, id}
	if id == "" {
		parts = []string{"at", kind, service, at.UTC().Format(time.RFC3339Nano), title}
	}
	return hashParts(parts...)
}

func hashParts(parts ...string) string {
	b, _ := json.Marshal(parts) // a []string always encodes
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// cleanChangeText makes untrusted text one line: tabs and line breaks (the
// Unicode separators too) become spaces and other control characters are
// removed, as the store does.
func cleanChangeText(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t' || r == '\u2028' || r == '\u2029':
			return ' '
		case unicode.IsControl(r):
			return -1
		}
		return r
	}, strings.ToValidUTF8(s, ""))
}

// perMinuteLimiter allows max events per key in any sliding minute. Keys are
// owners.yaml pipeline ids only, so the map is bounded by the owners file.
type perMinuteLimiter struct {
	max  int
	mu   sync.Mutex
	seen map[string][]time.Time
}

func (l *perMinuteLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := l.seen[key]
	cut := now.Add(-time.Minute)
	i := 0
	for i < len(ts) && !ts[i].After(cut) {
		i++
	}
	ts = ts[i:]
	if len(ts) >= l.max {
		l.seen[key] = ts
		return false
	}
	l.seen[key] = append(ts, now)
	return true
}

// throttledWarn says whether a warning for key is due: at most one per every.
type throttledWarn struct {
	every time.Duration
	mu    sync.Mutex
	last  map[string]time.Time
}

func (t *throttledWarn) due(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if last, ok := t.last[key]; ok && now.Sub(last) < t.every {
		return false
	}
	t.last[key] = now
	return true
}

// writeChangeJSON and writeChangeError write the room service's JSON and
// error envelope ({"error": {"code", "message"}}, docs/api.md).
func writeChangeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeChangeError(w http.ResponseWriter, status int, code, msg string) {
	writeChangeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}
