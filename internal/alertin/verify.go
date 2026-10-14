package alertin

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/owners"
)

// Header names and default windows. Grafana has no default timestamp header,
// so the contact point must set "Timestamp Header" to
// DefaultGrafanaTimestampHeader (or Config.GrafanaTimestampHeader).
const (
	DefaultGrafanaSignatureHeader = "X-Grafana-Alerting-Signature"
	DefaultGrafanaTimestampHeader = "X-Grafana-Alerting-Timestamp"
	PagerDutySignatureHeader      = "X-PagerDuty-Signature"
	DefaultDatadogTokenHeader     = "X-Piperoom-Token"
	GenericSignatureHeader        = "X-Piperoom-Signature"

	// DefaultMaxSkew bounds signed header timestamps (Grafana, generic) both ways.
	DefaultMaxSkew = 5 * time.Minute
	// DefaultMaxEventAge bounds the event time inside a signed body (PagerDuty
	// occurred_at, Datadog date). It is longer than MaxSkew because senders
	// retry failed deliveries with the original body.
	DefaultMaxEventAge = time.Hour
)

// Config holds per-source secrets and the pipeline-matching inputs. An empty
// secret disables that source. Secrets come from the caller (the room
// service reads them through internal/config); this package reads no
// environment variables.
type Config struct {
	AlertmanagerToken string // bearer token Alertmanager sends (http_config.authorization.credentials)

	GrafanaSecret          string // HMAC secret of the Grafana webhook contact point
	GrafanaSignatureHeader string // default DefaultGrafanaSignatureHeader
	GrafanaTimestampHeader string // default DefaultGrafanaTimestampHeader; required on every request

	PagerDutySecret string // V3 webhook subscription signing secret

	DatadogToken       string // value Datadog sends in DatadogTokenHeader (webhook custom header)
	DatadogTokenHeader string // default DefaultDatadogTokenHeader

	GenericSecret string // HMAC secret for the generic JSON form

	// Owners lets alerts that do not name a pipeline be matched by the
	// connector, table or consumer-group names they mention. May be nil.
	Owners *owners.Owners
	// PagerDutyServices maps a PagerDuty service ID to a pipeline id, because
	// V3 incident payloads carry no labels or custom details.
	PagerDutyServices map[string]string
	// DefaultPipeline is used when nothing else finds a pipeline. May be empty.
	DefaultPipeline string

	MaxSkew     time.Duration    // default DefaultMaxSkew
	MaxEventAge time.Duration    // default DefaultMaxEventAge
	Now         func() time.Time // default time.Now; tests override it
}

func (c Config) withDefaults() Config {
	if c.GrafanaSignatureHeader == "" {
		c.GrafanaSignatureHeader = DefaultGrafanaSignatureHeader
	}
	if c.GrafanaTimestampHeader == "" {
		c.GrafanaTimestampHeader = DefaultGrafanaTimestampHeader
	}
	if c.DatadogTokenHeader == "" {
		c.DatadogTokenHeader = DefaultDatadogTokenHeader
	}
	if c.MaxSkew <= 0 {
		c.MaxSkew = DefaultMaxSkew
	}
	if c.MaxEventAge <= 0 {
		c.MaxEventAge = DefaultMaxEventAge
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Enabled reports whether src has a secret configured.
func (c Config) Enabled(src Source) bool {
	switch src {
	case Alertmanager:
		return c.AlertmanagerToken != ""
	case Grafana:
		return c.GrafanaSecret != ""
	case PagerDuty:
		return c.PagerDutySecret != ""
	case Datadog:
		return c.DatadogToken != ""
	case Generic:
		return c.GenericSecret != ""
	}
	return false
}

// Ingest verifies a webhook from src, parses it and checks the event is not
// stale. body must be the exact bytes received. This is the one call the
// room service needs.
func (c Config) Ingest(src Source, h http.Header, body []byte) (Alert, error) {
	c = c.withDefaults()
	if len(body) > MaxBodyBytes {
		return Alert{}, fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, len(body), MaxBodyBytes)
	}
	if err := c.Verify(src, h, body); err != nil {
		return Alert{}, err
	}
	a, err := c.Parse(src, body)
	if err != nil {
		return Alert{}, err
	}
	if !a.OccurredAt.IsZero() {
		if age := c.Now().Sub(a.OccurredAt); age > c.MaxEventAge || age < -c.MaxSkew {
			return Alert{}, fmt.Errorf("%w: occurred %s ago, limit %s", ErrStale, age.Round(time.Second), c.MaxEventAge)
		}
	}
	return a, nil
}

// Verify authenticates one request from src. body must be the exact bytes
// received. It fails closed for unknown or disabled sources. The error text
// says why, for logs; it never contains a secret or an expected value.
func (c Config) Verify(src Source, h http.Header, body []byte) error {
	c = c.withDefaults()
	if !c.Enabled(src) {
		return fmt.Errorf("%w: source %q is not configured", ErrUnauthenticated, src)
	}
	switch src {
	case Alertmanager:
		return verifyBearer(h, c.AlertmanagerToken)
	case Grafana:
		return verifyGrafana(h, body, c.GrafanaSecret, c.GrafanaSignatureHeader, c.GrafanaTimestampHeader, c.Now(), c.MaxSkew)
	case PagerDuty:
		return verifyPagerDuty(h, body, c.PagerDutySecret)
	case Datadog:
		return verifyHeaderToken(h, c.DatadogTokenHeader, c.DatadogToken)
	case Generic:
		return verifyGeneric(h, body, c.GenericSecret, c.Now(), c.MaxSkew)
	}
	return fmt.Errorf("%w: unknown source %q", ErrUnauthenticated, src)
}

// SignGeneric returns the X-Piperoom-Signature value for body at time ts.
// Senders of the generic form (and tests) use it.
func SignGeneric(secret string, ts time.Time, body []byte) string {
	t := strconv.FormatInt(ts.Unix(), 10)
	return "t=" + t + ",v1=" + hex.EncodeToString(hmacSHA256(secret, []byte(t), []byte("."), body))
}

// equalSecret compares in constant time. Hashing first makes the comparison
// independent of the secret's length too.
func equalSecret(got, want string) bool {
	g := sha256.Sum256([]byte(got))
	w := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(g[:], w[:]) == 1
}

func hmacSHA256(secret string, parts ...[]byte) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

// hexMACMatches reports whether sigHex is the hex form of want, in constant time.
func hexMACMatches(sigHex string, want []byte) bool {
	got, err := hex.DecodeString(strings.TrimSpace(sigHex))
	return err == nil && hmac.Equal(got, want)
}

// checkTimestamp parses a unix timestamp (seconds, or milliseconds when it is
// that large) and checks it is within skew of now in either direction.
func checkTimestamp(ts string, now time.Time, skew time.Duration) error {
	sec, err := strconv.ParseInt(strings.TrimSpace(ts), 10, 64)
	if err != nil {
		return fmt.Errorf("%w: timestamp is not a unix time", ErrUnauthenticated)
	}
	if sec > 1e12 {
		sec /= 1000
	}
	if d := now.Sub(time.Unix(sec, 0)); d > skew || d < -skew {
		return fmt.Errorf("%w: timestamp outside allowed skew of %s", ErrUnauthenticated, skew)
	}
	return nil
}

func verifyBearer(h http.Header, token string) error {
	scheme, cred, ok := strings.Cut(h.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return fmt.Errorf("%w: missing bearer token", ErrUnauthenticated)
	}
	if !equalSecret(strings.TrimSpace(cred), token) {
		return fmt.Errorf("%w: wrong bearer token", ErrUnauthenticated)
	}
	return nil
}

func verifyHeaderToken(h http.Header, name, token string) error {
	v := h.Get(name)
	if v == "" {
		return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, name)
	}
	if !equalSecret(v, token) {
		return fmt.Errorf("%w: wrong %s token", ErrUnauthenticated, name)
	}
	return nil
}

// verifyGrafana checks Grafana's webhook HMAC: hex(HMAC-SHA256(secret,
// ts + ":" + body)). Grafana makes the timestamp optional; we require it so a
// captured request cannot be replayed after MaxSkew.
func verifyGrafana(h http.Header, body []byte, secret, sigHeader, tsHeader string, now time.Time, skew time.Duration) error {
	sig := h.Get(sigHeader)
	if sig == "" {
		return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, sigHeader)
	}
	ts := strings.TrimSpace(h.Get(tsHeader))
	if ts == "" {
		return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, tsHeader)
	}
	if err := checkTimestamp(ts, now, skew); err != nil {
		return err
	}
	if !hexMACMatches(sig, hmacSHA256(secret, []byte(ts), []byte(":"), body)) {
		return fmt.Errorf("%w: wrong grafana signature", ErrUnauthenticated)
	}
	return nil
}

// verifyPagerDuty checks a V3 webhook signature header. It may hold several
// comma-separated signatures during secret rotation; any v1 match passes.
func verifyPagerDuty(h http.Header, body []byte, secret string) error {
	v := h.Get(PagerDutySignatureHeader)
	if v == "" {
		return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, PagerDutySignatureHeader)
	}
	want := hmacSHA256(secret, body)
	for _, part := range strings.Split(v, ",") {
		ver, sig, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && ver == "v1" && hexMACMatches(sig, want) {
			return nil
		}
	}
	return fmt.Errorf("%w: no matching v1 pagerduty signature", ErrUnauthenticated)
}

// verifyGeneric checks "t=<unix>,v1=<hex>[,v1=<hex>...]" where each v1 is
// HMAC-SHA256(secret, t + "." + body). Several v1 values allow secret rotation.
func verifyGeneric(h http.Header, body []byte, secret string, now time.Time, skew time.Duration) error {
	ts, sigs, err := genericHeaderFields(h, now, skew)
	if err != nil {
		return err
	}
	want := hmacSHA256(secret, []byte(ts), []byte("."), body)
	for _, s := range sigs {
		if hexMACMatches(s, want) {
			return nil
		}
	}
	return fmt.Errorf("%w: wrong generic signature", ErrUnauthenticated)
}

// parseGenericHeader splits "t=<unix>,v1=<hex>[,v1=<hex>...]" into the first
// t value and every v1 value.
func parseGenericHeader(v string) (ts string, sigs []string) {
	for _, part := range strings.Split(v, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		switch {
		case !ok:
		case k == "t" && ts == "":
			ts = val
		case k == "v1":
			sigs = append(sigs, val)
		}
	}
	return ts, sigs
}

// genericHeaderFields checks everything about the generic signature header
// that does not need the body: present, has t= and v1=, fresh timestamp.
func genericHeaderFields(h http.Header, now time.Time, skew time.Duration) (string, []string, error) {
	v := h.Get(GenericSignatureHeader)
	if v == "" {
		return "", nil, fmt.Errorf("%w: missing %s header", ErrUnauthenticated, GenericSignatureHeader)
	}
	ts, sigs := parseGenericHeader(v)
	if ts == "" || len(sigs) == 0 {
		return "", nil, fmt.Errorf("%w: %s needs t= and v1=", ErrUnauthenticated, GenericSignatureHeader)
	}
	if err := checkTimestamp(ts, now, skew); err != nil {
		return "", nil, err
	}
	return ts, sigs, nil
}

// isSHA256Hex reports whether s (trimmed) is 64 hex digits, the shape of every
// HMAC-SHA256 signature the sources send.
func isSHA256Hex(s string) bool {
	b, err := hex.DecodeString(strings.TrimSpace(s))
	return err == nil && len(b) == sha256.Size
}

// PreVerify checks what can be checked from the headers alone, so the caller
// can refuse a request before reading its body (docs/api.md: 401 before the
// body is read). Token sources are verified in full here. HMAC sources need
// the body for the MAC, so only the presence and shape of the signature
// headers and the signed timestamp are checked; Verify (or Ingest) must still
// run on the body. Errors wrap ErrUnauthenticated.
func (c Config) PreVerify(src Source, h http.Header) error {
	c = c.withDefaults()
	if !c.Enabled(src) {
		return fmt.Errorf("%w: source %q is not configured", ErrUnauthenticated, src)
	}
	switch src {
	case Alertmanager:
		return verifyBearer(h, c.AlertmanagerToken)
	case Datadog:
		return verifyHeaderToken(h, c.DatadogTokenHeader, c.DatadogToken)
	case Grafana:
		sig := h.Get(c.GrafanaSignatureHeader)
		if sig == "" {
			return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, c.GrafanaSignatureHeader)
		}
		if !isSHA256Hex(sig) {
			return fmt.Errorf("%w: malformed %s header", ErrUnauthenticated, c.GrafanaSignatureHeader)
		}
		ts := strings.TrimSpace(h.Get(c.GrafanaTimestampHeader))
		if ts == "" {
			return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, c.GrafanaTimestampHeader)
		}
		return checkTimestamp(ts, c.Now(), c.MaxSkew)
	case PagerDuty:
		v := h.Get(PagerDutySignatureHeader)
		if v == "" {
			return fmt.Errorf("%w: missing %s header", ErrUnauthenticated, PagerDutySignatureHeader)
		}
		for _, part := range strings.Split(v, ",") {
			ver, sig, ok := strings.Cut(strings.TrimSpace(part), "=")
			if ok && ver == "v1" && isSHA256Hex(sig) {
				return nil
			}
		}
		return fmt.Errorf("%w: no well-formed v1 pagerduty signature", ErrUnauthenticated)
	case Generic:
		_, sigs, err := genericHeaderFields(h, c.Now(), c.MaxSkew)
		if err != nil {
			return err
		}
		for _, s := range sigs {
			if isSHA256Hex(s) {
				return nil
			}
		}
		return fmt.Errorf("%w: malformed %s header", ErrUnauthenticated, GenericSignatureHeader)
	}
	return fmt.Errorf("%w: unknown source %q", ErrUnauthenticated, src)
}
