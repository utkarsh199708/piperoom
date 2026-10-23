// Package workerlogs reads a Kafka Connect worker's log lines from Loki's
// query_range API for the connect_worker_logs tool (Q3c).
//
// The LogQL stream selector comes from the operator's configuration
// (WORKER_LOGS_SELECTOR) and never from the model. The model names a
// connector, a task, a minimum level and a time window; this package adds
// only fixed line filters for them, so no model text reaches LogQL except as
// a quoted literal inside a filter.
//
// The text of a log line is untrusted. This package keeps it as Loki returned
// it (cut to MaxLineBytes); the tool redacts and scans it before it reaches a
// prompt (AGENTS.md section 7).
package workerlogs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
)

// Limits of one read.
const (
	// MaxLimit is the most lines one read asks Loki for and returns.
	MaxLimit = 200
	// MaxResponseBytes caps the response body (AGENTS.md rule 7).
	MaxResponseBytes = 4 << 20
	// DefaultTimeout bounds one read (AGENTS.md rule 4).
	DefaultTimeout = 10 * time.Second
	// MaxLineBytes caps one line's text before the tool redacts it. A longer
	// line is cut with a marker; a secret that straddles the cut is not
	// masked, which is why the cap is well above any line Connect writes.
	MaxLineBytes = 8 << 10
	// DefaultWindow is the window of a query without Since.
	DefaultWindow = 15 * time.Minute
	// MinWindow and MaxWindow bound Since: an hour of a busy worker is more
	// than the 200-line cap shows, and a day is longer than Loki keeps.
	MinWindow = time.Minute
	MaxWindow = 24 * time.Hour
)

// DefaultSelector is the stream selector of the dev stack's logs profile
// (dev/logs/promtail.yml labels every Connect line service="connect").
const DefaultSelector = `{service="connect"}`

// Errors the tool names in its messages.
var (
	// ErrUnauthorized is returned for 401 and 403: Loki refused the token.
	ErrUnauthorized = errors.New("workerlogs: Loki refused the credentials")
	// ErrNotFound is returned for 404: the base URL is not a Loki root.
	ErrNotFound = errors.New("workerlogs: Loki endpoint not found")
	// ErrTooLarge is returned when the body exceeds MaxResponseBytes.
	ErrTooLarge = errors.New("workerlogs: response too large")
	// ErrBadResponse is returned when the body is not a Loki streams answer.
	ErrBadResponse = errors.New("workerlogs: unexpected Loki response")
)

// Config is the operator's Loki settings. URL "" means "not configured".
type Config struct {
	// URL is WORKER_LOGS_LOKI_URL: the Loki base URL, e.g.
	// http://localhost:3100 (dev) or https://logs-prod-012.grafana.net.
	URL string
	// Selector is WORKER_LOGS_SELECTOR: the fixed LogQL stream selector,
	// e.g. {service="connect"}. Never taken from the model.
	Selector string
	// Token is WORKER_LOGS_TOKEN: a bearer token for a Loki that needs one.
	// It prints and logs as "[redacted]".
	Token netsec.Secret
}

// LogQuery is one worker-log read. The model supplies Connector, Task,
// MinLevel and Since; Limit is the caller's cap.
type LogQuery struct {
	Connector string
	Task      int    // -1 = any task
	MinLevel  string // TRACE, DEBUG, INFO, WARN, ERROR or FATAL; "" = WARN
	Since     time.Time
	Limit     int // 1 to MaxLimit; 0 = MaxLimit
}

// LogLine is one log entry. Text is untrusted: the tool redacts it.
type LogLine struct {
	At     time.Time `json:"at"`
	Level  string    `json:"level"`
	Logger string    `json:"logger,omitempty"`
	Worker string    `json:"worker,omitempty"`
	Text   string    `json:"text"` // untrusted
}

// Reader is the read-only log reader connect_worker_logs depends on. Q0 item
// 3 named it clients.WorkerLogReader; that contract addition did not merge, so
// the type lives here (see the Q3c PR's contract change request).
type Reader interface {
	WorkerLogs(ctx context.Context, q LogQuery) ([]LogLine, error)
}

// ParseConfig trims and validates the three settings. An empty URL means "not
// configured" and allows no token. A token is sent only over https or to a
// loopback host, so it is never in clear text on the wire. The selector must
// be a bare LogQL stream selector: a pipeline stage in it would let the
// operator (not the model) run any query, and this check keeps that failure
// loud at startup instead of at the first read.
func ParseConfig(rawURL, selector, token string) (Config, error) {
	c := Config{
		URL:      strings.TrimSpace(rawURL),
		Selector: strings.TrimSpace(selector),
		Token:    netsec.Secret(strings.TrimSpace(token)),
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// ConfigFromEnv reads WORKER_LOGS_LOKI_URL, WORKER_LOGS_SELECTOR and
// WORKER_LOGS_TOKEN from the process environment.
//
// It is a local adapter: internal/config/config.go (a contract file) has no
// field for these settings, and Q0 item 3's ParseWorkerLogs did not land. The
// wiring row (Q3e) calls this until the contract carries them; see the Q3c
// PR's contract change request.
func ConfigFromEnv() (Config, error) {
	return ParseConfig(os.Getenv("WORKER_LOGS_LOKI_URL"), os.Getenv("WORKER_LOGS_SELECTOR"), os.Getenv("WORKER_LOGS_TOKEN"))
}

// Validate checks the settings and fills the default selector. It is called by
// New, so a Config built by hand is checked the same way.
func (c *Config) Validate() error {
	c.URL = strings.TrimSpace(c.URL)
	c.Selector = strings.TrimSpace(c.Selector)
	c.Token = netsec.Secret(strings.TrimSpace(c.Token.Reveal()))
	if c.Selector == "" {
		c.Selector = DefaultSelector
	}
	if err := validateSelector(c.Selector); err != nil {
		return fmt.Errorf("config: WORKER_LOGS_SELECTOR: %w", err)
	}
	if c.URL == "" {
		if c.Token != "" {
			return fmt.Errorf("config: WORKER_LOGS_TOKEN is set but WORKER_LOGS_LOKI_URL is empty")
		}
		return nil
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("config: WORKER_LOGS_LOKI_URL: want an http(s)://host[:port] URL")
	}
	if u.User != nil {
		return fmt.Errorf("config: WORKER_LOGS_LOKI_URL: must not carry user info; put the token in WORKER_LOGS_TOKEN")
	}
	if u.Scheme == "http" && c.Token != "" && !loopbackHost(u.Hostname()) {
		return fmt.Errorf("config: WORKER_LOGS_LOKI_URL: WORKER_LOGS_TOKEN is sent only over https, or http to a loopback host")
	}
	return nil
}

// validateSelector accepts only "{label<op>\"value\",...}" with op one of =,
// !=, =~ and !~. That is every stream selector Loki takes and nothing else:
// no pipeline stages (|), no spaceship operators, no comments.
func validateSelector(s string) error {
	if s == "" {
		return errors.New("empty; want a LogQL stream selector such as {service=\"connect\"}")
	}
	if len(s) > 512 {
		return errors.New("longer than 512 bytes")
	}
	if s[0] != '{' || s[len(s)-1] != '}' {
		return fmt.Errorf("%q: want a stream selector starting with { and ending with }", s)
	}
	if strings.Count(s, "{") != 1 || strings.Count(s, "}") != 1 {
		return fmt.Errorf("%q: nested braces are not a stream selector", s)
	}
	// The inside must be matchers only: names, operators, quoted values and
	// commas. This refuses a pipeline stage smuggled in with a '|'.
	inside := s[1 : len(s)-1]
	if strings.TrimSpace(inside) == "" {
		return fmt.Errorf("%q: a stream selector needs at least one matcher", s)
	}
	inQuote, escaped, equals, name := false, false, 0, false
	for _, r := range inside {
		switch {
		case escaped:
			escaped = false
			continue
		case r == '\\' && inQuote:
			escaped = true
			continue
		case r == '"':
			inQuote = !inQuote
			continue
		case inQuote:
			continue
		case r == '=':
			equals++
			name = false
		case r == '!' || r == '~':
			if !name {
				return fmt.Errorf("%q: unexpected %q in a matcher", s, r)
			}
		case r == ',':
			name = false
		case r == ' ' || r == '\t':
		default:
			if !isNameRune(r) {
				return fmt.Errorf("%q: %q is not part of a label matcher", s, r)
			}
			name = true
		}
	}
	if inQuote {
		return fmt.Errorf("%q: unterminated label value", s)
	}
	if equals == 0 {
		return fmt.Errorf("%q: a stream selector needs at least one matcher", s)
	}
	return nil
}

func isNameRune(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// sortNewestFirst orders lines by time, newest first, keeping Loki's order
// for equal timestamps.
func sortNewestFirst(lines []LogLine) {
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].At.After(lines[j].At) })
}
