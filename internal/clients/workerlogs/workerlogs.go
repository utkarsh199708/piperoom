// Package workerlogs reads Kafka Connect worker log lines from Grafana Loki
// (GET /loki/api/v1/query_range). The stream selector comes from the
// operator's configuration only; a caller passes a connector, a task, a
// minimum level and a start time, which become escaped line filters. Every
// line is untrusted text: callers redact, scan and wrap it before a model
// reads it (internal/tools/logtools).
package workerlogs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Limits of the reader (AGENTS.md rules 4 and 7).
const (
	// MaxLines is the most lines one call returns (Q0 item 3).
	MaxLines = 200
	// MaxBodyBytes caps the query_range answer.
	MaxBodyBytes = 4 << 20
	// MaxLookback is how far back Since may reach. Loki's default
	// max_query_length is 721h; a week keeps one query cheap.
	MaxLookback = 7 * 24 * time.Hour
	// DefaultLookback is used when LogQuery.Since is zero.
	DefaultLookback = time.Hour
	// MaxConnectorBytes bounds a connector name (Kafka Connect has no limit;
	// a topic name has 249).
	MaxConnectorBytes = 249
	// MaxTask is the highest task id accepted.
	MaxTask = 9999

	queryPath    = "/loki/api/v1/query_range"
	maxErrorBody = 4 << 10
	maxErrorText = 200
	maxLabelLen  = 128
)

// Errors a caller can match with errors.Is.
var (
	// ErrAuth is matched by a 401 or 403 answer: the token is missing or
	// lacks read access.
	ErrAuth = errors.New("workerlogs: not authorized")
	// ErrTooLarge wraps an answer over MaxBodyBytes.
	ErrTooLarge = errors.New("workerlogs: response too large")
	// ErrBadQuery wraps a LogQuery the reader refuses before any I/O.
	ErrBadQuery = errors.New("workerlogs: bad query")
)

// Levels in increasing order. "" on a LogLine means the level could not be
// read from the line or its labels.
var levels = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

func levelRank(l string) int {
	for i, x := range levels {
		if x == l {
			return i
		}
	}
	return -1
}

// LogQuery is one read (Q0 item 3, clients.LogQuery).
type LogQuery struct {
	Connector string    // required; matched literally in the line
	Task      int       // -1 = any task
	MinLevel  string    // TRACE..FATAL; "" = WARN
	Since     time.Time // zero = now - DefaultLookback
	Limit     int       // 1..MaxLines; 0 = MaxLines
}

// LogLine is one log entry (Q0 item 3, clients.LogLine). A stack trace the
// shipper joined to the line that logged it is one entry.
type LogLine struct {
	At     time.Time `json:"at"`
	Level  string    `json:"level"`
	Logger string    `json:"logger,omitempty"`
	Worker string    `json:"worker,omitempty"`
	Text   string    `json:"text"` // untrusted
}

// StatusError is a non-200 Loki answer.
type StatusError struct {
	Code int
	Body string // first 4 KiB, server text, untrusted
}

// Error shows at most 200 bytes of Body with control characters removed:
// the text reaches the room log and slog.
func (e *StatusError) Error() string {
	return fmt.Sprintf("workerlogs: GET %s: status %d: %s", queryPath, e.Code, oneLine(e.Body, maxErrorText))
}

// Is makes a 401 or 403 match ErrAuth.
func (e *StatusError) Is(target error) bool {
	return target == ErrAuth && (e.Code == http.StatusUnauthorized || e.Code == http.StatusForbidden)
}

// Client reads one Loki. It is safe for concurrent use.
type Client struct {
	base     *url.URL
	selector string
	hc       *http.Client
	now      func() time.Time
}

// Option configures New.
type Option func(*settings)

type settings struct {
	tls      netsec.TLS
	timeout  time.Duration
	username string
	orgID    string
}

// WithTLS sets the CA bundle, client certificate and server name.
func WithTLS(t netsec.TLS) Option { return func(s *settings) { s.tls = t } }

// WithTimeout replaces the 10 s whole-request timeout.
func WithTimeout(d time.Duration) Option { return func(s *settings) { s.timeout = d } }

// WithBasicAuthUser sends the token as the password of HTTP basic auth for
// this user instead of as a bearer token (Grafana Cloud: the stack's user id).
func WithBasicAuthUser(user string) Option { return func(s *settings) { s.username = user } }

// WithOrgID sends X-Scope-OrgID (a multi-tenant Loki's tenant).
func WithOrgID(id string) Option { return func(s *settings) { s.orgID = id } }

// New returns a reader for the Loki at baseURL (e.g. http://localhost:3100,
// or a gateway URL with a path prefix, without /loki/api/v1) over the
// streams selector picks. token may be empty; when set it goes only to
// baseURL's own host, and only over https or to a loopback host (the
// netsec rule). Nothing is dialled here.
func New(baseURL, selector string, token netsec.Secret, opts ...Option) (*Client, error) {
	s := settings{timeout: netsec.DefaultHTTPTimeout}
	for _, o := range opts {
		o(&s)
	}
	if err := CheckSelector(selector); err != nil {
		return nil, fmt.Errorf("workerlogs: %w: WORKER_LOGS_SELECTOR: %v", netsec.ErrInvalid, err)
	}
	if s.username != "" && token == "" {
		return nil, fmt.Errorf("workerlogs: %w: a basic auth user needs the token as its password", netsec.ErrInvalid)
	}
	if s.orgID != "" && (len(s.orgID) > maxLabelLen || strings.IndexFunc(s.orgID, unicode.IsControl) >= 0) {
		return nil, fmt.Errorf("workerlogs: %w: org id must be at most %d printable characters", netsec.ErrInvalid, maxLabelLen)
	}
	h := netsec.HTTP{TLS: s.tls}
	if s.username != "" {
		// netsec sends basic auth to the base host only, and only over https.
		h.Username, h.Password = s.username, token
	}
	hc, err := h.Client(baseURL, s.timeout)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: %w", err)
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: %w: base URL does not parse", netsec.ErrInvalid)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("workerlogs: %w: base URL must not have a query or fragment", netsec.ErrInvalid)
	}
	if token != "" && s.username == "" && u.Scheme != "https" && !loopback(u.Hostname()) {
		return nil, fmt.Errorf("workerlogs: %w: a token over http:// is sent in clear text; use an https:// URL", netsec.ErrInvalid)
	}
	if (token != "" && s.username == "") || s.orgID != "" {
		hc.Transport = &headers{next: hc.Transport, scheme: u.Scheme, host: u.Host, token: tokenIf(token, s.username == ""), orgID: s.orgID}
	}
	hc.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != via[0].URL.Scheme || req.URL.Host != via[0].URL.Host {
			return errors.New("redirect to another host refused")
		}
		return nil
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return &Client{base: u, selector: strings.TrimSpace(selector), hc: hc, now: time.Now}, nil
}

func tokenIf(t netsec.Secret, ok bool) netsec.Secret {
	if ok {
		return t
	}
	return ""
}

func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// headers adds the bearer token and the tenant to requests for the base
// URL's scheme and host only.
type headers struct {
	next         http.RoundTripper
	scheme, host string
	token        netsec.Secret
	orgID        string
}

func (h *headers) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != h.scheme || req.URL.Host != h.host {
		return h.next.RoundTrip(req)
	}
	r := req.Clone(req.Context())
	if h.token != "" {
		r.Header.Set("Authorization", "Bearer "+h.token.Reveal())
	}
	if h.orgID != "" {
		r.Header.Set("X-Scope-OrgID", h.orgID)
	}
	return h.next.RoundTrip(r)
}

// Selector returns the configured stream selector.
func (c *Client) Selector() string { return c.selector }

// normalize checks q and fills its defaults.
func (c *Client) normalize(q LogQuery, now time.Time) (LogQuery, error) {
	if q.Connector == "" || len(q.Connector) > MaxConnectorBytes {
		return q, fmt.Errorf("%w: connector must be 1 to %d bytes", ErrBadQuery, MaxConnectorBytes)
	}
	if strings.IndexFunc(q.Connector, unicode.IsControl) >= 0 {
		return q, fmt.Errorf("%w: connector has control characters", ErrBadQuery)
	}
	if q.Task < -1 || q.Task > MaxTask {
		return q, fmt.Errorf("%w: task %d: want -1 (any) or 0 to %d", ErrBadQuery, q.Task, MaxTask)
	}
	if q.MinLevel == "" {
		q.MinLevel = "WARN"
	}
	q.MinLevel = strings.ToUpper(q.MinLevel)
	if levelRank(q.MinLevel) < 0 {
		return q, fmt.Errorf("%w: level must be one of %s", ErrBadQuery, strings.Join(levels, ", "))
	}
	if q.Since.IsZero() {
		q.Since = now.Add(-DefaultLookback)
	}
	if !q.Since.Before(now) {
		return q, fmt.Errorf("%w: since must be in the past", ErrBadQuery)
	}
	if now.Sub(q.Since) > MaxLookback {
		return q, fmt.Errorf("%w: since is more than %s ago", ErrBadQuery, MaxLookback)
	}
	if q.Limit == 0 {
		q.Limit = MaxLines
	}
	if q.Limit < 1 || q.Limit > MaxLines {
		return q, fmt.Errorf("%w: limit %d: want 1 to %d", ErrBadQuery, q.Limit, MaxLines)
	}
	return q, nil
}

// WorkerLogs returns at most q.Limit lines of the configured streams that
// name q.Connector (and q.Task), at q.MinLevel or above, from q.Since to
// now: the newest ones, oldest first (newest last). The call is recorded as
// a provenance call with the host, the path and the LogQL query.
func (c *Client) WorkerLogs(ctx context.Context, q LogQuery) (_ []LogLine, err error) {
	now := c.now()
	q, err = c.normalize(q, now)
	if err != nil {
		return nil, err
	}
	query := buildQuery(c.selector, q)
	v := url.Values{}
	v.Set("query", query)
	v.Set("start", strconv.FormatInt(q.Since.UnixNano(), 10))
	v.Set("end", strconv.FormatInt(now.UnixNano(), 10))
	v.Set("limit", strconv.Itoa(q.Limit))
	// backward = Loki keeps the newest `limit` entries; they are re-sorted
	// oldest first below.
	v.Set("direction", "backward")
	u := *c.base
	u.Path += queryPath
	u.RawQuery = v.Encode()

	start := time.Now()
	defer func() {
		tools.Record(ctx, tools.Call{
			Kind: tools.CallHTTP, Target: c.base.Host, Text: "GET " + c.base.Path + queryPath,
			ParamsRedacted: []string{"query=" + query, "start=" + q.Since.UTC().Format(time.RFC3339), "limit=" + strconv.Itoa(q.Limit)},
			At:             start, TookMS: time.Since(start).Milliseconds(),
		})
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		// *url.Error quotes the whole URL; keep the host only.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("workerlogs: GET %s on %s: %w", queryPath, c.base.Host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return nil, &StatusError{Code: resp.StatusCode, Body: string(b)}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("workerlogs: read answer from %s: %w", c.base.Host, err)
	}
	if len(body) > MaxBodyBytes {
		return nil, fmt.Errorf("%w: more than %d MB from %s", ErrTooLarge, MaxBodyBytes>>20, c.base.Host)
	}
	lines, err := parse(body)
	if err != nil {
		return nil, err
	}
	min := levelRank(q.MinLevel)
	out := lines[:0]
	for _, l := range lines {
		// A line whose level cannot be read is kept: dropping it could hide
		// the one stack trace that matters.
		if l.Level == "" || levelRank(l.Level) >= min {
			out = append(out, l)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	if len(out) > q.Limit {
		out = out[len(out)-q.Limit:]
	}
	return out, nil
}

// answer is the query_range body for a log query: data.resultType
// "streams", each result a stream's labels and its [ns-timestamp, line]
// pairs. Loki 3 may add a third element (structured metadata) to a pair
// when asked to categorize labels; it is ignored.
type answer struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func parse(body []byte) ([]LogLine, error) {
	var a answer
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&a); err != nil {
		return nil, fmt.Errorf("workerlogs: answer is not query_range JSON: %w", err)
	}
	if a.Status != "success" {
		return nil, fmt.Errorf("workerlogs: answer status %q", oneLine(a.Status, 40))
	}
	if a.Data.ResultType != "streams" {
		return nil, fmt.Errorf("workerlogs: answer resultType %q, want streams (is WORKER_LOGS_SELECTOR a log query?)", oneLine(a.Data.ResultType, 40))
	}
	var out []LogLine
	for _, r := range a.Data.Result {
		worker := workerOf(r.Stream)
		labelLevel := normLevel(firstLabel(r.Stream, "level", "detected_level"))
		for _, pair := range r.Values {
			if len(pair) < 2 {
				return nil, fmt.Errorf("workerlogs: answer has a value with %d elements, want [timestamp, line]", len(pair))
			}
			var ts, line string
			if err := json.Unmarshal(pair[0], &ts); err != nil {
				return nil, fmt.Errorf("workerlogs: answer timestamp is not a string: %w", err)
			}
			ns, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("workerlogs: answer timestamp %q is not nanoseconds", oneLine(ts, 40))
			}
			if err := json.Unmarshal(pair[1], &line); err != nil {
				return nil, fmt.Errorf("workerlogs: answer line is not a string: %w", err)
			}
			lv := labelLevel
			if lv == "" {
				lv = lineLevel(line)
			}
			out = append(out, LogLine{At: time.Unix(0, ns).UTC(), Level: lv, Logger: loggerOf(line), Worker: worker, Text: line})
		}
	}
	return out, nil
}

func firstLabel(m map[string]string, names ...string) string {
	for _, n := range names {
		if v := m[n]; v != "" {
			return v
		}
	}
	return ""
}

// workerOf names the worker from the stream labels shippers commonly set.
// Label values are operator-side text but still cleaned and cut.
func workerOf(m map[string]string) string {
	return oneLine(firstLabel(m, "pod", "instance", "host", "hostname", "container"), maxLabelLen)
}

func normLevel(s string) string {
	switch u := strings.ToUpper(strings.TrimSpace(s)); u {
	case "WARNING":
		return "WARN"
	case "SEVERE", "CRITICAL":
		return "ERROR"
	default:
		if levelRank(u) >= 0 {
			return u
		}
	}
	return ""
}

// levelRE finds the level word near the start of a log4j line
// ("2026-10-23 10:00:00,123 ERROR ..." or "[2026-...] WARN ...").
var levelRE = regexp.MustCompile(`\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|SEVERE|FATAL)\b`)

func lineLevel(line string) string {
	head := line
	if len(head) > 64 {
		head = head[:64]
	}
	if m := levelRE.FindString(head); m != "" {
		return normLevel(m)
	}
	return ""
}

// loggerRE finds the logger at the end of a line's first line: Kafka's
// "(org.apache.kafka.connect.runtime.WorkerTask:212)" or Debezium's
// "[io.debezium.connector.postgresql.PostgresConnectorTask]".
var loggerRE = regexp.MustCompile(`(?:\(([A-Za-z_][\w.$]*):\d+\)|\[([A-Za-z_][\w.$]*)\])\s*$`)

func loggerOf(line string) string {
	first, _, _ := strings.Cut(line, "\n")
	if len(first) > 4096 {
		first = first[len(first)-4096:]
	}
	m := loggerRE.FindStringSubmatch(first)
	if m == nil {
		return ""
	}
	if m[1] != "" {
		return oneLine(m[1], maxLabelLen)
	}
	return oneLine(m[2], maxLabelLen)
}

// oneLine drops control characters and cuts s to n bytes on a rune
// boundary: s is remote text echoed into an error or a field.
func oneLine(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}
