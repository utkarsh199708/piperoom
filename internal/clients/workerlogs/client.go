package workerlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Client reads worker logs from one Loki with one *http.Client.
type Client struct {
	endpoint string // the full query_range URL
	scheme   string
	host     string // scheme+host is all an error or a provenance call names
	selector string
	token    netsec.Secret
	hc       *http.Client
	now      func() time.Time
}

var _ Reader = (*Client)(nil)

// New returns a client for cfg.URL. A nil hc builds one through netsec with a
// DefaultTimeout, redirects only within the same host; a caller-supplied hc
// (a test's short timeout) is copied, never modified.
func New(cfg Config, hc *http.Client) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("workerlogs: %w", err)
	}
	if cfg.URL == "" {
		return nil, errors.New("workerlogs: WORKER_LOGS_LOKI_URL is empty")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: WORKER_LOGS_LOKI_URL: %w", err)
	}
	c := &Client{
		endpoint: endpointURL(cfg.URL),
		scheme:   u.Scheme,
		host:     u.Host,
		selector: cfg.Selector,
		token:    cfg.Token,
		now:      time.Now,
	}
	if hc == nil {
		built, err := netsec.HTTP{}.Client(cfg.URL, DefaultTimeout)
		if err != nil {
			return nil, fmt.Errorf("workerlogs: %w", err)
		}
		built.CheckRedirect = sameHostRedirects
		hc = built
	}
	hc = cloneClient(hc)
	if cfg.Token != "" {
		rt := hc.Transport
		if rt == nil {
			rt = http.DefaultTransport
		}
		hc.Transport = &bearer{next: rt, scheme: c.scheme, host: c.host, token: cfg.Token}
	}
	c.hc = hc
	return c, nil
}

func cloneClient(hc *http.Client) *http.Client {
	c := *hc
	return &c
}

// endpointURL appends Loki's query_range path to the base URL. Grafana's docs
// give the base as https://host:port (dev) or https://host/loki (some
// proxies), so a base already ending in /loki gets just the API path.
func endpointURL(base string) string {
	b := strings.TrimRight(base, "/")
	if strings.HasSuffix(b, "/loki") {
		return b + "/api/v1/query_range"
	}
	return b + "/loki/api/v1/query_range"
}

func sameHostRedirects(req *http.Request, via []*http.Request) error {
	if len(via) >= 3 {
		return errors.New("workerlogs: too many redirects")
	}
	if req.URL.Host != via[0].URL.Host {
		return errors.New("workerlogs: redirect to another host refused")
	}
	return nil
}

// bearer adds "Authorization: Bearer" to requests for one scheme and host, so
// a redirect to another host never receives the token.
type bearer struct {
	next   http.RoundTripper
	scheme string
	host   string
	token  netsec.Secret
}

func (b *bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != b.scheme || req.URL.Host != b.host {
		return b.next.RoundTrip(req)
	}
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+b.token.Reveal())
	return b.next.RoundTrip(r)
}

// WorkerLogs reads the lines matching q, oldest first, at most q.Limit (200)
// of them: Loki is asked for the newest ones, and they are reversed. The query
// is built from the configured selector plus line filters for q's fields.
func (c *Client) WorkerLogs(ctx context.Context, q LogQuery) ([]LogLine, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()
	query, err := c.logql(q)
	if err != nil {
		return nil, err
	}
	limit := q.Limit
	if limit <= 0 || limit > MaxLimit {
		limit = MaxLimit
	}
	end := c.now()
	start := q.Since
	if start.IsZero() {
		start = end.Add(-DefaultWindow)
	}
	if !start.Before(end) {
		return nil, fmt.Errorf("workerlogs: since %s is not before now", start.UTC().Format(time.RFC3339))
	}
	if end.Sub(start) > MaxWindow {
		return nil, fmt.Errorf("workerlogs: since is %s back, more than the %s Loki holds for this reader", end.Sub(start).Round(time.Second), MaxWindow)
	}

	u, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: query_range URL: %w", err)
	}
	v := u.Query()
	v.Set("query", query)
	v.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	v.Set("end", strconv.FormatInt(end.UnixNano(), 10))
	v.Set("limit", strconv.Itoa(limit))
	// Backward: a busy worker's oldest lines are not the evidence the room
	// needs; the newest `limit` are read and then shown oldest first.
	v.Set("direction", "backward")
	u.RawQuery = v.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	started := time.Now()
	defer func() {
		tools.Record(ctx, tools.Call{
			Kind: tools.CallHTTP, Target: c.host, Text: "GET " + u.Path,
			ParamsRedacted: []string{
				"connector=" + q.Connector,
				"task=" + strconv.Itoa(q.Task),
				"min_level=" + q.MinLevel,
				"since=" + start.UTC().Format(time.RFC3339),
			},
			At: started, TookMS: time.Since(started).Milliseconds(),
		})
	}()
	resp, err := c.hc.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("workerlogs: query %s: %w", c.host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, statusError(c.host, resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("workerlogs: read %s: %w", c.host, err)
	}
	if int64(len(body)) > MaxResponseBytes {
		return nil, fmt.Errorf("%w: more than %d MB from %s", ErrTooLarge, MaxResponseBytes>>20, c.host)
	}
	lines, err := parseAnswer(body)
	if err != nil {
		return nil, fmt.Errorf("workerlogs: %s: %w", c.host, err)
	}
	return finish(lines, q, limit), nil
}

// statusError maps Loki's refusal to a sentinel the tool names in its message.
func statusError(host string, resp *http.Response) error {
	detail := readDetail(resp)
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("%w: %s answered %d%s", ErrUnauthorized, host, resp.StatusCode, detail)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s answered 404%s (WORKER_LOGS_LOKI_URL must be the Loki root, e.g. http://localhost:3100)", ErrNotFound, host, detail)
	default:
		return fmt.Errorf("workerlogs: %s answered %d%s", host, resp.StatusCode, detail)
	}
}

// readDetail returns Loki's own error text, one line and short, for the error.
func readDetail(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	s := strings.Join(strings.Fields(string(b)), " ")
	if s == "" {
		return ""
	}
	if r := []rune(s); len(r) > 200 {
		s = string(r[:200]) + "…"
	}
	return ": " + s
}

// lokiAnswer is query_range's streams answer.
type lokiAnswer struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// parseAnswer decodes a streams answer. A metric answer (a LogQL function in
// the selector), a non-success status and any malformed body are errors, so a
// proxy's HTML page cannot read as "no logs".
func parseAnswer(body []byte) ([]LogLine, error) {
	var ans lokiAnswer
	if err := json.Unmarshal(body, &ans); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrBadResponse, err)
	}
	if ans.Status != "success" {
		if ans.Error != "" {
			return nil, fmt.Errorf("%w: Loki: %s", ErrBadResponse, oneLine(ans.Error, 200))
		}
		return nil, fmt.Errorf("%w: status %q", ErrBadResponse, oneLine(ans.Status, 40))
	}
	if ans.Data.ResultType != "streams" {
		return nil, fmt.Errorf("%w: resultType %q, want streams", ErrBadResponse, oneLine(ans.Data.ResultType, 40))
	}
	var out []LogLine
	for _, stream := range ans.Data.Result {
		labelLevel := strings.ToUpper(strings.TrimSpace(stream.Stream["level"]))
		for _, v := range stream.Values {
			if len(v) < 2 {
				continue
			}
			var ts, text string
			if err := json.Unmarshal(v[0], &ts); err != nil {
				continue
			}
			if err := json.Unmarshal(v[1], &text); err != nil {
				continue
			}
			n, err := strconv.ParseInt(ts, 10, 64)
			if err != nil {
				continue
			}
			text = strings.TrimSpace(text)
			if text == "" {
				continue
			}
			line := LogLine{At: time.Unix(0, n).UTC(), Text: cutText(text)}
			line.Level = detectLevel(text)
			if line.Level == "" {
				line.Level = labelLevel
			}
			line.Worker, line.Logger = contextTokens(text)
			out = append(out, line)
		}
	}
	return out, nil
}

// finish drops lines below the requested level, then orders the newest limit
// lines oldest first. Loki's own limit may have let a lower-level line through
// (the level filter matches text, not the label), so the filter runs first.
func finish(lines []LogLine, q LogQuery, limit int) []LogLine {
	sortNewestFirst(lines)
	minLevel := minLevelOf(q)
	if minLevel != "" && minLevel != "TRACE" {
		kept := make([]LogLine, 0, len(lines))
		for _, l := range lines {
			if l.Level == "" || levelAtLeast(l.Level, minLevel) {
				kept = append(kept, l)
			}
		}
		lines = kept
	}
	if len(lines) > limit {
		lines = lines[:limit]
	}
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].At.Before(lines[j].At) })
	if lines == nil {
		lines = []LogLine{}
	}
	return lines
}

// logql builds the query: the configured selector, then line filters for the
// model's fields. Nothing else of the model reaches the query, and the
// connector name is checked against the shape Connect accepts before it is
// quoted, so it cannot add a stage or a matcher.
func (c *Client) logql(q LogQuery) (string, error) {
	if q.Connector == "" {
		return "", errors.New("workerlogs: connector is required")
	}
	if !ConnectorNameOK(q.Connector) {
		return "", fmt.Errorf("workerlogs: connector %q is not a connector name", oneLine(q.Connector, 80))
	}
	if q.Task < -1 || q.Task > 255 {
		return "", fmt.Errorf("workerlogs: task %d: want 0 to 255, or -1 for any", q.Task)
	}
	level, ok := normalizeLevel(q.MinLevel)
	if !ok {
		return "", fmt.Errorf("workerlogs: level %q: want TRACE, DEBUG, INFO, WARN, ERROR or FATAL", oneLine(q.MinLevel, 20))
	}

	b := c.selector
	if lv := levelFilter(level); lv != "" {
		// A text filter, not the level label: a Promtail pipeline labels only
		// the line that carries the level, and a joined stack trace has to
		// stay with it. Case-insensitive: both Connect and Debezium write it
		// uppercase, but a custom log4j pattern may not.
		b += ` |~ "(?i)\\b(?:` + lv + `)\\b"`
	}
	b += ` |= ` + strconv.Quote(q.Connector)
	if q.Task >= 0 {
		b += ` |~ "task-` + strconv.Itoa(q.Task) + `\\b"`
	}
	return b, nil
}

// ConnectorNameOK reports whether name has the shape Kafka Connect accepts: a
// letter or digit, then letters, digits, '.', '_' and '-'. It keeps a model
// value out of the LogQL grammar except as a quoted literal.
func ConnectorNameOK(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for i, r := range name {
		ok := r == '.' || r == '_' || r == '-' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if !ok || (i == 0 && (r == '.' || r == '_' || r == '-')) {
			return false
		}
	}
	return true
}

// minLevelOf returns q's level, normalized; "" and unknown answer WARN, the
// same default logql applies.
func minLevelOf(q LogQuery) string {
	if lv, ok := normalizeLevel(q.MinLevel); ok {
		return lv
	}
	return "WARN"
}

func normalizeLevel(s string) (string, bool) {
	lv := strings.ToUpper(strings.TrimSpace(s))
	if lv == "" {
		return "WARN", true
	}
	if _, ok := levelRank[lv]; ok {
		return lv, true
	}
	return "", false
}

// levelFilter is the alternation of the levels at or above level; "" for TRACE
// and DEBUG (every line, continuation lines included).
func levelFilter(level string) string {
	if levelRank[level] <= levelRank["DEBUG"] {
		return ""
	}
	var out []string
	for lv, rank := range levelRank {
		if rank >= levelRank[level] && rank >= levelRank["INFO"] {
			out = append(out, lv)
		}
	}
	sort.Strings(out)
	return strings.Join(out, "|")
}

var levelRank = map[string]int{"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "FATAL": 5}

func levelAtLeast(level, min string) bool {
	r, ok := levelRank[strings.ToUpper(level)]
	if !ok {
		return true // an unknown level is kept: it may be the evidence
	}
	return r >= levelRank[min]
}
