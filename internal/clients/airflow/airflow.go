package airflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Limits of the client (AGENTS.md rules 4 and 7).
const (
	// MaxResponseBytes caps every response body read, JSON or log text.
	MaxResponseBytes = 4 << 20
	// DefaultLogTailBytes is TaskLogTail's maxBytes when the caller passes 0 or less.
	DefaultLogTailBytes = 64 << 10
	// DefaultRunLimit is DAGRuns' limit when the caller passes 0 or less.
	DefaultRunLimit = 25
	// MaxPageLimit is Airflow's default [api] maximum_page_limit: the server
	// clamps a larger limit to it, so the client asks for no more.
	MaxPageLimit = 100

	maxErrorBody   = 4 << 10
	maxTitleRunes  = 80
	apiV1          = "/api/v1"
	acceptJSON     = "application/json"
	acceptLogPlain = "text/plain"
)

// ErrAuth is matched (errors.Is) by the *StatusError of a 401 or 403 answer:
// the credentials are missing or wrong, or the user's role lacks the read.
var ErrAuth = errors.New("airflow: not authorized")

// StatusError is a non-2xx answer other than 404 (which wraps
// clients.ErrNotFound instead).
type StatusError struct {
	Path string
	Code int
	Body string // first 4 KiB of the answer, trimmed; server text, untrusted
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("airflow: GET %s: status %d: %s", e.Path, e.Code, e.Body)
}

// Is makes a 401 or 403 match ErrAuth.
func (e *StatusError) Is(target error) bool {
	return target == ErrAuth && (e.Code == http.StatusUnauthorized || e.Code == http.StatusForbidden)
}

// Client reads one Airflow webserver's stable REST API. It is safe for
// concurrent use.
type Client struct {
	baseURL string
	hc      *http.Client
}

var _ Reader = (*Client)(nil)

// Option configures New.
type Option func(*settings)

type settings struct {
	sec   netsec.HTTP
	token netsec.Secret
}

// WithBasicAuth sends HTTP basic auth (AIRFLOW_USERNAME, AIRFLOW_PASSWORD;
// Airflow's basic_auth API backend). It needs an https base URL.
func WithBasicAuth(username string, password netsec.Secret) Option {
	return func(s *settings) { s.sec.Username, s.sec.Password = username, password }
}

// WithToken sends "Authorization: Bearer <token>" (AIRFLOW_TOKEN), as managed
// Airflow services expect. It needs an https base URL and excludes basic auth.
func WithToken(token netsec.Secret) Option {
	return func(s *settings) { s.token = token }
}

// WithTLS sets the CA bundle, client certificate and server name for an
// https base URL.
func WithTLS(t netsec.TLS) Option {
	return func(s *settings) { s.sec.TLS = t }
}

// New returns a client for the Airflow webserver at baseURL: its root, e.g.
// https://airflow.example.com, without /api/v1. Every request has a 10 s
// timeout on top of the caller's context. Like netsec, it refuses to send a
// credential over plain http.
func New(baseURL string, opts ...Option) (*Client, error) {
	var s settings
	for _, o := range opts {
		o(&s)
	}
	if s.token != "" && (s.sec.Username != "" || s.sec.Password != "") {
		return nil, fmt.Errorf("airflow: %w: set basic auth or a token, not both", netsec.ErrInvalid)
	}
	// netsec checks the URL (http(s), a host, no userinfo), refuses basic
	// auth over http and scopes basic auth to the base URL's host.
	hc, err := s.sec.Client(baseURL, netsec.DefaultHTTPTimeout)
	if err != nil {
		return nil, fmt.Errorf("airflow: %w", err)
	}
	if s.token != "" {
		u, err := url.Parse(baseURL)
		if err != nil {
			// Unreachable after netsec's parse; the URL is not echoed either way.
			return nil, fmt.Errorf("airflow: %w: base URL does not parse", netsec.ErrInvalid)
		}
		if u.Scheme != "https" {
			return nil, fmt.Errorf("airflow: %w: a token over http:// is sent in clear text; use an https:// URL", netsec.ErrInvalid)
		}
		hc.Transport = &bearer{next: hc.Transport, scheme: u.Scheme, host: u.Host, token: s.token}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), hc: hc}, nil
}

// bearer adds the token to requests for the base URL's scheme and host only,
// so a redirect to another host never receives it.
type bearer struct {
	next         http.RoundTripper
	scheme, host string
	token        netsec.Secret
}

func (b *bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != b.scheme || req.URL.Host != b.host {
		return b.next.RoundTrip(req)
	}
	// A RoundTripper must not modify the caller's request.
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+b.token.Reveal())
	return b.next.RoundTrip(r)
}

// DAGRuns returns the runs of dagID, newest logical date first, at most
// limit (0 or less means DefaultRunLimit; more than MaxPageLimit is cut to it).
func (c *Client) DAGRuns(ctx context.Context, dagID string, limit int) ([]DAGRun, error) {
	if limit <= 0 {
		limit = DefaultRunLimit
	}
	limit = min(limit, MaxPageLimit)
	var raw struct {
		DAGRuns []struct {
			RunID string    `json:"dag_run_id"`
			State string    `json:"state"`
			Start time.Time `json:"start_date"` // null decodes to the zero time
			End   time.Time `json:"end_date"`
			Note  string    `json:"note"`
		} `json:"dag_runs"`
	}
	path := apiV1 + "/dags/" + url.PathEscape(dagID) + "/dagRuns?limit=" + strconv.Itoa(limit) + "&order_by=-execution_date"
	if err := c.getJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	out := make([]DAGRun, 0, len(raw.DAGRuns))
	for _, r := range raw.DAGRuns {
		out = append(out, DAGRun{ID: r.RunID, State: r.State, StartedAt: r.Start, EndedAt: r.End, Note: r.Note})
	}
	return out, nil
}

// TaskInstances returns the task instances of one run (the first
// MaxPageLimit; Airflow pages beyond that).
func (c *Client) TaskInstances(ctx context.Context, dagID, runID string) ([]TaskInstance, error) {
	var raw struct {
		TaskInstances []struct {
			TaskID   string    `json:"task_id"`
			Try      int       `json:"try_number"`
			State    string    `json:"state"`
			Start    time.Time `json:"start_date"`
			End      time.Time `json:"end_date"`
			Operator string    `json:"operator"`
		} `json:"task_instances"`
	}
	path := runPath(dagID, runID) + "/taskInstances?limit=" + strconv.Itoa(MaxPageLimit)
	if err := c.getJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	out := make([]TaskInstance, 0, len(raw.TaskInstances))
	for _, t := range raw.TaskInstances {
		out = append(out, TaskInstance{
			TaskID: t.TaskID, Try: t.Try, State: t.State, StartedAt: t.Start, EndedAt: t.End, Operator: t.Operator,
		})
	}
	return out, nil
}

// TaskLogTail returns the end of the log of one try (1 or more) of a task:
// at most maxBytes bytes (0 or less means DefaultLogTailBytes), starting at
// a line when the cut falls inside one. The text is untrusted and returned
// as read. See TaskLogTailCut for a log longer than MaxResponseBytes.
func (c *Client) TaskLogTail(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (string, error) {
	tail, _, err := c.TaskLogTailCut(ctx, dagID, runID, taskID, try, maxBytes)
	return tail, err
}

// TaskLogTailCut is TaskLogTail that also reports cut: the log was longer
// than MaxResponseBytes, so the tail is the end of its first 4 MiB, not the
// end of the log. Airflow's v1 API has no documented way to read a log from
// the end. Not on Reader; the batch tools may assert for it.
func (c *Client) TaskLogTailCut(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (tail string, cut bool, err error) {
	if try < 1 {
		return "", false, fmt.Errorf("airflow: task log tail: try %d: want 1 or more", try)
	}
	if maxBytes <= 0 {
		maxBytes = DefaultLogTailBytes
	}
	path := runPath(dagID, runID) + "/taskInstances/" + url.PathEscape(taskID) + "/logs/" + strconv.Itoa(try)
	var body []byte
	err = c.get(ctx, path, acceptLogPlain, func(r io.Reader) error {
		b, err := io.ReadAll(io.LimitReader(r, MaxResponseBytes+1))
		if err != nil {
			return fmt.Errorf("read log: %w", err)
		}
		body = b
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if len(body) > MaxResponseBytes {
		body, cut = body[:MaxResponseBytes], true
	}
	return tailFrom(body, maxBytes), cut, nil
}

func runPath(dagID, runID string) string {
	return apiV1 + "/dags/" + url.PathEscape(dagID) + "/dagRuns/" + url.PathEscape(runID)
}

// tailFrom returns the last n bytes of b. When that cuts a line it starts at
// the next line, as long as the break lies in the first half of the window:
// past that, dropping the long line's end would lose more than it tidies, so
// it starts at the next UTF-8 rune instead.
func tailFrom(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	start := len(b) - n
	if b[start-1] == '\n' {
		return string(b[start:])
	}
	if i := bytes.IndexByte(b[start:], '\n'); i >= 0 && i < n/2 && start+i+1 < len(b) {
		return string(b[start+i+1:])
	}
	for start < len(b) && !utf8.RuneStart(b[start]) {
		start++
	}
	return string(b[start:])
}

// getJSON GETs path and decodes at most MaxResponseBytes of JSON into out; a
// body cut by the cap fails to decode.
func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	return c.get(ctx, path, acceptJSON, func(r io.Reader) error {
		if err := json.NewDecoder(io.LimitReader(r, MaxResponseBytes)).Decode(out); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		return nil
	})
}

// get sends one GET, maps the status and hands a 2xx body to read. It
// records one provenance call per request, on success and on error, holding
// the method and path only: never a header, so never a credential.
func (c *Client) get(ctx context.Context, path, accept string, read func(io.Reader) error) error {
	// A refused path is never sent, so it gets no receipt (#446).
	if err := netsec.CheckPath(path); err != nil {
		return fmt.Errorf("airflow: GET: %w", err)
	}
	start := time.Now()
	err := c.roundTrip(ctx, path, accept, read)
	tools.Record(ctx, tools.Call{
		Kind:   tools.CallHTTP,
		Target: c.baseURL,
		Text:   http.MethodGet + " " + path,
		At:     start,
		TookMS: time.Since(start).Milliseconds(),
	})
	return err
}

func (c *Client) roundTrip(ctx context.Context, path, accept string, read func(io.Reader) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("airflow: GET %s: %w", path, err)
	}
	req.Header.Set("Accept", accept)
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("airflow: GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("airflow: GET %s: %w%s", path, clients.ErrNotFound, notFoundTitle(resp.Body))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return &StatusError{Path: path, Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if err := read(resp.Body); err != nil {
		return fmt.Errorf("airflow: GET %s: %w", path, err)
	}
	return nil
}

// notFoundTitle returns " (title)" from Airflow's problem+json 404 body
// ("DAG not found", "DAGRun not found", "Task instance not found") so the
// caller can tell which object is missing; "" when the body has none.
func notFoundTitle(body io.Reader) string {
	var p struct {
		Title string `json:"title"`
	}
	if err := json.NewDecoder(io.LimitReader(body, maxErrorBody)).Decode(&p); err != nil || p.Title == "" {
		return ""
	}
	t := p.Title
	if r := []rune(t); len(r) > maxTitleRunes {
		t = string(r[:maxTitleRunes])
	}
	return " (" + t + ")"
}
