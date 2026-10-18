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
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Limits of the client (AGENTS.md rules 4 and 7).
const (
	// MaxResponseBytes caps every JSON body read and the log tail kept in
	// memory (a larger maxBytes is cut to it).
	MaxResponseBytes = 4 << 20
	// MaxLogStreamBytes caps how much of one log is streamed through the
	// tail buffer. Memory stays at the tail's size; only time grows with it.
	MaxLogStreamBytes = 64 << 20
	// DefaultLogTailBytes is TaskLogTail's maxBytes when the caller passes 0 or less.
	DefaultLogTailBytes = 64 << 10
	// DefaultRunLimit is DAGRuns' limit when the caller passes 0 or less.
	DefaultRunLimit = 25
	// MaxPageLimit is Airflow's default [api] maximum_page_limit: the server
	// clamps a larger limit to it, so the client asks for no more.
	MaxPageLimit = 100
	// MaxTaskInstances caps how many task instances of one run TaskInstances
	// pages through; beyond it the result is cut and ErrTruncated returned.
	MaxTaskInstances = 1000

	// maxTaskPages bounds the requests of one TaskInstances call when the
	// server's page size is below MaxPageLimit.
	maxTaskPages   = 20
	maxErrorBody   = 4 << 10
	maxErrorText   = 256
	maxTitleBytes  = 120
	apiV1          = "/api/v1"
	acceptJSON     = "application/json"
	acceptLogPlain = "text/plain"
)

// ErrAuth is matched (errors.Is) by the *StatusError of a 401 or 403 answer:
// the credentials are missing or wrong, or the user's role lacks the read.
var ErrAuth = errors.New("airflow: not authorized")

// ErrTruncated is wrapped by the error of a list call that returns only part
// of the list (TaskInstances over MaxTaskInstances). The partial result is
// returned with it.
var ErrTruncated = errors.New("airflow: list truncated")

// StatusError is a non-2xx answer other than 404 (which wraps
// clients.ErrNotFound instead).
type StatusError struct {
	Path string
	Code int
	Body string // first 4 KiB of the answer, trimmed; server text, untrusted
}

// Error shows at most 256 bytes of Body without control characters: the
// error text reaches the room log and slog, and the body is server text.
func (e *StatusError) Error() string {
	return fmt.Sprintf("airflow: GET %s: status %d: %s", e.Path, e.Code, errorText(e.Body, maxErrorText))
}

// errorText makes untrusted server text safe for one error line: control
// characters become spaces, space runs collapse, and the result is cut to at
// most limit bytes on a rune boundary, ending in "…" when cut.
func errorText(s string, limit int) string {
	s = strings.Join(strings.Fields(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)), " ")
	if len(s) <= limit {
		return s
	}
	const marker = "…"
	cut := limit - len(marker)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + marker
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
	// logCeiling is MaxLogStreamBytes; a field rather than the constant so
	// a test can lower it instead of streaming 64 MiB on a slow runner.
	logCeiling int64
}

var (
	_ clients.OrchestratorReader = (*Client)(nil)
	_ clients.DAGScheduleReader  = (*Client)(nil)
)

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
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), hc: hc, logCeiling: MaxLogStreamBytes}, nil
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
			RunID         string    `json:"dag_run_id"`
			State         string    `json:"state"`
			Start         time.Time `json:"start_date"` // null decodes to the zero time
			End           time.Time `json:"end_date"`
			Note          string    `json:"note"`
			LogicalDate   time.Time `json:"logical_date"`   // Airflow 2.2+
			ExecutionDate time.Time `json:"execution_date"` // the same date; the only one before 2.2
			RunType       string    `json:"run_type"`
		} `json:"dag_runs"`
	}
	path := apiV1 + "/dags/" + url.PathEscape(dagID) + "/dagRuns?limit=" + strconv.Itoa(limit) + "&order_by=-execution_date"
	if err := c.getJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	out := make([]DAGRun, 0, len(raw.DAGRuns))
	for _, r := range raw.DAGRuns {
		logical := r.LogicalDate
		if logical.IsZero() {
			logical = r.ExecutionDate
		}
		out = append(out, DAGRun{
			ID: r.RunID, State: r.State, StartedAt: r.Start, EndedAt: r.End, Note: r.Note,
			LogicalDate: logical, RunType: r.RunType,
		})
	}
	return out, nil
}

// TaskInstances returns the task instances of one run, paging with limit and
// offset until total_entries is read. A run with more than MaxTaskInstances
// (a wide mapped task) returns the first MaxTaskInstances and an error
// wrapping ErrTruncated; the caller may use both.
func (c *Client) TaskInstances(ctx context.Context, dagID, runID string) ([]TaskInstance, error) {
	type key struct {
		task     string
		mapIndex int
	}
	out := []TaskInstance{}
	seen := map[key]bool{}
	offset, total := 0, 0
	for page := 0; page < maxTaskPages && len(out) < MaxTaskInstances; page++ {
		var raw struct {
			TaskInstances []struct {
				TaskID   string    `json:"task_id"`
				Try      int       `json:"try_number"`
				State    string    `json:"state"`
				Start    time.Time `json:"start_date"`
				End      time.Time `json:"end_date"`
				Operator string    `json:"operator"`
				MapIndex *int      `json:"map_index"` // absent before Airflow 2.3
			} `json:"task_instances"`
			Total int `json:"total_entries"`
		}
		path := runPath(dagID, runID) + "/taskInstances?limit=" + strconv.Itoa(MaxPageLimit) + "&offset=" + strconv.Itoa(offset)
		if err := c.getJSON(ctx, path, &raw); err != nil {
			return nil, err
		}
		total = raw.Total
		for _, t := range raw.TaskInstances {
			mapIndex := -1
			if t.MapIndex != nil {
				mapIndex = *t.MapIndex
			}
			// Offset paging over a list that changes between pages can
			// repeat an entry; keep the first.
			if k := (key{t.TaskID, mapIndex}); !seen[k] && len(out) < MaxTaskInstances {
				seen[k] = true
				out = append(out, TaskInstance{
					TaskID: t.TaskID, Try: t.Try, State: t.State, StartedAt: t.Start, EndedAt: t.End, Operator: t.Operator,
					MapIndex: mapIndex,
				})
			}
		}
		offset += len(raw.TaskInstances)
		if len(raw.TaskInstances) == 0 || offset >= total {
			return out, nil
		}
	}
	return out, fmt.Errorf("airflow: task instances of %s/%s: read %d of %d: %w", dagID, runID, len(out), total, ErrTruncated)
}

// TaskLogTail returns the end of the log of one try (1 or more) of a task:
// at most maxBytes bytes (0 or less means DefaultLogTailBytes, more than
// MaxResponseBytes is cut to it), starting at a line when the cut falls
// inside one. Airflow's v1 API has no documented way to read a log from the
// end, so the whole log streams through a buffer that keeps only the tail.
// The text is untrusted and returned as read. See TaskLogTailCut for a log
// longer than MaxLogStreamBytes.
func (c *Client) TaskLogTail(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (string, error) {
	tail, _, err := c.TaskLogTailCut(ctx, dagID, runID, taskID, try, maxBytes)
	return tail, err
}

// TaskLogTailCut is TaskLogTail that also reports cut: the log was longer
// than MaxLogStreamBytes, reading stopped there, and the tail is the end of
// its first 64 MiB, not the end of the log. Not on Reader; the batch tools
// may assert for it.
func (c *Client) TaskLogTailCut(ctx context.Context, dagID, runID, taskID string, try int, maxBytes int) (tail string, cut bool, err error) {
	return c.TaskLogTailMapped(ctx, dagID, runID, taskID, -1, try, maxBytes)
}

// TaskLogTailMapped is TaskLogTailCut for one map index of a mapped task: it
// adds the logs call's map_index query parameter. A negative mapIndex is an
// unmapped task (the parameter is left out; Airflow's default is -1).
func (c *Client) TaskLogTailMapped(ctx context.Context, dagID, runID, taskID string, mapIndex, try, maxBytes int) (tail string, cut bool, err error) {
	if try < 1 {
		return "", false, fmt.Errorf("airflow: task log tail: try %d: want 1 or more", try)
	}
	if maxBytes <= 0 {
		maxBytes = DefaultLogTailBytes
	}
	maxBytes = min(maxBytes, MaxResponseBytes)
	path := runPath(dagID, runID) + "/taskInstances/" + url.PathEscape(taskID) + "/logs/" + strconv.Itoa(try)
	if mapIndex >= 0 {
		path += "?map_index=" + strconv.Itoa(mapIndex)
	}
	// One byte more than the tail lets tailFrom see whether the window
	// starts at a line.
	tb := &tailBuffer{buf: make([]byte, maxBytes+1)}
	err = c.get(ctx, path, acceptLogPlain, func(r io.Reader) error {
		if _, err := io.Copy(tb, io.LimitReader(r, c.logCeiling)); err != nil {
			return fmt.Errorf("read log: %w", err)
		}
		// A byte past the ceiling means the log goes on.
		var probe [1]byte
		n, err := io.ReadFull(r, probe[:])
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read log: %w", err)
		}
		cut = n == 1
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return tailFrom(tb.Bytes(), maxBytes), cut, nil
}

// tailBuffer is an io.Writer that keeps only the last len(buf) bytes
// written to it, so a log of any length streams through bounded memory.
type tailBuffer struct {
	buf  []byte
	next int  // where the next byte goes; the oldest byte once full
	full bool // buf has wrapped at least once
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	n, size := len(p), len(t.buf)
	if n >= size {
		copy(t.buf, p[n-size:])
		t.next, t.full = 0, true
		return n, nil
	}
	k := copy(t.buf[t.next:], p)
	copy(t.buf, p[k:])
	if t.next+n >= size {
		t.full = true
	}
	t.next = (t.next + n) % size
	return n, nil
}

// Bytes returns the kept bytes, oldest first.
func (t *tailBuffer) Bytes() []byte {
	if !t.full {
		return t.buf[:t.next]
	}
	out := make([]byte, 0, len(t.buf))
	out = append(out, t.buf[t.next:]...)
	return append(out, t.buf[:t.next]...)
}

// DAG returns the schedule state of one DAG (GET /api/v1/dags/{dag_id}).
func (c *Client) DAG(ctx context.Context, dagID string) (DAG, error) {
	var raw struct {
		DAGID       string          `json:"dag_id"`
		Paused      bool            `json:"is_paused"`
		Description string          `json:"timetable_description"` // Airflow 2.3+
		Interval    json.RawMessage `json:"schedule_interval"`
		CreateAfter time.Time       `json:"next_dagrun_create_after"`
		Next        time.Time       `json:"next_dagrun"`
	}
	if err := c.getJSON(ctx, apiV1+"/dags/"+url.PathEscape(dagID), &raw); err != nil {
		return DAG{}, err
	}
	next := raw.CreateAfter
	if next.IsZero() {
		next = raw.Next
	}
	return DAG{
		ID:        raw.DAGID,
		Schedule:  scheduleText(raw.Interval, raw.Description),
		Paused:    raw.Paused,
		NextRunAt: next,
	}, nil
}

// scheduleInterval is Airflow's schedule_interval object: a cron expression
// (also a preset such as "@daily"), a timedelta, or a dateutil relativedelta
// (only its relative fields are read).
type scheduleInterval struct {
	Type         string `json:"__type"`
	Value        string `json:"value"`
	Years        int64  `json:"years"`
	Months       int64  `json:"months"`
	Days         int64  `json:"days"`
	Hours        int64  `json:"hours"`
	Minutes      int64  `json:"minutes"`
	Seconds      int64  `json:"seconds"`
	Microseconds int64  `json:"microseconds"`
}

// scheduleText renders the interval and appends the description when it
// adds something. A schedule_interval of a shape this client does not know
// leaves only the description: the schedule is context, so it never fails
// the read.
func scheduleText(rawInterval json.RawMessage, desc string) string {
	var iv string
	var si scheduleInterval
	if len(rawInterval) > 0 && json.Unmarshal(rawInterval, &si) == nil {
		iv = si.text()
	}
	switch {
	case iv != "" && desc != "" && iv != desc:
		return iv + " (" + desc + ")"
	case iv != "":
		return iv
	}
	return desc
}

func (s scheduleInterval) text() string {
	switch s.Type {
	case "CronExpression":
		return s.Value
	case "TimeDelta":
		d := time.Duration(s.Days)*24*time.Hour + time.Duration(s.Seconds)*time.Second + time.Duration(s.Microseconds)*time.Microsecond
		return "every " + d.String()
	case "RelativeDelta":
		var parts []string
		for _, p := range []struct {
			n    int64
			unit string
		}{{s.Years, "y"}, {s.Months, "mo"}, {s.Days, "d"}, {s.Hours, "h"}, {s.Minutes, "m"}, {s.Seconds, "s"}} {
			if p.n != 0 {
				parts = append(parts, strconv.FormatInt(p.n, 10)+p.unit)
			}
		}
		if len(parts) > 0 {
			return "every " + strings.Join(parts, " ")
		}
	}
	return ""
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
	if err := json.NewDecoder(io.LimitReader(body, maxErrorBody)).Decode(&p); err != nil {
		return ""
	}
	if t := errorText(p.Title, maxTitleBytes); t != "" {
		return " (" + t + ")"
	}
	return ""
}
