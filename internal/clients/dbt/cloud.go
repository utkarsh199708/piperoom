package dbt

import (
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

	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Limits of the dbt Cloud reader (AGENTS.md rules 4 and 7).
const (
	// MaxListBytes caps the runs list body.
	MaxListBytes = 4 << 20
	// RunsLookback is how many of the job's newest runs are searched for
	// the newest finished one: a few may be queued or running.
	RunsLookback = 10

	maxErrorBody = 4 << 10
	apiV2        = "/api/v2"
	acceptJSON   = "application/json"
	// runCancelled is dbt Cloud's run status code for a cancelled run, which
	// usually stopped before writing artifacts.
	runCancelled = 30
)

// ErrAuth is matched (errors.Is) by the *StatusError of a 401 or 403 answer:
// the token is missing, wrong, or lacks read access to the job.
var ErrAuth = errors.New("dbt: not authorized")

// StatusError is a non-2xx dbt Cloud answer other than an artifact's 404
// (which wraps ErrNoArtifact instead).
type StatusError struct {
	Path string
	Code int
	Body string // first 4 KiB of the answer, trimmed; server text, untrusted
}

// Error shows at most 200 bytes of Body without control characters: the
// error text reaches the room log and slog, and the body is server text.
func (e *StatusError) Error() string {
	return fmt.Sprintf("dbt: GET %s: status %d: %s", e.Path, e.Code, errorText(e.Body, maxErrorText))
}

// Is makes a 401 or 403 match ErrAuth.
func (e *StatusError) Is(target error) bool {
	return target == ErrAuth && (e.Code == http.StatusUnauthorized || e.Code == http.StatusForbidden)
}

// Cloud reads the artifacts of one dbt Cloud job's newest finished run
// through the Administrative API v2. Each call looks the run up again, so a
// new run is read without a restart. It is safe for concurrent use.
type Cloud struct {
	baseURL string
	account string
	job     string
	hc      *http.Client
}

// CloudOption configures NewCloud.
type CloudOption func(*cloudSettings)

type cloudSettings struct {
	tls     netsec.TLS
	timeout time.Duration
}

// WithTLS sets the CA bundle, client certificate and server name (a dbt
// Cloud single-tenant or proxy deployment with a private CA).
func WithTLS(t netsec.TLS) CloudOption {
	return func(s *cloudSettings) { s.tls = t }
}

// WithTimeout replaces the 10 s whole-request timeout. An artifact of up to
// 16 MiB over a slow link may need more.
func WithTimeout(d time.Duration) CloudOption {
	return func(s *cloudSettings) { s.timeout = d }
}

// NewCloud returns a reader for job jobID of account accountID at baseURL
// (e.g. https://cloud.getdbt.com, without /api/v2), sending token as
// "Authorization: Bearer <token>". The token goes with every request, so
// baseURL must be https; both ids are whole numbers.
func NewCloud(baseURL, accountID, jobID string, token netsec.Secret, opts ...CloudOption) (*Cloud, error) {
	s := cloudSettings{timeout: netsec.DefaultHTTPTimeout}
	for _, o := range opts {
		o(&s)
	}
	if token == "" {
		return nil, fmt.Errorf("dbt: %w: dbt Cloud needs a token", netsec.ErrInvalid)
	}
	if !wholeNumber(accountID) || !wholeNumber(jobID) {
		return nil, fmt.Errorf("dbt: %w: dbt Cloud account and job ids must be whole numbers", netsec.ErrInvalid)
	}
	// netsec checks the URL (http(s), a host, no userinfo) and builds the
	// TLS transport.
	hc, err := netsec.HTTP{TLS: s.tls}.Client(baseURL, s.timeout)
	if err != nil {
		return nil, fmt.Errorf("dbt: %w", err)
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		// Unreachable after netsec's parse; the URL is not echoed either way.
		return nil, fmt.Errorf("dbt: %w: base URL does not parse", netsec.ErrInvalid)
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("dbt: %w: a token over http:// is sent in clear text; use an https:// URL", netsec.ErrInvalid)
	}
	hc.Transport = &bearer{next: hc.Transport, scheme: u.Scheme, host: u.Host, token: token}
	return &Cloud{baseURL: strings.TrimRight(baseURL, "/"), account: accountID, job: jobID, hc: hc}, nil
}

func wholeNumber(s string) bool {
	if s == "" || len(s) > 19 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// bearer adds the token to requests for the base URL's scheme and host only,
// so a redirect to artifact storage on another host never receives it.
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

// RunResults returns run_results.json of the job's newest finished run.
func (c *Cloud) RunResults(ctx context.Context) (RunResults, error) {
	b, err := c.artifact(ctx, RunResultsFile)
	if err != nil {
		return RunResults{}, err
	}
	return parseRunResults(b)
}

// ModelLineage reads manifest.json of the job's newest finished run and
// returns model's direct parents and children. model is a unique_id or a
// model's short name; an unknown one is clients.ErrNotFound.
func (c *Cloud) ModelLineage(ctx context.Context, model string) (Lineage, error) {
	b, err := c.artifact(ctx, ManifestFile)
	if err != nil {
		return Lineage{}, err
	}
	m, err := parseManifest(b)
	if err != nil {
		return Lineage{}, err
	}
	return m.lineage(model)
}

// ModelSQL reads manifest.json of the job's newest finished run and returns
// model's compiled SQL (its raw code when dbt never compiled it). An unknown
// model, or a node that is not a model, is clients.ErrNotFound. The SQL is
// untrusted file text.
func (c *Cloud) ModelSQL(ctx context.Context, model string) (ModelSQL, error) {
	b, err := c.artifact(ctx, ManifestFile)
	if err != nil {
		return ModelSQL{}, err
	}
	m, err := parseManifest(b)
	if err != nil {
		return ModelSQL{}, err
	}
	return m.modelSQL(model)
}

// artifact fetches one artifact of the newest finished run. It does not fall
// back to an older run when the newest has no such artifact: older results
// presented as the latest would mislead the investigation.
func (c *Cloud) artifact(ctx context.Context, name string) ([]byte, error) {
	runID, err := c.latestRun(ctx)
	if err != nil {
		return nil, err
	}
	path := c.accountPath() + "/runs/" + strconv.FormatInt(runID, 10) + "/artifacts/" + name
	var b []byte
	err = c.get(ctx, path, func(r io.Reader) error {
		var err error
		b, err = readCapped(r, name)
		return err
	})
	var se *StatusError
	if errors.As(err, &se) && se.Code == http.StatusNotFound {
		return nil, fmt.Errorf("dbt: run %d has no %s: %w", runID, name, ErrNoArtifact)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

// latestRun returns the id of the job's newest finished (not cancelled) run.
func (c *Cloud) latestRun(ctx context.Context) (int64, error) {
	var raw struct {
		Data []struct {
			ID         int64 `json:"id"`
			Status     int   `json:"status"`
			IsComplete bool  `json:"is_complete"`
		} `json:"data"`
	}
	path := c.accountPath() + "/runs/?job_definition_id=" + url.QueryEscape(c.job) +
		"&order_by=-id&limit=" + strconv.Itoa(RunsLookback)
	err := c.get(ctx, path, func(r io.Reader) error {
		b, err := io.ReadAll(io.LimitReader(r, MaxListBytes+1))
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if len(b) > MaxListBytes {
			return fmt.Errorf("runs list: %w", ErrTooLarge)
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			return fmt.Errorf("runs list: %w: %v", ErrMalformed, err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, r := range raw.Data {
		if r.IsComplete && r.Status != runCancelled {
			return r.ID, nil
		}
	}
	return 0, fmt.Errorf("dbt: job %s: no finished run among the newest %d: %w", c.job, RunsLookback, ErrNoArtifact)
}

func (c *Cloud) accountPath() string {
	return apiV2 + "/accounts/" + url.PathEscape(c.account)
}

// get sends one GET, maps the status and hands a 2xx body to read. It
// records one provenance call per request, on success and on error, holding
// the method and path only: never a header, so never the token.
func (c *Cloud) get(ctx context.Context, path string, read func(io.Reader) error) error {
	// The runs list path ends in "/" (dbt Cloud's canonical form); only the
	// segments before it are checked.
	check, _, _ := strings.Cut(path, "?")
	if err := netsec.CheckPath(strings.TrimSuffix(check, "/")); err != nil {
		return fmt.Errorf("dbt: GET: %w", err)
	}
	start := time.Now()
	err := c.roundTrip(ctx, path, read)
	tools.Record(ctx, tools.Call{
		Kind:   tools.CallHTTP,
		Target: c.baseURL,
		Text:   http.MethodGet + " " + path,
		At:     start,
		TookMS: time.Since(start).Milliseconds(),
	})
	return err
}

func (c *Cloud) roundTrip(ctx context.Context, path string, read func(io.Reader) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("dbt: GET %s: %w", path, err)
	}
	req.Header.Set("Accept", acceptJSON)
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("dbt: GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return &StatusError{Path: path, Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if err := read(resp.Body); err != nil {
		return fmt.Errorf("dbt: GET %s: %w", path, err)
	}
	return nil
}
