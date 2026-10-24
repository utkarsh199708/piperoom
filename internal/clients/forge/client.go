// Package forge is the GitHub client of the pull-request fix
// (docs/design/fix-pr.md, Q-board row Q7b). It authenticates as one GitHub
// App installation, narrows every installation token to the one repository
// of the call and the permissions the call needs, reads files, trees and
// pull requests (Client, a ForgeReader) and, only when built as a Writer,
// opens one pull request from one new piperoom/ branch (a ForgeWriter).
//
// Every request goes through Client.do, which refuses any method and path
// outside the allow-list of design §4.3 before anything is sent: no merge, no
// ref update, no force, no ref delete but the writer's own new branch.
// Redirects are refused, bodies are read to 4 MB, the key, the JWT and every
// token are netsec.Secret and never reach a log, an error or an event.
package forge

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
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/tools"
)

// Limits (AGENTS.md rules 4 and 7).
const (
	// DefaultAPIURL is GitHub.com's REST API.
	DefaultAPIURL = "https://api.github.com"
	// MaxBodyBytes caps every answer.
	MaxBodyBytes = 4 << 20
	// apiVersion pins the REST API version the client was written against.
	apiVersion   = "2022-11-28"
	maxErrorBody = 4 << 10
	maxErrorText = 200
)

// Errors a caller can match with errors.Is. A 404 also matches
// clients.ErrNotFound.
var (
	// ErrAuth matches a 401, or a 403 that is not a rate limit.
	ErrAuth = errors.New("forge: not authorized")
	// ErrRateLimited matches a 429, or a 403 with no requests left.
	ErrRateLimited = errors.New("forge: rate limited")
	// ErrTooLarge wraps an answer over MaxBodyBytes.
	ErrTooLarge = errors.New("forge: response too large")
	// ErrRefused wraps a request outside the allow-list, or bad arguments,
	// refused before any I/O.
	ErrRefused = errors.New("forge: request refused")
	// errRedirect is CheckRedirect's answer to every redirect.
	errRedirect = errors.New("forge: redirect refused")
)

// StatusError is a non-2xx GitHub answer. Message is GitHub's text: cleaned
// and cut, still untrusted.
type StatusError struct {
	Method  string
	Path    string // the request path, no query, no token
	Code    int
	Message string
	// Reset is when a rate limit ends (x-ratelimit-reset or Retry-After);
	// zero when unknown or not a rate limit.
	Reset time.Time
	limit bool
}

// Error names the request and GitHub's message; never a token.
func (e *StatusError) Error() string {
	s := fmt.Sprintf("forge: %s %s: status %d", e.Method, e.Path, e.Code)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.limit && !e.Reset.IsZero() {
		s += " (rate limited until " + e.Reset.UTC().Format(time.RFC3339) + ")"
	}
	return s
}

// Is maps status codes onto the package's and clients' errors.
func (e *StatusError) Is(target error) bool {
	switch target {
	case clients.ErrNotFound:
		return e.Code == http.StatusNotFound
	case ErrRateLimited:
		return e.limit
	case ErrAuth:
		return !e.limit && (e.Code == http.StatusUnauthorized || e.Code == http.StatusForbidden)
	}
	return false
}

// Config builds a Client. The values come from config.ParseForge (proposed
// in Q0 item 6, §5.6) in wiring; this package reads no environment.
type Config struct {
	// APIURL is GITHUB_API_URL: https://api.github.com (the default) or a
	// GitHub Enterprise Server's https://<host>/api/v3. https unless the host
	// is loopback; no user info, query or fragment.
	APIURL         string
	AppID          int64
	InstallationID int64
	Key            AppKey
	// TLS tunes the https connection (a private CA for GitHub Enterprise).
	TLS netsec.TLS
	// Timeout is the per-request timeout; zero means 10 s.
	Timeout time.Duration
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Client is the GitHub reader (ForgeReader). Safe for concurrent use.
type Client struct {
	base           *url.URL
	webHost        string
	appID          int64
	installationID int64
	key            AppKey
	hc             *http.Client
	now            func() time.Time
	tok            tokens
	bot            struct {
		mu    sync.Mutex
		login string
	}
}

var _ ForgeReader = (*Client)(nil)

// New checks cfg and returns a reader. Nothing is dialled here.
func New(cfg Config) (*Client, error) {
	if cfg.APIURL == "" {
		cfg.APIURL = DefaultAPIURL
	}
	if cfg.AppID <= 0 || cfg.InstallationID <= 0 {
		return nil, fmt.Errorf("forge: %w: the App id and installation id must be positive integers", netsec.ErrInvalid)
	}
	if cfg.Key.k == nil {
		return nil, fmt.Errorf("forge: %w: no App private key", netsec.ErrInvalid)
	}
	u, err := url.Parse(cfg.APIURL)
	if err != nil {
		return nil, fmt.Errorf("forge: %w: GITHUB_API_URL does not parse", netsec.ErrInvalid)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Host == "" {
		return nil, fmt.Errorf("forge: %w: GITHUB_API_URL must be https://host[/path] with no user info, query or fragment", netsec.ErrInvalid)
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback(u.Hostname())) {
		return nil, fmt.Errorf("forge: %w: GITHUB_API_URL must be https (http only to a loopback host)", netsec.ErrInvalid)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = netsec.DefaultHTTPTimeout
	}
	hc, err := netsec.HTTP{TLS: cfg.TLS}.Client(cfg.APIURL, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("forge: %w", err)
	}
	hc.Transport = &bearer{next: hc.Transport, scheme: u.Scheme, host: u.Host}
	// A redirect could carry the bearer token elsewhere (the transport only
	// sets it for the API host, but a same-host redirect could still reach a
	// path outside the allow-list): refuse every one.
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return errRedirect }
	u.Path = strings.TrimRight(u.Path, "/")
	web := u.Host
	if strings.EqualFold(u.Hostname(), "api.github.com") {
		web = "github.com"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Client{base: u, webHost: web, appID: cfg.AppID, installationID: cfg.InstallationID, key: cfg.Key, hc: hc, now: cfg.Now}, nil
}

// String keeps a printed Client free of its key and cached tokens (fmt
// cannot call String on the unexported fields that hold them).
func (c *Client) String() string { return "forge.Client{" + c.base.Host + "}" }

// GoString is String for %#v.
func (c *Client) GoString() string { return c.String() }

// WebHost is the host of the forge's web pages (github.com for
// api.github.com, else the API host): every repos[].url must be on it.
func (c *Client) WebHost() string { return c.webHost }

func loopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type authKey struct{}

// bearer sets "Authorization: Bearer <token>" from the request context, and
// only for the API's own scheme and host.
type bearer struct {
	next         http.RoundTripper
	scheme, host string
}

func (b *bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, _ := req.Context().Value(authKey{}).(netsec.Secret)
	if tok == "" || req.URL.Scheme != b.scheme || req.URL.Host != b.host {
		return b.next.RoundTrip(req)
	}
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+tok.Reveal())
	return b.next.RoundTrip(r)
}

// endpoint names one allowed request (design §4.3 and the reads of §5.1).
type endpoint int

const (
	epToken endpoint = iota
	epRepo
	epRef
	epContents
	epTree
	epCommit
	epPull
	epPulls
	epCheckRuns
	epStatus
	epNewBlob
	epNewTree
	epNewCommit
	epNewRef
	epNewPull
	epDeleteOwnRef
	epApp
)

// seg is one path segment GitHub names a repository by.
const seg = `[A-Za-z0-9_.-]+`

// allow is the request allow-list: method and path pattern per endpoint.
// Client.do refuses a request whose method and path do not match its
// endpoint's entry. The only DELETE is of a piperoom/ branch ref; there is
// no PATCH of a ref, no PUT (merge), no review, no settings.
var allow = map[endpoint]struct {
	method string
	re     *regexp.Regexp
}{
	epToken:        {http.MethodPost, regexp.MustCompile(`^/app/installations/[0-9]+/access_tokens$`)},
	epRepo:         {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `$`)},
	epRef:          {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/ref/heads/[^?]+$`)},
	epContents:     {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/contents/[^?]+$`)},
	epTree:         {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/trees/[0-9a-f]{40}$`)},
	epCommit:       {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/commits/[0-9a-f]{40}$`)},
	epPull:         {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/pulls/[0-9]+$`)},
	epPulls:        {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/pulls$`)},
	epCheckRuns:    {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/commits/[0-9a-f]{40}/check-runs$`)},
	epStatus:       {http.MethodGet, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/commits/[0-9a-f]{40}/status$`)},
	epNewBlob:      {http.MethodPost, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/blobs$`)},
	epNewTree:      {http.MethodPost, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/trees$`)},
	epNewCommit:    {http.MethodPost, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/commits$`)},
	epNewRef:       {http.MethodPost, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/refs$`)},
	epNewPull:      {http.MethodPost, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/pulls$`)},
	epApp:          {http.MethodGet, regexp.MustCompile(`^/app$`)},
	epDeleteOwnRef: {http.MethodDelete, regexp.MustCompile(`^/repos/` + seg + `/` + seg + `/git/refs/heads/piperoom/[^?]+$`)},
}

// request is one REST call. path is relative to the API URL, escaped.
type request struct {
	ep     endpoint
	method string
	path   string
	query  url.Values
	body   any
	// accept lists the 2xx codes taken as success; nil means 200.
	accept []int
}

// do sends r with tok and decodes a JSON answer into out (nil: discard).
// It records a CallForge provenance call: the method and path, no token, no
// query beyond ref.
func (c *Client) do(ctx context.Context, tok netsec.Secret, r request, out any) error {
	a, ok := allow[r.ep]
	if !ok || a.method != r.method || !a.re.MatchString(r.path) {
		return fmt.Errorf("%w: %s %s is not on the allow-list", ErrRefused, r.method, r.path)
	}
	text := r.method + " " + r.path
	if ref := r.query.Get("ref"); ref != "" {
		text += "?ref=" + ref
	}
	start := time.Now()
	if r.ep != epToken {
		// A token mint reads nothing of the customer's: no receipt.
		defer func() {
			tools.Record(ctx, tools.Call{Kind: callForge, Target: c.base.Host, Text: text, At: start, TookMS: time.Since(start).Milliseconds()})
		}()
	}
	u := c.base.String() + r.path
	if len(r.query) > 0 {
		u += "?" + r.query.Encode()
	}
	var body io.Reader
	if r.body != nil {
		b, err := json.Marshal(r.body)
		if err != nil {
			return fmt.Errorf("forge: encoding %s %s: %w", r.method, r.path, err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.WithValue(ctx, authKey{}, tok), r.method, u, body)
	if err != nil {
		return fmt.Errorf("forge: building %s %s: %w", r.method, r.path, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("User-Agent", "piperoom")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		// *url.Error quotes the URL; the path is in our own text already.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("forge: %s %s on %s: %w", r.method, r.path, c.base.Host, err)
	}
	defer resp.Body.Close()
	accept := r.accept
	if accept == nil {
		accept = []int{http.StatusOK}
	}
	okCode := false
	for _, code := range accept {
		okCode = okCode || resp.StatusCode == code
	}
	if !okCode {
		return c.statusError(r, resp)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("forge: reading %s %s: %w", r.method, r.path, err)
	}
	if len(b) > MaxBodyBytes {
		return fmt.Errorf("%w: %s %s answered more than %d MB", ErrTooLarge, r.method, r.path, MaxBodyBytes>>20)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("forge: %s %s: answer is not the expected JSON: %w", r.method, r.path, err)
	}
	return nil
}

func (c *Client) statusError(r request, resp *http.Response) error {
	if resp.StatusCode < 300 {
		// An unexpected success code: its body may hold a token (a mint
		// answer), so it is never quoted.
		return &StatusError{Method: r.method, Path: r.path, Code: resp.StatusCode, Message: "unexpected success code"}
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	var gh struct {
		Message string `json:"message"`
	}
	msg := string(b)
	if json.Unmarshal(b, &gh) == nil && gh.Message != "" {
		msg = gh.Message
	}
	e := &StatusError{Method: r.method, Path: r.path, Code: resp.StatusCode, Message: oneLine(msg, maxErrorText)}
	remaining := resp.Header.Get("X-RateLimit-Remaining")
	retry := resp.Header.Get("Retry-After")
	if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode == http.StatusForbidden && (remaining == "0" || retry != "")) {
		e.limit = true
		if n, err := strconv.Atoi(retry); err == nil && n >= 0 {
			e.Reset = c.now().Add(time.Duration(n) * time.Second)
		} else if n, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil && n > 0 {
			e.Reset = time.Unix(n, 0)
		}
	}
	return e
}

// callForge is tools.CallForge of Q0 item 6 (§5.5), not yet in tool.go.
const callForge = "forge"

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
