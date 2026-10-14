package secrets

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients/netsec"
)

// maxResponseBytes caps every HTTP response body (AGENTS.md §4 rule 7).
const maxResponseBytes = 4 << 20

// defaultHTTPTimeout is the client-wide timeout; each call also has a
// context deadline.
const defaultHTTPTimeout = 10 * time.Second

// newHTTPClient builds one client that always verifies the server. caPEM,
// when set, replaces t.CAFile (a CA pasted into a card).
func newHTTPClient(t netsec.TLS, caPEM string, timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	if caPEM != "" {
		t.CAFile = ""
	}
	cfg, err := t.Config()
	if err != nil {
		return nil, fmt.Errorf("secrets: TLS settings: %w", err)
	}
	if caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("%w: the CA certificate is not PEM", ErrBadProviderConfig)
		}
		cfg.RootCAs = pool
	}
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     cfg,
		TLSHandshakeTimeout: 5 * time.Second,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: 4,
		ForceAttemptHTTP2:   true,
	}
	return noRedirects(&http.Client{Transport: tr, Timeout: timeout}), nil
}

// errRedirect marks a 3xx answer. Redirects are never followed: net/http
// keeps custom headers such as X-Vault-Token on a cross-host redirect and
// resends a POST body (an approle secret_id, a service-account JWT) on a
// 307/308, possibly to http://. Vault standbys forward requests by default,
// so a correctly set up Vault does not need them.
var errRedirect = errors.New("secrets: the server answered with a redirect, which Piperoom does not follow")

// noRedirects returns a copy of c that refuses every redirect. An injected
// client is wrapped the same way, so the guard cannot be bypassed by
// Options.HTTPClient.
func noRedirects(c *http.Client) *http.Client {
	cp := *c
	cp.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &cp
}

// checkAddress accepts an https URL, or http only to a loopback host (the
// dev stack), so a token is never sent in clear text over a network.
func checkAddress(raw, what string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(raw), "/"))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("%w: %s address must be an http(s) URL", ErrBadProviderConfig, what)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("%w: %s address must not carry credentials, a query or a fragment", ErrBadProviderConfig, what)
	}
	if u.Scheme == "http" && !isLoopback(u.Hostname()) {
		return nil, fmt.Errorf("%w: %s address must use https (http only for localhost)", ErrBadProviderConfig, what)
	}
	return u, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// httpStatusError is a non-2xx answer. Msg comes from the server's error
// field, truncated; managers do not echo secret values in errors.
type httpStatusError struct {
	Status int
	Msg    string
}

func (e *httpStatusError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("HTTP %d", e.Status)
	}
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Msg)
}

// Unwrap maps the status to this package's sentinel errors.
func (e *httpStatusError) Unwrap() error {
	switch {
	case e.Status == http.StatusUnauthorized:
		return ErrLogin
	case e.Status == http.StatusForbidden:
		return ErrAccessDenied
	case e.Status == http.StatusNotFound:
		return ErrNotFound
	case e.Status == http.StatusTooManyRequests || e.Status >= 500:
		return ErrUnreachable
	}
	return nil
}

// doJSON sends req and decodes a 2xx JSON body into out (nil = discard).
// Transport errors wrap ErrUnreachable. errMsg extracts a message from an
// error body.
func doJSON(c *http.Client, req *http.Request, out any, errMsg func([]byte) string) (int, error) {
	resp, err := c.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %s: %w", ErrUnreachable, req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("%w: reading response: %w", ErrUnreachable, err)
	}
	if len(body) > maxResponseBytes {
		return resp.StatusCode, fmt.Errorf("secrets: response larger than %d bytes", maxResponseBytes)
	}
	if resp.StatusCode >= 300 && resp.StatusCode <= 399 {
		// The Location header is not quoted: it can carry tokens in a query.
		return resp.StatusCode, fmt.Errorf("%w (HTTP %d on %s %s)", errRedirect, resp.StatusCode, req.Method, req.URL.Path)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg := ""
		if errMsg != nil {
			msg = errMsg(body)
		}
		return resp.StatusCode, &httpStatusError{Status: resp.StatusCode, Msg: truncate(msg, 200)}
	}
	if out != nil && len(bytes.TrimSpace(body)) > 0 {
		// The decode error is not wrapped: its text can quote the body.
		if json.Unmarshal(body, out) != nil {
			return resp.StatusCode, fmt.Errorf("secrets: %s %s: response is not the expected JSON", req.Method, req.URL.Path)
		}
	}
	return resp.StatusCode, nil
}

func newJSONRequest(ctx context.Context, method, u string, body any) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("secrets: encoding request: %w", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, fmt.Errorf("secrets: building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// truncate cuts s to at most n bytes on a rune boundary, so the result
// stays valid UTF-8.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

// readSecretFile reads a small secret file (a token, a JWT), trimmed.
func readSecretFile(path string) (netsec.Secret, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("secrets: reading %s: %w", path, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 64<<10))
	if err != nil {
		return "", fmt.Errorf("secrets: reading %s: %w", path, err)
	}
	return netsec.Secret(strings.TrimSpace(string(b))), nil
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode().IsRegular()
}

// withDefaultDeadline gives ctx the default call deadline when the caller
// set none, so no provider call can hang (AGENTS.md §4 rule 4).
func withDefaultDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultHTTPTimeout)
}
