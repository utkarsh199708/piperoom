package azure

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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

// maxResponseBytes caps every HTTP response body (AGENTS.md §4 rule 7).
const maxResponseBytes = 4 << 20

// defaultTimeout is the client-wide timeout and the deadline given to a
// call whose context has none.
const defaultTimeout = 10 * time.Second

// checkTimeout is the per-check deadline (spec.Kind.Test).
const checkTimeout = 5 * time.Second

// checkBudget is the deadline for a whole Check run (spec.Kind.Test).
const checkBudget = 20 * time.Second

// statusError is a non-2xx answer. Msg comes from the server's error
// field, truncated; Key Vault and Entra ID do not echo secret values.
type statusError struct {
	Status int
	Code   string
	Msg    string
}

func (e *statusError) Error() string {
	s := "HTTP " + strconv.Itoa(e.Status)
	if e.Code != "" {
		s += " " + e.Code
	}
	if e.Msg != "" {
		s += ": " + e.Msg
	}
	return s
}

// Unwrap maps the status to the secrets package's sentinel errors.
func (e *statusError) Unwrap() error {
	switch {
	case e.Status == http.StatusUnauthorized:
		return secrets.ErrLogin
	case e.Status == http.StatusForbidden && e.Code == codeSecretDisabled:
		// A disabled secret is a 403 too, but granting a role would not
		// help: report it like GCP's disabled version, as not found.
		return secrets.ErrNotFound
	case e.Status == http.StatusForbidden:
		return secrets.ErrAccessDenied
	case e.Status == http.StatusNotFound:
		return secrets.ErrNotFound
	case e.Status == http.StatusTooManyRequests || e.Status >= 500:
		return secrets.ErrUnreachable
	}
	return nil
}

var errRedirect = errors.New("azure: the server answered with a redirect, which Piperoom does not follow")

// do sends req and decodes a 2xx JSON body into out (nil = discard).
// Transport errors wrap secrets.ErrUnreachable. errMsg extracts code and
// message from an error body.
func do(c *http.Client, req *http.Request, out any, errMsg func([]byte) (string, string)) error {
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %s %s: %w", secrets.ErrUnreachable, req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("%w: reading response: %w", secrets.ErrUnreachable, err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("azure: response larger than %d bytes", maxResponseBytes)
	}
	if resp.StatusCode >= 300 && resp.StatusCode <= 399 {
		return fmt.Errorf("%w (HTTP %d on %s %s)", errRedirect, resp.StatusCode, req.Method, req.URL.Path)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		code, msg := "", ""
		if errMsg != nil {
			code, msg = errMsg(body)
		}
		return &statusError{Status: resp.StatusCode, Code: truncate(code, 60), Msg: truncate(msg, 200)}
	}
	if out != nil && len(bytes.TrimSpace(body)) > 0 {
		// The decode error is not wrapped: its text can quote the body.
		if json.Unmarshal(body, out) != nil {
			return fmt.Errorf("azure: %s %s: response is not the expected JSON", req.Method, req.URL.Path)
		}
	}
	return nil
}

// codeSecretDisabled is Key Vault's inner error code for a disabled secret.
const codeSecretDisabled = "SecretDisabled"

// kvErrMsg reads Key Vault's {"error":{"code","message","innererror"}}
// body. The inner code, when present, is the more specific one (Forbidden
// vs. SecretDisabled), so it wins.
func kvErrMsg(b []byte) (string, string) {
	var e struct {
		Error struct {
			Code       string `json:"code"`
			Message    string `json:"message"`
			InnerError struct {
				Code string `json:"code"`
			} `json:"innererror"`
		} `json:"error"`
	}
	if json.Unmarshal(b, &e) != nil {
		return "", ""
	}
	if e.Error.InnerError.Code != "" {
		return e.Error.InnerError.Code, e.Error.Message
	}
	return e.Error.Code, e.Error.Message
}

// newClient builds one client that verifies the server and never follows
// redirects (a redirect would resend the bearer token to another host).
func newClient(t netsec.TLS, caPEM string, timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if caPEM != "" {
		t.CAFile = ""
	}
	cfg, err := t.Config()
	if err != nil {
		return nil, fmt.Errorf("azure: TLS settings: %w", err)
	}
	if caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("%w: the CA certificate is not PEM", secrets.ErrBadProviderConfig)
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

// noRedirects returns a copy of c that refuses every redirect, so an
// injected client gets the same guard.
func noRedirects(c *http.Client) *http.Client {
	cp := *c
	cp.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &cp
}

// checkAddress accepts an https URL, or http only to a loopback host
// (tests, a local emulator), so a token never crosses a network in clear.
func checkAddress(raw, what string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(raw), "/"))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("%w: %s address must be an http(s) URL", secrets.ErrBadProviderConfig, what)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("%w: %s address must be a bare origin (no path, credentials, query or fragment)", secrets.ErrBadProviderConfig, what)
	}
	if u.Scheme == "http" && !isLoopback(u.Hostname()) {
		return nil, fmt.Errorf("%w: %s address must use https (http only for localhost)", secrets.ErrBadProviderConfig, what)
	}
	u.Path = ""
	return u, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// withDefaultDeadline gives ctx the default deadline when it has none.
func withDefaultDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, defaultTimeout)
}

// truncate cuts s to at most n bytes on a rune boundary.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

func errorDetail(err error) string { return truncate(err.Error(), 300) }

func pass(id, title, detail string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckPass, Detail: detail}
}

func fail(id, title, detail, fix string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckFail, Detail: detail, Fix: fix}
}

func warn(id, title, detail, fix string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckWarn, Detail: detail, Fix: fix}
}

func skipped(id, title, detail string) spec.Check {
	return spec.Check{ID: id, Title: title, Status: spec.CheckSkipped, Detail: detail}
}

const sampleTitle = "Can read the sample secret"

// sampleCheck reads sample through p and reports its version and whether
// the key is present. It never reports the value.
func sampleCheck(ctx context.Context, p secrets.Provider, sample *secrets.Ref) spec.Check {
	if sample == nil {
		return skipped(secrets.CheckRead, sampleTitle, "no sample reference given")
	}
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	v, err := p.Resolve(cctx, *sample)
	if err != nil {
		return fail(secrets.CheckRead, sampleTitle, errorDetail(err),
			"give Piperoom's identity the Key Vault Secrets User role on "+sample.Path+" or on the vault")
	}
	detail := "read " + sample.Path
	if v.Version != "" {
		detail += ", version " + v.Version
	}
	if sample.Key != "" {
		if _, err := secrets.ExtractKey(v.Data, sample.Key); err != nil {
			return fail(secrets.CheckRead, sampleTitle,
				detail+"; no key "+sample.Key+" (keys: "+strings.Join(secrets.KeysOf(v.Data), ", ")+")",
				"pick one of the secret's keys")
		}
		detail += "; has key " + sample.Key
	}
	return pass(secrets.CheckRead, sampleTitle, detail)
}
