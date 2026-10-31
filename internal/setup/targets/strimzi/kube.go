package strimzi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// The in-cluster ServiceAccount files and environment (docs/design/setup-strimzi.md
// §5): the token is projected and rotated, so it is re-read per request.
const (
	saDir             = "/var/run/secrets/kubernetes.io/serviceaccount"
	defaultTokenFile  = saDir + "/token"
	defaultCAFile     = saDir + "/ca.crt"
	envServiceHost    = "KUBERNETES_SERVICE_HOST"
	envServicePort    = "KUBERNETES_SERVICE_PORT"
	requestTimeout    = 10 * time.Second
	maxResponseBytes  = 4 << 20
	maxRetryAfterWait = 10 * time.Second
)

// Errors of the API client. Compare with errors.Is.
var (
	// ErrUnauthorized: 401, the token is missing, expired or rejected.
	ErrUnauthorized = errors.New("strimzi: the API server rejected the ServiceAccount token")
	// ErrForbidden: 403, RBAC does not grant the request (a name the Role
	// does not list, or a verb it does not grant).
	ErrForbidden = errors.New("strimzi: the Role does not allow this request")
	// ErrNotFound: 404, the object is not there (the chart was not deployed
	// with this room's values).
	ErrNotFound = errors.New("strimzi: object not found")
	// ErrConflict: 409, the resourceVersion test failed (someone changed the
	// object since it was read).
	ErrConflict = errors.New("strimzi: the object changed since it was read")
	// ErrInvalid: 422, the CRD schema rejects the request.
	ErrInvalid = errors.New("strimzi: the API server rejected the object")
	// ErrThrottled: 429 after the one Retry-After the client honours.
	ErrThrottled = errors.New("strimzi: the API server is throttling")
	// ErrAPI: any other non-2xx status.
	ErrAPI = errors.New("strimzi: API request failed")
	// ErrTooLarge: a response body over the 4 MB cap.
	ErrTooLarge = errors.New("strimzi: API response over 4 MB")
)

// API is the Kubernetes API over net/http: one client, the projected token
// re-read per request, every response capped and its status checked. No
// kubectl, no client-go.
type API struct {
	base  string
	http  *http.Client
	token func() (string, error)
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// APIConfig configures NewAPI. Every field is optional in tests; InCluster
// fills the production values.
type APIConfig struct {
	// BaseURL is https://host:port of the API server.
	BaseURL string
	// HTTPClient is used as is when set (tests). Otherwise a client with
	// RootCAs from CAFile, TLS 1.2+, a 10 s timeout and no proxy is built,
	// dialing through Dial when set (the runner's guarded dialer).
	HTTPClient *http.Client
	CAFile     string
	Dial       func(ctx context.Context, network, addr string) (net.Conn, error)
	// Token returns the bearer token; nil reads TokenFile on every request.
	Token     func() (string, error)
	TokenFile string
}

// InClusterConfig is the Job's configuration: the API server from
// KUBERNETES_SERVICE_HOST/PORT and the mounted ServiceAccount files.
func InClusterConfig(getenv func(string) string) (APIConfig, error) {
	host, port := strings.TrimSpace(getenv(envServiceHost)), strings.TrimSpace(getenv(envServicePort))
	if host == "" || port == "" {
		return APIConfig{}, errors.New("strimzi: not running in a cluster (KUBERNETES_SERVICE_HOST or _PORT unset)")
	}
	if _, err := strconv.Atoi(port); err != nil {
		return APIConfig{}, fmt.Errorf("strimzi: KUBERNETES_SERVICE_PORT %q is not a port", port)
	}
	return APIConfig{BaseURL: "https://" + net.JoinHostPort(host, port), CAFile: defaultCAFile, TokenFile: defaultTokenFile}, nil
}

// checkBaseURL accepts an https URL, and plain http only to a loopback IP
// (the tests' httptest server): a prefix test would pass
// http://127.0.0.1.evil.example, whose host is not loopback at all. A path,
// query, fragment or userinfo is refused: the base is scheme://host:port.
func checkBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return fmt.Errorf("strimzi: the API server URL must be https://host[:port]")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if ip, err := netip.ParseAddr(u.Hostname()); err == nil && ip.IsLoopback() {
			return nil
		}
	}
	return fmt.Errorf("strimzi: the API server URL must be https (plain http only to a loopback IP)")
}

// NewAPI builds the client.
func NewAPI(c APIConfig) (*API, error) {
	if err := checkBaseURL(c.BaseURL); err != nil {
		return nil, err
	}
	a := &API{base: strings.TrimRight(c.BaseURL, "/"), http: c.HTTPClient, token: c.Token, now: time.Now, sleep: sleepCtx}
	if a.token == nil {
		file := c.TokenFile
		if file == "" {
			file = defaultTokenFile
		}
		a.token = func() (string, error) {
			b, err := os.ReadFile(file)
			if err != nil {
				return "", fmt.Errorf("strimzi: reading the ServiceAccount token: %w", err)
			}
			return strings.TrimSpace(string(b)), nil
		}
	}
	if a.http == nil {
		pool := x509.NewCertPool()
		if c.CAFile != "" {
			pem, err := os.ReadFile(c.CAFile)
			if err != nil {
				return nil, fmt.Errorf("strimzi: reading the cluster CA: %w", err)
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("strimzi: the cluster CA file holds no certificate")
			}
		}
		tr := &http.Transport{
			Proxy:               nil,
			TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: 5 * time.Second,
		}
		if c.Dial != nil {
			tr.DialContext = c.Dial
		}
		a.http = &http.Client{Timeout: requestTimeout, Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}
	}
	return a, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// PatchOp is one RFC 6902 JSON patch operation.
type PatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// Get reads one object (or any GET path) and returns its body.
func (a *API) Get(ctx context.Context, path string) ([]byte, error) {
	return a.do(ctx, http.MethodGet, path, "", nil)
}

// Patch sends a JSON patch; dryRun adds ?dryRun=All, so the API server
// validates and returns the object as it would store it without storing it.
func (a *API) Patch(ctx context.Context, path string, ops []PatchOp, dryRun bool) ([]byte, error) {
	body, err := json.Marshal(ops)
	if err != nil {
		return nil, fmt.Errorf("strimzi: encoding a patch: %w", err)
	}
	if dryRun {
		path += "?dryRun=All"
	}
	return a.do(ctx, http.MethodPatch, path, "application/json-patch+json", body)
}

// Delete deletes one object with foreground propagation.
func (a *API) Delete(ctx context.Context, path string) error {
	return a.DeleteIf(ctx, path, "", "")
}

// DeleteIf deletes one object only if it is still the one read: the API
// server refuses the delete with 409 (ErrConflict) when its uid or
// resourceVersion differ from the preconditions. Empty values send none.
func (a *API) DeleteIf(ctx context.Context, path, uid, resourceVersion string) error {
	opts := map[string]any{"propagationPolicy": "Foreground"}
	pre := map[string]any{}
	if uid != "" {
		pre["uid"] = uid
	}
	if resourceVersion != "" {
		pre["resourceVersion"] = resourceVersion
	}
	if len(pre) > 0 {
		opts["preconditions"] = pre
	}
	body, _ := json.Marshal(opts)
	_, err := a.do(ctx, http.MethodDelete, path, "application/json", body)
	return err
}

// do sends one request. A 429 is retried once after its Retry-After (at most
// 10 s); every other status maps to one of the errors above. The body of an
// error is never returned (it may echo the request) beyond the API's own
// status message, clipped.
func (a *API) do(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		out, retry, err := a.once(ctx, method, path, contentType, body)
		if retry > 0 && attempt == 0 {
			if err := a.sleep(ctx, retry); err != nil {
				return nil, err
			}
			continue
		}
		return out, err
	}
}

func (a *API) once(ctx context.Context, method, path, contentType string, body []byte) ([]byte, time.Duration, error) {
	rctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(rctx, method, a.base+path, rd)
	if err != nil {
		return nil, 0, fmt.Errorf("strimzi: building %s %s: %w", method, clipPath(path), err)
	}
	tok, err := a.token()
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("strimzi: %s %s: %w", method, clipPath(path), err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, 0, fmt.Errorf("strimzi: reading %s %s: %w", method, clipPath(path), err)
	}
	if len(data) > maxResponseBytes {
		return nil, 0, fmt.Errorf("%w: %s %s", ErrTooLarge, method, clipPath(path))
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return data, 0, nil
	}
	msg := statusMessage(data)
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return nil, 0, fmt.Errorf("%w (%s %s)", ErrUnauthorized, method, clipPath(path))
	case http.StatusForbidden:
		return nil, 0, fmt.Errorf("%w: %s %s: %s", ErrForbidden, method, clipPath(path), msg)
	case http.StatusNotFound:
		return nil, 0, fmt.Errorf("%w: %s", ErrNotFound, clipPath(path))
	case http.StatusConflict:
		return nil, 0, fmt.Errorf("%w: %s %s: %s", ErrConflict, method, clipPath(path), msg)
	case http.StatusUnprocessableEntity:
		return nil, 0, fmt.Errorf("%w: %s %s: %s", ErrInvalid, method, clipPath(path), msg)
	case http.StatusTooManyRequests:
		wait := time.Second
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s >= 0 {
			wait = time.Duration(s) * time.Second
		}
		if wait > maxRetryAfterWait {
			wait = maxRetryAfterWait
		}
		return nil, wait, fmt.Errorf("%w: %s %s", ErrThrottled, method, clipPath(path))
	}
	return nil, 0, fmt.Errorf("%w: %s %s: HTTP %d: %s", ErrAPI, method, clipPath(path), resp.StatusCode, msg)
}

// statusMessage is the message of a metav1.Status body, clipped and without
// control characters; "" when the body is not one.
func statusMessage(body []byte) string {
	var st struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &st) != nil {
		return ""
	}
	return clipText(st.Message, 200)
}

func clipPath(p string) string { return clipText(p, 160) }

// clipText bounds peer text for an error and drops control characters, so a
// message from the API server cannot forge a log line.
func clipText(s string, max int) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}
