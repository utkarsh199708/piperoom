package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/protocol"
)

const (
	// maxHTTPBody caps what http_get keeps from a response.
	maxHTTPBody = 56 << 10
	// maxURLLength caps the URL a request may name.
	maxURLLength = 2048
)

// httpGetOp reads a Connect or Schema Registry REST endpoint on a host the
// room registered. The model names a URL, never a credential: the host must
// match an Env.Targets address, redirects are refused, and the dial passes
// the Control hook.
type httpGetOp struct{}

type httpGetParams struct {
	URL    string `json:"url"`
	Method string `json:"method,omitempty"` // GET (default) or HEAD
}

type httpGetData struct {
	URL         string `json:"url"`
	Status      int    `json:"status"`
	ContentType string `json:"content_type,omitempty"`
	Body        string `json:"body,omitempty"`
	Bytes       int    `json:"bytes"`
	Truncated   bool   `json:"truncated,omitempty"`
}

func (httpGetOp) Name() string     { return "http_get" }
func (httpGetOp) Target() string   { return "" }
func (httpGetOp) Tier() string     { return protocol.TierT0 }
func (httpGetOp) Layer() string    { return "environment" }
func (httpGetOp) Describe() string { return "GET or HEAD a URL on a host in the room's targets" }

func (httpGetOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"url"},
		"properties": map[string]any{
			"url":    map[string]any{"type": "string", "minLength": 1, "maxLength": maxURLLength},
			"method": map[string]any{"type": "string", "enum": []string{"GET", "HEAD"}},
		},
	}
}

func (httpGetOp) Validate(params json.RawMessage, env Env) error {
	_, _, err := decodeHTTPGet(params, env)
	return err
}

// checkTarget is the runner's TargetChecker: a URL outside the targets is
// refused with RefuseTarget before the guard and before any dial.
func (httpGetOp) checkTarget(params json.RawMessage, env Env) error {
	_, _, err := decodeHTTPGet(params, env)
	return err
}

// ParamTarget is the target kind the URL's host matches, so the runner raises
// the class floor of that kind before the guard.
func (httpGetOp) ParamTarget(params json.RawMessage, env Env) string {
	_, u, err := decodeHTTPGet(params, env)
	if err != nil {
		return ""
	}
	if t, ok := matchTarget(env, u); ok {
		return t.Kind
	}
	return ""
}

func (httpGetOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, u, err := decodeHTTPGet(params, env)
	if err != nil {
		return Result{}, err
	}
	if env.HTTP == nil {
		return Result{}, errors.New("ops: no HTTP client configured")
	}
	req, err := http.NewRequestWithContext(ctx, p.Method, u.String(), nil)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrParams, err)
	}
	req.Header.Set("User-Agent", "piperoom-setup/"+CatalogueVersion)
	req.Header.Set("Accept", "application/json, text/plain, */*")

	start := env.Now()
	resp, err := env.HTTP.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("ops: http_get %s: %w", targetOf(u), err)
	}
	defer resp.Body.Close()
	raw, rerr := io.ReadAll(io.LimitReader(resp.Body, maxHTTPBody+1))
	if rerr != nil {
		return Result{}, fmt.Errorf("ops: reading %s: %w", targetOf(u), rerr)
	}
	truncated := len(raw) > maxHTTPBody
	if truncated {
		raw = raw[:maxHTTPBody]
	}
	body, _ := redact.Text(string(raw))
	data := httpGetData{
		URL:         targetOf(u),
		Status:      resp.StatusCode,
		ContentType: clip(resp.Header.Get("Content-Type")),
		Body:        body,
		Bytes:       len(raw),
		Truncated:   truncated,
	}
	res := Result{
		Data:      data,
		Truncated: truncated,
		Receipt:   events.SetupReceipt{Kind: "http", Target: data.URL, At: start, TookMS: env.Now().Sub(start).Milliseconds()},
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		res.Summary = fmt.Sprintf("%s: HTTP %d", p.Method+" "+data.URL, resp.StatusCode)
		return res, fmt.Errorf("%w: %s: %d %s", ErrHTTPStatus, data.URL, resp.StatusCode, clip(http.StatusText(resp.StatusCode)))
	}
	res.Summary = fmt.Sprintf("%s: HTTP %d, %d bytes", p.Method+" "+data.URL, resp.StatusCode, len(raw))
	if truncated {
		res.Summary += ", truncated"
	}
	return res, nil
}

// decodeHTTPGet decodes, bounds and allow-lists the request. It does no I/O.
func decodeHTTPGet(params json.RawMessage, env Env) (httpGetParams, *url.URL, error) {
	var p httpGetParams
	if err := decodeParams(params, &p); err != nil {
		return p, nil, err
	}
	if p.Method == "" {
		p.Method = http.MethodGet
	}
	p.Method = strings.ToUpper(p.Method)
	if p.Method != http.MethodGet && p.Method != http.MethodHead {
		return p, nil, fmt.Errorf("%w: method must be GET or HEAD", ErrParams)
	}
	if p.URL == "" || len(p.URL) > maxURLLength {
		return p, nil, fmt.Errorf("%w: url is required and at most %d bytes", ErrParams, maxURLLength)
	}
	u, err := url.Parse(p.URL)
	if err != nil {
		return p, nil, fmt.Errorf("%w: url: %v", ErrParams, err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return p, nil, fmt.Errorf("%w: url must be http or https", ErrParams)
	case u.Host == "":
		return p, nil, fmt.Errorf("%w: url has no host", ErrParams)
	case u.User != nil:
		return p, nil, fmt.Errorf("%w: url must not carry user info", ErrParams)
	case u.Fragment != "":
		return p, nil, fmt.Errorf("%w: url must not carry a fragment", ErrParams)
	}
	if _, ok := matchTarget(env, u); !ok {
		return p, nil, fmt.Errorf("%w: %s is not one of the room's targets", ErrTarget, clip(u.Host))
	}
	return p, u, nil
}

// matchTarget reports whether the URL's host and port match a registered
// target's address. A target without a port matches the host on any port.
func matchTarget(env Env, u *url.URL) (events.SetupTarget, bool) {
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	for _, t := range env.Targets {
		th, tp := splitTargetAddress(t.Address)
		if th == "" || !strings.EqualFold(th, host) {
			continue
		}
		if tp != "" && tp != port {
			continue
		}
		return t, true
	}
	return events.SetupTarget{}, false
}

// splitTargetAddress reads a target address as host[:port], a URL, or a bare
// host. Credentials in a URL are dropped, never returned.
func splitTargetAddress(addr string) (host, port string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", ""
	}
	if strings.Contains(addr, "://") {
		if u, err := url.Parse(addr); err == nil && u.Hostname() != "" {
			return strings.ToLower(u.Hostname()), u.Port()
		}
		return "", ""
	}
	if h, p, err := net.SplitHostPort(addr); err == nil {
		return strings.ToLower(h), p
	}
	return strings.ToLower(strings.Trim(addr, "[]")), ""
}

// targetOf is a URL for a summary or receipt: scheme, host and path, never
// the query string (a query can carry a token a caller put there).
func targetOf(u *url.URL) string {
	return u.Scheme + "://" + u.Host + u.Path
}
