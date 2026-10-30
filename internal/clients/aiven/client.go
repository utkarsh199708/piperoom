// Package aiven is the Aiven client of the managed setup target (row U13d,
// docs/design/setup-managed.md §2.3, §3.3): one project and one Kafka
// Connect service on https://api.aiven.io/v1 with an application-user
// token.
//
// Endpoints, read from aiven/aiven-client's aiven/client/client.py
// (AivenClient; read 2026-10-30), all under the /v1 prefix:
//
//	GET  /project/{project}/service/{service}                          get_service (result key "service")
//	GET  /project/{project}/service/{service}/available-connectors     get_available_kafka_connectors
//	GET  /project/{project}/service/{service}/connectors               list_kafka_connectors ("connectors"[].name, .config)
//	GET  /project/{project}/service/{service}/connectors/{name}/status get_kafka_connector_status
//	POST /project/{project}/service/{service}/connectors               create_kafka_connector (the config as the body)
//	POST /project/{project}/integration                                create_service_integration
//
// Path segments are escaped one by one, as build_path quotes each part with
// safe="". The token goes in the Authorization header as "aivenv1 <token>"
// (client.py _execute), never in a URL: the auth transport sets it on https
// requests to the API host only and refuses every other request.
//
// Not confirmed by that source and therefore decoded defensively: the
// shape of available-connectors (read as "plugins"[]), of the connector
// status (read as "status": {"state", "tasks"[]}), and of the create
// responses (only a name or id is read). get_service's secrets
// (service_uri, users[].password, connection_info, access_key, access_cert)
// are never decoded: Service holds the fields setup needs and nothing else.
package aiven

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/redact"
)

// DefaultBaseURL is the Aiven API (client.py's base URL; the /v1 prefix is
// added per call).
const DefaultBaseURL = "https://api.aiven.io"

// CallTimeout bounds one call (AGENTS.md rule 4).
const CallTimeout = 10 * time.Second

// MinTokenBytes is the shortest token accepted (design §2.3).
const MinTokenBytes = 20

const (
	maxBody       = 4 << 20
	maxRetries    = 3
	maxRetryAfter = 5 * time.Second
)

// Errors callers branch on with errors.Is.
var (
	ErrBadRequest = errors.New("aiven: bad request")
	ErrForbidden  = errors.New("aiven: unauthorized or forbidden")
	ErrNotFound   = errors.New("aiven: not found")
	ErrConflict   = errors.New("aiven: conflict")
	ErrThrottled  = errors.New("aiven: rate limited")
	ErrAPI        = errors.New("aiven: api error")
	ErrInvalid    = errors.New("aiven: invalid input")
	// ErrNotSupported wraps clients.ErrNotSupported for the restart calls.
	ErrNotSupported = fmt.Errorf("aiven: restarts are not in this client: %w", clients.ErrNotSupported)
)

var (
	projectRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	serviceRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
	nameRE    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
)

// CheckProject checks a project name.
func CheckProject(v string) error { return check(projectRE, "project", v) }

// CheckService checks a service name.
func CheckService(v string) error { return check(serviceRE, "service", v) }

// CheckName checks a connector name.
func CheckName(v string) error { return check(nameRE, "connector name", v) }

func check(re *regexp.Regexp, what, v string) error {
	if !re.MatchString(v) {
		return fmt.Errorf("%w: %s %s does not match %s", ErrInvalid, what, strconv.Quote(clip(v, 80)), re.String())
	}
	return nil
}

// Config builds a Client for one project and its Kafka Connect service.
type Config struct {
	BaseURL string // "" means DefaultBaseURL
	Token   netsec.Secret
	Project string
	Service string
	// Transport replaces the network for tests; the header and https rules
	// apply over it all the same.
	Transport http.RoundTripper
}

// Client is the Aiven client of one Connect service.
type Client struct {
	base    *url.URL
	project string
	service string
	http    *http.Client
}

// New checks cfg and builds the client.
func New(cfg Config) (*Client, error) {
	base := cfg.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	if err := CheckProject(cfg.Project); err != nil {
		return nil, err
	}
	if err := CheckService(cfg.Service); err != nil {
		return nil, err
	}
	if len(cfg.Token) < MinTokenBytes || strings.ContainsAny(cfg.Token.Reveal(), " \t\r\n") {
		return nil, fmt.Errorf("%w: the token is shorter than %d bytes or holds white space", ErrInvalid, MinTokenBytes)
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("%w: the base URL must be https://host", ErrInvalid)
	}
	rt := cfg.Transport
	if rt == nil {
		// netsec's client gives the TLS defaults; with no login it adds
		// no auth of its own.
		hc, err := netsec.HTTP{}.Client(base, CallTimeout)
		if err != nil {
			return nil, fmt.Errorf("aiven: %w", err)
		}
		rt = hc.Transport
	}
	hc := &http.Client{Timeout: CallTimeout, Transport: &tokenAuth{next: rt, host: u.Host, token: cfg.Token}}
	return &Client{base: u, project: cfg.Project, service: cfg.Service, http: hc}, nil
}

// tokenAuth sets "Authorization: aivenv1 <token>" on https requests to the
// API host and refuses any other request.
type tokenAuth struct {
	next  http.RoundTripper
	host  string
	token netsec.Secret
}

func (a *tokenAuth) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" || req.URL.Host != a.host {
		return nil, fmt.Errorf("%w: refusing to send the token to %s://%s", ErrInvalid, req.URL.Scheme, req.URL.Host)
	}
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "aivenv1 "+a.token.Reveal())
	return a.next.RoundTrip(r)
}

// path builds /v1/<parts...> with each part escaped as one segment.
func path(parts ...string) (escaped, plain string) {
	var e, p strings.Builder
	e.WriteString("/v1")
	p.WriteString("/v1")
	for _, s := range parts {
		e.WriteString("/" + url.PathEscape(s))
		p.WriteString("/" + s)
	}
	return e.String(), p.String()
}

func (c *Client) servicePath(parts ...string) (string, string) {
	return path(append([]string{"project", c.project, "service", c.service}, parts...)...)
}

func (c *Client) do(ctx context.Context, method string, p [2]string, body, out any, what string) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("aiven: %s: encoding the request: %w", what, err)
		}
		payload = b
	}
	for attempt := 0; ; attempt++ {
		wait, err := c.once(ctx, method, p, payload, out, what)
		if err == nil || !errors.Is(err, ErrThrottled) || attempt >= maxRetries {
			return err
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("%w: %s: %w", ErrAPI, what, ctx.Err())
		case <-t.C:
		}
	}
}

func (c *Client) once(ctx context.Context, method string, p [2]string, payload []byte, out any, what string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	u := *c.base
	u.RawPath, u.Path = p[0], p[1]
	var rd io.Reader
	if payload != nil {
		rd = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return 0, fmt.Errorf("%w: %s: building the request", ErrInvalid, what)
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return 0, fmt.Errorf("%w: %s: %v", ErrAPI, what, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return 0, fmt.Errorf("%w: %s: reading the response: %v", ErrAPI, what, err)
	}
	if len(raw) > maxBody {
		return 0, fmt.Errorf("%w: %s: response over %d bytes", ErrAPI, what, maxBody)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return retryAfter(resp.Header.Get("Retry-After")), statusError(resp.StatusCode, raw, what)
	}
	if out == nil || len(bytes.TrimSpace(raw)) == 0 {
		return 0, nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return 0, fmt.Errorf("%w: %s: decoding the response: %v", ErrAPI, what, err)
	}
	return 0, nil
}

func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || n < 0 {
		return time.Second
	}
	d := time.Duration(n) * time.Second
	if d > maxRetryAfter {
		d = maxRetryAfter
	}
	return d
}

// statusError classifies a non-2xx answer; Aiven's body carries "message"
// and "errors"[].message.
func statusError(status int, raw []byte, what string) error {
	class := ErrAPI
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		class = ErrBadRequest
	case http.StatusUnauthorized, http.StatusForbidden:
		class = ErrForbidden
	case http.StatusNotFound:
		class = ErrNotFound
	case http.StatusConflict:
		class = ErrConflict
	case http.StatusTooManyRequests:
		class = ErrThrottled
	}
	var body struct {
		Message string `json:"message"`
		Errors  []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	msg := ""
	if json.Unmarshal(raw, &body) == nil {
		msg = body.Message
		if msg == "" && len(body.Errors) > 0 {
			msg = body.Errors[0].Message
		}
	}
	detail := fmt.Sprintf("HTTP %d", status)
	if msg != "" {
		m, _ := redact.Text(msg)
		detail += ": " + clip(m, 240)
	}
	return fmt.Errorf("%w: %s: %s", class, what, detail)
}

// ErrorClass returns a stable class name for err.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrBadRequest):
		return "bad_request"
	case errors.Is(err, ErrForbidden):
		return "forbidden"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrThrottled):
		return "throttled"
	case errors.Is(err, ErrInvalid):
		return "invalid"
	}
	return "api"
}

func clip(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(s, "?"))
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "…"
}
