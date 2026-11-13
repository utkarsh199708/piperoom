// Package confluentcloud is the Confluent Cloud client of the managed setup
// target (row U13c, docs/design/setup-managed.md §2.2, §3.2): the Connect v1
// API on https://api.confluent.cloud with a Cloud API key, the Kafka
// clusters v2 (CMK) read of one cluster, and the cluster's own Kafka REST v3
// ACL create with a cluster API key.
//
// Endpoints, read from the OpenAPI documents of
// github.com/confluentinc/ccloud-sdk-go-v2 (connect/v1/api/openapi.yaml,
// cmk/v2/api/openapi.yaml, kafkarest/v3/api/openapi.yaml) on 2026-10-30:
//
//	GET    /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins                          listConnectv1ConnectorPlugins
//	PUT    /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{plugin_name}/config/validate validateConnectv1ConnectorPlugin
//	GET    /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors                                 listConnectv1Connectors (an array of names)
//	POST   /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors                                 createConnectv1Connector ({"name","config"})
//	GET    /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/status          readConnectv1ConnectorStatus
//	GET    /connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors/{connector_name}/config          getConnectv1ConnectorConfig
//	GET    /cmk/v2/clusters/{id}?environment={environment_id}                                                               getCmkV2Cluster
//	POST   {cluster http_endpoint}/kafka/v3/clusters/{cluster_id}/acls                                                      createKafkaAcls (201)
//
// Every security scheme is HTTP basic auth (the key as username, the secret
// as password); the client sends it over https only: netsec.HTTP refuses a
// login over http, and the auth transport refuses any other scheme or host.
// Response fields the documents state are decoded; the managed Debezium
// plugin names and the role a setup key needs are not stated there and are
// not assumed here (ccloud_list_plugins is the source of truth at run time).
//
// Errors map by HTTP status to ErrBadRequest (400), ErrForbidden (401,
// 403), ErrNotFound (404), ErrConflict (409), ErrThrottled (429 after three
// retries that honour Retry-After up to 5 s each) and ErrAPI. Bodies are
// capped at 4 MB; every call has a 10 s deadline. A connector configuration
// leaves the package only through actions.RedactConfig, and a validate
// result keeps the config names and their error messages, never the values.
package confluentcloud

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

// DefaultBaseURL is the Confluent Cloud API.
const DefaultBaseURL = "https://api.confluent.cloud"

// CallTimeout bounds one call (AGENTS.md rule 4).
const CallTimeout = 10 * time.Second

// maxBody caps a response body (AGENTS.md rule 7).
const maxBody = 4 << 20

// maxRetries is how often a 429 is retried (design §4).
const maxRetries = 3

// maxRetryAfter caps one Retry-After wait.
const maxRetryAfter = 5 * time.Second

// Errors callers branch on with errors.Is.
var (
	ErrBadRequest = errors.New("confluentcloud: bad request")
	ErrForbidden  = errors.New("confluentcloud: unauthorized or forbidden")
	ErrNotFound   = errors.New("confluentcloud: not found")
	ErrConflict   = errors.New("confluentcloud: conflict")
	ErrThrottled  = errors.New("confluentcloud: rate limited")
	ErrAPI        = errors.New("confluentcloud: api error")
	ErrInvalid    = errors.New("confluentcloud: invalid input")
	// ErrNotSupported wraps clients.ErrNotSupported for the restart calls.
	ErrNotSupported = fmt.Errorf("confluentcloud: the Connect v1 API has no restart call in this client: %w", clients.ErrNotSupported)
)

// Patterns every id and name is checked against before a request.
var (
	envRE       = regexp.MustCompile(`^env-[a-z0-9]{3,16}$`)
	clusterRE   = regexp.MustCompile(`^lkc-[a-z0-9]{3,16}$`)
	nameRE      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
	pluginRE    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)
	principalRE = regexp.MustCompile(`^User:sa-[a-z0-9]{3,16}$`)
	resourceRE  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)
)

// CheckEnvironment checks an environment id (env-...).
func CheckEnvironment(v string) error { return check(envRE, "environment id", v) }

// CheckCluster checks a Kafka cluster id (lkc-...).
func CheckCluster(v string) error { return check(clusterRE, "cluster id", v) }

// CheckName checks a connector name.
func CheckName(v string) error { return check(nameRE, "connector name", v) }

// CheckPlugin checks a managed connector plugin (class) name.
func CheckPlugin(v string) error { return check(pluginRE, "plugin name", v) }

// CheckPrincipal checks an ACL principal (User:sa-...).
func CheckPrincipal(v string) error { return check(principalRE, "principal", v) }

// CheckResource checks an ACL topic prefix or group name.
func CheckResource(v string) error { return check(resourceRE, "ACL resource name", v) }

func check(re *regexp.Regexp, what, v string) error {
	if !re.MatchString(v) {
		return fmt.Errorf("%w: %s %s does not match %s", ErrInvalid, what, strconv.Quote(clip(v, 80)), re.String())
	}
	return nil
}

// Config builds a Client. Environment and Cluster scope every Connect call.
type Config struct {
	BaseURL     string // "" means DefaultBaseURL
	APIKey      string
	APISecret   netsec.Secret
	Environment string
	Cluster     string
	// Transport replaces the network for tests (a replay.Replayer); the
	// basic-auth and https rules apply over it all the same.
	Transport http.RoundTripper
}

// Client is the Connect v1 and CMK v2 client of one cluster.
type Client struct {
	base    *url.URL
	env     string
	cluster string
	http    *http.Client
}

// New checks cfg and builds the client.
func New(cfg Config) (*Client, error) {
	base := cfg.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	if err := CheckEnvironment(cfg.Environment); err != nil {
		return nil, err
	}
	if err := CheckCluster(cfg.Cluster); err != nil {
		return nil, err
	}
	hc, u, err := authClient(base, cfg.APIKey, cfg.APISecret, cfg.Transport)
	if err != nil {
		return nil, err
	}
	return &Client{base: u, env: cfg.Environment, cluster: cfg.Cluster, http: hc}, nil
}

// authClient is netsec's https-only basic-auth client, or the same rule
// over a test transport.
func authClient(base, user string, pass netsec.Secret, rt http.RoundTripper) (*http.Client, *url.URL, error) {
	if user == "" || pass == "" {
		return nil, nil, fmt.Errorf("%w: an API key and secret are required", ErrInvalid)
	}
	h := netsec.HTTP{Username: user, Password: pass}
	hc, err := h.Client(base, CallTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("confluentcloud: %w", err)
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: base URL", ErrInvalid)
	}
	if u.Scheme != "https" {
		// netsec allows http without a login; this client always has one.
		return nil, nil, fmt.Errorf("%w: basic auth needs an https:// base URL", ErrInvalid)
	}
	if rt != nil {
		hc = &http.Client{Timeout: CallTimeout, Transport: &basicAuth{next: rt, host: u.Host, user: user, pass: pass}}
	}
	return hc, u, nil
}

// basicAuth adds the login to https requests for one host and refuses any
// other request outright (a test transport has no other host to reach).
type basicAuth struct {
	next http.RoundTripper
	host string
	user string
	pass netsec.Secret
}

func (b *basicAuth) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" || req.URL.Host != b.host {
		return nil, fmt.Errorf("%w: refusing to send credentials to %s://%s", ErrInvalid, req.URL.Scheme, req.URL.Host)
	}
	r := req.Clone(req.Context())
	r.SetBasicAuth(b.user, b.pass.Reveal())
	return b.next.RoundTrip(r)
}

// connectPath is the Connect v1 path of this cluster plus parts, each
// escaped as one segment.
func (c *Client) connectPath(parts ...string) string {
	p := "/connect/v1/environments/" + url.PathEscape(c.env) + "/clusters/" + url.PathEscape(c.cluster)
	for _, s := range parts {
		p += "/" + url.PathEscape(s)
	}
	return p
}

// do sends one request with a JSON body (nil for none) and decodes a 2xx
// JSON response into out (nil to discard). 429 is retried.
func do(ctx context.Context, hc *http.Client, base *url.URL, method, path, rawQuery string, body, out any, what string) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("confluentcloud: %s: encoding the request: %w", what, err)
		}
		payload = b
	}
	for attempt := 0; ; attempt++ {
		retryAfter, err := once(ctx, hc, base, method, path, rawQuery, payload, out, what)
		if err == nil || !errors.Is(err, ErrThrottled) || attempt >= maxRetries {
			return err
		}
		t := time.NewTimer(retryAfter)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("%w: %s: %w", ErrAPI, what, ctx.Err())
		case <-t.C:
		}
	}
}

func once(ctx context.Context, hc *http.Client, base *url.URL, method, path, rawQuery string, payload []byte, out any, what string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	u := *base
	u.Path = path
	u.RawPath = ""
	if strings.Contains(path, "%") {
		// PathEscape put an escape in a segment; keep the escaped form.
		u.RawPath = path
		if p, err := url.PathUnescape(path); err == nil {
			u.Path = p
		}
	}
	u.RawQuery = rawQuery
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
	resp, err := hc.Do(req)
	if err != nil {
		// *url.Error quotes the URL; the reason is enough.
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

// statusError classifies a non-2xx answer. The message is the API's own
// ({"error":{"code","message"}} for Connect v1, {"errors":[{"detail"}]} for
// CMK v2, {"error_code","message"} for Kafka REST v3), redacted and clipped.
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
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
		Errors  []struct {
			Detail string `json:"detail"`
		} `json:"errors"`
	}
	msg := ""
	if json.Unmarshal(raw, &body) == nil {
		switch {
		case body.Error != nil && body.Error.Message != "":
			msg = body.Error.Message
		case body.Message != "":
			msg = body.Message
		case len(body.Errors) > 0:
			msg = body.Errors[0].Detail
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
