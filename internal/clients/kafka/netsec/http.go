package netsec

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// DefaultHTTPTimeout is the whole-request timeout for Schema Registry and
// Connect calls (AGENTS.md rule 4).
const DefaultHTTPTimeout = 10 * time.Second

// HTTP holds Schema Registry or Kafka Connect connection security. TLS is on
// when the base URL is https; the TLS fields only tune it.
type HTTP struct {
	// Username and Password are sent as HTTP basic auth (for Confluent Cloud:
	// the API key and API secret). Set both or neither.
	Username string
	Password Secret
	TLS      TLS
}

// Client returns an *http.Client for talking to baseURL with these settings.
// Credentials are attached only to requests for baseURL's own scheme and
// host, so a redirect to another host never receives them.
func (h HTTP) Client(baseURL string, timeout time.Duration) (*http.Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		// The *url.Error text quotes the whole URL, which may hold userinfo;
		// report only the reason.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("%w: base URL does not parse: %v", ErrInvalid, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("%w: base URL must be http(s)://host[:port], got scheme %q", ErrInvalid, u.Scheme)
	}
	if u.User != nil {
		// Go's client would send URL userinfo as basic auth, and the URL
		// itself ends up in logs; keep the secret in Password instead.
		return nil, fmt.Errorf("%w: put credentials in Username/Password, not in the URL", ErrInvalid)
	}
	hasLogin := h.Username != "" || h.Password != ""
	if hasLogin && (h.Username == "" || h.Password == "") {
		return nil, fmt.Errorf("%w: basic auth needs both a username and a password", ErrInvalid)
	}
	if u.Scheme == "http" {
		if hasLogin {
			return nil, fmt.Errorf("%w: basic auth over http:// sends the password in clear text; use an https:// URL", ErrInvalid)
		}
		if !h.TLS.IsZero() {
			return nil, fmt.Errorf("%w: TLS files are set but the URL is http://", ErrInvalid)
		}
	}

	tr, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("netsec: http.DefaultTransport is %T, not *http.Transport", http.DefaultTransport)
	}
	tr = tr.Clone()
	if u.Scheme == "https" {
		cfg, err := h.TLS.Config()
		if err != nil {
			return nil, fmt.Errorf("netsec: http TLS: %w", err)
		}
		tr.TLSClientConfig = cfg
	}

	var rt http.RoundTripper = tr
	if hasLogin {
		rt = &basicAuth{next: tr, scheme: u.Scheme, host: u.Host, user: h.Username, pass: h.Password}
	}
	if timeout <= 0 {
		timeout = DefaultHTTPTimeout
	}
	return &http.Client{Transport: rt, Timeout: timeout}, nil
}

// basicAuth adds basic auth to requests for one scheme+host.
type basicAuth struct {
	next   http.RoundTripper
	scheme string
	host   string
	user   string
	pass   Secret
}

func (b *basicAuth) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != b.scheme || req.URL.Host != b.host {
		return b.next.RoundTrip(req)
	}
	// A RoundTripper must not modify the caller's request.
	r := req.Clone(req.Context())
	r.SetBasicAuth(b.user, b.pass.Reveal())
	return b.next.RoundTrip(r)
}
