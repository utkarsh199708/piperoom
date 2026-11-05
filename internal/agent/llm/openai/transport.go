package openai

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// errDenied marks a dial or proxy target on the deny list. Its text names
// the address (what DNS answered), never the key.
var errDenied = errors.New("llm/openai: model endpoint address refused")

// errOtherHost refuses a request to a host the key was not configured for.
var errOtherHost = errors.New("llm/openai: refusing to send the key to a host other than the configured base URL")

// metadataHostNames are refused by name before any lookup (design 10.3);
// config.ParseBaseURL refuses them as a base URL, this covers the proxy path.
var metadataHostNames = map[string]bool{"metadata.google.internal": true, "metadata": true}

// hooks replace the resolver and the proxy lookup in tests.
type hooks struct {
	// lookup resolves a host name; nil is net.DefaultResolver.LookupNetIP.
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// proxy is the proxy lookup used when Options.Proxy is on; nil is
	// http.ProxyFromEnvironment.
	proxy func(*http.Request) (*url.URL, error)
	// tlsConfig trusts a test server's certificate; nil is the system roots.
	tlsConfig *tls.Config
}

// guard is the dial rule of one endpoint.
type guard struct {
	// plainHTTP: the base URL is http, so only loopback (or, with
	// privateHTTP, RFC 1918, ULA and loopback) may be dialed.
	plainHTTP   bool
	privateHTTP bool
	lookup      func(ctx context.Context, host string) ([]netip.Addr, error)
}

// check applies the deny list, and for plain http the private rule, to one
// address.
func (g guard) check(a netip.Addr) error {
	a = a.Unmap().WithZone("")
	if config.ModelAddrDenied(a) {
		return fmt.Errorf("%w: %s is link-local, metadata, multicast, broadcast or unspecified", errDenied, a)
	}
	switch {
	case !g.plainHTTP:
	case g.privateHTTP:
		if !config.ModelPrivateHTTPAddr(a) {
			return fmt.Errorf("%w: plain http only to loopback, RFC 1918 or ULA addresses, not %s", errDenied, a)
		}
	case !a.IsLoopback():
		return fmt.Errorf("%w: plain http only to loopback, not %s", errDenied, a)
	}
	return nil
}

// resolve returns host's addresses, refusing the whole name when it is a
// metadata name or any of its addresses fails check.
func (g guard) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	h := strings.TrimSuffix(strings.ToLower(host), ".")
	if metadataHostNames[h] {
		return nil, fmt.Errorf("%w: %s is a cloud metadata name", errDenied, h)
	}
	if a, err := netip.ParseAddr(h); err == nil {
		if err := g.check(a); err != nil {
			return nil, err
		}
		return []netip.Addr{a}, nil
	}
	lookup := g.lookup
	if lookup == nil {
		lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	addrs, err := lookup(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("llm/openai: resolve model endpoint: %w", err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("llm/openai: resolve model endpoint: no address")
	}
	for _, a := range addrs {
		if err := g.check(a); err != nil {
			return nil, err
		}
	}
	return addrs, nil
}

// dialer resolves through resolve and dials the checked addresses only; its
// Control hook checks the address the kernel connects to once more, so no
// path dials an address the rule refuses.
func (g guard) dialContext(d *net.Dialer) func(ctx context.Context, network, address string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("llm/openai: dial: %w", err)
		}
		addrs, err := g.resolve(ctx, host)
		if err != nil {
			return nil, err
		}
		var last error
		for _, a := range addrs {
			c, err := d.DialContext(ctx, network, net.JoinHostPort(a.Unmap().String(), port))
			if err == nil {
				return c, nil
			}
			last = err
		}
		return nil, last
	}
}

func (g guard) control(_ context.Context, _, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("llm/openai: dial guard: %w", err)
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("llm/openai: dial guard: %q is not an IP address", host)
	}
	return g.check(a)
}

// newClient builds the one *http.Client of a Model (AGENTS.md rule 7,
// design 10.3): the guarded dialer, no proxy unless proxyOn (and never for a
// plain-http endpoint, which is local), no redirects, the key bound to the
// base URL's host.
func newClient(base config.BaseURL, proxyOn bool, key netsec.Secret, azureAuth bool, h hooks) *http.Client {
	g := guard{plainHTTP: base.URL.Scheme == "http", privateHTTP: base.PrivateHTTP, lookup: h.lookup}
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second, ControlContext: g.control}
	tr := &http.Transport{
		Proxy:                  nil,
		DialContext:            g.dialContext(d),
		ForceAttemptHTTP2:      true,
		TLSHandshakeTimeout:    10 * time.Second,
		IdleConnTimeout:        90 * time.Second,
		MaxIdleConnsPerHost:    16,
		MaxResponseHeaderBytes: 64 << 10,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if h.tlsConfig != nil {
		tr.TLSClientConfig = h.tlsConfig
	}
	if proxyOn && !g.plainHTTP {
		lookupProxy := h.proxy
		if lookupProxy == nil {
			lookupProxy = http.ProxyFromEnvironment
		}
		tr.Proxy = func(r *http.Request) (*url.URL, error) {
			// The dial hook sees only the proxy's address, and the proxy
			// resolves the target itself: refuse a denied target here,
			// before the CONNECT. The proxy resolves it again, so DNS
			// rebinding between the two lookups is a residual risk that
			// the proxy's own deny rule covers (design 10.3).
			if _, err := g.resolve(r.Context(), r.URL.Hostname()); err != nil {
				return nil, err
			}
			return lookupProxy(r)
		}
	}
	// CheckRedirect answers http.ErrUseLastResponse: a 3xx comes back as the
	// response and is classified as a failure, so the key never follows it.
	return &http.Client{
		Transport: &keyTransport{base: tr, scheme: base.URL.Scheme, host: base.URL.Host, key: key, azure: azureAuth},
		Timeout:   TurnTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// keyTransport adds the key to requests for the configured host only and
// refuses any other host, so the key cannot leave for another one.
type keyTransport struct {
	base   http.RoundTripper
	scheme string
	host   string
	key    netsec.Secret
	azure  bool
}

func (t *keyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.EqualFold(r.URL.Scheme, t.scheme) || !strings.EqualFold(r.URL.Host, t.host) {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, errOtherHost
	}
	if t.key != "" {
		r = r.Clone(r.Context())
		if t.azure {
			r.Header.Set("api-key", t.key.Reveal())
		} else {
			r.Header.Set("Authorization", "Bearer "+t.key.Reveal())
		}
	}
	return t.base.RoundTrip(r)
}

// send POSTs body and returns the status, the headers and the capped body.
// status is 0 when there was no HTTP answer. A body over maxResponseBytes,
// or one that broke off, is an error with the status set.
func (m *Model) send(ctx context.Context, body []byte) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("llm/openai: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return resp.StatusCode, resp.Header, nil, fmt.Errorf("llm/openai: read response: %w", err)
	}
	if len(raw) > maxResponseBytes {
		return resp.StatusCode, resp.Header, nil, fmt.Errorf("llm/openai: response larger than %d bytes", maxResponseBytes)
	}
	return resp.StatusCode, resp.Header, raw, nil
}

// String hides the key (see Model.String).
func (t *keyTransport) String() string { return "keyTransport(" + t.scheme + "://" + t.host + ")" }

// GoString is String, for %#v.
func (t *keyTransport) GoString() string { return t.String() }
