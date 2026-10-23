package gemini

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/redact"
)

// errDenied marks a dial or proxy target refused by the deny list; never
// retried.
var errDenied = errors.New("llm/gemini: address refused for a model endpoint")

// errRedirect marks a 3xx the client refused to follow (design 10.3: a
// redirect could carry the key to another host).
var errRedirect = errors.New("llm/gemini: redirect not followed")

func refuseRedirect(*http.Request, []*http.Request) error { return errRedirect }

// metadataNames are refused by name before any lookup (as config.ParseBaseURL).
var metadataNames = map[string]bool{"metadata.google.internal": true, "metadata": true}

// dialAllowed applies the deny list to the address actually dialed: never a
// metadata, link-local, multicast or unspecified address; for plain http only
// loopback, or with PIPEROOM_MODEL_ALLOW_PRIVATE_HTTP's PrivateHTTP also
// RFC 1918 and ULA.
func dialAllowed(base config.BaseURL, address string) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: unparsable %q", errDenied, address)
	}
	addr := ap.Addr().Unmap()
	if config.ModelAddrDenied(addr) {
		return errDenied
	}
	if base.URL != nil && base.URL.Scheme == "http" {
		if base.PrivateHTTP {
			if !config.ModelPrivateHTTPAddr(addr) {
				return errDenied
			}
		} else if !addr.IsLoopback() {
			return errDenied
		}
	}
	return nil
}

// proxyFunc picks the proxy for a request; http.ProxyFromEnvironment in
// production.
type proxyFunc func(*http.Request) (*url.URL, error)

// modelTransport is the deny-listed transport every model request uses. A
// nil fromEnv means http.ProxyFromEnvironment.
func modelTransport(base config.BaseURL, useProxy bool, fromEnv proxyFunc) *http.Transport {
	d := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			return dialAllowed(base, address)
		},
	}
	t := &http.Transport{
		DialContext:           d.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// Proxy stays nil unless PIPEROOM_MODEL_PROXY=on: through a proxy the
		// dial hook sees only the proxy's address.
	}
	if useProxy {
		if fromEnv == nil {
			fromEnv = http.ProxyFromEnvironment
		}
		t.Proxy = checkedProxy(base, fromEnv, net.DefaultResolver)
	}
	return t
}

// resolver is the part of *net.Resolver the proxy check uses.
type resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// checkedProxy resolves the target first and refuses it when its name or any
// address is on the deny list, then returns the environment's proxy. The
// proxy resolves the name again (a residual DNS rebinding risk, design 10.3).
func checkedProxy(base config.BaseURL, fromEnv proxyFunc, r resolver) proxyFunc {
	return func(req *http.Request) (*url.URL, error) {
		host := strings.TrimSuffix(strings.ToLower(req.URL.Hostname()), ".")
		if metadataNames[host] {
			return nil, errDenied
		}
		var addrs []netip.Addr
		if a, err := netip.ParseAddr(host); err == nil {
			addrs = []netip.Addr{a}
		} else {
			ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
			defer cancel()
			if addrs, err = r.LookupNetIP(ctx, "ip", host); err != nil {
				return nil, fmt.Errorf("llm/gemini: resolve model host: %w", err)
			}
		}
		for _, a := range addrs {
			if err := dialAllowed(base, netip.AddrPortFrom(a, 443).String()); err != nil {
				return nil, err
			}
		}
		return fromEnv(req)
	}
}

// hostGuard refuses any request to a host other than the configured one, so
// neither the key nor a token can leave for another host.
type hostGuard struct {
	host string
	base http.RoundTripper
}

func (g *hostGuard) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.URL.Host, g.host) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("%w: request to a host other than the configured base URL", errDenied)
	}
	return g.base.RoundTrip(req)
}

// apiKeyTransport adds the Gemini API key header.
type apiKeyTransport struct {
	key  netsec.Secret
	base http.RoundTripper
}

func (t *apiKeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Set("x-goog-api-key", t.key.Reveal())
	return t.base.RoundTrip(r)
}

// defaultTokenSource looks up Google application default credentials with
// client (the credential transport, which may reach the metadata server) set
// as the oauth2 HTTP client of the context the token source keeps.
func defaultTokenSource(ctx context.Context, client *http.Client) (oauth2.TokenSource, error) {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	cctx := context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient, client)
	creds, err := google.FindDefaultCredentials(cctx, vertexScope)
	if err != nil {
		return nil, fmt.Errorf("llm/gemini: vertex: application default credentials: %w", err)
	}
	return oauth2.ReuseTokenSource(nil, creds.TokenSource), nil
}

// maxErrorBody caps APIError.Body and debug text.
const maxErrorBody = 2 << 10

// safeText redacts secrets in s and cuts it to maxErrorBody bytes.
func safeText(s string) string {
	s, _ = redact.Text(s)
	if len(s) <= maxErrorBody {
		return s
	}
	s = s[:maxErrorBody]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
