package runner

import (
	"context"
	"errors"
	"net"
	"strings"
)

// LookupIPFunc resolves a host to its addresses; the Runner uses it to decide
// loopback. Tests inject a fixed resolver.
type LookupIPFunc func(ctx context.Context, host string) ([]net.IP, error)

// defaultLookupIP is the system resolver.
func defaultLookupIP(ctx context.Context, host string) ([]net.IP, error) {
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// isLoopback reports whether the server URL's host is a loopback address.
func (r *Runner) isLoopback(ctx context.Context) bool {
	return isLoopbackHost(ctx, r.url.Hostname(), r.lookupIP)
}

// isLoopbackHost reports whether host is loopback: the name "localhost", a
// loopback IP literal, or a name that resolves to a loopback IP. Anything
// else, including a name that fails to resolve, is not loopback, so --yes
// stays refused and TLS stays required (fail closed).
func isLoopbackHost(ctx context.Context, host string, lookup LookupIPFunc) bool {
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	ips, err := lookup(ctx, host)
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if ip.IsLoopback() {
			return true
		}
	}
	return false
}

// ErrPlainHTTPOffLoopback reports a server origin of http:// whose host is not
// loopback: the transport must be TLS unless loopback.
var ErrPlainHTTPOffLoopback = errors.New("setup: refuse a plain http server off loopback (TLS is required)")

// wsURL builds the WebSocket URL from the server origin. http and https map
// to ws and wss; a plain http server is refused off loopback.
func (r *Runner) wsURL(loopback bool) (string, error) {
	u := *r.url
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		if !loopback {
			return "", ErrPlainHTTPOffLoopback
		}
		u.Scheme = "ws"
	}
	u.Path = wsPath
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
