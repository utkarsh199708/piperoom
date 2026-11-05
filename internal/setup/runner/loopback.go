package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
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
// loopback IP literal, or a name whose resolved addresses are all loopback. A
// name that resolves to a mix of loopback and non-loopback addresses, or that
// fails to resolve (including to no addresses), is not loopback, so --yes
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
	if len(ips) == 0 {
		return false
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}
	return true
}

// ErrDialNotLoopback reports that the WebSocket dial would connect to a
// non-loopback remote address while the runner had decided the server was
// loopback (a DNS rebinding answer). The connection is refused.
var ErrDialNotLoopback = errors.New("setup: dial target resolved off loopback")

// loopbackControl is a net.Dialer.Control that refuses to connect to a
// non-loopback remote address. It pins a loopback dial to loopback: net calls
// Control per connection attempt with the resolved remote address, so even a
// name that re-resolves to a public address between the loopback check and the
// connect is refused rather than dialed.
func loopbackControl(_ string, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("setup: parsing dial target %q: %w", address, err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%w: %s", ErrDialNotLoopback, host)
	}
	return nil
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
