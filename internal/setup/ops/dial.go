package ops

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"

	"piperoom/internal/connprobe"
)

// errRedirectRefused fails a request that answers with a redirect, so a
// listed target cannot bounce the runner to an address outside Env.Targets.
var errRedirectRefused = errors.New("ops: refusing a redirect")

// dialGuard classifies the address the kernel is about to connect to. It
// reuses connprobe's ranges (link-local, cloud metadata, multicast,
// unspecified) and allows loopback and RFC 1918 because a setup room may
// target a local dev stack; the target allow-list is what keeps http_get and
// net_probe to registered hosts.
var dialGuard = connprobe.Guard{AllowLoopback: true, AllowPrivate: true}

// newGuardedDialer returns the one net.Dialer every runner connection uses
// (http_get, net_probe, later the remote clients). Its ControlContext hook
// receives the resolved address the kernel is about to dial, so DNS cannot
// answer one address to an earlier check and another to connect; there is no
// earlier resolve whose answer could differ.
func newGuardedDialer(resolver *net.Resolver) *net.Dialer {
	d := &net.Dialer{Timeout: defaultOpTimeout, KeepAlive: 30 * time.Second, Resolver: resolver}
	d.ControlContext = func(_ context.Context, network, address string, _ syscall.RawConn) error {
		return checkDialAddress(network, address)
	}
	return d
}

// checkDialAddress refuses a dial to a denylisted address, naming the address
// in the error so a test and a person can see what DNS answered.
func checkDialAddress(_, address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("ops: dial guard: %w", err)
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("ops: dial guard: %q is not an IP address", host)
	}
	if err := dialGuard.CheckAddr(a); err != nil {
		return fmt.Errorf("ops: refusing dial to %s: %w", net.JoinHostPort(host, port), err)
	}
	return nil
}

// newGuardedHTTPClient builds the *http.Client http_get uses: one client, a
// timeout, no proxy, no redirects, and the guarded dialer.
func newGuardedHTTPClient(d *net.Dialer) *http.Client {
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           d.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		DisableKeepAlives:     true,
		// A hostile endpoint must not make the runner buffer the default
		// 10 MiB of response headers; the body is capped far lower too.
		MaxResponseHeaderBytes: 64 << 10,
	}
	return &http.Client{
		Transport: tr,
		Timeout:   defaultOpTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errRedirectRefused
		},
	}
}
