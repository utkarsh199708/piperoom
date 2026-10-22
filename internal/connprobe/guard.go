package connprobe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// Guard decides which addresses a probe may dial. A connection test makes the
// server connect where an admin says, so it must not become a way to reach the
// cloud metadata service or other link-local endpoints (SSRF). The check is on
// the resolved address, and DialContext dials exactly the address it checked,
// so DNS cannot answer one address to the check and another to the dial.
type Guard struct {
	// AllowLoopback allows 127.0.0.0/8 and ::1. Phase 1 tests only
	// connections the operator configured, where a local dev stack is normal,
	// so the API sets it; user-entered connections (phase 2) will not.
	AllowLoopback bool
	// AllowPrivate allows RFC 1918, ULA and CGNAT ranges. Real pipelines
	// live there, so the API sets it.
	AllowPrivate bool
	// Resolver looks names up; nil means net.DefaultResolver.
	Resolver interface {
		LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
	}
	// Dialer dials a checked address; nil means a net.Dialer with a 10 s
	// timeout.
	Dialer interface {
		DialContext(ctx context.Context, network, address string) (net.Conn, error)
	}
}

// ErrBlockedAddress wraps every refusal, so callers can tell a guard refusal
// from a network error with errors.Is.
var ErrBlockedAddress = errors.New("connprobe: address not allowed")

// metadataHosts are names that resolve to metadata services; refused by name
// as well as by address.
var metadataHosts = map[string]bool{
	"metadata.google.internal": true,
	"metadata":                 true,
	"instance-data":            true,
}

// alwaysBlocked are address ranges no probe may reach: link-local (cloud
// metadata lives at 169.254.169.254 and fd00:ec2::254), unspecified,
// multicast and broadcast, and the Alibaba metadata address.
var alwaysBlocked = []netip.Prefix{
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fd00:ec2::254/128"),
	netip.MustParsePrefix("100.100.100.200/32"),
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("ff00::/8"),
	netip.MustParsePrefix("255.255.255.255/32"),
}

var privateRanges = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("fc00::/7"),
}

// IPv6 transition-mechanism prefixes that embed an IPv4 address a router may
// deliver the packet to: NAT64 (well-known and local-use), 6to4 and Teredo.
var (
	nat64WellKnown = netip.MustParsePrefix("64:ff9b::/96")
	nat64LocalUse  = netip.MustParsePrefix("64:ff9b:1::/48")
	sixToFour      = netip.MustParsePrefix("2002::/16")
	teredo         = netip.MustParsePrefix("2001::/32")
)

// embeddedV4 returns the IPv4 address an IPv6 transition-mechanism address
// carries, if any. On a network that routes these, the packet reaches that
// IPv4 host, so the guard judges the embedded address too.
func embeddedV4(a netip.Addr) (netip.Addr, bool) {
	if !a.Is6() || a.Is4In6() {
		return netip.Addr{}, false
	}
	b := a.As16()
	switch {
	case nat64WellKnown.Contains(a) || nat64LocalUse.Contains(a):
		return netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}), true
	case sixToFour.Contains(a):
		return netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]}), true
	case teredo.Contains(a):
		// Teredo stores the client IPv4 inverted in the last four bytes.
		return netip.AddrFrom4([4]byte{^b[12], ^b[13], ^b[14], ^b[15]}), true
	}
	return netip.Addr{}, false
}

// CheckAddr reports whether the guard allows a. IPv4-mapped IPv6 addresses
// are checked as IPv4, so ::ffff:169.254.169.254 is refused like the plain
// one, and an address that embeds an IPv4 address (NAT64, 6to4, Teredo) is
// refused whenever the embedded address would be.
func (g *Guard) CheckAddr(a netip.Addr) error {
	a = a.Unmap()
	if v4, ok := embeddedV4(a); ok {
		if err := g.CheckAddr(v4); err != nil {
			return fmt.Errorf("%w (embedded in %s)", err, a)
		}
	}
	for _, p := range alwaysBlocked {
		if p.Contains(a) {
			return fmt.Errorf("%w: %s is a link-local, metadata or reserved address", ErrBlockedAddress, a)
		}
	}
	if a.IsLoopback() && !g.AllowLoopback {
		return fmt.Errorf("%w: %s is a loopback address", ErrBlockedAddress, a)
	}
	if !g.AllowPrivate {
		for _, p := range privateRanges {
			if p.Contains(a) {
				return fmt.Errorf("%w: %s is a private address", ErrBlockedAddress, a)
			}
		}
	}
	return nil
}

// Resolve looks host up and returns its addresses if every one is allowed.
// One refused address refuses the host: a name that resolves to both a public
// and a metadata address is not safe to dial.
func (g *Guard) Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	h := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if h == "" {
		return nil, fmt.Errorf("%w: empty host", ErrBlockedAddress)
	}
	if metadataHosts[h] {
		return nil, fmt.Errorf("%w: %s is a metadata service name", ErrBlockedAddress, h)
	}
	if a, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		if err := g.CheckAddr(a); err != nil {
			return nil, err
		}
		return []netip.Addr{a}, nil
	}
	var r interface {
		LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
	} = net.DefaultResolver
	if g.Resolver != nil {
		r = g.Resolver
	}
	addrs, err := r.LookupNetIP(ctx, "ip", h)
	if err != nil {
		return nil, fmt.Errorf("connprobe: resolve %s: %w", h, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("connprobe: resolve %s: no addresses", h)
	}
	for _, a := range addrs {
		if err := g.CheckAddr(a); err != nil {
			return nil, err
		}
	}
	return addrs, nil
}

// DialContext resolves addr's host, checks every address, and dials the first
// that answers, by IP. Use it as the dial function of every client a probe
// builds (http.Transport.DialContext, pgconn.Config.DialFunc, kgo.Dialer).
func (g *Guard) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("connprobe: dial %q: %w", addr, err)
	}
	addrs, err := g.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	var d interface {
		DialContext(ctx context.Context, network, address string) (net.Conn, error)
	} = &net.Dialer{Timeout: DefaultStepTimeout, KeepAlive: 30 * time.Second}
	if g.Dialer != nil {
		d = g.Dialer
	}
	var last error
	for _, a := range addrs {
		c, err := d.DialContext(ctx, network, net.JoinHostPort(a.String(), port))
		if err == nil {
			return c, nil
		}
		last = err
	}
	return nil, fmt.Errorf("connprobe: dial %s: %w", host, last)
}

// CheckURL checks the host of a URL (Connect, Schema Registry) before a probe
// uses it; DialContext checks again at dial time.
func (g *Guard) CheckURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("connprobe: not a URL with a host")
	}
	_, err = g.Resolve(ctx, u.Hostname())
	return err
}
