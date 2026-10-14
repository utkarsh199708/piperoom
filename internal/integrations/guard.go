package integrations

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"
)

// ErrBlockedHost marks a dial the SSRF guard refused: a cloud metadata
// address (always), or a host outside PIPEROOM_INTEGRATION_ALLOWED_HOSTS.
var ErrBlockedHost = errors.New("integrations: host not allowed")

// metadataPrefixes are blocked whatever the allow list says (integrations.md
// §8): link-local IPv4 (AWS/GCP/Azure/Oracle metadata live at
// 169.254.169.254), IPv6 link-local, the AWS and GCP IPv6 metadata
// endpoints and Alibaba Cloud's metadata address.
var metadataPrefixes = []netip.Prefix{
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("100.100.100.200/32"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fd00:ec2::254/128"),
	netip.MustParsePrefix("fd20:ce::254/128"),
}

// embedsIPv4 are IPv6 ranges whose last 32 bits are an IPv4 address a
// router may forward to: the NAT64 well-known prefix and the deprecated
// IPv4-compatible form (::a.b.c.d), which Unmap does not touch.
var embedsIPv4 = []netip.Prefix{
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("::/96"),
}

// isMetadata reports whether a (or the IPv4 address it embeds) is a cloud
// metadata address.
func isMetadata(a netip.Addr) bool {
	a = a.Unmap()
	for _, p := range metadataPrefixes {
		if p.Contains(a) {
			return true
		}
	}
	if a.Is6() {
		for _, p := range embedsIPv4 {
			if p.Contains(a) {
				b := a.As16()
				v4 := netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})
				for _, mp := range metadataPrefixes {
					if mp.Contains(v4) {
						return true
					}
				}
			}
		}
	}
	return false
}

// Resolver looks up the addresses of a host. *net.Resolver satisfies it.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Guard decides which hosts an integration test or client may dial. Test
// dials a host an admin typed, so without it the server would reach any
// address its network can (SSRF). Build it with NewGuard.
type Guard struct {
	hosts    map[string]bool // exact host names, lower case
	suffixes []string        // "*.corp.example" entries, stored as ".corp.example"
	nets     []netip.Prefix
	resolver Resolver
	dialer   *net.Dialer
}

// NewGuard parses PIPEROOM_INTEGRATION_ALLOWED_HOSTS: a comma list of host
// names ("kafka.corp", "*.corp.example"), IP addresses and CIDRs. Empty
// allows every host except the metadata addresses. resolver nil uses
// net.DefaultResolver.
func NewGuard(allowed string, resolver Resolver) (*Guard, error) {
	g := &Guard{hosts: map[string]bool{}, resolver: resolver, dialer: &net.Dialer{Timeout: 10 * time.Second}}
	if g.resolver == nil {
		g.resolver = net.DefaultResolver
	}
	for _, raw := range strings.Split(allowed, ",") {
		e := strings.ToLower(strings.TrimSpace(raw))
		if e == "" {
			continue
		}
		if p, err := netip.ParsePrefix(e); err == nil {
			g.nets = append(g.nets, p.Masked())
			continue
		}
		if a, err := netip.ParseAddr(e); err == nil {
			g.nets = append(g.nets, netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()))
			continue
		}
		if strings.HasPrefix(e, "*.") && validHostname(e[2:]) {
			g.suffixes = append(g.suffixes, e[1:])
			continue
		}
		if !validHostname(e) {
			return nil, fmt.Errorf("integrations: PIPEROOM_INTEGRATION_ALLOWED_HOSTS: entry %q is not a host, IP or CIDR", raw)
		}
		g.hosts[e] = true
	}
	return g, nil
}

func (g *Guard) open() bool { return len(g.hosts) == 0 && len(g.suffixes) == 0 && len(g.nets) == 0 }

func (g *Guard) hostListed(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if g.hosts[host] {
		return true
	}
	for _, s := range g.suffixes {
		if strings.HasSuffix(host, s) {
			return true
		}
	}
	return false
}

func (g *Guard) addrAllowed(a netip.Addr, hostListed bool) error {
	a = a.Unmap()
	if isMetadata(a) {
		return fmt.Errorf("%w: %s is a cloud metadata address", ErrBlockedHost, a)
	}
	if g.open() || hostListed {
		return nil
	}
	for _, p := range g.nets {
		if p.Contains(a) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s is not in PIPEROOM_INTEGRATION_ALLOWED_HOSTS", ErrBlockedHost, a)
}

// Resolve checks host and returns the addresses it may be dialed at. Every
// address of a name must pass, so a name cannot smuggle a metadata address
// next to an allowed one.
func (g *Guard) Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	host = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(host, "]"), "["), ".")
	if host == "" {
		return nil, fmt.Errorf("%w: empty host", ErrBlockedHost)
	}
	if a, err := netip.ParseAddr(host); err == nil {
		if err := g.addrAllowed(a, false); err != nil {
			return nil, err
		}
		return []netip.Addr{a.Unmap()}, nil
	}
	listed := g.hostListed(host)
	if !g.open() && !listed && len(g.nets) == 0 {
		return nil, fmt.Errorf("%w: %s is not in PIPEROOM_INTEGRATION_ALLOWED_HOSTS", ErrBlockedHost, host)
	}
	addrs, err := g.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("integrations: resolving %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("integrations: resolving %s: no addresses", host)
	}
	for _, a := range addrs {
		if err := g.addrAllowed(a, listed); err != nil {
			return nil, err
		}
	}
	return addrs, nil
}

// CheckHost is Resolve without the addresses: a pre-flight for clients whose
// own dialer cannot be replaced (postgres.New). Such a client resolves the
// name again, so a DNS answer that changes in between is not caught there.
func (g *Guard) CheckHost(ctx context.Context, host string) error {
	_, err := g.Resolve(ctx, host)
	return err
}

// DialContext dials addr ("host:port") through the guard. It connects to the
// checked address itself, so a DNS answer cannot change between check and dial.
func (g *Guard) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("integrations: bad address %q: %w", addr, err)
	}
	addrs, err := g.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	var last error
	for _, a := range addrs {
		c, err := g.dialer.DialContext(ctx, network, net.JoinHostPort(a.String(), port))
		if err == nil {
			return c, nil
		}
		last = err
		if ctx.Err() != nil {
			break
		}
	}
	return nil, last
}

// validHostname reports whether s looks like a DNS name (letters, digits,
// '-', '_', dots; no empty labels).
func validHostname(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
				return false
			}
		}
	}
	return true
}
