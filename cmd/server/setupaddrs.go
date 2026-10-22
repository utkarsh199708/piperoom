package main

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"time"
)

// The server's own side of the setup target denylist (docs/design/setup-agent.md
// §2.6): whether it listens on loopback only (a loopback target is then this
// machine, which quickstart/standalone allows), and which addresses are its
// own (a target resolving to one of them is refused).

// setupListenLoopback reports whether the server's listen address is loopback
// only. A malformed address is not loopback: the listener refuses it later.
func setupListenLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return isLoopbackHost(host)
}

// setupOwnAddrs returns the server's own non-loopback addresses: the IP of the
// listen address, the host's interface addresses when it listens on every
// interface, or the addresses the listen host name resolves to. It is best
// effort (a failure is a debug line and no addresses), because the target
// check must not depend on the resolver.
func setupOwnAddrs(addr string, log *slog.Logger) []netip.Addr {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil
	}
	host = strings.Trim(host, "[]")
	if a, err := netip.ParseAddr(host); err == nil {
		a = a.Unmap()
		switch {
		case a.IsUnspecified():
			return interfaceAddrs(log)
		case a.IsLoopback():
			// Loopback is governed by setupListenLoopback, not by this list.
			return nil
		}
		return []netip.Addr{a}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		log.Debug("server: setup target denylist: listen host does not resolve", "host", host, "err", err)
		return nil
	}
	return ownOnly(addrs)
}

// interfaceAddrs returns the host's non-loopback, non-link-local addresses.
func interfaceAddrs(log *slog.Logger) []netip.Addr {
	list, err := net.InterfaceAddrs()
	if err != nil {
		log.Debug("server: setup target denylist: interface addresses unavailable", "err", err)
		return nil
	}
	out := make([]netip.Addr, 0, len(list))
	for _, a := range list {
		ipn, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(ipn.IP)
		if !ok {
			continue
		}
		out = append(out, ip)
	}
	return ownOnly(out)
}

// ownOnly drops loopback, unspecified and link-local addresses from a list.
func ownOnly(in []netip.Addr) []netip.Addr {
	out := make([]netip.Addr, 0, len(in))
	for _, a := range in {
		a = a.WithZone("").Unmap()
		if a.IsLoopback() || a.IsUnspecified() || a.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, a)
	}
	return out
}
