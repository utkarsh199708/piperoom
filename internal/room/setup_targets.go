package room

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode"

	"piperoom/internal/connprobe"
	"piperoom/internal/events"
)

// maxSetupAddressChars caps a target address. An address is a host, a host:port,
// a URL or a kubectl context and namespace; anything longer is a paste error.
const maxSetupAddressChars = 512

// setupHostNameRE is the shape of a host the resolver is asked about: a name,
// an IPv4 literal or a bracketed/trailing-colon-free IPv6 literal. A space, a
// slash or a '@' (user info pasted into the host) is refused before any lookup.
var setupHostNameRE = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// hasControlChars reports whether s holds a control character. Such text
// cannot be stored (Postgres jsonb refuses NUL) and is never a host.
func hasControlChars(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f || unicode.Is(unicode.Cf, r) {
			return true
		}
	}
	return false
}

// Setup targets and classes (docs/design/setup-agent.md §2.6, §3.5): the
// address denylist the server applies when a driver registers a target, and
// the class it derives for one. The runner re-resolves every name on its own
// network and checks the address it dials (U1-2), so the server's check is the
// first filter, not the guarantee.

// targetResolveTimeout bounds the name lookup at registration. The server
// answers 400 address_refused for a refused address; a name the server cannot
// resolve is accepted (the runner's dial-time check still applies), so a
// corporate name behind split DNS is not blocked by the server's resolver.
const targetResolveTimeout = 5 * time.Second

// TargetResolver looks a host name up. *net.Resolver implements it; tests
// inject a fake. Nil means net.DefaultResolver.
type TargetResolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// setupTargetKinds are the kinds events.SetupTarget documents.
// "image_registry" is the container registry a Strimzi KafkaConnect build
// pushes to: the Strimzi driver refuses a build whose registry is not one.
var setupTargetKinds = map[string]bool{
	"postgres": true, "kafka": true, "connect": true, "registry": true, "image_registry": true,
	"kubernetes": true, "aws": true, "confluent": true, "aiven": true, "host": true,
}

// networkTargetKinds are the kinds whose address is dialed, so the denylist
// applies. The others name a kubectl context, a namespace or a cloud account
// and have no IP the server could check.
var networkTargetKinds = map[string]bool{
	"postgres": true, "kafka": true, "connect": true, "registry": true, "image_registry": true, "host": true,
}

// Class ranks. A class is a floor that only rises; production outranks shared,
// shared outranks local.
var setupClassRank = map[string]int{
	protocolClassLocal: 0, protocolClassShared: 1, protocolClassProduction: 2,
}

// protocolClassLocal, protocolClassShared and protocolClassProduction mirror
// protocol.ClassLocal and the two next to it. The room does not import the
// protocol package for three strings: they are the wire contract's words and
// are repeated here as constants for validation only.
const (
	protocolClassLocal      = "local"
	protocolClassShared     = "shared"
	protocolClassProduction = "production"
)

// validSetupClass reports whether class is one the API accepts. An empty class
// means "no floor": the server derives one.
func validSetupClass(class string) bool {
	_, ok := setupClassRank[class]
	return ok
}

// classAtLeast raises a class to the floor: max(rank(a), rank(b)). An invalid
// (empty) side reads as no class, so the other side stands; both invalid is
// "".
func classAtLeast(a, b string) string {
	if !validSetupClass(a) {
		return b
	}
	if !validSetupClass(b) {
		return a
	}
	if setupClassRank[b] > setupClassRank[a] {
		return b
	}
	return a
}

// setupTargetHost returns the host of one target address: a URL's host, the
// host of host:port, or the address itself. It is "" for an address with no
// host (an empty one, or a URL without one).
func setupTargetHost(address string) string {
	raw := strings.TrimSpace(address)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return ""
		}
		return strings.TrimSuffix(u.Hostname(), ".")
	}
	if strings.HasPrefix(raw, "[") {
		// A bracketed IPv6 literal, with or without a port.
		if i := strings.Index(raw, "]"); i > 1 {
			return raw[1:i]
		}
		return ""
	}
	if h, _, err := net.SplitHostPort(raw); err == nil {
		return strings.TrimSuffix(h, ".")
	}
	return strings.TrimSuffix(raw, ".")
}

// setupTargetHosts splits an address into its hosts: a Kafka target may name a
// comma-separated broker list, and every broker is checked.
func setupTargetHosts(address string) []string {
	parts := strings.Split(address, ",")
	hosts := make([]string, 0, len(parts))
	for _, p := range parts {
		hosts = append(hosts, setupTargetHost(p))
	}
	return hosts
}

// checkSetupTarget validates one target's kind and address and applies the
// denylist of design §2.6: link-local (cloud metadata lives there), metadata
// service names and addresses, multicast, unspecified, loopback of another
// host and this server's own addresses are refused. Private ranges are
// allowed: a corporate database at 10.x or 192.168.x is the normal case.
//
// The denylist is not limited to the network kinds: a registered target of any
// kind is what the ops layer allow-lists for http_get and net_probe, so an
// address that parses to a host is checked whatever kind carries it. A
// non-network kind may still name an opaque identifier that is not host-shaped
// (a kubectl context with a namespace, a cloud account alias with a path), and
// only a network kind must name a host.
func (s *Service) checkSetupTarget(ctx context.Context, kind, address string) error {
	if !setupTargetKinds[kind] {
		return validation(fmt.Sprintf("target kind %q: want one of postgres, kafka, connect, registry, image_registry, kubernetes, aws, confluent, aiven, host", kind))
	}
	raw := strings.TrimSpace(address)
	if raw == "" {
		return validation("target address is required")
	}
	if err := checkLen("target address", raw, maxSetupAddressChars); err != nil {
		return err
	}
	if hasControlChars(raw) {
		return validation("target address contains characters that cannot be stored")
	}
	for _, part := range strings.Split(raw, ",") {
		if setupTargetURLHasUserinfo(part) {
			// The address would be stored verbatim in events, snapshots and
			// exports, so a credential in it is refused before anything is
			// written or resolved.
			return credentialRefused()
		}
		host := setupTargetHost(part)
		if host == "" || !setupHostNameRE.MatchString(host) {
			if networkTargetKinds[kind] {
				return validation("target address must name a host, a host:port or a URL with a host")
			}
			// An opaque identifier of a non-network kind: nothing the server
			// could resolve or deny.
			continue
		}
		if err := s.checkSetupTargetHost(ctx, host); err != nil {
			return err
		}
	}
	return nil
}

// setupTargetURLHasUserinfo reports whether a URL-form address carries user
// info. A non-URL address never does: user info pasted into a bare host fails
// setupHostNameRE's shape instead.
func setupTargetURLHasUserinfo(address string) bool {
	raw := strings.TrimSpace(address)
	if !strings.Contains(raw, "://") {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && u.User != nil
}

// checkSetupTargetHost resolves one host and applies the denylist to every
// address it resolves to. A name the resolver cannot answer is accepted: the
// server cannot judge what it cannot see, and the runner checks the address it
// dials.
func (s *Service) checkSetupTargetHost(ctx context.Context, host string) error {
	guard := &connprobe.Guard{
		// Loopback is this machine only on a standalone/quickstart server; on
		// a team server a loopback target is another host's.
		AllowLoopback: s.setupLocal,
		AllowPrivate:  true,
		Resolver:      s.setupResolver,
	}
	rctx, cancel := context.WithTimeout(ctx, targetResolveTimeout)
	defer cancel()
	addrs, err := guard.Resolve(rctx, host)
	if err != nil {
		if errors.Is(err, connprobe.ErrBlockedAddress) {
			return addressRefused()
		}
		s.log.Debug("room: setup target name did not resolve; the runner checks it at dial time", "host", host, "err", err)
		return nil
	}
	for _, a := range addrs {
		if s.isOwnAddr(a) {
			return addressRefused()
		}
	}
	return nil
}

// isOwnAddr reports whether a is one of this server's own addresses. The
// comparison is on the unmapped, zone-less form, as connprobe's guard does.
func (s *Service) isOwnAddr(a netip.Addr) bool {
	a = a.WithZone("").Unmap()
	for _, own := range s.setupOwn {
		if own.WithZone("").Unmap() == a {
			return true
		}
	}
	return false
}

// setupClass derives a target's class from what the server can see without the
// runner: a production pattern (PIPEROOM_SETUP_PROD_PATTERNS: a host glob, a
// kubectl context or a cloud account id) makes it production; everything else
// is shared. "local" needs the runner's Hello and env_detect (U1-8's
// classifier). reason is one of the fixed codes setup.target_classed
// documents.
func (s *Service) setupClass(kind, address string) (class, reason string) {
	if len(s.setupCfg.ProdPatterns) == 0 {
		return protocolClassShared, "default"
	}
	candidates := []string{strings.ToLower(strings.TrimSpace(address))}
	for _, host := range setupTargetHosts(address) {
		if host != "" {
			candidates = append(candidates, strings.ToLower(host))
		}
	}
	if kind == "kubernetes" {
		// A kubectl context and namespace: a prod pattern names the context.
		if c, _, ok := strings.Cut(address, "/"); ok && c != "" {
			candidates = append(candidates, strings.ToLower(c))
		}
	}
	for _, pattern := range s.setupCfg.ProdPatterns {
		p := strings.ToLower(strings.TrimSpace(pattern))
		if p == "" {
			continue
		}
		for _, c := range candidates {
			if ok, err := path.Match(p, c); err == nil && ok {
				return protocolClassProduction, "prod_pattern"
			}
		}
	}
	return protocolClassShared, "default"
}

// setupTargetPlan is one target after validation: the target the room stores,
// the class after the person's floor and the derived class are combined, and
// who decided it (setup.target_classed.by).
type setupTargetPlan struct {
	target  events.SetupTarget
	class   string
	reason  string
	by      string // "server" | "person"
	derived string
}

// planSetupTarget validates one target and combines the person's floor with the
// derived class: max(floor, derived) and never below shared. A floor below the
// derived class is refused (400 class_lowered), as the route's docs say.
func (s *Service) planSetupTarget(ctx context.Context, t events.SetupTarget) (setupTargetPlan, error) {
	kind := strings.TrimSpace(t.Kind)
	address := strings.TrimSpace(t.Address)
	floor := strings.TrimSpace(t.Class)
	if floor != "" && !validSetupClass(floor) {
		return setupTargetPlan{}, validation(fmt.Sprintf("target class %q: want local, shared or production", floor))
	}
	if err := s.checkSetupTarget(ctx, kind, address); err != nil {
		return setupTargetPlan{}, err
	}
	derived, reason := s.setupClass(kind, address)
	min := classAtLeast(derived, protocolClassShared)
	if floor != "" && setupClassRank[floor] < setupClassRank[min] {
		return setupTargetPlan{}, classLowered(fmt.Sprintf("target class %q is below the derived class %q; a class can only be raised", floor, min))
	}
	effective := min
	by := "server"
	if floor != "" {
		// The person stated the class — even when it equals the derivation,
		// the statement is theirs, and recording it as the server's would
		// read as refinable server output in the fold (applyClass). A floor
		// below min was refused above, so effective only rises here.
		effective = floor
		reason = "person"
		by = "person"
	}
	return setupTargetPlan{
		target:  events.SetupTarget{Kind: kind, Address: address, Class: effective},
		class:   effective,
		reason:  reason,
		by:      by,
		derived: derived,
	}, nil
}
