package nettools

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/connprobe"
)

// WorkerResolver answers from the network position of one Kafka Connect
// worker: its own name resolution (the hosts file and DNS servers its
// processes see) and a TCP connect that starts on the worker. It is the seam
// connect_resolve reads through. The room service cannot answer either
// question itself (its resolver and routes are not the worker's), and Kafka
// Connect's REST API exposes neither: a validation that cannot reach the
// database returns the JDBC driver's text, which names no address.
//
// An implementation only reads: it resolves a name and opens and closes one
// TCP connection, sends no byte and changes nothing on the worker. It takes
// its worker from configuration, never from the model.
type WorkerResolver interface {
	// Worker names the worker the answers come from (a Compose service, a
	// pod, a host), for the check's text. It is never a credential.
	Worker() string
	// Lookup resolves host the way the worker resolves it, in the worker
	// resolver's order. A name the worker's resolver answers does not exist
	// wraps ErrWorkerNoSuchHost; any other error means the worker could not
	// be asked (it is down, the path to it answered 404, it timed out).
	Lookup(ctx context.Context, host string) ([]netip.Addr, error)
	// Reach opens and closes one TCP connection from the worker to addr.
	// nil means it connected. ErrWorkerRefused and ErrWorkerTimeout are the
	// worker's own answers (refused; no answer within the worker's connect
	// timeout); a context error means the step ended first; any other error
	// means the worker could not be asked.
	Reach(ctx context.Context, addr netip.AddrPort) error
}

// Sentinel answers of a WorkerResolver. They are what the worker saw, not a
// failure to ask it, so the check reports them as findings.
var (
	// ErrWorkerNoSuchHost is the worker's resolver answering that the name
	// does not exist.
	ErrWorkerNoSuchHost = errors.New("nettools: the worker's resolver has no such host")
	// ErrWorkerRefused is a TCP connect from the worker that was refused.
	ErrWorkerRefused = errors.New("nettools: the connection from the worker was refused")
	// ErrWorkerTimeout is a TCP connect from the worker that got no answer
	// within the worker's own connect timeout.
	ErrWorkerTimeout = errors.New("nettools: the connection from the worker timed out")
)

// resolveID is the check id of ConnectResolveCheck.
const resolveID = "connect_resolve"

// maxResolvedShown caps the addresses one check names.
const maxResolvedShown = 5

// workerDialGuard is what the check lets the worker reach: loopback and
// private ranges are allowed (on the worker, loopback is the worker itself,
// and a hosts entry pointing there is a real fault worth showing), but the
// check never asks a worker to open a connection to a link-local, metadata,
// multicast or unspecified address. The address is still reported.
var workerDialGuard = connprobe.Guard{AllowLoopback: true, AllowPrivate: true}

// documentationRanges are the RFC 5737 and RFC 3849 ranges reserved for
// documentation. Nothing routes them, so a name that resolves into one is
// pointed at a black hole: the check says so, because that is the finding.
var documentationRanges = []netip.Prefix{
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("2001:db8::/32"),
}

// ConnectResolveCheck reports what the Kafka Connect worker w resolves host
// to and whether the first address, the one a JVM client connects to, answers
// TCP on port from the worker. local is what Piperoom itself resolves host to
// (nil when unknown); the check names both when they differ, which is the
// contrast that tells a stale hosts entry or split DNS on the worker from a
// database that is down. Lookup and Reach each run within one step (step, or
// 3 s when 0), under ctx's deadline.
//
// A wrong answer is StatusFail; a worker that could not be asked is
// StatusWarn, since that says nothing about the database. The worker's error
// text is untrusted: it is redacted and clipped, and only addresses parsed by
// netip are echoed whole.
func ConnectResolveCheck(ctx context.Context, w WorkerResolver, host string, port int, local []netip.Addr, step time.Duration) Check {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if w == nil {
		return Check{ID: resolveID, Status: StatusSkip, Detail: "no view from inside a Connect worker is configured"}
	}
	if host == "" || port < 1 || port > 65535 {
		return Check{ID: resolveID, Status: StatusSkip, Detail: "the connector names no database host and port"}
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	worker := "the Connect worker"
	if name := clip(w.Worker()); name != "" {
		worker += " " + name
	}
	start := time.Now()
	ms := func() int64 { return time.Since(start).Milliseconds() }
	fail := func(problem, fix string) Check {
		return Check{ID: resolveID, Addr: addr, Status: StatusFail, MS: ms(), Problem: problem, Fix: fix}
	}
	warn := func(problem string) Check {
		return Check{ID: resolveID, Addr: addr, Status: StatusWarn, MS: ms(), Problem: problem,
			Fix: "check that the Connect worker, and the path Piperoom asks it through, are up, then re-run the probe"}
	}

	var addrs []netip.Addr
	if ip, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		addrs = []netip.Addr{ip.WithZone("")}
	} else {
		lctx, cancel := context.WithTimeout(ctx, stepOr(step))
		got, err := w.Lookup(lctx, host)
		lerr := lctx.Err()
		cancel()
		switch {
		case err == nil:
			addrs = got
		case errors.Is(err, ErrWorkerNoSuchHost):
			return fail(worker+" cannot resolve "+host+localSuffix(local, nil)+": "+redactedText(err.Error()),
				"make the name resolve on the worker (its DNS servers, its hosts file), or set database.hostname to a name the worker resolves")
		case lerr != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
			return fail("the name lookup of "+host+" on "+worker+" did not finish within the step's time"+localSuffix(local, nil),
				"check the DNS servers the worker uses and that they answer from the worker's network")
		default:
			return warn("could not ask " + worker + " to resolve " + host + ": " + redactedText(err.Error()))
		}
	}
	if len(addrs) == 0 {
		return fail(worker+" has no addresses for "+host+localSuffix(local, nil),
			"make the name resolve on the worker (its DNS servers, its hosts file)")
	}
	resolved := worker + " resolves " + host + " to " + joinAddrs(addrs) + blackHoleNote(addrs[0]) + localSuffix(local, addrs)
	differs := len(local) > 0 && !sameAddrs(local, addrs)
	fixFor := func(what string) string {
		if differs {
			return "the worker resolves " + host + " differently from Piperoom: check the worker's hosts file (extra_hosts, an edited /etc/hosts) and its DNS servers, then restart the worker so its JVM drops any cached address"
		}
		return what
	}

	first := addrs[0].WithZone("")
	if err := workerDialGuard.CheckAddr(first); err != nil {
		return fail(resolved+"; that is a link-local, metadata or reserved address, so the worker was not asked to connect to it",
			fixFor("make "+host+" resolve on the worker to the database's address"))
	}
	target := netip.AddrPortFrom(first, uint16(port))
	rctx, cancel := context.WithTimeout(ctx, stepOr(step))
	err := w.Reach(rctx, target)
	rerr := rctx.Err()
	cancel()
	switch {
	case err == nil:
		return Check{ID: resolveID, Addr: addr, Status: StatusOK, MS: ms(),
			Detail: resolved + "; TCP from the worker to " + target.String() + " connected"}
	case errors.Is(err, ErrWorkerRefused):
		return fail(resolved+"; TCP from the worker to "+target.String()+" was refused",
			fixFor("check that the database listens on "+strconv.Itoa(port)+" at that address and accepts connections from the worker's network"))
	case errors.Is(err, ErrWorkerTimeout) || rerr != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		return fail(resolved+"; TCP from the worker to "+target.String()+" did not connect within the step's time",
			fixFor("check the routes and firewalls between the worker and "+target.String()))
	default:
		return warn(resolved + "; could not ask the worker to connect to " + target.String() + ": " + redactedText(err.Error()))
	}
}

// joinAddrs names at most maxResolvedShown addresses, in resolver order.
func joinAddrs(addrs []netip.Addr) string {
	n := len(addrs)
	if n > maxResolvedShown {
		n = maxResolvedShown
	}
	parts := make([]string, 0, n+1)
	for _, a := range addrs[:n] {
		parts = append(parts, a.WithZone("").String())
	}
	if more := len(addrs) - n; more > 0 {
		parts = append(parts, "and "+strconv.Itoa(more)+" more")
	}
	return strings.Join(parts, ", ")
}

// localSuffix names Piperoom's own answer for the same name, when it is known
// and differs from the worker's (worker nil: the worker had no answer).
func localSuffix(local, worker []netip.Addr) string {
	if len(local) == 0 || (worker != nil && sameAddrs(local, worker)) {
		return ""
	}
	return " (Piperoom resolves it to " + joinAddrs(local) + ")"
}

// sameAddrs reports whether a and b hold the same set of addresses.
func sameAddrs(a, b []netip.Addr) bool {
	set := make(map[netip.Addr]bool, len(a))
	for _, x := range a {
		set[x.WithZone("").Unmap()] = true
	}
	seen := make(map[netip.Addr]bool, len(b))
	for _, y := range b {
		y = y.WithZone("").Unmap()
		if !set[y] {
			return false
		}
		seen[y] = true
	}
	return len(seen) == len(set)
}

// blackHoleNote marks an address from a documentation range.
func blackHoleNote(a netip.Addr) string {
	a = a.WithZone("").Unmap()
	for _, p := range documentationRanges {
		if p.Contains(a) {
			return ", a documentation address that routes nowhere"
		}
	}
	return ""
}

// localAddrs reads Piperoom's own answer for host out of the probe's dns
// checks (their Detail is the resolved list this package writes). nil when
// the dns step for host failed or did not run.
func localAddrs(checks []Check, host string) []netip.Addr {
	for _, c := range checks {
		if c.ID != "dns" || c.Status != StatusOK {
			continue
		}
		h, _, err := net.SplitHostPort(c.Addr)
		if err != nil || !strings.EqualFold(h, host) {
			continue
		}
		var out []netip.Addr
		for _, s := range strings.Split(c.Detail, ", ") {
			if a, err := netip.ParseAddr(strings.TrimSpace(s)); err == nil {
				out = append(out, a)
			}
		}
		return out
	}
	return nil
}

// connectResolve runs ConnectResolveCheck for the database host of the
// pipeline's running source connector: the host and port Connect itself
// uses, read from the config Connect holds.
func (t *netProbe) connectResolve(ctx context.Context, tgt Target, src *sourceConfig, local []Check) Check {
	if t.d.Connect == nil {
		return Check{ID: resolveID, Status: StatusSkip, Detail: "no Connect client, so the connector's database host is unknown"}
	}
	name, cfg, why := src.get(ctx, t, tgt)
	if name == "" {
		return Check{ID: resolveID, Status: StatusSkip, Detail: why}
	}
	host := strings.TrimSpace(cfg["database.hostname"])
	if host == "" {
		return Check{ID: resolveID, Status: StatusSkip, Detail: "connector " + clip(name) + " names no database.hostname"}
	}
	port, err := strconv.Atoi(strings.TrimSpace(cfg["database.port"]))
	if err != nil || port < 1 || port > 65535 {
		port = endpointPort(tgt, host)
	}
	if port == 0 {
		return Check{ID: resolveID, Status: StatusSkip, Detail: "connector " + clip(name) + " names no numeric database.port"}
	}
	return ConnectResolveCheck(ctx, t.d.ConnectWorker, host, port, localAddrs(local, host), t.d.StepTimeout)
}

// endpointPort is the port of the target's endpoint for host; 0 when none.
func endpointPort(tgt Target, host string) int {
	for _, e := range tgt.Endpoints {
		h, p, err := net.SplitHostPort(e.Addr)
		if err != nil || !strings.EqualFold(h, host) {
			continue
		}
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 65535 {
			return n
		}
	}
	return 0
}

// sourceConfig reads the running source connector's config once for both
// Connect checks of a source probe.
type sourceConfig struct {
	done bool
	name string
	cfg  map[string]string
	why  string
}

func (s *sourceConfig) get(ctx context.Context, t *netProbe, tgt Target) (string, map[string]string, string) {
	if !s.done {
		s.name, s.cfg, s.why = t.runningSourceConfig(ctx, tgt)
		s.done = true
	}
	return s.name, s.cfg, s.why
}
