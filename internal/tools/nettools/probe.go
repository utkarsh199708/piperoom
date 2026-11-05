package nettools

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/connprobe"
	"piperoom/internal/owners"
)

// Check statuses. They are the setup UI's vocabulary (internal/connprobe), so
// one probe result reads the same in a room and on the runner.
const (
	StatusOK   = "ok"
	StatusFail = "fail"
	StatusWarn = "warn"
	StatusSkip = "skip"
)

// Check is one step of a probe: its id names the step, so a failing probe is
// reported as the step that failed (dns, tcp, tls, listeners, connect_view).
type Check struct {
	// ID is a stable, lower-case step id.
	ID string `json:"id"`
	// Addr is the endpoint the step ran against; "" for a target-wide step.
	Addr string `json:"addr,omitempty"`
	// Status is StatusOK, StatusFail, StatusWarn or StatusSkip.
	Status string `json:"status"`
	// MS is how long the step took; 0 when skipped.
	MS int64 `json:"ms"`
	// Problem says what is wrong in one sentence (fixed text plus, where it
	// helps, a clipped system error; never a value from the settings).
	Problem string `json:"problem,omitempty"`
	// Fix says what to do about it.
	Fix string `json:"fix,omitempty"`
	// Detail is a short, non-secret fact for a check that did not fail.
	Detail string `json:"detail,omitempty"`
}

func (c Check) failed() bool { return c.Status == StatusFail }

// Location is the network position a probe runs from. The room service probes
// from its own process (ProcessLocation); the setup runner passes its guarded
// dialer and its own resolver, so a probe from the runner cannot reach an
// address the runner refuses.
type Location interface {
	// Dial opens a connection to address ("host:port") and must honor ctx.
	Dial(ctx context.Context, network, address string) (net.Conn, error)
	// Lookup resolves host to its addresses, in the resolver's order.
	Lookup(ctx context.Context, host string) ([]netip.Addr, error)
}

// ProcessLocation is the location of the process itself: the system resolver
// and a net.Dialer that refuses link-local, cloud-metadata, multicast and
// unspecified addresses at dial time (processDialGuard). A probe target can
// come from remote data (a broker's advertised listener, a connector's
// database.hostname), so without the guard a rogue broker could point the
// room service at 169.254.169.254.
type ProcessLocation struct{}

// processDialGuard allows loopback and private ranges, where real pipelines
// and dev stacks live, and refuses the rest of connprobe's blocked ranges.
var processDialGuard = connprobe.Guard{AllowLoopback: true, AllowPrivate: true}

// Dial implements Location. The check runs in ControlContext, on the address
// the kernel is about to connect to, so a DNS answer cannot differ between a
// check and the connect.
func (ProcessLocation) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	d := net.Dialer{ControlContext: func(_ context.Context, _, address string, _ syscall.RawConn) error {
		return checkProcessDial(address)
	}}
	return d.DialContext(ctx, network, address)
}

// remoteDialGuard is the stricter policy for an address read from remote
// data (Endpoint.Remote): private ranges stay allowed, where real source
// databases and brokers live, but loopback is refused.
var remoteDialGuard = connprobe.Guard{AllowPrivate: true}

// remoteLocation wraps a Location for a Remote endpoint. Dial resolves the
// host itself, refuses the dial if any address remoteDialGuard blocks, and
// dials the checked IP, so a second DNS answer cannot swap it.
type remoteLocation struct{ inner Location }

func (r remoteLocation) Lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	return r.inner.Lookup(ctx, host)
}

func (r remoteLocation) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("nettools: dial guard: %w", err)
	}
	var addrs []netip.Addr
	if a, perr := netip.ParseAddr(host); perr == nil {
		addrs = []netip.Addr{a}
	} else if addrs, err = r.inner.Lookup(ctx, host); err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("nettools: %s has no addresses", host)
	}
	for _, a := range addrs {
		if err := remoteDialGuard.CheckAddr(a); err != nil {
			return nil, fmt.Errorf("nettools: refusing dial to an address from remote config: %w", err)
		}
	}
	return r.inner.Dial(ctx, network, net.JoinHostPort(addrs[0].String(), port))
}

// checkProcessDial refuses a resolved address processDialGuard blocks.
func checkProcessDial(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("nettools: dial guard: %w", err)
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("nettools: dial guard: %q is not an IP address", host)
	}
	if err := processDialGuard.CheckAddr(a); err != nil {
		return fmt.Errorf("nettools: refusing dial: %w", err)
	}
	return nil
}

// Lookup implements Location.
func (ProcessLocation) Lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// Prober runs the probe's DNS, TCP and TLS steps from one location. It is the
// seam U1-2 uses: the setup runner builds a Prober with its guarded dialer, the
// resolver it already has and the provisional pipeline's targets, and gets the
// same rules the room's tool applies. The checks that need a Kafka or Connect
// client (advertised listeners, Connect's view of the database) belong to the
// tool, not here.
type Prober struct {
	// Loc is the network position; nil = ProcessLocation{}.
	Loc Location
	// StepTimeout bounds one step; 0 = stepTimeout (3 s).
	StepTimeout time.Duration
}

// Probe runs the steps of every endpoint of t, in endpoint order. One endpoint
// failing does not stop the others: a broker list is probed broker by broker.
func (p Prober) Probe(ctx context.Context, t Target) []Check {
	var out []Check
	for _, e := range t.Endpoints {
		out = append(out, p.probeEndpoint(ctx, e)...)
	}
	return out
}

// ProbeLabel resolves label through src for pipeline pl and runs the steps. It
// is the seam U1-2 uses from the setup runner: a Location plus the provisional
// pipeline's TargetSource is all it needs. The Kafka listener and Connect
// checks need clients and stay in the room's tool.
func (p Prober) ProbeLabel(ctx context.Context, src TargetSource, pl owners.Pipeline, label string) (Target, []Check, error) {
	if src == nil {
		return Target{}, nil, errors.New("nettools: ProbeLabel needs a target source")
	}
	labels := src.Labels(pl)
	if !hasLabel(labels, label) {
		return Target{}, nil, unknownLabel(labels, label)
	}
	tgt, err := src.Target(ctx, pl, label)
	if err != nil {
		return Target{}, nil, fmt.Errorf("nettools: resolving target %q: %w", label, err)
	}
	if len(tgt.Endpoints) == 0 {
		return tgt, nil, fmt.Errorf("nettools: target %q has no address configured%s", tgt.Label, noteSuffix(tgt.Note))
	}
	return tgt, p.Probe(ctx, tgt), nil
}

func (p Prober) probeEndpoint(ctx context.Context, e Endpoint) []Check {
	if e.Remote {
		inner := p.Loc
		if inner == nil {
			inner = ProcessLocation{}
		}
		p.Loc = remoteLocation{inner: inner}
	}
	host, _, err := net.SplitHostPort(e.Addr)
	if err != nil || host == "" {
		return []Check{{
			ID: "dns", Addr: e.Addr, Status: StatusFail,
			Problem: "the target address is not host:port",
			Fix:     "fix the configured address",
		}}
	}
	dns := p.dns(ctx, e.Addr, host)
	checks := []Check{dns}
	if dns.failed() {
		return append(checks,
			Check{ID: "tcp", Addr: e.Addr, Status: StatusSkip, Detail: "DNS failed first"},
			Check{ID: "tls", Addr: e.Addr, Status: StatusSkip, Detail: "DNS failed first"})
	}
	tcp := p.tcp(ctx, e.Addr)
	checks = append(checks, tcp)
	if !e.TLS {
		return append(checks, Check{ID: "tls", Addr: e.Addr, Status: StatusSkip, Detail: "not configured with TLS"})
	}
	if tcp.failed() {
		return append(checks, Check{ID: "tls", Addr: e.Addr, Status: StatusSkip, Detail: "TCP failed first"})
	}
	return append(checks, p.tls(ctx, e))
}

// stepCtx derives one step's deadline. The caller's context (the tool's 10 s
// budget) always wins over the step's own bound.
func (p Prober) stepCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	d := p.StepTimeout
	if d <= 0 {
		d = stepTimeout
	}
	return context.WithTimeout(ctx, d)
}

func (p Prober) dns(ctx context.Context, addr, host string) Check {
	start := time.Now()
	if ip, err := netip.ParseAddr(host); err == nil {
		return Check{ID: "dns", Addr: addr, Status: StatusOK, MS: time.Since(start).Milliseconds(), Detail: ip.String()}
	}
	loc := p.Loc
	if loc == nil {
		loc = ProcessLocation{}
	}
	cctx, cancel := p.stepCtx(ctx)
	defer cancel()
	addrs, err := loc.Lookup(cctx, host)
	if err != nil {
		c := Check{ID: "dns", Addr: addr, Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Fix: "check the configured host name and the DNS the probe location uses"}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || cctx.Err() != nil {
			c.Problem = "the name lookup did not finish within the step's time"
		} else {
			c.Problem = "the name does not resolve: " + clip(err.Error())
		}
		return c
	}
	if len(addrs) == 0 {
		return Check{ID: "dns", Addr: addr, Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Problem: "the name has no addresses", Fix: "check the configured host name"}
	}
	names := make([]string, 0, 5)
	for _, a := range addrs {
		if len(names) < 5 {
			names = append(names, a.String())
		}
	}
	return Check{ID: "dns", Addr: addr, Status: StatusOK, MS: time.Since(start).Milliseconds(), Detail: strings.Join(names, ", ")}
}

func (p Prober) tcp(ctx context.Context, addr string) Check {
	start := time.Now()
	loc := p.Loc
	if loc == nil {
		loc = ProcessLocation{}
	}
	cctx, cancel := p.stepCtx(ctx)
	defer cancel()
	conn, err := loc.Dial(cctx, "tcp", addr)
	if err != nil {
		c := Check{ID: "tcp", Addr: addr, Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Fix: "check that the service listens on this port, its advertised listener setting and every firewall between the probe location and it"}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || cctx.Err() != nil {
			c.Problem = "TCP did not connect within the step's time"
		} else {
			c.Problem = "TCP failed: " + clip(err.Error())
		}
		return c
	}
	_ = conn.Close()
	return Check{ID: "tcp", Addr: addr, Status: StatusOK, MS: time.Since(start).Milliseconds(), Detail: "TCP connected"}
}

func (p Prober) tls(ctx context.Context, e Endpoint) Check {
	start := time.Now()
	loc := p.Loc
	if loc == nil {
		loc = ProcessLocation{}
	}
	host, _, _ := net.SplitHostPort(e.Addr)
	serverName := host
	var roots *x509.CertPool
	if e.Config != nil {
		roots = e.Config.RootCAs
		if e.Config.ServerName != "" {
			serverName = e.Config.ServerName
		}
	}
	cctx, cancel := p.stepCtx(ctx)
	defer cancel()
	conn, err := loc.Dial(cctx, "tcp", e.Addr)
	if err != nil {
		return Check{ID: "tls", Addr: e.Addr, Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Problem: "TCP failed before the TLS handshake: " + clip(err.Error()),
			Fix:     "fix the TCP failure first"}
	}
	defer conn.Close()
	info, err := netsec.CertFromConn(cctx, conn, serverName, roots)
	if err != nil {
		return Check{ID: "tls", Addr: e.Addr, Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Problem: "TLS handshake failed: " + clip(err.Error()),
			Fix:     "check the endpoint's TLS settings and that it is not a plain-text port"}
	}
	ms := time.Since(start).Milliseconds()
	chain := "chain verified"
	if !info.Verified {
		chain = "chain not trusted by the configured CAs"
	}
	base := fmt.Sprintf("subject %q, issuer %q, expires %s (%d days), %s",
		clip(info.Subject), clip(info.Issuer), info.NotAfter.UTC().Format(time.DateOnly), info.DaysLeft, chain)
	if len(info.DNSNames) > 0 {
		names := make([]string, len(info.DNSNames))
		for i, n := range info.DNSNames {
			names[i] = clip(n)
		}
		base += ", SAN " + strings.Join(names, ", ")
	}
	c := Check{ID: "tls", Addr: e.Addr, MS: ms, Detail: base}
	switch {
	case info.DaysLeft < 0:
		c.Status = StatusFail
		c.Problem = fmt.Sprintf("the certificate expired on %s", info.NotAfter.UTC().Format(time.DateOnly))
		c.Fix = "renew the certificate (issuer " + clip(info.Issuer) + ")"
	case !info.NameMatches:
		c.Status = StatusFail
		c.Problem = fmt.Sprintf("the certificate is not valid for %q", clip(serverName))
		c.Fix = "add the name the probe uses to the certificate's SANs, or set the TLS server name the client should verify"
	case !info.Verified:
		c.Status = StatusFail
		c.Problem = "the certificate chain is not trusted by the configured CAs"
		c.Fix = "point the TLS CA file at the CA that signs the endpoint's certificate"
	case info.DaysLeft <= 30:
		c.Status = StatusWarn
		c.Problem = fmt.Sprintf("the certificate expires in %d days (%s)", info.DaysLeft, info.NotAfter.UTC().Format(time.DateOnly))
		c.Fix = "renew the certificate before it expires"
	default:
		c.Status = StatusOK
	}
	return c
}
