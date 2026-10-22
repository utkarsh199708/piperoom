package ops

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/connprobe"
	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// probeStepTimeout bounds one DNS, TCP or TLS step (Q6a: 3 s per step).
const probeStepTimeout = 3 * time.Second

// NetProbe is Q6a's net_probe from the runner's position. The seam exists so
// the runner reuses Q6a's DNS/TCP/TLS rules instead of duplicating them: when
// internal/tools/nettools lands, Config.NetProbe wraps it and nothing else
// here changes. The built-in probe below covers DNS, TCP and a certificate
// read; Q6a adds the per-target detail and the advertised-listener compare.
type NetProbe interface {
	Probe(ctx context.Context, req ProbeRequest, env Env) (ProbeResult, error)
}

// ProbeRequest is one target to probe: a registered target, the resolved
// host and port, and whether to read its certificate.
type ProbeRequest struct {
	Target events.SetupTarget
	Host   string
	Port   string
	TLS    bool
}

// ProbeResult is the checks of one probe, in the connprobe shape the UI and
// the model already read.
type ProbeResult struct {
	Checks []connprobe.Check
}

// netProbeOp probes one registered target from the runner's network position.
// The model names a target label, never an address.
type netProbeOp struct{}

type netProbeParams struct {
	Target string `json:"target"`
	Port   int    `json:"port,omitempty"`
	TLS    bool   `json:"tls,omitempty"`
}

type netProbeData struct {
	Target  string            `json:"target"`
	Address string            `json:"address"`
	Checks  []connprobe.Check `json:"checks"`
}

func (netProbeOp) Name() string   { return "net_probe" }
func (netProbeOp) Target() string { return "" }
func (netProbeOp) Tier() string   { return protocol.TierT0 }
func (netProbeOp) Layer() string  { return "environment" }
func (netProbeOp) Describe() string {
	return "probe DNS, TCP and TLS of a registered target from the runner"
}

func (netProbeOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"target"},
		"properties": map[string]any{
			"target": map[string]any{"type": "string", "minLength": 1},
			"port":   map[string]any{"type": "integer", "minimum": 0, "maximum": 65535},
			"tls":    map[string]any{"type": "boolean"},
		},
	}
}

func (netProbeOp) Validate(params json.RawMessage, env Env) error {
	_, _, err := decodeNetProbe(params, env)
	return err
}

// checkTarget is the runner's TargetChecker: an unknown target label is
// refused with RefuseTarget before the guard and before any dial.
func (netProbeOp) checkTarget(params json.RawMessage, env Env) error {
	_, _, err := decodeNetProbe(params, env)
	return err
}

// ParamTarget is the kind of the target label, so the runner raises the
// class floor for it before the guard.
func (netProbeOp) ParamTarget(params json.RawMessage, env Env) string {
	_, t, err := decodeNetProbe(params, env)
	if err != nil {
		return ""
	}
	return t.Kind
}

func (netProbeOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, target, err := decodeNetProbe(params, env)
	if err != nil {
		return Result{}, err
	}
	host, port := splitTargetAddress(target.Address)
	if host == "" {
		return Result{}, fmt.Errorf("%w: target %q has no host address", ErrTarget, clip(p.Target))
	}
	if port == "" {
		// A target registered without a port offers only the standard port
		// (http_get's rule): 443 when the probe reads TLS, else 80. A bare
		// host must not hand the model every service on that machine.
		port = "80"
		if p.TLS {
			port = "443"
		}
	}
	// p.Port is a cross-check, never an override: the probe goes to the
	// target's own port, and naming any other port is refused.
	if p.Port != 0 && strconv.Itoa(p.Port) != port {
		return Result{}, fmt.Errorf("%w: port %d is not target %q's port", ErrTarget, p.Port, clip(p.Target))
	}
	probe := env.Probe
	if probe == nil {
		probe = builtinProbe{}
	}
	res, err := probe.Probe(ctx, ProbeRequest{Target: target, Host: host, Port: port, TLS: p.TLS}, env)
	if err != nil {
		return Result{Summary: fmt.Sprintf("net_probe %s %s: %v", p.Target, net.JoinHostPort(host, port), err)}, err
	}
	addr := net.JoinHostPort(host, port)
	data := netProbeData{Target: p.Target, Address: addr, Checks: res.Checks}
	out := Result{
		Summary: summarizeProbe(p.Target, addr, res.Checks),
		Data:    data,
		Receipt: events.SetupReceipt{Kind: "", Target: addr, At: env.Now()},
	}
	for _, c := range res.Checks {
		if c.Status == connprobe.StatusFail {
			return out, fmt.Errorf("%w: %s", ErrProbeFailed, c.ID)
		}
	}
	return out, nil
}

// decodeNetProbe decodes params and resolves the label to one registered
// target whose kind it names. It does no I/O.
func decodeNetProbe(params json.RawMessage, env Env) (netProbeParams, events.SetupTarget, error) {
	var p netProbeParams
	if err := decodeParams(params, &p); err != nil {
		return p, events.SetupTarget{}, err
	}
	if strings.TrimSpace(p.Target) == "" {
		return p, events.SetupTarget{}, fmt.Errorf("%w: target is required", ErrParams)
	}
	if p.Port < 0 || p.Port > 65535 {
		return p, events.SetupTarget{}, fmt.Errorf("%w: port must be 0..65535 (0 = the target's port)", ErrParams)
	}
	for _, t := range env.Targets {
		if strings.EqualFold(t.Kind, p.Target) {
			return p, t, nil
		}
	}
	return p, events.SetupTarget{}, fmt.Errorf("%w: %q is not one of the room's targets", ErrTarget, clip(p.Target))
}

// summarizeProbe is one line: the checks that passed, or the first failure
// with its fixed problem text.
func summarizeProbe(label, addr string, checks []connprobe.Check) string {
	var passed []string
	for _, c := range checks {
		if c.Status == connprobe.StatusFail {
			problem := c.Problem
			if problem == "" {
				problem = "failed"
			}
			return fmt.Sprintf("net_probe %s %s: %s failed: %s", label, addr, c.ID, problem)
		}
		passed = append(passed, c.ID+" "+c.Status)
	}
	if len(passed) == 0 {
		return fmt.Sprintf("net_probe %s %s: no checks ran", label, addr)
	}
	return fmt.Sprintf("net_probe %s %s: %s", label, addr, strings.Join(passed, ", "))
}

// builtinProbe is the fallback probe: DNS, TCP and an optional certificate
// read, all through the guarded dialer. Q6a replaces it through
// Config.NetProbe.
type builtinProbe struct{}

func (builtinProbe) Probe(ctx context.Context, req ProbeRequest, env Env) (ProbeResult, error) {
	var checks []connprobe.Check
	checks = append(checks, timedCheck("dns", func() connprobe.Check { return dnsCheck(ctx, req, env) }))
	checks = append(checks, timedCheck("tcp", func() connprobe.Check { return tcpCheck(ctx, req, env) }))
	if req.TLS {
		checks = append(checks, timedCheck("tls", func() connprobe.Check { return tlsCheck(ctx, req, env) }))
	}
	return ProbeResult{Checks: checks}, nil
}

// timedCheck fills MS on the finished check.
func timedCheck(id string, fn func() connprobe.Check) connprobe.Check {
	start := time.Now()
	c := fn()
	c.ID = id
	c.MS = time.Since(start).Milliseconds()
	return c
}

func dnsCheck(ctx context.Context, req ProbeRequest, env Env) connprobe.Check {
	if addr, err := netip.ParseAddr(strings.Trim(req.Host, "[]")); err == nil {
		if cerr := dialGuard.CheckAddr(addr); cerr != nil {
			return connprobe.Check{Status: connprobe.StatusFail,
				Problem: "the target address is not allowed",
				Fix:     "link-local and metadata addresses are never a target"}
		}
		return connprobe.Check{Status: connprobe.StatusOK, Detail: addr.String()}
	}
	resolver := env.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	cctx, cancel := context.WithTimeout(ctx, probeStepTimeout)
	defer cancel()
	addrs, err := resolver.LookupNetIP(cctx, "ip", req.Host)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return connprobe.Check{Status: connprobe.StatusFail, Problem: "the name lookup did not finish", Fix: "check DNS and the runner's network"}
		}
		return connprobe.Check{Status: connprobe.StatusFail, Problem: "the name does not resolve", Fix: "check the target address or the runner's DNS"}
	}
	names := make([]string, 0, 5)
	for _, a := range addrs {
		if cerr := dialGuard.CheckAddr(a); cerr != nil {
			return connprobe.Check{Status: connprobe.StatusFail,
				Problem: fmt.Sprintf("%s resolves to %s, a link-local or metadata address", clip(req.Host), a),
				Fix:     "link-local and metadata addresses are never a target"}
		}
		if len(names) < 5 {
			names = append(names, a.String())
		}
	}
	if len(names) == 0 {
		return connprobe.Check{Status: connprobe.StatusFail, Problem: "the name has no addresses", Fix: "check the target address"}
	}
	return connprobe.Check{Status: connprobe.StatusOK, Detail: strings.Join(names, ", ")}
}

func tcpCheck(ctx context.Context, req ProbeRequest, env Env) connprobe.Check {
	addr := net.JoinHostPort(req.Host, req.Port)
	if env.Dial == nil {
		return connprobe.Check{Status: connprobe.StatusFail, Problem: "the runner has no dialer"}
	}
	cctx, cancel := context.WithTimeout(ctx, probeStepTimeout)
	defer cancel()
	conn, err := env.Dial(cctx, "tcp", addr)
	if err != nil {
		return connprobe.Check{Status: connprobe.StatusFail,
			Problem: "TCP " + addr + ": " + clip(err.Error()),
			Fix:     "check that the service listens on this port and that the firewall allows the runner"}
	}
	_ = conn.Close()
	return connprobe.Check{Status: connprobe.StatusOK, Detail: "TCP " + addr + " connected"}
}

func tlsCheck(ctx context.Context, req ProbeRequest, env Env) connprobe.Check {
	addr := net.JoinHostPort(req.Host, req.Port)
	if env.Dial == nil {
		return connprobe.Check{Status: connprobe.StatusSkip, Detail: "the runner has no dialer"}
	}
	cctx, cancel := context.WithTimeout(ctx, probeStepTimeout)
	defer cancel()
	conn, err := env.Dial(cctx, "tcp", addr)
	if err != nil {
		return connprobe.Check{Status: connprobe.StatusSkip, Detail: "TCP failed first"}
	}
	defer conn.Close()
	cfg := &tls.Config{InsecureSkipVerify: true, ServerName: req.Host, MinVersion: tls.VersionTLS12} //nolint:gosec // reading the certificate, not trusting it
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(cctx); err != nil {
		return connprobe.Check{Status: connprobe.StatusFail, Problem: "TLS handshake failed", Fix: "check the endpoint's TLS settings"}
	}
	state := tc.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return connprobe.Check{Status: connprobe.StatusFail, Problem: "the endpoint presented no certificate", Fix: "check the endpoint's TLS settings"}
	}
	leaf := state.PeerCertificates[0]
	_, verr := leaf.Verify(x509.VerifyOptions{DNSName: req.Host})
	verified := "chain verified against the system roots"
	if verr != nil {
		verified = "chain not verified against the system roots"
	}
	days := int(time.Until(leaf.NotAfter).Hours() / 24)
	return connprobe.Check{
		Status: connprobe.StatusOK,
		Detail: fmt.Sprintf("subject %q, issuer %q, expires %s (%d days), %s",
			clip(leaf.Subject.CommonName), clip(leaf.Issuer.CommonName), leaf.NotAfter.UTC().Format("2006-01-02"), days, verified),
	}
}
