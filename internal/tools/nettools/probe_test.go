package nettools

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func checkByID(t *testing.T, checks []Check, id string) Check {
	t.Helper()
	for _, c := range checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no %q check in %+v", id, checks)
	return Check{}
}

func TestProberLoopbackTCP(t *testing.T) {
	addr := listenLoopback(t)
	checks := Prober{}.Probe(context.Background(), Target{Label: LabelConnect, Kind: KindConnect,
		Endpoints: []Endpoint{{Addr: addr}}})
	if len(checks) != 3 {
		t.Fatalf("checks = %+v", checks)
	}
	for _, id := range []string{"dns", "tcp"} {
		if c := checkByID(t, checks, id); c.Status != StatusOK {
			t.Fatalf("%s = %+v", id, c)
		}
	}
	if c := checkByID(t, checks, "tls"); c.Status != StatusSkip || !strings.Contains(c.Detail, "not configured") {
		t.Fatalf("tls = %+v", c)
	}
}

func TestProberDNSFailure(t *testing.T) {
	loc := &fakeLocation{lookup: func(context.Context, string) ([]netip.Addr, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "db.invalid", IsNotFound: true}
	}}
	checks := Prober{Loc: loc}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: "db.invalid:5432"}}})
	dns := checkByID(t, checks, "dns")
	if dns.Status != StatusFail || !strings.Contains(dns.Problem, "does not resolve") {
		t.Fatalf("dns = %+v", dns)
	}
	for _, id := range []string{"tcp", "tls"} {
		if c := checkByID(t, checks, id); c.Status != StatusSkip {
			t.Fatalf("%s = %+v, want skip after a DNS failure", id, c)
		}
	}
}

func TestProberDNSNoAddresses(t *testing.T) {
	loc := &fakeLocation{lookup: func(context.Context, string) ([]netip.Addr, error) { return nil, nil }}
	dns := checkByID(t, Prober{Loc: loc}.Probe(context.Background(),
		Target{Endpoints: []Endpoint{{Addr: "db.example:5432"}}}), "dns")
	if dns.Status != StatusFail || !strings.Contains(dns.Problem, "no addresses") {
		t.Fatalf("dns = %+v", dns)
	}
}

func TestProberConnectionRefused(t *testing.T) {
	addr := deadLoopbackAddr(t)
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr}}})
	tcp := checkByID(t, checks, "tcp")
	if tcp.Status != StatusFail || !strings.Contains(tcp.Problem, "TCP failed") {
		t.Fatalf("tcp = %+v", tcp)
	}
	if c := checkByID(t, checks, "tls"); c.Status != StatusSkip {
		t.Fatalf("tls = %+v", c)
	}
}

func TestProberStepTimeout(t *testing.T) {
	loc := &fakeLocation{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	checks := Prober{Loc: loc, StepTimeout: 40 * time.Millisecond}.Probe(context.Background(),
		Target{Endpoints: []Endpoint{{Addr: "10.0.0.1:5432"}}})
	tcp := checkByID(t, checks, "tcp")
	if tcp.Status != StatusFail || !strings.Contains(tcp.Problem, "within the step's time") {
		t.Fatalf("tcp = %+v", tcp)
	}
	if loc.dialDeadline <= 0 || loc.dialDeadline > 100*time.Millisecond {
		t.Fatalf("dial deadline = %v, want about 40ms", loc.dialDeadline)
	}
}

func TestProberEveryEndpointIsProbed(t *testing.T) {
	live := listenLoopback(t)
	dead := deadLoopbackAddr(t)
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: live}, {Addr: dead}}})
	if len(checks) != 6 {
		t.Fatalf("checks = %+v", checks)
	}
	var liveOK, deadFail bool
	for _, c := range checks {
		if c.ID != "tcp" {
			continue
		}
		if c.Addr == live && c.Status == StatusOK {
			liveOK = true
		}
		if c.Addr == dead && c.Status == StatusFail {
			deadFail = true
		}
	}
	if !liveOK || !deadFail {
		t.Fatalf("checks = %+v", checks)
	}
}

func TestProberTLSOK(t *testing.T) {
	now := time.Now()
	addr, roots, _, clientCerts := tlsServer(t, certOpts{
		dns: []string{"localhost"}, ips: []string{"127.0.0.1"},
		notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	cfg := &tls.Config{RootCAs: roots, ServerName: "localhost"}
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true, Config: cfg}}})
	tlsCheck := checkByID(t, checks, "tls")
	if tlsCheck.Status != StatusOK {
		t.Fatalf("tls = %+v", tlsCheck)
	}
	for _, want := range []string{"chain verified", "SAN localhost", "test-endpoint"} {
		if !strings.Contains(tlsCheck.Detail, want) {
			t.Fatalf("detail %q does not contain %q", tlsCheck.Detail, want)
		}
	}
	if n := clientCerts.Load(); n != 0 {
		t.Fatalf("the server saw %d client certificates", n)
	}
}

// A caller's tls.Config may carry a client certificate; the probe's certificate
// step must never present it.
func TestProberTLSNeverPresentsClientCert(t *testing.T) {
	now := time.Now()
	addr, roots, _, clientCerts := tlsServer(t, certOpts{
		dns: []string{"localhost"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	cfg := &tls.Config{
		RootCAs: roots, ServerName: "localhost",
		Certificates: []tls.Certificate{{Certificate: [][]byte{{1, 2, 3}}}},
	}
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true, Config: cfg}}})
	if c := checkByID(t, checks, "tls"); c.Status != StatusOK {
		t.Fatalf("tls = %+v", c)
	}
	if n := clientCerts.Load(); n != 0 {
		t.Fatalf("the probe presented %d client certificate(s)", n)
	}
}

func TestProberTLSExpired(t *testing.T) {
	now := time.Now()
	addr, roots, _, _ := tlsServer(t, certOpts{
		dns: []string{"localhost"}, notBefore: now.Add(-30 * 24 * time.Hour), notAfter: now.Add(-3 * 24 * time.Hour),
	})
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}})
	c := checkByID(t, checks, "tls")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "expired") {
		t.Fatalf("tls = %+v", c)
	}
}

func TestProberTLSSANMismatch(t *testing.T) {
	now := time.Now()
	addr, roots, _, _ := tlsServer(t, certOpts{
		dns: []string{"other.example"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}})
	c := checkByID(t, checks, "tls")
	if c.Status != StatusFail || !strings.Contains(c.Problem, `not valid for "localhost"`) {
		t.Fatalf("tls = %+v", c)
	}
	if !strings.Contains(c.Fix, "SANs") {
		t.Fatalf("fix = %q", c.Fix)
	}
}

func TestProberTLSChainUntrusted(t *testing.T) {
	now := time.Now()
	addr, _, _, _ := tlsServer(t, certOpts{
		dns: []string{"localhost"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "localhost"}}}})
	c := checkByID(t, checks, "tls")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "not trusted") {
		t.Fatalf("tls = %+v", c)
	}
}

func TestProberTLSNearExpiryWarns(t *testing.T) {
	now := time.Now()
	addr, roots, _, _ := tlsServer(t, certOpts{
		dns: []string{"localhost"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(10 * 24 * time.Hour),
	})
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}})
	c := checkByID(t, checks, "tls")
	if c.Status != StatusWarn || !strings.Contains(c.Problem, "expires in") {
		t.Fatalf("tls = %+v", c)
	}
}

func TestProberTLSPlaintextServer(t *testing.T) {
	addr := listenLoopback(t)
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true}}})
	if c := checkByID(t, checks, "tcp"); c.Status != StatusOK {
		t.Fatalf("tcp = %+v", c)
	}
	c := checkByID(t, checks, "tls")
	if c.Status != StatusFail || !strings.Contains(c.Problem, "handshake failed") {
		t.Fatalf("tls = %+v", c)
	}
}

func TestProbeLabelFromSetup(t *testing.T) {
	addr := listenLoopback(t)
	src := TargetsFromSetup([]events.SetupTarget{{Kind: "connect", Address: addr}})
	tgt, checks, err := Prober{}.ProbeLabel(context.Background(), src, owners.Pipeline{}, "connect")
	if err != nil {
		t.Fatal(err)
	}
	if tgt.Label != "connect" || len(tgt.Endpoints) != 1 || tgt.Endpoints[0].Addr != addr {
		t.Fatalf("target = %+v", tgt)
	}
	if c := checkByID(t, checks, "tcp"); c.Status != StatusOK {
		t.Fatalf("tcp = %+v", c)
	}
	if _, _, err := (Prober{}).ProbeLabel(context.Background(), src, owners.Pipeline{}, "kafka"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("unknown label: err = %v", err)
	}
	if _, _, err := (Prober{}).ProbeLabel(context.Background(), nil, owners.Pipeline{}, "connect"); err == nil {
		t.Fatal("nil source accepted")
	}
}

func TestProberCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checks := Prober{}.Probe(ctx, Target{Endpoints: []Endpoint{{Addr: "127.0.0.1:1"}}})
	tcp := checkByID(t, checks, "tcp")
	if tcp.Status != StatusFail {
		t.Fatalf("tcp = %+v", tcp)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("context not cancelled")
	}
}
