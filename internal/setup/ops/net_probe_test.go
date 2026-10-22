package ops

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"piperoom/internal/connprobe"
	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// fakeDNS runs a tiny DNS responder on loopback that answers every A query
// with ip and every other query with no answer, and returns a resolver that
// uses it. It proves the dial guard against the address a resolver answers,
// not against a literal a test wrote into the dial.
func fakeDNS(t *testing.T, ip netip.Addr) *net.Resolver {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("dns listener: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	go serveDNS(pc, ip)
	addr := pc.LocalAddr().String()
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
}

func serveDNS(pc net.PacketConn, ip netip.Addr) {
	buf := make([]byte, 1500)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		q := append([]byte(nil), buf[:n]...)
		if len(q) < 12 {
			continue
		}
		i := 12
		for i < len(q) && q[i] != 0 {
			i += int(q[i]) + 1
		}
		if i+5 > len(q) {
			continue
		}
		qend := i + 5
		qtype := binary.BigEndian.Uint16(q[i+1 : i+3])

		resp := make([]byte, 0, qend+16)
		resp = append(resp, q[0], q[1])             // ID
		resp = append(resp, 0x81, 0x80)             // QR, RD, RA
		resp = append(resp, 0x00, 0x01)             // QDCOUNT = 1
		resp = append(resp, 0x00, 0x00)             // ANCOUNT
		resp = append(resp, 0x00, 0x00, 0x00, 0x00) // NS, AR
		resp = append(resp, q[12:qend]...)          // the question
		if qtype == 1 && ip.Is4() {                 // A
			resp[7] = 1                                 // ANCOUNT = 1
			resp = append(resp, 0xc0, 0x0c)             // owner: pointer to question
			resp = append(resp, 0x00, 0x01, 0x00, 0x01) // type A, class IN
			resp = append(resp, 0x00, 0x00, 0x00, 0x3c) // TTL 60
			resp = append(resp, 0x00, 0x04)             // RDLENGTH
			a := ip.As4()
			resp = append(resp, a[:]...)
		}
		_, _ = pc.WriteTo(resp, from)
	}
}

// The one dialer every runner connection uses refuses the address a resolver
// answers for a listed name, and the refusal names that address.
func TestDialGuardRefusesMetadataFromResolver(t *testing.T) {
	meta := netip.MustParseAddr("169.254.169.254")
	resolver := fakeDNS(t, meta)
	d := newGuardedDialer(resolver)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := d.DialContext(ctx, "tcp", "listed.example:80")
	if err == nil {
		t.Fatal("dial to a metadata address succeeded")
	}
	if !strings.Contains(err.Error(), meta.String()) {
		t.Fatalf("err = %v, want it to name %s", err, meta)
	}
}

func TestDialGuardAllowsLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	d := newGuardedDialer(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := d.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("loopback dial: %v", err)
	}
	_ = conn.Close()
}

func TestNetProbeTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: ln.Addr().String()})
	res, err := runOpErr(t, r, "net_probe", map[string]any{"target": "connect"})
	if err != nil {
		t.Fatalf("net_probe: %v (summary %s)", err, res.Summary)
	}
	if !strings.Contains(res.Summary, "tcp ok") {
		t.Fatalf("summary = %q", res.Summary)
	}
	var data netProbeData
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Checks) < 2 {
		t.Fatalf("checks = %+v", data.Checks)
	}
}

func TestNetProbeRefusedPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens on the port now
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: addr})
	res, err := runOpErr(t, r, "net_probe", map[string]any{"target": "connect"})
	if !errors.Is(err, ErrProbeFailed) {
		t.Fatalf("err = %v, want ErrProbeFailed", err)
	}
	if !strings.Contains(res.Summary, "tcp failed") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestNetProbeUnknownTarget(t *testing.T) {
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: "127.0.0.1:1"})
	if _, err := runOpErr(t, r, "net_probe", map[string]any{"target": "postgres"}); !errors.Is(err, ErrTarget) {
		t.Fatalf("err = %v, want ErrTarget", err)
	}
}

// A listed name the resolver answers with the metadata address is refused by
// the probe, and the refusal names the address.
func TestNetProbeMetadataFromResolver(t *testing.T) {
	meta := netip.MustParseAddr("169.254.169.254")
	r, err := New(Config{
		ProjectDir: t.TempDir(),
		Targets:    []events.SetupTarget{{Kind: "connect", Address: "listed.example:80"}},
		Resolver:   fakeDNS(t, meta),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	res, err := runOpErr(t, r, "net_probe", map[string]any{"target": "connect"})
	if !errors.Is(err, ErrProbeFailed) {
		t.Fatalf("err = %v, want ErrProbeFailed", err)
	}
	if !strings.Contains(res.Summary, meta.String()) {
		t.Fatalf("summary = %q, want it to name %s", res.Summary, meta)
	}
	var data netProbeData
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Checks[0].ID != "dns" || data.Checks[0].Status != connprobe.StatusFail {
		t.Fatalf("first check = %+v", data.Checks[0])
	}
}

func TestNetProbeTLS(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the TCP check closes before a TLS handshake; set before serving starts
	srv.StartTLS()
	defer srv.Close()
	r := testRegistry(t, targetFor(srv.URL))
	res, err := runOpErr(t, r, "net_probe", map[string]any{"target": "connect", "tls": true})
	if err != nil {
		t.Fatalf("net_probe tls: %v (%s)", err, res.Summary)
	}
	if !strings.Contains(res.Summary, "tls ok") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestNetProbeContextCancelled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: ln.Addr().String()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	raw, _ := json.Marshal(map[string]any{"target": "connect"})
	_, err = r.Execute(ctx, protocol.OpRequest{ID: "op", Op: "net_probe", Params: raw, Tier: protocol.TierT0})
	if !errors.Is(err, ErrProbeFailed) && !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a failed/cancelled probe", err)
	}
}
