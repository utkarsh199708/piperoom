package nettools

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode"

	"piperoom/internal/connprobe"
)

// A rogue broker or connector config can name any address; the room's own
// location must refuse link-local and cloud-metadata addresses at dial time,
// IPv4-mapped forms included.
func TestProcessLocationRefusesMetadata(t *testing.T) {
	for _, addr := range []string{"169.254.169.254:80", "[::ffff:169.254.169.254]:80", "[fe80::1]:80", "[64:ff9b::a9fe:a9fe]:80", "0.0.0.0:9092"} {
		if err := checkProcessDial(addr); !errors.Is(err, connprobe.ErrBlockedAddress) {
			t.Fatalf("%s: guard = %v", addr, err)
		}
	}
	// End to end through Dial on IPv4 (a runner may have no IPv6 socket).
	for _, addr := range []string{"169.254.169.254:80", "0.0.0.0:9092"} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := ProcessLocation{}.Dial(ctx, "tcp", addr)
		cancel()
		if err == nil {
			_ = conn.Close()
			t.Fatalf("%s: dial allowed", addr)
		}
		if !errors.Is(err, connprobe.ErrBlockedAddress) {
			t.Fatalf("%s: err = %v, want the dial guard's refusal", addr, err)
		}
	}
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: "169.254.169.254:80"}}})
	if c := checkByID(t, checks, "tcp"); c.Status != StatusFail {
		t.Fatalf("tcp to the metadata address = %+v", c)
	}
}

// Loopback and private ranges stay reachable: real pipelines and dev stacks
// live there.
func TestProcessLocationAllowsLoopback(t *testing.T) {
	addr := listenLoopback(t)
	conn, err := ProcessLocation{}.Dial(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("loopback dial: %v", err)
	}
	_ = conn.Close()
}

// clip turns every control, format and separator character into a space.
func TestClipStripsControls(t *testing.T) {
	got := clip("a\x1b[31mb‮c d\x0be\x00f g\x7fh")
	for _, r := range got {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			t.Fatalf("clip left %U in %q", r, got)
		}
	}
	if !strings.Contains(got, "[31mb") {
		t.Fatalf("clip dropped printable text: %q", got)
	}
}

// A certificate's SAN names are remote text: a hostile name cannot forge a
// line in the summary or carry an escape sequence, and each is bounded.
func TestProberTLSHostileSAN(t *testing.T) {
	now := time.Now()
	hostile := "evil\n- tcp 10.0.0.9:22 ok: TCP connected\x1b[2J" + strings.Repeat("x", 400)
	addr, roots, _, _ := tlsServer(t, certOpts{
		dns: []string{hostile, "localhost"}, notBefore: now.Add(-time.Hour), notAfter: now.Add(90 * 24 * time.Hour),
	})
	checks := Prober{}.Probe(context.Background(), Target{Endpoints: []Endpoint{{Addr: addr, TLS: true,
		Config: &tls.Config{RootCAs: roots, ServerName: "localhost"}}}})
	c := checkByID(t, checks, "tls")
	if !strings.Contains(c.Detail, "SAN evil") || !strings.Contains(c.Detail, "localhost") {
		t.Fatalf("detail = %q", c.Detail)
	}
	if strings.ContainsAny(c.Detail, "\n\r\x1b") {
		t.Fatalf("SAN text reached the detail raw: %q", c.Detail)
	}
	if len(c.Detail) > 600 {
		t.Fatalf("detail is %d bytes; each SAN name must be clipped", len(c.Detail))
	}
}
