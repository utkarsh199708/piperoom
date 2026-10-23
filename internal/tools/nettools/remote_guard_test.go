package nettools

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// A connector's database.hostname comes from whoever can write the
// connector config: it may name a private host, never Piperoom's loopback,
// unless the operator's own settings reach the source over loopback.
func TestRemoteEndpointRefusesLoopback(t *testing.T) {
	fc := &fakeConnect{configFn: func(context.Context, string) (map[string]string, error) {
		return map[string]string{"database.hostname": "127.0.0.1", "database.port": "6379"}, nil
	}}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"pg"}}
	targets, err := resolveAll(t, RoomTargets(config.Config{SourceDBURL: "postgres://u:p@src.internal:5432/db"}, fc), p)
	if err != nil {
		t.Fatal(err)
	}
	src := targetByLabel(t, targets, LabelSource)
	if len(src.Endpoints) != 2 || src.Endpoints[0].Remote || !src.Endpoints[1].Remote {
		t.Fatalf("endpoints = %+v", src.Endpoints)
	}

	dialed := []string{}
	loc := &fakeLocation{
		lookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
		},
		dial: func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = append(dialed, address)
			c1, c2 := net.Pipe()
			_ = c2.Close()
			return c1, nil
		},
	}
	checks := Prober{Loc: loc}.Probe(context.Background(), src)
	var refused bool
	for _, c := range checks {
		if c.ID == "tcp" && c.Addr == "127.0.0.1:6379" {
			refused = c.failed() && strings.Contains(c.Problem, "loopback")
		}
	}
	if !refused {
		t.Fatalf("loopback from a connector config was not refused: %+v", checks)
	}
	for _, a := range dialed {
		if strings.HasPrefix(a, "127.") {
			t.Fatalf("dialed %s", a)
		}
	}

	// Settings that reach the source over loopback make the connector's
	// loopback host expected.
	targets, err = resolveAll(t, RoomTargets(config.Config{SourceDBURL: "postgres://u:p@localhost:5432/db"}, fc), p)
	if err != nil {
		t.Fatal(err)
	}
	if src := targetByLabel(t, targets, LabelSource); len(src.Endpoints) != 2 || src.Endpoints[1].Remote {
		t.Fatalf("endpoints = %+v", src.Endpoints)
	}
}

// A remote name is resolved once and the checked IP is dialed, so a name
// that resolves to loopback is refused and a private one is pinned.
func TestRemoteLocationPinsTheCheckedAddress(t *testing.T) {
	answer := "127.0.0.1"
	var dialed string
	loc := remoteLocation{inner: &fakeLocation{
		lookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr(answer)}, nil
		},
		dial: func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = address
			c1, c2 := net.Pipe()
			_ = c2.Close()
			return c1, nil
		},
	}}
	if _, err := loc.Dial(context.Background(), "tcp", "db.example:5432"); err == nil || dialed != "" {
		t.Fatalf("loopback answer dialed: err=%v dialed=%q", err, dialed)
	}
	answer = "10.1.2.3"
	c, err := loc.Dial(context.Background(), "tcp", "db.example:5432")
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if dialed != "10.1.2.3:5432" {
		t.Fatalf("dialed %q, want the checked IP", dialed)
	}
	if _, err := loc.Dial(context.Background(), "tcp", "[::1]:5432"); err == nil {
		t.Fatal("loopback literal dialed")
	}
}

func TestIsLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"localhost:9092": true, "LOCALHOST.:9092": true, "127.0.0.2:1": true, "[::1]:1": true,
		"[::ffff:127.0.0.1]:1": true, "10.0.0.1:1": false, "kafka:9092": false, "bad": false,
	} {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}
