package connprobe

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
)

type fakeResolver map[string][]netip.Addr

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	a, ok := f[host]
	if !ok {
		return nil, errors.New("no such host")
	}
	return a, nil
}

type recordDialer struct{ addrs []string }

func (d *recordDialer) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	d.addrs = append(d.addrs, address)
	c1, c2 := net.Pipe()
	_ = c2.Close()
	return c1, nil
}

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParseAddr(s)
	}
	return out
}

func TestGuardCheckAddr(t *testing.T) {
	strict := &Guard{}
	open := &Guard{AllowLoopback: true, AllowPrivate: true}
	cases := []struct {
		addr       string
		strictOK   bool
		permissive bool
	}{
		{"8.8.8.8", true, true},
		{"169.254.169.254", false, false},
		{"::ffff:169.254.169.254", false, false},
		{"fd00:ec2::254", false, false},
		{"fe80::1", false, false},
		{"100.100.100.200", false, false},
		{"0.0.0.0", false, false},
		{"::", false, false},
		{"224.0.0.1", false, false},
		{"127.0.0.1", false, true},
		{"::1", false, true},
		{"10.1.2.3", false, true},
		{"172.20.0.5", false, true},
		{"192.168.1.1", false, true},
		{"100.64.0.1", false, true},
		{"fd12::1", false, true},
	}
	for _, c := range cases {
		a := netip.MustParseAddr(c.addr)
		if err := strict.CheckAddr(a); (err == nil) != c.strictOK {
			t.Errorf("strict %s: err=%v", c.addr, err)
		}
		if err := open.CheckAddr(a); (err == nil) != c.permissive {
			t.Errorf("permissive %s: err=%v", c.addr, err)
		}
		if err := strict.CheckAddr(a); err != nil && !errors.Is(err, ErrBlockedAddress) {
			t.Errorf("%s: refusal does not wrap ErrBlockedAddress: %v", c.addr, err)
		}
	}
}

func TestGuardResolve(t *testing.T) {
	g := &Guard{AllowPrivate: true, Resolver: fakeResolver{
		"kafka.internal": addrs("10.0.0.5"),
		"mixed.example":  addrs("8.8.8.8", "169.254.169.254"),
		"loop.example":   addrs("127.0.0.1"),
	}}
	ctx := context.Background()
	if a, err := g.Resolve(ctx, "kafka.internal"); err != nil || len(a) != 1 {
		t.Fatalf("kafka.internal: %v %v", a, err)
	}
	for _, h := range []string{"mixed.example", "loop.example", "metadata.google.internal", "METADATA.google.internal.", "169.254.169.254", "[fe80::1]", ""} {
		if _, err := g.Resolve(ctx, h); !errors.Is(err, ErrBlockedAddress) {
			t.Errorf("%q: want a guard refusal, got %v", h, err)
		}
	}
	if _, err := g.Resolve(ctx, "unknown.example"); err == nil || errors.Is(err, ErrBlockedAddress) {
		t.Errorf("unknown host: want a lookup error, got %v", err)
	}
}

func TestGuardDialsTheCheckedAddress(t *testing.T) {
	d := &recordDialer{}
	g := &Guard{AllowPrivate: true, Dialer: d, Resolver: fakeResolver{"kafka.internal": addrs("10.0.0.5")}}
	c, err := g.DialContext(context.Background(), "tcp", "kafka.internal:9092")
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if len(d.addrs) != 1 || d.addrs[0] != "10.0.0.5:9092" {
		t.Fatalf("dialed %v, want the resolved IP", d.addrs)
	}
	if _, err := g.DialContext(context.Background(), "tcp", "169.254.169.254:80"); !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("metadata dial: %v", err)
	}
	if _, err := g.DialContext(context.Background(), "tcp", "no-port"); err == nil {
		t.Fatal("bad address accepted")
	}
}

func TestGuardCheckURL(t *testing.T) {
	g := &Guard{AllowLoopback: true}
	if err := g.CheckURL(context.Background(), "http://127.0.0.1:8083"); err != nil {
		t.Fatal(err)
	}
	if err := g.CheckURL(context.Background(), "http://169.254.169.254/latest"); !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("metadata URL: %v", err)
	}
	if err := g.CheckURL(context.Background(), "not a url"); err == nil {
		t.Fatal("bad URL accepted")
	}
}

func TestRunUnknownKindAndOK(t *testing.T) {
	r := Run(context.Background(), Target{Kind: "nope"}, nil)
	if r.OK || len(r.Checks) != 1 || r.Checks[0].Status != StatusFail {
		t.Fatalf("unknown kind: %+v", r)
	}
	// A probe's failed check makes the result not ok; Run fills the kind.
	// (TestConnectOK covers ok = true.)
	r = Run(context.Background(), Target{Kind: KindConnect}, nil)
	if r.OK || r.Kind != KindConnect || len(r.Checks) == 0 {
		t.Fatalf("connect probe without settings: %+v", r)
	}
}

// An IPv6 transition-mechanism address that embeds a blocked IPv4 address is
// refused like the plain one; one embedding an allowed address still passes.
func TestCheckAddrEmbeddedIPv4(t *testing.T) {
	g := Guard{AllowLoopback: true, AllowPrivate: true}
	for _, raw := range []string{
		"64:ff9b::a9fe:a9fe",   // NAT64 well-known of 169.254.169.254
		"64:ff9b:1::a9fe:a9fe", // NAT64 local-use of 169.254.169.254
		"2002:a9fe:a9fe::1",    // 6to4 of 169.254.169.254
		"2001::5601:5601",      // Teredo of 169.254.169.254 (stored inverted)
		"64:ff9b::6464:64c8",   // NAT64 of 100.100.100.200 (metadata)
	} {
		if err := g.CheckAddr(netip.MustParseAddr(raw)); err == nil {
			t.Errorf("CheckAddr(%s) = nil, want blocked", raw)
		}
	}
	for _, raw := range []string{
		"64:ff9b::808:808", // NAT64 of 8.8.8.8
		"2002:808:808::1",  // 6to4 of 8.8.8.8
	} {
		if err := g.CheckAddr(netip.MustParseAddr(raw)); err != nil {
			t.Errorf("CheckAddr(%s) = %v, want nil", raw, err)
		}
	}
	strict := Guard{}
	if err := strict.CheckAddr(netip.MustParseAddr("64:ff9b::a00:1")); err == nil {
		t.Error("NAT64 of 10.0.0.1 passed a guard that blocks private ranges")
	}
}

// A zone suffix must not slip an address past the prefix checks, and the
// deprecated IPv4-compatible form embeds an IPv4 address too.
func TestCheckAddrZoneAndV4Compatible(t *testing.T) {
	g := Guard{AllowLoopback: true, AllowPrivate: true}
	for _, raw := range []string{
		"fe80::1%eth0",      // zoned link-local
		"fe80::1%25",        // numeric zone
		"::169.254.169.254", // IPv4-compatible of the metadata address
		"2001:0:808:808::1", // Teredo whose SERVER bytes are fine but still checked
	} {
		a := netip.MustParseAddr(raw)
		err := g.CheckAddr(a)
		switch raw {
		case "2001:0:808:808::1":
			// server 8.8.8.8 allowed, client ^0:^0:^0:^1 = 255.255.255.254 allowed
			if err != nil {
				t.Errorf("CheckAddr(%s) = %v, want nil", raw, err)
			}
		default:
			if err == nil {
				t.Errorf("CheckAddr(%s) = nil, want blocked", raw)
			}
		}
	}
	// A Teredo address whose embedded SERVER is blocked is refused even when
	// the inverted client bytes are fine.
	if err := g.CheckAddr(netip.MustParseAddr("2001:0:a9fe:a9fe::1:2")); err == nil {
		t.Error("Teredo with metadata server address passed")
	}
	// :: and ::1 keep their own classification, not an embedded-v4 one.
	if err := g.CheckAddr(netip.MustParseAddr("::1")); err != nil {
		t.Errorf("CheckAddr(::1) with loopback allowed = %v", err)
	}
}
