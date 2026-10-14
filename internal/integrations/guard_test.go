package integrations

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
)

type fakeResolver map[string][]string

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	ips, ok := f[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	var out []netip.Addr
	for _, s := range ips {
		out = append(out, netip.MustParseAddr(s))
	}
	return out, nil
}

func TestGuard(t *testing.T) {
	res := fakeResolver{
		"db.corp":        {"10.1.2.3"},
		"evil.example":   {"169.254.169.254"},
		"mixed.example":  {"10.1.2.3", "169.254.169.254"},
		"kafka.corp":     {"10.9.0.1"},
		"b1.kafka.corp":  {"10.9.0.2"},
		"public.example": {"203.0.113.7"},
		"v6meta.example": {"fd00:ec2::254"},
	}
	open, err := NewGuard("", res)
	if err != nil {
		t.Fatal(err)
	}
	strict, err := NewGuard("db.corp, *.kafka.corp, 10.50.0.0/16, 192.0.2.10", res)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		g     *Guard
		host  string
		allow bool
	}{
		{"open: private ip", open, "10.0.0.1", true},
		{"open: loopback", open, "127.0.0.1", true},
		{"open: name", open, "db.corp", true},
		{"open: metadata ip", open, "169.254.169.254", false},
		{"open: link-local", open, "169.254.1.1", false},
		{"open: mapped metadata", open, "::ffff:169.254.169.254", false},
		{"open: v6 metadata", open, "fd00:ec2::254", false},
		{"open: bracketed v6 metadata", open, "[fd00:ec2::254]", false},
		{"open: gcp v6 metadata", open, "fd20:ce::254", false},
		{"open: alibaba metadata", open, "100.100.100.200", false},
		{"open: v6 link-local", open, "fe80::1", false},
		{"open: nat64 metadata", open, "64:ff9b::a9fe:a9fe", false},
		{"open: v4-compatible metadata", open, "::169.254.169.254", false},
		{"open: nat64 other address", open, "64:ff9b::a00:1", true},
		{"open: v6 loopback", open, "::1", true},
		{"open: cgnat neighbour of alibaba", open, "100.100.100.201", true},
		{"open: name resolving to metadata", open, "evil.example", false},
		{"open: name with one metadata address", open, "mixed.example", false},
		{"open: name resolving to v6 metadata", open, "v6meta.example", false},
		{"open: unknown name", open, "nope.example", false},
		{"strict: listed name", strict, "db.corp", true},
		{"strict: suffix", strict, "b1.kafka.corp", true},
		{"strict: suffix does not match the bare domain", strict, "kafka.corp", false},
		{"strict: cidr", strict, "10.50.3.4", true},
		{"strict: single ip", strict, "192.0.2.10", true},
		{"strict: other ip", strict, "10.51.0.1", false},
		{"strict: unlisted name outside cidrs", strict, "public.example", false},
		{"strict: metadata even if listed range", strict, "169.254.169.254", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.g.CheckHost(context.Background(), tc.host)
			if tc.allow && err != nil {
				t.Fatalf("want allowed, got %v", err)
			}
			if !tc.allow && err == nil {
				t.Fatal("want refused")
			}
		})
	}
	// Metadata refusals are ErrBlockedHost.
	if err := open.CheckHost(context.Background(), "169.254.169.254"); !errors.Is(err, ErrBlockedHost) {
		t.Fatalf("err = %v", err)
	}
}

func TestGuardMetadataBlockedEvenWhenAllowed(t *testing.T) {
	g, err := NewGuard("169.254.0.0/16, fd00:ec2::254", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{"169.254.169.254", "fd00:ec2::254"} {
		if err := g.CheckHost(context.Background(), h); !errors.Is(err, ErrBlockedHost) {
			t.Fatalf("%s: err = %v, want blocked", h, err)
		}
	}
}

func TestGuardBadEntry(t *testing.T) {
	for _, s := range []string{"not a host!", "*.", "a..b"} {
		if _, err := NewGuard(s, nil); err == nil {
			t.Fatalf("%q: want error", s)
		}
	}
}

func TestGuardDial(t *testing.T) {
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
			c.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	g, _ := NewGuard("", fakeResolver{"local.test": {"127.0.0.1"}, "meta.test": {"169.254.169.254"}})
	c, err := g.DialContext(context.Background(), "tcp", net.JoinHostPort("local.test", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.Close()
	if _, err := g.DialContext(context.Background(), "tcp", net.JoinHostPort("meta.test", "80")); !errors.Is(err, ErrBlockedHost) {
		t.Fatalf("metadata dial err = %v", err)
	}
	if _, err := g.DialContext(context.Background(), "tcp", "no-port"); err == nil {
		t.Fatal("bad address: want error")
	}
}
