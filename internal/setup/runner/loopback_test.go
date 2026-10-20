package runner

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

func fixedResolver(ips ...string) LookupIPFunc {
	return func(_ context.Context, _ string) ([]net.IP, error) {
		out := make([]net.IP, 0, len(ips))
		for _, s := range ips {
			out = append(out, net.ParseIP(s))
		}
		return out, nil
	}
}

func failingResolver() LookupIPFunc {
	return func(_ context.Context, _ string) ([]net.IP, error) {
		return nil, errors.New("no such host")
	}
}

func TestIsLoopbackHost(t *testing.T) {
	cases := []struct {
		name   string
		host   string
		lookup LookupIPFunc
		want   bool
	}{
		{"localhost name", "localhost", failingResolver(), true},
		{"v4 loopback literal", "127.0.0.1", failingResolver(), true},
		{"v6 loopback literal", "::1", failingResolver(), true},
		{"non-loopback literal", "192.0.2.1", failingResolver(), false},
		{"name resolving to loopback", "runner.internal", fixedResolver("127.0.0.1"), true},
		{"name resolving elsewhere", "piperoom.acme", fixedResolver("192.0.2.5"), false},
		{"unresolvable name fails closed", "gone.example", failingResolver(), false},
		{"empty host fails closed", "", failingResolver(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isLoopbackHost(context.Background(), c.host, c.lookup); got != c.want {
				t.Errorf("isLoopbackHost(%q) = %v, want %v", c.host, got, c.want)
			}
		})
	}
}

// TestCodeValidation: the pairing code is checked for length and alphabet,
// and the error never quotes the code.
func TestCodeValidation(t *testing.T) {
	good := "ABCDEFGH"
	if _, err := New(Options{ServerURL: "http://127.0.0.1:8080", Code: good}); err != nil {
		t.Fatalf("valid code refused: %v", err)
	}
	bad := []struct {
		name string
		code string
	}{
		{"empty", ""},
		{"short", "ABCDEFG"},
		{"long", "ABCDEFGHI"},
		{"bad alphabet", "ABCDEFG0"}, // 0 is not in the code alphabet
		{"ambiguous O", "ABCDEFGO"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(Options{ServerURL: "http://127.0.0.1:8080", Code: c.code})
			if err == nil {
				t.Fatalf("code %q accepted", c.code)
			}
			if c.code != "" && strings.Contains(err.Error(), c.code) {
				t.Errorf("error quotes the code: %q", err)
			}
		})
	}
}
