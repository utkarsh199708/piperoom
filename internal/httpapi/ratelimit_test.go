package httpapi

import (
	"testing"
	"time"
)

func TestLimitKeyIPv6By64(t *testing.T) {
	cases := map[string]string{
		"203.0.113.9":                      "203.0.113.9",
		"::ffff:203.0.113.9":               "203.0.113.9", // v4-mapped counts as v4
		"2001:db8:0:1::1":                  "2001:db8:0:1::/64",
		"2001:db8:0:1:ffff:ffff:ffff:ffff": "2001:db8:0:1::/64",
		"2001:db8:0:2::1":                  "2001:db8:0:2::/64",
		"not-an-ip":                        "not-an-ip",
	}
	for in, want := range cases {
		if got := limitKey(in); got != want {
			t.Errorf("limitKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRateLimiterWindowAndLimit(t *testing.T) {
	l := newRateLimiter(3, time.Minute)
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if !l.allow("a", now) {
			t.Fatalf("call %d refused", i+1)
		}
	}
	if l.allow("a", now) {
		t.Fatal("the 4th call in the window was allowed")
	}
	if !l.allow("b", now) {
		t.Fatal("another key shares the budget")
	}
	if !l.allow("a", now.Add(time.Minute)) {
		t.Fatal("the window did not reset")
	}
}
