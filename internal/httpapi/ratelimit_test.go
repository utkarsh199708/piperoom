package httpapi

import (
	"fmt"
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

// The table never grows past its cap: when it is full of live buckets a new key
// is refused, and expired buckets are reclaimed so the table recovers.
func TestRateLimiterHardCap(t *testing.T) {
	l := newRateLimiter(300, time.Minute)
	l.max = 50
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 50; i++ {
		if !l.allow(fmt.Sprintf("k%d", i), now) {
			t.Fatalf("key %d refused before the cap", i)
		}
	}
	// A flood of new keys: all refused, the table does not grow.
	for i := 50; i < 500; i++ {
		if l.allow(fmt.Sprintf("k%d", i), now.Add(time.Second)) {
			t.Fatalf("new key %d allowed with a full table", i)
		}
	}
	if n := len(l.buckets); n != 50 {
		t.Fatalf("table size = %d, want 50", n)
	}
	// Keys already tracked keep working.
	if !l.allow("k0", now.Add(time.Second)) {
		t.Fatal("a tracked key was refused")
	}
	// After the window the old buckets are reclaimed and new keys fit again.
	if !l.allow("fresh", now.Add(2*time.Minute)) {
		t.Fatal("the table did not recover after the window")
	}
	if n := len(l.buckets); n != 1 {
		t.Fatalf("table size after recovery = %d, want 1", n)
	}
}
