package httpapi

import (
	"fmt"
	"testing"
	"time"
)

// A full table evicts the least recently used key; it never refuses a new key
// (that would be an anonymous lockout of /auth/login), and it never grows.
func TestRateLimiterFullTableEvictsInsteadOfRefusing(t *testing.T) {
	l := newRateLimiter(300, time.Minute) // the real cap: 10,000 keys
	if l.max != 10000 {
		t.Fatalf("cap = %d, want 10000", l.max)
	}
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for i := 0; i < l.max; i++ {
		if !l.allow(fmt.Sprintf("k%d", i), now) {
			t.Fatalf("key %d refused before the cap", i)
		}
	}
	if l.size() != 10000 {
		t.Fatalf("size = %d, want 10000", l.size())
	}
	// The table is full; a fresh key is allowed, and memory stays at the cap.
	if !l.allow("fresh", now) {
		t.Fatal("a new key was refused because the table is full")
	}
	if l.size() != 10000 {
		t.Fatalf("size after a new key = %d, want 10000", l.size())
	}
	// A flood of 5,000 more new keys (an attacker with a big IPv6 range): every
	// one is allowed, the table stays at the cap, and a real newcomer still gets in.
	for i := 0; i < 5000; i++ {
		if !l.allow(fmt.Sprintf("flood%d", i), now) {
			t.Fatalf("flood key %d refused", i)
		}
	}
	if l.size() != 10000 {
		t.Fatalf("size after the flood = %d, want 10000", l.size())
	}
	if !l.allow("a-real-person", now) {
		t.Fatal("a new person was locked out by the flood")
	}
	// The oldest keys were the ones evicted.
	if _, ok := l.byKey["k0"]; ok {
		t.Error("the least recently used key was not evicted")
	}
	if _, ok := l.byKey["a-real-person"]; !ok {
		t.Error("the newest key is not tracked")
	}
}

// A single key over its limit keeps getting 429, even while the table churns:
// it is the most recently used key, so it is never evicted.
func TestRateLimiterHotKeyStaysLimitedUnderChurn(t *testing.T) {
	l := newRateLimiter(300, time.Minute)
	l.max = 100
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	denied := 0
	for i := 0; i < 1000; i++ {
		if !l.allow("hot", now) {
			denied++
		}
		l.allow(fmt.Sprintf("noise%d", i), now) // a new key between every hot call
	}
	if denied != 700 {
		t.Fatalf("hot key denied %d times, want 700 (1000 calls, 300 allowed)", denied)
	}
	if l.size() != 100 {
		t.Fatalf("size = %d, want 100", l.size())
	}
	// A new window resets the hot key.
	if !l.allow("hot", now.Add(time.Minute)) {
		t.Fatal("the window did not reset")
	}
}

// The LRU order follows use, not insertion.
func TestRateLimiterEvictsLeastRecentlyUsed(t *testing.T) {
	l := newRateLimiter(10, time.Minute)
	l.max = 3
	now := time.Now()
	l.allow("a", now)
	l.allow("b", now)
	l.allow("c", now)
	l.allow("a", now) // a is now the most recently used; b is the oldest
	l.allow("d", now) // evicts b
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := l.byKey[k]; !ok {
			t.Errorf("%s was evicted", k)
		}
	}
	if _, ok := l.byKey["b"]; ok {
		t.Error("b should have been evicted")
	}
}
