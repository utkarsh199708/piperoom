package httpapi

import (
	"container/list"
	"net/netip"
	"sync"
	"time"
)

// maxLoginBuckets is the hard cap on tracked client keys (memory bound).
const maxLoginBuckets = 10000

// limitKey is the rate-limit key for a client address: an IPv6 address counts
// as its /64 (one subscriber owns a whole /64, so per-address keys would let one
// client mint a fresh budget for every address and fill the table), an IPv4
// address as itself.
func limitKey(ip string) string {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	a = a.Unmap()
	if a.Is6() {
		return netip.PrefixFrom(a, 64).Masked().String()
	}
	return a.String()
}

// rateLimiter is a fixed one-minute window counter per key, in memory and per
// process (design §1.1). The table holds at most max keys. When it is full the
// least recently used key is evicted to make room, so a flood of new keys can
// never lock a new person out of login (the route needs no login, design §1.1);
// the price is that an evicted key starts a fresh window. A key that keeps
// calling is always the most recently used, so it is never evicted and keeps
// its 429. Every operation is O(1) under the mutex: no scan of the table.
type rateLimiter struct {
	limit  int
	window time.Duration
	max    int // hard cap on tracked keys

	mu    sync.Mutex
	byKey map[string]*list.Element // value: *bucket
	lru   *list.List               // front = most recently used
}

type bucket struct {
	key   string
	start time.Time
	n     int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, max: maxLoginBuckets, byKey: map[string]*list.Element{}, lru: list.New()}
}

// size is the number of tracked keys.
func (l *rateLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.byKey)
}

// allow counts one call for key at now and reports whether it is within the limit.
func (l *rateLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.byKey[key]; ok {
		l.lru.MoveToFront(el)
		b := el.Value.(*bucket)
		if now.Sub(b.start) >= l.window {
			b.start, b.n = now, 0
		}
		b.n++
		return b.n <= l.limit
	}
	if len(l.byKey) >= l.max {
		oldest := l.lru.Back()
		delete(l.byKey, oldest.Value.(*bucket).key)
		l.lru.Remove(oldest)
	}
	l.byKey[key] = l.lru.PushFront(&bucket{key: key, start: now, n: 1})
	return 1 <= l.limit
}
