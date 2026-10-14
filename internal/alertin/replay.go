package alertin

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DefaultBearerDedupWindow is how long an identical body from a bearer-token
// source (Alertmanager, Datadog) counts as a repeat of the same delivery. Those
// sources sign nothing, so a captured request stays valid until the token
// changes; this only stops a burst of copies from flooding a room.
const DefaultBearerDedupWindow = time.Minute

// DefaultReplayEntries bounds a ReplayCache. Legit traffic within a window is
// far below it because the alert route is rate limited per source.
const DefaultReplayEntries = 16384

// ReplayWindow is how long a verified delivery from src must be remembered so
// an identical copy is caught: as long as its signature stays acceptable.
//   - Grafana, generic: the signed timestamp is accepted within MaxSkew either
//     side of the server clock, so a copy can arrive up to 2×MaxSkew after the
//     first one.
//   - PagerDuty: no signed timestamp; the body's event time bounds it.
//   - Bearer sources: DefaultBearerDedupWindow.
func (c Config) ReplayWindow(src Source) time.Duration {
	c = c.withDefaults()
	switch src {
	case Grafana, Generic:
		return 2 * c.MaxSkew
	case PagerDuty:
		return c.MaxEventAge + c.MaxSkew
	}
	return DefaultBearerDedupWindow
}

// DeliveryKey identifies one delivery of src for replay detection: the
// source, the signed timestamp and the body. It deliberately leaves out the
// signature header itself: a header with several v1= values (secret rotation)
// can be reordered or padded and still verify, so keying on its raw text would
// let a copy through.
func (c Config) DeliveryKey(src Source, h http.Header, body []byte) string {
	c = c.withDefaults()
	var ts string
	switch src {
	case Grafana:
		ts = strings.TrimSpace(h.Get(c.GrafanaTimestampHeader))
	case Generic:
		ts, _ = parseGenericHeader(h.Get(GenericSignatureHeader))
	}
	m := sha256.New()
	m.Write([]byte(src))
	m.Write([]byte{0})
	m.Write([]byte(ts))
	m.Write([]byte{0})
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// ReplayCache remembers delivery keys for a while, in memory and per process.
// It holds at most max keys; when full, the oldest is dropped. Use one per
// server; it is safe for concurrent use.
type ReplayCache struct {
	max int

	mu    sync.Mutex
	byKey map[string]*list.Element // value: *replayEntry
	order *list.List               // front = oldest insert
}

type replayEntry struct {
	key     string
	expires time.Time
}

// NewReplayCache returns a cache holding at most max keys
// (DefaultReplayEntries when max <= 0).
func NewReplayCache(max int) *ReplayCache {
	if max <= 0 {
		max = DefaultReplayEntries
	}
	return &ReplayCache{max: max, byKey: map[string]*list.Element{}, order: list.New()}
}

// Seen reports whether key was recorded and has not expired at now. If not, it
// records key until now+ttl. Check and record are one step, so of two
// identical concurrent requests exactly one is new.
func (rc *ReplayCache) Seen(key string, ttl time.Duration, now time.Time) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if el, ok := rc.byKey[key]; ok {
		e := el.Value.(*replayEntry)
		if now.Before(e.expires) {
			return true
		}
		rc.order.Remove(el)
		delete(rc.byKey, key)
	}
	// Drop expired entries from the front; entries further back that expired
	// earlier (shorter ttl) go when they reach the front or the cap evicts them.
	for el := rc.order.Front(); el != nil; el = rc.order.Front() {
		e := el.Value.(*replayEntry)
		if now.Before(e.expires) && len(rc.byKey) < rc.max {
			break
		}
		rc.order.Remove(el)
		delete(rc.byKey, e.key)
	}
	rc.byKey[key] = rc.order.PushBack(&replayEntry{key: key, expires: now.Add(ttl)})
	return false
}

// Forget removes key, so a retry of a delivery that failed after it was
// recorded (store error, rate limit) is not taken for a copy.
func (rc *ReplayCache) Forget(key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if el, ok := rc.byKey[key]; ok {
		rc.order.Remove(el)
		delete(rc.byKey, key)
	}
}

// Len is the number of keys held.
func (rc *ReplayCache) Len() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.byKey)
}
