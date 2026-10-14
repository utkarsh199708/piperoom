package watch

import (
	"context"
	"slices"
	"testing"
	"time"
)

// TestTickIsOneCheckTimeout: status, config and DLQ reads run side by side, so
// with every layer slow a tick takes about one call, not three in a row.
func TestTickIsOneCheckTimeout(t *testing.T) {
	const slow = 150 * time.Millisecond
	h := newHarness(t, opts{checkTimeout: time.Second})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-sink-dlq"})
	h.kafka.set("orders-sink-dlq", 0, 0)
	h.step(0) // learns the DLQ topic from the config
	h.conn.mu.Lock()
	h.conn.delay = slow
	h.conn.mu.Unlock()
	h.kafka.mu.Lock()
	h.kafka.hook = func(context.Context) { time.Sleep(slow) }
	h.kafka.mu.Unlock()
	before := len(h.kafka.callLists())
	start := time.Now()
	h.step(time.Minute)
	took := time.Since(start)
	if took >= 2*slow {
		t.Fatalf("tick took %s with each layer at %s; want about one layer", took, slow)
	}
	calls := h.kafka.callLists()[before:]
	if len(calls) != 1 || !slices.Equal(calls[0], []string{"orders-sink-dlq"}) {
		t.Fatalf("DLQOffsets calls = %v, want one for the known topic", calls)
	}
	h.w.mu.Lock()
	st := h.w.dlqState["orders-sink-dlq"]
	n := 0
	if st != nil {
		n = len(st.samples)
	}
	h.w.mu.Unlock()
	if n != 2 {
		t.Fatalf("DLQ samples = %d after two ticks, want 2 (prefetched offsets not recorded)", n)
	}
}

// TestRefireAfterResolveIsNotARestart: the room service keeps a resolved room
// open, so the next fire joins it; the note must not claim a restart.
func TestRefireAfterResolveIsNotARestart(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	h.fixConnector("orders")
	h.steps(5, time.Minute)
	if got := len(h.rooms.resolved()); got != 1 {
		t.Fatalf("resolved = %d, want 1\n%s", got, h.logs)
	}
	h.breakConnector("orders", "boom again")
	h.steps(5, time.Minute)
	if got := h.rooms.postsContaining("re-attached after a restart"); len(got) != 0 {
		t.Fatalf("a re-fire in the same process claimed a restart: %+v", got)
	}
	if got := h.rooms.postsContaining("fired again after it resolved"); len(got) != 1 {
		t.Fatalf("re-fire note = %+v, want one", got)
	}
}

// TestDLQNewTopicIsReadAfterConfig: a topic the prefetch did not know (the
// config changed) is still read in the same tick.
func TestDLQNewTopicIsReadAfterConfig(t *testing.T) {
	h := newHarness(t, opts{})
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "dlq-a"})
	h.step(0)
	h.setSinkConfig("orders", map[string]string{"errors.tolerance": "all", "errors.deadletterqueue.topic.name": "dlq-b"})
	before := len(h.kafka.callLists())
	h.step(time.Minute)
	calls := h.kafka.callLists()[before:]
	if len(calls) != 2 || !slices.Equal(calls[0], []string{"dlq-a"}) || !slices.Equal(calls[1], []string{"dlq-b"}) {
		t.Fatalf("DLQOffsets calls = %v, want prefetch [dlq-a] then [dlq-b]", calls)
	}
}
