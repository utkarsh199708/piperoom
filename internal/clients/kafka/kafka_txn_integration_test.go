//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestAdminReadsOnTransactionalTopic: a commit marker at the tail, then a
// transaction left open there, must stall neither TopicOffsets nor
// FindByKey, and End stays the high watermark.
func TestAdminReadsOnTransactionalTopic(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	w, adm := newTxnWriter(t, brokers)

	w.begin(3, "committed")
	w.end(kgo.TryCommit) // offsets 0..2, commit marker at 3
	waitForTopic(t, c, w.topic)
	waitForBrokerOffsets(t, adm, w.topic, 4, 4) // the commit marker is on the partition
	check := func(wantEnd int64) {
		t.Helper()
		start := time.Now()
		offs, err := c.TopicOffsets(context.Background(), w.topic)
		if err != nil {
			t.Fatal(err)
		}
		if len(offs) != 1 || offs[0].End != wantEnd || offs[0].LastTimestamp.IsZero() {
			t.Fatalf("offsets = %+v, want End %d and a timestamp", offs, wantEnd)
		}
		refs, err := c.FindByKey(context.Background(), w.topic, "absent", time.Now().Add(-time.Hour), 100)
		if err != nil || len(refs) != 0 {
			t.Fatalf("FindByKey = %+v, %v; want a clean miss", refs, err)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Fatalf("took %s: waited on a marker or an open transaction", d)
		}
	}
	check(4)

	w.begin(2, "open") // offsets 4..5 stay undecided
	waitForBrokerOffsets(t, adm, w.topic, 6, 4)
	check(6)
	w.end(kgo.TryAbort)
}
