//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// waitForBrokerOffsets polls the broker, through a kadm client independent of
// the Client under test, until partition 0 of topic reports high watermark hw
// and last stable offset lso. EndTransaction returns once the coordinator has
// logged PREPARE_COMMIT/PREPARE_ABORT; it writes the marker to the partition
// afterwards, asynchronously. Until that marker lands the partition still
// sees an open transaction (HW one short, LSO pinned at the transaction's
// first offset), so asserting straight after EndTransaction races the broker.
func waitForBrokerOffsets(t *testing.T, adm *kadm.Client, topic string, hw, lso int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var gotHW, gotLSO int64 = -1, -1
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ends, eerr := adm.ListEndOffsets(ctx, topic)
		stable, serr := adm.ListCommittedOffsets(ctx, topic)
		cancel()
		if eerr == nil && serr == nil {
			if e, ok := ends.Lookup(topic, 0); ok && e.Err == nil {
				gotHW = e.Offset
			}
			if l, ok := stable.Lookup(topic, 0); ok && l.Err == nil {
				gotLSO = l.Offset
			}
			if gotHW == hw && gotLSO == lso {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s: broker at HW %d, LSO %d after 10 s (errs %v, %v); want HW %d, LSO %d",
				topic, gotHW, gotLSO, eerr, serr, hw, lso)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
