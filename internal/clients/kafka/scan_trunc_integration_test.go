//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestFindByKeyReportTruncated: key b at offsets 2-6 and a tombstone at 7.
// max_scan 4 stops at offset 3, before the partition end, and must say so;
// a large max_scan reaches the end and is not truncated.
func TestFindByKeyReportTruncated(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	topic := fixkTopic(t, c.adm)

	pc, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	keys := []string{"a", "a", "b", "b", "b", "b", "b", "b"}
	for i, k := range keys {
		r := &kgo.Record{Topic: topic, Key: []byte(k), Value: []byte("v")}
		if i == 7 {
			r.Value = nil // tombstone
		}
		if err := pc.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	waitForTopic(t, c, topic)

	rep, err := c.FindByKeyReport(ctx, topic, "b", time.Now().Add(-time.Hour), 4)
	if err != nil {
		t.Fatalf("FindByKeyReport: %v", err)
	}
	if len(rep.Refs) != 2 || rep.Refs[0].Offset != 2 || rep.Refs[1].Offset != 3 {
		t.Fatalf("refs = %+v, want offsets 2, 3", rep.Refs)
	}
	if len(rep.Truncated) != 1 || rep.Truncated[0] != 0 {
		t.Fatalf("truncated = %v, want [0]", rep.Truncated)
	}

	rep, err = c.FindByKeyReport(ctx, topic, "b", time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("FindByKeyReport full: %v", err)
	}
	if len(rep.Refs) != 6 || rep.Refs[5].Offset != 7 || len(rep.Truncated) != 0 {
		t.Fatalf("full scan = %+v, want 6 refs ending at 7, not truncated", rep)
	}
}
