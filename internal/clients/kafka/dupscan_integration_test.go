//go:build integration

package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestDupScanAgainstDevStack: the dev stack's real Debezium Avro events
// decode (no malformed, refused or lsn-less envelope), and copies of them
// with two repeats on an it_ topic are reported as duplicates. The X1g break
// (devctl break consumer-duplicates) is the end-to-end case (TC8) once it
// lands.
func TestDupScanAgainstDevStack(t *testing.T) {
	brokers := requireKafka(t)
	c := newClient(t)
	ctx := unscopedCtx(t)
	rep, err := c.DupScan(ctx, "pg.public.orders", epoch, 2000)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pg.public.orders: %d scanned, %d duplicates, %d regressions, undecodable %v", rep.Scanned, rep.Duplicates, rep.OrderRegressions, rep.UndecodableReasons)
	for _, why := range []string{UndecodableMalformed, UndecodableSchema, UndecodableNoLSN, UndecodableFormat} {
		if n := rep.UndecodableReasons[why]; n > 0 {
			t.Errorf("%d real events undecodable as %s", n, why)
		}
	}

	recs, err := NewReplayReader(c).ReadRecords(ctx, "pg.public.orders", 0, 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) < 2 {
		t.Skip("pg.public.orders has fewer than 2 records; run the dev stack's load first")
	}
	raw, w := rawClient(t, brokers), writerOn(t, brokers)
	topic := itTopic(t, raw, w, "it-dupscan", 1)
	var out []*kgo.Record
	for _, r := range append(recs, recs[0], recs[0]) {
		out = append(out, &kgo.Record{Topic: topic, Partition: 0, Key: r.Key, Value: r.Value})
	}
	if err := raw.ProduceSync(ctx, out...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	rep, err = c.DupScan(ctx, topic, epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != len(out) || rep.Duplicates < 2 || rep.Keys < 1 || rep.Undecodable-rep.UndecodableReasons[UndecodableNoKey] != 0 {
		t.Fatalf("report = %+v, want %d scanned and the 2 repeats found", rep, len(out))
	}
}
