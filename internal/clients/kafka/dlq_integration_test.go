//go:build integration

package kafka

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/testenv"
)

// requireKafka skips unless the dev Kafka broker accepts TCP. These tests
// need Kafka only, so they do not use testenv.Require (it waits for Connect).
func requireKafka(t *testing.T) []string {
	t.Helper()
	brokers := strings.Split(config.Load().KafkaBrokers, ",")
	conn, err := net.DialTimeout("tcp", brokers[0], 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s: %v", brokers[0], err)
	}
	_ = conn.Close()
	return brokers
}

func dlqTestClient(t *testing.T, brokers []string) *Client {
	t.Helper()
	c, err := New(brokers, nil)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func createTopic(t *testing.T, adm *kadm.Client, partitions int32) string {
	t.Helper()
	topic := testenv.Name("it-orders-dlq")
	resps, err := adm.CreateTopics(context.Background(), partitions, 1, nil, topic)
	if err != nil || resps.Error() != nil {
		t.Fatalf("CreateTopics: %v %v", err, resps.Error())
	}
	t.Cleanup(func() {
		if _, err := adm.DeleteTopics(context.Background(), topic); err != nil {
			t.Logf("DeleteTopics: %v", err)
		}
	})
	return topic
}

// waitForTopic polls DLQOffsets until the topic is visible: kadm caches
// metadata (MetadataMinAge, 5 s), so a "missing" answer sticks for a while.
func waitForTopic(t *testing.T, c *Client, topic string) map[string][]clients.PartitionOffsets {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for {
		offs, err := c.DLQOffsets(context.Background(), topic)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := offs[topic]; ok {
			return offs
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s still absent after 12 s", topic)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func dlqHeaders(msg string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "__connect.errors.exception.class.name", Value: []byte("org.apache.kafka.connect.errors.DataException")},
		{Key: "__connect.errors.exception.message", Value: []byte(msg)},
		{Key: "app-secret", Value: []byte("nope")},
	}
}

// TestDLQReaderAgainstDevStack checks DLQOffsets/PeekDLQ on a real broker:
// absent before creation (and not auto-created), counts after, newest first
// across partitions, redacted, no key or value.
func TestDLQReaderAgainstDevStack(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	ctx := context.Background()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(producer.Close)
	adm := kadm.NewClient(producer)

	topic := testenv.Name("it-orders-dlq")
	if offs, err := c.DLQOffsets(ctx, topic); err != nil || len(offs) != 0 {
		t.Fatalf("before creation: %v, %v", offs, err)
	}
	if _, err := c.PeekDLQ(ctx, topic, 3); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("peek of missing topic: %v, want ErrNotFound", err)
	}
	if td, err := adm.ListTopics(ctx, topic); err != nil || td.Has(topic) {
		t.Fatalf("topic was auto-created by a read: %v, %v", td, err)
	}

	resps, err := adm.CreateTopics(ctx, 3, 1, nil, topic)
	if err != nil || resps.Error() != nil {
		t.Fatalf("CreateTopics: %v %v", err, resps.Error())
	}
	t.Cleanup(func() {
		if _, err := adm.DeleteTopics(context.Background(), topic); err != nil {
			t.Logf("DeleteTopics: %v", err)
		}
	})

	const keyJunk, valJunk = "JUNKKEY-it-91", "JUNKVAL-it-37"
	base := time.Now().Add(-time.Minute)
	mk := func(part int32, sec int, msg string) *kgo.Record {
		return &kgo.Record{
			Topic: topic, Partition: part, Key: []byte(keyJunk), Value: []byte(`{"v":"` + valJunk + `"}`),
			Timestamp: base.Add(time.Duration(sec) * time.Second), Headers: dlqHeaders(msg),
		}
	}
	// Partition 0 holds four old records; partition 2 one record that is the
	// newest of all (and carries a Jackson-style [Source:] tail).
	recs := []*kgo.Record{mk(0, 0, "a"), mk(0, 1, "b"), mk(0, 2, "c"), mk(0, 3, "d"),
		mk(2, 30, `Unexpected character at [Source: (byte[])"{"email":"`+valJunk+`"}"; line: 1, column: 2]`)}
	if err := producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// kadm may have cached the "missing" answer; wait until it is gone.
	offs := waitForTopic(t, c, topic)
	var total int64
	for _, p := range offs[topic] {
		total += p.End - p.Start
	}
	if len(offs[topic]) != 3 || total != 5 {
		t.Fatalf("offsets = %+v, want 3 partitions holding 5 records", offs[topic])
	}

	got, err := c.PeekDLQ(ctx, topic, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Partition != 2 || got[1].Offset != 3 || got[2].Offset != 2 {
		t.Fatalf("peek = %+v, want p2 newest, then p0@3, p0@2", got)
	}
	if got[0].Headers["__connect.errors.exception.class.name"] == "" {
		t.Fatalf("headers missing: %+v", got[0])
	}
	if _, ok := got[0].Headers["app-secret"]; ok {
		t.Fatal("application header leaked")
	}
	if m := got[0].Headers["__connect.errors.exception.message"]; strings.Contains(m, valJunk) || strings.Contains(m, "Source") {
		t.Fatalf("message not redacted: %q", m)
	}
	b, _ := json.Marshal(got)
	for _, m := range []string{keyJunk, valJunk} {
		for _, f := range []string{m, base64.StdEncoding.EncodeToString([]byte(m)), hex.EncodeToString([]byte(m))} {
			if strings.Contains(string(b), f) {
				t.Fatalf("%q leaked into %s", f, b)
			}
		}
	}
}

// TestPeekDLQTransactionalWrites: an aborted transaction and a committed one
// leave control markers at the end. PeekDLQ must finish promptly, show only
// the committed records and never a marker.
func TestPeekDLQTransactionalWrites(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	ctx := context.Background()

	txn, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID(testenv.Name("it-txn")))
	if err != nil {
		t.Fatalf("txn producer: %v", err)
	}
	t.Cleanup(txn.Close)
	adm := kadm.NewClient(txn)
	topic := createTopic(t, adm, 1)

	write := func(n int, commit kgo.TransactionEndTry, tag string) {
		t.Helper()
		if err := txn.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			r := &kgo.Record{Topic: topic, Value: []byte("v"), Headers: dlqHeaders(tag)}
			if err := txn.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := txn.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := txn.EndTransaction(ctx, commit); err != nil {
			t.Fatal(err)
		}
	}
	write(2, kgo.TryAbort, "aborted")
	write(3, kgo.TryCommit, "committed")
	waitForBrokerOffsets(t, adm, topic, 7, 7) // both markers are on the partition

	offs := waitForTopic(t, c, topic)
	if p := offs[topic][0]; p.End-p.Start != 7 { // 2 + abort marker + 3 + commit marker
		t.Fatalf("offsets = %+v, want 7 offsets", offs[topic])
	}
	start := time.Now()
	got, err := c.PeekDLQ(ctx, topic, 10)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("PeekDLQ took %s: waited for a marker", d)
	}
	if len(got) != 3 {
		t.Fatalf("peek = %+v, want the 3 committed records", got)
	}
	for _, r := range got {
		if r.Headers["__connect.errors.exception.message"] != "committed" {
			t.Fatalf("aborted record shown: %+v", r)
		}
	}
}
