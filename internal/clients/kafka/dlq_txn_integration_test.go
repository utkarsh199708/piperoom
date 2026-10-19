//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/testenv"
)

// txnWriter writes DLQ-style records in transactions on one topic.
type txnWriter struct {
	t     *testing.T
	cl    *kgo.Client
	topic string
}

func newTxnWriter(t *testing.T, brokers []string) (*txnWriter, *kadm.Client) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID(testenv.Name("it-txn")))
	if err != nil {
		t.Fatalf("txn producer: %v", err)
	}
	t.Cleanup(cl.Close)
	adm := kadm.NewClient(cl)
	return &txnWriter{t: t, cl: cl, topic: createTopic(t, adm, 1)}, adm
}

// begin starts a transaction, writes n records and flushes them, leaving it
// open.
func (w *txnWriter) begin(n int, tag string) {
	w.t.Helper()
	ctx := context.Background()
	if err := w.cl.BeginTransaction(); err != nil {
		w.t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		r := &kgo.Record{Topic: w.topic, Value: []byte("v"), Headers: dlqHeaders(tag)}
		if err := w.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			w.t.Fatal(err)
		}
	}
	if err := w.cl.Flush(ctx); err != nil {
		w.t.Fatal(err)
	}
}

func (w *txnWriter) end(commit kgo.TransactionEndTry) {
	w.t.Helper()
	if err := w.cl.EndTransaction(context.Background(), commit); err != nil {
		w.t.Fatal(err)
	}
}

// peekQuick runs PeekDLQ and fails when it is slow or does not return exactly
// the "committed" records.
func peekQuick(t *testing.T, c *Client, topic string, want int) {
	t.Helper()
	start := time.Now()
	got, err := c.PeekDLQ(context.Background(), topic, 10)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("PeekDLQ took %s: waited on a marker or an open transaction", d)
	}
	if len(got) != want {
		t.Fatalf("peek = %+v, want %d committed records", got, want)
	}
	for _, r := range got {
		if r.Headers["__connect.errors.exception.message"] != "committed" {
			t.Fatalf("uncommitted or aborted record shown: %+v", r)
		}
	}
}

// TestPeekDLQCommitThenAbort: the abort marker sits at end-1.
func TestPeekDLQCommitThenAbort(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	w, adm := newTxnWriter(t, brokers)

	w.begin(3, "committed")
	w.end(kgo.TryCommit) // offsets 0..2, commit marker at 3
	w.begin(2, "aborted")
	w.end(kgo.TryAbort) // offsets 4..5, abort marker at 6

	waitForTopic(t, c, w.topic)
	waitForBrokerOffsets(t, adm, w.topic, 7, 7) // both markers are on the partition
	peekQuick(t, c, w.topic, 3)
}

// TestPeekDLQTransactionLeftOpen: an open transaction at the tail holds the
// last stable offset below the high watermark. The peek plans against the
// LSO, returns the committed records promptly, and keeps working after the
// transaction aborts.
func TestPeekDLQTransactionLeftOpen(t *testing.T) {
	brokers := requireKafka(t)
	c := dlqTestClient(t, brokers)
	w, adm := newTxnWriter(t, brokers)

	// Committed records come from a second producer so this transaction can
	// stay open on the first.
	other, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID(testenv.Name("it-txn")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	ow := &txnWriter{t: t, cl: other, topic: w.topic}
	ow.begin(3, "committed")
	ow.end(kgo.TryCommit) // offsets 0..2, commit marker at 3

	w.begin(2, "open") // offsets 4..5 stay undecided
	waitForTopic(t, c, w.topic)
	waitForBrokerOffsets(t, adm, w.topic, 6, 4) // commit marker landed, LSO pinned at 4
	peekQuick(t, c, w.topic, 3)                 // transaction still open

	w.end(kgo.TryAbort) // abort marker at 6
	waitForBrokerOffsets(t, adm, w.topic, 7, 7)
	peekQuick(t, c, w.topic, 3) // after the abort marker landed
}
