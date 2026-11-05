package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These tests run the real wire protocol (kadm ListOffsets, read_committed
// Fetch, transaction markers) against kfake, franz-go's in-process broker, so
// they need no Docker. The fake-seam tests in admin_test.go and dlq_test.go
// check the logic; these check that the logic matches what a broker sends.

const kfTopic = "kf-txn"

// kfCluster starts a one-broker kfake cluster with kfTopic (one partition)
// and returns its seed addresses.
func kfCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, kfTopic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// kfClient is the client under test on the kfake cluster, without a Schema
// Registry: keys are compared raw.
func kfClient(t *testing.T, addrs []string) *Client {
	t.Helper()
	c, err := New(addrs, nil)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// kfProducer is a transactional producer on kfTopic.
type kfProducer struct {
	t  *testing.T
	cl *kgo.Client
}

func newKFProducer(t *testing.T, addrs []string, id string) *kfProducer {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.TransactionalID(id))
	if err != nil {
		t.Fatalf("txn producer: %v", err)
	}
	t.Cleanup(cl.Close)
	return &kfProducer{t: t, cl: cl}
}

// begin starts a transaction and writes one record per key, each with the
// given timestamp and a Connect error-message header of tag. The transaction
// is left open.
func (p *kfProducer) begin(ts time.Time, tag string, keys ...string) {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.cl.BeginTransaction(); err != nil {
		p.t.Fatalf("begin: %v", err)
	}
	for _, k := range keys {
		r := &kgo.Record{Topic: kfTopic, Key: []byte(k), Value: []byte("v"), Timestamp: ts, Headers: []kgo.RecordHeader{hdr(headerMessage, tag)}}
		if err := p.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			p.t.Fatalf("produce: %v", err)
		}
	}
}

func (p *kfProducer) end(how kgo.TransactionEndTry) {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.cl.EndTransaction(ctx, how); err != nil {
		p.t.Fatalf("end transaction: %v", err)
	}
}

const (
	// withinBound is how long a read under within may take. A read that gets
	// past an aborted tail or an open transaction returns after about one
	// stallAfter (0.5 to 0.6 s measured on an idle machine); one stuck there
	// waits out a timer instead: TopicOffsets the caller's withinDeadline
	// (and fails with the deadline), PeekDLQ withinDeadline, FindByKey
	// withinDeadline (at most its own 15 s). Every stuck read therefore runs
	// the full withinDeadline, so the bound only needs to sit below it: 8 s
	// gives a loaded CI machine about 15 times the idle time (the old 4 s
	// bound gave about 7 times, and a busy CI run still pushed a healthy
	// read past the record read's old 5 s deadline).
	withinBound = 8 * time.Second
	// withinDeadline is the ctx deadline under within. It is well past
	// withinBound, so a stuck PeekDLQ or FindByKey, which still returns no
	// error, overruns the bound instead of being cut short just under it.
	// It is short enough that three stuck reads in one test (10 + 10 + 10 s)
	// end before kgo's default 40 s transaction timeout aborts the open
	// transaction, which would unstick the reads after them.
	withinDeadline = 10 * time.Second
)

// within runs fn under withinDeadline and fails it when it takes longer than
// withinBound: every read here is expected to get past a marker, an aborted
// tail or an open transaction in about one stallAfter, and the old behaviour
// hung until its own 5 s or 15 s deadline or the caller's.
func within(t *testing.T, fn func(ctx context.Context)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), withinDeadline)
	defer cancel()
	start := time.Now()
	fn(ctx)
	if d := time.Since(start); d > withinBound {
		t.Fatalf("took %s (bound %s): waited on a marker or an open transaction", d, withinBound)
	}
}

// TestKFakeTopicOffsetsTransactionalTail: with a commit marker as the last
// offset, TopicOffsets returns at once, End is the high watermark (marker
// included) and LastTimestamp is the marker's (when the transaction ended).
// With a transaction then left open at the tail, End still moves with the
// high watermark, but LastTimestamp stays at the last stable point.
//
// Old behaviour (read_uncommitted consumer dropping markers, no LSO clamp):
// the read at the marker offset never got a record and failed after 5 s; and
// with the open transaction it reported the open records' timestamp.
func TestKFakeTopicOffsetsTransactionalTail(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	p := newKFProducer(t, addrs, "kf-offsets")

	// Record timestamps far from now, so the marker's (now) is told apart.
	committedTS := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	openTS := time.Now().Add(-1 * time.Hour).Truncate(time.Millisecond)

	p.begin(committedTS, "committed", "k0", "k1", "k2") // offsets 0..2
	beforeCommit := time.Now().Truncate(time.Millisecond)
	p.end(kgo.TryCommit) // commit marker at 3
	afterCommit := time.Now()

	var markerTS time.Time
	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if len(offs) != 1 || offs[0].Start != 0 || offs[0].End != 4 {
			t.Fatalf("offsets = %+v, want one partition [0, 4)", offs)
		}
		markerTS = offs[0].LastTimestamp
		if markerTS.Before(beforeCommit) || markerTS.After(afterCommit) {
			t.Fatalf("LastTimestamp = %s, want the commit marker's time in [%s, %s]",
				markerTS, beforeCommit, afterCommit)
		}
	})

	p.begin(openTS, "open", "k3", "k4") // offsets 4..5, undecided
	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets with open transaction: %v", err)
		}
		if len(offs) != 1 || offs[0].End != 6 {
			t.Fatalf("offsets = %+v, want End 6 (high watermark)", offs)
		}
		if !offs[0].LastTimestamp.Equal(markerTS) {
			t.Fatalf("LastTimestamp = %s, want %s (last stable point, not the open records' %s)",
				offs[0].LastTimestamp, markerTS, openTS)
		}
	})
	p.end(kgo.TryAbort)
}

// TestKFakeFindByKeySkipsAbortedAndOpen: FindByKey finds committed records
// and never records of an aborted transaction or of one still open, and a
// miss returns at once although the tail is a marker or an open transaction.
//
// Old behaviour: a read_uncommitted scan matched "aborted" and "open", and a
// miss on a marker tail hung until the 15 s scan deadline.
func TestKFakeFindByKeySkipsAbortedAndOpen(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	p := newKFProducer(t, addrs, "kf-find")
	ts := time.Now().Add(-time.Minute)
	since := ts.Add(-time.Minute)

	p.begin(ts, "committed", "c0", "c1", "c2") // 0..2, commit marker 3
	p.end(kgo.TryCommit)
	p.begin(ts, "aborted", "aborted", "aborted") // 4..5, abort marker 6
	p.end(kgo.TryAbort)

	find := func(ctx context.Context, key string) []int64 {
		t.Helper()
		refs, err := c.FindByKey(ctx, kfTopic, key, since, 100)
		if err != nil {
			t.Fatalf("FindByKey(%q): %v", key, err)
		}
		var offs []int64
		for _, r := range refs {
			if r.Key != key || r.Topic != kfTopic {
				t.Fatalf("FindByKey(%q) returned %+v", key, r)
			}
			offs = append(offs, r.Offset)
		}
		return offs
	}

	within(t, func(ctx context.Context) {
		if got := find(ctx, "c1"); len(got) != 1 || got[0] != 1 {
			t.Fatalf("committed c1 at offsets %v, want [1]", got)
		}
		if got := find(ctx, "aborted"); len(got) != 0 {
			t.Fatalf("aborted records matched at offsets %v", got)
		}
		if got := find(ctx, "absent"); len(got) != 0 {
			t.Fatalf("absent key matched at offsets %v", got)
		}
	})

	p.begin(ts, "open", "open", "open") // 7..8, undecided
	within(t, func(ctx context.Context) {
		if got := find(ctx, "open"); len(got) != 0 {
			t.Fatalf("records of an open transaction matched at offsets %v", got)
		}
		if got := find(ctx, "c2"); len(got) != 1 || got[0] != 2 {
			t.Fatalf("committed c2 at offsets %v, want [2]", got)
		}
	})

	// Once the transaction commits, its records become readable.
	p.end(kgo.TryCommit)
	within(t, func(ctx context.Context) {
		if got := find(ctx, "open"); len(got) != 2 || got[0] != 7 || got[1] != 8 {
			t.Fatalf("committed open-then-committed records at %v, want [7 8]", got)
		}
	})
}

// TestKFakePeekDLQTransactional: PeekDLQ returns only committed records, with
// an abort marker at the tail and then with a transaction left open there.
func TestKFakePeekDLQTransactional(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	p := newKFProducer(t, addrs, "kf-peek")
	ts := time.Now().Add(-time.Minute)

	p.begin(ts, "committed", "a", "b", "c")
	p.end(kgo.TryCommit)
	p.begin(ts, "aborted", "d", "e")
	p.end(kgo.TryAbort)

	peek := func(ctx context.Context) {
		t.Helper()
		got, err := c.PeekDLQ(ctx, kfTopic, 10)
		if err != nil {
			t.Fatalf("PeekDLQ: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("peek = %+v, want the 3 committed records", got)
		}
		for i, r := range got {
			if r.Headers[headerMessage] != "committed" {
				t.Fatalf("record %d is not committed: %+v", i, r)
			}
			if want := int64(2 - i); r.Offset != want {
				t.Fatalf("record %d at offset %d, want %d (newest first)", i, r.Offset, want)
			}
		}
	}
	within(t, peek)

	p.begin(ts, "open", "f", "g")
	within(t, peek)
	p.end(kgo.TryAbort)
	within(t, peek)
}

// TestKFakeAbortedBelowOpenTransaction (#300): transaction C writes offset 0,
// transaction A (another producer) writes offset 1, then C aborts with its
// marker at 2 while A is open, so the LSO is 1. The read_committed consumer
// drops offset 0 and never sees the marker at 2, so no record at or past the
// planned last offset 0 ever arrives. TopicOffsets, FindByKey and PeekDLQ
// still return at once: nothing readable is left below the end.
//
// Old behaviour: TopicOffsets failed after 5 s, PeekDLQ after its 10 s
// budget, and a FindByKey miss waited out its 15 s scan deadline.
func TestKFakeAbortedBelowOpenTransaction(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	pc := newKFProducer(t, addrs, "kf-aborted")
	pa := newKFProducer(t, addrs, "kf-open")
	ts := time.Now().Add(-time.Minute)

	pc.begin(ts, "aborted", "c0") // offset 0
	pa.begin(ts, "open", "a0")    // offset 1, left open
	pc.end(kgo.TryAbort)          // abort marker at 2; LSO = 1
	defer pa.end(kgo.TryAbort)

	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if len(offs) != 1 || offs[0].End != 3 || !offs[0].LastTimestamp.IsZero() {
			t.Fatalf("offsets = %+v, want End 3 and no readable record time", offs)
		}
	})
	within(t, func(ctx context.Context) {
		got, err := c.PeekDLQ(ctx, kfTopic, 5)
		if err != nil || len(got) != 0 {
			t.Fatalf("PeekDLQ = %+v, %v; want no records and no error", got, err)
		}
	})
	within(t, func(ctx context.Context) {
		refs, err := c.FindByKey(ctx, kfTopic, "c0", ts.Add(-time.Minute), 100)
		if err != nil || len(refs) != 0 {
			t.Fatalf("FindByKey = %+v, %v; want no match and no error", refs, err)
		}
	})
}
