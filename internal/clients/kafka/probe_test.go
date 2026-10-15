package kafka

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// plainProduce writes non-transactional records (committed at once) to
// kfTopic with the given timestamp.
func plainProduce(t *testing.T, addrs []string, ts time.Time, keys ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, k := range keys {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: kfTopic, Key: []byte(k), Value: []byte("v"), Timestamp: ts}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

// beginLarge starts a transaction and writes n records of size bytes of
// random (incompressible) value, each in its own batch.
func (p *kfProducer) beginLarge(ts time.Time, key string, n, size int) {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.cl.BeginTransaction(); err != nil {
		p.t.Fatalf("begin: %v", err)
	}
	for i := 0; i < n; i++ {
		v := make([]byte, size)
		_, _ = rand.Read(v)
		r := &kgo.Record{Topic: kfTopic, Key: []byte(key), Value: v, Timestamp: ts}
		if err := p.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			p.t.Fatalf("produce: %v", err)
		}
	}
}

// TestKFakeProbeRound: one probe round leaves a window open when it holds a
// committed record or a marker, and closes one that holds only aborted
// records. A probe that closed a window with a readable record would turn
// into a false "not found" or dropped DLQ records.
func TestKFakeProbeRound(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	pt := newKFProducer(t, addrs, "kf-probe-commit")
	pc := newKFProducer(t, addrs, "kf-probe-abort")
	pa := newKFProducer(t, addrs, "kf-probe-open")
	ts := time.Now().Add(-time.Minute)

	pt.begin(ts, "committed", "k0") // 0
	pt.end(kgo.TryCommit)           // commit marker 1
	pc.begin(ts, "aborted", "c0")   // 2
	pa.begin(ts, "open", "a0")      // 3, left open
	pc.end(kgo.TryAbort)            // abort marker 4; LSO = 3
	defer pa.end(kgo.TryAbort)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name     string
		w        readWindow
		wantNext int64 // -1: no entry (the window stays open)
	}{
		{"committed record", readWindow{0, 1}, -1},
		{"commit marker", readWindow{1, 2}, -1},
		{"aborted only", readWindow{2, 3}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := c.newProber(kfTopic).round(ctx, map[int32]readWindow{0: tc.w})[0]
			if tc.wantNext < 0 && ok {
				t.Fatalf("window %+v closed at %d, want it left open", tc.w, got)
			}
			if tc.wantNext >= 0 && (!ok || got != tc.wantNext) {
				t.Fatalf("window %+v: next = %d (%v), want %d", tc.w, got, ok, tc.wantNext)
			}
		})
	}
}

// TestKFakeMixedTailBelowOpenTransaction: a committed record and its marker,
// then an aborted record below an open transaction. PeekDLQ returns the
// committed record, FindByKey finds it, and TopicOffsets reports the marker's
// time (the newest readable record), not zero.
func TestKFakeMixedTailBelowOpenTransaction(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	pt := newKFProducer(t, addrs, "kf-mixed-commit")
	pc := newKFProducer(t, addrs, "kf-mixed-abort")
	pa := newKFProducer(t, addrs, "kf-mixed-open")
	ts := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	pt.begin(ts, "committed", "b0") // 0
	beforeCommit := time.Now().Truncate(time.Millisecond)
	pt.end(kgo.TryCommit) // marker 1
	afterCommit := time.Now()
	pc.begin(ts, "aborted", "c0") // 2
	pa.begin(ts, "open", "a0")    // 3, left open
	pc.end(kgo.TryAbort)          // marker 4; LSO = 3
	defer pa.end(kgo.TryAbort)

	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if lt := offs[0].LastTimestamp; lt.Before(beforeCommit) || lt.After(afterCommit) {
			t.Fatalf("LastTimestamp = %s, want the commit marker's time in [%s, %s]", lt, beforeCommit, afterCommit)
		}
	})
	within(t, func(ctx context.Context) {
		got, err := c.PeekDLQ(ctx, kfTopic, 5)
		if err != nil || len(got) != 1 || got[0].Offset != 0 {
			t.Fatalf("PeekDLQ = %+v, %v; want the committed record at 0", got, err)
		}
	})
	within(t, func(ctx context.Context) {
		refs, err := c.FindByKey(ctx, kfTopic, "b0", ts.Add(-time.Minute), 100)
		if err != nil || len(refs) != 1 || refs[0].Offset != 0 {
			t.Fatalf("FindByKey = %+v, %v; want the committed record at 0", refs, err)
		}
	})
}

// TestKFakeCommittedBelowAbortedTail: committed records, then an aborted
// record at LSO-1 below an open transaction. LastTimestamp is the newest
// committed record's time; zero would read as "never" in topology health.
func TestKFakeCommittedBelowAbortedTail(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	pc := newKFProducer(t, addrs, "kf-below-abort")
	pa := newKFProducer(t, addrs, "kf-below-open")
	committedTS := time.Now().Add(-20 * time.Second).Truncate(time.Millisecond)
	ts := time.Now().Add(-time.Minute)

	plainProduce(t, addrs, committedTS, "k0", "k1") // 0, 1
	pc.begin(ts, "aborted", "c0")                   // 2
	pa.begin(ts, "open", "a0")                      // 3, left open
	pc.end(kgo.TryAbort)                            // marker 4; LSO = 3
	defer pa.end(kgo.TryAbort)

	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if !offs[0].LastTimestamp.Equal(committedTS) {
			t.Fatalf("LastTimestamp = %s, want %s (k1, the newest readable record)", offs[0].LastTimestamp, committedTS)
		}
	})
}

// TestKFakeLargeAbortedTail: an aborted tail of many batches, far over one
// probe response, below an open transaction. Every read still returns at
// once: the probe keeps going while it makes progress.
func TestKFakeLargeAbortedTail(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	pc := newKFProducer(t, addrs, "kf-large-abort")
	pa := newKFProducer(t, addrs, "kf-large-open")
	committedTS := time.Now().Add(-20 * time.Second).Truncate(time.Millisecond)
	ts := time.Now().Add(-time.Minute)

	plainProduce(t, addrs, committedTS, "k0") // 0
	pc.beginLarge(ts, "c0", 1500, 4<<10)      // 1..1500, about 6 MiB
	pa.begin(ts, "open", "a0")                // 1501, left open
	pc.end(kgo.TryAbort)                      // LSO = 1501
	defer pa.end(kgo.TryAbort)

	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if !offs[0].LastTimestamp.Equal(committedTS) {
			t.Fatalf("LastTimestamp = %s, want %s", offs[0].LastTimestamp, committedTS)
		}
	})
	within(t, func(ctx context.Context) {
		got, err := c.PeekDLQ(ctx, kfTopic, 20)
		if err != nil || len(got) != 0 {
			t.Fatalf("PeekDLQ = %d records, %v; want none and no error", len(got), err)
		}
	})
	within(t, func(ctx context.Context) {
		dl, _ := ctx.Deadline()
		rctx, rec := recordCtx()
		rctx, cancel := context.WithDeadline(rctx, dl)
		defer cancel()
		refs, err := c.FindByKey(rctx, kfTopic, "k0", ts.Add(-time.Hour), 5000)
		if err != nil || len(refs) != 1 || refs[0].Offset != 0 {
			t.Fatalf("FindByKey = %+v, %v; want k0 at 0", refs, err)
		}
		// The scan's receipt backs the match, so it must survive the
		// provenance cap: the probes add one summary receipt after it,
		// not one per request.
		got := texts(rec.Calls())
		if n := len(got); n < 2 || got[n-2] != "kgo.PollFetches(topic, since, maxScan)" ||
			got[n-1] != "kmsg.FetchRequest(topic, read_committed) probes" || n > 5 {
			t.Fatalf("receipts = %v", got)
		}
	})
}

// firstPollBlocks is a consumer whose first poll returns nothing until its
// context ends, like a fresh consumer on a slow (TLS+SASL) cluster.
type firstPollBlocks struct {
	pollConsumer
	once sync.Once
}

func (f *firstPollBlocks) PollFetches(ctx context.Context) kgo.Fetches {
	first := false
	f.once.Do(func() { first = true })
	if first {
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	return f.pollConsumer.PollFetches(ctx)
}

// TestKFakeTopicOffsetsCommitAfterListing: the record at LSO-1 is aborted
// and the open transaction above it commits right after the offsets are
// listed. The probe then sees a readable record past its window and leaves
// it to the consumer, which delivers it as the stand-in timestamp.
func TestKFakeTopicOffsetsCommitAfterListing(t *testing.T) {
	addrs := kfCluster(t)
	c := kfClient(t, addrs)
	pc := newKFProducer(t, addrs, "kf-late-abort")
	pa := newKFProducer(t, addrs, "kf-late-open")
	ts := time.Now().Add(-time.Minute)
	openTS := time.Now().Add(-30 * time.Second).Truncate(time.Millisecond)

	pc.begin(ts, "aborted", "c0")  // 0
	pa.begin(openTS, "open", "a0") // 1
	pc.end(kgo.TryAbort)           // marker 2; LSO = 1
	c.consumerFn = func(opts []kgo.Opt) (pollConsumer, error) {
		pa.end(kgo.TryCommit) // after listing, before the first poll
		cl, err := c.newKgo(opts...)
		if err != nil {
			return nil, err
		}
		return &firstPollBlocks{pollConsumer: cl}, nil
	}
	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if !offs[0].LastTimestamp.Equal(openTS) {
			t.Fatalf("LastTimestamp = %s, want %s (a0, committed after listing)", offs[0].LastTimestamp, openTS)
		}
	})
}

// probeSeamClient is a PeekDLQ client over one partition with offsets
// [0, 5), a consumer that never delivers anything, and the given probe.
func probeSeamClient(probe func(ctx context.Context, topic string, wins map[int32]readWindow) map[int32]int64) *Client {
	return &Client{
		offs: &fakeLister{
			starts: merge(listed("d", map[int32]int64{0: 0})),
			ends:   merge(listed("d", map[int32]int64{0: 5})),
		},
		consumerFn: func([]kgo.Opt) (pollConsumer, error) { return &fakeConsumer{}, nil },
		probeFn:    probe,
	}
}

// TestProbeSeam: what a read does with each probe answer.
func TestProbeSeam(t *testing.T) {
	t.Run("probe fails: the read ends at its deadline as before", func(t *testing.T) {
		var calls int
		c := probeSeamClient(func(context.Context, string, map[int32]readWindow) map[int32]int64 {
			calls++
			return nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		defer cancel()
		_, err := c.PeekDLQ(ctx, "d", 5)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the deadline", err)
		}
		if calls == 0 {
			t.Fatal("the stalled read never probed")
		}
	})
	t.Run("next below end: the partition is not done", func(t *testing.T) {
		var froms []int64
		c := probeSeamClient(func(_ context.Context, _ string, wins map[int32]readWindow) map[int32]int64 {
			w := wins[0]
			froms = append(froms, w.from)
			if w.from == 0 {
				return map[int32]int64{0: 2}
			}
			return nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		defer cancel()
		_, err := c.PeekDLQ(ctx, "d", 5)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the deadline: [2, 5) is still unread", err)
		}
		if len(froms) < 2 || froms[0] != 0 || froms[1] != 2 {
			t.Fatalf("probed from %v, want 0, then 2 at once", froms)
		}
	})
	t.Run("an answer for an unplanned partition finishes nothing", func(t *testing.T) {
		c := probeSeamClient(func(context.Context, string, map[int32]readWindow) map[int32]int64 {
			return map[int32]int64{7: 99}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		defer cancel()
		_, err := c.PeekDLQ(ctx, "d", 5)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the deadline: partition 0 is still unread", err)
		}
	})
	t.Run("progress each round: done within one idle cycle", func(t *testing.T) {
		c := probeSeamClient(func(_ context.Context, _ string, wins map[int32]readWindow) map[int32]int64 {
			return map[int32]int64{0: wins[0].from + 1}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		start := time.Now()
		got, err := c.PeekDLQ(ctx, "d", 5)
		if err != nil || len(got) != 0 {
			t.Fatalf("PeekDLQ = %+v, %v; want none and no error", got, err)
		}
		if d := time.Since(start); d > 2*stallAfter {
			t.Fatalf("took %s: the probe did not keep going while it made progress", d)
		}
	})
}

// kfClusterWith starts a kfake cluster with kfTopic on the given number of
// brokers and partitions.
func kfClusterWith(t *testing.T, brokers int, partitions int32) (*kfake.Cluster, []string) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(brokers), kfake.SeedTopics(partitions, kfTopic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c, c.ListenAddrs()
}

// partClient is a producer that writes to the record's Partition field;
// with txnID it is transactional.
func partClient(t *testing.T, addrs []string, txnID string) *kgo.Client {
	t.Helper()
	opts := []kgo.Opt{kgo.SeedBrokers(addrs...), kgo.RecordPartitioner(kgo.ManualPartitioner())}
	if txnID != "" {
		opts = append(opts, kgo.TransactionalID(txnID))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produceTo(t *testing.T, cl *kgo.Client, p int32, key string, ts time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: kfTopic, Partition: p, Key: []byte(key), Value: []byte("v"), Timestamp: ts}).FirstErr(); err != nil {
		t.Fatalf("produce %s to %d: %v", key, p, err)
	}
}

// TestKFakeStuckPartitionBesideBusyOne (#300): partition 1 has a committed
// k1, then an aborted c0 below an open a0; partition 0 gets a committed
// record every 100 ms. The consumer is never idle, yet partition 1 is
// probed on its own and every read returns at once.
func TestKFakeStuckPartitionBesideBusyOne(t *testing.T) {
	_, addrs := kfClusterWith(t, 1, 2)
	c := kfClient(t, addrs)
	plain := partClient(t, addrs, "")
	pc := partClient(t, addrs, "kf-busy-abort")
	pa := partClient(t, addrs, "kf-busy-open")
	committedTS := time.Now().Add(-20 * time.Second).Truncate(time.Millisecond)
	ts := time.Now().Add(-time.Minute)

	produceTo(t, plain, 1, "k1", committedTS) // p1: 0
	for _, step := range []struct {
		cl  *kgo.Client
		key string
	}{{pc, "c0"}, {pa, "a0"}} { // p1: 1 aborted, 2 left open
		if err := step.cl.BeginTransaction(); err != nil {
			t.Fatalf("begin: %v", err)
		}
		produceTo(t, step.cl, 1, step.key, ts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pc.EndTransaction(ctx, kgo.TryAbort); err != nil { // p1: abort marker 3; LSO = 2
		t.Fatalf("abort: %v", err)
	}
	cancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pa.EndTransaction(ctx, kgo.TryAbort)
	})

	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = plain.ProduceSync(ctx, &kgo.Record{Topic: kfTopic, Partition: 0, Key: []byte("busy"), Value: []byte("v")}).FirstErr()
				cancel()
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-stopped })
	time.Sleep(300 * time.Millisecond) // partition 0 is live before the reads start

	within(t, func(ctx context.Context) {
		offs, err := c.TopicOffsets(ctx, kfTopic)
		if err != nil {
			t.Fatalf("TopicOffsets: %v", err)
		}
		if len(offs) != 2 || !offs[1].LastTimestamp.Equal(committedTS) {
			t.Fatalf("offsets = %+v, want p1 LastTimestamp %s", offs, committedTS)
		}
	})
	within(t, func(ctx context.Context) {
		// The newest records win the limit, and partition 0 keeps getting
		// newer ones, so k1 may be cut; what matters is that partition 1
		// finishes, which within's 2 s bound checks (a stuck partition
		// waited out the deadline and still returned a nil error).
		if _, err := c.PeekDLQ(ctx, kfTopic, 5); err != nil {
			t.Fatalf("PeekDLQ: %v", err)
		}
	})
	within(t, func(ctx context.Context) {
		rep, err := c.FindByKeyReport(ctx, kfTopic, "k1", ts.Add(-time.Hour), 100)
		if err != nil || len(rep.Refs) != 1 || rep.Refs[0].Partition != 1 || len(rep.Truncated) != 0 {
			t.Fatalf("FindByKeyReport = %+v, %v; want k1 at 1/0, nothing truncated", rep, err)
		}
	})
}

// TestKFakeProbeLeaderMove: after the partition moves, the old leader
// answers with a partition error; the prober drops its cached metadata and
// the next round asks the new leader.
func TestKFakeProbeLeaderMove(t *testing.T) {
	cluster, addrs := kfClusterWith(t, 3, 1)
	c := kfClient(t, addrs)
	pc := newKFProducer(t, addrs, "kf-move-abort")
	pa := newKFProducer(t, addrs, "kf-move-open")
	ts := time.Now().Add(-time.Minute)
	pc.begin(ts, "aborted", "c0") // 0
	pa.begin(ts, "open", "a0")    // 1, left open
	pc.end(kgo.TryAbort)          // LSO = 1
	defer pa.end(kgo.TryAbort)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	w := map[int32]readWindow{0: {0, 1}}
	pr := c.newProber(kfTopic)
	if got := pr.round(ctx, w); got[0] != 1 {
		t.Fatalf("round before the move = %v, want {0:1}", got)
	}
	leader := pr.leaders[0]
	if err := cluster.MoveTopicPartition(kfTopic, 0, (leader+1)%3); err != nil {
		t.Fatalf("move: %v", err)
	}
	var got map[int32]int64
	for i := 0; i < 3 && got[0] != 1; i++ {
		got = pr.round(ctx, w)
	}
	if got[0] != 1 {
		t.Fatalf("rounds after the move = %v, want {0:1} from the new leader", got)
	}
}

// TestTopicOffsetsUndecidedProbeIsAnError: when the record at LSO-1 holds
// nothing readable and the walk back cannot decide, TopicOffsets fails (the
// node stays Unknown) instead of reporting a zero LastTimestamp ("never").
func TestTopicOffsetsUndecidedProbeIsAnError(t *testing.T) {
	c := probeSeamClient(func(_ context.Context, _ string, wins map[int32]readWindow) map[int32]int64 {
		return map[int32]int64{0: wins[0].end} // [4, 5) holds nothing readable
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	offs, err := c.TopicOffsets(ctx, "d")
	if !errors.Is(err, errProbe) {
		t.Fatalf("err = %v, want errProbe (the zero-value client has no broker to walk back on)", err)
	}
	if offs != nil {
		t.Fatalf("offsets = %+v, want none", offs)
	}
}
