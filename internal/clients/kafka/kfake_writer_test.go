package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// These tests run the Writer against kfake, franz-go's in-process broker,
// over the real wire protocol (OffsetFetch/Commit, DescribeGroups,
// ConsumerGroupDescribe, ListOffsets, Produce, read_committed Fetch). The
// fake-seam tests in writer_test.go check the logic; these check that it
// matches what a broker sends. The dev-stack versions are in
// writer_integration_test.go.

// kfWriterCluster starts a one-broker kfake cluster with topics and returns
// a raw client (manual partitioner) for setup and a Writer under test.
func kfWriterCluster(t *testing.T, partitions int32, topics ...string) (addrs []string, raw *kgo.Client, w *Writer) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topics...))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	addrs = c.ListenAddrs()
	return addrs, rawClient(t, addrs), writerOn(t, addrs)
}

// The helpers below are shared with writer_integration_test.go.

func rawClient(t *testing.T, addrs []string) *kgo.Client {
	t.Helper()
	raw, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	t.Cleanup(raw.Close)
	return raw
}

func writerOn(t *testing.T, addrs []string) *Writer {
	t.Helper()
	w, err := NewWriter(addrs)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	t.Cleanup(w.Close)
	return w
}

// unscopedCtx is a 10 s context with the explicit scope opt-out
// (tools.WithUnscoped), as the CLI uses: the Writer's writes refuse a ctx
// with no scope, and these tests have no room pipeline.
func unscopedCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(tools.WithUnscoped(context.Background()), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// produceAt writes one record per timestamp to partition p of topic.
func produceAt(t *testing.T, raw *kgo.Client, topic string, p int32, tss ...time.Time) {
	t.Helper()
	for _, ts := range tss {
		r := &kgo.Record{Topic: topic, Partition: p, Value: []byte("v"), Timestamp: ts}
		if err := raw.ProduceSync(unscopedCtx(t), r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

func commitAt(t *testing.T, raw *kgo.Client, group, topic string, offs map[int32]int64) {
	t.Helper()
	os := kadm.Offsets{}
	for p, o := range offs {
		os.AddOffset(topic, p, o, -1)
	}
	resp, err := kadm.NewClient(raw).CommitOffsets(unscopedCtx(t), group, os)
	if err != nil || resp.Error() != nil {
		t.Fatalf("commit: %v %v", err, resp.Error())
	}
}

func committedNow(t *testing.T, raw *kgo.Client, group, topic string) map[int32]int64 {
	t.Helper()
	resp, err := kadm.NewClient(raw).FetchOffsets(unscopedCtx(t), group)
	if err != nil || resp.Error() != nil {
		t.Fatalf("fetch offsets: %v %v", err, resp.Error())
	}
	out := map[int32]int64{}
	for p, r := range resp[topic] {
		out[p] = r.At
	}
	return out
}

func TestKFakeWriterResetToEarliestAndTimestamp(t *testing.T) {
	const topic, group = "kf-reset", "kf-reset-group"
	_, raw, w := kfWriterCluster(t, 2, topic)

	old := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	recent := time.Now().Add(-1 * time.Hour).Truncate(time.Millisecond)
	produceAt(t, raw, topic, 0, old, old, old, recent, recent) // p0: 0..4
	produceAt(t, raw, topic, 1, old, old)                      // p1: 0..1, nothing after recent
	commitAt(t, raw, group, topic, map[int32]int64{0: 5, 1: 2})

	prev, err := w.PreviousOffsets(unscopedCtx(t), group)
	if err != nil || len(prev) != 2 || prev[0] != (CommittedOffset{topic, 0, 5}) || prev[1] != (CommittedOffset{topic, 1, 2}) {
		t.Fatalf("previous offsets = %+v, %v", prev, err)
	}

	moved, err := w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Earliest: true})
	if err != nil || moved != 2 {
		t.Fatalf("earliest: moved=%d err=%v", moved, err)
	}
	if got := committedNow(t, raw, group, topic); got[0] != 0 || got[1] != 0 {
		t.Fatalf("after earliest: %v, want 0 and 0", got)
	}

	at := recent.Add(-30 * time.Minute)
	plan, err := w.PlanGroupReset(unscopedCtx(t), group, clients.OffsetTarget{Timestamp: at})
	if err != nil || len(plan) != 2 || plan[0].Target != 3 || plan[0].End != 5 || plan[1].Target != 2 || plan[1].Committed != 0 {
		t.Fatalf("plan = %+v, %v; want p0 -> 3 (first recent record), p1 -> 2 (its end)", plan, err)
	}
	moved, err = w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Timestamp: at})
	if err != nil || moved != 2 {
		t.Fatalf("timestamp: moved=%d err=%v", moved, err)
	}
	if got := committedNow(t, raw, group, topic); got[0] != 3 || got[1] != 2 {
		t.Fatalf("after timestamp: %v, want 3 and 2", got)
	}
	if moved, err = w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Timestamp: at}); err != nil || moved != 0 {
		t.Fatalf("again: moved=%d err=%v, want nothing to move", moved, err)
	}
	if _, err := w.ResetGroupOffsets(unscopedCtx(t), "kf-no-such-group", clients.OffsetTarget{Earliest: true}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown group: %v", err)
	}
}

// joinGroup starts a consumer group member on topic and waits until it has
// its partitions. leave stops it and leaves the group; it runs at cleanup
// too and may be called more than once.
func joinGroup(t *testing.T, addrs []string, group, topic string, extra ...kgo.Opt) (leave func()) {
	t.Helper()
	assigned := make(chan struct{}, 1)
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(addrs...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
			select {
			case assigned <- struct{}{}:
			default:
			}
		}),
	}, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() { // a KIP-848 member makes progress only while it polls
		defer close(stopped)
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			cl.PollFetches(ctx)
			cancel()
		}
	}()
	var once sync.Once
	leave = func() {
		once.Do(func() {
			close(stop)
			<-stopped
			cl.Close()
		})
	}
	t.Cleanup(leave)
	select {
	case <-assigned:
	case <-time.After(10 * time.Second):
		t.Fatal("member got no partitions in 10 s")
	}
	return leave
}

// TestKFakeWriterRefusesActiveGroups: a classic member and a KIP-848 member
// both block a reset. kfake answers the classic describe of a KIP-848 group
// with its state ("Stable", no classic member) where Kafka 4 answers
// GROUP_ID_NOT_FOUND; TestCheckGroupInactive covers that second path, the
// dev-stack test the real answer.
func TestKFakeWriterRefusesActiveGroups(t *testing.T) {
	const topic = "kf-active"
	addrs, raw, w := kfWriterCluster(t, 1, topic)
	for _, tc := range []struct {
		name  string
		extra []kgo.Opt
	}{
		{"classic", nil},
		{"kip-848", []kgo.Opt{kgo.ServerSideBalancer(), kgo.Balancers(kgo.RangeBalancer())}},
	} {
		group := "kf-active-" + tc.name
		commitAt(t, raw, group, topic, map[int32]int64{0: 0})
		joinGroup(t, addrs, group, topic, tc.extra...)
		moved, err := w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Earliest: true})
		if !errors.Is(err, clients.ErrGroupActive) || moved != 0 {
			t.Fatalf("%s: moved=%d err=%v, want ErrGroupActive", tc.name, moved, err)
		}
		if err := w.CheckGroupInactive(unscopedCtx(t), group); !errors.Is(err, clients.ErrGroupActive) {
			t.Fatalf("%s: check: %v", tc.name, err)
		}
	}
}

func TestKFakeWriterProduceAndReadBack(t *testing.T) {
	const topic = "kf-replay"
	_, _, w := kfWriterCluster(t, 3, topic)
	in := []clients.Record{
		{Key: []byte("k1"), Value: []byte("v1"), Headers: []clients.RecordHeader{{Key: "trace", Value: []byte("a")}}},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k1"), Value: []byte("v3")},
		{Key: []byte("k1"), Value: []byte("v4"), Timestamp: time.Unix(1, 0)},
	}
	acked, err := w.Produce(unscopedCtx(t), topic, in)
	if err != nil || acked != len(in) {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	if _, err := w.Produce(unscopedCtx(t), "kf-no-such-topic", in); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v", err)
	}

	byKey := map[string][]clients.Record{}
	for p := int32(0); p < 3; p++ {
		recs, err := w.ReadRecords(unscopedCtx(t), topic, p, 0, 100)
		if err != nil {
			t.Fatalf("read p%d: %v", p, err)
		}
		for _, r := range recs {
			byKey[string(r.Key)] = append(byKey[string(r.Key)], r)
		}
	}
	k1 := byKey["k1"]
	if len(k1) != 3 || len(byKey["k2"]) != 1 {
		t.Fatalf("read back %d k1 and %d k2 records, want 3 and 1", len(k1), len(byKey["k2"]))
	}
	for i, want := range []string{"v1", "v3", "v4"} {
		if string(k1[i].Value) != want || k1[i].Partition != k1[0].Partition {
			t.Fatalf("k1 record %d = %q on p%d: same key must keep its order on one partition", i, k1[i].Value, k1[i].Partition)
		}
	}
	if len(k1[0].Headers) != 1 || k1[0].Headers[0].Key != "trace" || string(k1[0].Headers[0].Value) != "a" {
		t.Fatalf("headers = %+v", k1[0].Headers)
	}
	if time.Since(k1[2].Timestamp) > time.Minute {
		t.Fatalf("record time %v: Produce must stamp the produce time, not the record's", k1[2].Timestamp)
	}

	from := k1[1].Offset
	recs, err := w.ReadRecords(unscopedCtx(t), topic, k1[0].Partition, from, 1)
	if err != nil || len(recs) != 1 || recs[0].Offset != from {
		t.Fatalf("read one from %d: %+v %v", from, recs, err)
	}
}
