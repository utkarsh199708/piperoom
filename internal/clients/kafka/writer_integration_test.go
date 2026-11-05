//go:build integration

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/testenv"
)

// Writer against the dev stack's Kafka 4.1: group offset resets (classic and
// KIP-848 members), produce and whole-record reads. Every topic and group is
// it_-named and deleted afterwards. There is no room pipeline here, so every
// call runs with the explicit opt-out (unscopedCtx: tools.WithUnscoped), as
// the CLI does; the Writer's writes refuse a ctx with no scope.

// itTopic creates an it_ topic with partitions and waits until the Writer's
// metadata sees it (kadm caches a "missing" answer for a few seconds).
func itTopic(t *testing.T, raw *kgo.Client, w *Writer, prefix string, partitions int32) string {
	t.Helper()
	adm := kadm.NewClient(raw)
	topic := testenv.Name(prefix)
	resps, err := adm.CreateTopics(context.Background(), partitions, 1, nil, topic)
	if err != nil || resps.Error() != nil {
		t.Fatalf("CreateTopics: %v %v", err, resps.Error())
	}
	t.Cleanup(func() {
		if _, err := adm.DeleteTopics(context.Background(), topic); err != nil {
			t.Logf("DeleteTopics: %v", err)
		}
	})
	testenv.WaitFor(t, 15*time.Second, func() (bool, error) {
		err := w.topicExists(context.Background(), topic)
		return err == nil, nil
	})
	return topic
}

// itGroup returns an it_ group name that is deleted afterwards.
func itGroup(t *testing.T, raw *kgo.Client, prefix string) string {
	t.Helper()
	group := testenv.Name(prefix)
	t.Cleanup(func() {
		if _, err := kadm.NewClient(raw).DeleteGroups(context.Background(), group); err != nil {
			t.Logf("DeleteGroups: %v", err)
		}
	})
	return group
}

func TestWriterResetGroupOffsetsAgainstDevStack(t *testing.T) {
	brokers := requireKafka(t)
	raw, w := rawClient(t, brokers), writerOn(t, brokers)
	topic := itTopic(t, raw, w, "it-reset", 2)
	group := itGroup(t, raw, "it-reset")

	old := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	recent := time.Now().Add(-1 * time.Hour).Truncate(time.Millisecond)
	produceAt(t, raw, topic, 0, old, old, old, recent, recent) // p0: 0..4
	produceAt(t, raw, topic, 1, old, old)                      // p1: nothing after recent
	commitAt(t, raw, group, topic, map[int32]int64{0: 5, 1: 2})

	prev, err := w.PreviousOffsets(unscopedCtx(t), group)
	if err != nil || len(prev) != 2 || prev[0].Offset != 5 || prev[1].Offset != 2 {
		t.Fatalf("previous offsets = %+v, %v", prev, err)
	}
	if err := w.CheckGroupInactive(unscopedCtx(t), group); err != nil {
		t.Fatalf("a group with only commits is inactive: %v", err)
	}

	moved, err := w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Earliest: true})
	if err != nil || moved != 2 {
		t.Fatalf("earliest: moved=%d err=%v", moved, err)
	}
	if got := committedNow(t, raw, group, topic); got[0] != 0 || got[1] != 0 {
		t.Fatalf("after earliest: %v", got)
	}

	at := recent.Add(-30 * time.Minute)
	moved, err = w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Timestamp: at})
	if err != nil || moved != 2 {
		t.Fatalf("timestamp: moved=%d err=%v", moved, err)
	}
	if got := committedNow(t, raw, group, topic); got[0] != 3 || got[1] != 2 {
		t.Fatalf("after timestamp: %v, want p0=3 (first recent record), p1=2 (end)", got)
	}

	if _, err := w.ResetGroupOffsets(unscopedCtx(t), testenv.Name("it-nogroup"), clients.OffsetTarget{Earliest: true}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown group: %v", err)
	}
}

func TestWriterRefusesActiveGroupAgainstDevStack(t *testing.T) {
	brokers := requireKafka(t)
	raw, w := rawClient(t, brokers), writerOn(t, brokers)
	topic := itTopic(t, raw, w, "it-active", 1)
	produceAt(t, raw, topic, 0, time.Now(), time.Now())

	for _, tc := range []struct {
		name  string
		extra []kgo.Opt
	}{
		{"classic", nil},
		{"kip848", []kgo.Opt{kgo.ServerSideBalancer(), kgo.Balancers(kgo.RangeBalancer())}},
	} {
		group := itGroup(t, raw, "it-active-"+tc.name)
		commitAt(t, raw, group, topic, map[int32]int64{0: 2})
		leave := joinGroup(t, brokers, group, topic, tc.extra...)
		moved, err := w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Earliest: true})
		if !errors.Is(err, clients.ErrGroupActive) || moved != 0 {
			t.Fatalf("%s: moved=%d err=%v, want ErrGroupActive", tc.name, moved, err)
		}
		if got := committedNow(t, raw, group, topic); got[0] != 2 {
			t.Fatalf("%s: a refused reset changed the offset: %v", tc.name, got)
		}

		leave()
		testenv.WaitFor(t, 30*time.Second, func() (bool, error) {
			return w.CheckGroupInactive(context.Background(), group) == nil, nil
		})
		if moved, err := w.ResetGroupOffsets(unscopedCtx(t), group, clients.OffsetTarget{Earliest: true}); err != nil || moved != 1 {
			t.Fatalf("%s after leave: moved=%d err=%v", tc.name, moved, err)
		}
	}
}

// TestWriterReplayAgainstDevStack copies DLQ-style records to another topic
// the way dlq_replay will: ReadRecords then Produce, in order.
func TestWriterReplayAgainstDevStack(t *testing.T) {
	brokers := requireKafka(t)
	raw, w := rawClient(t, brokers), writerOn(t, brokers)
	src := itTopic(t, raw, w, "it-replay-dlq", 1)
	dst := itTopic(t, raw, w, "it-replay-to", 3)

	var in []*kgo.Record
	for i, k := range []string{"a", "b", "a", "c", "a"} {
		in = append(in, &kgo.Record{Topic: src, Partition: 0, Key: []byte(k), Value: []byte{byte('0' + i)},
			Headers: []kgo.RecordHeader{{Key: "__connect.errors.topic", Value: []byte("orders")}, {Key: "app", Value: []byte("x")}}})
	}
	if err := raw.ProduceSync(unscopedCtx(t), in...).FirstErr(); err != nil {
		t.Fatalf("seed dlq: %v", err)
	}

	recs, err := w.ReadRecords(unscopedCtx(t), src, 0, 1, 10)
	if err != nil || len(recs) != 4 || recs[0].Offset != 1 || string(recs[0].Key) != "b" || len(recs[0].Headers) != 2 {
		t.Fatalf("read = %+v, %v; want offsets 1..4 with keys and headers", recs, err)
	}
	if _, err := w.ReadRecords(unscopedCtx(t), src, 0, 5, 10); err != nil {
		t.Fatalf("read at end: %v", err)
	}

	acked, err := w.Produce(unscopedCtx(t), dst, recs)
	if err != nil || acked != 4 {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	ends, err := kadm.NewClient(raw).ListEndOffsets(unscopedCtx(t), dst)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	var aPart int32 = -1
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	if total != 4 {
		t.Fatalf("end offsets of %s sum to %d, want 4", dst, total)
	}
	var aValues []string
	for p := int32(0); p < 3; p++ {
		got, err := w.ReadRecords(unscopedCtx(t), dst, p, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got {
			if string(r.Key) == "a" {
				aPart = p
				aValues = append(aValues, string(r.Value))
			}
		}
	}
	if len(aValues) != 2 || aValues[0] != "2" || aValues[1] != "4" || aPart < 0 {
		t.Fatalf("key a replayed as %v on p%d, want [2 4] in order on one partition", aValues, aPart)
	}

	if _, err := w.Produce(unscopedCtx(t), testenv.Name("it-missing"), recs); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v", err)
	}
}
