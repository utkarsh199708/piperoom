package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/config"
)

// These run the Kafka helpers against kfake, franz-go's in-process broker.

func kfakeStack(t *testing.T, partitions int32, topics ...string) (*stack, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topics...))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	raw, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	return &stack{cfg: config.Config{KafkaBrokers: strings.Join(c.ListenAddrs(), ",")}}, raw
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestTopicSpanAndEnsureDelete(t *testing.T) {
	s, _ := kfakeStack(t, 1, "seed")
	ctx := testCtx(t)
	if span, ok, err := s.topicSpan(ctx, "it_missing"); err != nil || ok || span.records() != 0 {
		t.Fatalf("missing topic = %v %v %v, want not found and no error", span, ok, err)
	}
	for i := 0; i < 2; i++ { // the second create is a no-op
		if err := s.ensureTopic(ctx, "it_dlq", 2); err != nil {
			t.Fatalf("ensureTopic run %d: %v", i+1, err)
		}
	}
	span, ok, err := s.topicSpan(ctx, "it_dlq")
	if err != nil || !ok || len(span) != 2 || span.records() != 0 {
		t.Fatalf("new topic span = %v %v %v", span, ok, err)
	}
	for i := 0; i < 2; i++ { // deleting a deleted topic is not an error
		if err := s.deleteTopics(ctx, "it_dlq", "it_never_existed"); err != nil {
			t.Fatalf("deleteTopics run %d: %v", i+1, err)
		}
	}
	if _, ok, err := s.topicSpan(ctx, "it_dlq"); err != nil || ok {
		t.Fatalf("deleted topic still listed: %v %v", ok, err)
	}
}

func TestReadTopicOrderAndProduce(t *testing.T) {
	s, raw := kfakeStack(t, 3, "it_src")
	ctx := testCtx(t)
	// Partition 2 before 0, several per partition: readTopic returns
	// partitions ascending, offsets ascending inside each.
	var recs []*kgo.Record
	for _, p := range []int32{2, 0, 2, 0, 1} {
		recs = append(recs, &kgo.Record{Topic: "it_src", Partition: p, Value: []byte(fmt.Sprintf("p%d-%d", p, len(recs)))})
	}
	if err := raw.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	got, err := s.readTopic(ctx, "it_src")
	if err != nil {
		t.Fatal(err)
	}
	var vals []string
	for _, r := range got {
		vals = append(vals, string(r.Value))
	}
	if want := "p0-1 p0-3 p1-4 p2-0 p2-2"; strings.Join(vals, " ") != want {
		t.Errorf("readTopic order = %v, want %s", vals, want)
	}
	if none, err := s.readTopic(ctx, "it_missing"); err != nil || len(none) != 0 {
		t.Errorf("missing topic read = %v, %v", none, err)
	}
	// produce keeps the order and lands keyed records on one partition per key.
	out := []*kgo.Record{
		{Topic: "it_src", Key: []byte("a"), Value: []byte("1")},
		{Topic: "it_src", Key: []byte("a"), Value: []byte("2")},
	}
	if err := s.produce(ctx, out); err != nil {
		t.Fatal(err)
	}
	after, err := s.readTopic(ctx, "it_src")
	if err != nil {
		t.Fatal(err)
	}
	var keyed []*kgo.Record
	for _, r := range after {
		if string(r.Key) == "a" {
			keyed = append(keyed, r)
		}
	}
	if len(keyed) != 2 || keyed[0].Partition != keyed[1].Partition || string(keyed[0].Value) != "1" || string(keyed[1].Value) != "2" {
		t.Errorf("keyed produce = %v", keyed)
	}
	if err := s.produce(ctx, nil); err != nil {
		t.Errorf("produce of nothing = %v", err)
	}
}

func TestResetGroupToStart(t *testing.T) {
	s, raw := kfakeStack(t, 2, "it_orders")
	ctx := testCtx(t)
	for _, p := range []int32{0, 1, 1} {
		if err := raw.ProduceSync(ctx, &kgo.Record{Topic: "it_orders", Partition: p, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adm := kadm.NewClient(raw)
	// The group has read everything (offsets at the end) and left.
	ends, err := adm.ListEndOffsets(ctx, "it_orders")
	if err != nil {
		t.Fatal(err)
	}
	if err := adm.CommitAllOffsets(ctx, "it_group", ends.Offsets()); err != nil {
		t.Fatal(err)
	}
	if err := s.resetGroupToStart(ctx, "it_group", "it_orders"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	fetched, err := adm.FetchOffsets(ctx, "it_group")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []int32{0, 1} {
		o, ok := fetched.Lookup("it_orders", p)
		if !ok || o.At != 0 {
			t.Errorf("partition %d committed = %+v %v, want 0", p, o, ok)
		}
	}

	// A group with a live member is refused.
	member, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(s.cfg.KafkaBrokers, ",")...),
		kgo.ConsumerGroup("it_live"), kgo.ConsumeTopics("it_orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		pctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		member.PollFetches(pctx)
		cancel()
		g, err := adm.DescribeGroups(ctx, "it_live")
		if err == nil && len(g["it_live"].Members) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("member did not join")
		}
	}
	err = s.resetGroupToStart(ctx, "it_live", "it_orders")
	if !errors.Is(err, errGroupActive) {
		t.Fatalf("reset of an active group = %v, want errGroupActive", err)
	}
}
