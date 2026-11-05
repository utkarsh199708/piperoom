//go:build integration

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/testenv"
)

// The Kafka helpers the P3 scenarios use, against the dev stack's broker, on
// it_* topics and groups that each test removes. The scenarios themselves
// mutate the shared pipeline; run them with `devctl break|fix` under the stack
// lock (dev/scenarios.md), not from tests.

func devStack(t *testing.T) *stack {
	t.Helper()
	cfg := testenv.Require(t)
	return &stack{cfg: cfg, http: &http.Client{Timeout: 10 * time.Second}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestIntegrationDLQTopicRoundTrip(t *testing.T) {
	s := devStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dlq, src := testenv.Name("dlq"), testenv.Name("src")
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		if err := s.deleteTopics(cctx, dlq, src); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	if err := s.ensureTopic(ctx, dlq, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureTopic(ctx, dlq, 1); err != nil {
		t.Fatalf("second ensureTopic: %v", err)
	}
	if err := s.ensureTopic(ctx, src, 3); err != nil {
		t.Fatal(err)
	}
	in := []*kgo.Record{
		{Topic: dlq, Key: []byte("a"), Value: []byte("1"), Headers: []kgo.RecordHeader{{Key: "__connect.errors.offset", Value: []byte("4")}, {Key: "keep", Value: []byte("y")}}},
		{Topic: dlq, Key: []byte("a"), Value: []byte("2")},
	}
	if err := s.produce(ctx, in); err != nil {
		t.Fatal(err)
	}
	recs, err := s.readTopic(ctx, dlq)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || string(recs[0].Value) != "1" || string(recs[1].Value) != "2" {
		t.Fatalf("read %d records: %v", len(recs), recs)
	}
	if err := s.produce(ctx, replayRecords(recs, src)); err != nil {
		t.Fatal(err)
	}
	out, err := s.readTopic(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].Partition != out[1].Partition || string(out[0].Value) != "1" {
		t.Fatalf("replayed = %v", out)
	}
	if len(out[0].Headers) != 1 || out[0].Headers[0].Key != "keep" {
		t.Errorf("replayed headers = %v", out[0].Headers)
	}
	if err := s.deleteTopics(ctx, dlq); err != nil {
		t.Fatal(err)
	}
	testenv.WaitFor(t, 20*time.Second, func() (bool, error) {
		_, ok, err := s.topicSpan(ctx, dlq)
		return !ok, err
	})
}

func TestIntegrationResetGroupToStart(t *testing.T) {
	s := devStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	topic, group := testenv.Name("orders"), testenv.Name("group")
	cl, err := s.kafkaClient()
	if err != nil {
		t.Fatal(err)
	}
	// t.Cleanup, not defer: the cleanup below still uses adm after the test body
	// returns, and cleanups run last-registered first.
	t.Cleanup(cl.Close)
	adm := kadm.NewClient(cl)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		if _, err := adm.DeleteGroups(cctx, group); err != nil {
			t.Errorf("cleanup group: %v", err)
		}
		if err := s.deleteTopics(cctx, topic); err != nil {
			t.Errorf("cleanup topic: %v", err)
		}
	})
	if err := s.ensureTopic(ctx, topic, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.produce(ctx, []*kgo.Record{{Topic: topic, Key: []byte("a"), Value: []byte("1")}, {Topic: topic, Key: []byte("b"), Value: []byte("2")}}); err != nil {
		t.Fatal(err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err := adm.CommitAllOffsets(ctx, group, ends.Offsets()); err != nil {
		t.Fatal(err)
	}
	if err := s.resetGroupToStart(ctx, group, topic); err != nil {
		t.Fatal(err)
	}
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []int32{0, 1} {
		if o, ok := fetched.Lookup(topic, p); !ok || o.At != 0 {
			t.Errorf("partition %d committed %+v %v, want 0", p, o, ok)
		}
	}
	// A group with a live member is refused.
	live := testenv.Name("live")
	member, err := s.kafkaClient(kgo.ConsumerGroup(live), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		member.Close()
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		_, _ = adm.DeleteGroups(cctx, live)
	})
	testenv.WaitFor(t, 30*time.Second, func() (bool, error) {
		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		member.PollFetches(pctx)
		pcancel()
		g, err := adm.DescribeGroups(ctx, live)
		return err == nil && len(g[live].Members) > 0, nil
	})
	if err := s.resetGroupToStart(ctx, live, topic); !errors.Is(err, errGroupActive) {
		t.Fatalf("reset of an active group = %v, want errGroupActive", err)
	}
}
