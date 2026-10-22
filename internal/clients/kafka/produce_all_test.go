package kafka

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func fourRecords() []clients.Record {
	return []clients.Record{
		{Key: []byte("k0"), Value: []byte("v0"), Headers: []clients.RecordHeader{{Key: "h", Value: []byte("x")}}},
		{Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
}

// ProduceAll sends every record before the first ack and counts each ack:
// one failure leaves the others acked, and acked is exact.
func TestProduceAllCountsEveryAck(t *testing.T) {
	var mu sync.Mutex
	var promises []func()
	var got []*kgo.Record
	w := produceWriter(nil, topicHere("t"))
	w.produceFn = func(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, r)
		var err error
		if len(got) == 3 {
			err = kerr.NotEnoughReplicas
		}
		promises = append(promises, func() { promise(r, err) })
	}
	done := make(chan struct{})
	var acked int
	var err error
	go func() {
		defer close(done)
		acked, err = w.ProduceAll(unscoped(), "t", fourRecords())
	}()
	// Answer only once all four are in flight: ProduceAll does not wait
	// for one ack before sending the next record.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(promises)
		mu.Unlock()
		if n == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d records in flight", n)
		}
		time.Sleep(time.Millisecond)
	}
	for _, p := range promises {
		go p() // kgo answers from its own goroutine
	}
	<-done
	if acked != 3 || !errors.Is(err, kerr.NotEnoughReplicas) || !strings.Contains(err.Error(), "3 of 4 records acked") {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	for i, r := range got {
		if r.Topic != "t" || !r.Timestamp.IsZero() || !bytes.Equal(r.Value, fourRecords()[i].Value) {
			t.Fatalf("record %d = %+v: in order, topic the argument, time left to kgo", i, r)
		}
	}
	if len(got[0].Headers) != 1 || got[1].Key != nil {
		t.Fatal("headers or null key not kept")
	}

	// All acked.
	p := &produceLog{failAt: -1}
	if acked, err := produceWriter(p, topicHere("t")).ProduceAll(unscoped(), "t", fourRecords()); acked != 4 || err != nil {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
}

// ProduceAll refuses as Produce does, before producing anything.
func TestProduceAllRefusesBeforeProducing(t *testing.T) {
	p := &produceLog{failAt: -1}
	big := []clients.Record{{Value: []byte("ok")}, {Value: make([]byte, maxProduceRecordBytes+1)}}
	if acked, err := produceWriter(p, topicHere("t")).ProduceAll(unscoped(), "t", big); err == nil || acked != 0 {
		t.Fatalf("oversized: acked=%d err=%v", acked, err)
	}
	if _, err := produceWriter(p, nil).ProduceAll(unscoped(), "t", fourRecords()); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v", err)
	}
	if _, err := produceWriter(p, topicHere("t")).ProduceAll(context.Background(), "t", fourRecords()); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v", err)
	}
	if acked, err := produceWriter(p, nil).ProduceAll(unscoped(), "t", nil); acked != 0 || err != nil {
		t.Fatalf("no records: acked=%d err=%v", acked, err)
	}
	if len(p.got) != 0 {
		t.Fatalf("%d records produced by refused calls", len(p.got))
	}
}

// When ctx ends with records kgo never answers, they are not counted and
// the error says their outcome is unknown, within the grace second.
func TestProduceAllAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(unscoped(), 50*time.Millisecond)
	defer cancel()
	w := produceWriter(nil, topicHere("t"))
	n := 0
	w.produceFn = func(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
		n++
		if n == 1 {
			promise(r, nil)
		}
	}
	start := time.Now()
	acked, err := w.ProduceAll(ctx, "t", fourRecords())
	if acked != 1 || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "3 with an unknown outcome") {
		t.Fatalf("acked=%d err=%v", acked, err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("ProduceAll took %s after its deadline", d)
	}
}

// The pipeline's declared reprocess_topic is in the writers' produce scope
// (X2b); another topic is still refused before any I/O.
func TestProduceReprocessTopicScope(t *testing.T) {
	pl := scopePipeline()
	pl.ReprocessTopic = "orders-reprocess"
	ctx := tools.WithPipeline(context.Background(), pl)
	p := &produceLog{failAt: -1}
	w := produceWriter(p, topicHere("orders-reprocess"))
	if acked, err := w.Produce(ctx, "orders-reprocess", fourRecords()[:1]); acked != 1 || err != nil {
		t.Fatalf("Produce to the reprocess topic: acked=%d err=%v", acked, err)
	}
	if acked, err := w.ProduceAll(ctx, "orders-reprocess", fourRecords()); acked != 4 || err != nil {
		t.Fatalf("ProduceAll to the reprocess topic: acked=%d err=%v", acked, err)
	}
	before := len(p.got)
	for _, topic := range []string{"other-reprocess", "pay.public.payments"} {
		if _, err := w.ProduceAll(ctx, topic, fourRecords()); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("%s: %v", topic, err)
		}
	}
	// Without a declared reprocess_topic nothing but CDC topics passes.
	if _, err := w.Produce(tools.WithPipeline(context.Background(), scopePipeline()), "orders-reprocess", fourRecords()); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("undeclared: %v", err)
	}
	if len(p.got) != before {
		t.Fatal("a refused call produced")
	}
}
