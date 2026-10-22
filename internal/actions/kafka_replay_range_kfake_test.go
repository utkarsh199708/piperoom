package actions

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients/kafka"
	"piperoom/internal/events"
)

// TestReplayRangeKfake is TC1 over the real wire protocol: 1 000 records
// over 3 partitions, offsets 100..199 of partition 1 replayed with the real
// Writer (pipelined ProduceAll) and the read-only ReplayReader, under the
// room's scope as the executor runs it; the reprocess topic then holds
// exactly those 100 records, same keys, same order, Connect headers
// dropped, and Verify finds them all.
func TestReplayRangeKfake(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, rrTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(rrCtx(), 20*time.Second)
	defer cancel()

	raw, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := kadm.NewClient(raw).CreateTopic(ctx, 1, 1, nil, rrReprocess); err != nil {
		t.Fatal(err)
	}
	recs := make([]*kgo.Record, 1000)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: rrTopic, Partition: int32(i % 3), Key: fmt.Appendf(nil, "k-%d", i), Value: fmt.Appendf(nil, "v-%d", i),
			Headers: []kgo.RecordHeader{{Key: "__connect.errors.topic", Value: []byte("x")}, {Key: "trace-id", Value: []byte("t")}}}
	}
	if err := raw.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	reader, err := kafka.New(c.ListenAddrs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	w, err := kafka.NewWriter(c.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	a := &replayRange{d: KafkaDeps{Records: kafka.NewReplayReader(reader), Writer: w}}
	params := rrParams(t, nil)

	if err := a.Validate(ctx, params); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	res, err := a.Preview(ctx, params)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if *res.Count != 100 {
		t.Fatalf("preview count %d", *res.Count)
	}
	if _, err := a.Execute(ctx, params); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	executedAt := time.Now()

	cons, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		rrReprocess: {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	var got []*kgo.Record
	for len(got) < 100 {
		fs := cons.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch after %d records: %v", len(got), errs)
		}
		got = append(got, fs.Records()...)
	}
	if len(got) != 100 {
		t.Fatalf("reprocess topic holds %d records, want 100", len(got))
	}
	for i, r := range got {
		// Partition 1 offset o holds record 3o+1.
		n := 3*(100+i) + 1
		if string(r.Key) != fmt.Sprintf("k-%d", n) || string(r.Value) != fmt.Sprintf("v-%d", n) {
			t.Fatalf("record %d = %s/%s, want k-%d (same keys, same order)", i, r.Key, r.Value, n)
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != "trace-id" {
			t.Fatalf("record %d headers %+v", i, r.Headers)
		}
	}

	v, err := a.Verify(ctx, params, executedAt)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if v.Before != 100 || v.After != 0 || events.RecoveryOutcomeFor(v.Before, v.After) != events.RecoveryRecovered {
		t.Fatalf("Verify = %+v", v)
	}
}
