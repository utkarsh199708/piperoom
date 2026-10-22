package kafkatools

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// TC5: on kfake (franz-go's in-process broker, real wire protocol, no
// Docker), partition 0 is written at 2/s and partition 1 at 0.5/s over the
// last 10 minutes, after older records outside the window; kafka_throughput
// computes both rates within 10 %, and reads a committed group's lag.
func TestThroughputKFake(t *testing.T) {
	const topic = "pg.public.orders"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	window := 10 * time.Minute
	now := time.Now()
	produce := func(p int32, n int, from, to time.Time) {
		t.Helper()
		step := to.Sub(from) / time.Duration(n)
		recs := make([]*kgo.Record, 0, n)
		for i := range n {
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Value: []byte("v"), Timestamp: from.Add(time.Duration(i) * step)})
		}
		if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			t.Fatalf("produce p%d: %v", p, err)
		}
	}
	// Outside the window: must not count.
	produce(0, 500, now.Add(-time.Hour), now.Add(-30*time.Minute))
	// Inside: 2/s and 0.5/s over the window (a few seconds inside its edges).
	start := now.Add(-window + 5*time.Second)
	produce(0, int(2*window.Seconds()), start, now)
	produce(1, int(0.5*window.Seconds()), start, now)

	// A group that has consumed everything on p1 and nothing new on p0.
	adm := kadm.NewClient(prod)
	var offs kadm.Offsets
	offs.Add(kadm.Offset{Topic: topic, Partition: 0, At: 500, LeaderEpoch: -1})
	offs.Add(kadm.Offset{Topic: topic, Partition: 1, At: int64(0.5 * window.Seconds()), LeaderEpoch: -1})
	if _, err := adm.CommitOffsets(ctx, "order-sync", offs); err != nil {
		t.Fatalf("commit: %v", err)
	}

	kc, err := kafka.New(c.ListenAddrs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kc.Close)
	tctx := tools.WithPipeline(ctx, owners.Pipeline{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, ConsumerGroups: []string{"order-sync"},
	})
	r, err := (&throughput{d: Deps{Kafka: kc}}).Run(tctx, json.RawMessage(`{"window":"10m"}`))
	if err != nil {
		t.Fatal(err)
	}
	d := r.Data.(map[string]any)
	got := map[int32]float64{}
	for _, p := range d["partitions"].([]tpPartition) {
		got[p.Partition] = p.PerSecond
	}
	for p, want := range map[int32]float64{0: 2, 1: 0.5} {
		if math.Abs(got[p]-want) > want*0.10 {
			t.Errorf("p%d rate %.3f/s, want %.1f/s ±10%%", p, got[p], want)
		}
	}
	gs := d["groups"].([]tpGroup)
	if len(gs) != 1 || gs[0].Error != "" || gs[0].Lag != int64(2*window.Seconds()) {
		t.Fatalf("groups = %+v, want lag %d all on p0", gs, int64(2*window.Seconds()))
	}
	if !strings.Contains(r.Summary, "pg.public.orders p0 holds 100% of order-sync's lag") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}
