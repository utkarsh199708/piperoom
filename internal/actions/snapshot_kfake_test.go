package actions

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients/kafka"
)

// TestSnapshotTableKfake runs connect_snapshot_table end to end with the
// real Kafka Writer against kfake, under the room's scope as the executor
// runs it: Validate, Preview, Execute, then the signal is read back from
// partition 0 of the signal topic.
func TestSnapshotTableKfake(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "pg-signal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	w, err := kafka.NewWriter(c.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)

	a := &snapshotTable{d: SnapshotDeps{Connect: &sigConnect{cfg: topicConfig()}, Kafka: w}}
	params := snapParams(ViaSignalTopic, "public.orders")
	ctx, cancel := context.WithTimeout(roomCtx(), 10*time.Second)
	defer cancel()
	if err := a.Validate(ctx, params); err != nil {
		t.Fatal(err)
	}
	if err := a.CheckScope(snapPipeline, params); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Preview(ctx, params); err != nil {
		t.Fatal(err)
	}
	out, err := a.Execute(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "signal 0 (partition 0 offset 0") {
		t.Fatalf("result %q", out)
	}

	cons, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		"pg-signal": {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	fs := cons.PollFetches(ctx)
	if errs := fs.Errors(); len(errs) > 0 {
		t.Fatalf("fetch: %v", errs)
	}
	recs := fs.Records()
	want := `{"type":"execute-snapshot","data":{"data-collections":["public.orders"],"type":"INCREMENTAL"}}`
	if len(recs) != 1 || string(recs[0].Key) != "pg" || string(recs[0].Value) != want {
		t.Fatalf("signal records %+v", recs)
	}
}
