package kafkatools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// A group of the pipeline can also read topics of other pipelines; only the
// pipeline's own partitions and lag are shown.
func TestConsumerLagShowsOnlyThePipelinesTopics(t *testing.T) {
	d, _ := scopeFakes(t)
	d.Kafka.(*fakeKafkaConfig).lagFn = func(_ context.Context, g string) (clients.GroupLag, error) {
		return clients.GroupLag{Group: g, State: "Stable", Members: 2, TotalLag: 1100, Partitions: []clients.PartitionLag{
			{Topic: "pg.public.orders", Partition: 0, Lag: 100},
			{Topic: "bill.public.invoices", Partition: 0, Lag: 1000},
		}}, nil
	}
	r, err := scopeTool(t, d, "kafka_consumer_lag").Run(roomCtx(), json.RawMessage(`{"group":"order-sync"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.Summary, "bill.public") || !strings.Contains(r.Summary, "total lag 100") || !strings.Contains(r.Summary, "pg.public.orders/0") {
		t.Fatalf("summary = %q", r.Summary)
	}
	data, _ := json.Marshal(r.Data)
	if strings.Contains(string(data), "bill.public") {
		t.Fatalf("data leaks the other topic: %s", data)
	}

	// Unscoped keeps the old output.
	r, err = scopeTool(t, d, "kafka_consumer_lag").Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"group":"order-sync"}`))
	if err != nil || !strings.Contains(r.Summary, "bill.public.invoices") || !strings.Contains(r.Summary, "total lag 1100") {
		t.Fatalf("unscoped summary = %q, err = %v", r.Summary, err)
	}
}

// TestConsumerLagUnknownPartitions: during a broker outage GroupLag lists the
// partitions it could not read in Unknown. The summary must call the total a
// lower bound instead of "total lag 0", and the other pipelines' unknown
// partitions stay out, as their lag does.
func TestConsumerLagUnknownPartitions(t *testing.T) {
	d, _ := scopeFakes(t)
	d.Kafka.(*fakeKafkaConfig).lagFn = func(_ context.Context, g string) (clients.GroupLag, error) {
		return clients.GroupLag{Group: g, State: "Stable", Members: 1, TotalLag: 0,
			Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: 5, End: 5, Lag: 0}},
			Unknown: []clients.PartitionLag{
				{Topic: "pg.public.orders", Partition: 1, Committed: 40, End: -1, Lag: -1},
				{Topic: "bill.public.invoices", Partition: 0, Committed: -1, End: -1, Lag: -1},
			}}, nil
	}
	r, err := scopeTool(t, d, "kafka_consumer_lag").Run(roomCtx(), json.RawMessage(`{"group":"order-sync"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"total lag at least 0", "lag unknown for 1 partition(s)", "pg.public.orders/1"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary %q: want %q", r.Summary, want)
		}
	}
	if strings.Contains(r.Summary, "bill.public") {
		t.Fatalf("summary leaks the other topic: %q", r.Summary)
	}
	data, _ := json.Marshal(r.Data)
	if !strings.Contains(string(data), `"unknown_partitions":[{"topic":"pg.public.orders","partition":1`) || strings.Contains(string(data), "bill.public") {
		t.Fatalf("data = %s", data)
	}
}

// TestListGroupsCountsUnknownPartitions: a group whose every partition of the
// topic is unreadable still reads the topic.
func TestListGroupsCountsUnknownPartitions(t *testing.T) {
	lag := clients.GroupLag{Unknown: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: -1, End: -1, Lag: -1}}}
	if !groupReadsTopic(lag, "pg.public.orders") {
		t.Fatal("groupReadsTopic ignores Unknown")
	}
}
