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
	d, _ := scopeFakes()
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
