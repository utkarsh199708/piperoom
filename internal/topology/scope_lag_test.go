package topology

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// The pipeline's consumer group also reads another pipeline's topic. The map
// shows the lag of the pipeline's own partitions only: the other topic's lag
// (a marker number) and name never reach the graph.
func TestBuildGroupLagCountsOnlyThePipelinesTopics(t *testing.T) {
	const foreignTopic = "bill.public.FOREIGNMARK_invoices"
	const foreignLag = 987654321
	now := time.Now()
	lag := clients.GroupLag{State: "Stable", Members: 1, TotalLag: 7 + foreignLag, Partitions: []clients.PartitionLag{
		{Topic: "pg.public.orders", Partition: 0, Committed: 3, End: 10, Lag: 7},
		{Topic: foreignTopic, Partition: 0, Committed: 0, End: foreignLag, Lag: foreignLag},
	}}
	connect := &fakeConnect{
		config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
		status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
	}
	build := func(t *testing.T, c *fakeConnect, prefix string) Node {
		t.Helper()
		o := testOwners()
		o.Pipelines[0].TopicPrefix = prefix
		b := NewBuilder(o, c,
			&fakeKafka{
				lags:    map[string]clients.GroupLag{"order-sync": lag},
				offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: now}}},
			},
			&fakeSource{slots: []clients.ReplicationSlot{{Name: "debezium", Active: true}}},
			&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": now}},
		)
		g, err := b.Build(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(g)
		for _, leak := range []string{"FOREIGNMARK", "987654"} {
			if strings.Contains(string(raw), leak) {
				t.Fatalf("graph leaks %q: %s", leak, raw)
			}
		}
		for _, n := range g.Nodes {
			if n.Kind == KindConsumerGroup {
				return n
			}
		}
		t.Fatal("no consumer group node")
		return Node{}
	}

	t.Run("topics from the connector config", func(t *testing.T) {
		n := build(t, connect, "")
		if n.Facts["lag"] != "7" || n.Health != HealthOK {
			t.Fatalf("group node = %+v, want lag 7 and ok", n)
		}
	})
	t.Run("Connect down: topics from owners.yaml", func(t *testing.T) {
		down := &fakeConnect{configErr: map[string]error{"pg-orders-connector": clients.ErrNotFound}}
		n := build(t, down, "pg")
		if n.Facts["lag"] != "7" {
			t.Fatalf("group node = %+v, want lag 7", n)
		}
	})
}

func TestPipelineLag(t *testing.T) {
	own := map[string]bool{"pg.public.orders": true}
	got, ok := pipelineLag(clients.GroupLag{TotalLag: 11, Partitions: []clients.PartitionLag{
		{Topic: "pg.public.orders", Lag: 4}, {Topic: "x", Lag: 7},
	}}, own)
	if !ok || got.TotalLag != 4 || len(got.Partitions) != 1 || got.Partitions[0].Topic != "pg.public.orders" {
		t.Fatalf("pipelineLag = %+v, %v", got, ok)
	}
	// Every partition foreign: the lag is unknown, neither 0 nor the group's total.
	got, ok = pipelineLag(clients.GroupLag{TotalLag: 7, Partitions: []clients.PartitionLag{{Topic: "x", Lag: 7}}}, own)
	if ok || got.TotalLag == 7 || len(got.Partitions) != 0 {
		t.Fatalf("pipelineLag all foreign = %+v, %v; want not ok and no foreign lag", got, ok)
	}
	// No partitions: nothing to tell apart, the total is kept.
	if got, ok := pipelineLag(clients.GroupLag{TotalLag: 5}, own); !ok || got.TotalLag != 5 {
		t.Fatalf("pipelineLag without partitions = %+v, %v", got, ok)
	}
}

// A group whose partitions are all on topics that are not the pipeline's (for
// example renamed by a RegexRouter) shows lag unknown, not "0" and ok.
func TestBuildGroupWithNoPipelinePartitionIsUnknown(t *testing.T) {
	now := time.Now()
	b := NewBuilder(testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
		},
		&fakeKafka{lags: map[string]clients.GroupLag{"order-sync": {State: "Stable", TotalLag: 5000, Partitions: []clients.PartitionLag{
			{Topic: "routed.orders", Partition: 0, Lag: 5000},
		}}}},
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "debezium", Active: true}}},
		&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": now}},
	)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range g.Nodes {
		if n.Kind != KindConsumerGroup {
			continue
		}
		if n.Health != HealthUnknown || n.Facts["lag"] != "unknown (no partition of this pipeline's topics)" || n.Facts["state"] != "Stable" {
			t.Fatalf("group node = %+v, want health unknown and lag unknown", n)
		}
		return
	}
	t.Fatal("no consumer group node")
}
