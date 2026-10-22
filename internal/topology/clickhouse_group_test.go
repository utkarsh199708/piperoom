package topology

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// fakeWarehouseGroups is fakeWarehouseLoad plus the optional engine-group
// reader (clickhouse.Client.EngineConsumerGroup): the warehouse tells the
// builder which Kafka consumer group each engine table consumes with.
type fakeWarehouseGroups struct {
	fakeWarehouseLoad
	groups map[string]string
	errs   map[string]error
}

func (f *fakeWarehouseGroups) EngineConsumerGroup(_ context.Context, table string) (string, error) {
	if err := f.errs[table]; err != nil {
		return "", err
	}
	return f.groups[table], nil
}

// engineGroupLag is a Stable group that is behind on the pipeline's topic.
func engineGroupLag(topic string, lag int64) clients.GroupLag {
	return clients.GroupLag{
		Group:    "piperoom-clickhouse-orders",
		State:    "Stable",
		Members:  1,
		TotalLag: lag,
		Partitions: []clients.PartitionLag{
			{Topic: topic, Partition: 0, Committed: 10, End: 10 + lag, Lag: lag},
		},
	}
}

// clickhouseGroupBuilder is the ClickHouse orders builder with the engine's
// group reader; wh decides what the warehouse answers.
func clickhouseGroupBuilder(o *owners.Owners, wh clients.Warehouse, lags map[string]clients.GroupLag) *builder {
	b := newBuilder(o, clickhouseConnect(), &fakeKafka{
		lags:    lags,
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-time.Minute)}}},
	}, &fakeSource{slots: []clients.ReplicationSlot{{Name: "pg_orders_slot", Active: true, RetainedWALBytes: 10}}}, wh, nil)
	b.now = func() time.Time { return srNow }
	return b
}

// TC1: the ClickHouse engine's Kafka consumer group (kafka_group_name) becomes
// a consumer_group node on the kafka_engine node, with its own lag, and the
// owners config records it so tools.Scope accepts it.
func TestBuildClickhouseEngineConsumerGroup(t *testing.T) {
	o := testClickhouseOwners()
	wh := &fakeWarehouseGroups{
		fakeWarehouseLoad: fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-45 * time.Minute)}}},
		groups:            map[string]string{"analytics.orders_kafka": "piperoom-clickhouse-orders"},
	}
	b := clickhouseGroupBuilder(o, wh, map[string]clients.GroupLag{
		"order-sync":                 lagOn("pg.public.orders"),
		"piperoom-clickhouse-orders": engineGroupLag("pg.public.orders", 5000),
	})
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	n := g.mustNode(t, "group:piperoom-clickhouse-orders")
	if n.Kind != KindConsumerGroup || n.Name != "piperoom-clickhouse-orders" {
		t.Fatalf("engine group node = %+v", n)
	}
	if n.Layer != "warehouse" || n.Owner != "meera" {
		t.Fatalf("engine group layer/owner = %s/%s, want warehouse/meera", n.Layer, n.Owner)
	}
	if n.Health != HealthWarn {
		t.Fatalf("engine group health = %s, want warn (lag 5000): %+v", n.Health, n)
	}
	if n.Facts["group"] != "piperoom-clickhouse-orders" || n.Facts["engine"] != "analytics.orders_kafka" {
		t.Fatalf("engine group facts = %v", n.Facts)
	}
	if n.Facts["lag"] != "5000" || n.Facts["state"] != "Stable" {
		t.Fatalf("engine group lag/state facts = %v", n.Facts)
	}

	edges := edgeStrings(g.Edges)
	for _, want := range []string{
		"topic:pg.public.orders>group:piperoom-clickhouse-orders ~",
		"group:piperoom-clickhouse-orders>kafka_engine:analytics.orders_kafka",
	} {
		if !slices.Contains(edges, want) {
			t.Fatalf("missing edge %q:\n%v", want, edges)
		}
	}
	// The group -> engine edge is proven by the engine's CREATE statement, not
	// inferred from the naming convention.
	for _, e := range g.Edges {
		if e.From == "group:piperoom-clickhouse-orders" && e.To == "kafka_engine:analytics.orders_kafka" && e.IsInferred() {
			t.Fatalf("group -> engine edge must be mapped, got %+v", e)
		}
	}
	if got, want := o.DerivedConsumerGroups("orders"), []string{"piperoom-clickhouse-orders"}; !slices.Equal(got, want) {
		t.Fatalf("derived consumer groups = %v, want %v (tools.Scope.CheckReadGroup reads this)", got, want)
	}

	// Data-flow order: topic, engine group, engine, view, sink.
	out := b.Render(g)
	gi := strings.Index(out, "group:piperoom-clickhouse-orders [")
	ei := strings.Index(out, "kafka_engine:analytics.orders_kafka [")
	if gi < 0 || ei < 0 || gi > ei {
		t.Fatalf("render must list the engine's consumer group before the engine:\n%s", out)
	}
	if !strings.Contains(out, "from: group:piperoom-clickhouse-orders\n") {
		t.Fatalf("engine node must take its input from the group:\n%s", out)
	}
}

// TC4: with two consumers on one topic (the declared CDC group and the
// warehouse engine's own group), each shows its own lag: a healthy sink is not
// flagged, and the declared CDC group reads healthy whatever the sink does.
func TestBuildClickhouseEngineGroupDoesNotBlameCDC(t *testing.T) {
	o := testClickhouseOwners()
	wh := &fakeWarehouseGroups{
		fakeWarehouseLoad: fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}},
		groups:            map[string]string{"analytics.orders_kafka": "piperoom-clickhouse-orders"},
	}
	b := clickhouseGroupBuilder(o, wh, map[string]clients.GroupLag{
		"order-sync":                 lagOn("pg.public.orders"),
		"piperoom-clickhouse-orders": engineGroupLag("pg.public.orders", 0),
	})
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := g.mustNode(t, "group:order-sync").Health; got != HealthOK {
		t.Fatalf("declared CDC group health = %s, want ok", got)
	}
	if got := g.mustNode(t, "group:piperoom-clickhouse-orders").Health; got != HealthOK {
		t.Fatalf("healthy sink's engine group health = %s, want ok", got)
	}
	if got := g.mustNode(t, "wh:analytics.orders").Health; got != HealthOK {
		t.Fatalf("fresh sink health = %s, want ok", got)
	}
	if out := b.Render(g); strings.Contains(out, " FAIL") {
		t.Fatalf("a healthy pipeline must not be flagged:\n%s", out)
	}
}

// TC3: an unreadable or unsupported group read leaves no group node and does
// not fail the build; the engine node says why the sink's consumer is not
// watched, so a missing engine group is not mistaken for a healthy one.
func TestBuildClickhouseEngineGroupUnreadable(t *testing.T) {
	tests := []struct {
		name string
		wh   clients.Warehouse
		want string
	}{
		{
			name: "access denied",
			wh: &fakeWarehouseGroups{
				fakeWarehouseLoad: fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}},
				errs:              map[string]error{"analytics.orders_kafka": errors.New("clickhouse: SHOW CREATE TABLE: code 497 ACCESS_DENIED")},
			},
			want: "unreadable",
		},
		{
			name: "not a kafka engine",
			wh: &fakeWarehouseGroups{
				fakeWarehouseLoad: fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}},
				errs:              map[string]error{"analytics.orders_kafka": clients.ErrNotSupported},
			},
			want: "not monitored",
		},
		{
			name: "warehouse without the reader",
			wh:   &fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}},
			want: "not monitored",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := testClickhouseOwners()
			b := clickhouseGroupBuilder(o, tt.wh, map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")})
			g, err := b.Build(context.Background(), "orders")
			if err != nil {
				t.Fatalf("Build must not fail on an unreadable engine group: %v", err)
			}
			for _, n := range g.Nodes {
				if n.Kind == KindConsumerGroup && n.Name == "piperoom-clickhouse-orders" {
					t.Fatalf("unexpected engine group node: %+v", n)
				}
			}
			if got := g.mustNode(t, "kafka_engine:analytics.orders_kafka").Facts["consumer_group"]; got != tt.want {
				t.Fatalf("engine consumer_group fact = %q, want %q", got, tt.want)
			}
			if got := o.DerivedConsumerGroups("orders"); got != nil {
				t.Fatalf("derived groups = %v, want none (scope must not widen on a failed read)", got)
			}
			// The engine still feeds the view and the sink, as before.
			if !slices.Contains(edgeStrings(g.Edges), "topic:pg.public.orders>kafka_engine:analytics.orders_kafka ~") {
				t.Fatalf("hop edge missing:\n%v", edgeStrings(g.Edges))
			}
		})
	}
}

// A group owners.yaml already declares is drawn once, by the declared-group
// rule (consumer layer), and the engine still takes its input from it.
func TestBuildClickhouseEngineGroupDeclared(t *testing.T) {
	o := testClickhouseOwners()
	o.Pipelines[0].ConsumerGroups = []string{"order-sync", "piperoom-clickhouse-orders"}
	wh := &fakeWarehouseGroups{
		fakeWarehouseLoad: fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}},
		groups:            map[string]string{"analytics.orders_kafka": "piperoom-clickhouse-orders"},
	}
	b := clickhouseGroupBuilder(o, wh, map[string]clients.GroupLag{
		"order-sync":                 lagOn("pg.public.orders"),
		"piperoom-clickhouse-orders": engineGroupLag("pg.public.orders", 0),
	})
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	count := 0
	for _, n := range g.Nodes {
		if n.ID == "group:piperoom-clickhouse-orders" {
			count++
			if n.Layer != "consumer" || n.Owner != "kabir" {
				t.Fatalf("declared group node = %+v, want the consumer layer node", n)
			}
		}
	}
	if count != 1 {
		t.Fatalf("group:piperoom-clickhouse-orders appears %d times, want once", count)
	}
	if !slices.Contains(edgeStrings(g.Edges), "group:piperoom-clickhouse-orders>kafka_engine:analytics.orders_kafka") {
		t.Fatalf("declared engine group must feed the engine:\n%v", edgeStrings(g.Edges))
	}
}
