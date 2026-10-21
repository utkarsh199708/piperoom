package topology

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// testClickhouseOwners is testOwners with the warehouse kind set to clickhouse.
// It keeps the consumer group so the replacement of the group -> sink edge is
// visible; tests that want the pure engine path clear the groups.
func testClickhouseOwners() *owners.Owners {
	o := testOwners()
	o.Pipelines[0].Connections = owners.Connections{"warehouse_kind": {Value: "clickhouse"}}
	return o
}

// fakeWarehouseLoad is a fakeWarehouse that also answers the ClickHouse
// load-history reader (clients.LoadErrorReader).
type fakeWarehouseLoad struct {
	fakeWarehouse
	loads   map[string][]clients.LoadError
	loadErr map[string]error
}

func (f *fakeWarehouseLoad) LoadErrors(_ context.Context, table string, _ time.Time, _ int) ([]clients.LoadError, error) {
	if f.loadErr != nil {
		if err := f.loadErr[table]; err != nil {
			return nil, err
		}
	}
	return f.loads[table], nil
}

func clickhouseConnect() *fakeConnect {
	return &fakeConnect{
		config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders", "slot.name": "pg_orders_slot"}},
		status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
	}
}

// TestBuildClickhouseHop proves Build wires the ClickHouse Kafka-ingestion hop
// for a clickhouse warehouse: the topic/group -> wh:sink edge becomes
// topic -> kafka_engine -> materialized_view -> wh:sink, and the golden render
// shows the engine and view listed between the topic and the sink.
func TestBuildClickhouseHop(t *testing.T) {
	o := testClickhouseOwners()
	o.Pipelines[0].ConsumerGroups = nil
	k := &fakeKafka{
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-40 * time.Minute)}}},
	}
	wh := &fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-45 * time.Minute)}}}
	b := newBuilder(o, clickhouseConnect(), k,
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg_orders_slot", Active: true, RetainedWALBytes: 10}}}, wh, nil)
	b.now = func() time.Time { return srNow }

	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := b.Render(g)
	const path = "testdata/clickhouse_render.golden"
	if *updateGolden {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("Render mismatch.\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

// TestBuildClickhouseReplacesWarehouseEdges proves the hop replaces, not adds
// beside, the topic/group -> wh:sink edges: the group still reads the topic,
// but nothing feeds the sink except the materialized view.
func TestBuildClickhouseReplacesWarehouseEdges(t *testing.T) {
	o := testClickhouseOwners() // keeps order-sync
	k := &fakeKafka{
		lags:    map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")},
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-40 * time.Minute)}}},
	}
	wh := &fakeWarehouseLoad{fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-time.Minute)}}}
	b := newBuilder(o, clickhouseConnect(), k,
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg_orders_slot", Active: true, RetainedWALBytes: 10}}}, wh, nil)
	b.now = func() time.Time { return srNow }

	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := edgeStrings(g.Edges)
	for _, e := range g.Edges {
		if e.To == "wh:analytics.orders" && e.From != "materialized_view:analytics.orders_mv" {
			t.Fatalf("sink fed by %q, want only the materialized view:\n%v", e.From, got)
		}
	}
	for _, want := range []string{
		"kafka_engine:analytics.orders_kafka>materialized_view:analytics.orders_mv ~",
		"materialized_view:analytics.orders_mv>wh:analytics.orders ~",
		"topic:pg.public.orders>kafka_engine:analytics.orders_kafka ~",
		"topic:pg.public.orders>group:order-sync",
	} {
		if !slices.Contains(got, want) {
			t.Fatalf("missing edge %q:\n%v", want, got)
		}
	}
	// The three hop edges are naming-convention guesses, so they must be
	// inferred, not proven.
	for _, e := range g.Edges {
		hop := (e.From == "topic:pg.public.orders" && e.To == "kafka_engine:analytics.orders_kafka") ||
			(e.From == "kafka_engine:analytics.orders_kafka" && e.To == "materialized_view:analytics.orders_mv") ||
			(e.From == "materialized_view:analytics.orders_mv" && e.To == "wh:analytics.orders")
		if hop && !e.IsInferred() {
			t.Fatalf("hop edge %+v must be inferred (naming convention)", e)
		}
	}
}

// TestBuildClickhouseEngineFirstFailing proves first-failing semantics: a
// Kafka engine that stopped consuming (a recent consumer exception) fails its
// own node, so the render's first failing component is the engine, not the
// sink it no longer fills (which is also stale).
func TestBuildClickhouseEngineFirstFailing(t *testing.T) {
	o := testClickhouseOwners()
	o.Pipelines[0].ConsumerGroups = nil
	k := &fakeKafka{
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: srNow.Add(-time.Minute)}}},
	}
	wh := &fakeWarehouseLoad{
		fakeWarehouse: fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": srNow.Add(-45 * time.Minute)}},
		loads: map[string][]clients.LoadError{
			"analytics.orders_kafka": {{Table: "analytics.orders_kafka", At: srNow.Add(-2 * time.Minute), Status: "LOAD_FAILED", FirstError: "ClickHouse Kafka engine consumer reported an exception"}},
		},
	}
	b := newBuilder(o, clickhouseConnect(), k,
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg_orders_slot", Active: true, RetainedWALBytes: 10}}}, wh, nil)
	b.now = func() time.Time { return srNow }

	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var engine, sink *Node
	for i := range g.Nodes {
		switch g.Nodes[i].Kind {
		case KindKafkaEngine:
			engine = &g.Nodes[i]
		case KindSinkTable:
			sink = &g.Nodes[i]
		}
	}
	if engine == nil {
		t.Fatalf("no kafka_engine node: %+v", g.Nodes)
	}
	if engine.Health != HealthFail {
		t.Fatalf("engine health = %s, want fail (consumer exception): %+v", engine.Health, engine)
	}
	if sink == nil || sink.Health != HealthFail {
		t.Fatalf("sink health = %v, want fail (stale): %+v", sinkHealthOrNil(sink), sink)
	}
	out := b.Render(g)
	ei := strings.Index(out, "kafka_engine:analytics.orders_kafka [warehouse, owner meera] FAIL")
	si := strings.Index(out, "wh:analytics.orders [warehouse, owner meera] FAIL")
	if ei < 0 || si < 0 || ei > si {
		t.Fatalf("engine must be the first failing component (engine FAIL listed before sink FAIL):\n%s", out)
	}
}

// sinkHealthOrNil returns the health of a sink node or "none" for a nil pointer,
// so a failure message reads even when the node is missing.
func sinkHealthOrNil(n *Node) string {
	if n == nil {
		return "none"
	}
	return string(n.Health)
}
