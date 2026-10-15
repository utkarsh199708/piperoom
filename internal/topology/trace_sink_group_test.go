package topology

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

func twoTableOwners(groups, sinks []string) *owners.Owners {
	o := testOwners()
	o.Pipelines[0].SourceTables = []string{"public.orders", "public.customers"}
	o.Pipelines[0].ConsumerGroups = groups
	o.Pipelines[0].SinkTables = sinks
	return o
}

func twoTableBuilder(o *owners.Owners, lags map[string]clients.GroupLag) Builder {
	return NewBuilder(
		o,
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders,public.customers"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING"}},
		},
		&fakeKafka{
			findByKey: map[string][]clients.RecordRef{"pg.public.customers": {{Key: "7", Partition: 0, Offset: 3}}},
			lags:      lags,
		},
		&fakeSource{rows: map[string]map[string]any{"public.customers|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{rows: map[string]map[string]any{"analytics.customers|7": {"id": "7"}}},
	)
}

// A trace of public.customers read the first sink table (analytics.orders)
// and the first group, and reported "stopped at the warehouse" for a row that
// had arrived (#401). The sink is the one with the same table name and the
// group is the one that reads the topic.
func TestTracePicksSinkAndGroupOfTheTable(t *testing.T) {
	o := twoTableOwners([]string{"order-sync", "customer-sync"}, []string{"analytics.orders", "analytics.customers"})
	b := twoTableBuilder(o, map[string]clients.GroupLag{
		"order-sync":    {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: 1}}},
		"customer-sync": {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.customers", Partition: 0, Committed: 4}}},
	})
	stages, err := b.Trace(context.Background(), "orders", "public.customers", "7")
	if err != nil {
		t.Fatal(err)
	}
	if g := stages[3]; g.Name != "customer-sync" || g.Status != "seen" {
		t.Fatalf("consumer stage = %+v, want customer-sync seen", g)
	}
	if w := stages[4]; w.Name != "analytics.customers" || w.Status != "seen" {
		t.Fatalf("warehouse stage = %+v, want analytics.customers seen", w)
	}
}

// When no sink matches and no group reads the topic, those stages are
// unknown: never another table's verdict, never "stopped_here".
func TestTraceUnknownSinkAndGroup(t *testing.T) {
	o := twoTableOwners([]string{"g1", "g2"}, []string{"analytics.a", "analytics.b"})
	b := twoTableBuilder(o, map[string]clients.GroupLag{
		"g1": {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders"}}},
		"g2": {State: "Dead"},
	})
	stages, err := b.Trace(context.Background(), "orders", "public.customers", "7")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stages[3:] {
		if s.Status != "unknown" {
			t.Fatalf("stage %+v: want unknown (all stages %+v)", s, stages)
		}
	}
	if stages[3].Facts["error"] != "no group reads the topic" || stages[4].Facts["error"] != "no matching sink table" {
		t.Fatalf("unknown reasons: %+v / %+v", stages[3].Facts, stages[4].Facts)
	}
}

// One sink, several source tables: the lone sink was used for every table,
// so a customers row read analytics.orders and got "stopped_here" (#401
// review). The warehouse stage is unknown unless the names match.
func TestTraceSingleSinkSeveralSources(t *testing.T) {
	lags := map[string]clients.GroupLag{"customer-sync": {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.customers", Partition: 0, Committed: 4}}}}

	b := twoTableBuilder(twoTableOwners([]string{"customer-sync"}, []string{"analytics.orders"}), lags)
	stages, err := b.Trace(context.Background(), "orders", "public.customers", "7")
	if err != nil {
		t.Fatal(err)
	}
	if w := stages[4]; w.Status != "unknown" || w.Facts["error"] != "no matching sink table" {
		t.Fatalf("warehouse stage = %+v, want unknown (no matching sink table)", w)
	}

	b = twoTableBuilder(twoTableOwners([]string{"customer-sync"}, []string{"analytics.customers"}), lags)
	stages, err = b.Trace(context.Background(), "orders", "public.customers", "7")
	if err != nil {
		t.Fatal(err)
	}
	if w := stages[4]; w.Name != "analytics.customers" || w.Status != "seen" {
		t.Fatalf("warehouse stage = %+v, want analytics.customers seen", w)
	}
}

// groupFor with several groups: a group whose lag cannot be read is skipped
// when another reads the topic; when every read fails the stage is unknown
// as unreachable.
func TestTraceGroupForErrors(t *testing.T) {
	reads := map[string]clients.GroupLag{"g2": {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.customers", Partition: 0, Committed: 4}}}}
	cases := []struct {
		name       string
		errs       map[string]error
		wantName   string
		wantStatus string
		wantError  string
	}{
		{"one group down, the other reads the topic", map[string]error{"group:g1": errors.New("down")}, "g2", "seen", ""},
		{"every group down", map[string]error{"group:g1": errors.New("down"), "group:g2": errors.New("down")}, "g1,g2", "unknown", "unreachable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := twoTableOwners([]string{"g1", "g2"}, []string{"analytics.orders", "analytics.customers"})
			b := NewBuilder(
				o,
				&fakeConnect{
					config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders,public.customers"}},
					status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING"}},
				},
				&fakeKafka{
					findByKey: map[string][]clients.RecordRef{"pg.public.customers": {{Key: "7", Partition: 0, Offset: 3}}},
					lags:      reads,
					err:       tc.errs,
				},
				&fakeSource{rows: map[string]map[string]any{"public.customers|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
				&fakeWarehouse{rows: map[string]map[string]any{"analytics.customers|7": {"id": "7"}}},
			)
			stages, err := b.Trace(context.Background(), "orders", "public.customers", "7")
			if err != nil {
				t.Fatal(err)
			}
			g := stages[3]
			if g.Name != tc.wantName || g.Status != tc.wantStatus || g.Facts["error"] != tc.wantError {
				t.Fatalf("consumer stage = %+v, want %s %s error=%q", g, tc.wantName, tc.wantStatus, tc.wantError)
			}
		})
	}
}

// Two source tables with one name in different schemas both matched the sink
// of that name, so archive.orders read analytics.orders' row (#416 review
// round 2). The name tells nothing then: the stage is unknown.
func TestTraceSameNameSourcesHaveNoSink(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].SourceTables = []string{"public.orders", "archive.orders"}
	o.Pipelines[0].SinkTables = []string{"analytics.orders"}
	b := NewBuilder(
		o,
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders,archive.orders"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING"}},
		},
		&fakeKafka{
			findByKey: map[string][]clients.RecordRef{"pg.archive.orders": {{Key: "7", Partition: 0, Offset: 3}}},
			lags:      map[string]clients.GroupLag{"order-sync": {State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.archive.orders", Partition: 0, Committed: 4}}}},
		},
		&fakeSource{rows: map[string]map[string]any{"archive.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{rows: map[string]map[string]any{"analytics.orders|7": {"id": "7"}}},
	)
	stages, err := b.Trace(context.Background(), "orders", "archive.orders", "7")
	if err != nil {
		t.Fatal(err)
	}
	if w := stages[4]; w.Status != "unknown" || w.Facts["error"] != "no matching sink table" {
		t.Fatalf("warehouse stage = %+v, want unknown (no matching sink table)", w)
	}
}
