package topology

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// fakeMySQLSource is a MySQL source client: a BinlogReader whose
// ReplicationSlots must never be called (it counts the calls).
type fakeMySQLSource struct {
	fakeSource
	binlog    BinlogStatus
	binlogErr error
	slotCalls atomic.Int32
}

func (f *fakeMySQLSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	f.slotCalls.Add(1)
	return nil, clients.ErrNotSupported
}

func (f *fakeMySQLSource) BinlogStatus(context.Context) (BinlogStatus, error) {
	return f.binlog, f.binlogErr
}

func mysqlOwners() *owners.Owners {
	return &owners.Owners{
		Pipelines: []owners.Pipeline{{
			ID:             "orders-mysql",
			SourceKind:     owners.SourceKindMySQL,
			TopicPrefix:    "shop",
			SourceTables:   []string{"shop.orders"},
			Connectors:     []string{"orders-mysql"},
			ConsumerGroups: []string{"order-sync"},
			SinkTables:     []string{"analytics.orders"},
			SinkTSColumn:   "updated_at",
		}},
		Layers: owners.UserLayers(map[string]string{
			"mysql":    "dana",
			"postgres": "priya",
		}),
	}
}

func mysqlConnect(state string) *fakeConnect {
	return &fakeConnect{
		config: map[string]map[string]string{"orders-mysql": {
			"connector.class":       "io.debezium.connector.mysql.MySqlConnector",
			"topic.prefix":          "shop",
			"database.include.list": "shop",
			"table.include.list":    "shop.orders",
		}},
		status: map[string]clients.ConnectorStatus{"orders-mysql": {State: state, Tasks: []clients.TaskStatus{{State: state}}}},
	}
}

func TestBuildMySQLSourceNode(t *testing.T) {
	now := time.Now()
	retention := int64(48)
	for _, tc := range []struct {
		name   string
		src    clients.SourceDB
		health Health
		facts  map[string]string
	}{
		{
			name: "binlog read",
			src: &fakeMySQLSource{binlog: BinlogStatus{GTIDMode: "ON", ExpireSeconds: 7 * 24 * 3600,
				Binlogs: []BinlogFile{{Name: "binlog.000041"}, {Name: "binlog.000042"}}, BinlogsOmitted: 3}},
			health: HealthOK,
			facts:  map[string]string{"gtid_mode": "ON", "binlog_expire": "168.0h", "binlog_oldest": "binlog.000041", "binlog_files": "5"},
		},
		{
			name:   "rds retention and no expiry",
			src:    &fakeMySQLSource{binlog: BinlogStatus{RetentionHours: &retention, Binlogs: []BinlogFile{{Name: "mysql-bin-changelog.000001"}}}},
			health: HealthOK,
			facts:  map[string]string{"gtid_mode": "unknown", "binlog_expire": "48.0h", "binlog_oldest": "mysql-bin-changelog.0000"}, // capped at 24 runes
		},
		{
			name:   "never purged, no files",
			src:    &fakeMySQLSource{binlog: BinlogStatus{GTIDMode: "OFF"}},
			health: HealthWarn,
			facts:  map[string]string{"binlog_expire": "never", "binlog_oldest": "none"},
		},
		{
			name:   "unreachable",
			src:    &fakeMySQLSource{binlogErr: errors.New("dial tcp: refused")},
			health: HealthUnknown,
			facts:  map[string]string{"error": "unreachable"},
		},
		{
			name:   "not supported",
			src:    &fakeMySQLSource{binlogErr: clients.ErrNotSupported},
			health: HealthUnknown,
			facts:  map[string]string{"binlog": "unknown"},
		},
		{
			// A client without a binlog reader (the Postgres fake): unknown,
			// and its slots are not read either.
			name:   "no binlog reader",
			src:    &fakeSource{slots: []clients.ReplicationSlot{{Name: "debezium", Active: true}}},
			health: HealthUnknown,
			facts:  map[string]string{"binlog": "unknown"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuilder(mysqlOwners(), mysqlConnect("RUNNING"),
				&fakeKafka{
					lags:    map[string]clients.GroupLag{"order-sync": {State: "Stable"}},
					offsets: map[string][]clients.PartitionOffsets{"shop.shop.orders": {{Partition: 0, LastTimestamp: now}}},
				},
				tc.src,
				&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": now}},
			)
			g, err := b.Build(context.Background(), "orders-mysql")
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			n := g.Nodes[0]
			if n.ID != "mysql:shop.orders" || n.Kind != KindSourceTable || n.Layer != "mysql" || n.Owner != "dana" || n.Name != "shop.orders" {
				t.Fatalf("source node = %+v", n)
			}
			if n.Health != tc.health {
				t.Fatalf("health = %s, want %s (facts %v)", n.Health, tc.health, n.Facts)
			}
			for k, v := range tc.facts {
				if n.Facts[k] != v {
					t.Errorf("fact %s = %q, want %q (all %v)", k, n.Facts[k], v, n.Facts)
				}
			}
			if _, ok := n.Facts["slot"]; ok {
				t.Fatalf("a MySQL node has no slot fact: %v", n.Facts)
			}
			if f, ok := tc.src.(*fakeMySQLSource); ok && f.slotCalls.Load() != 0 {
				t.Fatal("ReplicationSlots must not be called for a MySQL pipeline")
			}
			// The data edge starts at the mysql: node and is proven.
			var edge *Edge
			for i := range g.Edges {
				if g.Edges[i].To == "connect:orders-mysql" {
					edge = &g.Edges[i]
				}
			}
			if edge == nil || edge.From != "mysql:shop.orders" || edge.Mapping == MappingInferred {
				t.Fatalf("source edge = %+v (all %+v)", edge, g.Edges)
			}
			for _, e := range g.Edges {
				if strings.HasPrefix(e.From, "pg:") {
					t.Fatalf("pg: edge in a MySQL graph: %+v", e)
				}
			}
			found := false
			for _, n := range g.Nodes {
				found = found || (n.Kind == KindTopic && n.Name == "shop.shop.orders")
			}
			if !found {
				t.Fatalf("topic shop.shop.orders missing: %+v", g.Nodes)
			}
		})
	}
}

func TestBuildMySQLDatabaseFilter(t *testing.T) {
	c := mysqlConnect("RUNNING")
	c.config["orders-mysql"]["database.include.list"] = "crm"
	b := NewBuilder(mysqlOwners(), c, &fakeKafka{}, &fakeMySQLSource{}, &fakeWarehouse{})
	g, err := b.Build(context.Background(), "orders-mysql")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range g.Edges {
		if e.From == "mysql:shop.orders" {
			t.Fatalf("a database the connector does not capture has no proven edge: %+v", e)
		}
	}
}

func TestBuildMySQLConnectDown(t *testing.T) {
	c := mysqlConnect("RUNNING")
	c.configErr = map[string]error{"orders-mysql": errors.New("down")}
	b := NewBuilder(mysqlOwners(), c, &fakeKafka{}, &fakeMySQLSource{}, &fakeWarehouse{})
	g, err := b.Build(context.Background(), "orders-mysql")
	if err != nil {
		t.Fatal(err)
	}
	var ok bool
	for _, e := range g.Edges {
		if e.From == "mysql:shop.orders" && e.To == "connect:orders-mysql" && e.Mapping == MappingInferred {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("inferred mysql: edge missing: %+v", g.Edges)
	}
}

func TestBuildPostgresSourceNodeUnchanged(t *testing.T) {
	b := NewBuilder(testOwners(), &fakeConnect{
		config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders", "slot.name": "s1"}},
	}, &fakeKafka{}, &fakeSource{slots: []clients.ReplicationSlot{{Name: "s1", Active: true}}}, &fakeWarehouse{})
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	n := g.Nodes[0]
	if n.ID != "pg:public.orders" || n.Layer != "postgres" || n.Owner != "priya" || n.Facts["slot_active"] != "true" {
		t.Fatalf("postgres node = %+v", n)
	}
}

func TestTraceMySQLSourceLayer(t *testing.T) {
	src := &fakeMySQLSource{}
	src.rows = map[string]map[string]any{"shop.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}
	b := NewBuilder(mysqlOwners(), mysqlConnect("FAILED"), &fakeKafka{}, src, &fakeWarehouse{})
	stages, err := b.Trace(context.Background(), "orders-mysql", "shop.orders", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if stages[0].Layer != "mysql" || stages[0].Status != "seen" {
		t.Fatalf("source stage = %+v", stages[0])
	}
	if stages[1].Status != "stopped_here" {
		t.Fatalf("connector stage = %+v", stages[1])
	}
	for _, s := range stages {
		if s.Layer == "postgres" {
			t.Fatalf("postgres stage in a MySQL trace: %+v", stages)
		}
	}
	// Unreachable and missing rows name the mysql layer too.
	src.rowErr = errors.New("down")
	stages, _ = b.Trace(context.Background(), "orders-mysql", "shop.orders", "7")
	if stages[0].Layer != "mysql" || stages[0].Facts["error"] != "unreachable" {
		t.Fatalf("unreachable stage = %+v", stages[0])
	}
}

func TestSourceNodeIDs(t *testing.T) {
	pg := owners.Pipeline{}
	my := owners.Pipeline{SourceKind: owners.SourceKindMySQL}
	if sourceNodeID(pg, "public.a") != "pg:public.a" || sourceNodeID(my, "shop.a") != "mysql:shop.a" {
		t.Fatal("source node ids")
	}
	for _, id := range []string{"pg:shop.a", "mysql:shop.a"} {
		if sourceTableOf(id) != "shop.a" {
			t.Fatalf("sourceTableOf(%s)", id)
		}
	}
	if (traceFacts{}).srcLayer() != "postgres" {
		t.Fatal("empty source layer reads as postgres")
	}
}
