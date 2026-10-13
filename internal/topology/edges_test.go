package topology

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// twoLaneOwners is a pipeline with two source connectors, two topics, two
// consumer groups and one sink table, where only c1 -> p1.public.a -> g1 and
// c2 -> p2.public.b -> g2 really flow. The old builder drew every table into
// every connector and every topic into every group.
func twoLaneOwners() *owners.Owners {
	o := testOwners()
	o.Pipelines[0].SourceTables = []string{"public.a", "public.b"}
	o.Pipelines[0].Connectors = []string{"c1", "c2"}
	o.Pipelines[0].ConsumerGroups = []string{"g1", "g2"}
	o.Pipelines[0].SinkTables = []string{"analytics.x"}
	return o
}

func twoLaneConnect() *fakeConnect {
	return &fakeConnect{
		config: map[string]map[string]string{
			"c1": {"topic.prefix": "p1", "table.include.list": "public.a", "slot.name": "s1"},
			"c2": {"topic.prefix": "p2", "table.include.list": "public.b", "slot.name": "s2"},
		},
		status: map[string]clients.ConnectorStatus{"c1": {State: "RUNNING"}, "c2": {State: "RUNNING"}},
	}
}

func lagOn(topics ...string) clients.GroupLag {
	l := clients.GroupLag{State: "Stable"}
	for _, t := range topics {
		l.Partitions = append(l.Partitions, clients.PartitionLag{Topic: t, Partition: 0, Committed: 5, End: 5})
	}
	return l
}

func buildWith(t *testing.T, o *owners.Owners, c *fakeConnect, k *fakeKafka) Graph {
	t.Helper()
	b := newBuilder(o, c, k, &fakeSource{}, &fakeWarehouse{}, nil)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return g
}

// edgeStrings renders edges as "from>to" plus " ~" when inferred and " dep"
// for depends_on, sorted, so a test can compare the whole edge set.
func edgeStrings(es []Edge) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		s := e.From + ">" + e.To
		if e.IsDependency() {
			s += " dep"
		}
		if e.IsInferred() {
			s += " ~"
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func TestBuildEdgesNoCrossProduct(t *testing.T) {
	g := buildWith(t, twoLaneOwners(), twoLaneConnect(), &fakeKafka{
		lags: map[string]clients.GroupLag{"g1": lagOn("p1.public.a"), "g2": lagOn("p2.public.b", "unrelated")},
	})
	want := []string{
		"connect:c1>topic:p1.public.a",
		"connect:c2>topic:p2.public.b",
		"group:g1>wh:analytics.x ~", // owners.yaml declares no group -> sink link
		"group:g2>wh:analytics.x ~",
		"pg:public.a>connect:c1",
		"pg:public.b>connect:c2",
		"topic:p1.public.a>group:g1",
		"topic:p2.public.b>group:g2",
	}
	if got := edgeStrings(g.Edges); !slices.Equal(got, want) {
		t.Fatalf("edges:\n got %v\nwant %v", got, want)
	}
	// None of the cross pairs may exist at all, proven or not.
	for _, bad := range []Edge{
		{From: "pg:public.a", To: "connect:c2"}, {From: "pg:public.b", To: "connect:c1"},
		{From: "topic:p1.public.a", To: "group:g2"}, {From: "topic:p2.public.b", To: "group:g1"},
	} {
		for _, e := range g.Edges {
			if e.From == bad.From && e.To == bad.To {
				t.Errorf("cross-product edge %s -> %s drawn (%+v)", bad.From, bad.To, e)
			}
		}
	}
}

// When a link cannot be proven, owners.yaml's pairing is drawn as inferred,
// and only then: a proven sibling keeps its mapped edges.
func TestBuildEdgesInferredOnlyWhenUnprovable(t *testing.T) {
	cases := []struct {
		name   string
		kafka  *fakeKafka
		connCE map[string]error
		want   []string
	}{
		{
			name: "group lag unreadable",
			kafka: &fakeKafka{
				lags: map[string]clients.GroupLag{"g1": lagOn("p1.public.a")},
				err:  map[string]error{"group:g2": errors.New("kafka down")},
			},
			want: []string{
				"topic:p1.public.a>group:g1",
				"topic:p1.public.a>group:g2 ~",
				"topic:p2.public.b>group:g2 ~",
			},
		},
		{
			name:  "group has no offsets on any pipeline topic",
			kafka: &fakeKafka{lags: map[string]clients.GroupLag{"g1": lagOn("p1.public.a"), "g2": lagOn("elsewhere")}},
			want: []string{
				"topic:p1.public.a>group:g1",
				"topic:p1.public.a>group:g2 ~",
				"topic:p2.public.b>group:g2 ~",
			},
		},
		{
			name:  "both groups proven",
			kafka: &fakeKafka{lags: map[string]clients.GroupLag{"g1": lagOn("p1.public.a"), "g2": lagOn("p1.public.a", "p2.public.b")}},
			want: []string{
				"topic:p1.public.a>group:g1",
				"topic:p1.public.a>group:g2",
				"topic:p2.public.b>group:g2",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := buildWith(t, twoLaneOwners(), twoLaneConnect(), tc.kafka)
			var got []string
			for _, s := range edgeStrings(g.Edges) {
				if strings.Contains(s, ">group:") {
					got = append(got, s)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("topic -> group edges:\n got %v\nwant %v", got, tc.want)
			}
		})
	}
}

// A connector whose config cannot be read keeps the old shape (every table
// in, placeholder topics out) but every one of those edges is inferred; the
// readable connector keeps its proven edges.
func TestBuildEdgesUnreadableConnectorIsInferred(t *testing.T) {
	c := twoLaneConnect()
	c.configErr = map[string]error{"c2": errors.New("connect down")}
	g := buildWith(t, twoLaneOwners(), c, &fakeKafka{lags: map[string]clients.GroupLag{"g1": lagOn("p1.public.a"), "g2": lagOn("p1.public.a")}})
	got := edgeStrings(g.Edges)
	for _, want := range []string{
		"pg:public.a>connect:c1",
		"pg:public.a>connect:c2 ~",
		"pg:public.b>connect:c2 ~",
		"connect:c1>topic:p1.public.a",
		"connect:c2>topic:public.a ~",
		"connect:c2>topic:public.b ~",
	} {
		if !slices.Contains(got, want) {
			t.Errorf("missing edge %q in %v", want, got)
		}
	}
	if slices.Contains(got, "pg:public.b>connect:c1") || slices.Contains(got, "pg:public.b>connect:c1 ~") {
		t.Errorf("c1 does not stream public.b: %v", got)
	}
}

func sinkOwners() *owners.Owners {
	o := testOwners()
	// The sink is listed first on purpose: it must not lend public.orders a slot.
	o.Pipelines[0].Connectors = []string{"orders-sink", "pg-orders-connector"}
	return o
}

func TestBuildSinkConnectorEdges(t *testing.T) {
	src := map[string]string{"topic.prefix": "pg", "table.include.list": "public.orders", "slot.name": "real_slot"}
	cases := []struct {
		name string
		sink map[string]string
		want []string
	}{
		{
			name: "topics list",
			sink: map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics": "pg.public.orders"},
			want: []string{"connect:orders-sink>wh:analytics.orders ~", "topic:pg.public.orders>connect:orders-sink"},
		},
		{
			name: "topics regex",
			sink: map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics.regex": `pg\.public\..*`},
			want: []string{"connect:orders-sink>wh:analytics.orders ~", "topic:pg.public.orders>connect:orders-sink"},
		},
		{
			name: "topics regex names another topic",
			sink: map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics.regex": `other\..*`},
			want: []string{"connect:orders-sink>wh:analytics.orders ~"},
		},
		{
			name: "no topics key",
			sink: map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"},
			want: []string{"connect:orders-sink>wh:analytics.orders ~", "topic:pg.public.orders>connect:orders-sink ~"},
		},
		{
			name: "regex Go cannot compile",
			sink: map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics.regex": `pg(?=x)`},
			want: []string{"connect:orders-sink>wh:analytics.orders ~", "topic:pg.public.orders>connect:orders-sink ~"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeConnect{
				config: map[string]map[string]string{"orders-sink": tc.sink, "pg-orders-connector": src},
				status: map[string]clients.ConnectorStatus{"orders-sink": {State: "RUNNING"}, "pg-orders-connector": {State: "RUNNING"}},
			}
			b := newBuilder(sinkOwners(), c, &fakeKafka{lags: map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")}},
				&fakeSource{slots: []clients.ReplicationSlot{{Name: "real_slot", Active: true}, {Name: "debezium", Active: false}}}, &fakeWarehouse{}, nil)
			g, err := b.Build(context.Background(), "orders")
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, s := range edgeStrings(g.Edges) {
				if strings.Contains(s, "orders-sink") {
					got = append(got, s)
				}
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("sink edges:\n got %v\nwant %v", got, tc.want)
			}
			// A sink reads topics, never a source table, and writes no CDC topic.
			for _, n := range g.Nodes {
				if n.Kind == KindTopic && n.Name != "pg.public.orders" {
					t.Errorf("unexpected topic %s from the sink config", n.Name)
				}
			}
			if pg := g.mustNode(t, "pg:public.orders"); pg.Facts["slot_active"] != "true" {
				t.Errorf("source slot came from the sink's default slot name: %+v", pg.Facts)
			}
		})
	}
}

// A custom sink class that does not end in SinkConnector is still a sink when
// Connect's status says so, or when it has the sink-only topics keys.
func TestIsSinkConnector(t *testing.T) {
	cases := []struct {
		name string
		snap connectSnapshot
		want bool
	}{
		{"status type", connectSnapshot{config: map[string]string{"connector.class": "com.example.Writer"}, status: clients.ConnectorStatus{Type: "sink"}}, true},
		{"class suffix", connectSnapshot{config: map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"}}, true},
		{"topics key", connectSnapshot{config: map[string]string{"connector.class": "com.example.Writer", "topics": "a"}}, true},
		{"topics.regex key", connectSnapshot{config: map[string]string{"connector.class": "com.example.Writer", "topics.regex": "a.*"}}, true},
		{"debezium source", connectSnapshot{config: map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "topic.prefix": "pg"}, status: clients.ConnectorStatus{Type: "source"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSinkConnector(tc.snap); got != tc.want {
				t.Fatalf("isSinkConnector = %v, want %v", got, tc.want)
			}
		})
	}
	c := &fakeConnect{
		config: map[string]map[string]string{"orders-sink": {"connector.class": "com.example.Writer"}, "pg-orders-connector": {"topic.prefix": "pg"}},
		status: map[string]clients.ConnectorStatus{"orders-sink": {Type: "sink", State: "RUNNING"}, "pg-orders-connector": {Type: "source", State: "RUNNING"}},
	}
	g := buildWith(t, sinkOwners(), c, &fakeKafka{})
	if got := edgeStrings(g.Edges); slices.Contains(got, "pg:public.orders>connect:orders-sink") || slices.Contains(got, "pg:public.orders>connect:orders-sink ~") {
		t.Fatalf("source table drawn into a sink: %v", got)
	}
}

// Offsets read alongside an error prove nothing.
func TestGroupTopicsIgnoresLagOnError(t *testing.T) {
	topics := []string{"t1", "t2"}
	if got := groupTopics(groupSnapshot{lag: lagOn("t1"), err: errors.New("partial")}, topics); got != nil {
		t.Fatalf("groupTopics with an error = %v, want nil", got)
	}
	if got := groupTopics(groupSnapshot{lag: lagOn("t2", "t2", "other")}, topics); !slices.Equal(got, []string{"t2"}) {
		t.Fatalf("groupTopics = %v, want [t2]", got)
	}
}

func TestBuildRoutedTopics(t *testing.T) {
	cases := []struct {
		name     string
		cfg      map[string]string
		topic    string
		inferred bool
	}{
		{
			name: "regex router",
			cfg: map[string]string{
				"topic.prefix": "pg", "transforms": "route",
				"transforms.route.type":        "org.apache.kafka.connect.transforms.RegexRouter",
				"transforms.route.regex":       `pg\.public\.(.*)`,
				"transforms.route.replacement": "cdc.$1",
			},
			topic: "cdc.orders",
		},
		{
			name: "router with a predicate is not provable",
			cfg: map[string]string{
				"topic.prefix": "pg", "transforms": "route",
				"transforms.route.type":        "org.apache.kafka.connect.transforms.RegexRouter",
				"transforms.route.regex":       `pg\.public\.(.*)`,
				"transforms.route.replacement": "cdc.$1",
				"transforms.route.predicate":   "isOrders",
			},
			topic:    "pg.public.orders",
			inferred: true,
		},
		{
			name: "unknown router class",
			cfg: map[string]string{
				"topic.prefix": "pg", "transforms": "ts",
				"transforms.ts.type": "org.apache.kafka.connect.transforms.TimestampRouter",
			},
			topic:    "pg.public.orders",
			inferred: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeConnect{config: map[string]map[string]string{"pg-orders-connector": tc.cfg}}
			g := buildWith(t, testOwners(), c, &fakeKafka{})
			want := Edge{From: "connect:pg-orders-connector", To: "topic:" + tc.topic}
			if tc.inferred {
				want = inferred(want)
			}
			if !slices.Contains(g.Edges, want) {
				t.Fatalf("edge %+v missing: %v", want, edgeStrings(g.Edges))
			}
			g.mustNode(t, "topic:"+tc.topic)
		})
	}
}

func TestRouteTopics(t *testing.T) {
	logical := map[string]string{
		"transforms":                         "unwrap,merge",
		"transforms.unwrap.type":             "io.debezium.transforms.ExtractNewRecordState",
		"transforms.merge.type":              "io.debezium.transforms.ByLogicalTableRouter",
		"transforms.merge.topic.regex":       `pg\.public\.orders_shard_(\d+)`,
		"transforms.merge.topic.replacement": "pg.public.orders_all",
	}
	cases := []struct {
		name   string
		cfg    map[string]string
		in     []string
		want   []string
		proven bool
	}{
		{"no transforms", map[string]string{}, []string{"a.b"}, []string{"a.b"}, true},
		{"non-routing transform", map[string]string{"transforms": "u", "transforms.u.type": "io.debezium.transforms.ExtractNewRecordState"}, []string{"a.b"}, []string{"a.b"}, true},
		{"logical router merges shards", logical, []string{"pg.public.orders_shard_1", "pg.public.orders_shard_2", "pg.public.items"}, []string{"pg.public.orders_all", "pg.public.items"}, true},
		{
			"regex must match the whole name",
			map[string]string{"transforms": "r", "transforms.r.type": regexRouterClass, "transforms.r.regex": `public`, "transforms.r.replacement": "x"},
			[]string{"pg.public.orders"}, []string{"pg.public.orders"}, true,
		},
		{
			"bad regex",
			map[string]string{"transforms": "r", "transforms.r.type": regexRouterClass, "transforms.r.regex": `(`, "transforms.r.replacement": "x"},
			[]string{"a"}, []string{"a"}, false,
		},
		{
			"bad replacement",
			map[string]string{"transforms": "r", "transforms.r.type": regexRouterClass, "transforms.r.regex": `(a)`, "transforms.r.replacement": "$2"},
			[]string{"a"}, []string{"a"}, false,
		},
		{
			"outbox event router",
			map[string]string{"transforms": "o", "transforms.o.type": "io.debezium.transforms.outbox.EventRouter"},
			[]string{"a"}, []string{"a"}, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, proven := routeTopics(tc.cfg, tc.in)
			if !slices.Equal(got, tc.want) || proven != tc.proven {
				t.Fatalf("routeTopics = %v, %v; want %v, %v", got, proven, tc.want, tc.proven)
			}
		})
	}
}

func TestJavaReplacement(t *testing.T) {
	cases := []struct {
		in      string
		groups  int
		want    string
		wantErr bool
	}{
		{"cdc.$1", 1, "cdc.${1}", false},
		{"$1x", 1, "${1}x", false},
		{"$12", 1, "${1}2", false}, // Java stops at the last digit that names a group
		{"$12", 12, "${12}", false},
		{"${name}.x", 1, "${name}.x", false},
		{`a\$b`, 0, "a$$b", false},
		{`a\\b`, 0, `a\b`, false},
		{"$2", 1, "", true},
		{"a$", 0, "", true},
		{`a\`, 0, "", true},
		{"${open", 1, "", true},
	}
	for _, tc := range cases {
		got, err := javaReplacement(tc.in, tc.groups)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("javaReplacement(%q, %d) = %q, %v; want %q, err %v", tc.in, tc.groups, got, err, tc.want, tc.wantErr)
		}
	}
}

// The legacy single-lane pipeline keeps its four edges, now honest about
// which ones are proven.
func TestBuildSingleLaneMappings(t *testing.T) {
	now := time.Now()
	c := &fakeConnect{
		config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
		status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING"}},
	}
	g := buildWith(t, testOwners(), c, &fakeKafka{
		lags:    map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")},
		offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{LastTimestamp: now}}},
	})
	want := []string{
		"connect:pg-orders-connector>topic:pg.public.orders",
		"group:order-sync>wh:analytics.orders ~",
		"pg:public.orders>connect:pg-orders-connector",
		"topic:pg.public.orders>group:order-sync",
	}
	if got := edgeStrings(g.Edges); !slices.Equal(got, want) {
		t.Fatalf("edges:\n got %v\nwant %v", got, want)
	}
}
