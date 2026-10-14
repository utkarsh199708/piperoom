package topology

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

const dlqLayers = `
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

// dlqPipelineYAML has a source connector, a sink connector and a consumer
// group; %s is extra pipeline-level YAML (a watch block).
const dlqPipelineYAML = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, pg-orders-sink]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
%s
` + dlqLayers

const sinkClass = "io.confluent.connect.jdbc.JdbcSinkConnector"

var dlqNow = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

// dlqFixture is one scenario: the sink config and what Kafka says per topic.
type dlqFixture struct {
	watch      string
	sinkCfg    map[string]string
	offsets    map[string][]clients.PartitionOffsets
	topicErr   map[string]error
	sourceCfg  map[string]string
	sinkCfgErr error
}

func (f dlqFixture) build(t *testing.T) Graph {
	t.Helper()
	src := f.sourceCfg
	if src == nil {
		src = map[string]string{"topic.prefix": "pg", "table.include.list": "public.orders", "slot.name": "s"}
	}
	connect := &fakeConnect{
		config: map[string]map[string]string{"pg-orders-connector": src, "pg-orders-sink": f.sinkCfg},
		status: map[string]clients.ConnectorStatus{
			"pg-orders-connector": {State: "RUNNING"},
			"pg-orders-sink":      {State: "RUNNING"},
		},
	}
	if f.sinkCfgErr != nil {
		connect.configErr = map[string]error{"pg-orders-sink": f.sinkCfgErr}
	}
	offs := map[string][]clients.PartitionOffsets{
		"pg.public.orders": {{Partition: 0, Start: 0, End: 10, LastTimestamp: dlqNow.Add(-12 * time.Second)}},
	}
	for k, v := range f.offsets {
		offs[k] = v
	}
	kerr := map[string]error{}
	for k, v := range f.topicErr {
		kerr["topic:"+k] = v
	}
	b := newBuilder(
		loadOwners(t, fmt.Sprintf(dlqPipelineYAML, f.watch)),
		connect,
		&fakeKafka{
			lags:    map[string]clients.GroupLag{"order-sync": {State: "Stable"}},
			offsets: offs,
			err:     kerr,
		},
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "s", Active: true, RetainedWALBytes: 1153434}}},
		&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": dlqNow.Add(-time.Minute)}},
		nil,
	)
	b.now = func() time.Time { return dlqNow }
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return g
}

func (g Graph) node(id string) (Node, bool) {
	for _, n := range g.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

func (g Graph) mustNode(t *testing.T, id string) Node {
	t.Helper()
	n, ok := g.node(id)
	if !ok {
		t.Fatalf("node %s missing; nodes: %+v", id, g.Nodes)
	}
	return n
}

func sinkCfg(extra map[string]string) map[string]string {
	c := map[string]string{
		"connector.class":                               sinkClass,
		"errors.tolerance":                              "all",
		"errors.deadletterqueue.topic.name":             "orders-dlq",
		"errors.log.enable":                             "true",
		"errors.deadletterqueue.context.headers.enable": "true",
	}
	for k, v := range extra {
		if v == "" {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}

func dlqOffsets(records int64, age time.Duration) []clients.PartitionOffsets {
	return []clients.PartitionOffsets{
		{Partition: 0, Start: 10, End: 10 + records/2, LastTimestamp: dlqNow.Add(-age - time.Hour)},
		{Partition: 1, Start: 0, End: records - records/2, LastTimestamp: dlqNow.Add(-age)},
	}
}

func TestDLQNodeDiscoveredFromSinkConfig(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{
		sinkCfg: sinkCfg(nil),
		offsets: map[string][]clients.PartitionOffsets{"orders-dlq": dlqOffsets(1204, 3*time.Minute)},
	})
	n := g.mustNode(t, "dlq:orders-dlq")
	if n.Kind != KindDLQTopic || n.Name != "orders-dlq" || n.Layer != "kafka-connect" || n.Owner != "arjun" {
		t.Fatalf("node = %+v", n)
	}
	if n.Health != HealthWarn {
		t.Fatalf("health = %s, want warn (newest record 3m old)", n.Health)
	}
	want := map[string]string{"records": "1204", "newest_record": "3m ago", "recent": "true", "writer": "pg-orders-sink"}
	if !mapsEqual(n.Facts, want) {
		t.Fatalf("facts = %v, want %v", n.Facts, want)
	}
	// Flow order: DLQs sit after topics and before groups.
	var ids []string
	for _, x := range g.Nodes {
		ids = append(ids, x.ID)
	}
	wantIDs := []string{"pg:public.orders", "connect:pg-orders-connector", "connect:pg-orders-sink", "topic:pg.public.orders", "dlq:orders-dlq", "group:order-sync", "wh:analytics.orders"}
	if !slices.Equal(ids, wantIDs) {
		t.Fatalf("node order = %v, want %v", ids, wantIDs)
	}
	// Side edge only: into the DLQ, never out.
	into, out := 0, 0
	for _, e := range g.Edges {
		if e.To == "dlq:orders-dlq" {
			into++
			if e.From != "connect:pg-orders-sink" {
				t.Errorf("edge into DLQ from %s", e.From)
			}
		}
		if e.From == "dlq:orders-dlq" {
			out++
		}
	}
	if into != 1 || out != 0 {
		t.Fatalf("edges into=%d out=%d: %v", into, out, g.Edges)
	}
}

func fixtureGraph(t *testing.T, f dlqFixture) Graph { t.Helper(); return f.build(t) }

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestDLQDeclaredConsumerWriter(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{
		sinkCfg: sinkCfg(map[string]string{"errors.deadletterqueue.topic.name": ""}),
		watch:   "    watch:\n      dlq_topics:\n        - {topic: order-sync-dlq, written_by: order-sync}\n",
		offsets: map[string][]clients.PartitionOffsets{"order-sync-dlq": dlqOffsets(4, 5*time.Hour)},
	})
	n := g.mustNode(t, "dlq:order-sync-dlq")
	if n.Layer != "consumer" || n.Owner != "kabir" {
		t.Fatalf("layer/owner = %s/%s, want consumer/kabir", n.Layer, n.Owner)
	}
	if n.Health != HealthOK || n.Facts["recent"] != "" || n.Facts["writer"] != "order-sync" || n.Facts["records"] != "4" {
		t.Fatalf("node = %+v", n)
	}
	if !slices.Contains(g.Edges, Edge{From: "group:order-sync", To: "dlq:order-sync-dlq"}) {
		t.Fatalf("edge from the writing group missing: %v", g.Edges)
	}
}

func TestDLQDeclaredAndConfiguredMergeIntoOneNode(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{
		sinkCfg: sinkCfg(nil),
		watch:   "    watch:\n      dlq_topics:\n        - {topic: orders-dlq, written_by: order-sync}\n",
		offsets: map[string][]clients.PartitionOffsets{"orders-dlq": dlqOffsets(2, 5*time.Hour)},
	})
	n := 0
	for _, x := range g.Nodes {
		if x.ID == "dlq:orders-dlq" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("dlq:orders-dlq appears %d times", n)
	}
	node := g.mustNode(t, "dlq:orders-dlq")
	// A connector writer wins the layer; both writers are named, sorted.
	if node.Layer != "kafka-connect" || node.Facts["writer"] != "order-sync,pg-orders-sink" {
		t.Fatalf("node = %+v", node)
	}
	for _, from := range []string{"connect:pg-orders-sink", "group:order-sync"} {
		if !slices.Contains(g.Edges, Edge{From: from, To: "dlq:orders-dlq"}) {
			t.Errorf("edge from %s missing: %v", from, g.Edges)
		}
	}
}

func TestDLQDeclaredConnectorWriterWithUnreadableConfig(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{
		sinkCfgErr: errors.New("connect down"),
		watch:      "    watch:\n      dlq_topics:\n        - {topic: orders-dlq, written_by: pg-orders-sink}\n",
		offsets:    map[string][]clients.PartitionOffsets{"orders-dlq": dlqOffsets(2, 5*time.Hour)},
	})
	n := g.mustNode(t, "dlq:orders-dlq")
	if n.Layer != "kafka-connect" || n.Health != HealthOK {
		t.Fatalf("node = %+v", n)
	}
}

func TestDLQNoNodeWhenConfigReadFailed(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{sinkCfgErr: errors.New("connect down")})
	for _, n := range g.Nodes {
		if n.Kind == KindDLQTopic {
			t.Fatalf("unexpected DLQ node %+v", n)
		}
	}
}

func TestDLQNoNodeWithoutDLQ(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{sinkCfg: sinkCfg(map[string]string{"errors.deadletterqueue.topic.name": ""})})
	for _, n := range g.Nodes {
		if n.Kind == KindDLQTopic {
			t.Fatalf("unexpected DLQ node %+v", n)
		}
	}
}

func TestDLQHealthTable(t *testing.T) {
	cases := []struct {
		name       string
		watch      string
		declared   bool // topic declared in owners.yaml only (no sink config)
		offsets    []clients.PartitionOffsets
		err        error
		wantHealth Health
		wantFacts  map[string]string
	}{
		{
			name: "missing, sink-configured: warn", err: clients.ErrNotFound,
			wantHealth: HealthWarn,
			wantFacts:  map[string]string{"records": "0", "exists": "false", "writer": "pg-orders-sink"},
		},
		{
			name: "missing, declared only: ok", declared: true, err: clients.ErrNotFound,
			watch:      "    watch:\n      dlq_topics:\n        - {topic: orders-dlq, written_by: order-sync}\n",
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"records": "0", "exists": "false", "writer": "order-sync"},
		},
		{
			name: "old records: ok", offsets: dlqOffsets(10, 2*time.Hour),
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"records": "10", "newest_record": "2.0h ago", "writer": "pg-orders-sink"},
		},
		{
			name: "young records: warn", offsets: dlqOffsets(10, 59*time.Minute),
			wantHealth: HealthWarn,
			wantFacts:  map[string]string{"records": "10", "newest_record": "59m ago", "recent": "true", "writer": "pg-orders-sink"},
		},
		{
			name: "exactly the window: ok", offsets: dlqOffsets(10, time.Hour),
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"records": "10", "newest_record": "1.0h ago", "writer": "pg-orders-sink"},
		},
		{
			name:       "window from watch.dlq_growth",
			watch:      "    watch:\n      signals:\n        dlq_growth: {window: 3h}\n",
			offsets:    dlqOffsets(10, 2*time.Hour),
			wantHealth: HealthWarn,
			wantFacts:  map[string]string{"records": "10", "newest_record": "2.0h ago", "recent": "true", "writer": "pg-orders-sink"},
		},
		{
			name: "empty topic: ok, never", offsets: []clients.PartitionOffsets{{Partition: 0, Start: 5, End: 5}},
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"records": "0", "newest_record": "never", "writer": "pg-orders-sink"},
		},
		{
			name: "kafka unreachable: unknown", err: errors.New("broker down"),
			wantHealth: HealthUnknown,
			wantFacts:  map[string]string{"error": "unreachable"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := dlqFixture{
				sinkCfg: sinkCfg(nil),
				watch:   tc.watch,
				offsets: map[string][]clients.PartitionOffsets{"orders-dlq": tc.offsets},
			}
			if tc.declared {
				f.sinkCfg = sinkCfg(map[string]string{"errors.deadletterqueue.topic.name": ""})
			}
			if tc.err != nil {
				f.topicErr = map[string]error{"orders-dlq": tc.err}
			}
			n := fixtureGraph(t, f).mustNode(t, "dlq:orders-dlq")
			if n.Health != tc.wantHealth || !mapsEqual(n.Facts, tc.wantFacts) {
				t.Fatalf("health=%s facts=%v, want %s %v", n.Health, n.Facts, tc.wantHealth, tc.wantFacts)
			}
		})
	}
}

func TestDLQRecordsSumEndMinusStart(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{
		sinkCfg: sinkCfg(nil),
		offsets: map[string][]clients.PartitionOffsets{"orders-dlq": {
			{Partition: 0, Start: 100, End: 130, LastTimestamp: dlqNow.Add(-5 * time.Hour)},
			{Partition: 1, Start: 7, End: 9, LastTimestamp: dlqNow.Add(-6 * time.Hour)},
			{Partition: 2, Start: 50, End: 50},
		}},
	})
	if got := g.mustNode(t, "dlq:orders-dlq").Facts["records"]; got != "32" {
		t.Fatalf("records = %s, want 32", got)
	}
}

func TestThresholdsForMapsDLQWindow(t *testing.T) {
	o := loadOwners(t, fmt.Sprintf(pipelineYAML, "    watch:\n      signals:\n        dlq_growth: {window: 90m}\n"))
	p, _ := o.Pipeline("orders")
	got := ThresholdsFor(DefaultThresholds(), p)
	if got.DLQRecentWarn != 90*time.Minute {
		t.Fatalf("DLQRecentWarn = %v, want 90m", got.DLQRecentWarn)
	}
	if DefaultThresholds().DLQRecentWarn != time.Hour {
		t.Fatalf("base default changed")
	}
}

func TestConnectorDLQFacts(t *testing.T) {
	cases := []struct {
		name       string
		cfg        map[string]string
		topicErr   error
		wantHealth Health
		wantFacts  map[string]string
	}{
		{
			name:       "healthy DLQ sink",
			cfg:        sinkCfg(nil),
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"errors_tolerance": "all", "dlq": "orders-dlq"},
		},
		{
			name:       "silent drop turns the connector to warn",
			cfg:        sinkCfg(map[string]string{"errors.deadletterqueue.topic.name": "", "errors.log.enable": ""}),
			wantHealth: HealthWarn,
			wantFacts:  map[string]string{"errors_tolerance": "all", "dlq": "none", "config_risk": "silent_drop_sink"},
		},
		{
			name:       "sev3 and info risks keep ok",
			cfg:        sinkCfg(map[string]string{"errors.log.enable": "", "errors.deadletterqueue.context.headers.enable": ""}),
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"errors_tolerance": "all", "dlq": "orders-dlq", "config_risk": "dlq_no_log,dlq_no_headers"},
		},
		{
			name:       "missing DLQ topic is a risk",
			cfg:        sinkCfg(nil),
			topicErr:   clients.ErrNotFound,
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"errors_tolerance": "all", "dlq": "orders-dlq", "config_risk": "dlq_topic_missing"},
		},
		{
			name:       "kafka unreachable: no existence risk",
			cfg:        sinkCfg(nil),
			topicErr:   errors.New("down"),
			wantHealth: HealthOK,
			wantFacts:  map[string]string{"errors_tolerance": "all", "dlq": "orders-dlq"},
		},
		{
			name:       "default sink config adds no facts",
			cfg:        map[string]string{"connector.class": sinkClass},
			wantHealth: HealthOK,
			wantFacts:  map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := dlqFixture{sinkCfg: tc.cfg}
			if tc.topicErr != nil {
				f.topicErr = map[string]error{"orders-dlq": tc.topicErr}
			}
			n := fixtureGraph(t, f).mustNode(t, "connect:pg-orders-sink")
			if n.Health != tc.wantHealth || !mapsEqual(n.Facts, tc.wantFacts) {
				t.Fatalf("health=%s facts=%v, want %s %v", n.Health, n.Facts, tc.wantHealth, tc.wantFacts)
			}
		})
	}
}

func TestConnectorDLQFactsKeepFailedHealth(t *testing.T) {
	b := &builder{}
	n := Node{Health: HealthFail, Facts: map[string]string{"tasks_failed": "1"}}
	cfg := sinkCfg(map[string]string{"errors.deadletterqueue.topic.name": "", "errors.log.enable": ""})
	b.addConnectorDLQFacts(&n, connectSnapshot{config: cfg}, &collected{topicErr: map[string]error{}})
	if n.Health != HealthFail || n.Facts["tasks_failed"] != "1" || n.Facts["config_risk"] != "silent_drop_sink" {
		t.Fatalf("node = %+v", n)
	}
}

// Facts carry the DLQ topic name and risk codes only. Every other config value
// is a sentinel and must not show up anywhere in the graph or its rendering.
func TestDLQFactsNeverCarryConfigValues(t *testing.T) {
	const s = "SENTINEL-SECRET"
	cfg := sinkCfg(map[string]string{
		"errors.log.enable":           "",
		"errors.log.include.messages": "true",
		"connection.password":         s + "-pw",
		"connection.url":              "jdbc:postgresql://" + s + "/db",
		"transforms":                  s,
	})
	f := dlqFixture{sinkCfg: cfg, offsets: map[string][]clients.PartitionOffsets{"orders-dlq": dlqOffsets(2, 5*time.Hour)}}
	g := fixtureGraph(t, f)
	var render string
	{
		b := newBuilder(nil, nil, nil, nil, nil, nil)
		render = b.Render(g)
	}
	if strings.Contains(render, s) {
		t.Fatalf("render leaks a config value:\n%s", render)
	}
	for _, n := range g.Nodes {
		for k, v := range n.Facts {
			if strings.Contains(k, s) || strings.Contains(v, s) {
				t.Fatalf("fact %s=%s on %s leaks a config value", k, v, n.ID)
			}
		}
	}
}

func TestDLQGoldenRender(t *testing.T) {
	g := fixtureGraph(t, dlqFixture{
		sinkCfg: sinkCfg(map[string]string{"errors.log.enable": ""}),
		offsets: map[string][]clients.PartitionOffsets{"orders-dlq": dlqOffsets(1204, 3*time.Minute)},
	})
	got := newBuilder(nil, nil, nil, nil, nil, nil).Render(g)
	want, err := os.ReadFile("testdata/dlq_render.golden")
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("Render mismatch.\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}

func TestDLQBuildIsDeterministic(t *testing.T) {
	f := dlqFixture{
		sinkCfg: sinkCfg(nil),
		watch:   "    watch:\n      dlq_topics:\n        - {topic: a-dlq, written_by: order-sync}\n        - {topic: b-dlq, written_by: pg-orders-connector}\n",
		offsets: map[string][]clients.PartitionOffsets{"orders-dlq": dlqOffsets(2, 5*time.Hour)},
	}
	first := fixtureGraph(t, f)
	for i := 0; i < 5; i++ {
		again := fixtureGraph(t, f)
		if fmt.Sprint(first) != fmt.Sprint(again) {
			t.Fatalf("graph changed between builds:\n%v\n%v", first, again)
		}
	}
	// Declared first (file order), then discovered.
	var dlqs []string
	for _, n := range first.Nodes {
		if n.Kind == KindDLQTopic {
			dlqs = append(dlqs, n.Name)
		}
	}
	if !slices.Equal(dlqs, []string{"a-dlq", "b-dlq", "orders-dlq"}) {
		t.Fatalf("DLQ order = %v", dlqs)
	}
}
