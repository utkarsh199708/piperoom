package topology

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// ---- fakes ----

type fakeConnect struct {
	config    map[string]map[string]string
	status    map[string]clients.ConnectorStatus
	configErr map[string]error
	statusErr map[string]error
}

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) { return nil, nil }
func (f *fakeConnect) RestartTask(context.Context, string, int) error   { return nil }
func (f *fakeConnect) RestartConnector(context.Context, string) error   { return nil }
func (f *fakeConnect) Config(_ context.Context, name string) (map[string]string, error) {
	if f.configErr != nil {
		if err := f.configErr[name]; err != nil {
			return nil, err
		}
	}
	return f.config[name], nil
}
func (f *fakeConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	if f.statusErr != nil {
		if err := f.statusErr[name]; err != nil {
			return clients.ConnectorStatus{}, err
		}
	}
	return f.status[name], nil
}

type fakeKafka struct {
	lags      map[string]clients.GroupLag
	offsets   map[string][]clients.PartitionOffsets
	findByKey map[string][]clients.RecordRef
	err       map[string]error
}

func (f *fakeKafka) Topics(context.Context) ([]string, error)         { return nil, nil }
func (f *fakeKafka) ConsumerGroups(context.Context) ([]string, error) { return nil, nil }
func (f *fakeKafka) GroupLag(_ context.Context, group string) (clients.GroupLag, error) {
	if f.err != nil {
		if err := f.err["group:"+group]; err != nil {
			return clients.GroupLag{}, err
		}
	}
	return f.lags[group], nil
}
func (f *fakeKafka) TopicOffsets(_ context.Context, topic string) ([]clients.PartitionOffsets, error) {
	if f.err != nil {
		if err := f.err["topic:"+topic]; err != nil {
			return nil, err
		}
	}
	return f.offsets[topic], nil
}
func (f *fakeKafka) FindByKey(_ context.Context, topic, key string, _ time.Time, _ int) ([]clients.RecordRef, error) {
	if f.err != nil {
		if err := f.err["find:"+topic]; err != nil {
			return nil, err
		}
	}
	var out []clients.RecordRef
	for _, r := range f.findByKey[topic] {
		if r.Key == key {
			out = append(out, r)
		}
	}
	return out, nil
}

type fakeSource struct {
	slots     []clients.ReplicationSlot
	slotsErr  error
	rows      map[string]map[string]any
	rowErr    error
	pk        map[string]string
	recentDDL []clients.DDLChange
	recentErr error
	longTxns  []clients.Transaction
	longErr   error
}

func (f *fakeSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return f.slots, f.slotsErr
}
func (f *fakeSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return f.recentDDL, f.recentErr
}
func (f *fakeSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return f.longTxns, f.longErr
}
func (f *fakeSource) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	if f.rowErr != nil {
		return nil, false, f.rowErr
	}
	row, ok := f.rows[table+"|"+key]
	return row, ok, nil
}
func (f *fakeSource) PrimaryKeyColumn(_ context.Context, _ string) (string, error) {
	return "id", nil
}

type fakeWarehouse struct {
	rows     map[string]map[string]any
	fresh    map[string]time.Time
	freshErr map[string]error
}

func (f *fakeWarehouse) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	row, ok := f.rows[table+"|"+key]
	return row, ok, nil
}
func (f *fakeWarehouse) Freshness(_ context.Context, table, _ string) (time.Time, error) {
	if f.freshErr != nil {
		if err := f.freshErr[table]; err != nil {
			return time.Time{}, err
		}
	}
	return f.fresh[table], nil
}

func testOwners() *owners.Owners {
	return &owners.Owners{
		Pipelines: []owners.Pipeline{{
			ID:             "orders",
			Title:          "Orders CDC",
			SourceTables:   []string{"public.orders"},
			Connectors:     []string{"pg-orders-connector"},
			ConsumerGroups: []string{"order-sync"},
			SinkTables:     []string{"analytics.orders"},
			SinkTSColumn:   "updated_at",
		}},
		Layers: map[string]string{
			"postgres":        "priya",
			"kafka-connect":   "arjun",
			"schema-registry": "priya",
			"kafka":           "arjun",
			"consumer":        "kabir",
			"warehouse":       "meera",
		},
	}
}

// ---- health rules ----

func TestSourceHealth(t *testing.T) {
	th := DefaultThresholds()
	cases := []struct {
		name string
		slot clients.ReplicationSlot
		want Health
	}{
		{"healthy", clients.ReplicationSlot{Name: "s", Active: true, RetainedWALBytes: 100}, HealthOK},
		{"inactive", clients.ReplicationSlot{Name: "s", Active: false}, HealthWarn},
		{"retained over", clients.ReplicationSlot{Name: "s", Active: true, RetainedWALBytes: 2 << 30}, HealthWarn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, facts := sourceHealth(tc.slot, th)
			if got != tc.want {
				t.Fatalf("health = %s, want %s (facts %v)", got, tc.want, facts)
			}
			if facts["slot_active"] == "" || facts["slot_retained"] == "" {
				t.Fatalf("missing facts: %v", facts)
			}
		})
	}
}

func TestConnectorHealth(t *testing.T) {
	cases := []struct {
		name string
		st   clients.ConnectorStatus
		want Health
	}{
		{"running", clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}, HealthOK},
		{"failed task", clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{State: "FAILED"}}}, HealthFail},
		{"failed connector", clients.ConnectorStatus{State: "FAILED"}, HealthFail},
		{"paused", clients.ConnectorStatus{State: "PAUSED"}, HealthWarn},
		{"unassigned", clients.ConnectorStatus{State: "UNASSIGNED"}, HealthWarn},
		{"task paused", clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{State: "PAUSED"}}}, HealthWarn},
		{"task unassigned", clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{State: "UNASSIGNED"}}}, HealthWarn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := connectorHealth(tc.st); got != tc.want {
				t.Fatalf("health = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestGroupHealth(t *testing.T) {
	th := DefaultThresholds()
	cases := []struct {
		name string
		lag  clients.GroupLag
		want Health
	}{
		{"ok", clients.GroupLag{State: "Stable", TotalLag: 10}, HealthOK},
		{"empty", clients.GroupLag{State: "Empty", TotalLag: 0}, HealthWarn},
		{"lag", clients.GroupLag{State: "Stable", TotalLag: 5000}, HealthWarn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := groupHealth(tc.lag, th); got != tc.want {
				t.Fatalf("health = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestTopicHealth(t *testing.T) {
	now := time.Now()
	if got, facts := topicHealth(nil, now.Add(-2*time.Minute), now); got != HealthOK || !strings.Contains(facts["newest_record"], "ago") {
		t.Fatalf("ok topic: %s %v", got, facts)
	}
	if got, _ := topicHealth(errors.New("down"), time.Time{}, now); got != HealthUnknown {
		t.Fatalf("unreachable topic: %s", got)
	}
	if got, facts := topicHealth(nil, time.Time{}, now); got != HealthOK || facts["newest_record"] != "never" {
		t.Fatalf("quiet topic: %s %v", got, facts)
	}
}

func TestSinkHealth(t *testing.T) {
	th := DefaultThresholds()
	now := time.Now()
	cases := []struct {
		name string
		ts   time.Time
		want Health
	}{
		{"fresh", now.Add(-time.Minute), HealthOK},
		{"warn", now.Add(-11 * time.Minute), HealthWarn},
		{"fail", now.Add(-31 * time.Minute), HealthFail},
		{"empty", time.Time{}, HealthWarn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := sinkHealth(tc.ts, now, th); got != tc.want {
				t.Fatalf("health = %s, want %s", got, tc.want)
			}
		})
	}
}

// ---- topic derivation ----

func TestDeriveTopics(t *testing.T) {
	cases := []struct {
		name   string
		cfg    map[string]string
		tables []string
		want   []string
	}{
		{"single", map[string]string{"topic.prefix": "pg", "table.include.list": "public.orders"}, []string{"public.orders"}, []string{"pg.public.orders"}},
		{"empty include means all", map[string]string{"topic.prefix": "pg"}, []string{"public.orders", "public.items"}, []string{"pg.public.orders", "pg.public.items"}},
		{"filtered", map[string]string{"topic.prefix": "pg", "table.include.list": "public.orders"}, []string{"public.orders", "public.items"}, []string{"pg.public.orders"}},
		{"no prefix", map[string]string{"table.include.list": "public.orders"}, []string{"public.orders"}, nil},
		{
			"regex include",
			map[string]string{"topic.prefix": "pg", "table.include.list": "public.order.*"},
			[]string{"public.orders", "public.order_items", "public.items"},
			[]string{"pg.public.orders", "pg.public.order_items"},
		},
		{
			"regex include with exclude",
			map[string]string{"topic.prefix": "pg", "table.include.list": `public\..*`, "table.exclude.list": `public\.order_items`},
			[]string{"public.orders", "public.order_items"},
			[]string{"pg.public.orders"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveTopics(tc.cfg, tc.tables)
			if len(got) != len(tc.want) {
				t.Fatalf("topics = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("topics = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// ---- Debezium include/exclude lists (regex, anchored to the whole id) ----

func TestStreamsTableLists(t *testing.T) {
	cases := []struct {
		name  string
		cfg   map[string]string
		table string
		want  bool
	}{
		// Literal fast path.
		{"literal entry", map[string]string{"table.include.list": "public.orders"}, "public.orders", true},
		{"literal entry other table", map[string]string{"table.include.list": "public.orders"}, "public.items", false},
		{"empty include streams all", map[string]string{}, "public.orders", true},
		{"no list keys at all", nil, "public.orders", true},
		// Regex entries.
		{"regex star matches base", map[string]string{"table.include.list": "public.order.*"}, "public.orders", true},
		{"regex star matches suffix", map[string]string{"table.include.list": "public.order.*"}, "public.order_items", true},
		{"regex star filters others", map[string]string{"table.include.list": "public.order.*"}, "public.items", false},
		{"regex anchored to whole id", map[string]string{"table.include.list": "orders"}, "public.orders", false},
		{"escaped dot is exact", map[string]string{"table.include.list": `public\.orders`}, "publicXorders", false},
		{"escaped dot matches", map[string]string{"table.include.list": `public\.orders`}, "public.orders", true},
		// Debezium treats an unescaped dot as a regex wildcard.
		{"unescaped dot is wildcard", map[string]string{"table.include.list": "public.orders"}, "publicXorders", true},
		{"schema wildcard", map[string]string{"table.include.list": `public\..*`}, "public.orders", true},
		{"schema wildcard other schema", map[string]string{"table.include.list": `public\..*`}, "sales.orders", false},
		{"alternation anchored", map[string]string{"table.include.list": `orders|items`}, "public.orders", false},
		{"alternation entry", map[string]string{"table.include.list": `orders|items`}, "items", true},
		{"alternation with escaped dots", map[string]string{"table.include.list": `public\.orders|public\.items`}, "public.items", true},
		// Exclude lists.
		{"exclude wins over include", map[string]string{"table.include.list": "public.*", "table.exclude.list": "public.order_items"}, "public.order_items", false},
		{"exclude keeps the rest", map[string]string{"table.include.list": "public.*", "table.exclude.list": "public.order_items"}, "public.orders", true},
		{"exclude alone filters", map[string]string{"table.exclude.list": `public\.orders`}, "public.orders", false},
		{"exclude alone keeps others", map[string]string{"table.exclude.list": `public\.orders`}, "public.items", true},
		// Invalid regex entries fall back to literal comparison.
		{"invalid regex matches literally", map[string]string{"table.include.list": "public.["}, "public.[", true},
		{"invalid regex is not a wildcard", map[string]string{"table.include.list": "public.["}, "public.orders", false},
		{"invalid exclude entry", map[string]string{"table.include.list": "public.*", "table.exclude.list": "public.["}, "public.[", false},
		// Several comma-separated entries, mixed literal and regex.
		{"several mixed entries literal", map[string]string{"table.include.list": `public.orders, public.order_items,public.events_.*`}, "public.orders", true},
		{"several mixed entries regex", map[string]string{"table.include.list": `public.orders, public.order_items,public.events_.*`}, "public.events_2026", true},
		{"several mixed entries unlisted", map[string]string{"table.include.list": `public.orders, public.order_items,public.events_.*`}, "public.users", false},
		// Schema-level lists match the schema part only.
		{"schema include", map[string]string{"schema.include.list": "public"}, "public.orders", true},
		{"schema include filters other schema", map[string]string{"schema.include.list": "public"}, "sales.orders", false},
		{"schema include regex", map[string]string{"schema.include.list": "pub.*"}, "public.orders", true},
		{"schema exclude wins", map[string]string{"schema.include.list": "public|sales", "schema.exclude.list": "sales"}, "sales.orders", false},
		{"schema and table lists combine schema", map[string]string{"schema.include.list": "public", "table.include.list": `public\.orders`}, "sales.orders", false},
		{"schema and table lists combine table", map[string]string{"schema.include.list": "public", "table.include.list": `public\.orders`}, "public.items", false},
		{"schema and table lists combine match", map[string]string{"schema.include.list": "public", "table.include.list": `public\.orders`}, "public.orders", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := streamsTable(tc.cfg, tc.table); got != tc.want {
				t.Fatalf("streamsTable(%q) = %v, want %v", tc.table, got, tc.want)
			}
		})
	}
}

func TestInvalidListEntryWarnsOnce(t *testing.T) {
	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// Unique entry so the process-wide log-once cache cannot collide with
	// other tests.
	cfg := map[string]string{"table.include.list": "logonce_test_entry.["}
	want := []bool{true, true, true}
	for i := range want {
		if got := streamsTable(cfg, "logonce_test_entry.["); got != want[i] {
			t.Fatalf("call %d: streamsTable = %v, want %v (literal fallback)", i, got, want[i])
		}
	}
	if n := strings.Count(buf.String(), "logonce_test_entry.["); n != 1 {
		t.Fatalf("bad entry logged %d times, want 1:\n%s", n, buf.String())
	}
}

// ---- Render ----

func TestRender(t *testing.T) {
	g := Graph{
		PipelineID: "orders",
		Nodes: []Node{
			{ID: "pg:public.orders", Kind: KindSourceTable, Name: "public.orders", Layer: "postgres", Owner: "priya", Health: HealthWarn, Facts: map[string]string{"slot_retained": "1.3 GiB", "slot_active": "false"}},
			{ID: "connect:c1", Kind: KindConnector, Name: "c1", Layer: "kafka-connect", Owner: "arjun", Health: HealthFail, Facts: map[string]string{"tasks_failed": "1"}},
		},
	}
	b := NewBuilder(testOwners(), nil, nil, nil, nil)
	got := b.Render(g)
	want := "pipeline orders\n1 pg:public.orders [postgres, owner priya] WARN slot_active=false slot_retained=1.3 GiB\n2 connect:c1 [kafka-connect, owner arjun] FAIL tasks_failed=1\n"
	if got != want {
		t.Fatalf("Render mismatch:\ngot:  %q\nwant: %q", got, want)
	}
}

// ---- Build with fakes ----

func TestBuild(t *testing.T) {
	now := time.Now()
	b := NewBuilder(
		testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders", "slot.name": "pg-orders-connector"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
		},
		&fakeKafka{
			lags:    map[string]clients.GroupLag{"order-sync": {State: "Stable", TotalLag: 0}},
			offsets: map[string][]clients.PartitionOffsets{"pg.public.orders": {{Partition: 0, LastTimestamp: now.Add(-time.Minute)}}},
		},
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg-orders-connector", Active: true, RetainedWALBytes: 10}}},
		&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": now.Add(-time.Minute)}},
	)

	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(g.Nodes) != 5 {
		t.Fatalf("nodes = %d, want 5: %+v", len(g.Nodes), g.Nodes)
	}
	for _, n := range g.Nodes {
		if n.Health == HealthUnknown {
			t.Fatalf("unexpected unknown node: %+v", n)
		}
	}
	// Topic derived from connector config.
	found := false
	for _, n := range g.Nodes {
		if n.Kind == KindTopic && n.Name == "pg.public.orders" {
			found = true
		}
	}
	if !found {
		t.Fatalf("topic node missing: %+v", g.Nodes)
	}
	// Flow order: table, connector, topic, group, sink.
	wantKinds := []NodeKind{KindSourceTable, KindConnector, KindTopic, KindConsumerGroup, KindSinkTable}
	for i, k := range wantKinds {
		if g.Nodes[i].Kind != k {
			t.Fatalf("node %d kind = %s, want %s", i, g.Nodes[i].Kind, k)
		}
	}
	if len(g.Edges) != 4 {
		t.Fatalf("edges = %d, want 4", len(g.Edges))
	}
}

func TestBuildUnknownPipeline(t *testing.T) {
	b := NewBuilder(testOwners(), nil, nil, nil, nil)
	if _, err := b.Build(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for unknown pipeline")
	} else if !errors.Is(err, ErrBadInput) {
		t.Fatalf("error %v does not wrap ErrBadInput", err)
	}
}

// TestBuildRegexIncludeList proves the pipeline map for a connector whose
// table.include.list is a Debezium regex (public.order.*) rather than a list
// of literal table names.
func TestBuildRegexIncludeList(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].SourceTables = []string{"public.orders", "public.order_items"}
	now := time.Now()
	b := NewBuilder(
		o,
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.order.*", "slot.name": "pg-orders-connector"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
		},
		&fakeKafka{
			lags: map[string]clients.GroupLag{"order-sync": {State: "Stable", TotalLag: 0}},
			offsets: map[string][]clients.PartitionOffsets{
				"pg.public.orders":      {{Partition: 0, LastTimestamp: now.Add(-time.Minute)}},
				"pg.public.order_items": {{Partition: 0, LastTimestamp: now.Add(-time.Minute)}},
			},
		},
		&fakeSource{slots: []clients.ReplicationSlot{{Name: "pg-orders-connector", Active: true, RetainedWALBytes: 10}}},
		&fakeWarehouse{fresh: map[string]time.Time{"analytics.orders": now.Add(-time.Minute)}},
	)

	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	topics := map[string]bool{}
	for _, n := range g.Nodes {
		if n.Kind != KindTopic {
			continue
		}
		topics[n.Name] = true
		// A real topic (not a placeholder) was health-checked via collectTopics.
		if n.Facts["newest_record"] == "" || n.Facts["newest_record"] == "never" {
			t.Fatalf("topic %s was not health-checked: %+v", n.Name, n.Facts)
		}
	}
	if len(topics) != 2 || !topics["pg.public.orders"] || !topics["pg.public.order_items"] {
		t.Fatalf("topics = %v, want pg.public.orders and pg.public.order_items", topics)
	}
	for _, n := range g.Nodes {
		if n.Kind == KindSourceTable && n.Facts["slot"] != "" {
			t.Fatalf("source node %s failed to resolve its slot: %+v", n.Name, n.Facts)
		}
	}
}

// TestTraceRegexIncludeList proves a trace resolves the connector and topic
// when the include list is a regex; the old literal comparison reported
// ErrBadInput for such a table.
func TestTraceRegexIncludeList(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].SourceTables = []string{"public.orders", "public.order_items"}
	b := NewBuilder(
		o,
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": `public\.order.*`, "slot.name": "pg-orders-connector"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
		},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{"public.order_items|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{},
	)

	stages, err := b.Trace(context.Background(), "orders", "public.order_items", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if stages[1].Name != "pg-orders-connector" {
		t.Fatalf("connector stage = %q, want pg-orders-connector", stages[1].Name)
	}
	if stages[2].Name != "pg.public.order_items" {
		t.Fatalf("topic stage = %q, want pg.public.order_items", stages[2].Name)
	}
}

// TestTraceExcludedTable proves table.exclude.list is respected: a table in
// the include list pattern but on the exclude list has no streaming connector.
func TestTraceExcludedTable(t *testing.T) {
	o := testOwners()
	o.Pipelines[0].SourceTables = []string{"public.orders", "public.order_items"}
	b := NewBuilder(
		o,
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.*", "table.exclude.list": "public.order_items"}},
		},
		&fakeKafka{},
		&fakeSource{},
		&fakeWarehouse{},
	)

	_, err := b.Trace(context.Background(), "orders", "public.order_items", "7")
	if err == nil {
		t.Fatal("expected error for an excluded table")
	}
	if !errors.Is(err, ErrBadInput) {
		t.Fatalf("error %v does not wrap ErrBadInput", err)
	}
}

func TestBuildUnreachableSystems(t *testing.T) {
	b := NewBuilder(
		testOwners(),
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("down")}},
		&fakeKafka{err: map[string]error{"group:order-sync": errors.New("down")}},
		&fakeSource{slotsErr: errors.New("down")},
		&fakeWarehouse{freshErr: map[string]error{"analytics.orders": errors.New("down")}},
	)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build must not fail on unreachable systems: %v", err)
	}
	// Connector, group, sink are unknown; source is unknown (slot unreachable).
	for _, n := range g.Nodes {
		if n.Kind == KindConnector && n.Health != HealthUnknown {
			t.Fatalf("connector should be unknown, got %s", n.Health)
		}
	}
	// The graph shape must stay: an unknown-health topic node still exists even
	// though the connector config was unreachable.
	found := false
	for _, n := range g.Nodes {
		if n.Kind == KindTopic {
			found = true
			if n.Health != HealthUnknown {
				t.Fatalf("placeholder topic health = %s, want unknown", n.Health)
			}
		}
	}
	if !found {
		t.Fatalf("topic node missing when Connect is unreachable: %+v", g.Nodes)
	}
}

// ---- Trace ----

func traceFactsOK() traceFacts {
	return traceFacts{
		table: "public.orders", connector: "pg-orders-connector", topic: "pg.public.orders",
		group: "order-sync", sink: "analytics.orders",
		sourceFound: true, sourceTS: time.Now(),
		connectorState: "RUNNING",
		topicFound:     true, topicOffset: 42, topicPartition: 0,
		groupCommitted: 43,
		sinkFound:      true,
	}
}

func TestDecideTrace(t *testing.T) {
	cases := []struct {
		name       string
		f          traceFacts
		wantStatus []string // per stage
		stoppedAt  int      // index of stopped_here, -1 if none
	}{
		{
			name:       "all the way",
			f:          traceFactsOK(),
			wantStatus: []string{"seen", "seen", "seen", "seen", "seen"},
			stoppedAt:  -1,
		},
		{
			name: "connector failed before record",
			f: func() traceFacts {
				f := traceFactsOK()
				f.connectorStuck = true
				f.connectorState = "FAILED"
				f.tasksFailed = 1
				f.topicFound = false
				return f
			}(),
			wantStatus: []string{"seen", "stopped_here", "not_seen", "not_seen", "not_seen"},
			stoppedAt:  1,
		},
		{
			name: "task failed connector running",
			f: func() traceFacts {
				f := traceFactsOK()
				f.connectorStuck = true
				f.connectorState = "RUNNING"
				f.tasksFailed = 1
				f.topicFound = false
				return f
			}(),
			wantStatus: []string{"seen", "stopped_here", "not_seen", "not_seen", "not_seen"},
			stoppedAt:  1,
		},
		{
			name: "connector paused before record",
			f: func() traceFacts {
				f := traceFactsOK()
				f.connectorStuck = true
				f.connectorState = "PAUSED"
				f.topicFound = false
				return f
			}(),
			wantStatus: []string{"seen", "stopped_here", "not_seen", "not_seen", "not_seen"},
			stoppedAt:  1,
		},
		{
			name: "record in topic but not consumed",
			f: func() traceFacts {
				f := traceFactsOK()
				f.groupCommitted = 40 // < topicOffset 42
				return f
			}(),
			wantStatus: []string{"seen", "seen", "seen", "stopped_here", "not_seen"},
			stoppedAt:  3,
		},
		{
			name: "consumed but sink missing",
			f: func() traceFacts {
				f := traceFactsOK()
				f.sinkFound = false
				return f
			}(),
			wantStatus: []string{"seen", "seen", "seen", "seen", "stopped_here"},
			stoppedAt:  4,
		},
		{
			name: "source row missing",
			f: func() traceFacts {
				f := traceFactsOK()
				f.sourceFound = false
				return f
			}(),
			wantStatus: []string{"not_seen", "not_seen", "not_seen", "not_seen", "not_seen"},
			stoppedAt:  -1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stages := decideTrace(tc.f)
			if len(stages) != 5 {
				t.Fatalf("stages = %d, want 5", len(stages))
			}
			stopped := 0
			for i, s := range stages {
				if s.Status != tc.wantStatus[i] {
					t.Fatalf("stage %d status = %s, want %s", i, s.Status, tc.wantStatus[i])
				}
				if s.Status == "stopped_here" {
					stopped++
				}
			}
			if tc.stoppedAt == -1 && stopped != 0 {
				t.Fatalf("expected no stopped_here, got %d", stopped)
			}
			if tc.stoppedAt >= 0 && stopped != 1 {
				t.Fatalf("expected exactly one stopped_here, got %d", stopped)
			}
		})
	}
}

func TestTraceStuckAtConnector(t *testing.T) {
	b := NewBuilder(
		testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "FAILED", Tasks: []clients.TaskStatus{{State: "FAILED"}}}},
		},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{},
	)

	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if stages[1].Status != "stopped_here" {
		t.Fatalf("connector stage = %s, want stopped_here: %+v", stages[1].Status, stages)
	}
	// Facts never contain full row values.
	for _, s := range stages {
		for k, v := range s.Facts {
			if k == "diff_columns" {
				continue
			}
			if strings.Contains(v, "customer") {
				t.Fatalf("fact leaks a value: %s=%s", k, v)
			}
		}
	}
}

func TestTraceSourceMissing(t *testing.T) {
	b := NewBuilder(
		testOwners(),
		&fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}}},
		&fakeKafka{},
		&fakeSource{},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	for _, s := range stages {
		if s.Status != "not_seen" {
			t.Fatalf("stage %s = %s, want not_seen", s.Layer, s.Status)
		}
	}
}

func TestTraceTaskFailedConnectorRunning(t *testing.T) {
	// The schema-change shape: connector RUNNING, task FAILED, record not in
	// the topic. The connector must be stopped_here (not "seen").
	b := NewBuilder(
		testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "FAILED"}}}},
		},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if stages[1].Status != "stopped_here" {
		t.Fatalf("connector stage = %s, want stopped_here: %+v", stages[1].Status, stages)
	}
}

func TestTraceConnectorPaused(t *testing.T) {
	// connector-down: connector + task PAUSED, record not in the topic. The
	// connector must be stopped_here and must NOT be marked seen.
	b := NewBuilder(
		testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "PAUSED", Tasks: []clients.TaskStatus{{State: "PAUSED"}}}},
		},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace: %v", err)
	}
	if stages[1].Status != "stopped_here" {
		t.Fatalf("connector stage = %s, want stopped_here: %+v", stages[1].Status, stages)
	}
	if stages[2].Status != "not_seen" {
		t.Fatalf("topic stage = %s, want not_seen", stages[2].Status)
	}
}

func TestTraceConnectUnreachable(t *testing.T) {
	// Connect down must not fail the trace: every stage is returned, with the
	// connector and topic unknown.
	b := NewBuilder(
		testOwners(),
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("EOF")}},
		&fakeKafka{},
		&fakeSource{rows: map[string]map[string]any{"public.orders|7": {"id": "7", "updated_at": time.Now().Format(time.RFC3339Nano)}}},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace must not fail when Connect is unreachable: %v", err)
	}
	if len(stages) != 5 {
		t.Fatalf("stages = %d, want 5", len(stages))
	}
	if stages[1].Status != "unknown" {
		t.Fatalf("connector stage = %s, want unknown: %+v", stages[1].Status, stages)
	}
	if stages[1].Facts["error"] == "" {
		t.Fatalf("connector unknown stage missing error fact: %+v", stages[1])
	}
}

func TestTraceSourceReadError(t *testing.T) {
	b := NewBuilder(
		testOwners(),
		&fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg", "table.include.list": "public.orders"}}},
		&fakeKafka{},
		&fakeSource{rowErr: errors.New("down")},
		&fakeWarehouse{},
	)
	stages, err := b.Trace(context.Background(), "orders", "public.orders", "7")
	if err != nil {
		t.Fatalf("Trace must not fail when the source row read fails: %v", err)
	}
	if stages[0].Status != "unknown" {
		t.Fatalf("source stage = %s, want unknown: %+v", stages[0].Status, stages)
	}
}

func TestSlotNameFor(t *testing.T) {
	if got := slotNameFor("c1", map[string]string{}); got != "debezium" {
		t.Fatalf("default slot = %q, want debezium", got)
	}
	if got := slotNameFor("c1", map[string]string{"slot.name": "custom_slot"}); got != "custom_slot" {
		t.Fatalf("slot = %q, want custom_slot", got)
	}
}

func TestBuildConnectorMissing(t *testing.T) {
	b := NewBuilder(
		testOwners(),
		&fakeConnect{configErr: map[string]error{"pg-orders-connector": clients.ErrNotFound}},
		&fakeKafka{},
		&fakeSource{},
		&fakeWarehouse{},
	)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, n := range g.Nodes {
		if n.Kind == KindConnector {
			if n.Health != HealthFail {
				t.Fatalf("deleted connector health = %s, want fail: %+v", n.Health, n)
			}
			if n.Facts["missing"] != "true" {
				t.Fatalf("deleted connector facts = %v, want missing=true", n.Facts)
			}
		}
	}
}

func TestLayerOwner(t *testing.T) {
	b := NewBuilder(testOwners(), nil, nil, nil, nil)
	if got, ok := b.LayerOwner("orders", "warehouse"); !ok || got != "meera" {
		t.Fatalf("LayerOwner = %q, %v", got, ok)
	}
	if _, ok := b.LayerOwner("orders", "bogus"); ok {
		t.Fatal("bogus layer should not resolve")
	}
}

func TestDiffColumnsNormalizesNumbers(t *testing.T) {
	src := map[string]any{"id": "7", "amount": "50", "status": "placed", "updated_at": "2026-10-11T10:00:00Z"}
	sink := map[string]any{"id": "7", "amount": "50.00", "status": "placed", "updated_at": "2026-10-11T10:00:00Z", "loaded_at": "x"}
	got := diffColumns(src, sink)
	if len(got) != 0 {
		t.Fatalf("diff = %v, want none (50 vs 50.00 should match)", got)
	}
	sink["amount"] = "99.00"
	if got := diffColumns(src, sink); len(got) != 1 || got[0] != "amount" {
		t.Fatalf("diff = %v, want [amount]", got)
	}
}
