package topology

import (
	"context"
	"errors"
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
			"schema-registry": "arjun",
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
		name    string
		prefix  string
		include string
		tables  []string
		want    []string
	}{
		{"single", "pg", "public.orders", []string{"public.orders"}, []string{"pg.public.orders"}},
		{"empty include means all", "pg", "", []string{"public.orders", "public.items"}, []string{"pg.public.orders", "pg.public.items"}},
		{"filtered", "pg", "public.orders", []string{"public.orders", "public.items"}, []string{"pg.public.orders"}},
		{"no prefix", "", "public.orders", []string{"public.orders"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveTopics(tc.prefix, tc.include, tc.tables)
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
	// Topic may be absent because config failed.
	for _, n := range g.Nodes {
		if n.Kind == KindConnector && n.Health != HealthUnknown {
			t.Fatalf("connector should be unknown, got %s", n.Health)
		}
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
				f.connectorFailed = true
				f.connectorState = "FAILED"
				f.tasksFailed = 1
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
