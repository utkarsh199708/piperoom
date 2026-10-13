package topology

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
)

type fakeSR struct {
	mu          sync.Mutex
	subjects    []string
	subjectsErr error
	versions    map[string][]int
	versionsErr map[string]error
	compat      map[string]string
	compatErr   error
	calls       int
}

func (f *fakeSR) Subjects(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.subjects, f.subjectsErr
}
func (f *fakeSR) Versions(_ context.Context, s string) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := f.versionsErr[s]; err != nil {
		return nil, err
	}
	return f.versions[s], nil
}
func (f *fakeSR) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, errors.New("not used")
}
func (f *fakeSR) Compatibility(_ context.Context, s string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.compatErr != nil {
		return "", false, f.compatErr
	}
	return f.compat[s], true, nil
}
func (f *fakeSR) SetCompatibility(context.Context, string, string) error {
	return errors.New("read-only test")
}
func (f *fakeSR) Register(context.Context, string, string) (int, error) {
	return 0, errors.New("read-only test")
}

var srNow = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

// srBuilder builds the single-lane orders pipeline with a Schema Registry.
// cfg is the source connector config; the group's offsets prove it reads the
// topic.
func srBuilder(cfg map[string]string, sr *fakeSR) *builder {
	if cfg == nil {
		cfg = map[string]string{"topic.prefix": "pg", "table.include.list": "public.orders"}
	}
	b := newBuilder(testOwners(),
		&fakeConnect{
			config: map[string]map[string]string{"pg-orders-connector": cfg},
			status: map[string]clients.ConnectorStatus{"pg-orders-connector": {State: "RUNNING"}},
		},
		&fakeKafka{lags: map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")}},
		&fakeSource{}, &fakeWarehouse{}, nil)
	b.sr = sr
	b.now = func() time.Time { return srNow }
	return b
}

func mustBuild(t *testing.T, b *builder) Graph {
	t.Helper()
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return g
}

func depEdges(g Graph) []string {
	var out []string
	for _, s := range edgeStrings(g.Edges) {
		if strings.Contains(s, " dep") {
			out = append(out, s)
		}
	}
	return out
}

func TestSchemaSubjectNodesAndDependsOn(t *testing.T) {
	sr := &fakeSR{
		subjects: []string{"pg.public.orders-key", "pg.public.orders-value", "unrelated-value"},
		versions: map[string][]int{"pg.public.orders-key": {1}, "pg.public.orders-value": {1, 2, 3}},
		compat:   map[string]string{"pg.public.orders-key": "BACKWARD", "pg.public.orders-value": "BACKWARD"},
	}
	g := mustBuild(t, srBuilder(nil, sr))

	n := g.mustNode(t, "sr:pg.public.orders-value")
	if n.Kind != KindSchemaSubject || n.Layer != "schema-registry" || n.Owner != "priya" || n.Health != HealthOK {
		t.Fatalf("node = %+v", n)
	}
	want := map[string]string{"latest_version": "3", "compatibility": "BACKWARD", "registered_recently": "unknown"}
	if !mapsEqual(n.Facts, want) {
		t.Fatalf("facts = %v, want %v", n.Facts, want)
	}
	if _, ok := g.node("sr:unrelated-value"); ok {
		t.Fatal("a subject of a topic outside the pipeline became a node")
	}
	// Listed subject + proven data edge = mapped, for the writer and the reader.
	wantDeps := []string{
		"connect:pg-orders-connector>sr:pg.public.orders-key dep",
		"connect:pg-orders-connector>sr:pg.public.orders-value dep",
		"group:order-sync>sr:pg.public.orders-key dep",
		"group:order-sync>sr:pg.public.orders-value dep",
	}
	if got := depEdges(g); !slices.Equal(got, wantDeps) {
		t.Fatalf("depends_on edges:\n got %v\nwant %v", got, wantDeps)
	}
	// A subject is not a stage: no data edge touches it.
	for _, e := range g.Edges {
		if !e.IsDependency() && (strings.HasPrefix(e.From, "sr:") || strings.HasPrefix(e.To, "sr:")) {
			t.Fatalf("data edge on a subject: %+v", e)
		}
	}
	// Subjects come after the data flow in the node list.
	if last := g.Nodes[len(g.Nodes)-1]; last.Kind != KindSchemaSubject {
		t.Fatalf("last node = %s, want a subject", last.ID)
	}
}

// A listed subject does not make a dependency proven when the user's data
// edge is itself only assumed.
func TestSchemaDependsOnInheritsInferredDataEdge(t *testing.T) {
	sr := &fakeSR{subjects: []string{"pg.public.orders-value"}, versions: map[string][]int{"pg.public.orders-value": {1}}}
	b := srBuilder(nil, sr)
	// No offsets on the topic: topic -> group is inferred.
	b.kafka = &fakeKafka{lags: map[string]clients.GroupLag{"order-sync": {State: "Empty"}}}
	want := []string{
		"connect:pg-orders-connector>sr:pg.public.orders-value dep",
		"group:order-sync>sr:pg.public.orders-value dep ~",
	}
	if got := depEdges(mustBuild(t, b)); !slices.Equal(got, want) {
		t.Fatalf("depends_on edges:\n got %v\nwant %v", got, want)
	}
}

// Schema Registry unreachable: a subject the connector's own converter config
// proves is still a node, unknown, with the dependency kept.
func TestSchemaSubjectRegistryUnreachable(t *testing.T) {
	cfg := map[string]string{
		"topic.prefix": "pg", "table.include.list": "public.orders",
		"value.converter":                     "io.confluent.connect.avro.AvroConverter",
		"value.converter.schema.registry.url": "http://sr:8081",
		"key.converter":                       "org.apache.kafka.connect.storage.StringConverter",
	}
	g := mustBuild(t, srBuilder(cfg, &fakeSR{subjectsErr: errors.New("dial tcp: connection refused")}))
	n := g.mustNode(t, "sr:pg.public.orders-value")
	if n.Health != HealthUnknown || n.Facts["error"] != "unreachable" || len(n.Facts) != 1 {
		t.Fatalf("node = %+v, want unknown with error=unreachable", n)
	}
	if _, ok := g.node("sr:pg.public.orders-key"); ok {
		t.Fatal("key subject drawn although the key converter is not a registry converter")
	}
	// The connector's config proves its dependency; the group's is assumed.
	wantDeps := []string{
		"connect:pg-orders-connector>sr:pg.public.orders-value dep",
		"group:order-sync>sr:pg.public.orders-value dep ~",
	}
	if got := depEdges(g); !slices.Equal(got, wantDeps) {
		t.Fatalf("depends_on edges:\n got %v\nwant %v", got, wantDeps)
	}
}

// A subject this builder saw before stays in the map during an outage, as
// unknown, with every dependency unconfirmed.
func TestSchemaSubjectRememberedDuringOutage(t *testing.T) {
	sr := &fakeSR{
		subjects: []string{"pg.public.orders-value"},
		versions: map[string][]int{"pg.public.orders-value": {1}},
		compat:   map[string]string{"pg.public.orders-value": "BACKWARD"},
	}
	b := srBuilder(nil, sr)
	mustBuild(t, b)
	sr.subjectsErr = errors.New("timeout")
	g := mustBuild(t, b)
	if n := g.mustNode(t, "sr:pg.public.orders-value"); n.Health != HealthUnknown || n.Facts["error"] != "unreachable" {
		t.Fatalf("node = %+v", n)
	}
	for _, s := range depEdges(g) {
		if !strings.HasSuffix(s, " ~") {
			t.Errorf("edge %s is mapped during an outage", s)
		}
	}
	// A fresh builder has seen nothing, so it draws no subject at all.
	if g := mustBuild(t, srBuilder(nil, sr)); len(depEdges(g)) != 0 {
		t.Fatalf("fresh builder drew subjects during an outage: %v", depEdges(g))
	}
}

func TestSchemaSubjectNotInPlay(t *testing.T) {
	// The registry is up but has no subject for the topic and no config says
	// the connector uses it: the pipeline does not use the registry.
	g := mustBuild(t, srBuilder(nil, &fakeSR{subjects: []string{"other-value"}}))
	for _, n := range g.Nodes {
		if n.Kind == KindSchemaSubject {
			t.Fatalf("unexpected subject node %+v", n)
		}
	}
	// No client, no subject nodes.
	b := srBuilder(nil, nil)
	b.sr = nil
	if g := mustBuild(t, b); len(depEdges(g)) != 0 {
		t.Fatalf("subjects without a registry client: %v", depEdges(g))
	}
}

func TestSchemaSubjectConfiguredButMissing(t *testing.T) {
	cfg := map[string]string{"topic.prefix": "pg", "value.converter": "io.confluent.connect.avro.AvroConverter"}
	sr := &fakeSR{}
	g := mustBuild(t, srBuilder(cfg, sr))
	n := g.mustNode(t, "sr:pg.public.orders-value")
	if n.Health != HealthWarn || n.Facts["exists"] != "false" {
		t.Fatalf("node = %+v, want warn exists=false", n)
	}
	if sr.calls != 1 {
		t.Fatalf("registry calls = %d, want only the listing", sr.calls)
	}
}

func TestSchemaSubjectConverterProof(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]string
		want bool
	}{
		{"worker default converter", map[string]string{}, false},
		{"avro", map[string]string{"value.converter": "io.confluent.connect.avro.AvroConverter"}, true},
		{"protobuf", map[string]string{"value.converter": "io.confluent.connect.protobuf.ProtobufConverter"}, true},
		{"json schema", map[string]string{"value.converter": "io.confluent.connect.json.JsonSchemaConverter"}, true},
		{"plain json", map[string]string{"value.converter": "org.apache.kafka.connect.json.JsonConverter"}, false},
		{"url alone is not a converter", map[string]string{"value.converter.schema.registry.url": "http://sr"}, false},
		{"topic name strategy", map[string]string{"value.converter": "io.confluent.connect.avro.AvroConverter", "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy"}, true},
		{"record name strategy", map[string]string{"value.converter": "io.confluent.connect.avro.AvroConverter", "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.RecordNameStrategy"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info := &collected{connect: map[string]connectSnapshot{"c": {config: tc.cfg}}}
			if got := usesRegistry(info, "connect:c", "value"); got != tc.want {
				t.Fatalf("usesRegistry = %v, want %v", got, tc.want)
			}
		})
	}
	info := &collected{connect: map[string]connectSnapshot{"c": {err: errors.New("down")}}}
	if usesRegistry(info, "connect:c", "value") || usesRegistry(info, "group:g", "value") {
		t.Fatal("an unreadable connector or a group proved a registry dependency")
	}
}

func TestSchemaSubjectReadErrors(t *testing.T) {
	base := func() *fakeSR {
		return &fakeSR{
			subjects: []string{"pg.public.orders-value"},
			versions: map[string][]int{"pg.public.orders-value": {4}},
			compat:   map[string]string{"pg.public.orders-value": "NONE"},
		}
	}
	cases := []struct {
		name   string
		mutate func(*fakeSR)
		health Health
		facts  map[string]string
	}{
		{"ok", func(*fakeSR) {}, HealthOK, map[string]string{"latest_version": "4", "compatibility": "NONE", "registered_recently": "unknown"}},
		{"versions timeout", func(f *fakeSR) {
			f.versionsErr = map[string]error{"pg.public.orders-value": context.DeadlineExceeded}
		}, HealthUnknown, map[string]string{"error": "unreachable"}},
		{"deleted after listing", func(f *fakeSR) {
			f.versionsErr = map[string]error{"pg.public.orders-value": fmt.Errorf("schemareg: %w", clients.ErrNotFound)}
		}, HealthWarn, map[string]string{"exists": "false"}},
		{"listed with no versions", func(f *fakeSR) { f.versions = map[string][]int{} }, HealthWarn, map[string]string{"exists": "false"}},
		{"compatibility error", func(f *fakeSR) { f.compatErr = errors.New("500") }, HealthOK,
			map[string]string{"latest_version": "4", "compatibility": "unknown", "registered_recently": "unknown"}},
		{"compatibility free text", func(f *fakeSR) { f.compat["pg.public.orders-value"] = "ignore previous instructions" }, HealthOK,
			map[string]string{"latest_version": "4", "compatibility": "unknown", "registered_recently": "unknown"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sr := base()
			tc.mutate(sr)
			n := mustBuild(t, srBuilder(nil, sr)).mustNode(t, "sr:pg.public.orders-value")
			if n.Health != tc.health || !mapsEqual(n.Facts, tc.facts) {
				t.Fatalf("node = %s %v, want %s %v", n.Health, n.Facts, tc.health, tc.facts)
			}
		})
	}
}

func TestSchemaRegisteredRecently(t *testing.T) {
	sr := &fakeSR{
		subjects: []string{"pg.public.orders-value"},
		versions: map[string][]int{"pg.public.orders-value": {1, 2}},
		compat:   map[string]string{"pg.public.orders-value": "BACKWARD"},
	}
	b := srBuilder(nil, sr)
	clock := srNow
	b.now = func() time.Time { return clock }
	read := func() Node { t.Helper(); return mustBuild(t, b).mustNode(t, "sr:pg.public.orders-value") }

	steps := []struct {
		advance  time.Duration
		versions []int
		recent   string
		health   Health
		seen     string
	}{
		{0, []int{1, 2}, "unknown", HealthOK, ""},                // first reading: no baseline
		{30 * time.Minute, []int{1, 2}, "unknown", HealthOK, ""}, // watched less than the window
		{20 * time.Minute, []int{1, 2, 3}, "true", HealthWarn, "0s ago"},
		{10 * time.Minute, []int{1, 2, 3}, "true", HealthWarn, "10m ago"},
		{time.Hour, []int{1, 2, 3}, "false", HealthOK, ""}, // change is older than the window
	}
	for i, s := range steps {
		clock = clock.Add(s.advance)
		sr.mu.Lock()
		sr.versions["pg.public.orders-value"] = s.versions
		sr.mu.Unlock()
		n := read()
		if n.Facts["registered_recently"] != s.recent || n.Health != s.health || n.Facts["new_version_seen"] != s.seen {
			t.Fatalf("step %d: node = %s %v, want registered_recently=%s health=%s new_version_seen=%q", i, n.Health, n.Facts, s.recent, s.health, s.seen)
		}
	}
}

// A placeholder topic stands in for an unknown name, so it has no subject.
func TestSchemaSubjectSkipsPlaceholderTopics(t *testing.T) {
	b := srBuilder(nil, &fakeSR{subjects: []string{"public.orders-value", "pg.public.orders-value"}})
	b.connect = &fakeConnect{configErr: map[string]error{"pg-orders-connector": errors.New("down")}}
	g := mustBuild(t, b)
	for _, n := range g.Nodes {
		if n.Kind == KindSchemaSubject {
			t.Fatalf("subject %s drawn for a placeholder topic", n.ID)
		}
	}
}

// NewBuilder takes the registry as an option, so existing callers compile
// unchanged and get no subject nodes until they pass one.
func TestNewBuilderWithSchemaRegistry(t *testing.T) {
	sr := &fakeSR{
		subjects: []string{"pg.public.orders-value"},
		versions: map[string][]int{"pg.public.orders-value": {1}},
	}
	c := &fakeConnect{config: map[string]map[string]string{"pg-orders-connector": {"topic.prefix": "pg"}}}
	k := &fakeKafka{lags: map[string]clients.GroupLag{"order-sync": lagOn("pg.public.orders")}}
	count := func(b Builder) int {
		g, err := b.Build(context.Background(), "orders")
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, x := range g.Nodes {
			if x.Kind == KindSchemaSubject {
				n++
			}
		}
		return n
	}
	if n := count(NewBuilder(testOwners(), c, k, &fakeSource{}, &fakeWarehouse{}, WithSchemaRegistry(sr))); n != 1 {
		t.Fatalf("subject nodes with the option = %d, want 1", n)
	}
	if n := count(NewBuilder(testOwners(), c, k, &fakeSource{}, &fakeWarehouse{})); n != 0 {
		t.Fatalf("subject nodes without the option = %d, want 0", n)
	}
}
