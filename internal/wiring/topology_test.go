package wiring

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Every client is down, so Build touches each one once and returns.
var errDown = errors.New("down")

type downConnect struct{ clients.ConnectClient }

func (downConnect) Config(context.Context, string) (map[string]string, error) { return nil, errDown }

type downKafka struct{ clients.KafkaAdmin }

func (downKafka) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{}, errDown
}

func (downKafka) TopicOffsets(context.Context, string) ([]clients.PartitionOffsets, error) {
	return nil, errDown
}

type downSource struct{ clients.SourceDB }

func (downSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return nil, errDown
}

type countingRegistry struct {
	clients.SchemaRegistryClient
	subjects atomic.Int32
}

func (r *countingRegistry) Subjects(context.Context) ([]string, error) {
	r.subjects.Add(1)
	return nil, errDown
}

// Open and RegisterTools both build the topology builder through
// newTopologyBuilder; it must hand the builder the Schema Registry client,
// or the server's pipeline map never shows a schema subject.
func TestNewTopologyBuilderUsesRegistry(t *testing.T) {
	reg := &countingRegistry{}
	w := &Wired{
		Source:   downSource{},
		Connect:  downConnect{},
		Kafka:    downKafka{},
		Registry: reg,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{
			ID: "p", TopicPrefix: "pg", SourceTables: []string{"public.t"}, Connectors: []string{"c"}, ConsumerGroups: []string{"g"},
		}}},
	}
	if _, err := newTopologyBuilder(w).Build(context.Background(), "p"); err != nil {
		t.Fatalf("Build: %v", err)
	}
	if reg.subjects.Load() == 0 {
		t.Fatal("the builder never asked the Schema Registry: newTopologyBuilder did not pass it")
	}
}

// testPipeline has no groups or sinks, so a Build touches only the source,
// Connect and the registry.
func testPipeline() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "p", TopicPrefix: "pg", SourceTables: []string{"public.t"}, Connectors: []string{"c"},
	}}}
}

// Open's path: assemble (what Open returns) builds the topology builder with
// the registry built from SCHEMA_REGISTRY_URL.
func TestAssembleBuilderUsesRegistry(t *testing.T) {
	var subjects atomic.Int32
	sr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/subjects" {
			subjects.Add(1)
		}
		w.Write([]byte(`[]`))
	}))
	t.Cleanup(sr.Close)
	connectSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(connectSrv.Close)

	cfg := devA2Config()
	cfg.SchemaRegistryURL = sr.URL
	cfg.ConnectURL = connectSrv.URL
	a2, err := buildA2Clients(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a2.Kafka.Close)

	w := assemble(testPipeline(), "", a2, downSource{}, nil, func() {})
	if w.Registry == nil || w.Builder == nil {
		t.Fatalf("assemble: registry %v, builder %v", w.Registry, w.Builder)
	}
	if _, err := w.Builder.Build(context.Background(), "p"); err != nil {
		t.Fatalf("Build: %v", err)
	}
	if subjects.Load() == 0 {
		t.Fatal("Open's builder never asked the Schema Registry")
	}
}

// SCHEMA_REGISTRY_URL "" (or "off", which config stores as ""): no registry
// client at all, a nil interface on Wired (not a nil pointer in it), and a
// write-action registry that refuses cleanly.
func TestAssembleWithoutRegistry(t *testing.T) {
	connectSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(connectSrv.Close)
	cfg := devA2Config()
	cfg.SchemaRegistryURL = ""
	cfg.ConnectURL = connectSrv.URL
	a2, err := buildA2Clients(cfg, nil)
	if err != nil {
		t.Fatalf("buildA2Clients without a registry: %v", err)
	}
	t.Cleanup(a2.Kafka.Close)
	if a2.Registry != nil {
		t.Fatal("a registry client was built for an empty SCHEMA_REGISTRY_URL")
	}
	w := assemble(testPipeline(), "", a2, downSource{}, nil, func() {})
	if w.Registry != nil {
		t.Fatalf("Wired.Registry = %#v, want a nil interface", w.Registry)
	}
	g, err := w.Builder.Build(context.Background(), "p")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, n := range g.Nodes {
		if strings.HasPrefix(n.ID, "sr:") {
			t.Fatalf("subject node %s without a registry", n.ID)
		}
	}
	if _, err := actionRegistry(w).Subjects(context.Background()); !errors.Is(err, errNoRegistry) {
		t.Fatalf("action registry Subjects error = %v, want errNoRegistry", err)
	}
	if actionRegistry(&Wired{Registry: &countingRegistry{}}) == (noRegistry{}) {
		t.Fatal("a configured registry was replaced by the stub")
	}
}
