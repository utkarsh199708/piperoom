package wiring

import (
	"context"
	"errors"
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
