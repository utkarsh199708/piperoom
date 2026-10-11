//go:build !realtools

package main

import (
	"context"
	"errors"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// newA2Clients returns unreachable stub Connect/Kafka/Schema Registry clients
// for the default build (A2's packages have not merged yet). The topology
// builder maps a generic error to HealthUnknown, so pipeline_map and
// trace_record still work and just mark the Kafka/Connect layers unknown.
func newA2Clients(_ config.Config) (*a2Clients, func(), error) {
	return &a2Clients{
		connect:  stubConnect{},
		kafka:    stubKafka{},
		registry: stubRegistry{},
	}, func() {}, nil
}

// wireA2 is a no-op in the default build: A2's tools and actions are only
// compiled and registered behind -tags realtools.
func wireA2(_ *tools.Registry, _ *actions.Registry, _ *a2Clients) error {
	return nil
}

// unreachable is what the stubs return. It deliberately does not wrap
// clients.ErrNotFound: the topology builder treats generic errors as
// HealthUnknown ("unreachable"), not HealthFail ("missing").
var unreachable = errors.New("investigate: client not available in the default build (rebuild with -tags realtools)")

type stubConnect struct{}

func (stubConnect) ListConnectors(context.Context) ([]string, error) { return nil, unreachable }
func (stubConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, unreachable
}
func (stubConnect) Config(context.Context, string) (map[string]string, error) {
	return nil, unreachable
}
func (stubConnect) RestartTask(context.Context, string, int) error { return unreachable }
func (stubConnect) RestartConnector(context.Context, string) error { return unreachable }

type stubKafka struct{}

func (stubKafka) Topics(context.Context) ([]string, error) { return nil, unreachable }
func (stubKafka) TopicOffsets(context.Context, string) ([]clients.PartitionOffsets, error) {
	return nil, unreachable
}
func (stubKafka) ConsumerGroups(context.Context) ([]string, error) { return nil, unreachable }
func (stubKafka) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{}, unreachable
}
func (stubKafka) FindByKey(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
	return nil, unreachable
}

type stubRegistry struct{}

func (stubRegistry) Subjects(context.Context) ([]string, error) { return nil, unreachable }
func (stubRegistry) Versions(context.Context, string) ([]int, error) {
	return nil, unreachable
}
func (stubRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, unreachable
}
func (stubRegistry) Compatibility(context.Context, string) (string, bool, error) {
	return "", false, unreachable
}
func (stubRegistry) SetCompatibility(context.Context, string, string) error { return unreachable }
func (stubRegistry) Register(context.Context, string, string) (int, error)  { return 0, unreachable }

var (
	_ clients.ConnectClient        = stubConnect{}
	_ clients.KafkaAdmin           = stubKafka{}
	_ clients.SchemaRegistryClient = stubRegistry{}
)
