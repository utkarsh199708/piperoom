package demo

// Fake client implementations of the interfaces the real actions write
// through, so --demo exercises the REAL actions (actions.Register) against an
// in-memory Connect and Schema Registry instead of demo-only action types
// (LEAD-ARCH round 4, item 3). The fixtures tell the schema-change story from
// dev/owners.yaml: pg-orders-connector has task 0 FAILED after an
// incompatible Avro schema was rejected for pg.public.orders-value.

import (
	"context"
	"fmt"
	"sync"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
)

// Compile-time checks: the fakes must satisfy the exact contract interfaces.
var (
	_ clients.ConnectClient        = (*FakeConnect)(nil)
	_ clients.SchemaRegistryClient = (*FakeRegistry)(nil)
)

// FakeActionDeps returns the fake clients the demo actions run over.
func FakeActionDeps() actions.Deps {
	return actions.Deps{Connect: NewFakeConnect(), Registry: NewFakeRegistry()}
}

const (
	connectorName = "pg-orders-connector"
	subjectName   = "pg.public.orders-value"
)

// FakeConnect is an in-memory Kafka Connect: one source connector whose task 0
// starts FAILED (the demo story) and turns RUNNING when restarted.
type FakeConnect struct {
	mu    sync.Mutex
	task0 string
}

// NewFakeConnect returns the Connect state the demo story starts from.
func NewFakeConnect() *FakeConnect {
	return &FakeConnect{task0: "FAILED"}
}

func (f *FakeConnect) ListConnectors(context.Context) ([]string, error) {
	return []string{connectorName}, nil
}

func (f *FakeConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	if name != connectorName {
		return clients.ConnectorStatus{}, fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return clients.ConnectorStatus{
		Name: name, Type: "source", State: "RUNNING", WorkerID: "172.19.0.5:8083",
		Tasks: []clients.TaskStatus{{ID: 0, State: f.task0, WorkerID: "172.19.0.5:8083"}},
	}, nil
}

func (f *FakeConnect) Config(_ context.Context, name string) (map[string]string, error) {
	if name != connectorName {
		return nil, fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	return map[string]string{
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"topic.prefix":    "pg",
	}, nil
}

func (f *FakeConnect) RestartTask(_ context.Context, name string, task int) error {
	if name != connectorName {
		return fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	if task != 0 {
		return fmt.Errorf("demo: task %d: %w", task, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.task0 = "RUNNING"
	return nil
}

func (f *FakeConnect) RestartConnector(_ context.Context, name string) error {
	if name != connectorName {
		return fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.task0 = "RUNNING"
	return nil
}

// FakeRegistry is an in-memory Schema Registry with one subject at
// compatibility BACKWARD, the demo story's starting point.
type FakeRegistry struct {
	mu     sync.Mutex
	levels map[string]string
}

// NewFakeRegistry returns the Registry state the demo story starts from.
func NewFakeRegistry() *FakeRegistry {
	return &FakeRegistry{levels: map[string]string{subjectName: "BACKWARD"}}
}

func (f *FakeRegistry) Subjects(context.Context) ([]string, error) {
	return []string{subjectName}, nil
}

func (f *FakeRegistry) Versions(_ context.Context, subject string) ([]int, error) {
	if _, ok := f.level(subject); !ok {
		return nil, fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	return []int{1}, nil
}

func (f *FakeRegistry) Schema(_ context.Context, subject string, version int) (clients.SchemaVersion, error) {
	if _, ok := f.level(subject); !ok || (version != -1 && version != 1) {
		return clients.SchemaVersion{}, fmt.Errorf("demo: subject %q version %d: %w", subject, version, clients.ErrNotFound)
	}
	return clients.SchemaVersion{
		Subject: subject, Version: 1, ID: 1,
		Schema: `{"type":"record","name":"orders","fields":[{"name":"id","type":"int"},{"name":"amount","type":"int"}]}`,
	}, nil
}

func (f *FakeRegistry) Compatibility(_ context.Context, subject string) (string, bool, error) {
	level, ok := f.level(subject)
	if !ok {
		return "", false, fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	return level, true, nil
}

func (f *FakeRegistry) SetCompatibility(_ context.Context, subject, level string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.levels[subject]; !ok {
		return fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	f.levels[subject] = level
	return nil
}

func (f *FakeRegistry) Register(_ context.Context, subject, _ string) (int, error) {
	if _, ok := f.level(subject); !ok {
		return 0, fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	return 2, nil
}

func (f *FakeRegistry) level(subject string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	level, ok := f.levels[subject]
	return level, ok
}
