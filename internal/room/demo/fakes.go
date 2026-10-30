package demo

// Fake client implementations of the interfaces the real actions write
// through, so --demo exercises the REAL actions (actions.Register) against an
// in-memory Connect and Schema Registry instead of demo-only action types
// (LEAD-ARCH round 4, item 3). The fixtures tell the schema-change story from
// dev/owners.yaml: pg-orders-connector has task 0 FAILED after an
// incompatible Avro schema was rejected for pg.public.orders-value.
//
// One registry of actions is built once at startup (cmd/server), so the fakes
// are shared by every room of the process. Their mutable state is therefore
// keyed by the room id the room service puts in the action context
// (room.RoomID): each demo room starts the story from the same values and a
// fix executed in one room never changes another's preview (KAN-392).

import (
	"context"
	"fmt"
	"sync"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/room"
)

// Compile-time checks: the fakes must satisfy the exact contract interfaces.
var (
	_ clients.ConnectClient        = (*FakeConnect)(nil)
	_ clients.SchemaRegistryClient = (*FakeRegistry)(nil)
)

// FakeActionDeps returns fresh fake clients the demo actions run over.
func FakeActionDeps() actions.Deps {
	return NewFakes().Actions()
}

// Fakes is the demo's in-memory client pair. The --demo server registers the
// real actions over this pair and hands the same pair to Controls; a reset
// returns one room's state to the state the story starts from.
type Fakes struct {
	Connect  *FakeConnect
	Registry *FakeRegistry
}

// NewFakes returns the demo's fake clients in their starting state.
func NewFakes() *Fakes {
	return &Fakes{Connect: NewFakeConnect(), Registry: NewFakeRegistry()}
}

// Actions returns the actions.Deps that run over these clients.
func (f *Fakes) Actions() actions.Deps {
	return actions.Deps{Connect: f.Connect, Registry: f.Registry}
}

// Reset puts one room's clients back to the state the story starts from: the
// connector's task 0 is FAILED and the subject compatibility is BACKWARD.
// Other rooms keep their own state.
func (f *Fakes) Reset(roomID string) {
	f.Connect.Reset(roomID)
	f.Registry.Reset(roomID)
}

const (
	connectorName = "pg-orders-connector"
	subjectName   = "pg.public.orders-value"

	// storyTaskState and storyCompatibility are the story's starting values.
	// The scripted tool summaries in demo.go state the same values, so a fresh
	// demo room's preview and its summaries agree (KAN-392).
	storyTaskState     = "FAILED"
	storyCompatibility = "BACKWARD"
)

// FakeConnect is an in-memory Kafka Connect: one source connector whose task 0
// starts FAILED (the demo story) and turns RUNNING when restarted or resumed.
// Each room id gets its own connector state; calls without a room id share
// the "" state, which keeps single-room users and direct tests working.
type FakeConnect struct {
	mu     sync.Mutex
	states map[string]*connectState
	// last is the room that last started the story, was reset, or ran a fix.
	// The topology endpoint carries no room id, so the map follows this room.
	last string
}

type connectState struct {
	connState string
	task0     string
}

func newConnectState() *connectState {
	return &connectState{connState: "RUNNING", task0: storyTaskState}
}

// NewFakeConnect returns the Connect state the demo story starts from.
func NewFakeConnect() *FakeConnect {
	return &FakeConnect{states: make(map[string]*connectState)}
}

// stateLocked returns the state of roomID, creating it from the story on first
// use. The caller holds f.mu.
func (f *FakeConnect) stateLocked(roomID string) *connectState {
	st, ok := f.states[roomID]
	if !ok {
		st = newConnectState()
		f.states[roomID] = st
	}
	return st
}

// Reset forgets roomID's state so its next call starts the story again. The
// demo reset endpoint (KAN-398) calls it; until then a presenter restarts the
// process. An empty roomID resets the "" bucket.
func (f *FakeConnect) Reset(roomID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.states, roomID)
	f.last = roomID
}

// markRoom makes roomID the room the topology map follows. Controls.Run calls
// it when a room's story starts; Reset and the fix calls (RestartTask, Resume,
// RestartConnector) do it themselves. Reads such as Status and Config do not.
func (f *FakeConnect) markRoom(roomID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last = roomID
}

// lastRoomRecovered reports whether the room the map follows has its
// connector and task 0 RUNNING: the state the scripted fix leaves behind. A
// room without state (not started, or reset) is still on the story's failure.
func (f *FakeConnect) lastRoomRecovered() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[f.last]
	return ok && st.connState == "RUNNING" && st.task0 == "RUNNING"
}

func (f *FakeConnect) ListConnectors(context.Context) ([]string, error) {
	return []string{connectorName}, nil
}

func (f *FakeConnect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	if name != connectorName {
		return clients.ConnectorStatus{}, fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.stateLocked(room.RoomID(ctx))
	return clients.ConnectorStatus{
		Name: name, Type: "source", State: st.connState, WorkerID: "172.19.0.5:8083",
		Tasks: []clients.TaskStatus{{ID: 0, State: st.task0, WorkerID: "172.19.0.5:8083"}},
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

func (f *FakeConnect) RestartTask(ctx context.Context, name string, task int) error {
	if name != connectorName {
		return fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	if task != 0 {
		return fmt.Errorf("demo: task %d: %w", task, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := room.RoomID(ctx)
	f.stateLocked(id).task0 = "RUNNING"
	f.last = id
	return nil
}

// Resume sets the connector and its tasks to RUNNING, as RestartTask sets task 0.
func (f *FakeConnect) Resume(ctx context.Context, name string) error {
	if name != connectorName {
		return fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := room.RoomID(ctx)
	st := f.stateLocked(id)
	st.connState = "RUNNING"
	st.task0 = "RUNNING"
	f.last = id
	return nil
}

func (f *FakeConnect) RestartConnector(ctx context.Context, name string) error {
	if name != connectorName {
		return fmt.Errorf("demo: connector %q: %w", name, clients.ErrNotFound)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := room.RoomID(ctx)
	f.stateLocked(id).task0 = "RUNNING"
	f.last = id
	return nil
}

// FakeRegistry is an in-memory Schema Registry with one subject at
// compatibility BACKWARD, the demo story's starting point. Like FakeConnect,
// it keeps one state per room id.
type FakeRegistry struct {
	mu     sync.Mutex
	levels map[string]map[string]string
}

// NewFakeRegistry returns the Registry state the demo story starts from.
func NewFakeRegistry() *FakeRegistry {
	return &FakeRegistry{levels: make(map[string]map[string]string)}
}

// roomLevelsLocked returns roomID's subject levels, creating them from the
// story on first use. The caller holds f.mu.
func (f *FakeRegistry) roomLevelsLocked(roomID string) map[string]string {
	levels, ok := f.levels[roomID]
	if !ok {
		levels = map[string]string{subjectName: storyCompatibility}
		f.levels[roomID] = levels
	}
	return levels
}

// level returns a subject's compatibility level in ctx's room.
func (f *FakeRegistry) level(ctx context.Context, subject string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	level, ok := f.roomLevelsLocked(room.RoomID(ctx))[subject]
	return level, ok
}

// Reset forgets roomID's state so its next call starts the story again (see
// FakeConnect.Reset).
func (f *FakeRegistry) Reset(roomID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.levels, roomID)
}

func (f *FakeRegistry) Subjects(context.Context) ([]string, error) {
	return []string{subjectName}, nil
}

func (f *FakeRegistry) Versions(ctx context.Context, subject string) ([]int, error) {
	if _, ok := f.level(ctx, subject); !ok {
		return nil, fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	return []int{1}, nil
}

func (f *FakeRegistry) Schema(ctx context.Context, subject string, version int) (clients.SchemaVersion, error) {
	if _, ok := f.level(ctx, subject); !ok || (version != -1 && version != 1) {
		return clients.SchemaVersion{}, fmt.Errorf("demo: subject %q version %d: %w", subject, version, clients.ErrNotFound)
	}
	return clients.SchemaVersion{
		Subject: subject, Version: 1, ID: 1,
		Schema: `{"type":"record","name":"orders","fields":[{"name":"id","type":"int"},{"name":"amount","type":"int"}]}`,
	}, nil
}

func (f *FakeRegistry) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	level, ok := f.level(ctx, subject)
	if !ok {
		return "", false, fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	return level, true, nil
}

func (f *FakeRegistry) SetCompatibility(ctx context.Context, subject, level string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	levels := f.roomLevelsLocked(room.RoomID(ctx))
	if _, ok := levels[subject]; !ok {
		return fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	levels[subject] = level
	return nil
}

func (f *FakeRegistry) Register(ctx context.Context, subject, _ string) (int, error) {
	if _, ok := f.level(ctx, subject); !ok {
		return 0, fmt.Errorf("demo: subject %q: %w", subject, clients.ErrNotFound)
	}
	return 2, nil
}
