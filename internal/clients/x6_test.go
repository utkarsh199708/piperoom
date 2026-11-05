package clients_test

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"piperoom/internal/clients"
)

// workerFake is the shape internal/clients/connect implements in X6b (E5): a
// ConnectReader that also answers WorkerTasks.
type workerFake struct {
	connectOnly
	tasks map[string][]clients.TaskAssignment
}

func (f workerFake) WorkerTasks(ctx context.Context) (map[string][]clients.TaskAssignment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.tasks, nil
}

// connectOnly is a ConnectReader from before X6.
type connectOnly struct{}

func (connectOnly) ListConnectors(context.Context) ([]string, error) { return nil, nil }
func (connectOnly) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, nil
}
func (connectOnly) Config(context.Context, string) (map[string]string, error) { return nil, nil }

// E2, E5: WorkerTaskReader is optional: a pre-X6 ConnectReader still is one
// and is not a WorkerTaskReader; a reader that has it is found by assertion.
func TestWorkerTaskReaderOptional(t *testing.T) {
	var old clients.ConnectReader = connectOnly{}
	if _, ok := old.(clients.WorkerTaskReader); ok {
		t.Fatal("a pre-X6 ConnectReader claims WorkerTasks")
	}
	f := workerFake{tasks: map[string][]clients.TaskAssignment{
		"w1:8083":                {{Connector: "orders-sink", Type: "sink", Task: 0, State: "RUNNING"}},
		"w2:8083":                {},
		clients.UnassignedWorker: {{Connector: "orders-sink", Type: "sink", Task: 1, State: "UNASSIGNED"}},
	}}
	var r clients.ConnectReader = f
	wr, ok := r.(clients.WorkerTaskReader)
	if !ok {
		t.Fatal("WorkerTasks not found by assertion")
	}
	got, err := wr.WorkerTasks(context.Background())
	if err != nil || len(got) != 3 || len(got["w2:8083"]) != 0 {
		t.Fatalf("WorkerTasks = %v, %v", got, err)
	}
	if clients.WorkerTasksTimeout.Seconds() != 10 {
		t.Fatal("the deadline is 10 s")
	}
}

// E1: TaskAssignment round-trips under a strict decoder; task 0 is kept.
func TestTaskAssignmentRoundTrip(t *testing.T) {
	in := clients.TaskAssignment{Connector: "orders-sink", Type: "sink", Task: 0, State: "RUNNING"}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"task":0`)) {
		t.Fatalf("task 0 dropped: %s", raw)
	}
	var out clients.TaskAssignment
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil || !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip: %+v, %v", out, err)
	}
	dec = json.NewDecoder(bytes.NewReader([]byte(`{"connector":"c","type":"sink","task":1,"state":"FAILED","trace":"x"}`)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err == nil {
		t.Fatal("a trace field is accepted: TaskAssignment never carries one")
	}
}
