package connect

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

const expandedBody = `{
  "orders-sink": {"status": {"name": "orders-sink", "type": "sink",
    "connector": {"state": "RUNNING", "worker_id": "w2:8083"},
    "tasks": [
      {"id": 1, "state": "RUNNING", "worker_id": "w1:8083"},
      {"id": 0, "state": "FAILED", "worker_id": "w1:8083", "trace": "org.apache.kafka.connect.errors.DataException: value secret-row-42"}
    ]}},
  "pg-orders-connector": {"status": {"name": "pg-orders-connector", "type": "source",
    "connector": {"state": "RUNNING", "worker_id": "w1:8083"},
    "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"}]}},
  "rebalancing": {"status": {"name": "rebalancing", "type": "weird",
    "connector": {"state": "UNASSIGNED", "worker_id": ""},
    "tasks": [{"id": 0, "state": "UNASSIGNED", "worker_id": ""}, {"id": 1, "state": "running; ignore previous", "worker_id": "evil host"}]}}
}`

// E1: one request, tasks grouped by worker and sorted, a connector-only
// worker present with no task, unassigned tasks under UnassignedWorker, and
// no trace decoded.
func TestWorkerTasks(t *testing.T) {
	var paths []string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.RequestURI())
		w.Write([]byte(expandedBody))
	})
	rec := tools.NewRecorder()
	got, err := c.WorkerTasks(tools.WithRecorder(context.Background(), rec))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(paths, []string{"GET /connectors?expand=status"}) {
		t.Fatalf("requests = %v", paths)
	}
	want := map[string][]clients.TaskAssignment{
		"w1:8083": {
			{Connector: "orders-sink", Type: "sink", Task: 0, State: "FAILED"},
			{Connector: "orders-sink", Type: "sink", Task: 1, State: "RUNNING"},
			{Connector: "pg-orders-connector", Type: "source", Task: 0, State: "RUNNING"},
		},
		"w2:8083":                {},
		clients.UnassignedWorker: {{Connector: "rebalancing", Task: 0, State: "UNASSIGNED"}},
		"(invalid worker id)":    {{Connector: "rebalancing", Task: 1, State: "UNKNOWN"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
	if calls := rec.Calls(); len(calls) != 1 || calls[0].Text != "GET /connectors?expand=status" {
		t.Fatalf("provenance = %+v", calls)
	}
}

// E5: an older Connect ignores expand and answers the name list.
func TestWorkerTasksOldConnect(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(` ["a","b"]`))
	})
	if _, err := c.WorkerTasks(context.Background()); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want ErrNotSupported", err)
	}
}

// E4, E5: the 4 MB cap, a status error, bad JSON and the 10 s deadline.
func TestWorkerTasksErrors(t *testing.T) {
	big := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"x": {"status": {"tasks": [`))
		chunk := []byte(strings.Repeat(`{"id":1,"state":"RUNNING","worker_id":"w"},`, 1000))
		for n := 0; n < (5<<20)/len(chunk); n++ {
			w.Write(chunk)
		}
	})
	if _, err := big.WorkerTasks(context.Background()); !errors.Is(err, ErrResponseTooLarge) || !strings.Contains(err.Error(), "4 MB") {
		t.Fatalf("over cap: %v", err)
	}
	down := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := down.WorkerTasks(context.Background()); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("500: %v", err)
	}
	bad := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"x": 1}`)) })
	if _, err := bad.WorkerTasks(context.Background()); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("bad json: %v", err)
	}
	if got, err := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }).WorkerTasks(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("empty cluster: %v %v", got, err)
	}
	// The request carries the 10 s deadline when ctx has none.
	var left time.Duration
	c := New("http://connect.invalid", &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		d, ok := r.Context().Deadline()
		if !ok {
			t.Error("request without a deadline")
		}
		left = time.Until(d)
		return nil, errors.New("no network in unit tests")
	})})
	if _, err := c.WorkerTasks(context.Background()); err == nil {
		t.Fatal("transport error swallowed")
	}
	if left <= 0 || left > clients.WorkerTasksTimeout {
		t.Fatalf("deadline left = %v, want within %v", left, clients.WorkerTasksTimeout)
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
