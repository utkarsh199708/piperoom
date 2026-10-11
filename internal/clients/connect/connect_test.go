package connect

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

func readFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read testdata/%s: %v", name, err)
	}
	return string(b)
}

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return New(srv.URL, srv.Client())
}

func TestListConnectors(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/connectors" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(readFile(t, "connectors.json")))
	})
	names, err := c.ListConnectors(context.Background())
	if err != nil {
		t.Fatalf("ListConnectors: %v", err)
	}
	if len(names) != 1 || names[0] != "pg-orders-connector" {
		t.Fatalf("got %v", names)
	}
}

func TestStatusRunning(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(readFile(t, "status_running.json")))
	})
	cs, err := c.Status(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if cs.Name != "pg-orders-connector" || cs.State != "RUNNING" || cs.Type != "source" {
		t.Fatalf("got %+v", cs)
	}
	if len(cs.Tasks) != 1 || cs.Tasks[0].ID != 0 || cs.Tasks[0].State != "RUNNING" {
		t.Fatalf("tasks %+v", cs.Tasks)
	}
}

func TestStatusFailedKeepsTrace(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(readFile(t, "status_failed.json")))
	})
	cs, err := c.Status(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if cs.State != "RUNNING" { // connector state is RUNNING even though task FAILED
		t.Fatalf("connector state %q", cs.State)
	}
	if len(cs.Tasks) != 1 || cs.Tasks[0].State != "FAILED" {
		t.Fatalf("tasks %+v", cs.Tasks)
	}
	tr := cs.Tasks[0].Trace
	for _, want := range []string{"ConnectException", "Caused by:", "MISSING_UNION_BRANCH"} {
		if !strings.Contains(tr, want) {
			t.Fatalf("trace missing %q", want)
		}
	}
}

func TestConfig(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(readFile(t, "config.json")))
	})
	cfg, err := c.Config(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg["connector.class"] != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Fatalf("got %v", cfg)
	}
}

func TestRestartTaskPostsRightPath(t *testing.T) {
	var gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method %s", r.Method)
		}
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	})
	if err := c.RestartTask(context.Background(), "pg-orders-connector", 0); err != nil {
		t.Fatalf("RestartTask: %v", err)
	}
	if gotPath != "/connectors/pg-orders-connector/tasks/0/restart" {
		t.Fatalf("path %q", gotPath)
	}
}

func TestRestartConnectorQuery(t *testing.T) {
	var gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusAccepted)
	})
	if err := c.RestartConnector(context.Background(), "pg-orders-connector"); err != nil {
		t.Fatalf("RestartConnector: %v", err)
	}
	if !strings.Contains(gotQuery, "onlyFailed=true") || !strings.Contains(gotQuery, "includeTasks=true") {
		t.Fatalf("query %q", gotQuery)
	}
}

func TestNotFoundWrapsErrNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	_, err := c.Status(context.Background(), "nope")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestDeadline(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.ListConnectors(ctx); err == nil {
		t.Fatal("want error from deadline, got nil")
	}
}
