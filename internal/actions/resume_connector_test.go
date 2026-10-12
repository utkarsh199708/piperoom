package actions

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
)

// resumeTestDeps wires the real connect client against an httptest server, so
// the action exercises the full path (Validate -> Execute -> HTTP) without the
// dev stack.
func resumeTestDeps(t *testing.T, handler http.HandlerFunc) Deps {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return Deps{
		Connect:  connect.New(srv.URL, srv.Client()),
		Registry: &fakeRegistry{subjects: []string{"pg.public.orders-value"}},
	}
}

func resumeRegistry(t *testing.T, d Deps) *Registry {
	t.Helper()
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestResumeConnectorExecute(t *testing.T) {
	var gotMethod, gotPath string
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/connectors/pg-orders-connector/resume":
			gotMethod, gotPath = r.Method, r.URL.Path
			w.WriteHeader(http.StatusAccepted)
		case "/connectors/pg-orders-connector/status":
			w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
		default:
			http.NotFound(w, r)
		}
	})
	reg := resumeRegistry(t, d)
	a, _ := reg.Get("connect_resume_connector")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPut || gotPath != "/connectors/pg-orders-connector/resume" {
		t.Fatalf("method/path = %s %s, want PUT /connectors/pg-orders-connector/resume", gotMethod, gotPath)
	}
	if !strings.Contains(out, "resumed pg-orders-connector") || !strings.Contains(out, "RUNNING") {
		t.Fatalf("out %q", out)
	}
}

func TestResumeConnectorValidateUnknownConnector(t *testing.T) {
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors" {
			w.Write([]byte(`["pg-inventory-connector"]`))
			return
		}
		http.NotFound(w, r)
	})
	reg := resumeRegistry(t, d)
	a, _ := reg.Get("connect_resume_connector")
	if err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector"}`)); err == nil {
		t.Fatal("want error for unknown connector")
	}
}

func TestResumeConnectorValidateBadInput(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	a, _ := reg.Get("connect_resume_connector")
	for _, in := range []string{`{}`, `{"connector":""}`, `{`} {
		if err := a.Validate(json.RawMessage(in)); err == nil {
			t.Fatalf("Validate(%q): want error, got nil", in)
		}
	}
}

func TestResumeConnectorExecuteFailure(t *testing.T) {
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("worker exploded"))
	})
	reg := resumeRegistry(t, d)
	a, _ := reg.Get("connect_resume_connector")
	_, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("want status 500 error, got %v", err)
	}
}

func TestResumeConnectorOutOfScope(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	a, _ := reg.Get("connect_resume_connector")
	scoped, ok := a.(Scoped)
	if !ok {
		t.Fatal("connect_resume_connector does not implement Scoped")
	}
	if err := scoped.CheckScope(testPipeline(), json.RawMessage(`{"connector":"pg-inventory-connector"}`)); err == nil {
		t.Fatal("want out-of-scope error for a connector from another pipeline")
	}
}

// noResumeConnect implements clients.ConnectClient but not the local
// connectorResumer adapter: the action must refuse it with a clear error.
type noResumeConnect struct {
	names []string
}

func (f *noResumeConnect) ListConnectors(context.Context) ([]string, error) { return f.names, nil }
func (f *noResumeConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: "pg-orders-connector", State: "RUNNING"}, nil
}
func (f *noResumeConnect) Config(context.Context, string) (map[string]string, error) { return nil, nil }
func (f *noResumeConnect) RestartTask(context.Context, string, int) error            { return nil }
func (f *noResumeConnect) RestartConnector(context.Context, string) error            { return nil }

func TestResumeConnectorUnsupportedClient(t *testing.T) {
	d := testDeps()
	d.Connect = &noResumeConnect{names: []string{"pg-orders-connector"}}
	reg := resumeRegistry(t, d)
	a, _ := reg.Get("connect_resume_connector")
	_, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err == nil || !strings.Contains(err.Error(), "does not support Resume") {
		t.Fatalf("want unsupported-client error, got %v", err)
	}
}
