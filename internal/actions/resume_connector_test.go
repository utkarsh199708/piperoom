package actions

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// mustGet returns the registered action name, failing the test with a clear
// message when it is missing.
func mustGet(t *testing.T, reg *Registry, name string) Action {
	t.Helper()
	a, ok := reg.Get(name)
	if !ok {
		t.Fatalf("action %q not registered", name)
	}
	return a
}

// resumeRoutes answers the connector list, the status of pg-orders-connector
// (with the given state) and the resume PUT (with resumeCode).
func resumeRoutes(state string, resumeCode int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/connectors":
			w.Write([]byte(`["pg-orders-connector"]`))
		case "/connectors/pg-orders-connector/status":
			w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"` + state + `"},"tasks":[]}`))
		case "/connectors/pg-orders-connector/resume":
			w.WriteHeader(resumeCode)
		default:
			http.NotFound(w, r)
		}
	}
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
	a := mustGet(t, reg, "connect_resume_connector")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPut || gotPath != "/connectors/pg-orders-connector/resume" {
		t.Fatalf("method/path = %s %s, want PUT /connectors/pg-orders-connector/resume", gotMethod, gotPath)
	}
	if out != "resumed pg-orders-connector (RUNNING)" {
		t.Fatalf("out %q", out)
	}
}

// Connect applies resume asynchronously: a connector that is still PAUSED
// when the context ends is reported as accepted, not as an error.
func TestResumeConnectorExecuteStillPausedIsAccepted(t *testing.T) {
	d := resumeTestDeps(t, resumeRoutes("PAUSED", http.StatusAccepted))
	reg := resumeRegistry(t, d)
	a := mustGet(t, reg, "connect_resume_connector")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	out, err := a.Execute(ctx, json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatalf("Execute after a successful resume returned error: %v", err)
	}
	if !strings.Contains(out, "resume accepted for pg-orders-connector") {
		t.Fatalf("out %q, want it to say the resume was accepted", out)
	}
}

// The resume write succeeded (202) but the status read then fails: the action
// must not return an error, because the agent would retry a resume that worked.
func TestResumeConnectorExecuteAcceptedStatusFails(t *testing.T) {
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/connectors/pg-orders-connector/resume":
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("status unavailable"))
		}
	})
	reg := resumeRegistry(t, d)
	a := mustGet(t, reg, "connect_resume_connector")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatalf("Execute returned error after a successful resume: %v", err)
	}
	if !strings.Contains(out, "resume accepted for pg-orders-connector") || !strings.Contains(out, "status read failed") {
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
	a := mustGet(t, reg, "connect_resume_connector")
	if err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector"}`)); err == nil {
		t.Fatal("want error for unknown connector")
	}
}

// Resume only moves PAUSED or STOPPED connectors; a RUNNING or FAILED one is
// refused at validation time.
func TestResumeConnectorValidateStates(t *testing.T) {
	tests := []struct {
		state   string
		wantErr bool
	}{
		{state: "RUNNING", wantErr: true},
		{state: "FAILED", wantErr: true},
		{state: "PAUSED"},
		{state: "STOPPED"},
	}
	for _, tc := range tests {
		t.Run(tc.state, func(t *testing.T) {
			d := resumeTestDeps(t, resumeRoutes(tc.state, http.StatusAccepted))
			reg := resumeRegistry(t, d)
			a := mustGet(t, reg, "connect_resume_connector")
			err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector"}`))
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("Validate(%s): %v", tc.state, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "resume only applies to PAUSED or STOPPED") {
				t.Fatalf("Validate(%s): want state refusal, got %v", tc.state, err)
			}
		})
	}
}

func TestResumeConnectorValidateStatusError(t *testing.T) {
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors" {
			w.Write([]byte(`["pg-orders-connector"]`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})
	reg := resumeRegistry(t, d)
	a := mustGet(t, reg, "connect_resume_connector")
	if err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector"}`)); err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("want status 500 error from status read, got %v", err)
	}
}

func TestResumeConnectorValidateBadInput(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	a := mustGet(t, reg, "connect_resume_connector")
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
	a := mustGet(t, reg, "connect_resume_connector")
	_, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("want status 500 error, got %v", err)
	}
	if !strings.Contains(err.Error(), "actions: resume pg-orders-connector") {
		t.Fatalf("error %q is not wrapped with the action context", err)
	}
}

func TestResumeConnectorOutOfScope(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	a := mustGet(t, reg, "connect_resume_connector")
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
	a := mustGet(t, reg, "connect_resume_connector")
	_, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err == nil || !strings.Contains(err.Error(), "does not support Resume") {
		t.Fatalf("want unsupported-client error, got %v", err)
	}
}

func TestResumeConnectorValidateKnownConnector(t *testing.T) {
	d := resumeTestDeps(t, resumeRoutes("STOPPED", http.StatusAccepted))
	reg := resumeRegistry(t, d)
	a := mustGet(t, reg, "connect_resume_connector")
	if err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector"}`)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestResumeConnectorExecuteNotFound(t *testing.T) {
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	reg := resumeRegistry(t, d)
	a := mustGet(t, reg, "connect_resume_connector")
	_, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if !strings.Contains(err.Error(), "actions: resume pg-orders-connector") {
		t.Fatalf("error %q is not wrapped with the action context", err)
	}
}

func TestResumeConnectorExecuteConflict(t *testing.T) {
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("rebalance in progress"))
	})
	reg := resumeRegistry(t, d)
	a := mustGet(t, reg, "connect_resume_connector")
	_, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err == nil || !strings.Contains(err.Error(), "status 409") {
		t.Fatalf("want status 409 error, got %v", err)
	}
}

func TestResumeConnectorDescribe(t *testing.T) {
	reg := resumeRegistry(t, testDeps())
	a := mustGet(t, reg, "connect_resume_connector")
	if a.Layer() != "kafka-connect" || a.Describe() == "" {
		t.Fatalf("layer %q, describe %q", a.Layer(), a.Describe())
	}
}
