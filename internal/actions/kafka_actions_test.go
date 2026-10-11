package actions

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func testDeps() Deps {
	return Deps{
		Connect:  &fakeConnect{names: []string{"pg-orders-connector"}},
		Registry: &fakeRegistry{subjects: []string{"pg.public.orders-value"}},
	}
}

type fakeConnect struct {
	names       []string
	restarted   string
	restartTask int
}

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) { return f.names, nil }
func (f *fakeConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: "pg-orders-connector", State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}, nil
}
func (f *fakeConnect) Config(context.Context, string) (map[string]string, error) { return nil, nil }
func (f *fakeConnect) RestartTask(_ context.Context, n string, task int) error {
	f.restarted, f.restartTask = n, task
	return nil
}
func (f *fakeConnect) RestartConnector(_ context.Context, n string) error {
	f.restarted = n
	return nil
}

type fakeRegistry struct {
	subjects    []string
	compatLevel string
	regID       int
}

func (f *fakeRegistry) Subjects(context.Context) ([]string, error)      { return f.subjects, nil }
func (f *fakeRegistry) Versions(context.Context, string) ([]int, error) { return nil, nil }
func (f *fakeRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, nil
}
func (f *fakeRegistry) Compatibility(context.Context, string) (string, bool, error) {
	return f.compatLevel, true, nil
}
func (f *fakeRegistry) SetCompatibility(_ context.Context, _ string, level string) error {
	f.compatLevel = level
	return nil
}
func (f *fakeRegistry) Register(context.Context, string, string) (int, error) { return f.regID, nil }

func TestValidateUnknownConnector(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("connect_restart_task")
	if err := a.Validate(json.RawMessage(`{"connector":"nope","task":0}`)); err == nil {
		t.Fatal("want error for unknown connector")
	}
}

func TestValidateBadLevel(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	if err := a.Validate(json.RawMessage(`{"subject":"pg.public.orders-value","level":"BOGUS"}`)); err == nil {
		t.Fatal("want error for bad level")
	}
}

func TestRestartTaskExecute(t *testing.T) {
	d := testDeps()
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("connect_restart_task")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector","task":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "restarted pg-orders-connector task 0") {
		t.Fatalf("out %q", out)
	}
}

func TestRegisterSchemaExecute(t *testing.T) {
	d := testDeps()
	d.Registry = &fakeRegistry{regID: 42}
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_register")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"x","schema":"{\"type\":\"string\"}"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "schema id 42") {
		t.Fatalf("out %q", out)
	}
}

func TestSchemaRegisterRejectsBadJSON(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_register")
	if err := a.Validate(json.RawMessage(`{"subject":"x","schema":"not json"}`)); err == nil {
		t.Fatal("want error for invalid schema JSON")
	}
}
