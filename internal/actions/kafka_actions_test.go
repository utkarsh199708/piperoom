package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/internal/clients"
)

// errWriteRefused is returned by a fake's write method unless the test sets its
// allowWrites flag, so a write that a test did not ask for fails loudly.
var errWriteRefused = errors.New("fake write refused: allowWrites is off")

// refuseWrite counts the refusal (so guardWrites can fail the test) and
// returns the wrapped error.
func refuseWrite(refusals *atomic.Int32, fake, method string) error {
	refusals.Add(1)
	return fmt.Errorf("%s.%s: %w", fake, method, errWriteRefused)
}

// writeCounter is implemented by every fake that has write methods.
type writeCounter interface{ refusedWrites() int32 }

// guardWrites fails the test at cleanup if any of the fakes refused a write,
// so a path that swallows the refusal error still fails.
func guardWrites(t *testing.T, fakes ...writeCounter) {
	t.Helper()
	t.Cleanup(func() {
		for _, f := range fakes {
			if n := f.refusedWrites(); n > 0 {
				t.Errorf("%d write(s) refused by a fake: the test reached a write it did not opt in to", n)
			}
		}
	})
}

// guardDeps runs guardWrites on every fake in d. An action must write only
// through the client it is meant to change; a stray write to the other one
// fails the test.
func guardDeps(t *testing.T, d Deps) {
	t.Helper()
	var fakes []writeCounter
	for _, c := range []any{d.Connect, d.Registry} {
		if w, ok := c.(writeCounter); ok {
			fakes = append(fakes, w)
		}
	}
	guardWrites(t, fakes...)
}

// testDeps opts in to writes: the action tests that use it execute actions
// whose whole point is a write. Read-only tests must build their own fakes.
func testDeps() Deps {
	return Deps{
		Connect:  &fakeConnect{names: []string{"pg-orders-connector"}, allowWrites: true},
		Registry: &fakeRegistry{subjects: []string{"pg.public.orders-value"}, allowWrites: true},
	}
}

type fakeConnect struct {
	names       []string
	restarted   string
	restartTask int
	resumed     string
	// allowWrites lets the write methods record their call. Off by default.
	allowWrites bool
	refusals    atomic.Int32
}

func (f *fakeConnect) refusedWrites() int32 { return f.refusals.Load() }

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) { return f.names, nil }
func (f *fakeConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: "pg-orders-connector", State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}, nil
}
func (f *fakeConnect) Config(context.Context, string) (map[string]string, error) { return nil, nil }
func (f *fakeConnect) RestartTask(_ context.Context, n string, task int) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "fakeConnect", "RestartTask")
	}
	f.restarted, f.restartTask = n, task
	return nil
}
func (f *fakeConnect) RestartConnector(_ context.Context, n string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "fakeConnect", "RestartConnector")
	}
	f.restarted = n
	return nil
}
func (f *fakeConnect) Resume(_ context.Context, n string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "fakeConnect", "Resume")
	}
	f.resumed = n
	return nil
}

type fakeRegistry struct {
	subjects    []string
	compatLevel string
	compatErr   error
	regID       int
	// allowWrites lets SetCompatibility and Register take effect. Off by default.
	allowWrites bool
	refusals    atomic.Int32
}

func (f *fakeRegistry) refusedWrites() int32 { return f.refusals.Load() }

func (f *fakeRegistry) Subjects(context.Context) ([]string, error)      { return f.subjects, nil }
func (f *fakeRegistry) Versions(context.Context, string) ([]int, error) { return nil, nil }
func (f *fakeRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{}, nil
}
func (f *fakeRegistry) Compatibility(context.Context, string) (string, bool, error) {
	if f.compatErr != nil {
		return "", false, f.compatErr
	}
	return f.compatLevel, true, nil
}
func (f *fakeRegistry) SetCompatibility(_ context.Context, _ string, level string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "fakeRegistry", "SetCompatibility")
	}
	f.compatLevel = level
	return nil
}
func (f *fakeRegistry) Register(context.Context, string, string) (int, error) {
	if !f.allowWrites {
		return 0, refuseWrite(&f.refusals, "fakeRegistry", "Register")
	}
	return f.regID, nil
}

// TestInScopeSubject pins the exact-match rule: a subject is in scope only
// when it is "<TopicPrefix>.<source_table>-key" or "-value" for one of the
// pipeline's tables. "billing.public.orders-value" was wrongly accepted by
// the old suffix match.
func TestInScopeSubject(t *testing.T) {
	p := testPipeline()
	tests := []struct {
		name    string
		subject string
		wantErr bool
	}{
		{name: "value subject", subject: "pg.public.orders-value"},
		{name: "key subject", subject: "pg.public.orders-key"},
		{name: "foreign prefix refused", subject: "billing.public.orders-value", wantErr: true},
		{name: "missing prefix refused", subject: "public.orders-value", wantErr: true},
		{name: "wrong table refused", subject: "pg.public.other-value", wantErr: true},
		{name: "no key or value suffix refused", subject: "pg.public.orders", wantErr: true},
		{name: "empty subject refused", subject: "", wantErr: true},
		{name: "prefix must match exactly refused", subject: "xpg.public.orders-value", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := inScopeSubject(p, tc.subject)
			if tc.wantErr && err == nil {
				t.Fatalf("inScopeSubject(%q): want error, got nil", tc.subject)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("inScopeSubject(%q): want nil, got %v", tc.subject, err)
			}
		})
	}

	t.Run("empty topic_prefix refused", func(t *testing.T) {
		empty := p
		empty.TopicPrefix = ""
		// ".public.orders-value" matches the bare "." + table rule an empty
		// prefix produces; the guard must refuse before comparing.
		if err := inScopeSubject(empty, ".public.orders-value"); err == nil {
			t.Fatal(`want error for ".public.orders-value" with no topic_prefix`)
		}
	})
}

// TestCheckScopeBadParamsWrapsJSONError checks that CheckScope wraps the
// decoding error so callers can unwrap it with errors.As.
func TestCheckScopeBadParamsWrapsJSONError(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	scoped, ok := a.(Scoped)
	if !ok {
		t.Fatal("schema_set_compatibility does not implement Scoped")
	}
	err := scoped.CheckScope(testPipeline(), json.RawMessage(`{`))
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("want wrapped *json.SyntaxError, got %v", err)
	}
}

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
	d.Registry = &fakeRegistry{regID: 42, allowWrites: true}
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

func TestSchemaRegisterRejectsNonAvro(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_register")
	// Valid JSON but not a valid Avro schema.
	if err := a.Validate(json.RawMessage(`{"subject":"x","schema":"{\"foo\": 1}"}`)); err == nil {
		t.Fatal("want error for non-Avro schema")
	}
}

func TestRestartConnectorExecute(t *testing.T) {
	d := testDeps()
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("connect_restart_connector")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "restarted failed tasks of pg-orders-connector") {
		t.Fatalf("out %q", out)
	}
}

func TestSetCompatibilityExecute(t *testing.T) {
	d := testDeps()
	d.Registry = &fakeRegistry{compatLevel: "FULL", allowWrites: true}
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	if err != nil {
		t.Fatal(err)
	}
	if out != "pg.public.orders-value compatibility FULL → NONE" {
		t.Fatalf("out %q", out)
	}
}

func TestSetCompatibilityReReadFails(t *testing.T) {
	d := testDeps()
	d.Registry = &fakeRegistry{compatErr: errors.New("registry down"), allowWrites: true}
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "set; re-read failed") {
		t.Fatalf("out %q", out)
	}
}
