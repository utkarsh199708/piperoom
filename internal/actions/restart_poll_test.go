package actions

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// scriptedConnect answers Status from a script: call n returns script[n], and
// the last entry repeats. statusErr, when set, fails every Status call, and
// restartErr fails both restart writes.
type scriptedConnect struct {
	script      []clients.ConnectorStatus
	statusErr   error
	restartErr  error
	statusCalls int
}

func (f *scriptedConnect) ListConnectors(context.Context) ([]string, error) {
	return []string{"pg-orders-connector"}, nil
}

func (f *scriptedConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	n := f.statusCalls
	f.statusCalls++
	if f.statusErr != nil {
		return clients.ConnectorStatus{}, f.statusErr
	}
	if n >= len(f.script) {
		n = len(f.script) - 1
	}
	return f.script[n], nil
}

func (f *scriptedConnect) Config(context.Context, string) (map[string]string, error) {
	return nil, nil
}

func (f *scriptedConnect) RestartTask(context.Context, string, int) error { return f.restartErr }

func (f *scriptedConnect) RestartConnector(context.Context, string) error { return f.restartErr }

// taskStates builds one status per state for task 0 (connector RUNNING).
func taskStates(states ...string) []clients.ConnectorStatus {
	var out []clients.ConnectorStatus
	for _, s := range states {
		out = append(out, clients.ConnectorStatus{
			Name:  "pg-orders-connector",
			State: "RUNNING",
			Tasks: []clients.TaskStatus{{ID: 0, State: s}},
		})
	}
	return out
}

// connectorStates builds one connector-level status per state, with no tasks.
func connectorStates(states ...string) []clients.ConnectorStatus {
	var out []clients.ConnectorStatus
	for _, s := range states {
		out = append(out, clients.ConnectorStatus{Name: "pg-orders-connector", State: s})
	}
	return out
}

// pollCtx returns a context that ends after d, so a never-RUNNING state takes
// the timeout path without waiting for the action's own 10 s limit.
func pollCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

const restartTaskIn = `{"connector":"pg-orders-connector","task":0}`
const restartConnectorIn = `{"connector":"pg-orders-connector"}`

func TestRestartTaskReportsRunningAfterPolling(t *testing.T) {
	sc := &scriptedConnect{script: taskStates("RESTARTING", "RESTARTING", "RUNNING")}
	a := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(context.Background(), json.RawMessage(restartTaskIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "restarted pg-orders-connector task 0 (RUNNING)" {
		t.Fatalf("out %q", out)
	}
	if sc.statusCalls != 3 {
		t.Fatalf("status calls = %d, want 3 (RUNNING on the third poll)", sc.statusCalls)
	}
}

// A task id missing from the status list is not an error: the list may not be
// filled yet, so the action keeps polling.
func TestRestartTaskMissingFromStatusKeepsPolling(t *testing.T) {
	script := []clients.ConnectorStatus{
		{Name: "pg-orders-connector", State: "RUNNING"},
		taskStates("RUNNING")[0],
	}
	sc := &scriptedConnect{script: script}
	a := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(context.Background(), json.RawMessage(restartTaskIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "restarted pg-orders-connector task 0 (RUNNING)" || sc.statusCalls != 2 {
		t.Fatalf("out %q after %d status calls", out, sc.statusCalls)
	}
}

// Connect may never reach RUNNING before the deadline. The restart did succeed,
// so the result is a non-error that says so.
func TestRestartTaskNeverRunningTimesOut(t *testing.T) {
	tests := []struct {
		name   string
		script []clients.ConnectorStatus
		want   string
	}{
		{
			name:   "state reported",
			script: taskStates("FAILED"),
			want:   "restart accepted for pg-orders-connector task 0; state now FAILED (Connect applies restarts asynchronously)",
		},
		{
			name:   "task never listed",
			script: []clients.ConnectorStatus{{Name: "pg-orders-connector", State: "RUNNING"}},
			want:   "restart accepted for pg-orders-connector task 0; state now UNKNOWN (Connect applies restarts asynchronously)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := &scriptedConnect{script: tc.script}
			a := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
			out, err := a.Execute(pollCtx(t, 30*time.Millisecond), json.RawMessage(restartTaskIn))
			if err != nil {
				t.Fatalf("Execute after an accepted restart returned error: %v", err)
			}
			if out != tc.want {
				t.Fatalf("out %q, want %q", out, tc.want)
			}
		})
	}
}

// The restart call succeeded but the status read then fails: the action must
// not return an error, or the agent would retry a restart that worked.
func TestRestartTaskStatusReadFailsAfterRestart(t *testing.T) {
	sc := &scriptedConnect{statusErr: errors.New("connect 503")}
	a := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(context.Background(), json.RawMessage(restartTaskIn))
	if err != nil {
		t.Fatalf("Execute returned error after an accepted restart: %v", err)
	}
	want := "restart accepted for pg-orders-connector task 0; status read failed: connect 503"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

// When the restart call itself fails, the restart did not happen: that is an
// error, and the action must not poll.
func TestRestartTaskRestartCallFails(t *testing.T) {
	boom := errors.New("connect 500")
	sc := &scriptedConnect{restartErr: boom, script: taskStates("RUNNING")}
	a := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	_, err := a.Execute(context.Background(), json.RawMessage(restartTaskIn))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the restart error", err)
	}
	if sc.statusCalls != 0 {
		t.Fatalf("status calls = %d after a failed restart, want 0", sc.statusCalls)
	}
}

func TestRestartConnectorReportsRunningAfterPolling(t *testing.T) {
	sc := &scriptedConnect{script: connectorStates("RESTARTING", "RESTARTING", "RUNNING")}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(context.Background(), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "restarted failed tasks of pg-orders-connector (connector RUNNING)" {
		t.Fatalf("out %q", out)
	}
	if sc.statusCalls != 3 {
		t.Fatalf("status calls = %d, want 3", sc.statusCalls)
	}
}

func TestRestartConnectorNeverRunningTimesOut(t *testing.T) {
	sc := &scriptedConnect{script: connectorStates("FAILED")}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(pollCtx(t, 30*time.Millisecond), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute after an accepted restart returned error: %v", err)
	}
	want := "restart accepted for pg-orders-connector; state now FAILED (Connect applies restarts asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

func TestRestartConnectorStatusReadFailsAfterRestart(t *testing.T) {
	sc := &scriptedConnect{statusErr: errors.New("connect 503")}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(context.Background(), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute returned error after an accepted restart: %v", err)
	}
	want := "restart accepted for pg-orders-connector; status read failed: connect 503"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

func TestRestartConnectorRestartCallFails(t *testing.T) {
	boom := errors.New("connect 500")
	sc := &scriptedConnect{restartErr: boom, script: connectorStates("RUNNING")}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	_, err := a.Execute(context.Background(), json.RawMessage(restartConnectorIn))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the restart error", err)
	}
	if sc.statusCalls != 0 {
		t.Fatalf("status calls = %d after a failed restart, want 0", sc.statusCalls)
	}
}

// compatBeforeReadFails fails only the first Compatibility call, which is the
// read before the write. The later calls use the embedded fake.
type compatBeforeReadFails struct {
	*fakeRegistry
	reads int
}

func (f *compatBeforeReadFails) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	f.reads++
	if f.reads == 1 {
		return "", false, errors.New("registry 503")
	}
	return f.fakeRegistry.Compatibility(ctx, subject)
}

func TestSetCompatibilityReportsOldAndNewLevel(t *testing.T) {
	fr := &fakeRegistry{compatLevel: "BACKWARD"}
	d := testDeps()
	d.Registry = fr
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	if err != nil {
		t.Fatal(err)
	}
	if out != "pg.public.orders-value compatibility BACKWARD → NONE" {
		t.Fatalf("out %q", out)
	}
}

// A failed before-read must not block the write: the old level is reported as
// unknown and the level is still set.
func TestSetCompatibilityBeforeReadFailsStillSets(t *testing.T) {
	fr := &fakeRegistry{compatLevel: "BACKWARD"}
	reads := &compatBeforeReadFails{fakeRegistry: fr}
	d := testDeps()
	d.Registry = reads
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if out != "pg.public.orders-value compatibility unknown → NONE" {
		t.Fatalf("out %q", out)
	}
	if fr.compatLevel != "NONE" {
		t.Fatalf("registry level = %q, want the set to have happened (NONE)", fr.compatLevel)
	}
}
