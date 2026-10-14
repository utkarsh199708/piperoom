package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// scriptedConnect answers Status from a script: call n returns script[n], and
// the last entry repeats. statusErr, when set, fails every Status call, and
// restartErr fails both restart writes. Restarts are refused unless allowWrites
// is set.
type scriptedConnect struct {
	script      []clients.ConnectorStatus
	statusErr   error
	restartErr  error
	statusCalls int
	allowWrites bool
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

func (f *scriptedConnect) RestartTask(context.Context, string, int) error {
	if !f.allowWrites {
		return refuseWrite("scriptedConnect", "RestartTask")
	}
	return f.restartErr
}

func (f *scriptedConnect) RestartConnector(context.Context, string) error {
	if !f.allowWrites {
		return refuseWrite("scriptedConnect", "RestartConnector")
	}
	return f.restartErr
}

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

// connectorWithTasks builds a connector status with one task per entry in
// taskStates, numbered from 0.
func connectorWithTasks(state string, taskStates ...string) clients.ConnectorStatus {
	st := clients.ConnectorStatus{Name: "pg-orders-connector", State: state}
	for i, s := range taskStates {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: i, State: s})
	}
	return st
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
	sc := &scriptedConnect{allowWrites: true, script: taskStates("RESTARTING", "RESTARTING", "RUNNING")}
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
	sc := &scriptedConnect{allowWrites: true, script: script}
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
			sc := &scriptedConnect{allowWrites: true, script: tc.script}
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
	sc := &scriptedConnect{allowWrites: true, statusErr: errors.New("connect 503")}
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
	sc := &scriptedConnect{allowWrites: true, restartErr: boom, script: taskStates("RUNNING")}
	a := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	_, err := a.Execute(context.Background(), json.RawMessage(restartTaskIn))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the restart error", err)
	}
	if sc.statusCalls != 0 {
		t.Fatalf("status calls = %d after a failed restart, want 0", sc.statusCalls)
	}
}

// A connector that is RUNNING while its task 0 is still FAILED is not a success.
// Success waits until the connector and every task are RUNNING.
func TestRestartConnectorWaitsForTasksToRun(t *testing.T) {
	sc := &scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{
		connectorWithTasks("RUNNING", "FAILED"),
		connectorWithTasks("RUNNING", "RESTARTING"),
		connectorWithTasks("RUNNING", "RUNNING"),
	}}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(context.Background(), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "restarted failed tasks of pg-orders-connector (connector and 1 tasks RUNNING)" {
		t.Fatalf("out %q", out)
	}
	if sc.statusCalls != 3 {
		t.Fatalf("status calls = %d, want 3", sc.statusCalls)
	}
}

// Task 0 stays FAILED: the timeout result names it, and it is still not an error.
func TestRestartConnectorTaskStaysFailedTimesOut(t *testing.T) {
	sc := &scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{connectorWithTasks("RUNNING", "FAILED")}}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(pollCtx(t, 30*time.Millisecond), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute after an accepted restart returned error: %v", err)
	}
	want := "restart accepted for pg-orders-connector; connector RUNNING, task 0 FAILED (Connect applies restarts asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

// With no tasks listed, the connector cannot be confirmed as RUNNING with its
// tasks, so the action keeps waiting and reports that no tasks are listed.
func TestRestartConnectorNoTasksListedTimesOut(t *testing.T) {
	sc := &scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{connectorWithTasks("RUNNING")}}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(pollCtx(t, 30*time.Millisecond), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := "restart accepted for pg-orders-connector; connector RUNNING, no tasks listed (Connect applies restarts asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

func TestRestartConnectorStatusReadFailsAfterRestart(t *testing.T) {
	sc := &scriptedConnect{allowWrites: true, statusErr: errors.New("connect 503")}
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
	sc := &scriptedConnect{allowWrites: true, restartErr: boom, script: []clients.ConnectorStatus{connectorWithTasks("RUNNING", "RUNNING")}}
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
	fr := &fakeRegistry{allowWrites: true, compatLevel: "BACKWARD"}
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
	fr := &fakeRegistry{allowWrites: true, compatLevel: "BACKWARD"}
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

// compatReReadFails fails only the second Compatibility call, the read after
// the write. The read before the write succeeds.
type compatReReadFails struct {
	*fakeRegistry
	reads int
}

func (f *compatReReadFails) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	f.reads++
	if f.reads == 2 {
		return "", false, errors.New("registry 503")
	}
	return f.fakeRegistry.Compatibility(ctx, subject)
}

// The re-read-failed result still reports the old level, which the agent needs
// to restore the subject.
func TestSetCompatibilityReReadFailsKeepsOldLevel(t *testing.T) {
	fr := &fakeRegistry{allowWrites: true, compatLevel: "BACKWARD"}
	d := testDeps()
	d.Registry = &compatReReadFails{fakeRegistry: fr}
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	want := "pg.public.orders-value compatibility BACKWARD → NONE (set; re-read failed: registry 503)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
	if fr.compatLevel != "NONE" {
		t.Fatalf("registry level = %q, want the set to have happened (NONE)", fr.compatLevel)
	}
}

// inheritedUntilSet reports the level as inherited from the global default
// (subjectLevel false) until SetCompatibility has run, then as the subject's
// own level.
type inheritedUntilSet struct {
	*fakeRegistry
	set bool
}

func (f *inheritedUntilSet) SetCompatibility(ctx context.Context, subject, level string) error {
	if !f.fakeRegistry.allowWrites {
		return refuseWrite("inheritedUntilSet", "SetCompatibility")
	}
	f.set = true
	return f.fakeRegistry.SetCompatibility(ctx, subject, level)
}

func (f *inheritedUntilSet) Compatibility(ctx context.Context, subject string) (string, bool, error) {
	level, _, err := f.fakeRegistry.Compatibility(ctx, subject)
	return level, f.set, err
}

// A subject that only inherits the global level is marked "(global)" so the
// agent knows the subject had no level of its own.
func TestSetCompatibilityMarksInheritedOldLevel(t *testing.T) {
	fr := &fakeRegistry{allowWrites: true, compatLevel: "BACKWARD"}
	d := testDeps()
	d.Registry = &inheritedUntilSet{fakeRegistry: fr}
	reg := NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("schema_set_compatibility")
	out, err := a.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if out != "pg.public.orders-value compatibility BACKWARD (global) → NONE" {
		t.Fatalf("out %q", out)
	}
}

// The connector itself is not RUNNING while its task is: success is refused and
// the timeout result names the connector.
func TestRestartConnectorConnectorNotRunningTimesOut(t *testing.T) {
	sc := &scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{connectorWithTasks("FAILED", "RUNNING")}}
	a := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	out, err := a.Execute(pollCtx(t, 30*time.Millisecond), json.RawMessage(restartConnectorIn))
	if err != nil {
		t.Fatalf("Execute after an accepted restart returned error: %v", err)
	}
	want := "restart accepted for pg-orders-connector; connector FAILED (Connect applies restarts asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

// A read error after the context has ended is a timeout with the last state
// seen, not an error.
func TestAwaitRunningReadErrorAfterCtxEndedIsTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	state, running, err := awaitRunning(ctx, time.Millisecond, func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "RESTARTING", nil
		}
		cancel()
		return "", errors.New("connect gone")
	})
	if err != nil || running || state != "RESTARTING" {
		t.Fatalf("state %q running %v err %v, want RESTARTING, not running, nil error", state, running, err)
	}
}

// Write failures are wrapped with the action context (AGENTS.md rule 3).
func TestWriteErrorsCarryActionContext(t *testing.T) {
	boom := errors.New("connect 500")
	sc := &scriptedConnect{allowWrites: true, restartErr: boom}
	task := &restartTask{d: Deps{Connect: sc, Registry: &fakeRegistry{}}}
	if _, err := task.Execute(context.Background(), json.RawMessage(restartTaskIn)); err == nil || !strings.Contains(err.Error(), "actions: restart pg-orders-connector task 0") {
		t.Fatalf("restart task error %v", err)
	}
	conn := &restartConnector{d: Deps{Connect: sc, Registry: &fakeRegistry{}}}
	if _, err := conn.Execute(context.Background(), json.RawMessage(restartConnectorIn)); err == nil || !strings.Contains(err.Error(), "actions: restart pg-orders-connector") {
		t.Fatalf("restart connector error %v", err)
	}
	comp := &setCompatibility{d: Deps{Connect: sc, Registry: &compatSetFails{fakeRegistry: &fakeRegistry{allowWrites: true}, err: boom}}}
	if _, err := comp.Execute(context.Background(), json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`)); err == nil || !strings.Contains(err.Error(), "actions: set compatibility pg.public.orders-value") || !errors.Is(err, boom) {
		t.Fatalf("set compatibility error %v", err)
	}
}

// compatSetFails fails SetCompatibility with err.
type compatSetFails struct {
	*fakeRegistry
	err error
}

func (f *compatSetFails) SetCompatibility(context.Context, string, string) error {
	if !f.fakeRegistry.allowWrites {
		return refuseWrite("compatSetFails", "SetCompatibility")
	}
	return f.err
}

// A read error while the context is still live is returned as an error.
func TestAwaitRunningReadErrorWithLiveCtxIsError(t *testing.T) {
	boom := errors.New("connect 503")
	state, running, err := awaitRunning(context.Background(), time.Millisecond, func(context.Context) (string, error) {
		return "", boom
	})
	if !errors.Is(err, boom) || running || state != "" {
		t.Fatalf("state %q running %v err %v, want the read error", state, running, err)
	}
}
