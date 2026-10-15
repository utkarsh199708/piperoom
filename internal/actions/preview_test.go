package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
)

// pvConnect is a Connect fake that records every call so tests can prove a
// Preview never writes.
type pvConnect struct {
	status   clients.ConnectorStatus
	statErr  error
	cfg      map[string]string
	block    bool
	calls    []string
	validate *validateCall
	vres     connect.ValidateResult
	verr     error
}

type validateCall struct {
	class string
	cfg   map[string]string
}

func (f *pvConnect) rec(s string) { f.calls = append(f.calls, s) }

func (f *pvConnect) ListConnectors(context.Context) ([]string, error) {
	f.rec("ListConnectors")
	return []string{"c"}, nil
}
func (f *pvConnect) Status(ctx context.Context, _ string) (clients.ConnectorStatus, error) {
	f.rec("Status")
	if f.block {
		<-ctx.Done()
		return clients.ConnectorStatus{}, ctx.Err()
	}
	return f.status, f.statErr
}
func (f *pvConnect) Config(context.Context, string) (map[string]string, error) {
	f.rec("Config")
	return f.cfg, nil
}
func (f *pvConnect) RestartTask(context.Context, string, int) error { f.rec("RestartTask"); return nil }
func (f *pvConnect) RestartConnector(context.Context, string) error {
	f.rec("RestartConnector")
	return nil
}
func (f *pvConnect) Resume(context.Context, string) error { f.rec("Resume"); return nil }
func (f *pvConnect) UpdateConfig(context.Context, string, string, string) error {
	f.rec("UpdateConfig")
	return nil
}
func (f *pvConnect) ValidateConfig(_ context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	f.rec("ValidateConfig")
	f.validate = &validateCall{class, cfg}
	return f.vres, f.verr
}

// pvConnectNoValidate hides ValidateConfig.
type pvConnectNoValidate struct{ clients.ConnectClient }

type pvRegistry struct {
	sv         clients.SchemaVersion
	level      string
	subjLevel  bool
	calls      []string
	compatible bool
	msgs       []string
	checkErr   error
}

func (f *pvRegistry) Subjects(context.Context) ([]string, error) { return []string{"s"}, nil }
func (f *pvRegistry) Versions(context.Context, string) ([]int, error) {
	return nil, nil
}
func (f *pvRegistry) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	f.calls = append(f.calls, "Schema")
	return f.sv, nil
}
func (f *pvRegistry) Compatibility(context.Context, string) (string, bool, error) {
	f.calls = append(f.calls, "Compatibility")
	return f.level, f.subjLevel, nil
}
func (f *pvRegistry) SetCompatibility(context.Context, string, string) error {
	f.calls = append(f.calls, "SetCompatibility")
	return nil
}
func (f *pvRegistry) Register(context.Context, string, string) (int, error) {
	f.calls = append(f.calls, "Register")
	return 1, nil
}
func (f *pvRegistry) CheckCompatibility(context.Context, string, string) (bool, []string, error) {
	f.calls = append(f.calls, "CheckCompatibility")
	return f.compatible, f.msgs, f.checkErr
}

type pvRegistryNoCheck struct{ clients.SchemaRegistryClient }

func raw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPreviewRestartTaskReadsStatus(t *testing.T) {
	fc := &pvConnect{status: clients.ConnectorStatus{State: "RUNNING", WorkerID: "w:1",
		Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", WorkerID: "172.18.0.6:8083"}}}}
	a := &restartTask{d: Deps{Connect: fc}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"connector": "c", "task": 0}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Target != "connector c task 0" || !strings.HasPrefix(res.Current, "FAILED on 172.18.0.6:8083") ||
		!strings.Contains(res.Current, "GET /connectors/c/status") || res.Reversible || res.DryRun != nil || res.Downstream != nil {
		t.Fatalf("unexpected preview: %+v", res)
	}
}

func TestPreviewRestartTaskTargetGone(t *testing.T) {
	fc := &pvConnect{statErr: fmt.Errorf("connect: GET: %w", clients.ErrNotFound)}
	a := &restartTask{d: Deps{Connect: fc}}
	_, err := a.Preview(context.Background(), raw(t, map[string]any{"connector": "c", "task": 0}))
	if !errors.Is(err, clients.ErrNotFound) || !strings.HasPrefix(err.Error(), "actions: preview connect_restart_task: ") {
		t.Fatalf("want wrapped ErrNotFound, got %v", err)
	}
}

func TestPreviewRestartConnectorAndResume(t *testing.T) {
	fc := &pvConnect{status: clients.ConnectorStatus{State: "PAUSED", Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED"}}}}
	rc := &restartConnector{d: Deps{Connect: fc}}
	res, err := rc.Preview(context.Background(), raw(t, map[string]any{"connector": "c"}))
	if err != nil || !strings.Contains(res.Current, "task 0 FAILED") || res.Expected != "connector and its failed tasks RUNNING" {
		t.Fatalf("restart connector: %+v %v", res, err)
	}
	rs := &resumeConnector{d: Deps{Connect: fc}}
	res, err = rs.Preview(context.Background(), raw(t, map[string]any{"connector": "c"}))
	if err != nil || !strings.HasPrefix(res.Current, "PAUSED") || res.Reversible {
		t.Fatalf("resume: %+v %v", res, err)
	}
}

func TestPreviewSetCompatibilityReversible(t *testing.T) {
	fr := &pvRegistry{level: "BACKWARD", subjLevel: true}
	a := &setCompatibility{d: Deps{Registry: fr}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"subject": "s-value", "level": "NONE"}))
	if err != nil {
		t.Fatal(err)
	}
	want := `schema_set_compatibility {"level":"BACKWARD","subject":"s-value"}`
	if !res.Reversible || res.Rollback != want || res.Expected != "NONE" || res.DryRun != nil {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestPreviewSetCompatibilityGlobalLevel(t *testing.T) {
	fr := &pvRegistry{level: "BACKWARD", subjLevel: false}
	a := &setCompatibility{d: Deps{Registry: fr}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"subject": "s-value", "level": "NONE"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Reversible || !strings.Contains(res.Current, "BACKWARD (global)") || !strings.Contains(res.Rollback, "needs a lead") {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestPreviewRegisterSchemaDryRun(t *testing.T) {
	params := raw(t, map[string]any{"subject": "s-value", "schema": `{"type":"string"}`})
	sv := clients.SchemaVersion{Version: 2, ID: 7}
	cases := []struct {
		name   string
		reg    clients.SchemaRegistryClient
		wantOK bool
		inText string
		skip   string
	}{
		{"compatible", &pvRegistry{sv: sv, compatible: true}, true, "compatible: true", ""},
		{"incompatible with messages", &pvRegistry{sv: sv, msgs: []string{"field amount type changed"}}, false, "field amount type changed", ""},
		{"incompatible no messages", &pvRegistry{sv: sv}, false, "no detail from this Registry version", ""},
		{"no checker", &pvRegistryNoCheck{&pvRegistry{sv: sv}}, false, "", noDryRun},
		{"check fails", &pvRegistry{sv: sv, checkErr: errors.New("boom")}, false, "", "compatibility check failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := (&registerSchema{d: Deps{Registry: tc.reg}}).Preview(context.Background(), params)
			if err != nil {
				t.Fatal(err)
			}
			if res.Current != "latest version 2 (id 7)" || res.Expected != "a new version 3" || res.Reversible {
				t.Fatalf("unexpected: %+v", res)
			}
			d := res.DryRun
			if d == nil || d.Source != sourceCompat || d.OK != tc.wantOK || !strings.Contains(d.Text, tc.inText) || !strings.Contains(d.Skipped, tc.skip) {
				t.Fatalf("dry run: %+v", d)
			}
		})
	}
}

// A stored value the allow-list no longer offers (double) cannot be restored
// by an action, so the preview must not promise a rollback.
func TestPreviewUpdateConfigStoredDoubleNotReversible(t *testing.T) {
	fc := &pvConnect{cfg: map[string]string{"connector.class": "io.debezium.X", "decimal.handling.mode": "double"}}
	a := &updateConnectorConfig{d: Deps{Connect: fc}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"connector": "c", "key": "decimal.handling.mode", "value": "precise"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Reversible || res.Rollback != "" {
		t.Fatalf("want no rollback for a stored double: %+v", res)
	}
}

func TestPreviewUpdateConfigDryRun(t *testing.T) {
	cfg := map[string]string{"connector.class": "io.debezium.X", "decimal.handling.mode": "precise", "topic.prefix": "pg"}
	fc := &pvConnect{cfg: cfg, vres: connect.ValidateResult{ErrorCount: 2,
		Errors: map[string][]string{"b.key": {"bad"}, "a.key": {"worse"}}}}
	a := &updateConnectorConfig{d: Deps{Connect: fc}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"connector": "c", "key": "decimal.handling.mode", "value": "string"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reversible || res.Rollback != `connect_update_connector_config {"connector":"c","key":"decimal.handling.mode","value":"precise"}` {
		t.Fatalf("rollback: %+v", res)
	}
	if res.DryRun == nil || res.DryRun.OK || res.DryRun.Source != sourceValidate || res.DryRun.Text != "2 errors: a.key: worse; b.key: bad" {
		t.Fatalf("dry run: %+v", res.DryRun)
	}
	if fc.validate.class != "io.debezium.X" || fc.validate.cfg["decimal.handling.mode"] != "string" {
		t.Fatalf("validate sent %+v", fc.validate)
	}
	if cfg["decimal.handling.mode"] != "precise" {
		t.Fatal("Preview mutated the stored config")
	}
	// A client without the validate endpoint is skipped, not failed.
	res, err = (&updateConnectorConfig{d: Deps{Connect: pvConnectNoValidate{fc}}}).Preview(context.Background(),
		raw(t, map[string]any{"connector": "c", "key": "decimal.handling.mode", "value": "string"}))
	if err != nil || res.DryRun == nil || res.DryRun.Skipped != noDryRun {
		t.Fatalf("no validator: %+v %v", res.DryRun, err)
	}
}

func TestPreviewUpdateConfigSkipsSecrets(t *testing.T) {
	fc := &pvConnect{cfg: map[string]string{"connector.class": "io.debezium.X", "database.password": "hunter2", "decimal.handling.mode": "precise"}}
	a := &updateConnectorConfig{d: Deps{Connect: fc}}
	res, err := a.Preview(context.Background(), raw(t, map[string]any{"connector": "c", "key": "decimal.handling.mode", "value": "string"}))
	if err != nil {
		t.Fatal(err)
	}
	if fc.validate != nil {
		t.Fatal("config with secret keys was sent to validate")
	}
	d := res.DryRun
	if d == nil || d.OK || d.Skipped != "the connector config holds secret keys; not sent to validate" {
		t.Fatalf("dry run: %+v", d)
	}
	if b, _ := json.Marshal(res); strings.Contains(string(b), "hunter2") {
		t.Fatalf("preview leaks a config value: %s", b)
	}
}

func TestPreviewUpdateConfigUnsetNotReversible(t *testing.T) {
	fc := &pvConnect{cfg: map[string]string{"connector.class": "x"}}
	res, err := (&updateConnectorConfig{d: Deps{Connect: fc}}).Preview(context.Background(),
		raw(t, map[string]any{"connector": "c", "key": "tombstones.on.delete", "value": "false"}))
	if err != nil || res.Reversible || res.Rollback != "" || !strings.Contains(res.Current, "= unset") {
		t.Fatalf("unset key: %+v %v", res, err)
	}
}

func TestPreviewBadParams(t *testing.T) {
	d := Deps{Connect: &pvConnect{}, Registry: &pvRegistry{}}
	for _, a := range []Action{&restartTask{d: d}, &restartConnector{d: d}, &resumeConnector{d: d},
		&updateConnectorConfig{d: d}, &setCompatibility{d: d}, &registerSchema{d: d}} {
		_, err := a.(Preview).Preview(context.Background(), json.RawMessage(`{}`))
		if err == nil || !strings.HasPrefix(err.Error(), "actions: preview "+a.Name()+": ") {
			t.Errorf("%s: want wrapped error, got %v", a.Name(), err)
		}
	}
}

func TestPreviewNeverWrites(t *testing.T) {
	fc := &pvConnect{
		status: clients.ConnectorStatus{State: "FAILED", Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED"}}},
		cfg:    map[string]string{"connector.class": "x", "decimal.handling.mode": "precise"},
	}
	fr := &pvRegistry{sv: clients.SchemaVersion{Version: 1, ID: 1}, level: "BACKWARD", subjLevel: true, compatible: true}
	d := Deps{Connect: fc, Registry: fr}
	steps := map[Action]any{
		&restartTask{d: d}:           map[string]any{"connector": "c", "task": 0},
		&restartConnector{d: d}:      map[string]any{"connector": "c"},
		&resumeConnector{d: d}:       map[string]any{"connector": "c"},
		&updateConnectorConfig{d: d}: map[string]any{"connector": "c", "key": "decimal.handling.mode", "value": "string"},
		&setCompatibility{d: d}:      map[string]any{"subject": "s", "level": "NONE"},
		&registerSchema{d: d}:        map[string]any{"subject": "s", "schema": `{"type":"string"}`},
	}
	for a, p := range steps {
		if _, err := a.(Preview).Preview(context.Background(), raw(t, p)); err != nil {
			t.Fatalf("%s: %v", a.Name(), err)
		}
	}
	for _, c := range append(append([]string{}, fc.calls...), fr.calls...) {
		switch c {
		case "RestartTask", "RestartConnector", "Resume", "UpdateConfig", "SetCompatibility", "Register":
			t.Errorf("Preview called writer method %s", c)
		}
	}
}

func TestPreviewHonoursDeadline(t *testing.T) {
	fc := &pvConnect{block: true}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (&restartTask{d: Deps{Connect: fc}}).Preview(ctx, raw(t, map[string]any{"connector": "c", "task": 0}))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want deadline error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Preview ignored the context deadline")
	}
}
