package actions

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// configConnect is a Connect fake with a mutable config. It implements the
// local connectorConfigUpdater interface the action asserts for.
type configConnect struct {
	scriptedConnect
	cfg         map[string]string
	updateErr   error
	storeAs     string // when set, UpdateConfig stores this instead of the value
	rereadErr   error  // Config fails after the first successful write
	updates     []string
	configCalls int
}

func (f *configConnect) Config(context.Context, string) (map[string]string, error) {
	f.configCalls++
	if f.rereadErr != nil && len(f.updates) > 0 {
		return nil, f.rereadErr
	}
	out := map[string]string{}
	for k, v := range f.cfg {
		out[k] = v
	}
	return out, nil
}

func (f *configConnect) UpdateConfig(_ context.Context, name, key, value string) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updates = append(f.updates, name+" "+key+"="+value)
	if f.storeAs != "" {
		value = f.storeAs
	}
	f.cfg[key] = value
	return nil
}

func newConfigConnect(script ...clients.ConnectorStatus) *configConnect {
	return &configConnect{
		scriptedConnect: scriptedConnect{script: script},
		cfg:             map[string]string{"connector.class": "x", "decimal.handling.mode": "precise"},
	}
}

func configAction(c *configConnect) *updateConnectorConfig {
	return &updateConnectorConfig{d: Deps{Connect: c, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
}

const updateIn = `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"double"}`

func TestUpdateConnectorConfigExecuteHappyPath(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RESTARTING"), connectorWithTasks("RUNNING", "RUNNING"))
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := "pg-orders-connector decimal.handling.mode: precise → double; connector and 1 tasks RUNNING"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
	if len(c.updates) != 1 || c.updates[0] != "pg-orders-connector decimal.handling.mode=double" {
		t.Fatalf("updates %v", c.updates)
	}
	if c.statusCalls != 2 {
		t.Fatalf("status calls %d, want 2", c.statusCalls)
	}
}

// The full path with the real connect client: the PUT body is the whole config
// with only the one key changed.
func TestUpdateConnectorConfigPutBodyHasFullMap(t *testing.T) {
	stored := map[string]string{
		"name": "pg-orders-connector", "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "precise", "slot.name": "orders_slot",
	}
	var putBody map[string]string
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/connectors":
			w.Write([]byte(`["pg-orders-connector"]`))
		case r.URL.Path == "/connectors/pg-orders-connector/config" && r.Method == http.MethodGet:
			json.NewEncoder(w).Encode(stored)
		case r.URL.Path == "/connectors/pg-orders-connector/config" && r.Method == http.MethodPut:
			if err := json.NewDecoder(r.Body).Decode(&putBody); err != nil {
				t.Errorf("bad PUT body: %v", err)
			}
			stored = putBody
			w.Write([]byte(`{}`))
		case r.URL.Path == "/connectors/pg-orders-connector/status":
			w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"},{"id":1,"state":"RUNNING"}]}`))
		default:
			http.NotFound(w, r)
		}
	})
	a := mustGet(t, resumeRegistry(t, d), "connect_update_connector_config")
	if err := a.Validate(json.RawMessage(updateIn)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	out, err := a.Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != "pg-orders-connector decimal.handling.mode: precise → double; connector and 2 tasks RUNNING" {
		t.Fatalf("out %q", out)
	}
	if len(putBody) != 4 || putBody["decimal.handling.mode"] != "double" ||
		putBody["slot.name"] != "orders_slot" || putBody["name"] != "pg-orders-connector" ||
		putBody["connector.class"] != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Fatalf("PUT body %v", putBody)
	}
}

func TestUpdateConnectorConfigUnsetOldValue(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	delete(c.cfg, "decimal.handling.mode")
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "decimal.handling.mode: unset → double;") {
		t.Fatalf("out %q", out)
	}
}

func TestUpdateConnectorConfigAlreadySetMakesNoWrite(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.cfg["decimal.handling.mode"] = "double"
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.updates) != 0 || !strings.Contains(out, "already double") {
		t.Fatalf("out %q, updates %v", out, c.updates)
	}
}

func TestUpdateConnectorConfigRereadMismatch(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.storeAs = "precise"
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("a mismatch after the write must not be an error: %v", err)
	}
	if !strings.Contains(out, "re-read shows decimal.handling.mode = precise") {
		t.Fatalf("out %q", out)
	}
}

func TestUpdateConnectorConfigRereadFailsIsNotError(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.rereadErr = errors.New("boom")
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("error after the write: %v", err)
	}
	if !strings.Contains(out, "written") || !strings.Contains(out, "re-read of the config failed") {
		t.Fatalf("out %q", out)
	}
}

func TestUpdateConnectorConfigStatusFailsIsNotError(t *testing.T) {
	c := newConfigConnect()
	c.statusErr = errors.New("status unavailable")
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("error after the write: %v", err)
	}
	if !strings.Contains(out, "written; status read failed") {
		t.Fatalf("out %q", out)
	}
}

// Tasks that never come back RUNNING: the write succeeded, so the result is a
// non-error with the restart wording.
func TestUpdateConnectorConfigTasksNeverRunning(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "FAILED"))
	out, err := configAction(c).Execute(pollCtx(t, 40*time.Millisecond), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("timeout after the write must not be an error: %v", err)
	}
	want := "pg-orders-connector decimal.handling.mode: precise → double; connector RUNNING, task 0 FAILED (Connect applies restarts asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

func TestUpdateConnectorConfigClientErrorsAreWrapped(t *testing.T) {
	sentinel := errors.New("connect: PUT /connectors/pg-orders-connector/config: status 409")
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.updateErr = sentinel
	_, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if !errors.Is(err, sentinel) || !strings.HasPrefix(err.Error(), "actions: update config pg-orders-connector: ") {
		t.Fatalf("err %v", err)
	}
}

func TestUpdateConnectorConfigClientWithoutUpdateConfig(t *testing.T) {
	a := &updateConnectorConfig{d: Deps{Connect: &fakeConnect{names: []string{"pg-orders-connector"}}, Registry: &fakeRegistry{}}}
	_, err := a.Execute(context.Background(), json.RawMessage(updateIn))
	if err == nil || !strings.Contains(err.Error(), "does not support UpdateConfig") {
		t.Fatalf("err %v", err)
	}
}

func TestUpdateConnectorConfigValidate(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	tests := []struct {
		name    string
		params  string
		wantErr string // substring; empty means no error
	}{
		{"ok double", updateIn, ""},
		{"ok string", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"string"}`, ""},
		{"ok time precision", `{"connector":"pg-orders-connector","key":"time.precision.mode","value":"connect"}`, ""},
		{"ok tombstones", `{"connector":"pg-orders-connector","key":"tombstones.on.delete","value":"false"}`, ""},
		{"ok errors tolerance", `{"connector":"pg-orders-connector","key":"errors.tolerance","value":"all"}`, ""},
		{"key not allowed", `{"connector":"pg-orders-connector","key":"tasks.max","value":"4"}`, "not allowed"},
		{"bad value", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"float"}`, "not allowed for"},
		{"value of another key", `{"connector":"pg-orders-connector","key":"errors.tolerance","value":"double"}`, "not allowed for"},
		{"empty value", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":""}`, "not allowed for"},
		{"password key", `{"connector":"pg-orders-connector","key":"database.password","value":"x"}`, "credential"},
		{"secret key", `{"connector":"pg-orders-connector","key":"client.secret","value":"x"}`, "credential"},
		{"token key", `{"connector":"pg-orders-connector","key":"auth.token","value":"x"}`, "credential"},
		{"jaas key", `{"connector":"pg-orders-connector","key":"sasl.jaas.config","value":"x"}`, "credential"},
		{"key converter", `{"connector":"pg-orders-connector","key":"key.converter.basic.auth.user.info","value":"x"}`, "credential"},
		{"mixed case secret", `{"connector":"pg-orders-connector","key":"Database.PASSWORD","value":"x"}`, "credential"},
		{"unknown connector", `{"connector":"pg-inventory-connector","key":"errors.tolerance","value":"all"}`, "not found"},
		{"missing connector", `{"key":"errors.tolerance","value":"all"}`, "required"},
		{"bad json", `{`, "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Validate(json.RawMessage(tc.params))
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("want nil, got %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// Even a list entry added later cannot open up a credential key.
func TestUpdateConnectorConfigSecretCheckBeatsAllowList(t *testing.T) {
	const key = "database.password"
	connectorConfigAllowList[key] = map[string]bool{"x": true}
	t.Cleanup(func() { delete(connectorConfigAllowList, key) })
	a := &updateConnectorConfig{d: testDeps()}
	err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector","key":"database.password","value":"x"}`))
	if err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("err %v", err)
	}
}

func TestLooksSecret(t *testing.T) {
	for _, k := range []string{"database.password", "sasl.jaas.config", "api.key", "key.converter", "ssl.key.password", "auth_token", "client.secret", "aws.secret.access.key"} {
		if !looksSecret(k) {
			t.Errorf("looksSecret(%q) = false", k)
		}
	}
	for _, k := range connectorConfigKeys() {
		if looksSecret(k) {
			t.Errorf("allow-listed key %q looks secret", k)
		}
	}
}

func TestUpdateConnectorConfigScope(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	if err := a.CheckScope(testPipeline(), json.RawMessage(updateIn)); err != nil {
		t.Fatalf("in scope: %v", err)
	}
	other := `{"connector":"pg-inventory-connector","key":"decimal.handling.mode","value":"double"}`
	if err := a.CheckScope(testPipeline(), json.RawMessage(other)); err == nil {
		t.Fatal("want error for another pipeline's connector")
	}
	if err := a.CheckScope(testPipeline(), json.RawMessage(`{`)); err == nil {
		t.Fatal("want error for bad params")
	}
}

func TestUpdateConnectorConfigSchemaAndRegistry(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a := mustGet(t, reg, "connect_update_connector_config")
	if a.Layer() != "kafka-connect" {
		t.Errorf("layer %q", a.Layer())
	}
	if _, ok := a.(Scoped); !ok {
		t.Error("action is not Scoped")
	}
	if !strings.Contains(a.Describe(), "old value") {
		t.Errorf("Describe %q should say how to undo", a.Describe())
	}
	props := a.ParamsSchema()["properties"].(map[string]any)
	keyProp := props["key"].(map[string]any)
	enum, _ := keyProp["enum"].([]string)
	if len(enum) != len(connectorConfigAllowList) {
		t.Errorf("key enum %v", enum)
	}
	for _, name := range []string{"connector", "key", "value"} {
		if _, ok := props[name]; !ok {
			t.Errorf("schema lacks %q", name)
		}
	}
}
