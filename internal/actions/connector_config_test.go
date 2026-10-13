package actions

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// configConnect is a Connect fake with a mutable config. It implements the
// local connectorConfigUpdater interface the action asserts for.
type configConnect struct {
	scriptedConnect
	cfg       map[string]string
	updateErr error
	storeAs   string // when set, UpdateConfig stores this instead of the value
	rereadErr error  // Config fails after the first successful write
	updates   []string
}

func (f *configConnect) Config(context.Context, string) (map[string]string, error) {
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
		cfg:             map[string]string{"connector.class": "x", "decimal.handling.mode": "precise", "database.password": "hunter2"},
	}
}

func configAction(c *configConnect) *updateConnectorConfig {
	return &updateConnectorConfig{d: Deps{Connect: c, Registry: &fakeRegistry{}}}
}

const updateIn = `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"double"}`

// Live behaviour: right after the PATCH the first status read still shows the
// old RUNNING tasks, so the result says "at first read" and makes no claim
// that a restart finished.
func TestUpdateConnectorConfigExecuteHappyPath(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := "pg-orders-connector decimal.handling.mode: precise → double; status RUNNING at first read (Connect restarts tasks asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
	if len(c.updates) != 1 || c.updates[0] != "pg-orders-connector decimal.handling.mode=double" {
		t.Fatalf("updates %v", c.updates)
	}
	if c.statusCalls != 1 {
		t.Fatalf("status calls %d, want 1 (no polling)", c.statusCalls)
	}
	if strings.Contains(out, "hunter2") || strings.Contains(out, "database.password") {
		t.Fatalf("result leaks other config keys: %q", out)
	}
}

// A task that is not RUNNING at the first read is reported, not an error.
func TestUpdateConnectorConfigTaskNotRunningAtFirstRead(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "FAILED"))
	out, err := configAction(c).Execute(context.Background(), json.RawMessage(updateIn))
	if err != nil {
		t.Fatalf("a non-RUNNING task after the write must not be an error: %v", err)
	}
	want := "pg-orders-connector decimal.handling.mode: precise → double; status connector RUNNING, task 0 FAILED at first read (Connect restarts tasks asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
}

// The full path with the real connect client: one PATCH carrying only the one
// key, so the rest of the config (including the password) is never written.
func TestUpdateConnectorConfigSendsOnlyOneKey(t *testing.T) {
	var patchBody, patchMethod string
	patches := 0
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/connectors":
			w.Write([]byte(`["pg-orders-connector"]`))
		case r.URL.Path == "/connectors/pg-orders-connector/config" && r.Method == http.MethodGet:
			v := "precise"
			if patches > 0 {
				v = "double"
			}
			w.Write([]byte(`{"name":"pg-orders-connector","decimal.handling.mode":"` + v + `","database.password":"hunter2"}`))
		case r.URL.Path == "/connectors/pg-orders-connector/config" && r.Method == http.MethodPatch:
			patches++
			patchMethod = r.Method
			b, _ := io.ReadAll(r.Body)
			patchBody = string(b)
			w.Write([]byte(`{}`))
		case r.URL.Path == "/connectors/pg-orders-connector/status":
			w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
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
	if out != "pg-orders-connector decimal.handling.mode: precise → double; status RUNNING at first read (Connect restarts tasks asynchronously)" {
		t.Fatalf("out %q", out)
	}
	if patchMethod != http.MethodPatch || patchBody != `{"decimal.handling.mode":"double"}` {
		t.Fatalf("%s body %q", patchMethod, patchBody)
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

func TestUpdateConnectorConfigClientErrorsAreWrapped(t *testing.T) {
	sentinel := errors.New("connect: PATCH /connectors/pg-orders-connector/config: status 409")
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

func paramsJSON(t *testing.T, key, value string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(updateConnectorConfigParams{Connector: "pg-orders-connector", Key: key, Value: value})
	if err != nil {
		t.Fatal(err)
	}
	return b
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
		{"ok precise", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"precise"}`, ""},
		{"ok time precision", `{"connector":"pg-orders-connector","key":"time.precision.mode","value":"connect"}`, ""},
		{"ok tombstones", `{"connector":"pg-orders-connector","key":"tombstones.on.delete","value":"false"}`, ""},
		{"ok errors.log.enable", `{"connector":"pg-orders-connector","key":"errors.log.enable","value":"true"}`, ""},
		{"errors.log.enable false refused", `{"connector":"pg-orders-connector","key":"errors.log.enable","value":"false"}`, "not allowed for"},
		{"dlq context headers false refused", `{"connector":"pg-orders-connector","key":"errors.deadletterqueue.context.headers.enable","value":"false"}`, "not allowed for"},
		{"errors.log.enable case variant", `{"connector":"pg-orders-connector","key":"errors.log.enable","value":"True"}`, "not allowed for"},
		{"errors.log.include.messages refused", `{"connector":"pg-orders-connector","key":"errors.log.include.messages","value":"true"}`, "not allowed"},
		{"dlq topic name refused", `{"connector":"pg-orders-connector","key":"errors.deadletterqueue.topic.name","value":"x"}`, "not allowed"},
		{"errors.tolerance removed", `{"connector":"pg-orders-connector","key":"errors.tolerance","value":"none"}`, "not allowed"},
		{"errors.tolerance all removed", `{"connector":"pg-orders-connector","key":"errors.tolerance","value":"all"}`, "not allowed"},
		{"key not allowed", `{"connector":"pg-orders-connector","key":"tasks.max","value":"4"}`, "not allowed"},
		{"bad value", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"float"}`, "not allowed for"},
		{"value of another key", `{"connector":"pg-orders-connector","key":"tombstones.on.delete","value":"double"}`, "not allowed for"},
		{"empty value", `{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":""}`, "not allowed for"},
		{"password key", `{"connector":"pg-orders-connector","key":"database.password","value":"x"}`, "credential"},
		{"secret key", `{"connector":"pg-orders-connector","key":"client.secret","value":"x"}`, "credential"},
		{"token key", `{"connector":"pg-orders-connector","key":"auth.token","value":"x"}`, "credential"},
		{"jaas key", `{"connector":"pg-orders-connector","key":"sasl.jaas.config","value":"x"}`, "credential"},
		{"key converter", `{"connector":"pg-orders-connector","key":"key.converter.basic.auth.user.info","value":"x"}`, "credential"},
		{"mixed case secret", `{"connector":"pg-orders-connector","key":"Database.PASSWORD","value":"x"}`, "credential"},
		{"unknown connector", `{"connector":"pg-inventory-connector","key":"tombstones.on.delete","value":"true"}`, "not found"},
		{"missing connector", `{"key":"tombstones.on.delete","value":"true"}`, "required"},
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

// Keys and values match exactly: no case folding, no trimming, no Unicode
// normalisation. Both Validate and Execute refuse every variant.
func TestUpdateConnectorConfigExactMatchOnly(t *testing.T) {
	const (
		key = "decimal.handling.mode"
		val = "double"
	)
	cases := []struct{ name, key, value string }{
		{"key mixed case", "Decimal.Handling.Mode", val},
		{"key upper case", "DECIMAL.HANDLING.MODE", val},
		{"key leading space", " " + key, val},
		{"key trailing space", key + " ", val},
		{"key trailing zero width space", key + "​", val},
		{"key cyrillic e", "dеcimal.handling.mode", val}, // U+0435
		{"value upper case", key, "DOUBLE"},
		{"value leading space", key, " double"},
		{"value trailing space", key, "double "},
		{"value trailing zero width space", key, "double​"},
		{"value cyrillic lookalike", key, "dоuble"}, // U+043E
		{"value newline", key, "double\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
			a := configAction(c)
			p := paramsJSON(t, tc.key, tc.value)
			if err := a.Validate(p); err == nil {
				t.Fatal("Validate accepted a lookalike")
			}
			if _, err := a.Execute(context.Background(), p); err == nil {
				t.Fatal("Execute accepted a lookalike")
			}
			if len(c.updates) != 0 {
				t.Fatalf("a write happened: %v", c.updates)
			}
		})
	}
}

// Even a list entry added later cannot open up a credential key. The list is
// swapped through the action's test-only field, not a shared variable.
func TestUpdateConnectorConfigSecretCheckBeatsAllowList(t *testing.T) {
	a := &updateConnectorConfig{
		d:     testDeps(),
		allow: map[string]map[string]bool{"database.password": {"x": true}, "decimal.handling.mode": {"double": true}},
	}
	err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector","key":"database.password","value":"x"}`))
	if err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("err %v", err)
	}
	// The override really is in force: only its own entries pass.
	if err := a.Validate(json.RawMessage(updateIn)); err != nil {
		t.Fatalf("override entry refused: %v", err)
	}
	if err := a.Validate(json.RawMessage(`{"connector":"pg-orders-connector","key":"tombstones.on.delete","value":"true"}`)); err == nil {
		t.Fatal("default entry accepted despite the override")
	}
}

func TestDefaultAllowListIsFreshEachCall(t *testing.T) {
	first := defaultConnectorConfigAllowList()
	first["tasks.max"] = map[string]bool{"1": true}
	delete(first, "decimal.handling.mode")
	second := defaultConnectorConfigAllowList()
	if _, bad := second["tasks.max"]; bad {
		t.Fatal("allow-list is shared state")
	}
	if len(second) != 5 {
		t.Fatalf("allow-list %v, want the 5 default keys", sortedKeys(second))
	}
	for _, k := range []string{"errors.tolerance", "errors.log.include.messages"} {
		if _, ok := second[k]; ok {
			t.Fatalf("%s must not be allow-listed", k)
		}
	}
	for _, k := range []string{"errors.log.enable", "errors.deadletterqueue.context.headers.enable"} {
		if len(second[k]) != 1 || !second[k]["true"] {
			t.Fatalf("%s must allow exactly \"true\", got %v", k, second[k])
		}
	}
}

func TestLooksSecret(t *testing.T) {
	for _, k := range []string{"database.password", "sasl.jaas.config", "api.key", "key.converter", "ssl.key.password", "auth_token", "client.secret", "aws.secret.access.key"} {
		if !looksSecret(k) {
			t.Errorf("looksSecret(%q) = false", k)
		}
	}
	for _, k := range sortedKeys(defaultConnectorConfigAllowList()) {
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
	d := a.Describe()
	for _, want := range []string{"old value", "new events only", "output schema or precision"} {
		if !strings.Contains(d, want) {
			t.Errorf("Describe %q lacks %q", d, want)
		}
	}
	// The schema has no enum, so Describe must name every allowed key.
	for _, k := range sortedKeys(defaultConnectorConfigAllowList()) {
		if !strings.Contains(d, k) {
			t.Errorf("Describe does not name %q", k)
		}
	}
	for _, bad := range []string{"low-risk", "errors.tolerance"} {
		if strings.Contains(d, bad) {
			t.Errorf("Describe contains %q", bad)
		}
	}
	props := a.ParamsSchema()["properties"].(map[string]any)
	for _, name := range []string{"connector", "key", "value"} {
		if _, ok := props[name]; !ok {
			t.Errorf("schema lacks %q", name)
		}
	}
}

// errors.log.enable=true is visibility only, except where it would start
// writing record values into the worker log.
func TestVisibilityGuards(t *testing.T) {
	const logIn = `{"connector":"pg-orders-connector","key":"errors.log.enable","value":"true"}`
	const hdrIn = `{"connector":"pg-orders-connector","key":"errors.deadletterqueue.context.headers.enable","value":"true"}`
	sink := "io.confluent.connect.jdbc.JdbcSinkConnector"
	tests := []struct {
		name    string
		cfg     map[string]string
		in      string
		wantErr string
	}{
		{"log enable ok", map[string]string{"connector.class": sink}, logIn, ""},
		{"log enable, include.messages false", map[string]string{"errors.log.include.messages": "false"}, logIn, ""},
		{"log enable refused with include.messages", map[string]string{"errors.log.include.messages": "true"}, logIn, "turn errors.log.include.messages off first"},
		{"include.messages any case", map[string]string{"errors.log.include.messages": " TRUE "}, logIn, "include.messages"},
		{"headers ok on sink", map[string]string{"connector.class": sink}, hdrIn, ""},
		{"headers refused on source", map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}, hdrIn, "sink connector"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
			c.cfg = tc.cfg
			a := configAction(c)
			check := func(what string, err error) {
				t.Helper()
				switch {
				case tc.wantErr == "" && err != nil:
					t.Fatalf("%s: %v", what, err)
				case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
					t.Fatalf("%s: err = %v, want %q", what, err, tc.wantErr)
				}
			}
			check("Validate", a.Validate(json.RawMessage(tc.in)))
			_, err := a.Execute(context.Background(), json.RawMessage(tc.in))
			check("Execute", err)
			if tc.wantErr != "" && len(c.updates) != 0 {
				t.Fatalf("a refused change was written: %v", c.updates)
			}
		})
	}
}
