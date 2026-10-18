package actions

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// pauseConnect is a scripted Connect fake that implements
// clients.ConnectFixWriter. PauseConnector is refused unless allowWrites is
// set; UpdateConnectorConfigKeys is always refused (pause tests never change
// config).
type pauseConnect struct {
	scriptedConnect
	paused   string
	pauseErr error
}

func (f *pauseConnect) PauseConnector(_ context.Context, name string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "pauseConnect", "PauseConnector")
	}
	if f.pauseErr != nil {
		return f.pauseErr
	}
	f.paused = name
	return nil
}

func (f *pauseConnect) UpdateConnectorConfigKeys(context.Context, string, map[string]string) error {
	return refuseWrite(&f.refusals, "pauseConnect", "UpdateConnectorConfigKeys")
}

var (
	_ clients.ConnectFixWriter = (*pauseConnect)(nil)
	_ clients.ConnectFixWriter = (*configConnect)(nil)
)

const pauseIn = `{"connector":"pg-orders-connector"}`

func pauseAction(t *testing.T, c clients.ConnectClient) *pauseConnector {
	t.Helper()
	a := &pauseConnector{d: Deps{Connect: c, Registry: &fakeRegistry{}}, pollInterval: time.Millisecond}
	guardDeps(t, a.d)
	return a
}

// scoped returns a ctx carrying the test pipeline, as the room's executor and
// preview pass it (P3-0).
func scoped() context.Context {
	return tools.WithPipeline(context.Background(), testPipeline())
}

// otherPipeline is a room pipeline that does not own pg-orders-connector.
func otherPipeline() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "pg-inventory", TopicPrefix: "inv", Connectors: []string{"pg-inventory-connector"}})
}

func TestPauseExecuteHappyPath(t *testing.T) {
	c := &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{
		connectorWithTasks("RUNNING", "RUNNING"), connectorWithTasks("PAUSED", "PAUSED"),
	}}}
	out, err := pauseAction(t, c).Execute(scoped(), json.RawMessage(pauseIn))
	if err != nil {
		t.Fatal(err)
	}
	if c.paused != "pg-orders-connector" || out != "paused pg-orders-connector (PAUSED); undo with connect_resume_connector" {
		t.Fatalf("paused %q, out %q", c.paused, out)
	}
}

func TestPauseExecuteAlreadyPausedWritesNothing(t *testing.T) {
	c := &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{connectorWithTasks("PAUSED", "PAUSED")}}}
	out, err := pauseAction(t, c).Execute(scoped(), json.RawMessage(pauseIn))
	if err != nil || c.paused != "" || out != "pg-orders-connector is already PAUSED; no change made" {
		t.Fatalf("out %q err %v paused %q", out, err, c.paused)
	}
	fc := &pvConnect{status: connectorWithTasks("PAUSED", "PAUSED")}
	res, err := (&pauseConnector{d: Deps{Connect: fc}}).Preview(scoped(), json.RawMessage(pauseIn))
	if err != nil || res.Rollback != "" || res.Reversible || !strings.Contains(res.Expected, "no change") {
		t.Fatalf("preview of a paused connector: %+v %v", res, err)
	}
}

func TestPauseExecuteNotYetPausedIsNotError(t *testing.T) {
	c := &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{connectorWithTasks("RUNNING", "RUNNING")}}}
	ctx, cancel := context.WithTimeout(scoped(), 30*time.Millisecond)
	defer cancel()
	out, err := pauseAction(t, c).Execute(ctx, json.RawMessage(pauseIn))
	if err != nil || !strings.Contains(out, "pause accepted") || !strings.Contains(out, "state now RUNNING") {
		t.Fatalf("out %q err %v", out, err)
	}
}

func TestPauseExecuteStatusFailsAfterWriteIsNotError(t *testing.T) {
	c := &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: true, statusErr: errors.New("boom")}}
	out, err := pauseAction(t, c).Execute(scoped(), json.RawMessage(pauseIn))
	if err != nil || !strings.Contains(out, "status read failed") {
		t.Fatalf("out %q err %v", out, err)
	}
}

func TestPauseExecuteRefusals(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		conn    clients.ConnectClient
		in      string
		wantErr string
		wantIs  error
	}{
		{"other pipeline's connector", otherPipeline(), &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: true}}, pauseIn, "not part of this room's pipeline", tools.ErrOutOfScope},
		{"read-only client (write mode off)", scoped(), &fakeConnect{names: []string{"pg-orders-connector"}}, pauseIn, "does not support PauseConnector", nil},
		{"write fails", scoped(), &pauseConnect{scriptedConnect: scriptedConnect{allowWrites: true, script: []clients.ConnectorStatus{connectorWithTasks("RUNNING", "RUNNING")}}, pauseErr: errors.New("status 409")}, pauseIn, "actions: pause pg-orders-connector: status 409", nil},
		{"missing connector", scoped(), &pauseConnect{}, `{}`, "required", nil},
		{"bad json", scoped(), &pauseConnect{}, `{`, "required", nil},
		{"oversize name", scoped(), &pauseConnect{}, `{"connector":"` + strings.Repeat("x", 300) + `"}`, "longer than", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := &pauseConnector{d: Deps{Connect: tc.conn}}
			_, err := a.Execute(tc.ctx, json.RawMessage(tc.in))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err %v, want %q", err, tc.wantErr)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err %v, want errors.Is %v", err, tc.wantIs)
			}
			if pc, ok := tc.conn.(*pauseConnect); ok && pc.paused != "" {
				t.Fatal("a refused pause was written")
			}
		})
	}
}

func TestPauseValidate(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		state   string
		in      string
		wantErr string
	}{
		{"running", context.Background(), "RUNNING", pauseIn, ""},
		{"failed can be paused", context.Background(), "FAILED", pauseIn, ""},
		{"scoped running", scoped(), "RUNNING", pauseIn, ""},
		{"already paused is a no-op, not a refusal", context.Background(), "PAUSED", pauseIn, ""},
		{"stopped", context.Background(), "STOPPED", pauseIn, ""},
		{"unknown connector", context.Background(), "RUNNING", `{"connector":"nope"}`, "not found"},
		{"other pipeline in ctx", otherPipeline(), "RUNNING", pauseIn, "not part of this room's pipeline"},
		{"missing connector", context.Background(), "RUNNING", `{}`, "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A read-only client: Validate must not need the writer, so the
			// agent can propose with write mode off.
			c := &scriptedConnect{script: []clients.ConnectorStatus{connectorWithTasks(tc.state, "RUNNING")}}
			err := pauseAction(t, c).Validate(tc.ctx, json.RawMessage(tc.in))
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("want nil, got %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestPausePreview(t *testing.T) {
	st := connectorWithTasks("RUNNING", "RUNNING", "RUNNING")
	st.Type = "source"
	fc := &pvConnect{status: st}
	a := &pauseConnector{d: Deps{Connect: fc}}
	res, err := a.Preview(scoped(), raw(t, map[string]any{"connector": "pg-orders-connector"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reversible || res.Rollback != `connect_resume_connector {"connector":"pg-orders-connector"}` {
		t.Fatalf("rollback: %+v", res)
	}
	for _, want := range []string{"PAUSED", "lag grow", "no data is lost", "retains WAL"} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("Expected %q lacks %q", res.Expected, want)
		}
	}
	if !strings.Contains(res.Current, "connector and 2 tasks RUNNING") {
		t.Errorf("Current %q", res.Current)
	}
	for _, c := range fc.calls {
		if c != "Status" {
			t.Errorf("Preview called %s; only the status read is allowed", c)
		}
	}
	if _, err := a.Preview(otherPipeline(), raw(t, map[string]any{"connector": "pg-orders-connector"})); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope preview: %v", err)
	}
}

func TestPauseCheckScopeAndRegistry(t *testing.T) {
	reg := NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	a := mustGet(t, reg, "connect_pause_connector")
	if a.Layer() != "kafka-connect" {
		t.Errorf("layer %q", a.Layer())
	}
	sc := a.(Scoped)
	if err := sc.CheckScope(testPipeline(), json.RawMessage(pauseIn)); err != nil {
		t.Fatalf("in scope: %v", err)
	}
	if err := sc.CheckScope(testPipeline(), json.RawMessage(`{"connector":"pg-inventory-connector"}`)); err == nil {
		t.Fatal("other pipeline's connector passed CheckScope")
	}
	if err := sc.CheckScope(testPipeline(), json.RawMessage(`{`)); err == nil {
		t.Fatal("bad params passed CheckScope")
	}
	if !strings.Contains(a.Describe(), "connect_resume_connector") {
		t.Error("Describe does not name the undo")
	}
}

// The full path with the real connect client: the PUT is sent only with the
// room's whole pipeline in ctx.
func TestPauseRealClientScope(t *testing.T) {
	var puts atomic.Int32
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/connectors/pg-orders-connector/pause" && r.Method == http.MethodPut:
			puts.Add(1)
			w.WriteHeader(http.StatusAccepted)
		case r.URL.Path == "/connectors/pg-orders-connector/status":
			state := "RUNNING"
			if puts.Load() > 0 {
				state = "PAUSED"
			}
			w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"` + state + `"},"tasks":[]}`))
		default:
			http.NotFound(w, r)
		}
	})
	a := mustGet(t, resumeRegistry(t, d), "connect_pause_connector")
	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{"no scope", context.Background()},
		{"pipeline id only", tools.WithPipelineID(context.Background(), "pg-orders")},
	} {
		_, err := a.Execute(tc.ctx, json.RawMessage(pauseIn))
		if !errors.Is(err, tools.ErrNoScope) {
			t.Fatalf("%s: err %v, want ErrNoScope", tc.name, err)
		}
	}
	if puts.Load() != 0 {
		t.Fatal("an unscoped pause reached Connect")
	}
	out, err := a.Execute(scoped(), json.RawMessage(pauseIn))
	if err != nil || puts.Load() != 1 || !strings.Contains(out, "(PAUSED)") {
		t.Fatalf("out %q err %v puts %d", out, err, puts.Load())
	}
}

// ---- connect_update_connector_config, N keys ----

const twoKeysIn = `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"string","tombstones.on.delete":"false"}}`

// Two keys are one Execute (one proposal step, one approval) and one write.
func TestUpdateConfigTwoKeysOneWrite(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.allowWrites = true
	c.cfg["tombstones.on.delete"] = "true"
	a := configAction(t, c)
	if err := a.Validate(t.Context(), json.RawMessage(twoKeysIn)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	out, err := a.Execute(scoped(), json.RawMessage(twoKeysIn))
	if err != nil {
		t.Fatal(err)
	}
	want := "pg-orders-connector decimal.handling.mode: precise → string, tombstones.on.delete: true → false; status RUNNING at first read (Connect restarts tasks asynchronously)"
	if out != want {
		t.Fatalf("out %q\nwant %q", out, want)
	}
	if len(c.updates) != 2 || c.updates[0] != "pg-orders-connector decimal.handling.mode=string" || c.updates[1] != "pg-orders-connector tombstones.on.delete=false" {
		t.Fatalf("updates %v", c.updates)
	}
	if strings.Contains(out, "hunter2") {
		t.Fatalf("result leaks another config value: %q", out)
	}
}

// Only the keys that differ are written; the rest are reported unchanged.
func TestUpdateConfigWritesOnlyChangedKeys(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.allowWrites = true
	c.cfg["decimal.handling.mode"] = "string"
	out, err := configAction(t, c).Execute(scoped(), json.RawMessage(twoKeysIn))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.updates) != 1 || c.updates[0] != "pg-orders-connector tombstones.on.delete=false" {
		t.Fatalf("updates %v", c.updates)
	}
	if !strings.Contains(out, "tombstones.on.delete: unset → false (unchanged: decimal.handling.mode: already string)") {
		t.Fatalf("out %q", out)
	}
	c2 := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c2.allowWrites = true
	c2.cfg["decimal.handling.mode"], c2.cfg["tombstones.on.delete"] = "string", "false"
	out, err = configAction(t, c2).Execute(scoped(), json.RawMessage(twoKeysIn))
	if err != nil || len(c2.updates) != 0 || !strings.HasSuffix(out, "; no change made") {
		t.Fatalf("all set: out %q err %v updates %v", out, err, c2.updates)
	}
}

// The old one-key shape still parses, and means the same as a one-entry changes.
func TestUpdateConfigOldShapeParses(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	old, err := a.parseParams(json.RawMessage(updateIn))
	if err != nil {
		t.Fatal(err)
	}
	neu, err := a.parseParams(json.RawMessage(`{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"string"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !old.legacy || neu.legacy || old.connector != neu.connector || len(old.changes) != 1 || old.changes["decimal.handling.mode"] != neu.changes["decimal.handling.mode"] {
		t.Fatalf("old %+v new %+v", old, neu)
	}
}

func TestUpdateConfigNKeysValidate(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	nine := map[string]string{}
	for i := 0; i < 9; i++ {
		nine["k"+string(rune('a'+i))] = "x"
	}
	nineJSON, _ := json.Marshal(map[string]any{"connector": "pg-orders-connector", "changes": nine})
	tests := []struct {
		name    string
		params  string
		wantErr string // substring; empty means no error
	}{
		{"two keys ok", twoKeysIn, ""},
		{"both shapes", `{"connector":"pg-orders-connector","key":"tombstones.on.delete","value":"true","changes":{"decimal.handling.mode":"string"}}`, "not both"},
		{"changes and value", `{"connector":"pg-orders-connector","value":"true","changes":{"decimal.handling.mode":"string"}}`, "not both"},
		{"empty changes", `{"connector":"pg-orders-connector","changes":{}}`, "at least one key"},
		{"too many keys", string(nineJSON), "at most 8"},
		{"non-string value", `{"connector":"pg-orders-connector","changes":{"tombstones.on.delete":true}}`, "required"},
		{"changes not an object", `{"connector":"pg-orders-connector","changes":"x"}`, "required"},
		{"unknown key", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"string","tasks.max":"4"}}`, `key "tasks.max" is not allowed`},
		{"secret key", `{"connector":"pg-orders-connector","changes":{"database.password":"x"}}`, "credential"},
		{"secret-looking value", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"apikey-123"}}`, "looks like a credential"},
		{"bad key charset", `{"connector":"pg-orders-connector","changes":{"decimal handling":"string"}}`, "not a config key name"},
		{"key too long", `{"connector":"pg-orders-connector","changes":{"` + strings.Repeat("a", 200) + `":"x"}}`, "not a config key name"},
		{"value too long", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"` + strings.Repeat("s", 300) + `"}}`, "longer than 256"},
		{"bad value", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"double"}}`, "double loses precision"},
		{"unknown connector", `{"connector":"pg-inventory-connector","changes":{"decimal.handling.mode":"string"}}`, "not found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Validate(t.Context(), json.RawMessage(tc.params))
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("want nil, got %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
			if err != nil && strings.Contains(err.Error(), "apikey-123") {
				t.Fatalf("error echoes a secret-looking value: %v", err)
			}
		})
	}
}

// Validate refuses another pipeline's connector when ctx carries the room's
// pipeline, and does not need a scope at all (createProposal's Validate).
func TestUpdateConfigValidateScope(t *testing.T) {
	a := &updateConnectorConfig{d: Deps{Connect: &fakeConnect{names: []string{"pg-orders-connector", "pg-inventory-connector"}}}}
	if err := a.Validate(context.Background(), json.RawMessage(twoKeysIn)); err != nil {
		t.Fatalf("unscoped Validate: %v", err)
	}
	if err := a.Validate(scoped(), json.RawMessage(twoKeysIn)); err != nil {
		t.Fatalf("scoped Validate: %v", err)
	}
	if err := a.Validate(otherPipeline(), json.RawMessage(twoKeysIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline: %v", err)
	}
	if err := a.CheckScope(testPipeline(), json.RawMessage(`{"connector":"pg-inventory-connector","changes":{"decimal.handling.mode":"string"}}`)); err == nil {
		t.Fatal("CheckScope passed another pipeline's connector")
	}
}

func TestUpdateConfigExecuteRefusals(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.allowWrites = true
	if _, err := configAction(t, c).Execute(otherPipeline(), json.RawMessage(twoKeysIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline: %v", err)
	}
	// The guard is re-checked on the config read at Execute, for every key.
	c.cfg["errors.log.include.messages"] = "true"
	_, err := configAction(t, c).Execute(scoped(), json.RawMessage(`{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"string","errors.log.enable":"true"}}`))
	if err == nil || !strings.Contains(err.Error(), "turn errors.log.include.messages off first") {
		t.Fatalf("guard: %v", err)
	}
	if len(c.updates) != 0 {
		t.Fatalf("a refused change was written: %v", c.updates)
	}
}

// The real client: one PATCH carrying exactly the keys to change, so the rest
// of the config (including the password) is never sent; without a scope the
// client refuses before any request.
func TestUpdateConfigRealClientOnePatch(t *testing.T) {
	var patches []string
	d := resumeTestDeps(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/connectors":
			w.Write([]byte(`["pg-orders-connector"]`))
		case r.URL.Path == "/connectors/pg-orders-connector/config" && r.Method == http.MethodGet:
			if len(patches) > 0 {
				w.Write([]byte(`{"decimal.handling.mode":"string","tombstones.on.delete":"false","database.password":"hunter2"}`))
				return
			}
			w.Write([]byte(`{"decimal.handling.mode":"precise","tombstones.on.delete":"true","database.password":"hunter2"}`))
		case r.URL.Path == "/connectors/pg-orders-connector/config" && r.Method == http.MethodPatch:
			b, _ := io.ReadAll(r.Body)
			patches = append(patches, string(b))
			w.Write([]byte(`{}`))
		case r.URL.Path == "/connectors/pg-orders-connector/status":
			w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
		default:
			http.NotFound(w, r)
		}
	})
	a := mustGet(t, resumeRegistry(t, d), "connect_update_connector_config")
	if _, err := a.Execute(context.Background(), json.RawMessage(twoKeysIn)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("unscoped: %v", err)
	}
	if len(patches) != 0 {
		t.Fatal("an unscoped write reached Connect")
	}
	out, err := a.Execute(scoped(), json.RawMessage(twoKeysIn))
	if err != nil {
		t.Fatal(err)
	}
	if len(patches) != 1 || patches[0] != `{"decimal.handling.mode":"string","tombstones.on.delete":"false"}` {
		t.Fatalf("patches %q", patches)
	}
	if strings.Contains(out, "hunter2") || !strings.Contains(out, "status RUNNING") {
		t.Fatalf("out %q", out)
	}
}

func TestUpdateConfigPreviewNKeys(t *testing.T) {
	cfg := map[string]string{
		"connector.class":       "io.debezium.X",
		"decimal.handling.mode": "precise",
		"tombstones.on.delete":  "true",
	}
	fc := &pvConnect{cfg: cfg}
	a := &updateConnectorConfig{d: Deps{Connect: fc}}
	res, err := a.Preview(scoped(), json.RawMessage(twoKeysIn))
	if err != nil {
		t.Fatal(err)
	}
	if res.Target != "connector pg-orders-connector keys decimal.handling.mode, tombstones.on.delete" ||
		res.Current != "decimal.handling.mode = precise; tombstones.on.delete = true (GET /connectors/pg-orders-connector/config)" ||
		res.Expected != "decimal.handling.mode = string; tombstones.on.delete = false; Connect restarts the tasks" {
		t.Fatalf("preview %+v", res)
	}
	want := `connect_update_connector_config {"changes":{"decimal.handling.mode":"precise","tombstones.on.delete":"true"},"connector":"pg-orders-connector"}`
	if !res.Reversible || res.Rollback != want {
		t.Fatalf("rollback %q", res.Rollback)
	}
	// The dry run gets the whole config with both keys merged.
	if fc.validate == nil || fc.validate.cfg["decimal.handling.mode"] != "string" || fc.validate.cfg["tombstones.on.delete"] != "false" || fc.validate.cfg["connector.class"] != "io.debezium.X" {
		t.Fatalf("validate sent %+v", fc.validate)
	}
	for _, c := range fc.calls {
		if c != "Config" && c != "ValidateConfig" {
			t.Errorf("Preview called %s", c)
		}
	}
	if _, err := a.Preview(otherPipeline(), json.RawMessage(twoKeysIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out of scope: %v", err)
	}
}

// A stored value that holds a secret is redacted in the preview, a partial
// undo is not offered, and a config with secret keys is not sent to validate.
func TestUpdateConfigPreviewRedactsAndPartialUndo(t *testing.T) {
	fc := &pvConnect{cfg: map[string]string{
		"connector.class":       "io.debezium.X",
		"decimal.handling.mode": "password=hunter2",
		"database.password":     "hunter2",
	}}
	a := &updateConnectorConfig{d: Deps{Connect: fc}}
	res, err := a.Preview(scoped(), json.RawMessage(twoKeysIn))
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := json.Marshal(res); strings.Contains(string(b), "hunter2") {
		t.Fatalf("preview leaks a secret: %s", b)
	}
	if res.Reversible || res.Rollback != "" {
		t.Fatalf("partial undo offered: %+v", res)
	}
	if fc.validate != nil || res.DryRun == nil || res.DryRun.Skipped == "" {
		t.Fatalf("secret config sent to validate: %+v", res.DryRun)
	}
}

func TestUpdateConfigSchemaListsAllowedKeys(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	s := a.ParamsSchema()
	props := s["properties"].(map[string]any)
	ch := props["changes"].(map[string]any)
	if ch["additionalProperties"] != false {
		t.Fatal("changes accepts keys outside the allow-list")
	}
	cp := ch["properties"].(map[string]any)
	for _, k := range sortedKeys(defaultConnectorConfigAllowList()) {
		if _, ok := cp[k]; !ok {
			t.Errorf("changes schema lacks %q", k)
		}
	}
	if len(cp) != len(defaultConnectorConfigAllowList()) {
		t.Errorf("changes schema has %d keys", len(cp))
	}
	if req := s["required"].([]string); len(req) != 1 || req[0] != "connector" {
		t.Errorf("required %v", req)
	}
}
