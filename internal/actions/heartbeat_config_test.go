package actions

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func hbParams(t *testing.T, key, value string) json.RawMessage {
	t.Helper()
	return paramsJSON(t, key, value)
}

// heartbeat.interval.ms: 0 (off) or 1000..3600000, plain digits only.
func TestHeartbeatIntervalBounds(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	cases := []struct {
		value   string
		wantErr string // substring; empty = accepted
	}{
		{"0", ""},
		{"1000", ""},
		{"10000", ""},
		{"60000", ""},
		{"3600000", ""},
		{"999", "want 0 (off) or 1000 to 3600000"},
		{"1", "want 0 (off) or 1000 to 3600000"},
		{"3600001", "want 0 (off) or 1000 to 3600000"},
		{"9999999999", "want 0 (off) or 1000 to 3600000"},
		// Not a whole number in plain digits.
		{"", "plain digits"},
		{"-1", "plain digits"},
		{"+1000", "plain digits"},
		{" 1000", "plain digits"},
		{"1000 ", "plain digits"},
		{"1000\n", "plain digits"},
		{"01000", "plain digits"},
		{"00", "plain digits"},
		{"1e4", "plain digits"},
		{"1000.0", "plain digits"},
		{"1,000", "plain digits"},
		{"0x3e8", "plain digits"},
		{"١٠٠٠", "plain digits"},        // Arabic-Indic digits
		{"１０００", "plain digits"},        // full-width digits
		{"10000000000", "plain digits"}, // 11 digits
		{strings.Repeat("9", 5000), "plain digits"},
		{"off", "plain digits"},
		{"true", "plain digits"},
	}
	for _, tc := range cases {
		name := tc.value
		if len(name) > 20 {
			name = name[:20] + "..."
		}
		t.Run("value="+name, func(t *testing.T) {
			p := hbParams(t, "heartbeat.interval.ms", tc.value)
			err := a.Validate(p)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("Validate refused %q: %v", tc.value, err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("Validate(%q) = %v, want an error containing %q", tc.value, err, tc.wantErr)
			}
			// Execute applies the same rule: a refused value is never written.
			c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
			_, execErr := configAction(c).Execute(context.Background(), p)
			if (execErr == nil) != (tc.wantErr == "") {
				t.Fatalf("Execute(%q) err = %v, want error = %v", tc.value, execErr, tc.wantErr != "")
			}
			if tc.wantErr != "" && len(c.updates) != 0 {
				t.Fatalf("a refused value was written: %v", c.updates)
			}
			if tc.wantErr == "" && (len(c.updates) != 1 || c.updates[0] != "pg-orders-connector heartbeat.interval.ms="+tc.value) {
				t.Fatalf("updates = %v", c.updates)
			}
			if err != nil && len(err.Error()) > 400 {
				t.Fatalf("error echoes the whole value (%d bytes)", len(err.Error()))
			}
		})
	}
}

// The heartbeat fix the QA run could not propose: a slot stuck on a quiet table.
func TestHeartbeatIntervalExecute(t *testing.T) {
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	delete(c.cfg, "heartbeat.interval.ms")
	out, err := configAction(c).Execute(context.Background(), hbParams(t, "heartbeat.interval.ms", "10000"))
	if err != nil {
		t.Fatal(err)
	}
	want := "pg-orders-connector heartbeat.interval.ms: unset → 10000; status RUNNING at first read (Connect restarts tasks asynchronously)"
	if out != want {
		t.Fatalf("out %q, want %q", out, want)
	}
	// Already set: no write, so the tasks are not restarted for nothing.
	c2 := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c2.cfg["heartbeat.interval.ms"] = "10000"
	out, err = configAction(c2).Execute(context.Background(), hbParams(t, "heartbeat.interval.ms", "10000"))
	if err != nil || len(c2.updates) != 0 || !strings.Contains(out, "already 10000") {
		t.Fatalf("out %q err %v updates %v", out, err, c2.updates)
	}
}

// heartbeat.action.query runs SQL on the source: a human sets it, never the agent.
func TestHeartbeatActionQueryIsHumanOnly(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	for _, value := range []string{"INSERT INTO hb VALUES (1)", "", "0", "SELECT 1"} {
		p := hbParams(t, "heartbeat.action.query", value)
		for name, err := range map[string]error{"Validate": a.Validate(p), "preview": previewParse(a, p)} {
			if err == nil || !strings.Contains(err.Error(), "human-only") || !strings.Contains(err.Error(), "SQL on the source database") {
				t.Errorf("%s(%q) = %v, want a human-only refusal that says it runs SQL on the source", name, value, err)
			}
		}
		c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
		if _, err := configAction(c).Execute(context.Background(), p); err == nil || len(c.updates) != 0 {
			t.Errorf("Execute(%q) err = %v, updates %v", value, err, c.updates)
		}
	}
	// Other heartbeat keys are not on the list; they get the plain refusal.
	for _, key := range []string{"heartbeat.topics.prefix", "Heartbeat.Action.Query", "heartbeat.action.query "} {
		err := a.Validate(hbParams(t, key, "x"))
		if err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("key %q: err = %v", key, err)
		}
	}
	// The text the agent sees names whom to ask.
	err := a.Validate(hbParams(t, "heartbeat.action.query", "SELECT 1"))
	if !strings.Contains(err.Error(), "ask the owner of the connector") {
		t.Errorf("err = %v", err)
	}
}

func previewParse(a *updateConnectorConfig, raw json.RawMessage) error {
	_, err := a.parseParams(raw)
	return err
}

func TestHeartbeatKeyIsListedEverywhere(t *testing.T) {
	a := &updateConnectorConfig{d: testDeps()}
	enum := a.ParamsSchema()["properties"].(map[string]any)["key"].(map[string]any)["enum"].([]string)
	want := []string{
		"decimal.handling.mode", "errors.deadletterqueue.context.headers.enable", "errors.log.enable",
		"heartbeat.interval.ms", "time.precision.mode", "tombstones.on.delete",
	}
	if strings.Join(enum, ",") != strings.Join(want, ",") {
		t.Fatalf("key enum = %v, want %v", enum, want)
	}
	d := a.Describe()
	for _, w := range []string{"heartbeat.interval.ms", "1000 to 3600000", "heartbeat.action.query is human-only", "SQL on the source database"} {
		if !strings.Contains(d, w) {
			t.Errorf("Describe lacks %q", w)
		}
	}
	if looksSecret("heartbeat.interval.ms") {
		t.Error("heartbeat.interval.ms looks secret")
	}
	// A refused key lists the allowed ones, heartbeat included.
	err := a.Validate(hbParams(t, "tasks.max", "4"))
	if err == nil || !strings.Contains(err.Error(), "heartbeat.interval.ms") {
		t.Errorf("err = %v", err)
	}
}

// The test-only override replaces the whole list: the numeric key goes with it.
func TestHeartbeatBoundsFollowTheAllowListOverride(t *testing.T) {
	a := &updateConnectorConfig{
		d:     testDeps(),
		allow: map[string]map[string]bool{"decimal.handling.mode": {"double": true}},
	}
	if err := a.Validate(hbParams(t, "heartbeat.interval.ms", "10000")); err == nil {
		t.Fatal("heartbeat accepted although the list was replaced")
	}
	enum := a.ParamsSchema()["properties"].(map[string]any)["key"].(map[string]any)["enum"].([]string)
	if len(enum) != 1 || enum[0] != "decimal.handling.mode" {
		t.Fatalf("enum = %v", enum)
	}
	// And a credential-looking key still loses to the secret check.
	b := &updateConnectorConfig{d: testDeps(), allow: map[string]map[string]bool{"x.password": {"1": true}}}
	if err := b.Validate(hbParams(t, "x.password", "1")); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("err = %v", err)
	}
}

func TestHeartbeatPreviewReversibility(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     map[string]string
		wantRev bool
		wantRB  string
	}{
		{"old value in range", map[string]string{"connector.class": "x", "heartbeat.interval.ms": "5000"}, true,
			`connect_update_connector_config {"connector":"c","key":"heartbeat.interval.ms","value":"5000"}`},
		{"old value 0 (off)", map[string]string{"connector.class": "x", "heartbeat.interval.ms": "0"}, true,
			`connect_update_connector_config {"connector":"c","key":"heartbeat.interval.ms","value":"0"}`},
		// An action could not set these back, so the preview does not promise it.
		{"old value out of range", map[string]string{"connector.class": "x", "heartbeat.interval.ms": "500"}, false, ""},
		{"old value not a number", map[string]string{"connector.class": "x", "heartbeat.interval.ms": "${file:/x:hb}"}, false, ""},
		{"unset", map[string]string{"connector.class": "x"}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &pvConnect{cfg: tc.cfg}
			res, err := (&updateConnectorConfig{d: Deps{Connect: fc}}).Preview(context.Background(),
				raw(t, map[string]any{"connector": "c", "key": "heartbeat.interval.ms", "value": "10000"}))
			if err != nil {
				t.Fatal(err)
			}
			if res.Reversible != tc.wantRev || res.Rollback != tc.wantRB {
				t.Fatalf("reversible %v rollback %q, want %v %q", res.Reversible, res.Rollback, tc.wantRev, tc.wantRB)
			}
			if !strings.Contains(res.Expected, "heartbeat.interval.ms = 10000") {
				t.Fatalf("expected %q", res.Expected)
			}
		})
	}
}

func TestHeartbeatSchemaEnumIsStableForOtherKeys(t *testing.T) {
	// The five string-valued keys keep their exact value lists.
	a := defaultConnectorConfigAllowList()
	if len(a) != 5 {
		t.Fatalf("string allow-list has %d keys, want the original 5", len(a))
	}
	if _, ok := a["heartbeat.interval.ms"]; ok {
		t.Fatal("a numeric key must not sit in the string allow-list")
	}
	b := defaultConnectorConfigBounds()
	b["tasks.max"] = wholeRange{min: 1, max: 1}
	if _, bad := defaultConnectorConfigBounds()["tasks.max"]; bad {
		t.Fatal("the bounds table is shared state")
	}
}
