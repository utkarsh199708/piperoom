package catalog

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/targets/compose"
)

// pendingOnRunner are the builtin ops no runner catalogue ships yet (the
// setup-only source ops land with U1-9's SetupSourceWriter and the
// catalogue's source entries). Every other builtin op must match the runner.
var pendingOnRunner = []string{
	"source_create_publication", "source_create_canary_table", "source_insert_canary",
	"source_create_read_role", "source_alter_system",
}

// TestBuiltinMatchesRunner pins the server's description to the runner's
// catalogue: the real ops registry and Compose driver, built in a temporary
// project directory, must declare the same tier, target, layer, description
// and schema for every op, and every runner op except the preview op (a
// server-internal transport, never a plan step) must be described.
func TestBuiltinMatchesRunner(t *testing.T) {
	r, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if _, err := compose.Register(r, compose.Options{}); err != nil {
		t.Fatal(err)
	}
	cat, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range r.Names() {
		if name == ops.OpPreview {
			if _, ok := cat.Spec(name); ok {
				t.Errorf("%s is described: it is the preview transport, not an op a tool, action or plan names", name)
			}
			continue
		}
		op, _ := r.Get(name)
		s, ok := cat.Spec(name)
		if !ok {
			t.Errorf("runner op %s is not in the builtin catalogue", name)
			continue
		}
		if s.Tier != op.Tier() || s.Target != op.Target() || s.Layer != op.Layer() || s.Describe != op.Describe() {
			t.Errorf("%s: builtin {%s %q %s %q}, runner {%s %q %s %q}", name,
				s.Tier, s.Target, s.Layer, s.Describe, op.Tier(), op.Target(), op.Layer(), op.Describe())
		}
		want, err := json.Marshal(op.ParamsSchema())
		if err != nil {
			t.Fatal(err)
		}
		var a, b any
		_ = json.Unmarshal(want, &a)
		_ = json.Unmarshal(s.Schema, &b)
		if !reflect.DeepEqual(a, b) {
			t.Errorf("%s: schema drifted\n builtin %s\n runner  %s", name, s.Schema, want)
		}
	}
	for _, s := range cat.Specs() {
		if _, ok := r.Get(s.Name); !ok && !slices.Contains(pendingOnRunner, s.Name) {
			t.Errorf("builtin op %s is not in the runner's catalogue", s.Name)
		}
	}
}

func TestBuiltinShape(t *testing.T) {
	cat, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range cat.Reads() {
		if s.Tier != plan.TierT0 || s.Via != ViaSetup {
			t.Errorf("Reads has %s at %s via %s", s.Name, s.Tier, s.Via)
		}
	}
	for _, s := range cat.Writes() {
		if s.Tier == plan.TierT0 {
			t.Errorf("Writes has the T0 op %s", s.Name)
		}
	}
	if _, ok := cat.Spec("secret_set"); ok {
		t.Error("secret_set is in the catalogue: a secret value must never travel in proposal params")
	}
	if f, ok := cat.Floor("compose_up"); !ok || f != plan.TierT1 {
		t.Errorf("Floor(compose_up) = %q, %v", f, ok)
	}
	if _, ok := cat.Floor("drop_everything"); ok {
		t.Error("Floor answers an op that is not in the catalogue")
	}
	// A caller cannot change the catalogue through a returned spec.
	s, _ := cat.Spec("source_create_publication")
	s.TableKeys[0] = "x"
	s.Schema[0] = '['
	again, _ := cat.Spec("source_create_publication")
	if again.TableKeys[0] != "tables" || again.Schema[0] != '{' {
		t.Error("Spec returned the catalogue's own slices")
	}
}

func TestNewRefuses(t *testing.T) {
	good := Spec{Name: "a_op", Tier: plan.TierT1, Via: ViaSetup, Schema: raw(`{"type":"object"}`)}
	for _, tc := range []struct {
		name string
		mut  func(*Spec)
	}{
		{"empty name", func(s *Spec) { s.Name = "" }},
		{"not snake case", func(s *Spec) { s.Name = "Compose-Up" }},
		{"bad tier", func(s *Spec) { s.Tier = "T9" }},
		{"bad via", func(s *Spec) { s.Via = "magic" }},
		{"setup op without schema", func(s *Spec) { s.Schema = nil }},
		{"schema not JSON", func(s *Spec) { s.Schema = raw(`{`) }},
		{"schema not an object type", func(s *Spec) { s.Schema = raw(`{"type":"array"}`) }},
		{"unchecked keyword", func(s *Spec) { s.Schema = raw(`{"type":"object","oneOf":[]}`) }},
		{"nested unchecked keyword", func(s *Spec) {
			s.Schema = raw(`{"type":"object","properties":{"a":{"type":"string","format":"uri"}}}`)
		}},
		{"bad pattern", func(s *Spec) {
			s.Schema = raw(`{"type":"object","properties":{"a":{"type":"string","pattern":"("}}}`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := good
			tc.mut(&s)
			if _, err := New(s); !errors.Is(err, ErrSpec) {
				t.Fatalf("New = %v, want ErrSpec", err)
			}
		})
	}
	if _, err := New(good, good); !errors.Is(err, ErrSpec) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := New(good, Spec{Name: "b_op", Tier: plan.TierT2, Via: ViaClient}); err != nil {
		t.Fatalf("a client op needs no schema: %v", err)
	}
}

func TestWithKeepsSetupSpec(t *testing.T) {
	cat, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	u, err := cat.With(
		Spec{Name: "source_create_publication", Tier: plan.TierT2, Via: ViaClient},
		Spec{Name: "connect_status", Tier: plan.TierT0, Via: ViaClient, Target: "connect", Layer: "kafka-connect"},
		Spec{Name: "connect_status", Tier: plan.TierT0, Via: ViaClient},
	)
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := u.Spec("source_create_publication"); s.Via != ViaSetup || len(s.Schema) == 0 {
		t.Errorf("the client spec replaced the setup spec: %+v", s)
	}
	if s, ok := u.Spec("connect_status"); !ok || s.Layer != "kafka-connect" {
		t.Errorf("connect_status = %+v, %v", s, ok)
	}
	if len(u.Specs()) != len(cat.Specs())+1 {
		t.Errorf("With added %d specs, want 1", len(u.Specs())-len(cat.Specs()))
	}
}

func TestCheck(t *testing.T) {
	cat, err := Builtin()
	if err != nil {
		t.Fatal(err)
	}
	spec := func(name string) Spec {
		s, ok := cat.Spec(name)
		if !ok {
			t.Fatalf("no %s", name)
		}
		return s
	}
	for _, tc := range []struct {
		op, params, wantErr string
	}{
		{"env_detect", ``, ""},
		{"env_detect", `{}`, ""},
		{"env_detect", `{"x":1}`, "not a parameter"},
		{"env_detect", `[]`, "JSON object"},
		{"env_detect", `{} {}`, "trailing"},
		{"env_detect", `{`, "not JSON"},
		{"file_read", `{"path":"a.txt"}`, ""},
		{"file_read", `{}`, "params.path is required"},
		{"file_read", `{"path":""}`, "shorter"},
		{"file_read", `{"path":7}`, "must be a string"},
		{"file_read", `{"path":"a","PATH":"b"}`, "letter case"},
		{"http_get", `{"url":"http://c:8083/","method":"HEAD"}`, ""},
		{"http_get", `{"url":"http://c:8083/","method":"POST"}`, "allowed values"},
		{"http_get", `{"url":"` + strings.Repeat("a", 2049) + `"}`, "longer"},
		{"net_probe", `{"target":"db","port":5432,"tls":true}`, ""},
		{"net_probe", `{"target":"db","port":70000}`, "above"},
		{"net_probe", `{"target":"db","port":1.5}`, "integer"},
		{"net_probe", `{"target":"db","tls":"yes"}`, "boolean"},
		{"compose_up", `{"services":["connect"]}`, ""},
		{"compose_up", `{"services":"connect"}`, "array"},
		{"compose_up", `{"services":[1]}`, "params.services[0] must be a string"},
		{"compose_up", `{"services":["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"]}`, "more than 16"},
		{"compose_write", `{"templates":[]}`, "fewer than 1"},
		{"compose_write", `{"templates":["connect"],"ports":{"connect":80}}`, "below"},
		{"compose_write", `{"templates":["connect"],"project":"other"}`, "pattern"},
		{"properties_write", `{"file":".env","entries":{}}`, "fewer than 1 keys"},
		{"properties_write", `{"file":".env","entries":{"A":1}}`, "params.entries.A must be a string"},
		{"source_create_publication", `{"publication":"dbz","tables":["public.orders"]}`, ""},
		{"source_create_publication", `{"publication":"dbz; drop","tables":["public.orders"]}`, "pattern"},
		{"source_create_publication", `{"publication":"dbz","tables":["orders"]}`, "pattern"},
		{"source_insert_canary", `{"table":"public.c","nonce":"00112233445566778899aabbccddeeff"}`, ""},
		{"source_insert_canary", `{"table":"public.c","nonce":"short"}`, "pattern"},
		{"source_alter_system", `{"name":"archive_command","value":"x"}`, "allowed values"},
	} {
		err := spec(tc.op).Check(json.RawMessage(tc.params))
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s %s: %v", tc.op, tc.params, err)
		case tc.wantErr != "" && (err == nil || !errors.Is(err, ErrParams) || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s %s: err = %v, want ErrParams containing %q", tc.op, tc.params, err, tc.wantErr)
		}
	}
	big := `{"path":"a","content":"` + strings.Repeat("a", maxParamsBytes) + `"}`
	if err := spec("file_write").Check(json.RawMessage(big)); !errors.Is(err, ErrParams) {
		t.Errorf("oversized params: %v", err)
	}
}
