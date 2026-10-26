package ops

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// The registry is the runner's Catalogue, structurally: no import of the
// runner in the package, only this assertion in a test.
var _ runner.Catalogue = (*Registry)(nil)

// setup_plan fills each stored step's Layer from the registry.
var _ setuptools.LayerCatalogue = (*Registry)(nil)

func TestFloorsAndTargets(t *testing.T) {
	cases := []struct {
		op     string
		tier   string
		target string
	}{
		{"env_detect", protocol.TierT0, ""},
		{"file_read", protocol.TierT0, ""},
		{"dir_list", protocol.TierT0, ""},
		{"jar_list", protocol.TierT0, ""},
		{"java_version", protocol.TierT0, ""},
		{"http_get", protocol.TierT0, ""},
		{"net_probe", protocol.TierT0, ""},
		{"sink_probe", protocol.TierT0, ""},
		{"file_write", protocol.TierT1, ""},
		{"file_delete", protocol.TierT1, ""},
		{"source_publication_tables", protocol.TierT0, "postgres"},
	}
	r := testRegistry(t)
	if len(r.Names()) != len(cases) {
		t.Fatalf("catalogue has %v, want %d ops", r.Names(), len(cases))
	}
	for _, c := range cases {
		tier, ok := r.Floor(c.op)
		if !ok || tier != c.tier {
			t.Errorf("Floor(%q) = %q, %v; want %q", c.op, tier, ok, c.tier)
		}
		op, ok := r.Get(c.op)
		if !ok || op.Target() != c.target {
			t.Errorf("Target(%q) = %q, want %q", c.op, op.Target(), c.target)
		}
		if layer, ok := r.LayerOf(c.op); !ok || layer != op.Layer() || layer == "" {
			t.Errorf("LayerOf(%q) = %q, %v; want the op's layer %q", c.op, layer, ok, op.Layer())
		}
		if action, ok := r.StepAction(c.op); !ok || action != c.op {
			t.Errorf("StepAction(%q) = %q, %v", c.op, action, ok)
		}
		if r.CatalogueVersion() != CatalogueVersion || CatalogueVersion == "" {
			t.Errorf("CatalogueVersion = %q", r.CatalogueVersion())
		}
	}
	if _, ok := r.Floor("no_such_op"); ok {
		t.Error("Floor of an unknown op = true")
	}
	if _, ok := r.LayerOf("no_such_op"); ok {
		t.Error("LayerOf of an unknown op = true")
	}
	if _, ok := r.StepAction("no_such_op"); ok {
		t.Error("StepAction of an unknown op = true")
	}
}

// The dynamic target kind comes from the params, so the runner raises the
// class floor for the endpoint the op actually reaches.
func TestParamTarget(t *testing.T) {
	r := testRegistry(t,
		events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"},
		events.SetupTarget{Kind: "postgres", Address: "db.acme:5432"},
		events.SetupTarget{Kind: "host", Address: "sink.example:8090"},
	)
	get, _ := json.Marshal(map[string]string{"url": "http://connect.acme:8083/connectors"})
	if got := r.Target(protocol.OpRequest{Op: "http_get", Params: get}); got != "connect" {
		t.Fatalf("http_get Target = %q, want connect", got)
	}
	probe, _ := json.Marshal(map[string]string{"target": "postgres"})
	if got := r.Target(protocol.OpRequest{Op: "net_probe", Params: probe}); got != "postgres" {
		t.Fatalf("net_probe Target = %q, want postgres", got)
	}
	sinkProbe, _ := json.Marshal(map[string]string{"target": "sink.example:8090"})
	if got := r.Target(protocol.OpRequest{Op: "sink_probe", Params: sinkProbe}); got != "host" {
		t.Fatalf("sink_probe Target = %q, want host", got)
	}
	// An unregistered label raises no floor and is refused by Validate.
	other, _ := json.Marshal(map[string]string{"target": "kafka"})
	if got := r.Target(protocol.OpRequest{Op: "net_probe", Params: other}); got != "" {
		t.Fatalf("unregistered label Target = %q, want empty", got)
	}
}

func TestGuardTable(t *testing.T) {
	r := testRegistry(t)
	cases := []struct {
		op, class string
		want      bool
	}{
		{"file_write", protocol.ClassProduction, true},
		{"file_delete", protocol.ClassProduction, true},
		{"file_write", protocol.ClassShared, false},
		{"file_write", protocol.ClassLocal, false},
		{"file_delete", protocol.ClassShared, false},
		{"file_read", protocol.ClassProduction, false},
		{"dir_list", protocol.ClassProduction, false},
		{"http_get", protocol.ClassProduction, false},
		{"net_probe", protocol.ClassProduction, false},
		{"sink_probe", protocol.ClassProduction, false},
		{"file_write", "garbage", false},
		{"no_such_op", protocol.ClassProduction, false},
	}
	for _, c := range cases {
		if got := r.GuardForbidden(c.op, c.class); got != c.want {
			t.Errorf("GuardForbidden(%q, %q) = %v, want %v", c.op, c.class, got, c.want)
		}
	}
}

// TestGuardTableAgreesWithPlan: the runner's guard table and the server's
// (internal/setup/plan) refuse the same pairs for every op name they share,
// so a step the server would refuse is never run because the runner's shorter
// table spells it differently. Both directions are checked with the strictest
// facts.
func TestGuardTableAgreesWithPlan(t *testing.T) {
	r := testRegistry(t)
	for op, classes := range guardTable {
		for _, class := range classes {
			if _, forbidden := plan.Forbidden(plan.GuardRequest{Op: op, Class: class}); !forbidden {
				t.Errorf("the runner forbids %s on %s, the plan table does not", op, class)
			}
		}
	}
	for _, row := range plan.GuardRows() {
		if _, ok := r.Get(row.Op); !ok {
			continue // the runner's catalogue does not implement this op
		}
		if !r.GuardForbidden(row.Op, row.Class) {
			t.Errorf("the plan table forbids %s on %s, the runner does not", row.Op, row.Class)
		}
	}
}

// CheckTarget is the runner's pre-guard allow-list: a host outside the
// targets is RefuseTarget, bad params RefuseSchema.
func TestCheckTarget(t *testing.T) {
	r := testRegistry(t, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"}, events.SetupTarget{Kind: "host", Address: "sink.acme:8090"})
	cases := []struct {
		op     string
		params string
		want   string
	}{
		{"http_get", `{"url":"http://connect.acme:8083/"}`, ""},
		{"http_get", `{"url":"http://elsewhere.example:8083/"}`, protocol.RefuseTarget},
		{"http_get", `{"url":"http://connect.acme:8083/","method":"POST"}`, protocol.RefuseSchema},
		{"net_probe", `{"target":"connect"}`, ""},
		{"net_probe", `{"target":"kafka"}`, protocol.RefuseTarget},
		{"sink_probe", `{"target":"sink.acme:8090"}`, ""},
		{"sink_probe", `{"target":"other.acme:8090"}`, protocol.RefuseTarget},
		{"sink_probe", `{}`, protocol.RefuseSchema},
		{"file_read", `{"path":"a.txt"}`, ""},
		{"no_such_op", `{}`, protocol.RefuseUnknownOp},
	}
	for _, c := range cases {
		got := r.CheckTarget(protocol.OpRequest{Op: c.op, Params: json.RawMessage(c.params)})
		if got != c.want {
			t.Errorf("CheckTarget(%q, %s) = %q, want %q", c.op, c.params, got, c.want)
		}
	}
}

// Summary and Data never leave the registry over their caps.
func TestFinishCapsAndRedacts(t *testing.T) {
	r := testRegistry(t)
	long := strings.Repeat("a", tools.MaxSummaryBytes+100)
	res := r.finish("op_1", Result{Summary: long}, nil)
	if len(res.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("Summary = %d bytes, cap %d", len(res.Summary), tools.MaxSummaryBytes)
	}
	if !res.Truncated || !res.OK {
		t.Fatalf("result = %+v", res)
	}
	big := map[string]string{"body": strings.Repeat("b", protocol.MaxDataBytes+1)}
	res = r.finish("op_2", Result{Data: big}, nil)
	if len(res.Data) > protocol.MaxDataBytes {
		t.Fatalf("Data = %d bytes, cap %d", len(res.Data), protocol.MaxDataBytes)
	}
	if !res.Truncated {
		t.Fatal("Truncated = false for cut Data")
	}
	// A failed result is OK false and keeps its summary.
	res = r.finish("op_3", Result{Summary: "nope"}, errors.New("boom"))
	if res.OK || res.Summary != "nope" {
		t.Fatalf("failed result = %+v", res)
	}
}

func TestRegisterRefusesDuplicates(t *testing.T) {
	r := testRegistry(t)
	if err := r.Register(envDetectOp{}); err == nil {
		t.Fatal("duplicate Register = nil")
	}
	if err := r.Register(nil); err == nil {
		t.Fatal("nil Register = nil")
	}
}

func TestRegisterMountDir(t *testing.T) {
	r := testRegistry(t)
	if err := r.RegisterMountDir("connect/plugins"); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterMountDir("connect/plugins"); err != nil {
		t.Fatalf("idempotent RegisterMountDir = %v", err)
	}
	if err := r.RegisterMountDir("../escape"); err == nil {
		t.Fatal("RegisterMountDir accepted dot-dot")
	}
}

// TestDecodeParamsRefusesCaseCollidingKeys: two keys the decoder would fold
// into one field are refused, at any depth, instead of letting the last win.
func TestDecodeParamsRefusesCaseCollidingKeys(t *testing.T) {
	var dst struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	for _, raw := range []string{
		`{"name":"wal_level","NAME":"archive_command","value":"x"}`,
		`{"name":"wal_level","name":"archive_command","value":"x"}`,
	} {
		if err := decodeParams(json.RawMessage(raw), &dst); !errors.Is(err, ErrParams) {
			t.Fatalf("decodeParams(%s) = %v, want ErrParams", raw, err)
		}
	}
	if err := decodeParams(json.RawMessage(`{"name":"wal_level","value":"logical"}`), &dst); err != nil || dst.Name != "wal_level" {
		t.Fatalf("decodeParams = %v (%+v), want the value", err, dst)
	}
}
