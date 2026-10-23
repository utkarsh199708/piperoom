package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

func roomTargets(ts ...events.SetupTarget) func(context.Context) []events.SetupTarget {
	return func(context.Context) []events.SetupTarget { return ts }
}

func TestPlanToolRecordsResolvedPlan(t *testing.T) {
	sink := &recordingSink{}
	tl, err := NewPlanTool(PlanConfig{Sink: sink, Targets: roomTargets(
		events.SetupTarget{Kind: "postgres", Address: "db.acme:5432", Class: protocol.ClassProduction},
		events.SetupTarget{Kind: "connect", Address: "localhost:8083", Class: protocol.ClassLocal},
		events.SetupTarget{Kind: "connect", Address: "connect.acme:8083", Class: protocol.ClassShared},
		events.SetupTarget{Kind: "kafka", Address: "localhost:9092", Class: protocol.ClassLocal},
	)})
	if err != nil {
		t.Fatal(err)
	}
	input := `{"phases":[
	  {"name":"look","steps":[{"op":"env_detect","verify":"the OS is known"},{"op":"kafka_topics"}]},
	  {"name":"stack","steps":[{"op":"compose_up","params_hint":"connect only"}]},
	  {"name":"connector","steps":[{"op":"source_create_publication"},{"op":"connect_create_connector"}]}
	],"supersedes":"plan_0123456789abcdef"}`
	res, err := tl.Run(context.Background(), json.RawMessage(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.evs) != 1 || sink.evs[0] != events.SetupPlan {
		t.Fatalf("emitted %v, want one setup.plan", sink.evs)
	}
	p := sink.pays[0].(events.SetupPlanPayload)
	if !regexp.MustCompile(`^plan_[0-9a-f]{16}$`).MatchString(p.PlanID) || p.Supersedes != "plan_0123456789abcdef" {
		t.Fatalf("plan id %q supersedes %q", p.PlanID, p.Supersedes)
	}
	type want struct{ op, target, tier, layer string }
	wants := []want{
		{"env_detect", "", plan.TierT0, "environment"},
		// one kafka target: it is the step's target; a read stays T0
		{"kafka_topics", "localhost:9092", plan.TierT0, "kafka"},
		// no registered compose target: shared, never local
		{"compose_up", "", plan.TierT2, "environment"},
		// the one postgres target is production: T2 floor raised to T3
		{"source_create_publication", "db.acme:5432", plan.TierT3, "postgres"},
		// two connect targets: none named, the higher class (shared)
		{"connect_create_connector", "", plan.TierT2, "kafka-connect"},
	}
	var got []want
	idx := 0
	for _, ph := range p.Phases {
		for _, s := range ph.Steps {
			if s.Index != idx {
				t.Errorf("step %s index %d, want %d", s.Op, s.Index, idx)
			}
			idx++
			got = append(got, want{s.Op, s.Target, s.Tier, s.Layer})
		}
	}
	if len(got) != len(wants) {
		t.Fatalf("steps = %+v", got)
	}
	for i := range wants {
		if got[i] != wants[i] {
			t.Errorf("step %d = %+v, want %+v", i, got[i], wants[i])
		}
	}
	if !strings.Contains(res.Summary, p.PlanID) || !strings.Contains(res.Summary, "source_create_publication T3 on db.acme:5432") {
		t.Fatalf("summary = %q", res.Summary)
	}
	if pl, ok := res.Data.(events.SetupPlanPayload); !ok || pl.PlanID != p.PlanID {
		t.Fatalf("data = %#v", res.Data)
	}
	// The hint is model text the payload never stores.
	if raw, _ := json.Marshal(p); strings.Contains(string(raw), "connect only") {
		t.Fatalf("payload stores the params hint: %s", raw)
	}
}

func TestPlanToolRefuses(t *testing.T) {
	targets := roomTargets(events.SetupTarget{Kind: "postgres", Address: "db.acme:5432", Class: protocol.ClassProduction})
	for _, tc := range []struct {
		name, input string
		want        error
	}{
		{"op not in the catalogue", `{"phases":[{"name":"x","steps":[{"op":"shell_exec"}]}]}`, plan.ErrUnknownOp},
		{"server-owned tier", `{"phases":[{"name":"x","steps":[{"op":"compose_up","tier":"T0"}]}]}`, plan.ErrPlan},
		{"server-owned target", `{"phases":[{"name":"x","steps":[{"op":"compose_up","target":"localhost"}]}]}`, plan.ErrPlan},
		{"plan id from the model", `{"plan_id":"plan_0000000000000000","phases":[{"name":"x","steps":[{"op":"compose_up"}]}]}`, tools.ErrBadInput},
		{"supersedes not a plan id", `{"supersedes":"../x","phases":[{"name":"x","steps":[{"op":"compose_up"}]}]}`, tools.ErrBadInput},
		{"empty plan", `{"phases":[]}`, plan.ErrEmptyPlan},
		{"unknown field", `{"phases":[{"name":"x","steps":[{"op":"compose_up"}]}],"run":true}`, plan.ErrPlan},
		{"guard forbids it on production", `{"phases":[{"name":"x","steps":[{"op":"source_alter_system"}]}]}`, plan.ErrGuard},
		{"too many steps", `{"phases":[{"name":"x","steps":[` + strings.TrimSuffix(strings.Repeat(`{"op":"compose_up"},`, plan.MaxStepsPerPhase+1), ",") + `]}]}`, plan.ErrTooLarge},
		{"not JSON", `{`, plan.ErrPlan},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &recordingSink{}
			tl, err := NewPlanTool(PlanConfig{Sink: sink, Targets: targets})
			if err != nil {
				t.Fatal(err)
			}
			_, err = tl.Run(context.Background(), json.RawMessage(tc.input))
			if !errors.Is(err, tools.ErrBadInput) || !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want ErrBadInput and %v", err, tc.want)
			}
			if len(sink.evs) != 0 {
				t.Fatalf("a refused plan emitted %v", sink.evs)
			}
		})
	}

	// A sink failure is not the model's to fix.
	boom := errors.New("room: agent sink refused event type")
	tl, err := NewPlanTool(PlanConfig{Sink: &recordingSink{err: boom}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tl.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"x","steps":[{"op":"compose_up"}]}]}`))
	if !errors.Is(err, boom) || errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("sink failure = %v", err)
	}
	if _, err := NewPlanTool(PlanConfig{}); err == nil {
		t.Fatal("setup_plan without a sink")
	}
}

func TestResolveTarget(t *testing.T) {
	for _, tc := range []struct {
		name        string
		kind        string
		targets     []events.SetupTarget
		target, cls string
	}{
		{"no kind", "", []events.SetupTarget{{Kind: "postgres", Address: "db", Class: "local"}}, "", "shared"},
		{"no target of the kind", "connect", []events.SetupTarget{{Kind: "postgres", Address: "db", Class: "local"}}, "", "shared"},
		{"one local", "postgres", []events.SetupTarget{{Kind: "postgres", Address: "db", Class: "local"}}, "db", "local"},
		{"class not a class reads shared", "postgres", []events.SetupTarget{{Kind: "postgres", Address: "db", Class: "dev"}}, "db", "shared"},
		{"empty class reads shared", "postgres", []events.SetupTarget{{Kind: "postgres", Address: "db"}}, "db", "shared"},
		{"two: the higher class, no target", "postgres", []events.SetupTarget{
			{Kind: "postgres", Address: "a", Class: "local"}, {Kind: "postgres", Address: "b", Class: "production"}}, "", "production"},
	} {
		target, cls := resolveTarget(tc.kind, tc.targets)
		if target != tc.target || cls != tc.cls {
			t.Errorf("%s: = %q %q, want %q %q", tc.name, target, cls, tc.target, tc.cls)
		}
	}
}

func TestPlanCatalogue(t *testing.T) {
	cat, err := PlanCatalogue()
	if err != nil {
		t.Fatal(err)
	}
	for op, floor := range map[string]string{
		"compose_up": plan.TierT1, "connect_create_connector": plan.TierT2, "pg_set_replica_identity": plan.TierT2,
		"source_alter_system": plan.TierT3, "connect_status": plan.TierT0, "env_detect": plan.TierT0,
	} {
		if got, ok := cat.Floor(op); !ok || got != floor {
			t.Errorf("Floor(%s) = %q, %v; want %s", op, got, ok, floor)
		}
	}
	if s, _ := cat.Spec("source_create_publication"); len(s.Schema) == 0 {
		t.Error("the client spec replaced the setup spec of source_create_publication")
	}
	if _, ok := cat.Floor("op_preview"); ok {
		t.Error("op_preview is a transport, not a plan step")
	}
}
