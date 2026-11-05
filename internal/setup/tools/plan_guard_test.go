package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// guardCatalogue answers a floor for every op the guard table names.
func guardCatalogue() fakeCatalogue {
	cat := fakeCatalogue{"compose_up": plan.TierT1}
	for _, row := range plan.GuardRows() {
		cat[row.Op] = plan.TierT1
	}
	cat[plan.OpSourceAlterSystem] = plan.TierT3
	return cat
}

func oneStepPlan(op string) json.RawMessage {
	return json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"` + op + `"}]}]}`)
}

// TestSetupPlanGuardOnProduction is review item E of #1012: in a room whose
// targets are all production, setup_plan plans the steps whose guard row
// depends on the proposal's params (source_alter_system, compose_down), and
// still refuses every row that refuses production whatever the params.
func TestSetupPlanGuardOnProduction(t *testing.T) {
	cases := []struct {
		op      string
		planned bool
	}{
		{plan.OpSourceAlterSystem, true},
		{plan.OpComposeDown, true},
		{"compose_up", true}, // no row guards it
	}
	for _, row := range plan.GuardRows() {
		if row.Class == plan.ClassProduction && !row.FactDependent() {
			cases = append(cases, struct {
				op      string
				planned bool
			}{row.Op, false})
		}
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			sink := &recordingSink{}
			tool := planTool(t, roomDeps(t, guardCatalogue(), sink, plan.ClassProduction))
			res, err := tool.Run(context.Background(), oneStepPlan(c.op))
			if !c.planned {
				if !errors.Is(err, tools.ErrBadInput) || !errors.Is(err, plan.ErrGuard) {
					t.Fatalf("Run = %v, want ErrBadInput wrapping plan.ErrGuard", err)
				}
				if got := len(sink.recorded()); got != 0 {
					t.Fatalf("a refused plan emitted %d events", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := len(sink.recorded()); got != 1 {
				t.Fatalf("emitted %d events, want 1 setup.plan", got)
			}
			deferred := plan.GuardDeferred(c.op, plan.ClassProduction)
			if got := strings.Contains(res.Summary, "guard checked on the proposal's params"); got != deferred {
				t.Fatalf("summary %q: deferral marked = %v, want %v", res.Summary, got, deferred)
			}
		})
	}
}

// TestSetupPlanGuardOtherClasses: off production nothing is guarded, so every
// guarded op plans and none is marked deferred; a class that is not a class
// is refused before the guard (plan.ErrClass), never planned.
func TestSetupPlanGuardOtherClasses(t *testing.T) {
	ops := []string{plan.OpSourceAlterSystem, plan.OpComposeDown, plan.OpSourceDropTable, plan.OpFileWrite}
	for _, class := range []string{plan.ClassLocal, plan.ClassShared} {
		for _, op := range ops {
			tool := planTool(t, roomDeps(t, guardCatalogue(), &recordingSink{}, class))
			res, err := tool.Run(context.Background(), oneStepPlan(op))
			if err != nil {
				t.Fatalf("%s on %s: %v", op, class, err)
			}
			if strings.Contains(res.Summary, "guard checked") {
				t.Errorf("%s on %s marked deferred: %q", op, class, res.Summary)
			}
		}
	}
	for _, class := range []string{"", "staging", "Production"} {
		for _, op := range ops {
			sink := &recordingSink{}
			tool := planTool(t, roomDeps(t, guardCatalogue(), sink, class))
			_, err := tool.Run(context.Background(), oneStepPlan(op))
			if !errors.Is(err, tools.ErrBadInput) || !errors.Is(err, plan.ErrClass) {
				t.Errorf("%s on %q: Run = %v, want ErrBadInput wrapping plan.ErrClass", op, class, err)
			}
			if got := len(sink.recorded()); got != 0 {
				t.Errorf("%s on %q emitted %d events", op, class, got)
			}
		}
	}
}

// TestSetupPlanFactsStillDecide: a non-nil Facts keeps deciding every row at
// plan time, with the zero StepFacts as the strictest reading.
func TestSetupPlanFactsStillDecide(t *testing.T) {
	deps := roomDeps(t, guardCatalogue(), &recordingSink{}, plan.ClassProduction)
	deps.Facts = func(plan.Step) plan.StepFacts { return plan.StepFacts{} }
	tool := planTool(t, deps)
	for _, op := range []string{plan.OpSourceAlterSystem, plan.OpComposeDown} {
		if _, err := tool.Run(context.Background(), oneStepPlan(op)); !errors.Is(err, plan.ErrGuard) {
			t.Errorf("%s with zero facts: Run = %v, want plan.ErrGuard", op, err)
		}
	}
	deps.Facts = func(plan.Step) plan.StepFacts { return plan.StepFacts{Param: "wal_level"} }
	tool = planTool(t, deps)
	if _, err := tool.Run(context.Background(), oneStepPlan(plan.OpSourceAlterSystem)); err != nil {
		t.Errorf("alter system with wal_level: %v", err)
	}
}

// TestSetupPlanDeferredStepStillGuardedAtCreate: a step planned with its
// guard deferred is decided by plan.Forbidden on the proposal's real params,
// as the room's Create check calls it (setupGuardFacts in internal/room,
// covered there by TestSetupGuardAtCreate and TestSetupRouteTiers). Params
// that break the exception are refused; only the exception passes.
func TestSetupPlanDeferredStepStillGuardedAtCreate(t *testing.T) {
	tool := planTool(t, roomDeps(t, guardCatalogue(), &recordingSink{}, plan.ClassProduction))
	for _, op := range []string{plan.OpSourceAlterSystem, plan.OpComposeDown} {
		res, err := tool.Run(context.Background(), oneStepPlan(op))
		if err != nil {
			t.Fatalf("plan %s: %v", op, err)
		}
		if got := res.Data.(events.SetupPlanPayload).Phases[0].Steps[0].Op; got != op {
			t.Fatalf("planned op = %q, want %q", got, op)
		}
	}
	alterFacts := func(t *testing.T, params string) plan.StepFacts {
		t.Helper()
		p, err := plan.DecodeAlterSystemStep(json.RawMessage(params))
		if err != nil {
			return plan.StepFacts{} // the room reads undecodable params as the strictest facts
		}
		return plan.StepFacts{Param: p.Name}
	}
	cases := []struct {
		name      string
		op        string
		facts     func(t *testing.T) plan.StepFacts
		forbidden bool
	}{
		{"alter system off the allow-list", plan.OpSourceAlterSystem, func(t *testing.T) plan.StepFacts {
			return alterFacts(t, `{"name":"shared_buffers","value":"1GB"}`)
		}, true},
		{"alter system with no name", plan.OpSourceAlterSystem, func(t *testing.T) plan.StepFacts {
			return alterFacts(t, `{"value":"logical"}`)
		}, true},
		{"alter system with folded duplicate keys", plan.OpSourceAlterSystem, func(t *testing.T) plan.StepFacts {
			return alterFacts(t, `{"name":"wal_level","Name":"shared_buffers"}`)
		}, true},
		{"alter system wrong case", plan.OpSourceAlterSystem, func(t *testing.T) plan.StepFacts {
			return alterFacts(t, `{"name":"WAL_LEVEL","value":"logical"}`)
		}, true},
		{"alter system on the allow-list", plan.OpSourceAlterSystem, func(t *testing.T) plan.StepFacts {
			return alterFacts(t, `{"name":"wal_level","value":"logical"}`)
		}, false},
		{"compose_down removing volumes", plan.OpComposeDown, func(*testing.T) plan.StepFacts {
			return plan.StepFacts{KeepsVolumes: false}
		}, true},
		{"compose_down created by this room only", plan.OpComposeDown, func(*testing.T) plan.StepFacts {
			return plan.StepFacts{CreatedByRoom: true}
		}, true},
		{"compose_down keeping volumes", plan.OpComposeDown, func(*testing.T) plan.StepFacts {
			return plan.StepFacts{KeepsVolumes: true}
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, forbidden := plan.Forbidden(plan.GuardRequest{Op: c.op, Class: plan.ClassProduction, Facts: c.facts(t)})
			if forbidden != c.forbidden {
				t.Fatalf("Forbidden at Create = %v, want %v", forbidden, c.forbidden)
			}
		})
	}
}
