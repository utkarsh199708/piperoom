package setuptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// fakeCatalogue is a plan.Catalogue over a map of op -> floor.
type fakeCatalogue map[string]string

func (c fakeCatalogue) Floor(op string) (string, bool) {
	floor, ok := c[op]
	return floor, ok
}

// recordingSink records every event and can refuse selected types.
type recordingSink struct {
	mu        sync.Mutex
	events    []sinkEvent
	refuse    map[events.Type]bool
	refuseAll bool
}

type sinkEvent struct {
	typ     events.Type
	payload any
}

func (s *recordingSink) Emit(_ context.Context, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refuseAll || s.refuse[typ] {
		return errors.New("refused " + string(typ))
	}
	s.events = append(s.events, sinkEvent{typ: typ, payload: payload})
	return nil
}

func (s *recordingSink) recorded() []sinkEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sinkEvent(nil), s.events...)
}

func roomDeps(t *testing.T, cat plan.Catalogue, sink Emitter, class string) RoomDeps {
	t.Helper()
	return RoomDeps{
		Catalogue: cat,
		Sink:      sink,
		ClassOf:   FixedClass(class),
	}
}

func planTool(t *testing.T, deps RoomDeps) tools.Tool {
	t.Helper()
	toolsOut, err := RoomTools(deps)
	if err != nil {
		t.Fatalf("RoomTools: %v", err)
	}
	for _, tool := range toolsOut {
		if tool.Name() == "setup_plan" {
			return tool
		}
	}
	t.Fatal("setup_plan was not built")
	return nil
}

func TestRoomToolsAreKindRoom(t *testing.T) {
	deps := roomDeps(t, fakeCatalogue{"file_write": plan.TierT1}, &recordingSink{}, plan.ClassLocal)
	toolsOut, err := RoomTools(deps)
	if err != nil {
		t.Fatalf("RoomTools: %v", err)
	}
	names := RoomToolNames()
	if len(toolsOut) != len(names) {
		t.Fatalf("RoomTools returned %d tools, want %d", len(toolsOut), len(names))
	}
	for i, tool := range toolsOut {
		if tool.Name() != names[i] {
			t.Errorf("tool %d = %q, want %q", i, tool.Name(), names[i])
		}
		if tool.Kind() != tools.KindRoom {
			t.Errorf("%s kind = %q, want %q", tool.Name(), tool.Kind(), tools.KindRoom)
		}
		if _, err := tool.Run(context.Background(), nil); err != nil {
			// setup_plan with no input is bad input, not a panic; the request
			// tool succeeds. Only the kind is asserted here.
			if tool.Name() == "setup_plan" && !errors.Is(err, tools.ErrBadInput) {
				t.Errorf("%s(nil) = %v, want ErrBadInput", tool.Name(), err)
			}
		}
	}
}

func TestRoomToolsNeedTheirDeps(t *testing.T) {
	if _, err := RoomTools(RoomDeps{}); err == nil {
		t.Fatal("RoomTools with no deps = nil, want an error")
	}
	if _, err := RoomTools(RoomDeps{Catalogue: fakeCatalogue{}}); err == nil {
		t.Fatal("RoomTools with no sink = nil, want an error")
	}
	if _, err := RoomTools(RoomDeps{Catalogue: fakeCatalogue{}, Sink: &recordingSink{}}); err == nil {
		t.Fatal("RoomTools with no class resolver = nil, want an error")
	}
}

func TestSetupPlanRefusesAnOpOutsideTheCatalogue(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassLocal))

	input := json.RawMessage(`{"phases":[{"name":"stack","steps":[{"op":"compose_up"}]}]}`)
	_, err := tool.Run(context.Background(), input)
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("Run = %v, want ErrBadInput", err)
	}
	if !errors.Is(err, plan.ErrUnknownOp) {
		t.Fatalf("Run = %v, want it to wrap plan.ErrUnknownOp", err)
	}
	if !strings.Contains(err.Error(), "compose_up") {
		t.Fatalf("the refusal does not name the op: %v", err)
	}
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("a refused plan emitted %d events, want 0", got)
	}
}

func TestSetupPlanEmitsTheResolvedPlan(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1, "compose_up": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassLocal))

	input := json.RawMessage(`{
		"phases": [
			{"name":"stack","steps":[{"op":"file_write","params_hint":"compose.yaml","verify":"compose config renders"}]},
			{"name":"services","steps":[{"op":"compose_up"}]}
		]
	}`)
	res, err := tool.Run(context.Background(), input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	recorded := sink.recorded()
	if len(recorded) != 1 || recorded[0].typ != events.SetupPlan {
		t.Fatalf("emitted %+v, want exactly one setup.plan", recorded)
	}
	payload, ok := recorded[0].payload.(events.SetupPlanPayload)
	if !ok {
		t.Fatalf("payload = %T, want events.SetupPlanPayload", recorded[0].payload)
	}
	if !planIDRe.MatchString(payload.PlanID) || !strings.HasPrefix(payload.PlanID, "pl_") {
		t.Fatalf("plan id = %q, want a server-minted pl_<hex>", payload.PlanID)
	}
	if len(payload.Phases) != 2 || len(payload.Phases[1].Steps) != 1 {
		t.Fatalf("phases = %+v", payload.Phases)
	}
	step := payload.Phases[0].Steps[0]
	if step.Index != 0 || step.Op != "file_write" || step.Tier != plan.TierT1 {
		t.Fatalf("step = %+v, want index 0, file_write at T1", step)
	}
	if payload.Phases[1].Steps[0].Index != 1 {
		t.Fatalf("second step index = %d, want 1", payload.Phases[1].Steps[0].Index)
	}
	stored, ok := res.Data.(events.SetupPlanPayload)
	if !ok || stored.PlanID != payload.PlanID {
		t.Fatalf("result data = %T %+v, want the stored plan", res.Data, res.Data)
	}
	if !strings.Contains(res.Summary, payload.PlanID) || !strings.Contains(res.Summary, "file_write(T1)") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestSetupPlanAssignsAPlanID(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassLocal))
	res, err := tool.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	payload := res.Data.(events.SetupPlanPayload)
	if !strings.HasPrefix(payload.PlanID, "pl_") || len(payload.PlanID) > plan.MaxPlanIDBytes {
		t.Fatalf("plan id = %q, want pl_+hex within the cap", payload.PlanID)
	}
}

func TestSetupPlanRefusesAModelPlanID(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassLocal))
	// A model re-planning with the id of the plan whose ladder just ran would
	// make that verdict count for steps that never executed.
	for _, id := range []string{"pl_orders", "pl_0123456789abcdef0123", "x\\u0007"} {
		_, err := tool.Run(context.Background(), json.RawMessage(
			`{"plan_id":"`+id+`","phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
		if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "plan_id") {
			t.Fatalf("Run with plan_id %q = %v, want ErrBadInput naming plan_id", id, err)
		}
	}
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("a plan with a model plan_id emitted %d events, want 0", got)
	}
	props := tool.InputSchema()["properties"].(map[string]any)
	if _, ok := props["plan_id"]; ok {
		t.Fatal("plan_id is in the input schema; the server mints it")
	}
}

func TestSetupPlanMintsAFreshIDEveryTime(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	tool := planTool(t, roomDeps(t, cat, &recordingSink{}, plan.ClassLocal))
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		res, err := tool.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		id := res.Data.(events.SetupPlanPayload).PlanID
		if seen[id] {
			t.Fatalf("plan id %q was minted twice", id)
		}
		seen[id] = true
	}
}

func TestSetupPlanRefusesABadSupersedes(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassLocal))
	for _, sup := range []string{"pl old", "pl_\\u001b[31m", "<b>pl</b>"} {
		_, err := tool.Run(context.Background(), json.RawMessage(
			`{"supersedes":"`+sup+`","phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
		if !errors.Is(err, tools.ErrBadInput) {
			t.Fatalf("Run with supersedes %q = %v, want ErrBadInput", sup, err)
		}
	}
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("emitted %d events, want 0", got)
	}
}

// layerCatalogue is a fakeCatalogue that also answers layers.
type layerCatalogue struct {
	fakeCatalogue
	layers map[string]string
}

func (c layerCatalogue) LayerOf(op string) (string, bool) {
	l, ok := c.layers[op]
	return l, ok
}

func TestSetupPlanFillsLayerFromTheCatalogue(t *testing.T) {
	cat := layerCatalogue{
		fakeCatalogue: fakeCatalogue{"file_write": plan.TierT1, "compose_up": plan.TierT1},
		layers:        map[string]string{"file_write": "environment", "compose_up": "stack"},
	}
	tool := planTool(t, roomDeps(t, cat, &recordingSink{}, plan.ClassLocal))
	res, err := tool.Run(context.Background(), json.RawMessage(
		`{"phases":[{"name":"p","steps":[{"op":"file_write"},{"op":"compose_up"}]}]}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	steps := res.Data.(events.SetupPlanPayload).Phases[0].Steps
	if steps[0].Layer != "environment" || steps[1].Layer != "stack" {
		t.Fatalf("layers = %q, %q; want environment, stack", steps[0].Layer, steps[1].Layer)
	}
	if steps[0].Target != "" {
		t.Fatalf("target = %q; Target is left to the room (U1-10)", steps[0].Target)
	}
	// A model-written layer is still refused by plan.Decode.
	_, err = tool.Run(context.Background(), json.RawMessage(
		`{"phases":[{"name":"p","steps":[{"op":"file_write","layer":"x"}]}]}`))
	if !errors.Is(err, tools.ErrBadInput) || !errors.Is(err, plan.ErrPlan) {
		t.Fatalf("Run with a model layer = %v, want a plan.Decode refusal", err)
	}
}

func TestSetupPlanRaisesATierForASharedTarget(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassShared))
	res, err := tool.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	step := res.Data.(events.SetupPlanPayload).Phases[0].Steps[0]
	if step.Tier != plan.TierT2 {
		t.Fatalf("tier = %q, want T2 on a shared target", step.Tier)
	}
}

func TestSetupPlanRefusesAGuardedStep(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassProduction))
	_, err := tool.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("Run = %v, want ErrBadInput", err)
	}
	if !errors.Is(err, plan.ErrGuard) {
		t.Fatalf("Run = %v, want it to wrap plan.ErrGuard", err)
	}
	if got := len(sink.recorded()); got != 0 {
		t.Fatalf("a guarded plan emitted %d events, want 0", got)
	}
}

func TestSetupPlanRefusesAServerOwnedField(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	tool := planTool(t, roomDeps(t, cat, &recordingSink{}, plan.ClassLocal))
	_, err := tool.Run(context.Background(), json.RawMessage(
		`{"phases":[{"name":"p","steps":[{"op":"file_write","class":"local"}]}]}`))
	if !errors.Is(err, tools.ErrBadInput) || !errors.Is(err, plan.ErrPlan) {
		t.Fatalf("Run = %v, want a plan.Decode refusal", err)
	}
}

func TestSetupPlanClassResolutionError(t *testing.T) {
	deps := RoomDeps{
		Catalogue: fakeCatalogue{"file_write": plan.TierT1},
		Sink:      &recordingSink{},
		ClassOf: func(plan.Step) (string, error) {
			return "", errors.New("no target for this op")
		},
	}
	tool := planTool(t, deps)
	_, err := tool.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
	if err == nil || !strings.Contains(err.Error(), "no target") {
		t.Fatalf("Run = %v, want the class resolution error", err)
	}
}

func TestSetupPlanSinkFailureIsAnError(t *testing.T) {
	sink := &recordingSink{refuseAll: true}
	tool := planTool(t, roomDeps(t, fakeCatalogue{"file_write": plan.TierT1}, sink, plan.ClassLocal))
	_, err := tool.Run(context.Background(), json.RawMessage(`{"phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
	if err == nil || !strings.Contains(err.Error(), "recording setup.plan") {
		t.Fatalf("Run = %v, want the sink error", err)
	}
}

func TestSetupPlanKeepsSupersedes(t *testing.T) {
	cat := fakeCatalogue{"file_write": plan.TierT1}
	sink := &recordingSink{}
	tool := planTool(t, roomDeps(t, cat, sink, plan.ClassLocal))
	res, err := tool.Run(context.Background(), json.RawMessage(
		`{"supersedes":"pl_old","phases":[{"name":"p","steps":[{"op":"file_write"}]}]}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := res.Data.(events.SetupPlanPayload).Supersedes; got != "pl_old" {
		t.Fatalf("supersedes = %q, want pl_old", got)
	}
}

func TestSetupPlanInputSchemaIsBounded(t *testing.T) {
	tool := planTool(t, roomDeps(t, fakeCatalogue{}, &recordingSink{}, plan.ClassLocal))
	schema := tool.InputSchema()
	if schema["additionalProperties"] != false || schema["type"] != "object" {
		t.Fatalf("schema is not a closed object: %v", schema)
	}
	req, ok := schema["required"].([]string)
	if !ok || len(req) != 1 || req[0] != "phases" {
		t.Fatalf("required = %v, want [phases]", schema["required"])
	}
}
