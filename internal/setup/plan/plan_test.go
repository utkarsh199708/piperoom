package plan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// fakeCatalogue is a plan.Catalogue: op -> floor.
type fakeCatalogue map[string]string

func (c fakeCatalogue) Floor(op string) (string, bool) {
	tier, ok := c[op]
	return tier, ok
}

var testCatalogue = fakeCatalogue{
	"file_write":               TierT1,
	"compose_up":               TierT1,
	"compose_down":             TierT1,
	"connect_create_connector": TierT2,
	"source_alter_system":      TierT3,
	"source_drop_table":        TierT2,
	"connect_status":           TierT0,
}

func oneStep(op, class string) *Plan {
	return &Plan{Phases: []Phase{{Name: "stack", Steps: []Step{{Op: op, Class: class}}}}}
}

func TestResolveFillsIndexAndTier(t *testing.T) {
	p := &Plan{
		PlanID: "p1",
		Phases: []Phase{
			{Name: "prerequisites", Steps: []Step{{Op: "connect_status", Class: ClassProduction}, {Op: "compose_up", Class: ClassLocal}}},
			{Name: "connector", Steps: []Step{{Op: "connect_create_connector", Class: ClassShared}, {Op: "compose_up", Class: ClassProduction}}},
		},
	}
	if err := p.Resolve(testCatalogue, nil); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := []struct {
		index int
		tier  string
	}{
		{0, TierT0}, // a read stays a tool on a production target
		{1, TierT1},
		{2, TierT2},
		{3, TierT3},
	}
	var got []struct {
		index int
		tier  string
	}
	for _, phase := range p.Phases {
		for _, step := range phase.Steps {
			got = append(got, struct {
				index int
				tier  string
			}{step.Index, step.Tier})
		}
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("step %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if index := p.Payload().Phases[0].Steps[0].Index; index != 0 {
		t.Errorf("payload first index = %d", index)
	}
}

func TestResolveTierCanBeRaisedNotLowered(t *testing.T) {
	raised := oneStep("compose_up", ClassLocal)
	raised.Phases[0].Steps[0].Tier = TierT2
	if err := raised.Resolve(testCatalogue, nil); err != nil {
		t.Fatalf("a raised tier is allowed: %v", err)
	}
	if got := raised.Phases[0].Steps[0].Tier; got != TierT2 {
		t.Errorf("tier = %q, want T2", got)
	}
	lowered := oneStep("compose_up", ClassProduction)
	lowered.Phases[0].Steps[0].Tier = TierT1
	if err := lowered.Resolve(testCatalogue, nil); !errors.Is(err, ErrTier) {
		t.Errorf("lowering T3 to T1: error = %v, want ErrTier", err)
	}
	unknown := oneStep("compose_up", ClassLocal)
	unknown.Phases[0].Steps[0].Tier = "T9"
	if err := unknown.Resolve(testCatalogue, nil); !errors.Is(err, ErrTier) {
		t.Errorf("tier T9: error = %v, want ErrTier", err)
	}
}

func TestResolveRefusesUnknownOp(t *testing.T) {
	if err := oneStep("no_such_op", ClassLocal).Resolve(testCatalogue, nil); !errors.Is(err, ErrUnknownOp) {
		t.Errorf("unknown op: error = %v, want ErrUnknownOp", err)
	}
	if err := oneStep("", ClassLocal).Resolve(testCatalogue, nil); !errors.Is(err, ErrUnknownOp) {
		t.Errorf("empty op: error = %v, want ErrUnknownOp", err)
	}
}

func TestResolveRefusesUnknownClass(t *testing.T) {
	for _, class := range []string{"", "prod", "Production"} {
		if err := oneStep("compose_up", class).Resolve(testCatalogue, nil); !errors.Is(err, ErrClass) {
			t.Errorf("class %q: error = %v, want ErrClass", class, err)
		}
	}
}

func TestResolveRefusesEmptyPlan(t *testing.T) {
	for name, p := range map[string]*Plan{
		"no phases":   {PlanID: "p"},
		"nil phases":  {},
		"empty phase": {Phases: []Phase{{Name: "prerequisites"}}},
	} {
		if err := p.Resolve(testCatalogue, nil); !errors.Is(err, ErrEmptyPlan) {
			t.Errorf("%s: error = %v, want ErrEmptyPlan", name, err)
		}
	}
	if err := oneStep("compose_up", ClassLocal).Resolve(nil, nil); !errors.Is(err, ErrPlan) {
		t.Errorf("nil catalogue: error = %v, want ErrPlan", err)
	}
}

// TestResolveGuard is "a plan with a forbidden op on the class is refused":
// with no facts (the strictest reading), every guarded op is refused on
// production and accepted on shared, and the facts that a step carries can
// name its row's exception.
func TestResolveGuard(t *testing.T) {
	if err := oneStep("source_drop_table", ClassProduction).Resolve(testCatalogue, nil); !errors.Is(err, ErrGuard) {
		t.Fatalf("drop table on production: error = %v, want ErrGuard", err)
	}
	if err := oneStep("source_drop_table", ClassShared).Resolve(testCatalogue, nil); err != nil {
		t.Errorf("drop table on shared: %v", err)
	}
	down := oneStep("compose_down", ClassProduction)
	if err := down.Resolve(testCatalogue, nil); !errors.Is(err, ErrGuard) {
		t.Errorf("compose_down on production with no facts: error = %v, want ErrGuard", err)
	}
	if err := down.Resolve(testCatalogue, func(Step) StepFacts { return StepFacts{KeepsVolumes: true} }); err != nil {
		t.Errorf("compose_down keeping volumes: %v", err)
	}
	alter := oneStep("source_alter_system", ClassProduction)
	if err := alter.Resolve(testCatalogue, func(Step) StepFacts { return StepFacts{Param: "wal_level"} }); err != nil {
		t.Errorf("alter system on wal_level: %v", err)
	}
	if err := alter.Resolve(testCatalogue, func(Step) StepFacts { return StepFacts{Param: "shared_buffers"} }); !errors.Is(err, ErrGuard) {
		t.Errorf("alter system on shared_buffers: error = %v, want ErrGuard", err)
	}
}

func TestResolveSizeCaps(t *testing.T) {
	manyPhases := &Plan{}
	for i := 0; i <= MaxPhases; i++ {
		manyPhases.Phases = append(manyPhases.Phases, Phase{Name: "p", Steps: []Step{{Op: "compose_up", Class: ClassLocal}}})
	}
	if err := manyPhases.Resolve(testCatalogue, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("%d phases: error = %v, want ErrTooLarge", len(manyPhases.Phases), err)
	}

	manySteps := &Plan{Phases: []Phase{{Name: "p"}}}
	for i := 0; i <= MaxStepsPerPhase; i++ {
		manySteps.Phases[0].Steps = append(manySteps.Phases[0].Steps, Step{Op: "compose_up", Class: ClassLocal})
	}
	if err := manySteps.Resolve(testCatalogue, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("%d steps in a phase: error = %v, want ErrTooLarge", len(manySteps.Phases[0].Steps), err)
	}

	deep := &Plan{PlanID: strings.Repeat("p", MaxPlanIDBytes+1), Phases: []Phase{{Name: "p", Steps: []Step{{Op: "compose_up", Class: ClassLocal}}}}}
	if err := deep.Resolve(testCatalogue, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("long plan id: error = %v, want ErrTooLarge", err)
	}

	longVerify := oneStep("compose_up", ClassLocal)
	longVerify.Phases[0].Steps[0].Verify = strings.Repeat("v", MaxVerifyBytes+1)
	if err := longVerify.Resolve(testCatalogue, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("long verify: error = %v, want ErrTooLarge", err)
	}

	longHint := oneStep("compose_up", ClassLocal)
	longHint.Phases[0].Steps[0].ParamsHint = strings.Repeat("h", MaxParamsHintBytes+1)
	if err := longHint.Resolve(testCatalogue, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("long params_hint: error = %v, want ErrTooLarge", err)
	}

	noName := &Plan{Phases: []Phase{{Steps: []Step{{Op: "compose_up", Class: ClassLocal}}}}}
	if err := noName.Resolve(testCatalogue, nil); !errors.Is(err, ErrPlan) {
		t.Errorf("a phase without a name: error = %v, want ErrPlan", err)
	}
}

func TestDecode(t *testing.T) {
	valid := `{
		"plan_id": "p1",
		"phases": [
			{"name": "stack", "steps": [
				{"op": "compose_up", "params_hint": "the Kafka services", "verify": "docker ps shows them"},
				{"op": "connect_create_connector", "verify": "connect_status is RUNNING"}
			]}
		]
	}`
	p, err := Decode([]byte(valid))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if p.PlanID != "p1" || len(p.Phases) != 1 || len(p.Phases[0].Steps) != 2 {
		t.Fatalf("Decode = %+v", p)
	}
	if p.Phases[0].Steps[0].ParamsHint != "the Kafka services" {
		t.Errorf("params_hint = %q", p.Phases[0].Steps[0].ParamsHint)
	}

	tests := []struct {
		name string
		in   string
		want error
	}{
		{name: "empty input", in: "", want: ErrPlan},
		{name: "not JSON", in: "nope", want: ErrPlan},
		{name: "unknown field", in: `{"phases":[{"name":"p","steps":[{"op":"compose_up","model":"x"}]}]}`, want: ErrPlan},
		{name: "trailing data", in: `{"phases":[]} {"phases":[]}`, want: ErrPlan},
		{name: "a number for a phase", in: `{"phases":5}`, want: ErrPlan},
		{name: "too deep", in: `{"phases":[],"x":` + strings.Repeat("[", MaxPlanDepth+1) + strings.Repeat("]", MaxPlanDepth+1) + `}`, want: ErrTooLarge},
		{name: "too large", in: `{"plan_id":"` + strings.Repeat("p", MaxPlanBytes) + `"}`, want: ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode([]byte(tt.in)); !errors.Is(err, tt.want) {
				t.Errorf("Decode(%d bytes) error = %v, want %v", len(tt.in), err, tt.want)
			}
		})
	}
}

// TestDecodeRefusesServerFields: the server-owned fields of a step are never
// read from the wire. A plan whose input carries any of them is refused with
// an error that names the field, so a model cannot choose its own class, tier
// or target and dodge the guard table or the tier floor.
func TestDecodeRefusesServerFields(t *testing.T) {
	tests := map[string]string{
		"index":  `{"phases":[{"name":"stack","steps":[{"op":"compose_up","index":3}]}]}`,
		"target": `{"phases":[{"name":"stack","steps":[{"op":"compose_up","target":"db"}]}]}`,
		"class":  `{"phases":[{"name":"stack","steps":[{"op":"compose_up","class":"local"}]}]}`,
		"tier":   `{"phases":[{"name":"stack","steps":[{"op":"compose_up","tier":"T0"}]}]}`,
		"layer":  `{"phases":[{"name":"stack","steps":[{"op":"compose_up","layer":"postgres"}]}]}`,
	}
	for field, in := range tests {
		t.Run(field, func(t *testing.T) {
			_, err := Decode([]byte(in))
			if !errors.Is(err, ErrPlan) {
				t.Fatalf("Decode error = %v, want ErrPlan", err)
			}
			if !strings.Contains(err.Error(), `"`+field+`"`) {
				t.Errorf("error %q does not name the field %q", err, field)
			}
		})
	}
}

// TestPayloadCleansModelText: every model-written string the stored payload
// keeps goes through cleanText, so a control character in a phase name or a
// verify sentence never reaches a stored event.
func TestPayloadCleansModelText(t *testing.T) {
	p := &Plan{Phases: []Phase{{
		Name:  "stack\x1b[2J",
		Steps: []Step{{Op: "compose_up", Class: ClassLocal, Verify: "docker ps\r\nshows them"}},
	}}}
	if err := p.Resolve(testCatalogue, nil); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	payload := p.Payload()
	if got, want := payload.Phases[0].Name, "stack?[2J"; got != want {
		t.Errorf("payload phase name = %q, want %q", got, want)
	}
	if got, want := payload.Phases[0].Steps[0].Verify, "docker ps??shows them"; got != want {
		t.Errorf("payload verify = %q, want %q", got, want)
	}
}

// TestPayload: the stored payload carries what the server filled and neither
// the model's hint nor the resolved class.
func TestPayload(t *testing.T) {
	p := &Plan{
		PlanID:     "p1",
		Supersedes: "p0",
		Phases: []Phase{{
			Name:  "connector",
			Steps: []Step{{Op: "connect_create_connector", ParamsHint: "orders", Verify: "RUNNING", Class: ClassShared, Target: "connect", Layer: "kafka-connect"}},
		}},
	}
	if err := p.Resolve(testCatalogue, nil); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	payload := p.Payload()
	if payload.PlanID != "p1" || payload.Supersedes != "p0" || len(payload.Phases) != 1 {
		t.Fatalf("Payload = %+v", payload)
	}
	step := payload.Phases[0].Steps[0]
	if step.Index != 0 || step.Op != "connect_create_connector" || step.Tier != TierT2 || step.Target != "connect" || step.Layer != "kafka-connect" || step.Verify != "RUNNING" {
		t.Errorf("payload step = %+v", step)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	for _, unwanted := range []string{"params_hint", "class"} {
		if strings.Contains(string(raw), `"`+unwanted+`"`) {
			t.Errorf("payload carries %q: %s", unwanted, raw)
		}
	}
}
