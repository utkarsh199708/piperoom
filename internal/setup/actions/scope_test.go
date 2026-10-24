package setupactions

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

func TestCheckScopeAcceptsThePipelinesTables(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	if err := a.CheckScope(testPipeline(), grantParams); err != nil {
		t.Fatalf("CheckScope(in scope) = %v", err)
	}
}

func TestCheckScopeRefusesATargetOutsideTheProvisionalPipeline(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	out := json.RawMessage(`{"role":"piperoom_reader","tables":["public.customers"]}`)
	err := a.CheckScope(testPipeline(), out)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope(out of scope) = %v, want ErrOutOfScope", err)
	}
	if !strings.Contains(err.Error(), "public.customers") || !strings.Contains(err.Error(), "orders") {
		t.Fatalf("the refusal names neither the table nor the pipeline: %v", err)
	}
}

func TestCheckScopeRefusesAnEmptyTableList(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	if err := a.CheckScope(testPipeline(), json.RawMessage(`{"role":"r","tables":[]}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope(no tables) = %v, want ErrOutOfScope", err)
	}
}

func TestCheckScopeRefusesBadParams(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	cases := []json.RawMessage{
		json.RawMessage(`{"role":"r"}`),
		json.RawMessage(`{"role":"r","tables":"public.orders"}`),
		json.RawMessage(`[]`),
	}
	for _, in := range cases {
		if err := a.CheckScope(testPipeline(), in); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("CheckScope(%s) = %v, want ErrBadInput", in, err)
		}
	}
}

func TestScopeConnector(t *testing.T) {
	scope := ScopeConnector()
	ok := json.RawMessage(`{"connector":"pg-orders"}`)
	if err := scope(testPipeline(), ok); err != nil {
		t.Fatalf("in-scope connector: %v", err)
	}
	other := json.RawMessage(`{"connector":"pg-billing"}`)
	if err := scope(testPipeline(), other); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("out-of-scope connector = %v, want ErrOutOfScope", err)
	}
}

func TestScopeFreeAcceptsEveryPipeline(t *testing.T) {
	if err := ScopeFree(owners.Pipeline{}, json.RawMessage(`{"path":"compose.yaml"}`)); err != nil {
		t.Fatalf("ScopeFree = %v", err)
	}
}

func TestScopeIsCheckedBeforeAnyRunnerCall(t *testing.T) {
	f := &fakeRunner{}
	a := newTestAdapter(t, f)
	out := json.RawMessage(`{"role":"piperoom_reader","tables":["public.customers"]}`)
	ctx := testContext(t, out)
	for name, fn := range map[string]func() error{
		"execute": func() error { _, _, err := a.ExecuteReceipt(ctx, out); return err },
		"preview": func() error { _, err := a.Preview(ctx, out); return err },
	} {
		if err := fn(); !errors.Is(err, tools.ErrOutOfScope) {
			t.Fatalf("%s = %v, want ErrOutOfScope", name, err)
		}
	}
	if got := len(f.recorded()); got != 0 {
		t.Fatalf("an out-of-scope target sent %d runner ops, want 0", got)
	}
}

func TestScopeRefusalKeepsTheActionName(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	err := a.CheckScope(owners.Pipeline{ID: "orders"}, grantParams)
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope = %v", err)
	}
	if !strings.Contains(err.Error(), "source_grant_select") {
		t.Fatalf("the refusal does not name the action: %v", err)
	}
}

// TestScopeDoesNoIO pins the contract rule that CheckScope decides from params
// and the pipeline alone: a nil runner still answers the scope question.
func TestScopeDoesNoIO(t *testing.T) {
	a, err := New(testSpec(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := a.CheckScope(testPipeline(), grantParams); err != nil {
		t.Fatalf("CheckScope with no runner = %v", err)
	}
	if err := a.CheckScope(testPipeline(), json.RawMessage(`{"role":"r","tables":["public.x"]}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("CheckScope with no runner = %v, want the scope refusal", err)
	}
}

// TestTierPropagation pins Tiered.Tier: the op's floor raised to the class the
// room resolved, with no I/O.
func TestTierPropagation(t *testing.T) {
	cases := []struct {
		floor string
		class string
		want  string
	}{
		{plan.TierT1, protocol.ClassLocal, plan.TierT1},
		{plan.TierT1, protocol.ClassShared, plan.TierT2},
		{plan.TierT1, protocol.ClassProduction, plan.TierT3},
		{plan.TierT2, protocol.ClassLocal, plan.TierT2},
		{plan.TierT2, protocol.ClassShared, plan.TierT2},
		{plan.TierT2, protocol.ClassProduction, plan.TierT3},
		{plan.TierT3, protocol.ClassLocal, plan.TierT3},
		{plan.TierT0, protocol.ClassProduction, plan.TierT0},
	}
	for _, tc := range cases {
		spec := testSpec()
		spec.Tier = tc.floor
		a, err := New(spec, Deps{})
		if err != nil {
			t.Fatalf("New(%s): %v", tc.floor, err)
		}
		got, err := a.Tier(grantParams, tc.class)
		if err != nil {
			t.Fatalf("Tier(%s, %s): %v", tc.floor, tc.class, err)
		}
		if got != tc.want {
			t.Errorf("Tier(%s, %s) = %q, want %q", tc.floor, tc.class, got, tc.want)
		}
	}
}

func TestTierRefusesAnUnknownClass(t *testing.T) {
	a := newTestAdapter(t, &fakeRunner{})
	if _, err := a.Tier(grantParams, ""); !errors.Is(err, plan.ErrClass) {
		t.Fatalf("Tier with no class = %v, want ErrClass", err)
	}
	if _, err := a.Tier(grantParams, "prod"); !errors.Is(err, plan.ErrClass) {
		t.Fatalf("Tier with an unknown class = %v, want ErrClass", err)
	}
}

// TestTierDoesNoIO: a nil runner answers Tier.
func TestTierDoesNoIO(t *testing.T) {
	a, err := New(testSpec(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, err := a.Tier(grantParams, plan.ClassShared); err != nil || got != plan.TierT2 {
		t.Fatalf("Tier = %q, %v", got, err)
	}
}
