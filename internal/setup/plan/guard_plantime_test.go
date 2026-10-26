package plan

import (
	"errors"
	"slices"
	"testing"
)

// guardCatalogue answers a floor for every op in the guard table, so a plan
// can name any guarded op.
func guardCatalogue() fakeCatalogue {
	cat := fakeCatalogue{}
	for _, row := range GuardRows() {
		cat[row.Op] = TierT1
	}
	cat[OpSourceAlterSystem] = TierT3
	return cat
}

// factCases are the StepFacts a room can derive from a step's params: the
// strictest reading, each exception alone, and both.
var factCases = []StepFacts{
	{},
	{Param: "wal_level"},
	{Param: "shared_buffers"},
	{KeepsVolumes: true},
	{CreatedByRoom: true},
	{Param: "wal_level", KeepsVolumes: true, CreatedByRoom: true},
}

// TestFactDependentRows pins which rows have an exception: exactly the ALTER
// SYSTEM allow-list and compose_down keeping its volumes. A new exception row
// fails here, so deferring it at plan time is a reviewed decision.
func TestFactDependentRows(t *testing.T) {
	var got []string
	for _, row := range GuardRows() {
		if row.FactDependent() {
			got = append(got, row.Op+"|"+row.Class)
		}
	}
	want := []string{OpSourceAlterSystem + "|" + ClassProduction, OpComposeDown + "|" + ClassProduction}
	if !slices.Equal(got, want) {
		t.Fatalf("fact-dependent rows = %v, want %v", got, want)
	}
}

// TestForbiddenAtPlanPerRow: on each row's own class, ForbiddenAtPlan refuses
// a row with no exception (with its reason) and defers a row with one; it
// never refuses what Forbidden allows with some facts, and every deferred row
// is still refused by Forbidden with the strictest facts, so the room's
// check points refuse it unless the params name the exception.
func TestForbiddenAtPlanPerRow(t *testing.T) {
	for _, row := range GuardRows() {
		t.Run(row.Op, func(t *testing.T) {
			got, refused := ForbiddenAtPlan(row.Op, row.Class)
			deferred := GuardDeferred(row.Op, row.Class)
			if row.FactDependent() {
				if refused {
					t.Fatalf("ForbiddenAtPlan refused a fact-dependent row (%s)", got.Reason)
				}
				if !deferred {
					t.Fatal("GuardDeferred = false for a fact-dependent row")
				}
				if _, forbidden := Forbidden(GuardRequest{Op: row.Op, Class: row.Class}); !forbidden {
					t.Fatal("Forbidden with the strictest facts allows a deferred row")
				}
				return
			}
			if !refused || got.Reason != row.Reason {
				t.Fatalf("ForbiddenAtPlan = %+v, %v; want refused with %q", got, refused, row.Reason)
			}
			if deferred {
				t.Fatal("GuardDeferred = true for an unconditional row")
			}
			for _, f := range factCases {
				if _, forbidden := Forbidden(GuardRequest{Op: row.Op, Class: row.Class, Facts: f}); !forbidden {
					t.Fatalf("plan refuses a row Forbidden allows with %+v", f)
				}
			}
		})
	}
}

// TestForbiddenAtPlanClasses: off production no row guards, so nothing is
// refused or deferred; a class that is not a class is refused whatever the
// op, with ReasonUnknownClass, and is never deferred.
func TestForbiddenAtPlanClasses(t *testing.T) {
	ops := []string{OpSourceAlterSystem, OpComposeDown, OpSourceDropTable, OpFileWrite, "compose_up"}
	cases := []struct {
		class      string
		wantRefuse bool
	}{
		{ClassLocal, false},
		{ClassShared, false},
		{"", true},
		{"staging", true},
		{"Production", true},
	}
	for _, c := range cases {
		for _, op := range ops {
			row, refused := ForbiddenAtPlan(op, c.class)
			if refused != c.wantRefuse {
				t.Errorf("ForbiddenAtPlan(%s, %q) refused = %v, want %v", op, c.class, refused, c.wantRefuse)
			}
			if refused && row.Reason != ReasonUnknownClass {
				t.Errorf("ForbiddenAtPlan(%s, %q) reason = %q, want %q", op, c.class, row.Reason, ReasonUnknownClass)
			}
			if GuardDeferred(op, c.class) {
				t.Errorf("GuardDeferred(%s, %q) = true, want false", op, c.class)
			}
		}
	}
	if GuardDeferred("compose_up", ClassProduction) {
		t.Error("GuardDeferred = true for an op no row guards")
	}
}

// TestResolveAtPlanTime: on production the two fact-dependent steps plan and
// every unconditional row refuses the plan with ErrGuard; the other checks
// are Resolve's.
func TestResolveAtPlanTime(t *testing.T) {
	cat := guardCatalogue()
	for _, row := range GuardRows() {
		t.Run(row.Op, func(t *testing.T) {
			err := oneStep(row.Op, ClassProduction).ResolveAtPlanTime(cat)
			if row.FactDependent() {
				if err != nil {
					t.Fatalf("ResolveAtPlanTime: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrGuard) {
				t.Fatalf("ResolveAtPlanTime = %v, want ErrGuard", err)
			}
		})
	}
	alter := oneStep(OpSourceAlterSystem, ClassProduction)
	if err := alter.ResolveAtPlanTime(cat); err != nil {
		t.Fatalf("alter system: %v", err)
	}
	if got := alter.Phases[0].Steps[0].Tier; got != TierT3 {
		t.Errorf("alter system tier = %q, want T3", got)
	}
	errorCases := []struct {
		name string
		p    *Plan
		cat  Catalogue
		want error
	}{
		{"unknown class", oneStep(OpSourceAlterSystem, "staging"), cat, ErrClass},
		{"empty class", oneStep(OpComposeDown, ""), cat, ErrClass},
		{"unknown op", oneStep("rm_rf", ClassProduction), cat, ErrUnknownOp},
		{"empty plan", &Plan{}, cat, ErrEmptyPlan},
		{"nil catalogue", oneStep(OpComposeDown, ClassProduction), nil, ErrPlan},
	}
	for _, c := range errorCases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.p.ResolveAtPlanTime(c.cat); !errors.Is(err, c.want) {
				t.Fatalf("ResolveAtPlanTime = %v, want %v", err, c.want)
			}
		})
	}
	// Resolve itself is unchanged: with no facts it still refuses both.
	for _, op := range []string{OpSourceAlterSystem, OpComposeDown} {
		if err := oneStep(op, ClassProduction).Resolve(cat, nil); !errors.Is(err, ErrGuard) {
			t.Errorf("Resolve(%s, nil facts) = %v, want ErrGuard", op, err)
		}
	}
}
