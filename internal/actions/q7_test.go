package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Q7 (Q0 item 6): Layered, Deferred, StepRef and the StepLayer fold.

// unlayeredAction answers Layer() "" and is not Layered: never registered.
type unlayeredAction struct{ classAction }

func (unlayeredAction) Layer() string { return "" }

// layeredAction derives its layer from params ({"layer": "..."}) the way the
// fix-PR action derives it from its path globs.
type layeredAction struct{ unlayeredAction }

func (layeredAction) LayerFor(p owners.Pipeline, raw json.RawMessage) (string, error) {
	var params struct {
		Layer string `json:"layer"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return "", err
	}
	if params.Layer == "unlisted" {
		return "", errors.New("path not listed")
	}
	return params.Layer, nil
}

// layeredSourceAction is Layered and SourceLayered: Layered wins.
type layeredSourceAction struct{ layeredAction }

func (layeredSourceAction) SourceLayered() {}

// deferredAction is the fix-PR shape: Layered and Deferred.
type deferredAction struct{ layeredAction }

func (deferredAction) Deferred() {}

// deferredClassed is a Deferred action that claims another class.
type deferredClassed struct {
	deferredAction
	class string
}

func (a deferredClassed) Class() string { return a.class }

var (
	_ Layered       = layeredAction{}
	_ Deferred      = deferredAction{}
	_ SourceLayered = layeredSourceAction{}
)

func TestStepLayer(t *testing.T) {
	cases := []struct {
		name       string
		a          Action
		sourceKind string
		params     string
		want       string
		wantErr    string
	}{
		{"plain action answers Layer()", kafkaAction{classAction{"kafka_x"}}, "", `{}`, "kafka", ""},
		{"plain postgres action ignores source kind", classAction{"pg_x"}, events.SourceKindMySQL, `{}`, "postgres", ""},
		{"source layered, default", sourceLayeredAction{classAction{"source_x"}}, "", `{}`, "postgres", ""},
		{"source layered, mysql", sourceLayeredAction{classAction{"source_x"}}, events.SourceKindMySQL, `{}`, events.LayerMySQL, ""},
		{"source layered, unknown kind fails closed", sourceLayeredAction{classAction{"source_x"}}, "oracle", `{}`, "", "unknown source_kind"},
		{"layered from params", layeredAction{unlayeredAction{classAction{"repo_x"}}}, "", `{"layer":"kafka-connect"}`, "kafka-connect", ""},
		{"layered error is wrapped", layeredAction{unlayeredAction{classAction{"repo_x"}}}, "", `{"layer":"unlisted"}`, "", "path not listed"},
		{"layered bad params", layeredAction{unlayeredAction{classAction{"repo_x"}}}, "", `not json`, "", "layer from params"},
		{"layered empty answer fails closed", layeredAction{unlayeredAction{classAction{"repo_x"}}}, "", `{}`, "", "no approving layer"},
		{"layered wins over source layered", layeredSourceAction{layeredAction{unlayeredAction{classAction{"both_x"}}}}, events.SourceKindMySQL, `{"layer":"warehouse"}`, "warehouse", ""},
		{"empty Layer() fails closed", unlayeredAction{classAction{"none_x"}}, "", `{}`, "", "no approving layer"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := owners.Pipeline{ID: "orders", SourceKind: c.sourceKind}
			got, err := StepLayer(c.a, p, json.RawMessage(c.params))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("StepLayer = %q, %v; want error containing %q", got, err, c.wantErr)
				}
				if got != "" {
					t.Fatalf("StepLayer returned layer %q with an error", got)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("StepLayer = %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

// LayerFor (Q8) keeps its answers: it knows SourceLayered only.
func TestLayerForIgnoresLayered(t *testing.T) {
	a := layeredAction{unlayeredAction{classAction{"repo_x"}}}
	if got := LayerFor(a, ""); got != "" {
		t.Fatalf("LayerFor(Layered) = %q, want \"\" (its Layer())", got)
	}
}

func TestRegisterQ7(t *testing.T) {
	cases := []struct {
		name    string
		a       Action
		wantErr string
	}{
		{"empty layer without Layered refused", unlayeredAction{classAction{"none_x"}}, "does not implement Layered"},
		{"empty layer with Layered registers", layeredAction{unlayeredAction{classAction{"repo_x"}}}, ""},
		{"deferred fix registers", deferredAction{layeredAction{unlayeredAction{classAction{"repo_pr"}}}}, ""},
		{"deferred explicit fix registers", deferredClassed{deferredAction{layeredAction{unlayeredAction{classAction{"repo_pr2"}}}}, ClassFix}, ""},
		{"deferred recovery refused", deferredClassed{deferredAction{layeredAction{unlayeredAction{classAction{"repo_rec"}}}}, ClassRecovery}, "a deferred action is a fix"},
		{"deferred performance refused", deferredClassed{deferredAction{layeredAction{unlayeredAction{classAction{"repo_perf"}}}}, ClassPerformance}, "a deferred action is a fix"},
		{"plain action registers as before", classAction{"pg_x"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := NewRegistry()
			err := r.Register(c.a)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("Register: %v", err)
				}
				if _, ok := r.Get(c.a.Name()); !ok {
					t.Fatal("registered action not found")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("Register = %v, want error containing %q", err, c.wantErr)
			}
			if len(r.All()) != 0 {
				t.Fatal("a refused action was registered")
			}
		})
	}
}

func TestStepRefContext(t *testing.T) {
	if _, ok := StepFrom(context.Background()); ok {
		t.Fatal("StepFrom on an empty context reported a step")
	}
	approvals := []string{"kafka-connect: Priya"}
	s := StepRef{RoomID: "r_1", ProposalID: "p_1", StepIndex: 2, Title: "fix connect image", Approvals: approvals}
	ctx := WithStep(context.Background(), s)
	approvals[0] = "changed by the caller"

	got, ok := StepFrom(ctx)
	if !ok {
		t.Fatal("StepFrom lost the step")
	}
	if got.RoomID != "r_1" || got.ProposalID != "p_1" || got.StepIndex != 2 || got.Title != "fix connect image" {
		t.Fatalf("StepFrom = %+v", got)
	}
	if len(got.Approvals) != 1 || got.Approvals[0] != "kafka-connect: Priya" {
		t.Fatalf("Approvals = %v: WithStep must copy the slice", got.Approvals)
	}
	got.Approvals[0] = "changed by the action"
	again, _ := StepFrom(ctx)
	if again.Approvals[0] != "kafka-connect: Priya" {
		t.Fatalf("Approvals = %v: StepFrom must hand out a copy", again.Approvals)
	}
	// A derived context keeps the step; another key does not shadow it.
	type otherKey struct{}
	if _, ok := StepFrom(context.WithValue(ctx, otherKey{}, "x")); !ok {
		t.Fatal("a derived context lost the step")
	}
}

func TestMaxDeferredSteps(t *testing.T) {
	if MaxDeferredSteps != 3 {
		t.Fatalf("MaxDeferredSteps = %d, want 3 (docs/design/fix-pr.md §14 D4)", MaxDeferredSteps)
	}
}
