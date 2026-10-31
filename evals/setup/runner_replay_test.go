package setup_test

import (
	"slices"
	"testing"

	"piperoom/evals"
	evalsetup "piperoom/evals/setup"
	"piperoom/internal/events"
	setupactions "piperoom/internal/setup/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/targets/strimzi"
)

// TestRecordedStrimziOpsPassTheRunnerAndTheServer replays every recorded
// Strimzi op that succeeded through the runner's own Validate and the
// server's scope for that action: a recording of a success the runner or
// the server now refuses (a rule tightened after it was recorded) fails here
// instead of replaying as if the rule did not exist (round 4 correctness 1).
// The pipeline is the scenario's own (its pipeline: block), not one derived
// from the recording, so a recording whose topic prefix, connectors or tables
// differ from the scenario's fails here (round 6, E).
func TestRecordedStrimziOpsPassTheRunnerAndTheServer(t *testing.T) {
	recs, err := evalsetup.LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	scs, err := evals.LoadScenarios("../scenarios")
	if err != nil {
		t.Fatal(err)
	}
	scenarios := map[string]evals.Scenario{}
	for _, sc := range scs {
		scenarios[sc.Name] = sc
	}
	specs := map[string]setupactions.Spec{}
	for _, s := range setupactions.EnvironmentSpecs() {
		specs[s.Name] = s
	}
	checked := 0
	for name, rec := range recs {
		if rec.Target != "strimzi" {
			continue
		}
		sc, ok := scenarios[rec.Scenario]
		if !ok {
			t.Errorf("%s: no scenario %s in ../scenarios", name, rec.Scenario)
			continue
		}
		pl := sc.Pipeline
		if pl.ID == "" || pl.TopicPrefix == "" || len(pl.Connectors) == 0 {
			t.Fatalf("%s: scenario %s has no pipeline id, topic_prefix or connectors", name, rec.Scenario)
		}
		reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		api, err := strimzi.NewAPI(strimzi.APIConfig{BaseURL: "https://127.0.0.1:1", Token: func() (string, error) { return "t", nil }})
		if err != nil {
			t.Fatal(err)
		}
		before := reg.Names()
		if _, err := strimzi.Register(reg, strimzi.Options{Pipeline: pl.ID, Connectors: pl.Connectors, API: api}); err != nil {
			t.Fatal(err)
		}
		env := ops.Env{Targets: []events.SetupTarget{
			{Kind: "kubernetes", Address: "recorded/data"},
			{Kind: "kafka", Address: "strimzi://prod-kafka"},
		}}
		for i, op := range rec.Ops {
			if op.Result == nil || !op.Result.OK {
				continue
			}
			if o, ok := reg.Get(op.Op); ok && !slices.Contains(before, op.Op) {
				if err := o.Validate(op.Params, env); err != nil {
					t.Errorf("%s ops[%d] %s: recorded as a success, the runner now refuses it: %v", name, i, op.Op, err)
				}
				checked++
			}
			if spec, ok := specs[op.Op]; ok {
				a, err := setupactions.New(spec, setupactions.Deps{})
				if err != nil {
					t.Fatal(err)
				}
				if err := a.CheckScope(pl, op.Params); err != nil {
					t.Errorf("%s ops[%d] %s: recorded as a success, the server's scope now refuses it: %v", name, i, op.Op, err)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no recorded Strimzi op was checked")
	}
}
