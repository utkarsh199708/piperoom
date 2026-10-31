package setup

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	setupactions "piperoom/internal/setup/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/targets/strimzi"
)

// TestRecordedStrimziOpsPassTheRunnerAndTheServer replays every recorded
// Strimzi op that succeeded through the runner's own Validate and the
// server's scope for that action: a recording of a success the runner or
// the server now refuses (a rule tightened after it was recorded) fails here
// instead of replaying as if the rule did not exist (round 4 correctness 1).
// The pipeline is the one the recording names: pipeline orders in namespace
// data, the topic prefix of its canary topic, the connectors and source
// tables its connector steps name.
func TestRecordedStrimziOpsPassTheRunnerAndTheServer(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
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
		pl := owners.Pipeline{ID: "orders"}
		for _, op := range rec.Ops {
			var p struct {
				Template string `json:"template"`
				Params   struct {
					Connector   string            `json:"connector"`
					TopicPrefix string            `json:"topic_prefix"`
					Config      map[string]string `json:"config"`
				} `json:"params"`
			}
			if op.Op != "k8s_apply" || json.Unmarshal(op.Params, &p) != nil {
				continue
			}
			if p.Params.TopicPrefix != "" {
				pl.TopicPrefix = p.Params.TopicPrefix
			}
			if p.Template == "connector" && !slices.Contains(pl.Connectors, p.Params.Connector) {
				pl.Connectors = append(pl.Connectors, p.Params.Connector)
			}
			for _, tb := range strings.Split(p.Params.Config["table.include.list"], ",") {
				if tb = strings.TrimSpace(tb); tb != "" && !slices.Contains(pl.SourceTables, tb) {
					pl.SourceTables = append(pl.SourceTables, tb)
				}
			}
		}
		connectors := pl.Connectors
		if len(connectors) == 0 {
			connectors = []string{"orders-src"}
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
		if _, err := strimzi.Register(reg, strimzi.Options{Pipeline: "orders", Connectors: connectors, API: api}); err != nil {
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
