package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

const slotInactiveHypothesis = "pg-orders-connector is STOPPED (not FAILED): with no tasks, the replication slot " +
	"debezium_orders is inactive and retains WAL, so pg_wal grows to 48.6 GiB and the disk is at risk. " +
	"A long transaction is ruled out."

// slotInactiveScript is a focused investigation of slot-inactive-wal-growth
// that ends by proposing the one fix the scenario expects.
func slotInactiveScript(proposed ...events.ProposalStep) []step {
	return []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_replication_slots", input: map[string]any{}},
		{hypothesis: slotInactiveHypothesis},
		proposal(proposed...),
		{idle: true},
	}
}

// TestSlotInactiveResumeIsProposed: the expected outcome of the slot-inactive
// scenario is connect_resume_connector for the stopped connector. The proposal
// must pass the action's real Validate (which reads the recorded STOPPED state)
// and CheckScope, and the run must pass.
func TestSlotInactiveResumeIsProposed(t *testing.T) {
	sc, err := LoadScenario("scenarios/slot-inactive-wal-growth.yaml")
	if err != nil {
		t.Fatal(err)
	}
	script := slotInactiveScript(ps("connect_resume_connector", `{"connector":"pg-orders-connector"}`))
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	if !r.Pass || !r.FixOK || !r.RootCause || !r.LayerNamed {
		t.Fatalf("pass=%v fixOK=%v rootCause=%v layer=%v failures=%v bad=%v",
			r.Pass, r.FixOK, r.RootCause, r.LayerNamed, r.Failures, r.BadSteps)
	}
	if r.Score != 100 {
		t.Errorf("score = %v, want 100 (failures %v)", r.Score, r.Failures)
	}
}

// TestSlotInactiveRestartIsForbidden: restarting a connector that has no tasks
// does not resume it, so trying connect_restart_connector fails the run even
// though Validate accepts the connector name.
func TestSlotInactiveRestartIsForbidden(t *testing.T) {
	sc, err := LoadScenario("scenarios/slot-inactive-wal-growth.yaml")
	if err != nil {
		t.Fatal(err)
	}
	script := slotInactiveScript(
		ps("connect_resume_connector", `{"connector":"pg-orders-connector"}`),
		ps("connect_restart_connector", `{"connector":"pg-orders-connector"}`))
	r := Judge(sc, transcriptOf(t, sc, script), nil, false)
	if r.Pass {
		t.Fatal("run that tried connect_restart_connector passed")
	}
	if len(r.ForbiddenProposed) != 1 || r.ForbiddenProposed[0] != "connect_restart_connector" {
		t.Errorf("forbidden = %v", r.ForbiddenProposed)
	}
}

// TestConnectStatusDataDrivesResumeValidation: connect_resume_connector accepts
// a connector only when the recorded status says PAUSED or STOPPED. A scenario
// without that status data cannot validate the action at all.
func TestConnectStatusDataDrivesResumeValidation(t *testing.T) {
	statusData := func(state string) []Recording {
		return []Recording{{
			Tool:    "connect_status",
			Input:   map[string]any{"connector": "pg-orders-connector"},
			Summary: "pg-orders-connector (source): " + state + " on connect:8083.",
			Data: map[string]any{
				"name": "pg-orders-connector", "state": state, "worker": "connect:8083", "tasks": nil,
			},
		}}
	}
	res := Resources{Connectors: []string{"pg-orders-connector"}}
	params := json.RawMessage(`{"connector":"pg-orders-connector"}`)
	validate := func(recs []Recording) error {
		reg, err := LiveActions(res, recs)
		if err != nil {
			t.Fatal(err)
		}
		a, ok := reg.Get("connect_resume_connector")
		if !ok {
			t.Fatal("connect_resume_connector not registered")
		}
		return a.Validate(params)
	}
	if err := validate(statusData("STOPPED")); err != nil {
		t.Errorf("STOPPED refused: %v", err)
	}
	if err := validate(statusData("PAUSED")); err != nil {
		t.Errorf("PAUSED refused: %v", err)
	}
	if err := validate(statusData("RUNNING")); err == nil || !strings.Contains(err.Error(), "resume only applies to PAUSED or STOPPED") {
		t.Errorf("RUNNING: err = %v, want a refusal", err)
	}
	if err := validate(nil); err == nil || !strings.Contains(err.Error(), "not available in replay") {
		t.Errorf("no status data: err = %v, want the replay refusal", err)
	}
}

// TestLiveActionsStatusIsReadOnly keeps the replayed Connect client honest:
// resume reads the recorded status and never writes.
func TestLiveActionsStatusIsReadOnly(t *testing.T) {
	reg, err := LiveActions(Resources{Connectors: []string{"pg-orders-connector"}}, []Recording{{
		Tool: "connect_status", Input: map[string]any{"connector": "pg-orders-connector"},
		Summary: "x", Data: map[string]any{"name": "pg-orders-connector", "state": "STOPPED", "tasks": nil},
	}})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := reg.Get("connect_resume_connector")
	params := json.RawMessage(`{"connector":"pg-orders-connector"}`)
	if err := a.Validate(params); err != nil {
		t.Fatalf("resume of a recorded STOPPED connector refused: %v", err)
	}
	_, err = a.Execute(context.Background(), params)
	if !errors.Is(err, errWritesDisabled) {
		t.Errorf("resume Execute err = %v, want the replay write refusal", err)
	}
}

// TestScenarioFixturesCarryTSColumnAndOwners checks the fixtures against the
// shapes the real code renders. The warehouse sink line of every pipeline map
// ends with the pipeline's ts_column (topology sinkNode sets it on every sink),
// every warehouse_freshness recording passes that ts_column, and every
// "[layer, owner x]" names the layer's owner in dev/owners.yaml.
func TestScenarioFixturesCarryTSColumnAndOwners(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load("../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ownerRe := regexp.MustCompile(`\[([a-z-]+), owner ([a-z]+)\]`)
	for _, sc := range scs {
		col := sc.Pipeline.SinkTSColumn
		if len(sc.Pipeline.SinkTables) > 0 && col == "" {
			t.Errorf("%s: sink_tables set but sink_ts_column empty", sc.Name)
		}
		maps := map[string]string{"alert.pipeline_map": sc.Alert.PipelineMap}
		for i, r := range sc.Recordings {
			switch r.Tool {
			case "pipeline_map":
				maps[fmt.Sprintf("recordings[%d]", i)] = r.Summary
			case "warehouse_freshness":
				if got, _ := r.Input["ts_column"].(string); got != col {
					t.Errorf("%s: recordings[%d] warehouse_freshness ts_column = %q, want %q", sc.Name, i, got, col)
				}
			}
		}
		for where, text := range maps {
			for _, line := range strings.Split(text, "\n") {
				if strings.Contains(line, " wh:") && !strings.HasSuffix(line, " ts_column="+col) {
					t.Errorf("%s: %s sink line lacks ts_column=%s: %q", sc.Name, where, col, line)
				}
				for _, m := range ownerRe.FindAllStringSubmatch(line, -1) {
					if want, ok := o.LayerOwner(m[1]); !ok || want != m[2] {
						t.Errorf("%s: %s line %q: owner of %s is %q in dev/owners.yaml", sc.Name, where, line, m[1], want)
					}
				}
			}
		}
	}
}
