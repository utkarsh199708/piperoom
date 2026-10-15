package evals

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/events"
)

const watcherCause = "pg-orders-connector task 0 FAILED because the source Postgres was restarted: the replication connection was " +
	"terminated by administrator command (57P01). Postgres is back, the slot is reserved and no DDL ran, so restarting task 0 resumes streaming."

func watcherScript(t *testing.T) []step {
	return []step{
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_replication_slots", input: map[string]any{}},
		{tool: "pg_recent_ddl", input: map[string]any{"since": "2026-10-11T09:10:00Z"}},
		{hypothesis: watcherCause},
		{proposeFix: proposeFixJSON(t, "connect_restart_task", restartT0)},
	}
}

// The watcher, not an alert, opens the room: the real watch.Watcher runs on
// the eval clock over the scenario's reads, fires connector_down after its
// default three minutes and calls OpenAlertRoom; the room then starts the
// agent with the watcher's title.
func TestWatcherOpensTheRoom(t *testing.T) {
	sc := loadScenario(t, "scenarios/watcher-start.yaml")
	r, tr := evalOnce(t, sc, watcherScript(t))
	if !r.Pass {
		t.Fatalf("good watcher run fails: %v (error %q)", r.Failures, r.Error)
	}
	if r.Entry != StartWatcher || tr.Room == nil || tr.Room.Entry != StartWatcher {
		t.Fatalf("entry = %q, room = %+v", r.Entry, tr.Room)
	}
	if got := tr.Room.Alert.Title; got != watcherTitle {
		t.Errorf("the agent's alert title is not the watcher's: %q", got)
	}
	if !strings.Contains(tr.Room.Alert.PipelineMap, "connect:pg-orders-connector") {
		t.Errorf("the room did not give the agent the pipeline map: %q", tr.Room.Alert.PipelineMap)
	}
	var created events.RoomCreatedPayload
	for _, ev := range tr.Events {
		if ev.Type == events.RoomCreated {
			if err := json.Unmarshal(ev.Payload, &created); err != nil {
				t.Fatal(err)
			}
		}
	}
	if created.AlertSource != "watcher" || created.DedupKey != "orders|watcher:orders" {
		t.Errorf("room.created = %+v", created)
	}
	if !strings.Contains(string(created.Alert), `"signal":"connector_down"`) {
		t.Errorf("evidence = %s", created.Alert)
	}
}

// A watcher that opens the room on other signals than the scenario names is
// measuring another incident: the run fails with an error.
func TestWatcherWrongSignalIsARunError(t *testing.T) {
	sc := loadScenario(t, "scenarios/watcher-start.yaml")
	sc.Start.Watcher.Signals = []string{"slot_inactive"}
	r, _ := evalOnce(t, sc, watcherScript(t))
	if r.Pass || !strings.Contains(r.Error, "expects [slot_inactive]") {
		t.Errorf("pass=%v error=%q", r.Pass, r.Error)
	}
}

// A watcher that never fires (every connector RUNNING) never opens a room.
func TestWatcherThatNeverFires(t *testing.T) {
	sc := loadScenario(t, "scenarios/watcher-start.yaml")
	sc.Start.Watcher.Connectors[0].Tasks[0].State = "RUNNING"
	sc.Start.Watcher.Slots[0].Active = true
	r, tr := evalOnce(t, sc, watcherScript(t))
	if r.Pass || !strings.Contains(r.Error, "did not open a room") || tr.Room != nil {
		t.Errorf("pass=%v error=%q room=%+v", r.Pass, r.Error, tr.Room)
	}
}

func TestValidateStart(t *testing.T) {
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	base := loadScenario(t, "scenarios/watcher-start.yaml")
	cases := map[string]func(*Scenario){
		"title with watcher":   func(s *Scenario) { s.Alert.Title = "x" },
		"no watcher block":     func(s *Scenario) { s.Start.Watcher = nil },
		"no signals":           func(s *Scenario) { s.Start.Watcher.Signals = nil },
		"unknown signal":       func(s *Scenario) { s.Start.Watcher.Signals = []string{"dlq_growth"} },
		"missing connector":    func(s *Scenario) { s.Start.Watcher.Connectors = nil },
		"unknown start":        func(s *Scenario) { s.Start.By = "pager" },
		"watcher on alert":     func(s *Scenario) { s.Start.By = StartAlert; s.Alert.Title = "x" },
		"bare action result":   func(s *Scenario) { s.Inputs = []Input{{Kind: "action_result", Text: "done"}} },
		"approval result":      func(s *Scenario) { s.Inputs = []Input{{Kind: "approval_result", Text: "ok"}} },
		"fix target param":     func(s *Scenario) { s.Expect.FixTargets["connect_restart_task"] = map[string]any{"connecter": "x"} },
		"fix action no target": func(s *Scenario) { delete(s.Expect.FixTargets, "connect_restart_connector") },
	}
	if err := ValidateAll([]Scenario{base}, defs); err != nil {
		t.Fatalf("the scenario itself: %v", err)
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			sc := loadScenario(t, "scenarios/watcher-start.yaml")
			mut(&sc)
			if err := ValidateAll([]Scenario{sc}, defs); err == nil {
				t.Error("accepted")
			}
		})
	}
}
