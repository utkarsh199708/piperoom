package evals

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestEvaluateGoodRunPasses(t *testing.T) {
	sc := loadSeed()
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Label: "test", Runs: 2, Parallel: 2, Runner: scriptedRunner(goodSchemaChangeScript()), Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(rep.Results) != 2 {
		t.Fatalf("results = %d, want 2", len(rep.Results))
	}
	for _, r := range rep.Results {
		if !r.Pass {
			t.Errorf("run %d failed: %v (%+v)", r.Run, r.Failures, r)
		}
		if r.Score != 100 {
			t.Errorf("run %d score = %v, want 100", r.Run, r.Score)
		}
		if r.ToolCalls != 3 || r.ToolCallsToRootCause != 3 || r.Proposals != 1 || len(r.Misses) != 0 ||
			len(r.Attempts) != 2 || r.RejectedAttempts != 0 {
			t.Errorf("run %d counters wrong: %+v", r.Run, r)
		}
	}
	if rep.Results[0].Run != 1 || rep.Results[1].Run != 2 {
		t.Errorf("results not in run order: %d, %d", rep.Results[0].Run, rep.Results[1].Run)
	}
	if got := rep.Overall.PassRate; got != 1 {
		t.Errorf("overall pass rate = %v, want 1", got)
	}
}

func TestEvaluateReplaysRecordedResults(t *testing.T) {
	sc := loadSeed()
	sc.Recordings = append(sc.Recordings,
		Recording{Tool: "connect_status", Error: "connect: GET /connectors/other/status: not found"},
		Recording{Tool: "pg_row", Summary: "found: yes"})
	var tr Transcript
	_, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner: scriptedRunner([]step{
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			{tool: "connect_status", input: map[string]any{"connector": "other"}},
			{tool: "pg_row", input: map[string]any{"table": "public.orders", "pk_column": "id", "key": "1"}},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
		}),
		Logger: quietLogger(),
		OnRun:  func(_ RunResult, t Transcript) { tr = t },
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	var results []events.AgentToolResultPayload
	for _, ev := range tr.Events {
		if ev.Type == events.AgentToolResult {
			var p events.AgentToolResultPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatal(err)
			}
			results = append(results, p)
		}
	}
	if len(results) != 4 {
		t.Fatalf("tool results = %d, want 4", len(results))
	}
	if !strings.Contains(results[0].Summary, "error code: 409") || results[0].IsError {
		t.Errorf("exact match not replayed: %+v", results[0])
	}
	if !results[1].IsError || !strings.Contains(results[1].Summary, "not found") {
		t.Errorf("wildcard error recording not used: %+v", results[1])
	}
	if results[2].IsError {
		t.Errorf("wildcard summary recording not used: %+v", results[2])
	}
	if !strings.Contains(results[3].Summary, "total lag 0") {
		t.Errorf("lag not replayed: %+v", results[3])
	}
}

func TestEvaluateRecordsMisses(t *testing.T) {
	sc := loadSeed()
	sc.Recordings = sc.Recordings[:1] // only pipeline_map
	// Facts need their tools' recordings; validation refuses facts no run
	// could establish.
	sc.Expect.ExpectedFacts, sc.Expect.Claims = nil, nil
	// Fix targets need a required fact to ground the fix on.
	sc.Expect.FixTargets = nil
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner: scriptedRunner(goodSchemaChangeScript()), Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	r := rep.Results[0]
	if len(r.Misses) != 2 || r.Misses[0].Tool != "connect_status" || r.Misses[1].Tool != "pg_recent_ddl" {
		t.Fatalf("misses = %+v", r.Misses)
	}
	if r.ToolErrors != 2 || r.ToolCalls != 3 || r.ToolCallsOK != 1 {
		t.Errorf("tool calls attempted/ok/errors = %d/%d/%d, want 3/1/2", r.ToolCalls, r.ToolCallsOK, r.ToolErrors)
	}
	if rep.Overall.Misses != 2 {
		t.Errorf("overall misses = %d", rep.Overall.Misses)
	}
}

func TestEvaluateDeliversInputsOnIdle(t *testing.T) {
	sc := loadSeed()
	sc.Inputs = []Input{{Kind: "steer", Text: "check the registry"}, {Kind: "comment", Text: "why?", CommentID: "c_1"}}
	script := append(goodSchemaChangeScript(), step{idle: true}, step{idle: true})
	var tr Transcript
	_, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner: scriptedRunner(script), Logger: quietLogger(),
		OnRun: func(_ RunResult, t Transcript) { tr = t },
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	var acks []string
	for _, ev := range tr.Events {
		if ev.Type == events.AgentMessage {
			var p events.AgentMessagePayload
			_ = json.Unmarshal(ev.Payload, &p)
			acks = append(acks, p.Text)
		}
	}
	// A room delivers a comment only when it mentions the agent; the harness
	// adds the mention a scenario comment implies.
	if strings.Join(acks, "|") != "ack: check the registry|ack: @agent why?" {
		t.Errorf("acks = %q", acks)
	}
	// The harness logs each delivered steer and comment as the room would.
	var logged []string
	for _, ev := range tr.Events {
		switch ev.Type {
		case events.SteerSent:
			var p events.SteerSentPayload
			_ = json.Unmarshal(ev.Payload, &p)
			logged = append(logged, "steer:"+p.Text)
		case events.CommentAdded:
			var p events.CommentAddedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			// The room assigns comment ids; the scenario's is not used.
			logged = append(logged, "comment:"+p.Text)
		}
	}
	if strings.Join(logged, "|") != "steer:check the registry|comment:@agent why?" {
		t.Errorf("logged inputs = %q", logged)
	}
	if tr.Idles != 3 {
		t.Errorf("idles = %d, want 3 (two inputs, then closed)", tr.Idles)
	}
}

func TestEvaluateRunnerErrorIsRecorded(t *testing.T) {
	boom := func(context.Context, agent.Alert, agent.Deps, agent.Options) error {
		return errors.New("model request failed")
	}
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, Config{Runner: boom, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	r := rep.Results[0]
	if r.Pass || r.Error != "model request failed" || rep.Overall.Errors != 1 {
		t.Errorf("runner error not recorded: %+v", r)
	}
}

func TestEvaluateTimeout(t *testing.T) {
	hang := func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		<-ctx.Done()
		return ctx.Err()
	}
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, Config{
		Runner: hang, Timeout: 20 * time.Millisecond, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	r := rep.Results[0]
	if !r.TimedOut || r.Error != "" || r.Pass {
		t.Errorf("timeout not recorded once: %+v", r)
	}
}

func TestEvaluateStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runs := 0
	stopAfterOne := func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		runs++
		cancel()
		<-ctx.Done()
		return nil
	}
	rep, err := Evaluate(ctx, []Scenario{loadSeed()}, Config{Runs: 5, Runner: stopAfterOne, Logger: quietLogger()})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if runs != 1 || len(rep.Results) != 0 {
		t.Errorf("runs = %d, results = %d; want 1 started, 0 judged", runs, len(rep.Results))
	}
}

func TestEvaluateRejectsInvalidScenarioBeforeRunning(t *testing.T) {
	sc := loadSeed()
	sc.Recordings = append(sc.Recordings, Recording{Tool: "no_such_tool", Summary: "x"})
	called := false
	runner := func(context.Context, agent.Alert, agent.Deps, agent.Options) error { called = true; return nil }
	_, err := Evaluate(context.Background(), []Scenario{sc}, Config{Runner: runner, Logger: quietLogger()})
	if err == nil || !strings.Contains(err.Error(), "no_such_tool") {
		t.Fatalf("err = %v, want unknown tool error", err)
	}
	if called {
		t.Error("runner called for an invalid scenario")
	}
}

func TestEvaluatePassesAlertAndOptions(t *testing.T) {
	var gotAlert agent.Alert
	var gotOpts agent.Options
	runner := func(_ context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		gotAlert, gotOpts = a, o
		if d.Actions == nil || len(d.Actions.All()) == 0 {
			t.Error("no actions given to the agent")
		}
		return nil
	}
	sc := loadSeed()
	_, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner: runner, Options: agent.Options{Model: "m", Effort: "low", MaxTurns: 7}, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotAlert.PipelineID != "orders" || gotAlert.Title != sc.Alert.Title || gotAlert.PipelineMap == "" || gotAlert.RoomID == "" {
		t.Errorf("alert = %+v", gotAlert)
	}
	if gotOpts != (agent.Options{Model: "m", Effort: "low", MaxTurns: 7}) {
		t.Errorf("options = %+v", gotOpts)
	}
}

// Evaluate's own validation refuses two scenarios of one name: they would
// share a report row and count every run twice.
func TestValidateAllRefusesDuplicateNames(t *testing.T) {
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	sc := loadSeed()
	if err := ValidateAll([]Scenario{sc}, defs); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err = ValidateAll([]Scenario{sc, sc}, defs)
	if err == nil || !strings.Contains(err.Error(), `duplicate scenario name "schema-change"`) {
		t.Fatalf("err = %v", err)
	}
}
