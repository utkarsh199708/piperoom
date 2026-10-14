package evals

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

const roomOwnersYAML = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    shadow: {enabled: true}
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

const roomUsersYAML = `users:
  - {id: arjun, name: Arjun, layers: [kafka-connect, kafka]}
  - {id: priya, name: Priya, layers: [postgres, schema-registry]}
`

// TestScenarioFromRealShadowView drives a real shadow room (room.Service over
// memstore, a fake agent), takes the body room.Service.Shadow returns for
// include=room, marshals it the way the HTTP handler does, and builds a
// scenario from it. The input is the server's own type, so the format the
// builder reads cannot drift from what the server writes.
func TestScenarioFromRealShadowView(t *testing.T) {
	dir := t.TempDir()
	op, up := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
	if err := os.WriteFile(op, []byte(roomOwnersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(up, []byte(roomUsersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(op)
	if err != nil {
		t.Fatal(err)
	}
	users, err := room.LoadUsers(up)
	if err != nil {
		t.Fatal(err)
	}

	const connectSummary = "pg-orders-connector (source): RUNNING.\nTask 0: FAILED: schema registry 409"
	runner := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		emit := func(typ events.Type, p any) {
			if err := d.Sink.Emit(ctx, typ, p); err != nil {
				t.Errorf("emit %s: %v", typ, err)
			}
		}
		emit(events.AgentStepStart, events.AgentStepStartPayload{StepID: "s_1", Title: "connector"})
		emit(events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", ToolUseID: "tu_1", Tool: "connect_status",
			Input: json.RawMessage(`{"connector":"pg-orders-connector","why":"first hop"}`)})
		emit(events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", ToolUseID: "tu_1", Tool: "connect_status",
			Summary: connectSummary, Data: json.RawMessage(`{"state":"RUNNING","tasks":[{"id":0,"state":"FAILED"}]}`)})
		emit(events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", ToolUseID: "tu_2", Tool: "kafka_list_groups",
			Input: json.RawMessage(`{"why":"x"}`)})
		emit(events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", ToolUseID: "tu_2", Tool: "kafka_list_groups",
			Summary: "error: kafka: connection refused", IsError: true})
		emit(events.AgentHypothesis, events.AgentHypothesisPayload{Text: "Connect task 0 failed: schema registry rejected amount (409).",
			BasedOnSteps: []string{"s_1"}, Confidence: "high"})
		emit(events.AgentStatus, events.AgentStatusPayload{State: "idle"})
		// A shadow room's inbox is closed, so this ends the run.
		if err := d.Inbox.Wait(ctx); err != nil && !errors.Is(err, agent.ErrInboxClosed) {
			return err
		}
		return nil
	}
	ms := memstore.New()
	svc := room.New(room.Deps{
		Store: ms, Users: users, Owners: own, Run: runner,
		WriteHandoff: func(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, nil
		},
		Options:       agent.Options{Model: "live", MaxTurns: 40},
		ShadowOptions: agent.Options{Model: "shadow", MaxTurns: 20, TokenBudget: 500000},
	})
	t.Cleanup(svc.Close)
	ctx := context.Background()

	snap, created, err := svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "orders stale", Severity: "sev2",
		PipelineID: "orders", DedupKey: "generic:evals",
	})
	if err != nil || !created {
		t.Fatalf("OpenAlertRoom: created=%v err=%v", created, err)
	}
	arjun, _ := users.Get("arjun")
	priya, _ := users.Get("priya")
	if _, err := svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	// Wait on the run's persisted finished timestamp; room creation alone does
	// not mean the asynchronous shadow investigation has completed.
	retry(t, func() error {
		_, err := svc.GetRoom(ctx, snap.ID)
		if err != nil {
			return err
		}
		run, err := ms.GetShadowRun(ctx, snap.ID)
		if err != nil {
			return err
		}
		if !run.Finished() {
			return errors.New("shadow run has not finished")
		}
		return nil
	})
	if _, err := svc.CloseRoom(ctx, arjun, snap.ID, "schema change; set compatibility NONE"); err != nil {
		t.Fatalf("CloseRoom: %v", err)
	}
	if _, err := svc.ShadowLabel(ctx, priya, snap.ID, "kafka-connect", []string{"schema registry", "409"}); err != nil {
		t.Fatalf("label: %v", err)
	}
	retry(t, func() error {
		_, err := svc.ShadowGrade(ctx, priya, snap.ID, true, "yes", "")
		return err
	})
	view, err := svc.Shadow(ctx, snap.ID, room.ShadowReadOptions{Room: true})
	if err != nil {
		t.Fatalf("Shadow: %v", err)
	}
	body, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}

	lookup := func(id string) (owners.Pipeline, bool) { return own.Pipeline(id) }
	res, err := ScenarioFromShadow(body, lookup, "")
	if err != nil {
		t.Fatalf("ScenarioFromShadow on the server's own output: %v\n%s", err, body)
	}
	sc := res.Scenario
	if len(sc.Recordings) != 2 {
		t.Fatalf("%d recordings, want 2: %+v", len(sc.Recordings), sc.Recordings)
	}
	cs := sc.Recordings[0]
	if cs.Tool != "connect_status" || cs.Summary != connectSummary || len(cs.Input) != 1 || cs.Data != nil {
		t.Errorf("connect_status recording = %+v", cs)
	}
	if e := sc.Recordings[1]; e.Tool != "kafka_list_groups" || e.Error != "kafka: connection refused" {
		t.Errorf("error recording = %+v", e)
	}
	if sc.Alert.Title != "orders stale" || sc.Expect.FirstFailingLayer != "kafka-connect" ||
		len(sc.Expect.RootCause) != 1 || strings.Join(sc.Expect.RootCause[0], ",") != "schema registry,409" {
		t.Errorf("scenario = %+v", sc)
	}
	if !strings.Contains(sc.Description, "layer_correct=true") || strings.Contains(sc.Description, "Connect task 0 failed") {
		t.Errorf("description = %q", sc.Description)
	}
}

func retry(t *testing.T, f func() error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not ready within 10s: %v", err)
}
