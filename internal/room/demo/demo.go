// Package demo provides a deterministic scripted agent, fake topology builder
// and fake clients (fakes.go) so cmd/server --demo works before the dev stack
// is needed. The scripted agent runs the REAL actions from internal/actions
// over the in-memory clients, so the demo exercises the same write path as
// production. It tells the real schema-change story from dev/owners.yaml
// (public.orders → pg-orders-connector → pg.public.orders → order-sync →
// analytics.orders): an amount int → numeric(12,2) change registers an
// incompatible Avro schema and kills the Debezium task; the agent diagnoses it
// and proposes schema_set_compatibility + connect_restart_task.
//
// Controls (controls.go) adds the --demo presenter controls: skip to the next
// step, pause and resume the steps, and reset the fake clients and the story.
package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/topology"
)

var (
	_ agent.Runner           = Run
	_ agent.HandoffWriter    = WriteHandoff
	_ agent.PostmortemWriter = WritePostmortem
)

// Action names the demo proposal references.
const (
	ActionSchemaSetCompatibility = "schema_set_compatibility"
	ActionConnectRestartTask     = "connect_restart_task"
)

const stepDelay = 550 * time.Millisecond

// step is one scripted investigation step of the schema-change story.
type step struct {
	id, title, tool, summary string
	data                     any
}

// story is the schema-change story, in order. It never changes after init.
var story = []step{
	{"s_1", "Check the Debezium connector", "connect_status",
		"pg-orders-connector (source): RUNNING. Tasks: 0=FAILED on 172.19.0.5:8083.\nTask 0 error: org.apache.kafka.connect.errors.DataException: Failed to serialize Avro data from topic pg.public.orders\n  Caused by: ... Schema being registered is incompatible with an earlier schema for subject pg.public.orders-value, details: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type inside a union field at path /fields/0/type/1 in the old schema'}, {oldSchemaVersion: 1}, {validateFields: 'false', compatibility: 'BACKWARD'}]; error code: 409\nRejected schema vs pg.public.orders-value v1: amount: int → bytes(decimal 12,2)",
		map[string]any{"connector": "pg-orders-connector", "state": "RUNNING", "tasks": []any{map[string]any{"id": 0, "state": "FAILED", "worker": "172.19.0.5:8083"}}}},
	{"s_2", "Inspect the Schema Registry subject", "schema_registry_versions",
		"subject pg.public.orders-value: versions [1], latest id 1, compatibility BACKWARD (subject-level)",
		map[string]any{"subject": "pg.public.orders-value", "versions": []int{1}, "latest_id": 1, "compatibility": "BACKWARD", "level": "subject"}},
	{"s_3", "Diff the attempted schema against v1", "schema_registry_diff",
		"amount: int → bytes(decimal 12,2)",
		map[string]any{"subject": "pg.public.orders-value", "from_version": 1, "field_diffs": []any{map[string]any{"field": "amount", "from": "int", "to": "bytes(decimal 12,2)"}}}},
	{"s_4", "Check consumer lag", "kafka_consumer_lag",
		"group order-sync: state Stable, members 1, total lag 0",
		map[string]any{"group": "order-sync", "state": "Stable", "members": 1, "total_lag": 0}},
	{"s_5", "Check the DDL log", "pg_recent_ddl",
		"2026-10-11T09:47:00Z ALTER TABLE public.orders: ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2) by etl",
		map[string]any{"command": "ALTER TABLE", "object": "public.orders", "sql": "ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)", "username": "etl", "at": "2026-10-11T09:47:00Z"}},
}

// Run is a scripted fake agent.Run. It replays the schema-change story with
// realistic delays, then waits for steers and approval results. It has no
// presenter controls; the --demo server runs Controls.Run instead.
func Run(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	return runStory(ctx, newRunState(), nil, deps, stepDelay)
}

// runStory replays the story until the room closes. A reset request restarts
// it from step 1, after the fake clients go back to their starting state.
func runStory(ctx context.Context, rc *runState, fakes *Fakes, deps agent.Deps, delay time.Duration) error {
	if delay <= 0 {
		delay = stepDelay
	}
	emit := func(typ events.Type, payload any) error {
		return deps.Sink.Emit(ctx, typ, payload)
	}
	watcher := &inboxWatcher{in: deps.Inbox}
	for {
		res, err := playStory(ctx, rc, deps, watcher, emit, delay)
		if err != nil {
			return err
		}
		switch res {
		case waitDone:
			return nil
		case waitReset:
			rc.takeReset()
			if fakes != nil {
				fakes.Reset(rc.roomID)
			}
			rc.restartStory()
		}
	}
}

// playStory replays the steps, the hypothesis and the fix proposal, then waits
// for human input. It returns waitReset when a presenter asked to replay.
func playStory(ctx context.Context, rc *runState, deps agent.Deps, watcher *inboxWatcher, emit func(events.Type, any) error, delay time.Duration) (runWait, error) {
	_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "investigating"})

	for i, st := range story {
		if res, _ := rc.boundary(ctx); res != waitReady {
			return res, nil
		}
		rc.beginStep(i + 1)
		_ = emit(events.AgentStepStart, events.AgentStepStartPayload{StepID: st.id, Title: st.title})
		if res := rc.hold(ctx, delay); res != waitReady {
			return res, nil
		}
		_ = emit(events.AgentToolCall, events.AgentToolCallPayload{StepID: st.id, ToolUseID: "tu_" + st.id, Tool: st.tool, Input: mustJSON(map[string]any{"probe": true})})
		if res := rc.hold(ctx, delay); res != waitReady {
			return res, nil
		}
		_ = emit(events.AgentToolResult, events.AgentToolResultPayload{
			StepID: st.id, ToolUseID: "tu_" + st.id, Tool: st.tool,
			Summary: st.summary, Data: mustJSON(st.data), IsError: false, TookMS: 120,
		})
		if res := rc.hold(ctx, delay); res != waitReady {
			return res, nil
		}
	}

	_ = emit(events.AgentHypothesis, events.AgentHypothesisPayload{
		Text:         "A schema change to orders.amount broke the pg-orders-connector on a Schema Registry incompatibility; no new rows reach the warehouse.",
		BasedOnSteps: []string{"s_1", "s_3", "s_5"},
		Confidence:   "high",
		Confirmed:    false,
	})
	// A shadow room has no proposal creator (docs/design/shadow.md 3.7): the
	// run stops at the hypothesis and ends on the closed inbox.
	if deps.Proposals == nil {
		rc.markIdle()
		_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Root cause found: the schema change on orders.amount is incompatible under BACKWARD compatibility for pg.public.orders-value."})
		_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: "no proposals in this room"})
		return waitForInput(ctx, rc, watcher, deps.Inbox, emit)
	}

	_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Root cause found: the schema change on orders.amount is incompatible under BACKWARD compatibility for pg.public.orders-value. I've written a fix proposal."})

	_, err := deps.Proposals.Create(ctx, events.ProposalCreatedPayload{
		Title: "Set subject compatibility to NONE and restart the connector task",
		Steps: []events.ProposalStep{
			{
				Action:      ActionSchemaSetCompatibility,
				Layer:       "schema-registry",
				Params:      mustJSON(map[string]any{"subject": "pg.public.orders-value", "level": "NONE"}),
				Description: "Allow the new decimal schema to register under the same subject.",
			},
			{
				Action:      ActionConnectRestartTask,
				Layer:       "kafka-connect",
				Params:      mustJSON(map[string]any{"connector": "pg-orders-connector", "task": 0}),
				Description: "Restart Debezium task 0 so it picks up the new schema.",
			},
		},
		Risk:           "low",
		Rollback:       "set compatibility back to BACKWARD",
		RequiredLayers: []string{"schema-registry", "kafka-connect"},
	})
	if err != nil {
		// An unconfigured pipeline disables proposals; that is an end state,
		// not a failure to retry (a retry would replay the whole story).
		var re *room.Error
		if errors.As(err, &re) && re.Code == "validation" {
			rc.markIdle()
			_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "I cannot propose a fix in this room: " + re.Msg})
			_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: "proposals disabled"})
			return waitForInput(ctx, rc, watcher, deps.Inbox, emit)
		}
		return waitDone, err
	}
	rc.markIdle()
	_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "The fix proposal is waiting for the schema-registry and kafka-connect owners."})
	_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "waiting_approval", Detail: "1 proposal open"})
	return waitForInput(ctx, rc, watcher, deps.Inbox, emit)
}

// waitForInput answers steers, comments and approval results until the room
// closes. An inbox that is closed (a shadow room) ends it at once with nil.
// The wait rides on the run's single inbox watcher, so a reset (which returns
// waitReset) never leaves a second Wait racing for the same input.
func waitForInput(ctx context.Context, rc *runState, watcher *inboxWatcher, in agent.Inbox, emit func(events.Type, any) error) (runWait, error) {
	if in == nil {
		return waitDone, nil
	}
	rc.markIdle()
	for ctx.Err() == nil {
		switch watcher.wait(ctx, rc) {
		case waitDone:
			return waitDone, nil
		case waitReset:
			return waitReset, nil
		}
		inputs, err := in.Drain(ctx)
		if err != nil {
			return waitDone, nil
		}
		for _, in := range inputs {
			switch in.Kind {
			case "steer":
				_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Got it: " + in.Text})
			case "comment":
				_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Noted from " + in.Actor.Name + ": " + in.Text, ReplyTo: in.CommentID})
			case "approval_result":
				_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "waiting_approval", Detail: in.Text})
			case "action_result":
				_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: in.Text})
			}
		}
	}
	return waitDone, nil
}

// inboxWatcher keeps one Inbox.Wait call in flight across the whole run. The
// demo's idle loop selects on it together with the presenter controls, so a
// reset can be noticed without abandoning a waiter that would race the next
// one for an input.
type inboxWatcher struct {
	in   agent.Inbox
	done chan error // nil while no Wait is in flight
}

// wait blocks until an input is pending (waitReady: the caller drains), the
// room closes (waitDone) or a presenter asks to replay (waitReset).
func (w *inboxWatcher) wait(ctx context.Context, rc *runState) runWait {
	for {
		if rc.resetPending() {
			return waitReset
		}
		if w.done == nil {
			w.done = make(chan error, 1)
			go func(ch chan error) { ch <- w.in.Wait(ctx) }(w.done)
		}
		select {
		case <-ctx.Done():
			return waitDone
		case <-rc.wake:
			// A pause, skip or reset signal: re-check the state.
		case err := <-w.done:
			w.done = nil
			if err != nil {
				return waitDone // room closed or ctx cancelled
			}
			return waitReady
		}
	}
}

// WriteHandoff is a fake hand-off writer returning the fixture summary.
func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts agent.Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{
		WhatBroke: "Debezium task 0 failed after orders.amount was altered to numeric(12,2).",
		Why:       "New schema is incompatible under BACKWARD compatibility for subject pg.public.orders-value.",
		Done:      []string{"Set compatibility to NONE", "Restarted task 0", "Warehouse caught up"},
		RuledOut:  []string{"Kafka brokers", "Consumer group config"},
		Open:      []string{"Consider pinning decimal.handling.mode"},
		Risks:     []string{"BACKWARD compatibility is off until re-enabled"},
	}, nil
}

// WritePostmortem is a fake closing-summary writer returning the fixture
// summary, with no grounding violations.
func WritePostmortem(ctx context.Context, roomEvents []events.Event, opts agent.Options) (agent.PostmortemDraft, error) {
	s, err := WriteHandoff(ctx, roomEvents, opts)
	return agent.PostmortemDraft{Summary: s}, err
}

// Builder is a fake topology.Builder for the orders pipeline.
type Builder struct{}

var _ topology.Builder = Builder{}

// Build returns a static graph matching dev/owners.yaml.
func (Builder) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	return topology.Graph{
		PipelineID: pipelineID,
		Nodes: []topology.Node{
			{ID: "pg:public.orders", Kind: topology.KindSourceTable, Name: "public.orders", Layer: "postgres", Owner: "priya", Health: topology.HealthWarn},
			{ID: "connect:pg-orders-connector", Kind: topology.KindConnector, Name: "pg-orders-connector", Layer: "kafka-connect", Owner: "arjun", Health: topology.HealthFail, Facts: map[string]string{"task_0": "FAILED"}},
			{ID: "topic:pg.public.orders", Kind: topology.KindTopic, Name: "pg.public.orders", Layer: "kafka", Owner: "arjun", Health: topology.HealthWarn},
			{ID: "group:order-sync", Kind: topology.KindConsumerGroup, Name: "order-sync", Layer: "consumer", Owner: "kabir", Health: topology.HealthWarn, Facts: map[string]string{"lag": "0"}},
			{ID: "wh:analytics.orders", Kind: topology.KindSinkTable, Name: "analytics.orders", Layer: "warehouse", Owner: "meera", Health: topology.HealthFail, Facts: map[string]string{"freshness": "stale"}},
		},
		Edges: []topology.Edge{
			{From: "pg:public.orders", To: "connect:pg-orders-connector"},
			{From: "connect:pg-orders-connector", To: "topic:pg.public.orders"},
			{From: "topic:pg.public.orders", To: "group:order-sync"},
			{From: "group:order-sync", To: "wh:analytics.orders"},
		},
	}, nil
}

// Render returns a compact text form for the model prompt.
func (Builder) Render(g topology.Graph) string {
	out := "pipeline " + g.PipelineID + "\n"
	for _, n := range g.Nodes {
		out += "- " + n.ID + " (" + string(n.Kind) + ", " + n.Layer + ", " + string(n.Health) + ")\n"
	}
	return out
}

// Trace returns a deterministic stage list matching the fixture. Like the real
// builder, a pipeline or table outside the orders pipeline is topology.ErrBadInput
// (the room service maps it to 400 validation), so the demo and real modes agree
// on the error taxonomy (review item 7).
func (Builder) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	if pipelineID != "orders" {
		return nil, fmt.Errorf("%w: pipeline %q not found", topology.ErrBadInput, pipelineID)
	}
	if table != "public.orders" {
		return nil, fmt.Errorf("%w: no connector streams table %q in pipeline %q", topology.ErrBadInput, table, pipelineID)
	}
	return []events.TraceStage{
		{Layer: "postgres", Name: "public.orders", Status: "seen", Facts: map[string]string{"row": "id=10042"}},
		{Layer: "kafka-connect", Name: "pg-orders-connector", Status: "seen", Facts: map[string]string{"task": "0"}},
		{Layer: "schema-registry", Name: "pg.public.orders-value", Status: "seen"},
		{Layer: "kafka", Name: "pg.public.orders", Status: "seen", Facts: map[string]string{"partition": "0", "offset": "48213"}},
		{Layer: "consumer", Name: "order-sync", Status: "not_seen"},
		{Layer: "warehouse", Name: "analytics.orders", Status: "stopped_here", Facts: map[string]string{"reason": "no row for id=10042"}},
	}, nil
}

// LayerOwner maps demo layers to demo owners, following dev/owners.yaml
// (schema-registry belongs to priya's source team).
func (Builder) LayerOwner(pipelineID, layer string) (string, bool) {
	m := map[string]string{
		"postgres":        "priya",
		"kafka-connect":   "arjun",
		"schema-registry": "priya",
		"kafka":           "arjun",
		"consumer":        "kabir",
		"warehouse":       "meera",
	}
	o, ok := m[layer]
	return o, ok
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}
