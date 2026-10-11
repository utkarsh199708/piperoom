// Package demo provides a deterministic scripted agent, fake topology builder
// and fake actions so cmd/server --demo works before A2/A3/A4 land. It tells
// the mockup story: a schema change broke a Connect task and the warehouse is
// stale; the agent investigates, forms a hypothesis and proposes a fix that
// needs schema-registry + warehouse approvals.
package demo

import (
	"context"
	"encoding/json"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/topology"
)

var (
	_ agent.Runner        = Run
	_ agent.HandoffWriter = WriteHandoff
)

// Action names the demo proposal references.
const (
	ActionSchemaRegister   = "schema_registry_register_compat"
	ActionWarehouseRefresh = "warehouse_refresh"
)

const stepDelay = 550 * time.Millisecond

// Run is a scripted fake agent.Run. It replays the mockup story with realistic
// delays, then waits for steers and approval results.
func Run(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	emit := func(typ events.Type, payload any) error {
		return deps.Sink.Emit(ctx, typ, payload)
	}
	pause := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(stepDelay):
			return true
		}
	}

	_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "investigating"})

	type step struct {
		id, title, tool, summary string
		data                     any
	}
	steps := []step{
		{"s_1", "Check Connect task state", "connect_status", "connector pg-orders-connector: task 1 FAILED", map[string]any{"connector": "pg-orders-connector", "task": 1, "state": "FAILED"}},
		{"s_2", "Read the failed task trace", "connect_task_trace", "DataException converting field customer_id", map[string]any{"error": "org.apache.kafka.connect.errors.DataException: Converting field 'customer_id'"}},
		{"s_3", "Compare schemas in the registry", "schema_versions", "orders-value v1 int, v2 bigint (incompatible)", map[string]any{"subject": "orders-value", "versions": []int{1, 2}}},
		{"s_4", "Check source replication slot", "replication_slots", "slot debezium retained 12.4 GB WAL", map[string]any{"slot": "debezium", "retained_wal_bytes": 13314398617}},
		{"s_5", "Trace a stale record", "record_trace", "record reaches the topic but not the warehouse", map[string]any{"stage": "stopped at consumer"}},
		{"s_6", "Check warehouse freshness", "warehouse_freshness", "warehouse.orders is 23 minutes stale", map[string]any{"max_updated_at": "2026-10-11T10:42:00Z", "stale": "23m"}},
	}

	for _, st := range steps {
		if !pause() {
			return nil
		}
		_ = emit(events.AgentStepStart, events.AgentStepStartPayload{StepID: st.id, Title: st.title})
		if !pause() {
			return nil
		}
		_ = emit(events.AgentToolCall, events.AgentToolCallPayload{StepID: st.id, ToolUseID: "tu_" + st.id, Tool: st.tool, Input: mustJSON(map[string]any{"probe": true})})
		if !pause() {
			return nil
		}
		_ = emit(events.AgentToolResult, events.AgentToolResultPayload{
			StepID: st.id, ToolUseID: "tu_" + st.id, Tool: st.tool,
			Summary: st.summary, Data: mustJSON(st.data), IsError: false, TookMS: 120,
		})
	}

	_ = emit(events.AgentHypothesis, events.AgentHypothesisPayload{
		Text:         "A backward-incompatible schema change (customer_id int -> bigint) broke the connector task, so rows stopped reaching the warehouse.",
		BasedOnSteps: []string{"s_1", "s_2", "s_3", "s_4", "s_5", "s_6"},
		Confidence:   "high",
		Confirmed:    false,
	})
	_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Root cause found: an incompatible schema change on orders killed the Connect task. I've written a fix proposal."})

	proposalID, err := deps.Proposals.Create(ctx, events.ProposalCreatedPayload{
		Title: "Re-register compatible schema, then refresh the warehouse",
		Steps: []events.ProposalStep{
			{
				Action:      ActionSchemaRegister,
				Layer:       "schema-registry",
				Params:      mustJSON(map[string]any{"subject": "orders-value", "compatibility": "backward"}),
				Description: "Register the corrected Avro schema for orders-value",
			},
			{
				Action:      ActionWarehouseRefresh,
				Layer:       "warehouse",
				Params:      mustJSON(map[string]any{"table": "warehouse.orders"}),
				Description: "Backfill warehouse.orders from the source",
			},
		},
		Risk:           "Brief warehouse read gap during backfill",
		Rollback:       "Re-register the previous schema version",
		RequiredLayers: []string{"schema-registry", "warehouse"},
	})
	if err != nil {
		return err
	}
	_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "Proposal " + proposalID + " is waiting for schema-registry and warehouse owners."})
	_ = emit(events.AgentStatus, events.AgentStatusPayload{State: "waiting_approval", Detail: "proposal " + proposalID})

	for ctx.Err() == nil {
		if err := deps.Inbox.Wait(ctx); err != nil {
			return nil // room closed or ctx cancelled
		}
		inputs, err := deps.Inbox.Drain(ctx)
		if err != nil {
			return nil
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
	return nil
}

// WriteHandoff is a fake hand-off writer returning a fixed summary.
func WriteHandoff(ctx context.Context, roomEvents []events.Event, opts agent.Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{
		WhatBroke: "A backward-incompatible schema change broke the Kafka Connect task",
		Why:       "customer_id changed int -> bigint without a compatible schema registration",
		Done:      []string{"Located the failing task", "Confirmed the schema mismatch"},
		RuledOut:  []string{"Kafka broker outage", "WAL slot loss"},
		Open:      []string{"Warehouse backfill"},
		Risks:     []string{"Warehouse read gap during backfill"},
	}, nil
}

// Actions returns a registry with the two demo actions the proposal needs.
func Actions() *actions.Registry {
	r := actions.NewRegistry()
	_ = r.Register(demoAction{name: ActionSchemaRegister, layer: "schema-registry", desc: "Register the corrected Avro schema", result: "registered compatible schema for orders-value (v2)"})
	_ = r.Register(demoAction{name: ActionWarehouseRefresh, layer: "warehouse", desc: "Backfill the warehouse table", result: "refreshed warehouse.orders (backfill complete)"})
	return r
}

type demoAction struct {
	name, layer, desc, result string
}

func (a demoAction) Name() string     { return a.name }
func (a demoAction) Layer() string    { return a.layer }
func (a demoAction) Describe() string { return a.desc }
func (a demoAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}}
}
func (a demoAction) Validate(params json.RawMessage) error { return nil }
func (a demoAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}
	return a.result, nil
}

// Builder is a fake topology.Builder for the orders pipeline.
type Builder struct{}

var _ topology.Builder = Builder{}

// Build returns a static graph matching the demo story.
func (Builder) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	return topology.Graph{
		PipelineID: pipelineID,
		Nodes: []topology.Node{
			{ID: "pg:public.orders", Kind: topology.KindSourceTable, Name: "public.orders", Layer: "postgres", Owner: "priya", Health: topology.HealthWarn, Facts: map[string]string{"slot_retained": "12.4 GB"}},
			{ID: "connect:pg-orders-connector", Kind: topology.KindConnector, Name: "pg-orders-connector", Layer: "kafka-connect", Owner: "arjun", Health: topology.HealthFail, Facts: map[string]string{"task_0": "FAILED"}},
			{ID: "topic:orders", Kind: topology.KindTopic, Name: "orders", Layer: "kafka", Owner: "arjun", Health: topology.HealthWarn, Facts: map[string]string{"lag": "0"}},
			{ID: "group:warehouse-sink", Kind: topology.KindConsumerGroup, Name: "warehouse-sink", Layer: "consumer", Owner: "kabir", Health: topology.HealthWarn, Facts: map[string]string{"lag": "23m"}},
			{ID: "wh:warehouse.orders", Kind: topology.KindSinkTable, Name: "warehouse.orders", Layer: "warehouse", Owner: "meera", Health: topology.HealthFail, Facts: map[string]string{"freshness": "23m stale"}},
		},
		Edges: []topology.Edge{
			{From: "pg:public.orders", To: "connect:pg-orders-connector"},
			{From: "connect:pg-orders-connector", To: "topic:orders"},
			{From: "topic:orders", To: "group:warehouse-sink"},
			{From: "group:warehouse-sink", To: "wh:warehouse.orders"},
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

// Trace returns a deterministic stage list.
func (Builder) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	return []events.TraceStage{
		{Layer: "postgres", Name: table, Status: "seen"},
		{Layer: "kafka-connect", Name: "pg-orders-connector", Status: "stopped_here", Facts: map[string]string{"reason": "task FAILED"}},
		{Layer: "warehouse", Name: "warehouse." + table, Status: "not_seen"},
	}, nil
}

// LayerOwner maps demo layers to demo owners.
func (Builder) LayerOwner(pipelineID, layer string) (string, bool) {
	m := map[string]string{
		"postgres":        "priya",
		"kafka-connect":   "arjun",
		"schema-registry": "arjun",
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
