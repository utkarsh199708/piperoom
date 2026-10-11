package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/topology"
)

// The demo replays the dev stack's schema-change story with no Claude call and
// no real systems, so the web UI can be built and shown deterministically.

// demoRunner returns a scripted agent.Runner. step is the pause between visible
// steps; use 0 in tests.
func demoRunner(step time.Duration) agent.Runner {
	return func(ctx context.Context, alert agent.Alert, deps agent.Deps, _ agent.Options) error {
		emit := func(typ events.Type, p any) error { return deps.Sink.Emit(ctx, typ, p) }
		pause := func() error {
			if step <= 0 {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(step):
				return nil
			}
		}
		if err := emit(events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "starting"}); err != nil {
			return err
		}
		for i, s := range demoSteps {
			id := fmt.Sprintf("s_%d", i+1)
			if err := emit(events.AgentStepStart, events.AgentStepStartPayload{StepID: id, Title: s.title}); err != nil {
				return err
			}
			use := fmt.Sprintf("tu_%d", i+1)
			if err := emit(events.AgentToolCall, events.AgentToolCallPayload{StepID: id, ToolUseID: use, Tool: s.tool, Input: json.RawMessage(s.input)}); err != nil {
				return err
			}
			if err := pause(); err != nil {
				return err
			}
			if err := emit(events.AgentToolResult, events.AgentToolResultPayload{
				StepID: id, ToolUseID: use, Tool: s.tool, Summary: s.summary, Data: json.RawMessage(s.data), TookMS: 120 + int64(i)*35,
			}); err != nil {
				return err
			}
			if err := pause(); err != nil {
				return err
			}
		}
		if err := emit(events.AgentHypothesis, events.AgentHypothesisPayload{
			Text:         "The ALTER on public.orders changed amount to numeric(12,2). Debezium's new Avro schema is not BACKWARD compatible with v1, so Schema Registry rejected it (409) and task 0 of pg-orders-connector failed. The consumer is healthy; the warehouse is stale because nothing new reaches Kafka.",
			BasedOnSteps: []string{"s_1", "s_3", "s_4", "s_5"},
			Confidence:   "high",
		}); err != nil {
			return err
		}
		_, err := deps.Proposals.Create(ctx, events.ProposalCreatedPayload{
			Title: "Relax compatibility, restart the connector, backfill the warehouse",
			Steps: []events.ProposalStep{
				{Action: "schema_set_compatibility", Params: json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`),
					Description: "Allow the new amount schema to register (temporary; restore BACKWARD afterwards)."},
				{Action: "warehouse_backfill", Params: json.RawMessage(`{"table":"analytics.orders","since":"2026-10-11T10:40:00Z"}`),
					Description: "Reload warehouse rows from after the ALTER once the connector is flowing again."},
			},
			Risk:     "Relaxing compatibility lets any producer register an incompatible schema until it is restored.",
			Rollback: "Set the subject compatibility back to BACKWARD; the backfill is idempotent.",
		})
		if err != nil {
			_ = emit(events.AgentMessage, events.AgentMessagePayload{Text: "I could not create the proposal: " + err.Error()})
		}
		if err := emit(events.AgentStatus, events.AgentStatusPayload{State: "waiting_approval", Detail: "proposal needs schema-registry and warehouse owners"}); err != nil {
			return err
		}
		return demoListen(ctx, deps)
	}
}

// demoListen answers humans until the room ends.
func demoListen(ctx context.Context, deps agent.Deps) error {
	for {
		if err := deps.Inbox.Wait(ctx); err != nil {
			return nil //nolint:nilerr // ctx ended: the room closed or the server is stopping
		}
		ins, _ := deps.Inbox.Drain(ctx)
		for _, in := range ins {
			var text, state string
			switch in.Kind {
			case "action_result":
				text, state = "Noted: "+in.Text, "idle"
			case "approval_result":
				text, state = "Noted: "+in.Text, "waiting_approval"
			case "steer":
				text, state = fmt.Sprintf("Understood, %s. In the demo I only replay the scripted investigation.", in.Actor.Name), "idle"
			default:
				text, state = fmt.Sprintf("Thanks %s. In the demo I do not read comments, but a real agent would answer here.", in.Actor.Name), "idle"
			}
			if err := deps.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: text, ReplyTo: in.CommentID}); err != nil {
				return nil
			}
			if err := deps.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: state}); err != nil {
				return nil
			}
		}
	}
}

type demoStep struct{ title, tool, input, summary, data string }

var demoSteps = []demoStep{
	{"Check the connector", "connect_status", `{"connector":"pg-orders-connector"}`,
		"pg-orders-connector: RUNNING, task 0 FAILED (SerializationException: schema registration rejected, HTTP 409)", `{"state":"RUNNING","tasks":[{"id":0,"state":"FAILED"}]}`},
	{"Check the consumer", "kafka_consumer_lag", `{"group":"order-sync"}`,
		"order-sync: Stable, 1 member, total lag 0 - the consumer is healthy", `{"total_lag":0}`},
	{"Check the replication slot", "pg_replication_slots", `{}`,
		"debezium_orders: active=false, 213 kB retained (growing)", `{"slots":[{"name":"debezium_orders","active":false,"retained_bytes":218112}]}`},
	{"Look for recent schema changes", "pg_recent_ddl", `{"since":"1h"}`,
		"1 DDL: ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)", `{"ddl":[{"tag":"ALTER TABLE","object":"public.orders"}]}`},
	{"Compare registry schema versions", "schema_registry_diff", `{"subject":"pg.public.orders-value"}`,
		"v1 amount=int; candidate amount=bytes(decimal 12,2); compatibility BACKWARD -> incompatible", `{"compatibility":"BACKWARD","compatible":false}`},
	{"Check warehouse freshness", "warehouse_freshness", `{"pipeline_id":"orders"}`,
		"analytics.orders: newest row is from before the ALTER (stale)", `{"stale":true}`},
}

// ---- demo actions: scoped like the real ones, but they change nothing ----

type demoAction struct {
	name, layer, desc string
	schema            map[string]any
	validate          func(json.RawMessage) error
	scope             func(owners.Pipeline, json.RawMessage) error
	out               string
}

func (a *demoAction) Name() string                     { return a.name }
func (a *demoAction) Layer() string                    { return a.layer }
func (a *demoAction) Describe() string                 { return a.desc }
func (a *demoAction) ParamsSchema() map[string]any     { return a.schema }
func (a *demoAction) Validate(p json.RawMessage) error { return a.validate(p) }
func (a *demoAction) CheckScope(p owners.Pipeline, raw json.RawMessage) error {
	return a.scope(p, raw)
}
func (a *demoAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(400 * time.Millisecond):
		return a.out + " (demo: nothing was changed)", nil
	}
}

func demoActionRegistry() (*actions.Registry, error) {
	reg := actions.NewRegistry()
	str := func(raw json.RawMessage, keys ...string) (map[string]string, error) {
		var m map[string]string
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("params must be a JSON object of strings: %w", err)
		}
		for _, k := range keys {
			if m[k] == "" {
				return nil, fmt.Errorf("%s is required", k)
			}
		}
		return m, nil
	}
	list := []*demoAction{
		{
			name: "schema_set_compatibility", layer: "schema-registry", desc: "Set a subject's compatibility level.",
			schema: map[string]any{"type": "object"},
			validate: func(raw json.RawMessage) error {
				m, err := str(raw, "subject", "level")
				if err == nil && !slices.Contains([]string{"NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FULL"}, m["level"]) {
					err = fmt.Errorf("unknown level %q", m["level"])
				}
				return err
			},
			scope: func(p owners.Pipeline, raw json.RawMessage) error {
				m, _ := str(raw)
				if !strings.HasPrefix(m["subject"], p.TopicPrefix+".") {
					return fmt.Errorf("subject %q is not in pipeline %q", m["subject"], p.ID)
				}
				return nil
			},
			out: "compatibility updated",
		},
		{
			name: "warehouse_backfill", layer: "warehouse", desc: "Reload warehouse rows since a time.",
			schema: map[string]any{"type": "object"},
			validate: func(raw json.RawMessage) error {
				_, err := str(raw, "table", "since")
				return err
			},
			scope: func(p owners.Pipeline, raw json.RawMessage) error {
				m, _ := str(raw)
				if !slices.Contains(p.SinkTables, m["table"]) {
					return fmt.Errorf("table %q is not a sink table of pipeline %q", m["table"], p.ID)
				}
				return nil
			},
			out: "backfill finished",
		},
	}
	for _, a := range list {
		if err := reg.Register(a); err != nil {
			return nil, err
		}
	}
	return reg, nil
}

// ---- demo topology ----

type demoTopology struct{ o *owners.Owners }

func (d demoTopology) Build(_ context.Context, pipelineID string) (topology.Graph, error) {
	p, ok := d.o.Pipeline(pipelineID)
	if !ok {
		return topology.Graph{}, fmt.Errorf("unknown pipeline %q", pipelineID)
	}
	owner := func(layer string) string { u, _ := d.o.LayerOwner(layer); return u }
	g := topology.Graph{PipelineID: pipelineID, Edges: []topology.Edge{}}
	var prev string
	add := func(n topology.Node) {
		g.Nodes = append(g.Nodes, n)
		if prev != "" {
			g.Edges = append(g.Edges, topology.Edge{From: prev, To: n.ID})
		}
		prev = n.ID
	}
	for _, t := range p.SourceTables {
		add(topology.Node{ID: "pg:" + t, Kind: topology.KindSourceTable, Name: t, Layer: "postgres", Owner: owner("postgres"),
			Health: topology.HealthWarn, Facts: map[string]string{"slot_retained": "213 kB"}})
	}
	for _, c := range p.Connectors {
		add(topology.Node{ID: "connect:" + c, Kind: topology.KindConnector, Name: c, Layer: "kafka-connect", Owner: owner("kafka-connect"),
			Health: topology.HealthFail, Facts: map[string]string{"task_0": "FAILED"}})
	}
	for _, t := range p.SourceTables {
		topic := p.TopicPrefix + "." + t
		add(topology.Node{ID: "topic:" + topic, Kind: topology.KindTopic, Name: topic, Layer: "kafka", Owner: owner("kafka"), Health: topology.HealthOK})
	}
	for _, c := range p.ConsumerGroups {
		add(topology.Node{ID: "group:" + c, Kind: topology.KindConsumerGroup, Name: c, Layer: "consumer", Owner: owner("consumer"),
			Health: topology.HealthOK, Facts: map[string]string{"lag": "0"}})
	}
	for _, t := range p.SinkTables {
		add(topology.Node{ID: "wh:" + t, Kind: topology.KindSinkTable, Name: t, Layer: "warehouse", Owner: owner("warehouse"),
			Health: topology.HealthWarn, Facts: map[string]string{"freshness": "stale"}})
	}
	return g, nil
}

func (d demoTopology) Render(g topology.Graph) string {
	var parts []string
	for _, n := range g.Nodes {
		parts = append(parts, fmt.Sprintf("%s [%s]", n.ID, n.Health))
	}
	return strings.Join(parts, " -> ")
}

func (demoTopology) Trace(_ context.Context, _, table, key string) ([]events.TraceStage, error) {
	return []events.TraceStage{
		{Layer: "postgres", Name: table, Status: "seen", Facts: map[string]string{"key": key}},
		{Layer: "kafka-connect", Name: "pg-orders-connector", Status: "stopped_here", Facts: map[string]string{"task_0": "FAILED"}},
		{Layer: "kafka", Name: "pg.public.orders", Status: "not_seen"},
		{Layer: "warehouse", Name: "analytics.orders", Status: "not_seen"},
	}, nil
}

func (d demoTopology) LayerOwner(_ string, layer string) (string, bool) { return d.o.LayerOwner(layer) }

// demoHandoff writes a canned hand-off summary from the log it is given.
func demoHandoff(_ context.Context, evs []events.Event, _ agent.Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{
		WhatBroke: "Warehouse analytics.orders went stale after an ALTER on public.orders.",
		Why:       "The new Avro schema is not BACKWARD compatible; the connector task failed on registration.",
		Done:      []string{fmt.Sprintf("Investigated (%d room events so far)", len(evs)), "Pinned the cause: schema-registry compatibility"},
		RuledOut:  []string{"Consumer lag (order-sync is healthy)"},
		Open:      []string{"Approve the proposal (schema-registry and warehouse owners)", "Restore BACKWARD compatibility afterwards"},
		Risks:     []string{"Relaxed compatibility lets incompatible schemas register until it is restored"},
	}, nil
}
