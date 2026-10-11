package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// step is one thing the scripted runner does, in order.
type step struct {
	tool       string         // call this read tool...
	input      map[string]any // ...with this input
	hypothesis string         // or record this hypothesis (citing every step so far)
	cite       []string       // explicit based_on_steps instead of "every step so far"
	proposal   *events.ProposalCreatedPayload
	idle       bool // or go idle and wait for input; ends the run when the inbox closes
}

// scriptedRunner is an agent.Runner that replays a fixed script through the
// real Deps, emitting the same events agent.Run does. It lets the harness be
// tested end to end without a model.
func scriptedRunner(script []step) agent.Runner {
	return func(ctx context.Context, alert agent.Alert, d agent.Deps, _ agent.Options) error {
		var stepIDs []string
		for i, s := range script {
			if err := ctx.Err(); err != nil {
				return err
			}
			switch {
			case s.tool != "":
				id := fmt.Sprintf("s_%d", i)
				stepIDs = append(stepIDs, id)
				in, _ := json.Marshal(s.input)
				_ = d.Sink.Emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: id, Title: s.tool})
				_ = d.Sink.Emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{StepID: id, Tool: s.tool, Input: in})
				t, ok := d.Tools.Get(s.tool)
				if !ok {
					return fmt.Errorf("scripted: no tool %s", s.tool)
				}
				res, err := t.Run(ctx, in)
				p := events.AgentToolResultPayload{StepID: id, Tool: s.tool, Summary: res.Summary}
				if err != nil {
					p.Summary, p.IsError = "error: "+err.Error(), true
				}
				_ = d.Sink.Emit(ctx, events.AgentToolResult, p)
			case s.hypothesis != "":
				cite := s.cite
				if cite == nil {
					cite = append([]string(nil), stepIDs...)
				}
				_ = d.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{
					Text: s.hypothesis, BasedOnSteps: cite, Confidence: "high",
				})
			case s.proposal != nil:
				// Like propose_fix: validate every step through the
				// registry and drop the whole proposal on the first
				// failure; only a fully valid one is created.
				valid := true
				for _, st := range s.proposal.Steps {
					a, ok := d.Actions.Get(st.Action)
					if !ok || a.Validate(st.Params) != nil {
						valid = false
						break
					}
				}
				if !valid {
					continue
				}
				if _, err := d.Proposals.Create(ctx, *s.proposal); err != nil {
					return err
				}
			case s.idle:
				_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
				for {
					in, err := d.Inbox.Drain(ctx)
					if err != nil {
						return err
					}
					if len(in) > 0 {
						for _, h := range in {
							_ = d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "ack: " + h.Text})
						}
						break
					}
					if err := d.Inbox.Wait(ctx); err != nil {
						if errors.Is(err, agent.ErrInboxClosed) {
							return nil
						}
						return err
					}
				}
			}
		}
		return nil
	}
}

// goodSchemaChangeScript is a focused, correct investigation of the seed
// scenario.
func goodSchemaChangeScript() []step {
	return []step{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_recent_ddl", input: map[string]any{"since": "2026-10-11T10:00:00Z"}},
		{hypothesis: "Kafka Connect task 0 failed: the Schema Registry rejected (409, BACKWARD) the new Avro schema after amount changed int -> numeric(12,2)."},
		{proposal: &events.ProposalCreatedPayload{
			Title: "Allow the new schema and restart task 0",
			Steps: []events.ProposalStep{
				{Action: "schema_set_compatibility", Params: json.RawMessage(`{"subject":"pg.public.orders-value","level":"NONE"}`)},
				{Action: "connect_restart_task", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)},
			},
		}},
		{idle: true},
	}
}

func loadSeed() Scenario {
	s, err := LoadScenario("scenarios/schema-change.yaml")
	if err != nil {
		panic(err)
	}
	return s
}

var noOpts agent.Options

func depsFor(reg *tools.Registry, areg *actions.Registry, rec *recorder, inbox *scriptedInbox) agent.Deps {
	return agent.Deps{Tools: reg, Actions: areg, Inbox: inbox, Sink: rec, Proposals: rec}
}

func pipelineOrders() owners.Pipeline {
	return owners.Pipeline{ID: "orders", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"}}
}

func timeZero() time.Time { return time.Time{} }
