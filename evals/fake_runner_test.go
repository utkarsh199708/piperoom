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
	confidence string         // the hypothesis's confidence; default high
	message    string         // or post this agent message...
	replyTo    string         // ...as a reply to this comment id
	proposal   *events.ProposalCreatedPayload
	idle       bool // or go idle and wait for input; ends the run when the inbox closes
	// proposeFix is a raw propose_fix tool input, run through the agent's real
	// propose_fix tool (schema check, Validate, Create) instead of the
	// Validate-only shortcut of proposal. A refusal is not a run error.
	proposeFix string
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
				conf := s.confidence
				if conf == "" {
					conf = "high"
				}
				_ = d.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{
					Text: s.hypothesis, BasedOnSteps: cite, Confidence: conf,
				})
			case s.message != "":
				_ = d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: s.message, ReplyTo: s.replyTo})
			case s.proposal != nil:
				// Like propose_fix: validate every step through the
				// registry and drop the whole proposal on the first
				// failure; only a fully valid one is created.
				valid := true
				for _, st := range s.proposal.Steps {
					a, ok := d.Actions.Get(st.Action)
					if !ok || a.Validate(ctx, st.Params) != nil {
						valid = false
						break
					}
				}
				if !valid {
					continue
				}
				// What propose_fix sends: the plan texts and each step's
				// layer. A refusal (the room's scope or plan checks) is
				// the tool's answer to the model, not a run error.
				p := *s.proposal
				if p.Risk == "" {
					p.Risk = "a restart replays the failed batch"
				}
				if p.Rollback == "" {
					p.Rollback = "set the previous value again"
				}
				p.Steps = append([]events.ProposalStep(nil), p.Steps...)
				for i := range p.Steps {
					if a, ok := d.Actions.Get(p.Steps[i].Action); ok && p.Steps[i].Layer == "" {
						p.Steps[i].Layer = a.Layer()
					}
				}
				_, _ = d.Proposals.Create(ctx, p)
			case s.proposeFix != "":
				_, _ = agent.NewProposeFixTool(d.Actions, d.Proposals).Run(ctx, json.RawMessage(s.proposeFix))
			case s.idle:
				_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
				for {
					in, err := d.Inbox.Drain(ctx)
					if err != nil {
						return err
					}
					if len(in) > 0 {
						for _, h := range in {
							// The room's text carries no ids; the structured ProposalID is echoed
							// so tests can still tell which proposal an input is bound to.
							ack := "ack: " + h.Text
							if h.ProposalID != "" {
								ack += " [" + h.ProposalID + "]"
							}
							_ = d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: ack})
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
		{hypothesis: "Kafka Connect task 0 failed: the Schema Registry rejected (409, BACKWARD) the new Avro schema after an ALTER TABLE in Postgres changed amount int -> numeric(12,2)."},
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
	return owners.Pipeline{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, Connectors: []string{"pg-orders-connector"}}
}

func timeZero() time.Time { return time.Time{} }
