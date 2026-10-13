package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// logBuilder assembles a realistic room log with gap-free seqs.
type logBuilder struct {
	evs  []events.Event
	step int
}

func (b *logBuilder) add(t events.Type, actor events.Actor, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err) // test helper: payloads are static structs
	}
	b.evs = append(b.evs, events.Event{
		RoomID: "r_test", Seq: int64(len(b.evs) + 1), Type: t, Actor: actor, Payload: raw,
		At: time.Date(2026, 10, 11, 9, 40, 0, 0, time.UTC).Add(time.Duration(len(b.evs)) * time.Second),
	})
}

func (b *logBuilder) room(title string) *logBuilder {
	b.add(events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "system"},
		events.RoomCreatedPayload{Title: title, Severity: "sev2", PipelineID: "orders"})
	return b
}

// tool records one investigation step with a call and its result.
func (b *logBuilder) tool(tool, summary string) *logBuilder {
	id := fmt.Sprintf("s_%d", b.step)
	use := fmt.Sprintf("tu_%d", b.step)
	b.step++
	b.add(events.AgentStepStart, events.AgentActor, events.AgentStepStartPayload{StepID: id, Title: tool})
	b.add(events.AgentToolCall, events.AgentActor, events.AgentToolCallPayload{StepID: id, ToolUseID: use, Tool: tool, Input: json.RawMessage(`{}`)})
	b.add(events.AgentToolResult, events.AgentActor, events.AgentToolResultPayload{StepID: id, ToolUseID: use, Tool: tool, Summary: summary})
	return b
}

func (b *logBuilder) hypothesis(text string) *logBuilder {
	b.add(events.AgentHypothesis, events.AgentActor, events.AgentHypothesisPayload{Text: text, BasedOnSteps: []string{"s_0"}, Confidence: "high"})
	return b
}

func (b *logBuilder) status(state string) *logBuilder {
	b.add(events.AgentStatus, events.AgentActor, events.AgentStatusPayload{State: state})
	return b
}

func (b *logBuilder) propose(id, title string, layers ...string) *logBuilder {
	steps := make([]events.ProposalStep, len(layers))
	for i, l := range layers {
		steps[i] = events.ProposalStep{Action: "act_" + l, Layer: l, Params: json.RawMessage(`{}`), Description: "d"}
	}
	b.add(events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: id, Title: title, Steps: steps, Risk: "low", Rollback: "undo", RequiredLayers: layers})
	return b
}

var testHuman = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}

func (b *logBuilder) approve(id, layer string) *logBuilder {
	b.add(events.ApprovalGiven, testHuman, events.ApprovalGivenPayload{ProposalID: id, Layer: layer})
	return b
}

func (b *logBuilder) reject(id, layer, why string) *logBuilder {
	b.add(events.ApprovalRejected, testHuman, events.ApprovalRejectedPayload{ProposalID: id, Layer: layer, Reason: why})
	return b
}

func (b *logBuilder) executed(id string, step int, out string) *logBuilder {
	b.add(events.ActionExecuted, events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "executor"},
		events.ActionExecutedPayload{ProposalID: id, StepIndex: step, OK: true, Output: out})
	return b
}

func (b *logBuilder) comment(id, text string) *logBuilder {
	b.add(events.CommentAdded, testHuman, events.CommentAddedPayload{CommentID: id, Text: text})
	return b
}

type handoffCase struct {
	name    string
	events  []events.Event
	summary events.HandoffSummary
}

// handoffCases are the regression set: logs shaped like the eval scenarios
// (evals/scenarios/*.yaml) with the summary a faithful model should write.
func handoffCases() []handoffCase {
	var cs []handoffCase

	cs = append(cs, handoffCase{"schema-change", (&logBuilder{}).room("orders stale").
		tool("warehouse_freshness", "wh:analytics.orders stale_for=14m").
		tool("connect_status", "pg-orders-connector (source): RUNNING. Task 0: FAILED: Schema being registered is incompatible with an earlier schema for subject \"pg.public.orders-value\" error code: 409 Value.amount: int to bytes(decimal 12,2)").
		tool("kafka_consumer_lag", "group order-sync: Stable, lag=0").
		hypothesis("amount changed from int to decimal(12,2); Schema Registry rejected pg.public.orders-value (409) and task 0 of pg-orders-connector failed").
		propose("p_1", "Set compatibility NONE on pg.public.orders-value then restart task 0", "schema-registry", "kafka-connect").
		approve("p_1", "schema-registry").status("waiting_approval").evs,
		events.HandoffSummary{
			WhatBroke: "analytics.orders went stale because task 0 of pg-orders-connector FAILED.",
			Why:       "amount changed from int to decimal(12,2); Schema Registry rejected the new schema for pg.public.orders-value with 409.",
			Done:      []string{"Read connect_status for pg-orders-connector (step s_0)", "Checked order-sync lag: 0"},
			RuledOut:  []string{"Consumer order-sync is healthy (Stable, lag=0)"},
			Open:      []string{"Proposal p_1 (Set compatibility NONE on pg.public.orders-value then restart task 0) still needs approval from kafka-connect"},
			Risks:     []string{"Relaxing compatibility on pg.public.orders-value affects every consumer"},
		}})

	cs = append(cs, handoffCase{"slot-inactive-wal-growth", (&logBuilder{}).room("pg_wal growing").
		tool("pg_replication_slots", "slot debezium_orders: active=false retained=48.6 GiB wal_status=extended").
		tool("connect_status", "pg-orders-connector (source): STOPPED").
		hypothesis("pg-orders-connector is STOPPED, so slot debezium_orders is inactive and pg_wal keeps growing").
		propose("p_7", "Resume pg-orders-connector", "kafka-connect").
		approve("p_7", "kafka-connect").executed("p_7", 0, "pg-orders-connector resumed").status("done").evs,
		events.HandoffSummary{
			WhatBroke: "pg_wal is growing because slot debezium_orders is inactive.",
			Why:       "pg-orders-connector was STOPPED, so Debezium disconnected from the slot.",
			Done:      []string{"Resumed pg-orders-connector (proposal p_7, approved by kafka-connect)"},
			RuledOut:  []string{"No failed task: the connector was stopped, not failed"},
			Open:      []string{"Watch retained WAL of debezium_orders shrink; it was 48.6 GiB"},
			Risks:     []string{"Disk may fill before the slot catches up"},
		}})

	cs = append(cs, handoffCase{"consumer-poison-pill", (&logBuilder{}).room("order-sync flapping").
		tool("kafka_list_groups", "order-sync: Empty / PreparingRebalance").
		tool("kafka_consumer_lag", "order-sync lag grows only on pg.public.orders partition 1 at offset 25377154").
		hypothesis("record at pg.public.orders/1@25377154 is not Avro; order-sync crashes before commit").
		comment("c_1", "I think someone ran a console producer at 09:02").evs,
		events.HandoffSummary{
			WhatBroke: "order-sync crash-loops on partition 1 of pg.public.orders.",
			Why:       "The record at offset 25377154 is not in Avro wire format, so decoding fails before the commit.",
			Done:      []string{"Found that lag grows only on partition 1"},
			RuledOut:  []string{"Connect and Schema Registry are healthy"},
			Open:      []string{"A human must decide whether to skip offset 25377154 (see comment c_1)"},
			Risks:     []string{"Orders hashed to partition 1 are not reaching the warehouse"},
		}})

	cs = append(cs, handoffCase{"publication-permission-denied", (&logBuilder{}).room("orders stale 28m").
		tool("connect_status", "pg-orders-connector task 0 FAILED: must be owner of table orders").
		tool("pg_recent_ddl", "ALTER TABLE public.orders OWNER TO orders_owner at 08:52").
		hypothesis("table owner of public.orders changed to orders_owner; dbz_publication cannot be altered").evs,
		events.HandoffSummary{
			WhatBroke: "Task 0 of pg-orders-connector is FAILED and the warehouse is 28 minutes stale.",
			Why:       "public.orders now belongs to orders_owner, so ALTER PUBLICATION dbz_publication fails with must be owner of table orders.",
			Done:      []string{"Found the OWNER TO change at 08:52 via pg_recent_ddl"},
			RuledOut:  []string{"The unrelated CREATE INDEX is not the cause"},
			Open:      []string{"A DBA must restore ownership or grant the connector user rights"},
			Risks:     []string{"The slot is inactive and retains WAL"},
		}})

	cs = append(cs, handoffCase{"source-column-dropped", (&logBuilder{}).room("orders stale 34m").
		tool("warehouse_freshness", "wh:analytics.orders stale_for=34m").
		tool("schema_registry_versions", "pg.public.orders-value v3 has no amount field").
		tool("kafka_consumer_lag", "order-sync Empty, lag growing on all partitions").
		hypothesis("DROP COLUMN amount removed it from value schema v3; order-sync still requires amount").evs,
		events.HandoffSummary{
			WhatBroke: "order-sync crash-loops and analytics.orders is 34 minutes stale.",
			Why:       "ALTER TABLE orders DROP COLUMN amount produced value schema v3 without amount, which order-sync still requires.",
			Done:      []string{"Compared schema versions of pg.public.orders-value"},
			RuledOut:  []string{"Connect is healthy; Schema Registry accepted v3"},
			Open:      []string{"Deploy order-sync that no longer needs amount"},
			Risks:     []string{"Lag grows on all partitions while the group is Empty"},
		}})

	cs = append(cs, handoffCase{"topic-retention-data-loss", (&logBuilder{}).room("reconciliation mismatch").
		tool("warehouse_freshness", "wh:analytics.orders stale_for=6h").
		tool("kafka_consumer_lag", "order-sync committed offsets sit above log start; lag=2.9M; 7.9M rows were updated").
		hypothesis("retention.ms of pg.public.orders deleted records before order-sync read them").evs,
		events.HandoffSummary{
			WhatBroke: "Change records on pg.public.orders were deleted by retention before order-sync read them.",
			Why:       "The consumer fell behind by more than the topic retention after a 7.9M-row update, so analytics.orders keeps old versions.",
			Done:      []string{"Compared committed offsets with the log start"},
			RuledOut:  []string{"No consumer error was logged"},
			Open:      []string{"Re-snapshot the lost rows; lag is 2.9M"},
			Risks:     []string{"Reconciliation shows rows that differ between source and warehouse"},
		}})

	// An open proposal waiting on two layers, one already approved.
	cs = append(cs, handoffCase{"open-proposal-two-layers", (&logBuilder{}).room("orders stale").
		tool("connect_status", "pg-orders-connector task 0 FAILED").
		hypothesis("schema incompatible").
		propose("p_9", "Register schema and restart task", "schema-registry", "kafka-connect", "postgres").
		approve("p_9", "postgres").status("waiting_approval").evs,
		events.HandoffSummary{
			WhatBroke: "Task 0 of pg-orders-connector failed.",
			Why:       "The new value schema is incompatible.",
			Done:      []string{"Proposed p_9 and got the postgres approval"},
			RuledOut:  []string{},
			Open:      []string{"Proposal p_9 (Register schema and restart task) waits for schema-registry and kafka-connect approval"},
			Risks:     []string{},
		}})

	// The agent run died on an error mid-investigation.
	cs = append(cs, handoffCase{"interrupted-run", (&logBuilder{}).room("orders stale").
		tool("connect_status", "pg-orders-connector task 0 FAILED").
		status("error").evs,
		events.HandoffSummary{
			WhatBroke: "Task 0 of pg-orders-connector failed.",
			Why:       "Not established: the agent run stopped with an error before a hypothesis.",
			Done:      []string{"Read connect_status once"},
			RuledOut:  []string{},
			Open:      []string{"The investigation was interrupted by an agent error; restart it"},
			Risks:     []string{"No root cause is known"},
		}})

	// A fully executed proposal needs no open item.
	cs = append(cs, handoffCase{"proposal-executed", (&logBuilder{}).room("connector stopped").
		tool("connect_status", "pg-orders-connector STOPPED").
		propose("p_2", "Resume pg-orders-connector", "kafka-connect").
		approve("p_2", "kafka-connect").executed("p_2", 0, "resumed").evs,
		events.HandoffSummary{
			WhatBroke: "pg-orders-connector was STOPPED.",
			Why:       "Someone stopped it and never resumed it.",
			Done:      []string{"Resumed it with p_2"},
			RuledOut:  []string{},
			Open:      []string{},
			Risks:     []string{},
		}})

	// A rejected proposal is closed too.
	cs = append(cs, handoffCase{"proposal-rejected", (&logBuilder{}).room("connector failed").
		tool("connect_status", "pg-orders-connector FAILED").
		propose("p_3", "Restart task 0", "kafka-connect").
		reject("p_3", "kafka-connect", "wait for the DBA").evs,
		events.HandoffSummary{
			WhatBroke: "pg-orders-connector FAILED.",
			Why:       "Not known yet.",
			Done:      []string{"Proposed p_3, rejected by kafka-connect"},
			RuledOut:  []string{},
			Open:      []string{"Find the cause"},
			Risks:     []string{},
		}})
	return cs
}

// TestHandoffRegressionFakeModel runs the real writeHandoff path (prompt
// build, request, parse) with a fake model that returns the faithful summary,
// then checks it. It guards the checker and the plumbing; it does not measure
// the real model (see TestClaudeHandoffQuality under the claude tag).
func TestHandoffRegressionFakeModel(t *testing.T) {
	cases := handoffCases()
	if len(cases) < 8 {
		t.Fatalf("regression set has %d cases, want at least 8", len(cases))
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			js, err := json.Marshal(c.summary)
			if err != nil {
				t.Fatal(err)
			}
			m := &handoffModel{msg: anthropic.BetaMessage{
				StopReason: anthropic.BetaStopReasonEndTurn,
				Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: string(js)}},
			}}
			got, err := writeHandoff(context.Background(), c.events, Options{}, m)
			if err != nil {
				t.Fatalf("writeHandoff: %v", err)
			}
			if v := CheckHandoff(c.events, got); len(v) != 0 {
				t.Fatalf("violations:\n%s", joinViolations(v))
			}
		})
	}
}

func joinViolations(vs []HandoffViolation) string {
	var b strings.Builder
	for _, v := range vs {
		b.WriteString("  " + v.String() + "\n")
	}
	return b.String()
}
