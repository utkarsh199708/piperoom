//go:build evalgen

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Room export fixtures for the learned-scenario exporter (L1a). Each file is a
// real GET /api/rooms/{id}/export document: the tool results come from the
// fakes of this package through the real tools (runTool), the events go into
// the in-memory store the room service uses, and room.Service.PrepareExport
// writes the document, hash included. Nothing here is a hand-written log the
// export route could not produce.

const roomFixtureDir = "evals/testdata/rooms"

// roomVariant says which shape of room to build.
type roomVariant struct {
	file string
	id   string
	// shadow marks room.created.shadow; unlabelled leaves out room.labelled;
	// open leaves the room open; disputed has people dispute the hypothesis
	// instead of confirming it (and no fix); unrecovered executes the fix but
	// the verification says it did not recover; secret plants a credential in
	// a summary, a steer and a comment.
	shadow, unlabelled, open, disputed, unrecovered, secret bool
}

func roomVariants() []roomVariant {
	return []roomVariant{
		{file: "orders-confirmed.json", id: "room_9c4e0001"},
		{file: "orders-hard.json", id: "room_9c4e0002", disputed: true},
		{file: "orders-shadow.json", id: "room_9c4e0003", shadow: true},
		{file: "orders-unlabelled.json", id: "room_9c4e0004", unlabelled: true},
		{file: "orders-open.json", id: "room_9c4e0005", open: true},
		{file: "orders-unrecovered.json", id: "room_9c4e0006", unrecovered: true},
		{file: "orders-secret.json", id: "room_9c4e0007", secret: true},
	}
}

// plantedSecret is a fake credential, only ever in the "secret" fixture.
const plantedSecret = "hunter2-fixture-only"

// ordersRoomWorld is the orders stack in a phase: before the fix task 0 of
// pg-orders-connector is FAILED on a Schema Registry 409 and the warehouse is
// stale; after it, everything is flowing.
func ordersRoomWorld(c clock, phase string, _ int) *world {
	w := newWorld()
	taskState, trace := "RUNNING", ""
	if phase == "broken" {
		taskState = "FAILED"
		trace = "org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler\n" +
			"\tCaused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: " +
			"Schema being registered is incompatible with an earlier schema for subject \"pg.public.orders-value\"; error code: 409"
	}
	w.addConnector("pg-orders-connector", "source", "RUNNING", "connect:8083", ordersConnectorConfig(),
		task(0, taskState, "connect:8083", trace))
	topic := "pg.public.orders"
	newest, behind := 14*time.Minute, 14*time.Minute
	if phase != "broken" {
		newest, behind = 2*time.Second, 3*time.Second
	}
	w.topics[topic] = partitions(c, newest, [2]int64{0, 5210}, [2]int64{0, 5188}, [2]int64{0, 5301})
	w.groups["order-sync"] = group("order-sync", "Stable", 1, topic, [2]int64{5210, 5210}, [2]int64{5188, 5188}, [2]int64{5301, 5301})
	w.slots = []clients.ReplicationSlot{slot("debezium_orders", taskState == "RUNNING", 213*1024, "reserved")}
	w.ddl = []clients.DDLChange{{
		At: c.ago(16 * time.Minute), CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders",
		Query: "ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(12,2)", Username: "app_migrator",
	}}
	w.whFresh["analytics.orders"] = c.ago(behind)
	return w
}

type roomCall struct {
	tool  string
	input map[string]any
	phase string
}

func roomCalls() []roomCall {
	return []roomCall{
		{"pipeline_map", m("pipeline_id", "orders"), "broken"},
		{"connect_status", m("connector", "pg-orders-connector"), "broken"},
		// The same call again: the exporter keeps both results as a sequence.
		{"connect_status", m("connector", "pg-orders-connector"), "broken"},
		{"pg_recent_ddl", m("table", "public.orders", "since", ago(24*time.Hour)), "broken"},
		{"connect_status", m("connector", "pg-orders-source"), "broken"}, // a name that does not exist: an error result
		{"kafka_consumer_lag", m("group", "order-sync"), "broken"},
		// After the fix.
		{"connect_status", m("connector", "pg-orders-connector"), "recovered"},
		{"warehouse_freshness", m("table", "analytics.orders", "ts_column", "updated_at"), "recovered"},
	}
}

// renderRoom builds one fixture inside a single wall-clock second, like
// spec.render, because the tools print ages against the wall clock.
func renderRoom(o *owners.Owners, v roomVariant) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 5*time.Millisecond)))
		c := clock{now: time.Now().UTC().Truncate(time.Second)}
		out, err := buildRoomExport(o, v, c)
		if err != nil {
			return nil, err
		}
		if time.Now().UTC().Truncate(time.Second).Equal(c.now) {
			return out, nil
		}
	}
	return nil, errors.New("could not generate within one second; is the machine overloaded?")
}

func buildRoomExport(o *owners.Owners, v roomVariant, c clock) ([]byte, error) {
	ctx := context.Background()
	at := fixedNow.Add(-2 * time.Hour)
	ms := memstore.NewWithClock(func() time.Time { at = at.Add(time.Second); return at })
	svc := room.New(room.Deps{Store: ms, Now: func() time.Time { return at }})
	defer svc.Close()

	system := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "system", Verified: true}
	agent := events.AgentActor
	priya := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya", Verified: true}
	arjun := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun", Verified: true}

	title := "orders stale"
	if err := ms.CreateRoom(ctx, store.Room{ID: v.id, Title: title, Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: at.Add(time.Second)}); err != nil {
		return nil, err
	}
	var failed error
	emit := func(typ events.Type, actor events.Actor, p any) int64 {
		ev, err := ms.Append(ctx, v.id, typ, actor, p)
		if err != nil && failed == nil {
			failed = fmt.Errorf("append %s: %w", typ, err)
		}
		return ev.Seq
	}
	emit(events.RoomCreated, system, events.RoomCreatedPayload{
		Title: title, Severity: "sev2", PipelineID: "orders", AlertSource: "alertmanager", Shadow: v.shadow,
	})

	// Tool calls: run the real tools against the fakes of the call's phase.
	step := 0
	for i, rc := range roomCalls() {
		if rc.phase == "recovered" && i == recoveredFrom() {
			// The fix happens here, between the last broken read and the first recovered one.
			if v.disputed {
				break
			}
			emitFix(emit, v, agent, priya, arjun)
		}
		step++
		// The tool runs against the wall clock; the recorded input is
		// expressed against fixedNow so the fixture is reproducible.
		toolIn, in := map[string]any{}, map[string]any{}
		for k, val := range rc.input {
			toolIn[k], in[k] = val, val
			if d, ok := val.(ago); ok {
				toolIn[k] = c.ago(time.Duration(d)).Format(time.RFC3339)
				in[k] = fixedNow.Add(-time.Duration(d)).Format(time.RFC3339)
			}
		}
		ob := runTool(o, "orders", ordersRoomWorld(c, rc.phase, 0), rc.tool, toolIn, c)
		rawIn, _ := json.Marshal(in)
		id := fmt.Sprintf("tu_%d", i+1)
		sid := fmt.Sprintf("s_%d", step)
		emit(events.AgentStepStart, agent, events.AgentStepStartPayload{StepID: sid, Title: rc.tool})
		emit(events.AgentToolCall, agent, events.AgentToolCallPayload{StepID: sid, ToolUseID: id, Tool: rc.tool, Input: rawIn})
		res := events.AgentToolResultPayload{StepID: sid, ToolUseID: id, Tool: rc.tool, Summary: ob.Summary, Truncated: ob.Truncated, TookMS: 12}
		if ob.Error != "" {
			res.Summary, res.IsError = "error: "+ob.Error, true
		}
		if ob.Data != nil {
			raw, err := json.Marshal(ob.Data)
			if err != nil {
				return nil, err
			}
			res.Data = raw
		}
		if v.secret && rc.tool == "pipeline_map" && rc.phase == "broken" {
			res.Summary += "\nreplica dsn: user=piperoom password=" + plantedSecret
		}
		emit(events.AgentToolResult, agent, res)
	}
	if v.disputed {
		h := emit(events.AgentHypothesis, agent, events.AgentHypothesisPayload{
			Text: "The warehouse loader is slow and needs a restart.", BasedOnSteps: []string{"s_1"}, Confidence: "medium",
		})
		emit(events.HypothesisVerdict, priya, events.HypothesisVerdictPayload{Verdict: "disputed", Note: "the connector is the problem", HypothesisSeq: h})
	}
	if !v.open {
		emit(events.RoomClosed, priya, events.RoomClosedPayload{Resolution: "task 0 restarted after the schema was fixed"})
		if err := ms.CloseRoom(ctx, v.id, at.Add(time.Second)); err != nil {
			return nil, err
		}
		if !v.unlabelled {
			label := events.RoomLabelledPayload{
				ActualLayer: "postgres",
				Cause: "Schema Registry rejected the new Avro schema (409) after ALTER TABLE orders ALTER COLUMN amount TYPE numeric; " +
					"pg-orders-connector task 0 failed",
			}
			if v.disputed {
				// A different incident than the confirmed room, so the two do not dedup.
				label.Cause = "pg-orders-connector lost its replication slot debezium_orders after a failover, so streaming could not resume"
			} else {
				label.FixThatWorked = "restarted task 0 once the schema was compatible"
				label.FixProposalID = "p_1"
			}
			emit(events.RoomLabelled, priya, label)
		}
	}
	if failed != nil {
		return nil, failed
	}
	ex, err := svc.PrepareExport(ctx, v.id)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := ex.Write(ctx, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// recoveredFrom is the index of the first call after the fix.
func recoveredFrom() int {
	for i, rc := range roomCalls() {
		if rc.phase == "recovered" {
			return i
		}
	}
	return -1
}

// emitFix writes the hypothesis, its verdict, a steer and a comment, the
// proposal with its approval and execution, and the verification.
func emitFix(emit func(events.Type, events.Actor, any) int64, v roomVariant, agent, priya, arjun events.Actor) {
	h := emit(events.AgentHypothesis, agent, events.AgentHypothesisPayload{
		Text:         "Connector task 0 failed because Schema Registry returned 409 for the new orders value schema.",
		BasedOnSteps: []string{"s_2", "s_4"}, Confidence: "high",
	})
	emit(events.HypothesisVerdict, priya, events.HypothesisVerdictPayload{Verdict: "confirmed", HypothesisSeq: h})
	steer := "check whether the schema is compatible before restarting"
	comment := "agree, the ALTER landed 16 minutes before the alert"
	if v.secret {
		steer += "; the replica login is password=" + plantedSecret
		comment += " (api_key=" + plantedSecret + ")"
	}
	emit(events.SteerSent, priya, events.SteerSentPayload{Text: steer})
	emit(events.CommentAdded, arjun, events.CommentAddedPayload{CommentID: "c_1", Text: comment, MentionsAgent: true})
	params := json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)
	emit(events.ProposalCreated, agent, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "Restart task 0 of pg-orders-connector", Risk: "low", Rollback: "none needed",
		RequiredLayers: []string{"kafka-connect"},
		Steps: []events.ProposalStep{{
			Action: "connect_restart_task", Layer: "kafka-connect", Params: params,
			Description: "restart task 0", Verify: "task 0 RUNNING",
		}},
	})
	emit(events.ApprovalGiven, arjun, events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "kafka-connect"})
	emit(events.ActionExecuted, events.Actor{Kind: events.ActorSystem, ID: "executor", Name: "executor", Verified: true},
		events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "restarted pg-orders-connector task 0 (RUNNING)"})
	verdict := "recovered: every layer check passed and data is flowing again"
	if v.unrecovered {
		verdict = "not recovered: task 0 is still FAILED"
	}
	emit(events.AgentVerification, agent, events.AgentVerificationPayload{
		ProposalID: "p_1", Recovered: !v.unrecovered, Verdict: verdict,
		Checks: []events.VerificationCheck{{Layer: "kafka-connect", Name: "connect_status", OK: !v.unrecovered, Detail: verdict}},
	})
}

// roomFixtures renders every fixture; with check it reports stale files
// instead of writing them.
func roomFixtures(check bool) (stale []string, err error) {
	o, err := owners.Load(devOwners)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(roomFixtureDir, 0o755); err != nil {
		return nil, err
	}
	for _, v := range roomVariants() {
		out, err := renderRoom(o, v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.file, err)
		}
		path := roomFixtureDir + "/" + v.file
		cur, _ := os.ReadFile(path)
		if bytes.Equal(cur, out) {
			continue
		}
		if check {
			stale = append(stale, path)
			continue
		}
		if err := os.WriteFile(path, out, 0o644); err != nil {
			return nil, err
		}
		fmt.Println("wrote", path)
	}
	return stale, nil
}
