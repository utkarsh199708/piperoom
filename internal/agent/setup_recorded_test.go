package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// The recorded-model tests of the setup mode (docs/design/setup-agent.md
// §3.1, §3.3): a scripted model drives a real setup runner loop over a fake
// paired runner, so the loop's own tool plumbing (setup_plan, propose_fix,
// setup_request_verification, the input handling and the bounds counters) is
// what the test exercises.

// ---- the fake paired runner ----

// fakeSetupRunner is a protocol.RunnerExecutor over an in-memory project
// directory: it starts empty and applies the ops a real runner would, so a
// turn's dir_list reflects exactly what earlier steps executed. It answers
// only the ops the scripted runs use; anything else fails the test.
type fakeSetupRunner struct {
	t     *testing.T
	mu    sync.Mutex
	dir   []string // project entries, in creation order
	up    bool     // compose_up ran
	conns []string // connectors create executed
	ops   []string // every op executed, in order
}

func (f *fakeSetupRunner) Execute(_ context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, req.Op)
	switch req.Op {
	case "env_detect":
		return protocol.OpResult{OK: true, Summary: "linux amd64; docker installed (server 27); compose plugin v2"}, nil
	case "dir_list":
		if len(f.dir) == 0 {
			return protocol.OpResult{OK: true, Summary: "project dir: empty"}, nil
		}
		return protocol.OpResult{OK: true, Summary: "project dir: " + strings.Join(f.dir, ", ")}, nil
	case "compose_write":
		var p struct {
			Templates []string `json:"templates"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return protocol.OpResult{}, fmt.Errorf("fakeSetupRunner: compose_write params: %w", err)
		}
		f.dir = append(f.dir, "compose.yaml", ".env")
		return protocol.OpResult{OK: true, Summary: "wrote compose.yaml and .env for templates " + strings.Join(p.Templates, "+")}, nil
	case "properties_write":
		f.dir = append(f.dir, "worker.properties")
		return protocol.OpResult{OK: true, Summary: "wrote worker.properties"}, nil
	case "compose_up":
		f.up = true
		return protocol.OpResult{OK: true, Summary: "stack up: kafka, connect and postgres containers running"}, nil
	case "compose_ps":
		if !f.up {
			return protocol.OpResult{OK: true, Summary: "no containers"}, nil
		}
		return protocol.OpResult{OK: true, Summary: "kafka running, connect running, postgres running"}, nil
	case "connect_create_connector":
		var p struct {
			Connector string `json:"connector"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return protocol.OpResult{}, fmt.Errorf("fakeSetupRunner: connect_create_connector params: %w", err)
		}
		f.conns = append(f.conns, p.Connector)
		return protocol.OpResult{OK: true, Summary: "created connector " + p.Connector + " (RUNNING)"}, nil
	}
	return protocol.OpResult{}, fmt.Errorf("fakeSetupRunner: op %q is not scripted", req.Op)
}

// execStep applies one executed proposal step to the fake runner, as the
// room's executor would through the action and the remote client.
func (f *fakeSetupRunner) execStep(op string, params json.RawMessage) {
	switch op {
	case "compose_write", "properties_write", "compose_up", "connect_create_connector":
		f.mu.Lock()
		f.ops = append(f.ops, op)
		f.mu.Unlock()
		if op == "compose_write" {
			f.dir = append(f.dir, "compose.yaml", ".env")
		}
		if op == "properties_write" {
			f.dir = append(f.dir, "worker.properties")
		}
		if op == "compose_up" {
			f.up = true
		}
		if op == "connect_create_connector" {
			var p struct {
				Connector string `json:"connector"`
			}
			_ = json.Unmarshal(params, &p)
			f.conns = append(f.conns, p.Connector)
		}
	default:
		f.t.Errorf("fakeSetupRunner: executed step %q is not scripted", op)
	}
}

// ---- the fake room (Proposals) ----

// execProposals is the room's proposal path as the test stands in for it:
// Create records the proposal and executes every step on the fake runner, so
// the dir_list of the next turn reflects the executed step.
type execProposals struct {
	fr *fakeSetupRunner
	mu sync.Mutex
	n  int
}

func (p *execProposals) Create(_ context.Context, pc events.ProposalCreatedPayload) (string, error) {
	p.mu.Lock()
	p.n++
	id := fmt.Sprintf("p_%d", p.n)
	p.mu.Unlock()
	for _, s := range pc.Steps {
		p.fr.execStep(s.Action, s.Params)
	}
	return id, nil
}

// planGatedProposals is the room's plan rule (design §3.3, ErrNotInPlan): a
// proposal whose step is not in the current setup plan is refused, where
// "current" is the latest setup.plan the room accepted (the agent's sink).
// An accepted proposal's steps execute on the fake runner, like the room's
// executor would, so a refused repeat is visible as an op that never ran.
type planGatedProposals struct {
	sink *fakeSink
	fr   *fakeSetupRunner
	mu   sync.Mutex
	n    int
	last int // setup.plan count the last accepted proposal saw
}

func (g *planGatedProposals) Create(_ context.Context, pc events.ProposalCreatedPayload) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	plans := len(g.sink.ofType(events.SetupPlan))
	if plans == g.last {
		return "", fmt.Errorf("room: proposal step is not in the current setup plan: %w", ErrNotInPlan)
	}
	g.last = plans
	g.n++
	for _, s := range pc.Steps {
		g.fr.execStep(s.Action, s.Params)
	}
	return fmt.Sprintf("p_%d", g.n), nil
}

// ---- shared setup ----

// setupAlert is the alert of a setup room whose request the server stored in
// Raw, as room.created does.
func setupAlert(raw string) Alert {
	return Alert{Title: "set up orders", PipelineID: "orders", Mode: ModeSetup, Raw: json.RawMessage(raw)}
}

const setupReqJSON = `{
	"pipeline_id": "orders",
	"tenant": "",
	"title": "orders pipeline",
	"source_kind": "postgres",
	"target": "db.local:5432",
	"topic_prefix": "pg",
	"tables": ["public.orders"],
	"connectors": ["pg-orders"],
	"targets": [
		{"kind": "postgres", "address": "db.local:5432", "class": "local"},
		{"kind": "connect", "address": "connect.local:8083", "class": "local"}
	]
}`

// setupActions is the actions registry the scripted runs propose over: the
// environment ops and the connector op as tiered fake actions, the way the
// wiring builds them over the paired runner (the agent package cannot import
// internal/setup/actions: it imports internal/room, which imports this one).
func setupActions(t *testing.T) *actions.Registry {
	t.Helper()
	reg := actions.NewRegistry()
	for _, a := range []*tieredAction{
		{fakeAction{name: "compose_write", layer: events.LayerEnvironment}, "T1"},
		{fakeAction{name: "properties_write", layer: events.LayerEnvironment}, "T1"},
		{fakeAction{name: "compose_up", layer: events.LayerEnvironment}, "T1"},
		{fakeAction{name: "connect_create_connector", layer: "kafka-connect"}, "T2"},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

// newSetupRunner builds a setup-mode runner over the fake paired runner, with
// the scripted model m: the setup read tools are the real ones (the agent
// package can import internal/setup/tools), the actions are setupActions, and
// the request is the room's Raw.
func newSetupRunner(t *testing.T, fr *fakeSetupRunner, m sdkModel, inbox Inbox, sink Sink, props Proposals, acts *actions.Registry) *runner {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	if err := setuptools.Register(reg, setuptools.Deps{
		Runner:   fr,
		RoomID:   "r_setup",
		CallRoom: func(context.Context) string { return "r_setup" },
		Class:    protocol.ClassLocal,
	}); err != nil {
		t.Fatalf("setuptools.Register: %v", err)
	}
	r, err := newRunner(setupAlert(setupReqJSON), Deps{Tools: reg, Actions: acts, Proposals: props, Inbox: inbox, Sink: sink, Logger: discardLogger()}, Options{MaxTurns: 12})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	r.model = viaSDK(m)
	r.clk = &fakeClock{}
	return r
}

// recordedModel is a scripted model whose turns may read the request; it
// records every request for later assertions.
type recordedModel struct {
	mu    sync.Mutex
	turns []func(text string) string
	calls [][]anthropic.BetaMessageParam
	t     *testing.T
}

func (m *recordedModel) Turn(_ context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.mu.Lock()
	m.calls = append(m.calls, cloneMessages(p.Messages))
	i := len(m.calls) - 1
	var turn func(string) string
	if i < len(m.turns) {
		turn = m.turns[i]
	}
	text := requestText(p.Messages)
	m.mu.Unlock()
	if turn == nil {
		m.t.Fatalf("recorded model ran out of scripted turns at turn %d", i)
	}
	return mustMessage(m.t, turn(text)), nil
}

// callsText is every text and tool-result content the model ever received.
func (m *recordedModel) callsText() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	for _, ms := range m.calls {
		for _, msg := range ms {
			for _, blk := range msg.Content {
				if blk.OfText != nil {
					b.WriteString(blk.OfText.Text)
					b.WriteString("\n")
				}
				if blk.OfToolResult != nil {
					js, _ := json.Marshal(blk.OfToolResult.Content)
					b.Write(js)
					b.WriteString("\n")
				}
			}
		}
	}
	return b.String()
}

// planToolUse is a setup_plan call recording the given phases.
func planToolUse(id string, phases ...map[string]any) map[string]any {
	return map[string]any{"type": "tool_use", "id": id, "name": "setup_plan", "input": map[string]any{"phases": phases}}
}

// planToolUseSuperseding is a setup_plan call that names the plan it replaces
// (supersedes is the model's own field in the design's tool schema).
func planToolUseSuperseding(id, supersedes string, phases ...map[string]any) map[string]any {
	in := map[string]any{"phases": phases}
	if supersedes != "" {
		in["supersedes"] = supersedes
	}
	return map[string]any{"type": "tool_use", "id": id, "name": "setup_plan", "input": in}
}

// firstPlanID reads the id the first setup_plan result assigned, which the
// model's re-plan names in supersedes: the summary the tool returned carries
// it, so it is part of the model's history, never invented by the script.
func firstPlanID(t *testing.T, m *recordedModel) string {
	t.Helper()
	text := m.callsText()
	const marker = "plan pl_"
	i := strings.Index(text, marker)
	if i < 0 {
		t.Fatalf("the first plan's summary never reached the model: %q", text)
	}
	rest := text[i+len("plan "):]
	if j := strings.IndexAny(rest, ": \n"); j > 0 {
		return rest[:j]
	}
	t.Fatalf("no plan id after %q in %q", marker, text)
	return ""
}

// TestSetupRecordedLadderRun is the row's recorded-model test (U1-10): the
// model takes the paired runner from an empty project directory to a ladder
// request — reads the empty dir, records the plan, proposes and executes the
// environment steps and the connector, and requests verification. The
// ladder's canary step then executes, and the model waits for the server's
// outcome without claiming one. The test, standing in for the ladder runner,
// records the green setup.verified after the run: the sink holds exactly
// that one, after the request, so the run itself wrote none. (The model
// never reads the verdict here: the setup_verified input that carries it is
// tested in setup_verified_test.go.)
func TestSetupRecordedLadderRun(t *testing.T) {
	fr := &fakeSetupRunner{t: t}
	m := &recordedModel{t: t}
	sink := &fakeSink{}
	m.turns = []func(string) string{
		// Turn 1: inspect the empty project.
		func(string) string {
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					{"type": "tool_use", "id": "tu_1", "name": "env_detect", "input": map[string]any{}},
					{"type": "tool_use", "id": "tu_2", "name": "dir_list", "input": map[string]any{"path": "."}},
				},
			})
			return string(b)
		},
		// Turn 2: the plan before any proposal. The dir read of turn 1 is a
		// tool_result block, which requestText does not carry; callsText has
		// every text and tool-result block the model received.
		func(text string) string {
			if !strings.Contains(m.callsText(), "project dir: empty") {
				t.Errorf("the first reads did not see the empty dir: %q", m.callsText())
			}
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					planToolUse("tu_3",
						map[string]any{
							"name": "stack",
							"steps": []map[string]any{
								{"op": "compose_write", "params_hint": "kafka, connect, registry and the source", "verify": "dir_list shows compose.yaml"},
								{"op": "properties_write", "params_hint": "the worker and .env properties", "verify": "dir_list shows worker.properties"},
								{"op": "compose_up", "params_hint": "start the rendered stack", "verify": "compose_ps shows the containers running"},
							},
						},
						map[string]any{
							"name": "connect",
							"steps": []map[string]any{
								{"op": "connect_create_connector", "params_hint": "the Postgres source connector", "verify": "the connector runs and its tasks are RUNNING"},
							},
						},
					),
				},
			})
			return string(b)
		},
		// Turn 3: propose the first step; the room executes it.
		func(string) string {
			return toolUseMsg("tu_4", "propose_fix", proposeInput("compose_write",
				`{"templates":["kafka-kraft","connect","registry","source-postgres"],"project":"piperoom-orders","ports":{"kafka":19092,"connect":18083,"registry":18081,"postgres":15432}}`,
				"render the Compose stack"))
		},
		// Turn 4: the executed step is visible; propose the next one.
		func(text string) string {
			if !strings.Contains(text, "compose.yaml") {
				t.Errorf("the executed compose_write is not in the project dir: %q", text)
			}
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					{"type": "tool_use", "id": "tu_5", "name": "dir_list", "input": map[string]any{"path": "."}},
					{"type": "tool_use", "id": "tu_6", "name": "propose_fix", "input": proposeInput("properties_write",
						`{"file":"worker.properties","entries":{"bootstrap.servers":"kafka:19092"}}`,
						"write the worker properties")},
				},
			})
			return string(b)
		},
		// Turn 5: start the stack.
		func(string) string {
			return toolUseMsg("tu_7", "propose_fix", proposeInput("compose_up", `{}`, "start the rendered stack"))
		},
		// Turn 6: the stack is up; create the connector.
		func(text string) string {
			if !strings.Contains(text, "stack up") {
				t.Errorf("compose_up did not run before the connector step: %q", text)
			}
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					{"type": "tool_use", "id": "tu_8", "name": "compose_ps", "input": map[string]any{}},
					{"type": "tool_use", "id": "tu_9", "name": "propose_fix", "input": proposeInput("connect_create_connector",
						`{"connector":"pg-orders","class":"io.debezium.connector.postgresql.PostgresConnector","config":{"database.hostname":"postgres"}}`,
						"create the Postgres source connector")},
				},
			})
			return string(b)
		},
		// Turn 7: everything the plan names has run; request the ladder.
		func(string) string {
			return toolUseMsg("tu_10", "setup_request_verification", map[string]any{})
		},
		// Turn 8 (after the canary input): the verdict is the server's to
		// record, so the model waits for it.
		func(text string) string {
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "end_turn",
				"content": []map[string]any{
					{"type": "text", "text": "Verification requested and the ladder's canary step executed. The server's ladder runner records the outcome itself; I am waiting for it and for rung 8's change."},
				},
			})
			return string(b)
		},
	}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil, // the opening drain
			nil,
			nil,
			{actionInput("proposal p_1 step 0: compose_write executed: wrote compose.yaml and .env", okPtr(true))},
			{actionInput("proposal p_2 step 0: properties_write executed: wrote worker.properties", okPtr(true))},
			{actionInput("proposal p_3 step 0: compose_up executed: stack up", okPtr(true))},
			{actionInput("proposal p_4 step 0: connect_create_connector executed: created connector pg-orders (RUNNING)", okPtr(true))},
			{
				// The ladder's canary proposal: system origin, executed, and
				// then the ladder's own verdict, in the room log.
				{Kind: "action_result", Text: "proposal p_canary step 0: source_insert_canary executed", OK: okPtr(true), ProposalID: "p_canary"},
			},
		},
		waitErr: context.Canceled,
	}
	props := &execProposals{fr: fr}
	r := newSetupRunner(t, fr, m, inbox, sink, props, setupActions(t))

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	// The ladder request reached the room log exactly once.
	reqs := sink.ofType(setuptools.TypeVerificationRequested)
	if len(reqs) != 1 {
		t.Fatalf("setup.verification_requested emitted %d times, want once", len(reqs))
	}
	// The fake ladder runner records its verdict after the request, as the
	// room's own emit; the agent never emits setup.verified.
	green := events.SetupVerifiedPayload{
		PlanID: "pl_1",
		Rungs: []events.SetupRung{
			{Name: "connector", OK: true}, {Name: "tasks", OK: true}, {Name: "topic", OK: true},
			{Name: "snapshot", OK: true}, {Name: "canary", OK: true}, {Name: "registry", OK: true},
			{Name: "sink", OK: true},
		},
	}
	if err := sink.Emit(context.Background(), events.SetupVerified, green); err != nil {
		t.Fatalf("the ladder's emit: %v", err)
	}
	verified := sink.ofType(events.SetupVerified)
	if len(verified) != 1 {
		t.Fatalf("setup.verified in the log %d times, want exactly the ladder's one", len(verified))
	}
	// The verdict follows the request in the log, and the run's own texts
	// never claim the verification: the last agent message credits the
	// server.
	if sinkIndexOf(sink, setuptools.TypeVerificationRequested) > sinkIndexOf(sink, events.SetupVerified) {
		t.Fatal("setup.verified precedes the verification request")
	}
	texts := messageTexts(sink)
	for _, m := range texts {
		if strings.Contains(m, "I verified") || strings.Contains(m, "I have verified") {
			t.Fatalf("the model claimed the verification: %q", m)
		}
	}
	// messageTexts appends the agent.verification verdicts after the agent
	// messages, so the last agent message is not its last entry: the run's
	// own texts must credit the server, whatever verdicts came before them.
	var last string
	for _, e := range sink.ofType(events.AgentMessage) {
		var p events.AgentMessagePayload
		if json.Unmarshal(e.payload, &p) == nil {
			last = p.Text
		}
	}
	if !strings.Contains(last, "server") {
		t.Fatalf("the last agent message does not credit the server: %q", last)
	}
	// The whole story ran against the fake runner: the dir went from empty
	// to the rendered files, the stack went up, and the connector exists.
	if len(fr.ops) == 0 {
		t.Fatal("the fake runner executed nothing")
	}
	if !slices.Contains(fr.dir, "compose.yaml") || !fr.up || len(fr.conns) != 1 || fr.conns[0] != "pg-orders" {
		t.Fatalf("the runner's state after the run: dir=%v up=%v conns=%v", fr.dir, fr.up, fr.conns)
	}
}

// TestSetupReplanRefusedUntilNewPlan is the row's re-plan test (U1-10): after
// a failed step the next proposal without a new setup_plan is refused with
// ErrNotInPlan as a propose_fix tool error (never a stop), the model reads
// the refusal and records a new plan, and the run recovers: the next proposal
// is accepted and its step executes.
func TestSetupReplanRefusedUntilNewPlan(t *testing.T) {
	fr := &fakeSetupRunner{t: t}
	m := &recordedModel{t: t}
	sink := &fakeSink{}
	m.turns = []func(string) string{
		// Turn 1: the initial plan.
		func(string) string {
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					planToolUse("tu_1",
						map[string]any{
							"name": "stack",
							"steps": []map[string]any{
								{"op": "compose_write", "params_hint": "render the stack", "verify": "dir_list shows compose.yaml"},
							},
						},
					),
				},
			})
			return string(b)
		},
		// Turn 2: the step, which the room executes and which fails.
		func(string) string {
			return toolUseMsg("tu_2", "propose_fix", proposeInput("compose_write",
				`{"templates":["kafka-kraft"],"project":"piperoom-orders"}`, "render the stack"))
		},
		// Turn 3: the same step again, without a new plan: refused.
		func(string) string {
			return toolUseMsg("tu_3", "propose_fix", proposeInput("compose_write",
				`{"templates":["kafka-kraft"],"project":"piperoom-orders"}`, "render the stack"))
		},
		// Turn 4: the model read the refusal and re-plans, superseding the
		// old plan, with a step that changes the failing input.
		func(text string) string {
			if !strings.Contains(m.callsText(), "not in the current setup plan") {
				t.Errorf("the refusal never reached the model: %q", m.callsText())
			}
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					planToolUseSuperseding("tu_4", firstPlanID(t, m),
						map[string]any{
							"name": "stack",
							"steps": []map[string]any{
								{"op": "compose_write", "params_hint": "render the stack without the registry template", "verify": "dir_list shows compose.yaml"},
							},
						},
					),
				},
			})
			return string(b)
		},
		// Turn 5: the new plan's step: accepted and executed.
		func(string) string {
			return toolUseMsg("tu_5", "propose_fix", proposeInput("compose_write",
				`{"templates":["kafka-kraft","connect"],"project":"piperoom-orders"}`, "render the stack without the registry"))
		},
		// Turn 6: done.
		func(string) string {
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "end_turn",
				"content": []map[string]any{
					{"type": "text", "text": "The new plan's step ran. Waiting for the next steer."},
				},
			})
			return string(b)
		},
	}
	// The duplicate-delivery dedupe of the steps bound is unit-tested in
	// TestSetupObserve; a duplicate result delivered as its own batch after
	// end_turn would wake the model for an extra turn here.
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil, // the opening drain
			nil,
			{actionInput("proposal p_1 step 0: compose_write failed: template registry refused: not installed", okPtr(false))},
			nil, // the refused proposal executed nothing
			nil,
			{actionInput("proposal p_2 step 0: compose_write executed: wrote compose.yaml", okPtr(true))},
		},
		waitErr: context.Canceled,
	}
	props := &planGatedProposals{sink: sink, fr: fr}
	r := newSetupRunner(t, fr, m, inbox, sink, props, setupActions(t))

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	// The refusal was a propose_fix tool error, never a stop: the run ended
	// cleanly (nil), and the model recovered with a new plan that supersedes
	// the first, then a proposal the room accepted.
	plans := sink.ofType(events.SetupPlan)
	if len(plans) != 2 {
		t.Fatalf("setup.plan emitted %d times, want 2 (the plan and its supersession)", len(plans))
	}
	var first, second events.SetupPlanPayload
	if err := json.Unmarshal(plans[0].payload, &first); err != nil {
		t.Fatalf("first plan payload: %v", err)
	}
	if err := json.Unmarshal(plans[1].payload, &second); err != nil {
		t.Fatalf("second plan payload: %v", err)
	}
	if second.Supersedes != first.PlanID {
		t.Fatalf("the new plan supersedes %q, want the first plan %q", second.Supersedes, first.PlanID)
	}
	// Both accepted proposals' steps ran on the fake runner — the failed
	// attempt and the recovered step — and the refused repeat is not among
	// them: the room's plan gate blocked it before the executor.
	var executed int
	for _, op := range fr.ops {
		if op == "compose_write" {
			executed++
		}
	}
	if executed != 2 {
		t.Fatalf("compose_write executed %d times, want 2 (the failed step and the recovered step, never the refused repeat)", executed)
	}
}

// TestSetupAttemptsBoundStopsAReplanLoop: the room makes the model re-plan
// after every failed step, so a model that fails, re-plans and fails again is
// stopped by the attempts bound: after the third consecutive failure the run
// emits setup.blocked (attempts) and returns a *BudgetError before another
// model turn, instead of looping until the wall clock.
func TestSetupAttemptsBoundStopsAReplanLoop(t *testing.T) {
	fr := &fakeSetupRunner{t: t}
	m := &recordedModel{t: t}
	sink := &fakeSink{}
	planTurn := func(id string) func(string) string {
		return func(string) string {
			b, _ := json.Marshal(map[string]any{
				"stop_reason": "tool_use",
				"content": []map[string]any{
					planToolUse(id, map[string]any{
						"name":  "stack",
						"steps": []map[string]any{{"op": "compose_write", "params_hint": "render the stack", "verify": "dir_list shows compose.yaml"}},
					}),
				},
			})
			return string(b)
		}
	}
	proposeTurn := func(id string) func(string) string {
		return func(string) string {
			return toolUseMsg(id, "propose_fix", proposeInput("compose_write",
				`{"templates":["kafka-kraft"],"project":"piperoom-orders"}`, "render the stack"))
		}
	}
	// Three rounds of plan and failed step; a seventh turn would mean the
	// bound did not stop the run (recordedModel fails the test then).
	m.turns = []func(string) string{
		planTurn("tu_1"), proposeTurn("tu_2"),
		planTurn("tu_3"), proposeTurn("tu_4"),
		planTurn("tu_5"), proposeTurn("tu_6"),
	}
	failed := func(n int) []HumanInput {
		return []HumanInput{actionInput(fmt.Sprintf("proposal p_%d step 0: compose_write failed: template refused", n), okPtr(false))}
	}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil, // the opening drain
			nil, failed(1),
			nil, failed(2),
			nil, failed(3),
		},
		waitErr: context.Canceled,
	}
	props := &planGatedProposals{sink: sink, fr: fr}
	r := newSetupRunner(t, fr, m, inbox, sink, props, setupActions(t))

	err := r.run(context.Background())
	be, ok := asBudgetError(err)
	if !ok || be.setupReason != events.SetupBlockedAttempts {
		t.Fatalf("run = %v, want a setup *BudgetError for attempts", err)
	}
	if n := len(m.calls); n != 6 {
		t.Fatalf("model turns = %d, want 6: the bound stops the run before a seventh", n)
	}
	blocked := sink.ofType(events.SetupBlocked)
	if len(blocked) != 1 {
		t.Fatalf("setup.blocked emitted %d times, want once", len(blocked))
	}
	var p events.SetupBlockedPayload
	if err := json.Unmarshal(blocked[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Reason != events.SetupBlockedAttempts {
		t.Fatalf("setup.blocked reason = %q, want %q", p.Reason, events.SetupBlockedAttempts)
	}
	if n := len(sink.ofType(events.SetupPlan)); n != 3 {
		t.Fatalf("setup.plan emitted %d times, want 3 (one per round)", n)
	}
}

// sinkIndexOf is the position of the first event of typ in the sink's log,
// or len(events) when none: ordering assertions compare positions.
func sinkIndexOf(s *fakeSink, typ events.Type) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.events {
		if e.typ == typ {
			return i
		}
	}
	return len(s.events)
}
