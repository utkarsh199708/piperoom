package agent

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// modelRecorder is a scripted model that also records the model id of every
// request.
type modelRecorder struct {
	scriptedModel
	models []string
}

func (m *modelRecorder) Turn(ctx context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.models = append(m.models, p.Model)
	return m.scriptedModel.Turn(ctx, p)
}

func TestPickModelRules(t *testing.T) {
	newR := func(triage string, turns int) *runner {
		return &runner{opts: Options{Model: "main"}, triageModel: triage, triageTurns: turns}
	}

	t.Run("no triage model always uses the main model", func(t *testing.T) {
		r := newR("", 6)
		for i := 0; i < 8; i++ {
			if name, _, triage := r.pickModel(); name != "main" || triage {
				t.Fatalf("turn %d: got %q (triage=%v), want main", i, name, triage)
			}
		}
	})

	t.Run("first N turns triage then main", func(t *testing.T) {
		r := newR("tri", 3)
		var got []string
		for i := 0; i < 6; i++ {
			name, _, _ := r.pickModel()
			got = append(got, name)
		}
		want := []string{"tri", "tri", "tri", "main", "main", "main"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("models = %v, want %v", got, want)
		}
	})

	t.Run("a verification verdict switches for good", func(t *testing.T) {
		r := newR("tri", 6)
		if name, _, _ := r.pickModel(); name != "tri" {
			t.Fatalf("before the verdict: got %q, want tri", name)
		}
		r.verified = true
		for i := 0; i < 3; i++ {
			if name, reason, _ := r.pickModel(); name != "main" || reason != "after verification" {
				t.Fatalf("after the verdict: got %q (%s), want main", name, reason)
			}
		}
	})
}

func triageRunner(t *testing.T, m sdkModel, inbox Inbox, sink *fakeSink, triageTurns int) *runner {
	t.Helper()
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	r := newTestRunner(t, m, inbox, sink, rec, Options{Model: "main", MaxTurns: 20})
	r.triageModel = "tri"
	r.triageTurns = triageTurns
	return r
}

func TestRunSwitchesToMainAfterNTriageTurns(t *testing.T) {
	m := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{
		toolTurnN(t, 1), toolTurnN(t, 2), toolTurnN(t, 3),
	}}}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, 2)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"tri", "tri", "main", "main"} // three tool turns, then the default end_turn
	if !reflect.DeepEqual(m.models, want) {
		t.Fatalf("models = %v, want %v", m.models, want)
	}
}

func TestRunRerunsAProposeFixTurnOnTheMainModel(t *testing.T) {
	propose := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"text","text":"triage draft that must not be kept"},
		{"type":"tool_use","id":"tu9","name":"propose_fix","input":{"title":"t"}}
	],"usage":{"input_tokens":50,"output_tokens":5}}`)
	mainAnswer := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"main answer"}],"usage":{"input_tokens":50,"output_tokens":7}}`)
	m := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{propose, mainAnswer}}}
	sink := &fakeSink{}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, sink, 6)
	b := newTokenBudget(0)
	r.model = &budgetModel{inner: viaSDK(m), budget: b, loop: true}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []string{"tri", "main"}; !reflect.DeepEqual(m.models, want) {
		t.Fatalf("models = %v, want %v", m.models, want)
	}
	// Both requests carry the same history: the triage answer was dropped.
	if !reflect.DeepEqual(m.calls[0], m.calls[1]) {
		t.Fatalf("the re-run changed the history:\n%s\n%s", messagesJSON(m.calls[0]), messagesJSON(m.calls[1]))
	}
	msgs := sink.ofType(events.AgentMessage)
	if len(msgs) != 1 || !strings.Contains(string(msgs[0].payload), "main answer") {
		t.Fatalf("agent messages = %v, want only the main model's answer", msgs)
	}
	// The dropped triage call was still paid for.
	if got := b.Used(); got != 50+5+50+7 {
		t.Fatalf("used = %d, want %d", got, 50+5+50+7)
	}
}

func TestRunTriageStaysOnForReadTurnsAndNeverReruns(t *testing.T) {
	m := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"tool_use","id":"tu1","name":"update_hypothesis","input":{"text":"h","based_on_steps":[],"confidence":"low"}}
		]}`),
	}}}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, 6)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []string{"tri", "tri"}; !reflect.DeepEqual(m.models, want) {
		t.Fatalf("models = %v, want %v: update_hypothesis is not a write proposal", m.models, want)
	}
}

func TestRunUsesMainModelFromTheFirstTurnAfterAVerdict(t *testing.T) {
	failed := false
	// A failed step is a [verification] verdict for the model.
	inbox := &fakeInbox{
		drains: [][]HumanInput{{{
			Kind: "action_result", OK: &failed, ProposalID: "p_1", StepIndex: new(int), Text: "step failed",
		}}},
		waitErr: context.Canceled,
	}
	m := &modelRecorder{}
	r := triageRunner(t, m, inbox, &fakeSink{}, 6)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []string{"main"}; !reflect.DeepEqual(m.models, want) {
		t.Fatalf("models = %v, want %v", m.models, want)
	}
}

func TestEndpointRunUsesTriageModelOnlyWhenItDiffers(t *testing.T) {
	run := func(triage, main string) []string {
		m := &modelRecorder{}
		ep := testEndpoint(m, 0)
		ep.triageModel = triage
		reg := tools.NewRegistry()
		err := ep.Run(context.Background(), Alert{RoomID: "r", Title: "t", PipelineID: "p"}, Deps{
			Tools: reg, Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: &fakeSink{}, Logger: discardLogger(),
		}, Options{Model: main})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return m.models
	}
	if got, want := run("tri", "main"), []string{"tri"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("with a triage model: %v, want %v", got, want)
	}
	if got, want := run("", "main"), []string{"main"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("without one: %v, want %v", got, want)
	}
	if got, want := run("main", "main"), []string{"main"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("triage equal to main: %v, want %v", got, want)
	}
}
