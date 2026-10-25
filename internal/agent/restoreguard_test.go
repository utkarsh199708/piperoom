package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// guardStep is one propose_fix step for the restore-guard tests.
type guardStep struct {
	action, params string
}

var (
	compatNone     = guardStep{"schema_set_compatibility", `{"level":"NONE","subject":"pg.public.orders-value"}`}
	compatBackward = guardStep{"schema_set_compatibility", `{"level":"BACKWARD","subject":"pg.public.orders-value"}`}
	restartTask0   = guardStep{"connect_restart_task", `{"connector":"pg-orders-connector","task":0}`}
)

// guardFix builds a propose_fix input the way the strict schema makes the
// model send it: params as JSON text inside a string.
func guardFix(t *testing.T, steps ...guardStep) json.RawMessage {
	t.Helper()
	ss := make([]map[string]any, len(steps))
	for i, s := range steps {
		ss[i] = map[string]any{"action": s.action, "params": s.params, "description": "d"}
	}
	raw, err := json.Marshal(map[string]any{"title": "fix", "steps": ss, "risk": "low", "rollback": "put it back"})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

const liveRestoreRefusal = "step 3 restores what step 1 changed, but step 2 between them depends on that change taking effect: send the restore as a separate proposal after you have verified recovery"

func TestProposeFixRestoreGuard(t *testing.T) {
	cases := []struct {
		name  string
		steps []guardStep
		want  string // "" = the proposal is created
		tool  bool   // the steps pass the fake registry's Validate, so run them through propose_fix too
	}{
		// The live room r_01M5RWMGFA12MXCBT0TV766C16 proposal, verbatim.
		{"live three-step plan", []guardStep{compatNone, restartTask0, compatBackward}, liveRestoreRefusal, true},
		{"params written in another key order", []guardStep{
			compatNone, restartTask0,
			{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"BACKWARD"}`},
		}, liveRestoreRefusal, true},
		{"two-step plan", []guardStep{compatNone, restartTask0}, "", true},
		{"single step", []guardStep{compatNone}, "", true},
		{"adjacent overwrite of the same subject", []guardStep{compatNone, compatBackward, restartTask0}, "", true},
		{"same setting repeated after the restart", []guardStep{compatNone, restartTask0, compatNone}, "", true},
		{"three steps on different targets", []guardStep{
			compatNone,
			{"connect_restart_task", `{"connector":"billing-connector","task":0}`},
			{"schema_set_compatibility", `{"level":"BACKWARD","subject":"pg.public.customers-value"}`},
		}, "", false},
		{"restart between two subjects", []guardStep{
			{"schema_set_compatibility", `{"level":"NONE","subject":"pg.public.orders-value"}`},
			restartTask0,
			{"schema_set_compatibility", `{"level":"BACKWARD","subject":"pg.public.orders-key"}`},
		}, "", false},
		{"restore after two other steps names the first", []guardStep{
			compatNone, restartTask0,
			{"connect_restart_task", `{"connector":"pg-orders-connector","task":1}`},
			compatBackward,
		}, "step 4 restores what step 1 changed, but step 2 between them", true},
		{"only the same action between them", []guardStep{
			compatNone,
			{"schema_set_compatibility", `{"level":"NONE","subject":"pg.public.orders-key"}`},
			compatBackward,
		}, "", false},
		{"an action with no value field repeated", []guardStep{restartTask0, compatNone, restartTask0}, "", true},
		{"connector config key restored after a restart", []guardStep{
			{"connect_update_connector_config", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"string"}}`},
			restartTask0,
			{"connect_update_connector_config", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"precise"}}`},
		}, "step 3 restores what step 1 changed, but step 2 between them", false},
		{"different connector config keys around a restart", []guardStep{
			{"connect_update_connector_config", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"string"}}`},
			restartTask0,
			{"connect_update_connector_config", `{"connector":"pg-orders-connector","changes":{"time.precision.mode":"connect"}}`},
		}, "", false},
	}
	// The rule itself, over every case: the fake registry knows one connector
	// and one subject, so the cases naming others cannot pass Validate.
	for _, c := range cases {
		t.Run("rule/"+c.name, func(t *testing.T) {
			steps := make([]events.ProposalStep, len(c.steps))
			for i, s := range c.steps {
				steps[i] = events.ProposalStep{Action: s.action, Params: json.RawMessage(s.params)}
			}
			err := restoreBeforeSettle(steps)
			if c.want == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err %v does not wrap tools.ErrBadInput", err)
			}
		})
	}
	// Through propose_fix, for the cases the fake registry validates.
	for _, c := range cases {
		if !c.tool {
			continue
		}
		t.Run("tool/"+c.name, func(t *testing.T) {
			r, proposals := proposeRunner(t, realActions(t))
			res := r.runRoomTool(context.Background(), toolUse{id: "tu_1", name: "propose_fix", input: guardFix(t, c.steps...)})
			if c.want == "" {
				if res.isError {
					t.Fatalf("propose_fix refused: %s", res.content)
				}
				if len(proposals.created.Steps) != len(c.steps) {
					t.Fatalf("stored %d steps, want %d", len(proposals.created.Steps), len(c.steps))
				}
				return
			}
			if !res.isError || !strings.Contains(res.content, c.want) {
				t.Fatalf("want a tool error with %q, got is_error=%v %s", c.want, res.isError, res.content)
			}
			// The guard runs before Proposals.Create: nothing is stored.
			if proposals.created.Title != "" || len(proposals.created.Steps) != 0 {
				t.Fatalf("a refused proposal was stored: %+v", proposals.created)
			}
		})
	}
}

// Through the whole loop: the refusal goes back to the model as an is_error
// tool_result, no proposal is created for it, and the model's corrected
// two-step proposal on the next turn is created.
func TestRestoreGuardModelRecovers(t *testing.T) {
	bad, good := guardFix(t, compatNone, restartTask0, compatBackward), guardFix(t, compatNone, restartTask0)
	props := &countingProposals{}
	sink := &fakeSink{}
	m := &funcModel{fn: func(_ context.Context, i int, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		switch i {
		case 0:
			return mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu_bad","name":"propose_fix","input":`+string(bad)+`}]}`), nil
		case 1:
			return mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu_good","name":"propose_fix","input":`+string(good)+`}]}`), nil
		}
		return mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`), nil
	}}
	r, err := newRunnerWith(Alert{Title: "orders stale", PipelineID: "orders"}, Deps{
		Tools: tools.NewRegistry(), Actions: realActions(t), Proposals: props,
		Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger(),
	}, Options{MaxTurns: 4}, viaSDK(m))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.run(context.Background())

	calls := m.calls()
	if len(calls) < 3 {
		t.Fatalf("model calls = %d, want both propose_fix results sent back", len(calls))
	}
	result := func(call int, id string) string {
		t.Helper()
		msgs := calls[call].Messages
		for _, b := range msgs[len(msgs)-1].Content {
			if b.OfToolResult != nil && b.OfToolResult.ToolUseID == id {
				js, _ := json.Marshal(b.OfToolResult)
				return string(js)
			}
		}
		t.Fatalf("no tool_result for %s in call %d", id, call)
		return ""
	}
	first := result(1, "tu_bad")
	if !strings.Contains(first, `"is_error":true`) || !strings.Contains(first, "step 3 restores what step 1 changed") {
		t.Fatalf("refusal not sent to the model as a tool error: %s", first)
	}
	second := result(2, "tu_good")
	if strings.Contains(second, `"is_error":true`) {
		t.Fatalf("corrected proposal refused: %s", second)
	}
	if n := props.count(); n != 1 {
		t.Fatalf("Proposals.Create called %d times, want 1 (the corrected proposal only)", n)
	}
}
