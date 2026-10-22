package actions_test

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
)

// plainAction has no Stager: X8 must leave it exactly as it was.
type plainAction struct{}

func (plainAction) Name() string                 { return "plain_action" }
func (plainAction) Layer() string                { return "kafka" }
func (plainAction) Describe() string             { return "test" }
func (plainAction) ParamsSchema() map[string]any { return map[string]any{"type": "object"} }
func (plainAction) Validate(context.Context, json.RawMessage) error {
	return nil
}
func (plainAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "ok", nil
}

type stagedAction struct{ plainAction }

func (stagedAction) Name() string { return "staged_action" }
func (stagedAction) Stage(context.Context, json.RawMessage) (actions.StageResult, error) {
	return actions.StageResult{Applied: true, Healthy: true, DurationMS: 1}, nil
}

// X8-0 TC3: Stager is optional; an action without it registers and runs as
// before, and only a Stager passes the assertion.
func TestStagerIsOptional(t *testing.T) {
	r := actions.NewRegistry()
	for _, a := range []actions.Action{plainAction{}, stagedAction{}} {
		if err := r.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	var staged []string
	for _, a := range r.All() {
		if _, ok := a.(actions.Stager); ok {
			staged = append(staged, a.Name())
		}
	}
	if len(staged) != 1 || staged[0] != "staged_action" {
		t.Fatalf("stagers = %v, want [staged_action]", staged)
	}
	// The alias keeps one definition for the action, the log and the API.
	var _ events.StageResult = actions.StageResult{}
}
