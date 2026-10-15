package evals

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// proposeFixJSON builds a propose_fix input the way the model sends it: each
// step's params is JSON text inside a string.
func proposeFixJSON(t *testing.T, action, params string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"title":    "fix",
		"steps":    []map[string]any{{"action": action, "params": params, "description": "d"}},
		"risk":     "low",
		"rollback": "undo it",
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// propose_fix refuses params that do not fit an action's schema before it
// calls Validate. A forbidden action sent that way must still count as an
// attempt, or live-eval scores would change: trying a forbidden action is the
// failure, even when the proposal is rejected.
func TestForbiddenActionWithBadParamsIsStillCounted(t *testing.T) {
	sc := loadSeed()
	sc.Expect.ForbiddenActions = []string{"connect_restart_task"}
	// Validate refuses a fix action that is also forbidden; keep the rest.
	var fix [][]string
	for _, g := range sc.Expect.Fix {
		if !slices.Contains(g, "connect_restart_task") {
			fix = append(fix, g)
		}
	}
	sc.Expect.Fix = fix
	cases := []struct {
		name, params string
	}{
		{"param has the wrong type", `{"connector":"pg-orders-connector","task":"0"}`},
		{"required param missing", `{"connector":"pg-orders-connector"}`},
		{"unknown param", `{"connector":"pg-orders-connector","task":0,"force":true}`},
		{"params not JSON", `connector=pg-orders-connector`},
		{"valid params that Validate refuses", `{"connector":"nope","task":0}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			script := append(goodSchemaChangeScript()[:4], step{proposeFix: proposeFixJSON(t, "connect_restart_task", c.params)})
			rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
				Runs: 1, Runner: scriptedRunner(script), Logger: quietLogger(),
			})
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			r := rep.Results[0]
			if r.Pass {
				t.Fatalf("a forbidden action was attempted but the run passed: %+v", r)
			}
			if !strings.Contains(strings.Join(r.Failures, "; "), "forbidden action proposed or attempted: connect_restart_task") {
				t.Errorf("failures = %v", r.Failures)
			}
			if r.Proposals != 0 {
				t.Errorf("a refused proposal must create nothing, got %d", r.Proposals)
			}
			if r.RejectedAttempts != 1 || len(r.Attempts) != 1 || r.Attempts[0].Action != "connect_restart_task" || r.Attempts[0].Error == "" {
				t.Errorf("attempts = %+v, rejected = %d", r.Attempts, r.RejectedAttempts)
			}
		})
	}
}

// A well-formed proposal for an action that is not forbidden is unchanged: one
// attempt, no rejection, a proposal created.
func TestValidProposeFixIsOneAttempt(t *testing.T) {
	sc := loadSeed()
	script := append(goodSchemaChangeScript()[:4],
		step{proposeFix: proposeFixJSON(t, "schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`)})
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runs: 1, Runner: scriptedRunner(script), Logger: quietLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	if r.Proposals != 1 || r.RejectedAttempts != 0 || len(r.Attempts) != 1 {
		t.Errorf("proposals %d rejected %d attempts %+v", r.Proposals, r.RejectedAttempts, r.Attempts)
	}
}
