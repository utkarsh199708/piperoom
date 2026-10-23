package evals

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// X2e: the two recovery scenarios on the room path with the scripted runner
// (the right recovery proposed, approved and executed by the harness), and
// the recovery actions' Verify on the replay clients (what the agent counts
// after the step, X2c).

type recoveryCase struct {
	file      string
	reads     []step
	fix       string // propose_fix input of the right action
	forbidden string // propose_fix input of a forbidden action
	wrongStep string // a valid step on another action: fix_targets fails it
	refused   string // what the forbidden action's Validate says
}

func recoveryCases(t *testing.T) []recoveryCase {
	pipe := map[string]any{"pipeline_id": "orders"}
	reconcile := map[string]any{"table": "public.orders", "since": "2026-10-22T08:40:00Z", "until": "2026-10-22T09:38:00Z"}
	return []recoveryCase{{
		file: "rows-lost-after-retention",
		reads: []step{
			{tool: "pipeline_map", input: pipe},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
			{tool: "reconcile", input: reconcile},
			{tool: "kafka_topic_offsets", input: map[string]any{"topic": "pg.public.orders"}},
			{tool: "kafka_find_by_key", input: map[string]any{"topic": "pg.public.orders", "key": "1301", "since": "2026-10-22T06:40:00Z"}},
		},
		fix:       proposeFixJSON(t, "pg_resnapshot_rows", `{"connector":"pg-orders-connector","table":"public.orders","key_range":{"column":"id","from":"1301","to":"1340"}}`),
		forbidden: proposeFixJSON(t, "kafka_replay_range", `{"topic":"pg.public.orders","to_topic":"pg.public.orders.reprocess","ranges":[{"partition":0,"from_offset":433,"to_offset":445}]}`),
		wrongStep: proposeFixJSON(t, "pg_snapshot_signal", `{"connector":"pg-orders-connector","tables":["public.orders"],"signal_table":"public.devctl_signal"}`),
		refused:   "below the log start",
	}, {
		file: "consumer-skipped-range",
		reads: []step{
			{tool: "pipeline_map", input: pipe},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
			{tool: "reconcile", input: reconcile},
			{tool: "kafka_find_by_key", input: map[string]any{"topic": "pg.public.orders", "key": "1301", "since": "2026-10-22T06:40:00Z"}},
			{tool: "trace_record", input: map[string]any{"pipeline_id": "orders", "table": "public.orders", "key": "1301"}},
		},
		fix:       proposeFixJSON(t, "kafka_replay_range", `{"topic":"pg.public.orders","to_topic":"pg.public.orders.reprocess","ranges":[{"partition":0,"from_offset":433,"to_offset":445},{"partition":1,"from_offset":434,"to_offset":446},{"partition":2,"from_offset":433,"to_offset":446}]}`),
		forbidden: proposeFixJSON(t, "kafka_reset_consumer_offsets", `{"group":"order-sync","topics":["pg.public.orders"],"to":"earliest"}`),
		wrongStep: proposeFixJSON(t, "pg_resnapshot_rows", `{"connector":"pg-orders-connector","table":"public.orders","key_range":{"column":"id","from":"1301","to":"1340"}}`),
		refused:   "active members",
	}}
}

// runRecovery runs a script on the room path, one run.
func runRecovery(t *testing.T, sc Scenario, script []step) RunResult {
	t.Helper()
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{Runs: 1, Runner: scriptedRunner(script), Logger: quietLogger()})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	return rep.Results[0]
}

// idles answers every scripted input (comments, steer, the executed fix)
// and one more, which ends the run.
func idles(sc Scenario) []step {
	out := make([]step, len(sc.Inputs)+1)
	for i := range out {
		out[i] = step{idle: true}
	}
	return out
}

// TC2 (scripted stand-in): a focused investigation that proposes the right
// recovery passes with the fix on its target, the step is executed, and no
// attempt is refused.
func TestRecoveryScenariosGoodRunPasses(t *testing.T) {
	for _, c := range recoveryCases(t) {
		t.Run(c.file, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + c.file + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			script := append(append([]step(nil), c.reads...), step{hypothesis: hypotheses[c.file].good}, step{proposeFix: c.fix})
			r := runRecovery(t, sc, append(script, idles(sc)...))
			if !r.Pass || r.Score != 100 {
				t.Fatalf("pass=%v score=%v failures=%v facts missing=%v misses=%v attempts=%+v", r.Pass, r.Score, r.Failures, r.FactsMissing, r.Misses, r.Attempts)
			}
			if r.RightAction == nil || !r.RightAction.OK {
				t.Errorf("right action = %+v", r.RightAction)
			}
			if r.RejectedAttempts != 0 || len(r.UnexecutedInputs) != 0 {
				t.Errorf("rejected %d, unexecuted %v", r.RejectedAttempts, r.UnexecutedInputs)
			}
		})
	}
}

// TC2: the forbidden action fails the run even though its Validate refuses
// it; a valid step of another action fails the fix (fix_targets); a wrong
// story fails the root cause.
func TestRecoveryScenariosWrongRunsFail(t *testing.T) {
	for _, c := range recoveryCases(t) {
		sc, err := LoadScenario("scenarios/" + c.file + ".yaml")
		if err != nil {
			t.Fatal(err)
		}
		base := append(append([]step(nil), c.reads...), step{hypothesis: hypotheses[c.file].good})
		t.Run(c.file+"/forbidden", func(t *testing.T) {
			r := runRecovery(t, sc, append(append(slices.Clone(base), step{proposeFix: c.forbidden}), idles(sc)...))
			if r.Pass || len(r.ForbiddenProposed) != 1 {
				t.Fatalf("pass=%v forbidden=%v failures=%v", r.Pass, r.ForbiddenProposed, r.Failures)
			}
			if len(r.Attempts) != 1 || !strings.Contains(r.Attempts[0].Error, c.refused) {
				t.Errorf("attempts = %+v, want a refusal with %q", r.Attempts, c.refused)
			}
		})
		t.Run(c.file+"/other action", func(t *testing.T) {
			r := runRecovery(t, sc, append(append(slices.Clone(base), step{proposeFix: c.wrongStep}), idles(sc)...))
			if r.Pass || !strings.Contains(strings.Join(r.Failures, "; "), "fix") {
				t.Fatalf("pass=%v failures=%v attempts=%+v", r.Pass, r.Failures, r.Attempts)
			}
		})
		t.Run(c.file+"/wrong story", func(t *testing.T) {
			script := append(append([]step(nil), c.reads...), step{hypothesis: hypotheses[c.file].wrong}, step{proposeFix: c.fix})
			if r := runRecovery(t, sc, append(script, idles(sc)...)); r.Pass || r.RootCause {
				t.Fatalf("wrong story passed: pass=%v root cause=%v", r.Pass, r.RootCause)
			}
		})
	}
}

// recoveryAction returns a scenario's action as the agent gets it (the
// recording wrapper over LiveActions) and the log the harness's executor
// writes.
func recoveryAction(t *testing.T, sc Scenario, name string) (actions.Action, *writeLog) {
	t.Helper()
	live, wl, err := liveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := (&attemptLog{}).wrap(live)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get(name)
	if !ok {
		t.Fatalf("no action %s", name)
	}
	return a, wl
}

// TC3: what the agent counts after the executed step. The wrapper keeps the
// recovery class (else the agent would not count at all), Validate reads
// the recorded stack, and Verify counts what the run executed: nothing
// before, every row or record of the range after, part of it after a
// narrower step.
func TestRecoveryVerifyOnReplay(t *testing.T) {
	cases := []struct {
		file, action, params, narrower string
		unit                           string
	}{
		{"rows-lost-after-retention", "pg_resnapshot_rows",
			`{"connector":"pg-orders-connector","table":"public.orders","key_range":{"column":"id","from":"1301","to":"1340"}}`,
			`{"connector":"pg-orders-connector","table":"public.orders","key_range":{"column":"id","from":"1301","to":"1320"}}`, "rows"},
		{"consumer-skipped-range", "kafka_replay_range",
			`{"topic":"pg.public.orders","to_topic":"pg.public.orders.reprocess","ranges":[{"partition":0,"from_offset":433,"to_offset":445},{"partition":1,"from_offset":434,"to_offset":446},{"partition":2,"from_offset":433,"to_offset":446}]}`,
			`{"topic":"pg.public.orders","to_topic":"pg.public.orders.reprocess","ranges":[{"partition":2,"from_offset":433,"to_offset":446}]}`, "records"},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + c.file + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
			a, wl := recoveryAction(t, sc, c.action)
			if got := actions.ClassOf(a); got != actions.ClassRecovery {
				t.Fatalf("class through the wrapper = %q", got)
			}
			v, ok := a.(actions.Verifier)
			if !ok {
				t.Fatal("the wrapper hides Verify")
			}
			if err := a.Validate(ctx, json.RawMessage(c.params)); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			at := time.Date(2026, 10, 22, 9, 41, 0, 0, time.UTC)
			verify := func() actions.VerifyResult {
				t.Helper()
				res, err := v.Verify(ctx, json.RawMessage(c.params), at)
				if err != nil {
					t.Fatalf("Verify: %v", err)
				}
				if res.Unit != c.unit {
					t.Fatalf("unit %q, want %q", res.Unit, c.unit)
				}
				return res
			}
			if res := verify(); res.Before != 40 || res.After != 40 {
				t.Errorf("before the step: %+v, want 40 missing of 40", res)
			}
			wl.stepRan(c.action, json.RawMessage(c.narrower))
			if res := verify(); res.Before != 40 || res.After == 0 || res.After == 40 {
				t.Errorf("after a narrower step: %+v, want part of 40 still missing", res)
			}
			wl.stepRan(c.action, json.RawMessage(c.params))
			if res := verify(); res.Before != 40 || res.After != 0 {
				t.Errorf("after the step: %+v, want 40 before and 0 after", res)
			}
		})
	}
}

// Replay limits are refusals the model can act on, never a pass: a
// time-window replay (offsets by time are not replayed) and a re-snapshot on
// a column no resources entry declares.
func TestRecoveryReplayLimits(t *testing.T) {
	sc, err := LoadScenario("scenarios/consumer-skipped-range.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	replay, _ := recoveryAction(t, sc, "kafka_replay_range")
	err = replay.Validate(ctx, json.RawMessage(`{"topic":"pg.public.orders","to_topic":"pg.public.orders.reprocess","from_time":"2026-10-22T08:51:00Z","to_time":"2026-10-22T08:52:00Z"}`))
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("time window: %v, want not supported", err)
	}
	err = replay.Validate(ctx, json.RawMessage(`{"topic":"pg.public.orders","to_topic":"pg.public.orders.reprocess","ranges":[{"partition":0,"from_offset":433,"to_offset":447}]}`))
	if err == nil || !strings.Contains(err.Error(), "not written yet") {
		t.Errorf("range past the end: %v", err)
	}
	resnap, _ := recoveryAction(t, sc, "pg_resnapshot_rows")
	err = resnap.Validate(ctx, json.RawMessage(`{"connector":"pg-orders-connector","table":"public.orders","since":{"column":"updated_at","at":"2026-10-22T08:51:00Z"}}`))
	if !errors.Is(err, errNotReplayed) {
		t.Errorf("since on updated_at: %v, want not replayed", err)
	}
	if res, err := resnap.(actions.Verifier).Verify(ctx, json.RawMessage(`{"connector":"pg-orders-connector","table":"public.orders","key_range":{"column":"id","from":"1","to":"9999"}}`), time.Now()); err != nil || res.Before != 1341 {
		t.Errorf("whole-table range: %+v %v", res, err)
	}
}
