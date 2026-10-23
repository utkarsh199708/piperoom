package evals

import (
	"context"
	"errors"
	"testing"

	evalsetup "piperoom/evals/setup"
	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// setupScenarioNames are the six U1-19 scenarios U1-20 scores in replay mode.
var setupScenarioNames = []string{
	"setup/compose-postgres-clean",
	"setup/compose-privileged-refused",
	"setup/flat-plugin-dir",
	"setup/wal-level-replica",
	"setup/missing-grant",
	"setup/db-unreachable-from-connect",
}

// TestSetupScenariosScoreInReplayMode is the U1-20 done-when for setup runs:
// each of the six U1-19 scenarios has a recorded runner session, the session
// replays every op it records, its verification ladder recomputes the
// recorded setup.verified from its own reads, and the scenario itself scores
// 100 in the harness with no model. No real runner, no Docker, no network.
func TestSetupScenariosScoreInReplayMode(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	recs, err := evalsetup.LoadDir("setup/recordings")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := LoadSetupReplays("setup/recordings")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Scenario{}
	for _, sc := range scs {
		byName[sc.Name] = sc
	}

	ctx := context.Background()
	attached := make([]Scenario, 0, len(setupScenarioNames))
	var results []RunResult
	for _, name := range setupScenarioNames {
		sc, ok := byName[name]
		if !ok {
			t.Fatalf("%s: no scenario file", name)
		}
		rec, ok := recs[name]
		if !ok {
			t.Fatalf("%s: no runner recording", name)
		}
		t.Run(name, func(t *testing.T) {
			// The recorded op session replays without a runner: every op it
			// records answers, with its receipt, and nothing is left over.
			sess, ok := sessions[name]
			if !ok {
				t.Fatalf("no replay from LoadSetupReplays")
			}
			for i, op := range rec.Ops {
				res, err := sess.Execute(ctx, "r_eval", op.Request())
				if op.Refusal != nil {
					var refused *protocol.RefusedError
					if !errors.As(err, &refused) {
						t.Fatalf("ops[%d] %s: err = %v, want a refusal", i, op.Op, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("ops[%d] %s: %v", i, op.Op, err)
				}
				if res.OK != op.Result.OK || res.Receipt.OutMAC != op.Result.Receipt.OutMAC {
					t.Errorf("ops[%d] %s: result = %+v, want the recorded outcome and receipt", i, op.Op, res)
				}
			}
			if left := sess.Remaining(); left != 0 {
				t.Errorf("%d recorded op(s) never replayed: %v", left, sess.Unexecuted())
			}

			// The ladder recomputes the recorded verification from the reads,
			// and the only setup.verified on the server side is its own.
			agentSink := &evalsetup.MemorySink{}
			if err := evalsetup.AgentSink(agentSink).Emit(ctx, evalsetup.Event{Type: events.SetupVerified}); !errors.Is(err, evalsetup.ErrAgentVerified) {
				t.Fatalf("agent setup.verified: err = %v, want a refusal", err)
			}
			sink := &evalsetup.MemorySink{}
			ladder := evalsetup.NewFakeLadder(evalsetup.LadderConfig{
				Source:     rec.SourceKind(),
				Target:     rec.Target,
				Sink:       rec.SinkKind(),
				PlanID:     rec.Ladder.PlanID,
				NonceHash:  rec.Ladder.NonceHash,
				Table:      rec.Ladder.Table,
				ExampleSQL: rec.Ladder.ExampleSQL,
			}, evalsetup.NewRecordingReader(rec), sink)
			verified, err := ladder.Verify(ctx)
			if err != nil {
				t.Fatalf("ladder: %v", err)
			}
			if rec.Ladder.Verified != nil {
				want := rec.Ladder.Verified.Rungs
				if len(verified.Rungs) != len(want) {
					t.Fatalf("rungs = %d, want %d", len(verified.Rungs), len(want))
				}
				for i := range want {
					if verified.Rungs[i].Name != want[i].Name || verified.Rungs[i].OK != want[i].OK || verified.Rungs[i].Note != want[i].Note {
						t.Errorf("rung %d = %+v, want %+v", i, verified.Rungs[i], want[i])
					}
				}
			}
			for _, ev := range sink.Events() {
				if ev.Type == events.SetupVerified && ev.Actor.Kind != events.ActorSystem {
					t.Errorf("setup.verified authored by %+v, want the ladder runner", ev.Actor)
				}
			}

			// The scenario itself scores 100 through the room path with the
			// replay's source and target attached by Config.SetupDir.
			rep, err := Evaluate(ctx, []Scenario{sc}, Config{
				Runner:   scriptedRunner(canPassScript(t, sc, nil)),
				Logger:   quietLogger(),
				SetupDir: "setup/recordings",
			})
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if len(rep.Results) != 1 {
				t.Fatalf("results = %d", len(rep.Results))
			}
			r := rep.Results[0]
			if !r.Pass || r.Score != 100 || len(r.Misses) != 0 {
				t.Errorf("pass=%v score=%v misses=%v failures=%v", r.Pass, r.Score, r.Misses, r.Failures)
			}
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].SourceKind != "postgres" || rep.Scenarios[0].Target != "compose" {
				t.Errorf("scenario kinds = %+v, want postgres/compose from the recording", rep.Scenarios)
			}
			attachSetup(&sc, rec)
			attached = append(attached, sc)
			results = append(results, r)
		})
	}

	// The six run as one report: every target and source group is green, and
	// the existing overall fields still cover the same runs.
	rep := buildReport(Config{Label: "setup-replay", Runs: 1}, attached, results, timeZero())
	if len(rep.ByTarget) != 1 || rep.ByTarget[0].Kind != "compose" || rep.ByTarget[0].Runs != 6 || rep.ByTarget[0].Passes != 6 {
		t.Errorf("by target = %+v, want compose 6/6", rep.ByTarget)
	}
	if len(rep.BySource) != 1 || rep.BySource[0].Kind != "postgres" || rep.BySource[0].Runs != 6 || rep.BySource[0].Passes != 6 {
		t.Errorf("by source = %+v, want postgres 6/6", rep.BySource)
	}
	if rep.Overall.Runs != 6 || rep.Overall.Passes != 6 || rep.Overall.PassRate != 1 {
		t.Errorf("overall = %+v, want the unchanged 6/6 fields", rep.Overall)
	}
}
