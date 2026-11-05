package evals

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	evalsetup "piperoom/evals/setup"
	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// setupScenarioNames are the six U1-19 scenarios U1-20 scores in replay mode.
// Every other session in setup/recordings (another source's setup scenarios)
// is scored the same way after them, without a list to keep.
var setupScenarioNames = []string{
	"setup/compose-postgres-clean",
	"setup/compose-privileged-refused",
	"setup/flat-plugin-dir",
	"setup/wal-level-replica",
	"setup/missing-grant",
	"setup/db-unreachable-from-connect",
}

// TestSetupScenariosScoreInReplayMode is the U1-20 done-when for setup runs:
// each of the six U1-19 scenarios, and every other scenario with a recorded
// runner session, has its session replay every op it records, its
// verification ladder recompute the recorded setup.verified from its own
// reads, and the scenario itself score 100 in the harness with no model. No
// real runner, no Docker, no network.
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

	// The six U1-19 scenarios first, then every other recorded session by
	// name.
	names := append([]string(nil), setupScenarioNames...)
	var others []string
	for name := range recs {
		if !slices.Contains(names, name) {
			others = append(others, name)
		}
	}
	sort.Strings(others)
	names = append(names, others...)
	wantTarget, wantSource := map[string]int{}, map[string]int{}

	ctx := context.Background()
	attached := make([]Scenario, 0, len(names))
	var results []RunResult
	for _, name := range names {
		sc, ok := byName[name]
		if !ok {
			t.Fatalf("%s: no scenario file", name)
		}
		rec, ok := recs[name]
		if !ok {
			t.Fatalf("%s: no runner recording", name)
		}
		wantTarget[rec.Target]++
		wantSource[rec.SourceKind()]++
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
			// A target plan.Ladder defines no ladder for (dbzserver, U11c)
			// records none; every other session records one.
			noLadder := evalsetup.PlanLadderUndefined(*rec)
			wantRungs := 0
			switch {
			case rec.Ladder == nil && noLadder == "":
				t.Fatalf("no ladder recorded, and plan.Ladder defines one for %s/%s", rec.SourceKind(), rec.Target)
			case rec.Ladder != nil && noLadder != "":
				t.Fatalf("a ladder is recorded, but %s", noLadder)
			case rec.Ladder != nil:
				wantRungs = len(rec.Ladder.Verified.Rungs)
				replayLadder(t, ctx, rec)
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
			// The recording is part of the score: the run replayed its ops and
			// ladder and found nothing to fail.
			if r.SetupReplay == nil || len(r.SetupReplay.Failures) != 0 {
				t.Errorf("setup replay = %+v, want a clean replay", r.SetupReplay)
			}
			if r.SetupReplay != nil && (r.SetupReplay.Ops != len(rec.Ops) || r.SetupReplay.Rungs != wantRungs || r.SetupReplay.NoLadder != noLadder) {
				t.Errorf("setup replay ops/rungs/no-ladder = %d/%d/%q, want %d/%d/%q", r.SetupReplay.Ops, r.SetupReplay.Rungs, r.SetupReplay.NoLadder, len(rec.Ops), wantRungs, noLadder)
			}
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].SourceKind != rec.SourceKind() || rep.Scenarios[0].Target != rec.Target {
				t.Errorf("scenario kinds = %+v, want %s/%s from the recording", rep.Scenarios, rec.SourceKind(), rec.Target)
			}
			attachSetup(&sc, rec)
			attached = append(attached, sc)
			results = append(results, r)
		})
	}

	// They run as one report: every target and source group is green with
	// the recordings' counts (the six U1-19 sessions are postgres on
	// compose), and the existing overall fields still cover the same runs.
	if wantSource["postgres"] < len(setupScenarioNames) {
		t.Errorf("postgres sessions = %d, want at least the %d U1-19 ones", wantSource["postgres"], len(setupScenarioNames))
	}
	rep := buildReport(Config{Label: "setup-replay", Runs: 1}, attached, results, timeZero())
	checkGroups := func(by string, got []Breakdown, want map[string]int) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("by %s = %+v, want the groups %v", by, got, want)
		}
		for _, g := range got {
			if n := want[g.Kind]; g.Runs != n || g.Passes != n {
				t.Errorf("by %s %s = %d/%d, want %d/%d", by, g.Kind, g.Passes, g.Runs, n, n)
			}
		}
	}
	checkGroups("target", rep.ByTarget, wantTarget)
	checkGroups("source", rep.BySource, wantSource)
	if n := len(names); rep.Overall.Runs != n || rep.Overall.Passes != n || rep.Overall.PassRate != 1 {
		t.Errorf("overall = %+v, want %d/%d", rep.Overall, n, n)
	}
}

// replayLadder runs the fake ladder over rec's reads: it must recompute the
// recorded setup.verified, and the only setup.verified on the server side is
// its own.
func replayLadder(t *testing.T, ctx context.Context, rec *evalsetup.Recording) {
	t.Helper()
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
}

// TestEvaluateRunsWithABrokenSetupDir: a malformed setup recording costs only
// the setup scenarios' runs. An incident scenario still runs, and the load
// error is logged as a warning rather than swallowed.
func TestEvaluateRunsWithABrokenSetupDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte(`{"scenario":"x","ops":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	rep, err := Evaluate(context.Background(), []Scenario{loadSeed()}, Config{
		Runner:   scriptedRunner(goodSchemaChangeScript()),
		Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
		SetupDir: dir,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(rep.Results) != 1 || !rep.Results[0].Pass {
		t.Errorf("results = %+v, want the scenario's run to pass", rep.Results)
	}
	if s := logs.String(); !strings.Contains(s, "level=WARN") || !strings.Contains(s, "ops is empty") {
		t.Errorf("logs = %q, want a warning naming the broken recording", s)
	}
}

// setupScenario loads one of the six U1-19 scenarios by name.
func setupScenario(t *testing.T, name string) Scenario {
	t.Helper()
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	for _, sc := range scs {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("%s: no scenario file", name)
	return Scenario{}
}

// writeSetupRecording writes rec as a recording file under dir.
func writeSetupRecording(t *testing.T, dir string, rec evalsetup.Recording) {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	name := strings.ReplaceAll(rec.Scenario, "/", "-") + ".json"
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// copySetupRecordings copies the named recordings from setup/recordings into
// dir. A name that is not copied is, to the run, a missing recording.
func copySetupRecordings(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join("setup", "recordings", strings.ReplaceAll(name, "/", "-")+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, strings.ReplaceAll(name, "/", "-")+".json"), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestEvaluateFailsASetupScenarioWithABrokenLadder is the U1-20 follow-up's
// blocking property: the recorded setup session and ladder are part of the
// scenario's score. A ladder that recomputes a different verdict than the one
// it records fails the run (score 0), it does not pass on the hypothesis alone.
func TestEvaluateFailsASetupScenarioWithABrokenLadder(t *testing.T) {
	const name = "setup/flat-plugin-dir"
	rec, err := evalsetup.Load(filepath.Join("setup", "recordings", "setup-flat-plugin-dir.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The reads still say the connector is RUNNING; the recording claims the
	// rung flipped red. The replay must not reach that verdict.
	rec.Ladder.Verified.Rungs[0].OK = false
	dir := t.TempDir()
	writeSetupRecording(t, dir, rec)

	sc := setupScenario(t, name)
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner:   scriptedRunner(canPassScript(t, sc, nil)),
		Logger:   quietLogger(),
		SetupDir: dir,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	r := rep.Results[0]
	if r.Pass || r.Score != 0 {
		t.Errorf("pass=%v score=%v, want the broken recording to fail the scenario", r.Pass, r.Score)
	}
	if !strings.Contains(strings.Join(r.Failures, "; "), "setup replay:") || !strings.Contains(strings.Join(r.Failures, "; "), "rung 0") {
		t.Errorf("failures = %v, want a setup replay failure naming the rung", r.Failures)
	}
	if r.SetupReplay == nil || r.SetupReplay.Ops != len(rec.Ops) {
		t.Errorf("setup replay = %+v, want the session replayed before the ladder failed", r.SetupReplay)
	}
}

// TestEvaluateFailsASetupScenarioWithoutARecording: a setup scenario is never
// scored without its recording. A directory that holds other scenarios'
// recordings leaves this one without one, and the run says so.
func TestEvaluateFailsASetupScenarioWithoutARecording(t *testing.T) {
	const name = "setup/flat-plugin-dir"
	dir := t.TempDir()
	copySetupRecordings(t, dir, "setup/missing-grant")

	sc := setupScenario(t, name)
	rep, err := Evaluate(context.Background(), []Scenario{sc}, Config{
		Runner:   scriptedRunner(canPassScript(t, sc, nil)),
		Logger:   quietLogger(),
		SetupDir: dir,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	r := rep.Results[0]
	if r.Pass || r.Score != 0 {
		t.Errorf("pass=%v score=%v, want a missing recording to fail the scenario", r.Pass, r.Score)
	}
	if !strings.Contains(strings.Join(r.Failures, "; "), "setup replay: no runner-session recording for "+name) {
		t.Errorf("failures = %v, want the missing recording named", r.Failures)
	}
}

// TestEvaluateFailsSetupScenariosWhenRecordingsDoNotLoad: one malformed file
// makes LoadDir fail. The setup scenarios then fail with the load error
// instead of running ungrouped and unverified; an incident scenario in the
// same set still runs.
func TestEvaluateFailsSetupScenariosWhenRecordingsDoNotLoad(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte(`{"scenario":"x","ops":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	sc := setupScenario(t, "setup/missing-grant")
	rep, err := Evaluate(context.Background(), []Scenario{sc, loadSeed()}, Config{
		Runner:   scriptedRunner(goodSchemaChangeScript()),
		Logger:   quietLogger(),
		SetupDir: dir,
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	byName := map[string]RunResult{}
	for _, r := range rep.Results {
		byName[r.Scenario] = r
	}
	setup := byName["setup/missing-grant"]
	if setup.Pass || setup.Score != 0 || !strings.Contains(strings.Join(setup.Failures, "; "), "setup replay: the recordings did not load") {
		t.Errorf("setup result = %+v, want a load failure", setup)
	}
	if seed := byName["schema-change"]; !seed.Pass {
		t.Errorf("incident scenario = %+v, want it to run", seed)
	}
}

// TestVerifySetupReplays is the -check path: good recordings pass, a ladder
// that does not reach its recorded verdict fails with the recording named.
func TestVerifySetupReplays(t *testing.T) {
	if err := VerifySetupReplays(filepath.Join("setup", "recordings")); err != nil {
		t.Fatalf("good recordings: %v", err)
	}
	rec, err := evalsetup.Load(filepath.Join("setup", "recordings", "setup-wal-level-replica.json"))
	if err != nil {
		t.Fatal(err)
	}
	rec.Ladder.Verified.Rungs[0].OK = false
	dir := t.TempDir()
	writeSetupRecording(t, dir, rec)
	err = VerifySetupReplays(dir)
	if err == nil || !strings.Contains(err.Error(), "setup/wal-level-replica") || !strings.Contains(err.Error(), "rung 0") {
		t.Fatalf("err = %v, want the broken recording named with its rung", err)
	}
}
