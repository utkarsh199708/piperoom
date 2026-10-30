package evals

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	evalsetup "piperoom/evals/setup"
)

// LoadSetupReplays loads the runner sessions under dir (U1-20,
// evals/setup/recordings) as fresh replays, keyed by scenario name. A setup
// run uses its scenario's Session as the paired runner's protocol
// RunnerExecutor; no real runner is started. Each call returns new sessions,
// so every run starts from the file's first op.
//
// Setup mode's room path (the agent's setup stack and the server's ladder
// runner) lands separately (U1-10, U1-8b); until it does, the harness and its
// tests drive these sessions directly. The scoring replay (setupReplay) is
// what every setup run already checks: the recorded session and ladder must
// reproduce themselves, or the scenario fails.
func LoadSetupReplays(dir string) (map[string]*evalsetup.Session, error) {
	recs, err := evalsetup.LoadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*evalsetup.Session, len(recs))
	for name, rec := range recs {
		out[name] = evalsetup.NewSession(*rec)
	}
	return out, nil
}

// SetupReplayResult is the part of a RunResult that the recorded setup
// session and ladder add (U1-20). Nil on a scenario without a setup
// recording (every incident scenario).
type SetupReplayResult struct {
	// Recording names the scenario the recording belongs to.
	Recording string `json:"recording,omitempty"`
	// Ops and Rungs are how many recorded ops and ladder rungs the replay
	// used. Both 0 when the recording is missing.
	Ops   int `json:"ops"`
	Rungs int `json:"rungs"`
	// NoLadder says why the session replayed no ladder: plan.Ladder defines
	// none for its target (evalsetup.PlanLadderUndefined; Rungs is 0).
	NoLadder string `json:"no_ladder,omitempty"`
	// Failures say why the recording did not reproduce its own session: a
	// missing file, an op a live-shaped request could not answer, a ladder
	// whose recomputed verdict differs from the recorded one. Non-empty
	// failures fail the run and take its score to 0.
	Failures []string `json:"failures,omitempty"`
}

// setupReplay replays sc's recorded runner session and ladder with
// live-shaped requests (evalsetup.Verify) for the scoring path. A setup
// scenario without a recording returns a result that fails with the reason
// Evaluate attached; a scenario that is not a setup scenario returns nil.
//
// The recording was made by a real (or, for the hand-composed U1-19 files,
// modelled) session, so a broken recording is a harness failure, not an agent
// failure. It must still fail the scenario: a green setup score on a session
// that cannot be replayed would hide the break.
func setupReplay(sc Scenario) *SetupReplayResult {
	if sc.setupRec == nil && sc.setupErr == "" {
		if !sc.isSetupScenario() {
			return nil
		}
		return &SetupReplayResult{Recording: sc.Name, Failures: []string{"no runner-session recording"}}
	}
	if sc.setupRec == nil {
		return &SetupReplayResult{Recording: sc.Name, Failures: []string{sc.setupErr}}
	}
	chk := evalsetup.Verify(context.Background(), sc.setupRec)
	return &SetupReplayResult{
		Recording: sc.setupRec.Scenario,
		Ops:       chk.Ops,
		Rungs:     chk.Rungs,
		NoLadder:  chk.NoLadder,
		Failures:  chk.Failures,
	}
}

// VerifySetupReplays loads dir and replays every recording as a live setup
// run would use it (evalsetup.Verify). evalrun -check calls it so a broken
// session or a ladder that does not reach its recorded verdict fails before it
// costs a run.
func VerifySetupReplays(dir string) error {
	recs, err := evalsetup.LoadDir(dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(recs)) {
		chk := evalsetup.Verify(context.Background(), recs[name])
		if chk.OK() {
			continue
		}
		errs = append(errs, fmt.Errorf("evals: setup recording %s: %s", name, strings.Join(chk.Failures, "; ")))
	}
	return errors.Join(errs...)
}
