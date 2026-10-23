package evals

import (
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
// tests drive these sessions directly, as TestSetupScenariosScoreInReplayMode
// does.
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
