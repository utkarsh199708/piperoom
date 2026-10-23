package evals

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	evalsetup "piperoom/evals/setup"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

// defaultRunTimeout bounds one run. A healthy investigation of a recorded
// scenario takes a few minutes; this only stops a stuck one.
const defaultRunTimeout = 15 * time.Minute

// Config controls an evaluation.
type Config struct {
	// Label names this configuration in the report, e.g. "main" or "prompt-v2".
	Label string
	// Options go to the agent unchanged.
	Options agent.Options
	// Runs is how many times each scenario runs (default 1). The model is not
	// deterministic, so compare pass rates over several runs.
	Runs int
	// Parallel is how many runs execute at once (default 1).
	Parallel int
	// Timeout bounds one run (default 15 min).
	Timeout time.Duration
	// Runner is the agent under test. The room service starts it exactly as
	// it starts a production agent (room_path.go). Nil builds the production
	// one: the Endpoint of wiring.NewAgentEndpoint, the constructor
	// cmd/server uses, configured from the environment (config.Load). Tests
	// pass a scripted runner or an Endpoint over a fake model server.
	Runner agent.Runner
	// Commit is the source revision the run was built from (evalrun fills
	// it, see Provenance); it is copied into every result.
	Commit string
	// Tap, when set, is the model tap the runner's requests go through; it
	// gives every result the models used and the prompt hash (modeltap.go).
	Tap *ModelTap
	// Logger receives the agent's own logs (default slog.Default()).
	Logger *slog.Logger
	// Judge forces the judge mode for every scenario ("terms", "claude" or
	// "auto"); empty uses each scenario's own `judge` field, else
	// DefaultJudge.
	Judge string
	// DefaultJudge is the mode of a scenario that sets none (default terms;
	// evalrun sets auto).
	DefaultJudge string
	// JudgeClient is the live judge. A scenario whose judge mode is "claude"
	// needs it; "auto" calls it on a cache miss while JudgeMaxCalls lasts,
	// and without it a miss leaves the hypothesis ambiguous (unjudged).
	JudgeClient HypothesisJudge
	// JudgeCache is the directory of recorded verdicts auto mode reads first
	// (judgecache.go); empty reads none.
	JudgeCache string
	// JudgeCacheOnly makes auto mode read only the cache: a miss fails the
	// run (unit tests and CI replays), it never calls the live judge.
	JudgeCacheOnly bool
	// JudgeMaxCalls caps the live judge calls of auto mode over the whole
	// evaluation (0: DefaultJudgeMaxCalls; negative: none). Past the cap an
	// ambiguous hypothesis is unjudged and counts as not passed.
	JudgeMaxCalls int
	// SetupDir is the directory of runner-session recordings (U1-20;
	// evals/setup/recordings). A scenario with a recording gets the source
	// and target kinds it reports under when it names none, so the report's
	// per-source and per-target pass rates cover the setup scenarios; the
	// recordings are also the replay a setup run's runner needs (evals/setup).
	// Empty reads none. A directory that fails to load is logged as a warning
	// and the run goes on without it.
	SetupDir string
	// judge is the evaluation's shared judge gate (Evaluate sets it).
	judge *judgeGate
	// refusal latches the first API refusal of the evaluation (infra.go).
	refusal *refusalLatch
	// OnRun, if set, is called after each run, for progress output. Calls are
	// serialized.
	OnRun func(RunResult, Transcript)
}

func (c Config) withDefaults() Config {
	if c.Runs <= 0 {
		c.Runs = 1
	}
	if c.Parallel <= 0 {
		c.Parallel = 1
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultRunTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.judge == nil {
		c.judge = newJudgeGate(c)
	}
	if c.refusal == nil {
		c.refusal = &refusalLatch{}
	}
	return c
}

// gate returns the evaluation's judge gate, or a fresh one for a Config that
// did not go through withDefaults.
func (c Config) gate() *judgeGate {
	if c.judge != nil {
		return c.judge
	}
	return newJudgeGate(c)
}

// Evaluate runs every scenario cfg.Runs times and returns the report. All
// scenarios are validated first, so a broken scenario costs no model calls.
// If ctx ends early, the report holds the runs that finished and the error is
// ctx's.
func Evaluate(ctx context.Context, scenarios []Scenario, cfg Config) (Report, error) {
	cfg = cfg.withDefaults()
	if cfg.SetupDir != "" {
		recs, err := evalsetup.LoadDir(cfg.SetupDir)
		if err != nil {
			// A broken recording costs the setup grouping, not every other
			// scenario's run: warn and run the set without the recordings'
			// kinds. evalrun -check fails on the same error.
			cfg.Logger.Warn("evals: setup recordings not loaded; setup scenarios report without their source and target", "dir", cfg.SetupDir, "err", err)
			recs = nil
		}
		// Copy the slice: attaching the recordings' kinds must not mutate the
		// caller's scenarios.
		scenarios = append([]Scenario(nil), scenarios...)
		for i := range scenarios {
			if rec, ok := recs[scenarios[i].Name]; ok {
				attachSetup(&scenarios[i], rec)
			}
		}
	}
	if cfg.Runner == nil {
		run, err := NewEndpointRunner(ctx, config.Load(), cfg.Logger)
		if err != nil {
			return Report{}, err
		}
		cfg.Runner = run
	}
	for _, sc := range scenarios {
		for _, w := range sc.Warnings() {
			cfg.Logger.Warn("evals: scenario warning", "scenario", sc.Name, "warning", w)
		}
	}
	defs, err := LiveTools()
	if err != nil {
		return Report{}, err
	}
	if err := ValidateAll(scenarios, defs); err != nil {
		return Report{}, err
	}
	for _, m := range []string{cfg.Judge, cfg.DefaultJudge} {
		if !validJudge(m) {
			return Report{}, fmt.Errorf("evals: unknown judge %q (want %q, %q (alias %q) or %q)", m, JudgeTerms, JudgeLLM, JudgeClaude, JudgeAuto)
		}
	}
	for _, sc := range scenarios {
		if effectiveJudge(cfg.Judge, sc, cfg.DefaultJudge) == JudgeLLM && cfg.JudgeClient == nil {
			return Report{}, fmt.Errorf("evals: scenario %q asks for judge %q but no judge client is configured (build with -tags livejudge and set the model provider's credentials)", sc.Name, JudgeLLM)
		}
	}

	type job struct {
		sc  Scenario
		run int
		idx int
	}
	var jobs []job
	for _, sc := range scenarios {
		for i := 1; i <= cfg.Runs; i++ {
			jobs = append(jobs, job{sc: sc, run: i, idx: len(jobs)})
		}
	}

	started := time.Now()
	results := make([]*RunResult, len(jobs))
	var (
		wg     sync.WaitGroup
		onRun  sync.Mutex
		feed   = make(chan job)
		errMu  sync.Mutex
		runErr error
	)
	for w := 0; w < cfg.Parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range feed {
				// The feeder's select may still hand out a job after ctx
				// ends; never start a model run then.
				if ctx.Err() != nil {
					continue
				}
				var (
					res RunResult
					tr  Transcript
				)
				if s := cfg.refusal.get(); s != nil {
					// The API refused an earlier run: start no model run, and
					// do not let the rest read as failures of the agent.
					res = infraResult(j.sc.Name, j.run, "not started: the API refused an earlier run ("+s.String()+")")
				} else {
					var err error
					res, tr, err = runOne(ctx, j.sc, j.run, defs, cfg)
					if err != nil {
						status, refused := APIRefusal(err)
						if !refused {
							errMu.Lock()
							runErr = errors.Join(runErr, err)
							errMu.Unlock()
							continue
						}
						// The live judge was refused after the run.
						cfg.refusal.set(status, err)
						res = infraResult(j.sc.Name, j.run, fmt.Sprintf("HTTP %d: %s", status, err))
					}
				}
				results[j.idx] = &res
				if cfg.OnRun != nil {
					onRun.Lock()
					cfg.OnRun(res, tr)
					onRun.Unlock()
				}
			}
		}()
	}
feeding:
	for _, j := range jobs {
		select {
		case feed <- j:
		case <-ctx.Done():
			break feeding
		}
	}
	close(feed)
	wg.Wait()

	var done []RunResult
	for _, r := range results {
		if r != nil {
			done = append(done, *r)
		}
	}
	rep := buildReport(cfg, scenarios, done, started)
	if err := ctx.Err(); err != nil {
		return rep, fmt.Errorf("evals: evaluation stopped: %w", err)
	}
	if s := cfg.refusal.get(); s != nil {
		rep.InfraStop = s.String()
		return rep, fmt.Errorf("%w (%s): the affected and remaining runs are infra, not scored", ErrAPIRefused, s)
	}
	if runErr != nil {
		return rep, runErr
	}
	return rep, nil
}

// ValidateAll checks every scenario against the given tool definitions and
// the production action names.
func ValidateAll(scenarios []Scenario, defs []tools.Tool) error {
	toolInputs := ToolInputs(defs)
	areg, err := LiveActions(Resources{}, nil)
	if err != nil {
		return err
	}
	var actionNames []string
	for _, a := range areg.All() {
		actionNames = append(actionNames, a.Name())
	}
	var errs []error
	seen := map[string]bool{}
	for _, sc := range scenarios {
		// Two scenarios of one name would share one report row and count
		// every run twice; LoadScenarios refuses them, other callers too.
		if seen[sc.Name] {
			errs = append(errs, fmt.Errorf("evals: duplicate scenario name %q", sc.Name))
		}
		seen[sc.Name] = true
		errs = append(errs, sc.Validate(toolInputs, actionNames))
		if fe := sc.checkFixTargets(areg); len(fe) > 0 {
			errs = append(errs, fmt.Errorf("evals: scenario %q: %w", sc.Name, errors.Join(fe...)))
		}
		// Malformed connect_status data would only surface mid-run otherwise.
		if _, err := connectStatuses(sc.Recordings); err != nil {
			errs = append(errs, fmt.Errorf("evals: scenario %q: %w", sc.Name, err))
		}
	}
	return errors.Join(errs...)
}

// ToolInputs maps each tool name to the property names of its input schema.
func ToolInputs(defs []tools.Tool) map[string][]string {
	out := make(map[string][]string, len(defs))
	for _, d := range defs {
		props, _ := d.InputSchema()["properties"].(map[string]any)
		fields := make([]string, 0, len(props))
		for k := range props {
			fields = append(fields, k)
		}
		out[d.Name()] = fields
	}
	return out
}
