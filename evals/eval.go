package evals

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/agent"
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
	// Runner is the agent under test (default agent.Run). Tests replace it.
	Runner agent.Runner
	// Logger receives the agent's own logs (default slog.Default()).
	Logger *slog.Logger
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
	if c.Runner == nil {
		c.Runner = agent.Run
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Evaluate runs every scenario cfg.Runs times and returns the report. All
// scenarios are validated first, so a broken scenario costs no model calls.
// If ctx ends early, the report holds the runs that finished and the error is
// ctx's.
func Evaluate(ctx context.Context, scenarios []Scenario, cfg Config) (Report, error) {
	cfg = cfg.withDefaults()
	defs, err := LiveTools()
	if err != nil {
		return Report{}, err
	}
	if err := ValidateAll(scenarios, defs); err != nil {
		return Report{}, err
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
				res, tr, err := runOne(ctx, j.sc, j.run, defs, cfg)
				if err != nil {
					errMu.Lock()
					runErr = errors.Join(runErr, err)
					errMu.Unlock()
					continue
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
	if runErr != nil {
		return rep, runErr
	}
	return rep, nil
}

// ValidateAll checks every scenario against the given tool definitions and
// the production action names.
func ValidateAll(scenarios []Scenario, defs []tools.Tool) error {
	toolInputs := ToolInputs(defs)
	areg, err := LiveActions(Resources{})
	if err != nil {
		return err
	}
	var actionNames []string
	for _, a := range areg.All() {
		actionNames = append(actionNames, a.Name())
	}
	var errs []error
	for _, sc := range scenarios {
		errs = append(errs, sc.Validate(toolInputs, actionNames))
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

// runOne runs the agent once on sc. The returned error is a harness failure
// (the run could not even start); agent failures are recorded in RunResult.
func runOne(ctx context.Context, sc Scenario, run int, defs []tools.Tool, cfg Config) (RunResult, Transcript, error) {
	rp, err := newReplayer(sc.Recordings)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	reg, err := rp.registry(defs)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	live, err := LiveActions(sc.Resources)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	attempts := &attemptLog{}
	areg, err := attempts.wrap(live)
	if err != nil {
		return RunResult{}, Transcript{}, err
	}
	rec := newRecorder()
	inbox := newScriptedInbox(sc.humanInputs())
	alert := sc.agentAlert(fmt.Sprintf("eval_%s_%d", sc.Name, run))

	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	start := time.Now()
	err = cfg.Runner(runCtx, alert, agent.Deps{
		Tools:     reg,
		Actions:   areg,
		Inbox:     inbox,
		Sink:      rec,
		Proposals: rec,
		Logger:    cfg.Logger.With("scenario", sc.Name, "run", run),
	}, cfg.Options)
	took := time.Since(start)
	// Only this run's own deadline counts as a timeout; the caller stopping
	// the whole evaluation does not.
	timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
	if timedOut && errors.Is(err, context.DeadlineExceeded) {
		err = nil // reported as TimedOut, not twice
	}
	if ctx.Err() != nil {
		return RunResult{}, Transcript{}, ctx.Err()
	}

	tr := Transcript{Events: rec.snapshot(), Misses: rp.Misses(), Attempts: attempts.snapshot(), Idles: inbox.idleCount()}
	res := Judge(sc, tr, err, timedOut)
	res.Run = run
	res.DurationMS = took.Milliseconds()
	return res, tr, nil
}
