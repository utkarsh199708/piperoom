// Command evalrun runs the agent eval scenarios against the real agent and
// model, writes a JSON report and prints a markdown table. It can also compare
// two reports, so a prompt or model change shows up as better or worse.
//
//	go run ./evals/cmd/evalrun -scenarios evals/scenarios -out report.json
//	go run ./evals/cmd/evalrun -runs 3 -label prompt-v2 -baseline base.json -out cand.json
//	go run ./evals/cmd/evalrun -compare base.json cand.json   # or base.json,cand.json
//	go run ./evals/cmd/evalrun -prices prices.json -out report.json   # adds cost per run
//	go run ./evals/cmd/evalrun -agreement saved-shadow-responses/   # offline; see evals/README.md
//	go run ./evals/cmd/evalrun -validate   # (or -check) validate scenarios, no model calls
//	go run ./evals/cmd/evalrun -from-shadow run.json -o evals/scenarios/new.yaml   # scenario from a graded shadow run
//	go run ./evals/cmd/evalrun -validate -set heldout   # the held-out set; never tune prompts on it
//	go run ./evals/cmd/evalrun -judge-fill evals/judgecache/ambiguous.json -dry-run   # what a verdict-cache fill would cost; no model calls
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"piperoom/evals"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// exitRegressed is the exit code when -fail-on-regression finds one, so CI can
// tell "worse" apart from "broken" (exit 1).
const exitRegressed = 2

// exitInfra is the exit code when the model API refused the run (an empty
// credit balance, a bad key): nothing was measured, so CI reads it as an
// outage, not as a worse agent (2) or a broken tool (1).
const exitInfra = 3

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9_-]`)

type flags struct {
	scenarios   string
	set         string
	only        string
	runs        int
	parallel    int
	timeout     time.Duration
	model       string
	effort      string
	maxTurns    int
	label       string
	judge       string
	judgeModel  string
	judgeCache  string
	cacheOnly   bool
	maxCalls    int
	judgeFill   string
	dryRun      bool
	out         string
	md          string
	transcripts string
	baseline    string
	compare     string
	agreement   string
	tolerance   float64
	learned     string
	failOnRegr  bool
	check       bool
	fromShadow  string
	owners      string
	name        string
	verbose     bool
	prices      string
	commit      string
	noTap       bool
	// args are the positional arguments: the candidate of -compare a.json b.json.
	args []string
}

func main() {
	f, err := parseFlags(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		os.Exit(1) // flag already printed the error and usage
	}
	code, err := run(f, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "evalrun:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func parseFlags(args []string, stderr io.Writer) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("evalrun", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.scenarios, "scenarios", "evals/scenarios", "directory of *.yaml scenarios")
	fs.StringVar(&f.set, "set", setDefault, `scenario set: "default" (the -scenarios directory) or "heldout" (its heldout/ subdirectory: report only, never tune prompts on it)`)
	fs.StringVar(&f.only, "only", "", "comma-separated scenario names to run (default all)")
	fs.IntVar(&f.runs, "runs", 1, "runs per scenario; use 3+ when comparing, the model is not deterministic")
	fs.IntVar(&f.parallel, "parallel", 1, "runs at once")
	fs.DurationVar(&f.timeout, "timeout", 15*time.Minute, "timeout per run")
	fs.StringVar(&f.model, "model", "", "model (default PIPEROOM_MODEL / claude-opus-5-5)")
	fs.StringVar(&f.effort, "effort", "", "effort (default PIPEROOM_EFFORT / high)")
	fs.IntVar(&f.maxTurns, "max-turns", 0, "agent max turns per episode (default 40)")
	fs.StringVar(&f.label, "label", "", "name for this configuration in the report, e.g. main or prompt-v2")
	fs.StringVar(&f.judge, "judge", "", `how root cause and layer are judged: "terms" (keyword grade only), "llm" (LLM judge on every hypothesis, needs a -tags livejudge build; "claude" is its alias) or "auto" (the LLM judge only where the keyword pass is ambiguous: recorded verdicts first, then live calls up to -judge-max-calls); default: each scenario's judge field, else auto`)
	fs.StringVar(&f.judgeModel, "judge-model", "", "model for the LLM judge (default: the configured model, PIPEROOM_MODEL)")
	fs.StringVar(&f.judgeCache, "judge-cache", evals.DefaultJudgeCacheDir, "directory of recorded judge verdicts that -judge auto reads first (empty: none)")
	fs.BoolVar(&f.cacheOnly, "judge-cache-only", false, "-judge auto reads only the recorded verdicts and fails on a miss; never a live judge call (replays in CI)")
	fs.IntVar(&f.maxCalls, "judge-max-calls", evals.DefaultJudgeMaxCalls, "at most this many live judge calls per run in -judge auto, and per -judge-fill; past it an ambiguous hypothesis is reported ambiguous (unjudged) and counts as not passed (negative: no live calls)")
	fs.StringVar(&f.judgeFill, "judge-fill", "", "MANIFEST (evals/judgecache/ambiguous.json): record a live judge verdict into -judge-cache for every listed corpus text that lacks one, up to -judge-max-calls, and exit. PAID: needs the owner's approval, a -tags livejudge build and the model provider's credentials (ANTHROPIC_API_KEY, or Vertex/Bedrock per PIPEROOM_MODEL_PROVIDER); -dry-run prints the estimate only")
	fs.BoolVar(&f.dryRun, "dry-run", false, "with -judge-fill: count the missing verdicts and estimate their tokens and cost; no model calls, nothing written")
	fs.StringVar(&f.out, "out", "", "write the JSON report here")
	fs.StringVar(&f.md, "md", "", "also write the markdown table here")
	fs.StringVar(&f.transcripts, "transcripts", "", "write each run's full transcript and result as JSON into this directory")
	fs.StringVar(&f.learned, "learned", "", "private directory of learned scenarios (evals/README.md): its *.yaml join the default set, and -check also validates its pending/ subdirectory, which is never replayed; without the flag nothing learned is read")
	fs.StringVar(&f.baseline, "baseline", "", "after the run, compare against this earlier report")
	fs.StringVar(&f.compare, "compare", "", "BASE.json CANDIDATE.json (or BASE.json,CANDIDATE.json): compare two reports, with a paired run-by-run comparison, and exit; no model calls")
	fs.StringVar(&f.prices, "prices", "", "model price table (JSON, as PIPEROOM_MODEL_PRICES) for cost per run; default PIPEROOM_MODEL_PRICES")
	fs.StringVar(&f.agreement, "agreement", "", "FILE-OR-DIR of saved GET /shadow?include=data,room responses: score the agent's final hypothesis against the human label and print the agreement table; offline, never for graders before they grade (-out writes the JSON summary, -judge claude uses the LLM judge)")
	fs.Float64Var(&f.tolerance, "tolerance", evals.DefaultScoreTolerance, "score drop per scenario that counts as a regression")
	fs.BoolVar(&f.failOnRegr, "fail-on-regression", false, "exit 2 when a comparison finds a regression")
	fs.BoolVar(&f.check, "check", false, "validate scenarios against the live tool and action definitions and exit; no model calls")
	fs.BoolVar(&f.check, "validate", false, "same as -check")
	fs.StringVar(&f.fromShadow, "from-shadow", "", "build a scenario from a saved GET /api/rooms/{id}/shadow?include=room response and exit; YAML goes to stdout or -out/-o; no model calls")
	fs.StringVar(&f.owners, "owners", "", "owners.yaml for -from-shadow (default OWNERS_FILE, else dev/owners.yaml)")
	fs.StringVar(&f.name, "name", "", "scenario name for -from-shadow (default shadow-<room id>)")
	fs.StringVar(&f.out, "o", "", "same as -out")
	fs.BoolVar(&f.verbose, "v", false, "show the agent's own logs")
	fs.StringVar(&f.commit, "commit", "", "source revision to record in every result (default: the build's VCS stamp, else git rev-parse HEAD)")
	fs.BoolVar(&f.noTap, "no-tap", false, "do not put the model tap in front of the Anthropic API; results then record prompt_hash \"untapped\"")
	// Go's flag package stops at the first positional argument; parse on after
	// each one so "-compare a.json b.json -fail-on-regression" keeps its flag.
	for {
		if err := fs.Parse(args); err != nil {
			return flags{}, err
		}
		if fs.NArg() == 0 {
			return f, nil
		}
		f.args = append(f.args, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// comparePaths returns the two reports of -compare: "a.json,b.json", or
// "a.json" with b.json as the one positional argument.
func comparePaths(compare string, args []string) ([]string, error) {
	paths := strings.Split(compare, ",")
	if len(paths) == 1 && len(args) == 1 {
		paths = append(paths, args[0])
	} else if len(args) > 0 {
		return nil, fmt.Errorf("unexpected arguments: %s", strings.Join(args, " "))
	}
	if len(paths) != 2 {
		return nil, errors.New("-compare wants BASE.json CANDIDATE.json (or BASE.json,CANDIDATE.json)")
	}
	return paths, nil
}

func run(f flags, stdout, stderr io.Writer) (int, error) {
	if f.compare != "" {
		paths, err := comparePaths(f.compare, f.args)
		if err != nil {
			return 1, err
		}
		base, err := evals.LoadReport(strings.TrimSpace(paths[0]))
		if err != nil {
			return 1, err
		}
		cand, err := evals.LoadReport(strings.TrimSpace(paths[1]))
		if err != nil {
			return 1, err
		}
		return compare(base, cand, f, stdout)
	}
	if len(f.args) > 0 {
		return 1, fmt.Errorf("unexpected arguments: %s", strings.Join(f.args, " "))
	}

	if f.agreement != "" {
		return runAgreement(f, stdout)
	}

	if f.fromShadow != "" {
		return runFromShadow(f, stdout, stderr)
	}

	if f.judgeFill != "" {
		return runJudgeFill(f, stdout)
	}
	if f.dryRun {
		return 1, errors.New("-dry-run goes with -judge-fill")
	}

	dir, err := scenarioDir(f)
	if err != nil {
		return 1, err
	}
	scenarios, pending, err := loadSet(f, dir)
	if err != nil {
		return 1, err
	}
	if scenarios, err = filter(scenarios, f.only); err != nil {
		return 1, err
	}
	if f.check {
		// -check also validates learned/pending/ so a fresh export is at least
		// well-formed. Pending files are never replayed (loadSet keeps them out
		// of scenarios).
		scenarios = append(scenarios, pending...)
		defs, err := evals.LiveTools()
		if err != nil {
			return 1, err
		}
		if err := evals.ValidateAll(scenarios, defs); err != nil {
			return 1, err
		}
		for _, sc := range scenarios {
			for _, w := range sc.Warnings() {
				fmt.Fprintf(stderr, "warning: %s: %s\n", sc.Name, w)
			}
			for _, n := range sc.Notes() {
				fmt.Fprintf(stderr, "note: %s: %s\n", sc.Name, n)
			}
		}
		fmt.Fprintf(stdout, "%d scenario(s) OK\n", len(scenarios))
		return 0, nil
	}

	cfg := config.Load()
	// Fail before any work rather than after the first model call; the key
	// itself is read by the SDK and never touched or logged here. Bedrock
	// and Vertex use their own credentials.
	if p := strings.ToLower(strings.TrimSpace(cfg.ModelProvider)); (p == "" || p == agent.ProviderAnthropic) && os.Getenv("ANTHROPIC_API_KEY") == "" {
		return 1, errors.New("ANTHROPIC_API_KEY is not set (use -check to validate scenarios without it)")
	}
	var baseline *evals.Report
	if f.baseline != "" {
		b, err := evals.LoadReport(f.baseline)
		if err != nil {
			return 1, err
		}
		baseline = &b
	}

	pricesFile := f.prices
	if pricesFile == "" {
		pricesFile = cfg.ModelPrices
	}
	prices, err := agent.LoadModelPrices(pricesFile)
	if err != nil {
		return 1, err
	}
	usage := evals.NewUsageCollector(prices)
	opts := agent.Options{Model: f.model, Effort: f.effort, MaxTurns: f.maxTurns}
	if opts.Model == "" {
		opts.Model = cfg.Model
	}
	if opts.Effort == "" {
		opts.Effort = cfg.Effort
	}
	judgeClient, err := liveJudge(f, scenarios)
	if err != nil {
		return 1, err
	}
	logLevel := slog.LevelError
	if f.verbose {
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: logLevel}))

	if f.transcripts != "" {
		if err := os.MkdirAll(f.transcripts, 0o755); err != nil {
			return 1, fmt.Errorf("create transcripts dir: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The agent runs as in cmd/server: the room service starts the Endpoint
	// that wiring.NewAgentEndpoint builds. The tap goes in first, because the
	// SDK reads its base URL when the Endpoint's client is built.
	var tap *evals.ModelTap
	if !f.noTap {
		if tap, err = evals.InstallModelTap(cfg.ModelProvider); err != nil {
			return 1, err
		}
		if tap != nil {
			defer tap.Close()
		}
	}
	runner, err := evals.NewEndpointRunner(ctx, cfg, logger)
	if err != nil {
		return 1, err
	}
	commit := f.commit
	if commit == "" {
		commit = evals.SourceCommit()
	}

	rep, runErr := evals.Evaluate(ctx, scenarios, evals.Config{
		Label:          f.label,
		Runner:         runner,
		Tap:            tap,
		Commit:         commit,
		Judge:          f.judge,
		DefaultJudge:   evals.JudgeAuto,
		JudgeClient:    judgeClient,
		JudgeCache:     f.judgeCache,
		JudgeCacheOnly: f.cacheOnly,
		JudgeMaxCalls:  judgeMaxCalls(f.maxCalls),
		Options:        opts,
		Runs:           f.runs,
		Parallel:       f.parallel,
		Timeout:        f.timeout,
		Logger:         logger,
		OnRun: func(r evals.RunResult, tr evals.Transcript) {
			usage.Add(r, tr)
			verdict := "PASS"
			switch {
			case r.Infra():
				verdict = "INFRA (not scored): " + evals.RunErrorText(r.Error)
			case !r.Pass:
				verdict = "FAIL " + strings.Join(r.Failures, "; ")
				if r.Error != "" {
					// Redacted and cut to 200 characters.
					verdict += ": " + evals.RunErrorText(r.Error)
				}
			}
			fmt.Fprintf(stderr, "%s #%d: %s (score %.0f, %d tool calls, %s)\n",
				r.Scenario, r.Run, verdict, r.Score, r.ToolCalls, time.Duration(r.DurationMS)*time.Millisecond)
			for _, u := range r.UnexecutedInputs {
				fmt.Fprintf(stderr, "  scripted fix not executed: %s\n", u)
			}
			if f.transcripts != "" {
				if err := writeTranscript(f.transcripts, r, tr); err != nil {
					fmt.Fprintln(stderr, "evalrun:", err)
				}
			}
		},
	})
	usage.Apply(&rep)
	// A stopped or partly failed evaluation still writes what finished.
	if err := writeReport(rep, f, stdout); err != nil {
		return 1, errors.Join(runErr, err)
	}
	if errors.Is(runErr, evals.ErrAPIRefused) {
		// Skip -baseline: the runs it would compare were never scored.
		fmt.Fprintln(stderr, "evalrun:", runErr)
		return exitInfra, nil
	}
	if runErr != nil {
		return 1, runErr
	}
	if baseline != nil {
		fmt.Fprintln(stdout)
		return compare(*baseline, rep, f, stdout)
	}
	return 0, nil
}

func compare(base, cand evals.Report, f flags, stdout io.Writer) (int, error) {
	subset := f.only != ""
	total := 0
	if subset {
		// A -only run is a subset of the baseline: compare only what ran, or
		// every unselected scenario reads as "missing from candidate".
		// Selected scenarios stay in the candidate even with no runs, so one
		// that failed to run is still a regression.
		names := make([]string, 0, len(cand.Scenarios))
		for _, s := range cand.Scenarios {
			names = append(names, s.Name)
		}
		total = len(base.Scenarios)
		base = base.Restrict(names)
	}
	c := evals.Compare(base, cand, f.tolerance)
	if subset {
		c.Notes = append(c.Notes, fmt.Sprintf("-only run: compared %d of %d baseline scenarios; OVERALL is over the same subset", len(base.Scenarios), total))
	}
	if err := c.WriteMarkdown(stdout); err != nil {
		return 1, err
	}
	if err := evals.ComparePaired(base, cand).WriteMarkdown(stdout); err != nil {
		return 1, err
	}
	// Invalid tool calls per run growing fails like a regression, as misses
	// per run growing fails evals-live; unknown (an older baseline) never does.
	if f.failOnRegr && (c.Regressions > 0 || c.InvalidToolCalls.Grew) {
		return exitRegressed, nil
	}
	return 0, nil
}

// liveJudge builds the live LLM judge a run needs. -judge llm (or a
// scenario's judge: llm; claude is the alias) must have it, and so must -judge auto (the
// default) unless it may make no live call (-judge-cache-only, or
// -judge-max-calls 0 or less). Without it evalrun refuses to start: in a build
// without -tags livejudge every ambiguous live hypothesis would otherwise count
// as unjudged and not passed, and the run would read as a regression.
func liveJudge(f flags, scenarios []evals.Scenario) (evals.HypothesisJudge, error) {
	needed := evals.IsLLMJudge(f.judge) || (f.judge == "" && anyClaudeJudge(scenarios))
	auto := f.judge == evals.JudgeAuto || (f.judge == "" && anyAutoJudge(scenarios))
	if !needed && (!auto || f.cacheOnly || f.maxCalls <= 0) {
		return nil, nil
	}
	j, err := newJudge(f.judgeModel)
	switch {
	case err != nil && (needed || !errors.Is(err, errNoLiveJudge)):
		// A -tags livejudge build whose provider setup failed (no Vertex
		// credentials, say) reports that error, not a missing build.
		return nil, err
	case err != nil:
		return nil, fmt.Errorf("-judge auto (the default) needs the live LLM judge for ambiguous hypotheses without a recorded verdict in %s, and this build has none (%w): build with -tags claude, or pass -judge terms for the keyword grade only (-judge-cache-only fails the run on a miss; -judge-max-calls 0 reports such hypotheses ambiguous (unjudged), not passed)", orNone(f.judgeCache), err)
	}
	return j, nil
}

// judgeMaxCalls maps the flag to Config.JudgeMaxCalls, where 0 means the
// default: -judge-max-calls 0 allows no live calls.
func judgeMaxCalls(n int) int {
	if n == 0 {
		return -1
	}
	return n
}

// errNoLiveJudge is newJudge's error in a build without -tags livejudge.
var errNoLiveJudge = errors.New("the live LLM judge needs a build with -tags livejudge (or its alias -tags claude)")

func orNone(s string) string {
	if s == "" {
		return "no cache"
	}
	return s
}

// anyAutoJudge reports whether a scenario is judged in auto mode: it says so,
// or sets no mode (the evalrun default is auto).
func anyAutoJudge(scenarios []evals.Scenario) bool {
	for _, s := range scenarios {
		if s.Judge == "" || s.Judge == evals.JudgeAuto {
			return true
		}
	}
	return false
}

// anyClaudeJudge reports whether a scenario asks for the LLM judge itself
// (judge: llm, or its alias claude).
func anyClaudeJudge(scenarios []evals.Scenario) bool {
	for _, s := range scenarios {
		if evals.IsLLMJudge(s.Judge) {
			return true
		}
	}
	return false
}

func filter(scenarios []evals.Scenario, only string) ([]evals.Scenario, error) {
	if only == "" {
		return scenarios, nil
	}
	want := map[string]bool{}
	for _, n := range strings.Split(only, ",") {
		if n = strings.TrimSpace(n); n != "" {
			want[n] = true
		}
	}
	var out []evals.Scenario
	for _, s := range scenarios {
		if want[s.Name] {
			out = append(out, s)
			delete(want, s.Name)
		}
	}
	if len(want) > 0 {
		var missing []string
		for n := range want {
			missing = append(missing, n)
		}
		return nil, fmt.Errorf("unknown scenario(s): %s", strings.Join(missing, ", "))
	}
	return out, nil
}

func writeReport(rep evals.Report, f flags, stdout io.Writer) error {
	if err := rep.WriteMarkdown(stdout); err != nil {
		return err
	}
	if f.md != "" {
		if err := writeFile(f.md, rep.WriteMarkdown); err != nil {
			return err
		}
	}
	if f.out != "" {
		if err := writeFile(f.out, rep.WriteJSON); err != nil {
			return err
		}
	}
	return nil
}

func writeFile(path string, write func(io.Writer) error) (err error) {
	fh, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() {
		if cerr := fh.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", path, cerr)
		}
	}()
	return write(fh)
}

func writeNewFile(path string, write func(io.Writer) error) (err error) {
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create %s without overwriting: %w", path, err)
	}
	defer func() {
		if cerr := fh.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", path, cerr)
		}
	}()
	return write(fh)
}

func writeTranscript(dir string, r evals.RunResult, tr evals.Transcript) error {
	// Scenario names come from YAML files; keep them from escaping dir.
	name := unsafeChars.ReplaceAllString(r.Scenario, "_")
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.json", name, r.Run))
	return writeFile(path, func(w io.Writer) error {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(struct {
			Result     evals.RunResult  `json:"result"`
			Transcript evals.Transcript `json:"transcript"`
		}{r, tr}); err != nil {
			return fmt.Errorf("write transcript: %w", err)
		}
		return nil
	})
}

// runFromShadow turns one saved shadow response into a scenario file. The YAML
// goes to -out/-o when given, else to stdout (so it can be piped); notes go to
// stderr. The scenario is validated like -check before anything is written.
func runFromShadow(f flags, stdout, stderr io.Writer) (int, error) {
	fh, err := os.Open(f.fromShadow)
	if err != nil {
		return 1, fmt.Errorf("open shadow response: %w", err)
	}
	defer fh.Close()
	raw, err := io.ReadAll(io.LimitReader(fh, evals.MaxShadowInputBytes+1))
	if err != nil {
		return 1, fmt.Errorf("read shadow response: %w", err)
	}
	ownersPath := f.owners
	if ownersPath == "" {
		ownersPath = config.Load().OwnersFile
	}
	own, err := owners.Load(ownersPath)
	if err != nil {
		return 1, fmt.Errorf("load owners for -from-shadow: %w", err)
	}
	res, err := evals.ScenarioFromShadow(raw, own.Pipeline, f.name)
	if err != nil {
		return 1, err
	}
	for _, w := range res.Warnings {
		fmt.Fprintln(stderr, "evalrun: warning:", w)
	}
	if f.out == "" {
		if _, err := stdout.Write(res.YAML); err != nil {
			return 1, fmt.Errorf("write scenario: %w", err)
		}
		return 0, nil
	}
	if err := writeNewFile(f.out, func(w io.Writer) error {
		_, err := w.Write(res.YAML)
		return err
	}); err != nil {
		return 1, err
	}
	fmt.Fprintf(stderr, "evalrun: wrote scenario %q to %s (validated)\n", res.Scenario.Name, f.out)
	return 0, nil
}

// Scenario sets. The held-out set lives in its own subdirectory, which
// LoadScenarios never reads for the default set (it does not recurse), so a
// default run can never include it.
const (
	setDefault = "default"
	setHeldout = "heldout"
)

// loadSet reads the scenarios a run replays and the pending ones only -check
// validates. The default set is the scenarios directory plus, when -learned
// names a private directory, its reviewed *.yaml (L1a). learned/pending/ is
// returned apart so no run can replay it by accident. The held-out set and a
// run without -learned read nothing learned.
func loadSet(f flags, dir string) (run, pending []evals.Scenario, err error) {
	run, err = evals.LoadScenarios(dir)
	if err != nil {
		return nil, nil, err
	}
	if f.learned != "" && (f.set == "" || f.set == setDefault) {
		learned, err := evals.LoadLearned(f.learned)
		if err != nil {
			return nil, nil, err
		}
		run = append(run, learned...)
		if pending, err = evals.LoadPending(f.learned); err != nil {
			return nil, nil, err
		}
	}
	if err := uniqueNames(append(append([]evals.Scenario(nil), run...), pending...)); err != nil {
		return nil, nil, err
	}
	return run, pending, nil
}

// uniqueNames refuses two scenarios of one name, across the directories a set reads.
func uniqueNames(scs []evals.Scenario) error {
	seen := map[string]bool{}
	for _, sc := range scs {
		if seen[sc.Name] {
			return fmt.Errorf("duplicate scenario name %q", sc.Name)
		}
		seen[sc.Name] = true
	}
	return nil
}

// scenarioDir resolves -set against -scenarios.
func scenarioDir(f flags) (string, error) {
	switch f.set {
	case "", setDefault:
		return f.scenarios, nil
	case setHeldout:
		return filepath.Join(f.scenarios, setHeldout), nil
	}
	return "", fmt.Errorf("unknown -set %q (want %s or %s)", f.set, setDefault, setHeldout)
}
