// Command evalrun runs the agent eval scenarios against the real agent and
// model, writes a JSON report and prints a markdown table. It can also compare
// two reports, so a prompt or model change shows up as better or worse.
//
//	go run ./evals/cmd/evalrun -scenarios evals/scenarios -out report.json
//	go run ./evals/cmd/evalrun -runs 3 -label prompt-v2 -baseline base.json -out cand.json
//	go run ./evals/cmd/evalrun -compare base.json,cand.json
//	go run ./evals/cmd/evalrun -validate   # (or -check) validate scenarios, no model calls
//	go run ./evals/cmd/evalrun -validate -set heldout   # the held-out set; never tune prompts on it
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
)

// exitRegressed is the exit code when -fail-on-regression finds one, so CI can
// tell "worse" apart from "broken" (exit 1).
const exitRegressed = 2

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
	out         string
	md          string
	transcripts string
	baseline    string
	compare     string
	tolerance   float64
	failOnRegr  bool
	check       bool
	verbose     bool
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
	fs.StringVar(&f.judge, "judge", "", `how root cause and layer are judged: "terms" (deterministic) or "claude" (LLM judge, needs a -tags claude build); default: each scenario's judge field, else terms`)
	fs.StringVar(&f.judgeModel, "judge-model", "", "model for -judge claude (default: the configured model, PIPEROOM_MODEL)")
	fs.StringVar(&f.out, "out", "", "write the JSON report here")
	fs.StringVar(&f.md, "md", "", "also write the markdown table here")
	fs.StringVar(&f.transcripts, "transcripts", "", "write each run's full transcript and result as JSON into this directory")
	fs.StringVar(&f.baseline, "baseline", "", "after the run, compare against this earlier report")
	fs.StringVar(&f.compare, "compare", "", "BASE.json,CANDIDATE.json: compare two reports and exit; no model calls")
	fs.Float64Var(&f.tolerance, "tolerance", evals.DefaultScoreTolerance, "score drop per scenario that counts as a regression")
	fs.BoolVar(&f.failOnRegr, "fail-on-regression", false, "exit 2 when a comparison finds a regression")
	fs.BoolVar(&f.check, "check", false, "validate scenarios against the live tool and action definitions and exit; no model calls")
	fs.BoolVar(&f.check, "validate", false, "same as -check")
	fs.BoolVar(&f.verbose, "v", false, "show the agent's own logs")
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	return f, nil
}

func run(f flags, stdout, stderr io.Writer) (int, error) {
	if f.compare != "" {
		paths := strings.Split(f.compare, ",")
		if len(paths) != 2 {
			return 1, errors.New("-compare wants BASE.json,CANDIDATE.json")
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

	dir, err := scenarioDir(f)
	if err != nil {
		return 1, err
	}
	scenarios, err := evals.LoadScenarios(dir)
	if err != nil {
		return 1, err
	}
	if scenarios, err = filter(scenarios, f.only); err != nil {
		return 1, err
	}
	if f.check {
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
		}
		fmt.Fprintf(stdout, "%d scenario(s) OK\n", len(scenarios))
		return 0, nil
	}

	// Fail before any work rather than after the first model call; the key
	// itself is read by the SDK and never touched or logged here.
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
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

	cfg := config.Load()
	opts := agent.Options{Model: f.model, Effort: f.effort, MaxTurns: f.maxTurns}
	if opts.Model == "" {
		opts.Model = cfg.Model
	}
	if opts.Effort == "" {
		opts.Effort = cfg.Effort
	}
	var judgeClient evals.HypothesisJudge
	if f.judge == evals.JudgeClaude || (f.judge == "" && anyClaudeJudge(scenarios)) {
		if judgeClient, err = newJudge(f.judgeModel); err != nil {
			return 1, err
		}
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

	rep, runErr := evals.Evaluate(ctx, scenarios, evals.Config{
		Label:       f.label,
		Judge:       f.judge,
		JudgeClient: judgeClient,
		Options:     opts,
		Runs:        f.runs,
		Parallel:    f.parallel,
		Timeout:     f.timeout,
		Logger:      logger,
		OnRun: func(r evals.RunResult, tr evals.Transcript) {
			verdict := "PASS"
			if !r.Pass {
				verdict = "FAIL " + strings.Join(r.Failures, "; ")
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
	// A stopped or partly failed evaluation still writes what finished.
	if err := writeReport(rep, f, stdout); err != nil {
		return 1, errors.Join(runErr, err)
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
	c := evals.Compare(base, cand, f.tolerance)
	if err := c.WriteMarkdown(stdout); err != nil {
		return 1, err
	}
	if f.failOnRegr && c.Regressions > 0 {
		return exitRegressed, nil
	}
	return 0, nil
}

// anyClaudeJudge reports whether a scenario asks for the LLM judge itself.
func anyClaudeJudge(scenarios []evals.Scenario) bool {
	for _, s := range scenarios {
		if s.Judge == evals.JudgeClaude {
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

// Scenario sets. The held-out set lives in its own subdirectory, which
// LoadScenarios never reads for the default set (it does not recurse), so a
// default run can never include it.
const (
	setDefault = "default"
	setHeldout = "heldout"
)

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
