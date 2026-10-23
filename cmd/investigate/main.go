// Command investigate runs the investigation agent loop from the terminal:
// it builds tools and actions from config.Load(), prints room events one line
// each (colored by type), reads stdin lines as driver steers, and auto-rejects
// proposals.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/agent/fake"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/netwire"
)

func main() {
	var (
		pipeline  = flag.String("pipeline", "orders", "pipeline id to investigate")
		title     = flag.String("title", "orders stale", "alert title")
		fakeTools = flag.Bool("fake-tools", false, "use the built-in schema-change replay tools instead of real clients")
		model     = flag.String("model", "", "model override (default from config)")
		effort    = flag.String("effort", "", "effort override (default from config)")
		maxTurns  = flag.Int("max-turns", 0, "max turns (default 40)")
		record    = flag.String("record", "", "write a recording of the run (events, tool calls, model, prompt hash, commit, timings) to this new directory")
		approve   = flag.Bool("approve-all", false, "DEV STACK ONLY: approve and EXECUTE every proposal. Refused unless every address is local, -confirm-dev-stack (or "+approveConfirmEnv+") is set and PIPEROOM_WRITE_MODE=on")
		confirm   = flag.Bool("confirm-dev-stack", false, "confirm that -approve-all runs against the local dev stack")
		kind      = flag.String("kind", "", "pipeline kind, cdc or batch. It overrides the pipeline's kind only for -list-tools; a run takes the pipeline's kind from the owners file, and -kind must then match it (it is the kind only when the owners file lacks the pipeline, e.g. with fake tools)")
		listOnly  = flag.Bool("list-tools", false, "print the read tools a room of -kind (default: the pipeline's kind) offers, one per line, and exit; opens no client")
	)
	flag.Parse()

	o := runOptions{
		Pipeline: *pipeline, Title: *title, FakeTools: *fakeTools, Model: *model, Effort: *effort,
		MaxTurns: *maxTurns, RecordDir: *record, ApproveAll: *approve, ConfirmDevStack: *confirm,
		Kind: *kind, ListTools: *listOnly,
	}
	os.Exit(exitCode(run(o), os.Stderr))
}

// exitCode prints err (if any) as one line to w and returns the process exit
// code. A spent room token budget is a stop, not a failure: the runner has
// already written its message through the terminal sink, so this prints one
// line and returns 0.
func exitCode(err error, w io.Writer) int {
	if err == nil {
		return 0
	}
	var be *agent.BudgetError
	if errors.As(err, &be) {
		fmt.Fprintln(w, "investigate: stopped:", truncatePrinted(sanitizeTerminal(be.Error())))
		return 0
	}
	fmt.Fprintln(w, "investigate:", truncatePrinted(sanitizeTerminal(err.Error())))
	return 1
}

// runOptions are the command-line settings of one run.
type runOptions struct {
	Pipeline, Title string
	FakeTools       bool
	Model, Effort   string
	MaxTurns        int
	RecordDir       string
	ApproveAll      bool
	ConfirmDevStack bool
	// Kind is -kind: "" (the pipeline's), cdc or batch.
	Kind string
	// ListTools is -list-tools; Stdout is where the list goes (nil: os.Stdout).
	ListTools bool
	Stdout    io.Writer
}

func run(o runOptions) (err error) {
	pipeline, title, fakeTools := o.Pipeline, o.Title, o.FakeTools
	model, effort, maxTurns := o.Model, o.Effort, o.MaxTurns
	cfg := config.Load()

	// The owners file is read first: the approve-all guard must see the
	// pipeline's connections before anything is opened.
	var own *owners.Owners
	if !fakeTools {
		own, err = owners.Load(cfg.OwnersFile)
		if err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
	}
	var pipe owners.Pipeline
	var havePipe bool
	if own != nil {
		pipe, havePipe = own.Pipeline(pipeline)
	}
	if o.ListTools {
		k, err := listKind(o.Kind, pipe, havePipe)
		if err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
		reg, err := listToolsRegistry(listOwners(own, pipe, havePipe), fakeTools)
		if err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
		out := o.Stdout
		if out == nil {
			out = os.Stdout
		}
		if err := listTools(out, reg, k); err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
		return nil
	}
	kind, err := roomKind(o.Kind, pipe, havePipe)
	if err != nil {
		return fmt.Errorf("investigate: %w", err)
	}
	if o.ApproveAll {
		if err := checkApproveAll(cfg, pipe, havePipe, o.ConfirmDevStack, os.Getenv); err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
		if err := checkApproveAllWriteMode(cfg); err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
		fmt.Fprint(os.Stderr, approveAllWarning)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The model endpoint (provider, triage model, token budget) comes from the
	// same settings as the server; a bad value stops the run before any tool opens.
	// One AWS config for the process, shared by Bedrock and MSK IAM.
	awsCfg, err := wiring.LoadAWS(ctx, cfg)
	if err != nil {
		return fmt.Errorf("investigate: %w", err)
	}
	ep, err := wiring.NewAgentEndpoint(ctx, cfg, nil, awsCfg)
	if err != nil {
		return fmt.Errorf("investigate: %w", err)
	}

	reg := tools.NewReadOnlyRegistry()
	areg := actions.NewRegistry()
	var topo topology.Builder
	if fakeTools {
		if err := fake.Register(reg); err != nil {
			return fmt.Errorf("investigate: register fake tools: %w", err)
		}
	} else {
		// Only the investigated pipeline's client set is opened: its own
		// connections: from owners.yaml, or the process-wide settings
		// (docs/design/tenancy.md). No other pipeline's credentials are read.
		// Without -approve-all nothing runs a fix, so no write client is
		// built whatever PIPEROOM_WRITE_MODE says.
		// net_probe is registered through the set's hook (netwire: wiring
		// cannot import nettools).
		sets, err := wiring.OpenStacks(ctx, cfg, awsCfg, own, wiring.StackOptions{Only: []string{pipeline}, ReadOnly: !o.ApproveAll,
			Prepare: func(w *wiring.Wired) { w.ProbeTools = netwire.Register }})
		if err != nil {
			return fmt.Errorf("investigate: %w", err)
		}
		defer sets.Close()
		set := sets.For(pipeline)
		reg, areg, topo = set.Tools, set.Actions, set.Topology
	}

	opts := agent.Options{Model: model, Effort: effort, MaxTurns: maxTurns}
	if opts.Model == "" {
		opts.Model = cfg.Model
	}
	if opts.Effort == "" {
		opts.Effort = cfg.Effort
	}

	inbox := newStdinInbox()
	var sink agent.Sink = newTerminalSink(os.Stdout)
	var agentInbox agent.Inbox = inbox
	var proposals agent.Proposals = &rejectProposals{}
	if o.ApproveAll {
		proposals = &approveProposals{acts: areg, pipe: pipe, havePipe: havePipe, inbox: inbox, out: os.Stdout, ctx: ctx}
	}
	if o.RecordDir != "" {
		commit := buildCommit()
		if commit == "" {
			commit = gitHead(ctx)
		}
		if commit == "" {
			commit = "unknown"
		}
		// The prompt follows the pipeline's source kind too (Q8g); without
		// the pipeline in owners (fake tools) it is the Postgres one.
		promptHash, herr := agent.PromptHashForSource(kind, sourceKindOf(pipe, havePipe))
		if herr != nil {
			return fmt.Errorf("investigate: %w", herr)
		}
		rec, rerr := newRecorder(o.RecordDir, sink, recordMeta{
			Pipeline: pipeline, Title: title, Model: opts.Model, Effort: opts.Effort, MaxTurns: opts.MaxTurns,
			SystemPromptSHA256: promptHash, Commit: commit, FakeTools: fakeTools, ApproveAll: o.ApproveAll,
		}, time.Now)
		if rerr != nil {
			return fmt.Errorf("investigate: %w", rerr)
		}
		defer func() {
			if cerr := rec.Close(err); cerr != nil && err == nil {
				err = fmt.Errorf("investigate: %w", cerr)
			}
		}()
		sink, agentInbox = rec, recordingInbox{Inbox: inbox, rec: rec}
	}

	alert := agent.Alert{
		Title:       title,
		PipelineID:  pipeline,
		PipelineMap: pipelineMapFor(ctx, topo, pipeline, slog.Default()),
		// The agent offers only this kind's tools (agent.ToolsForKind).
		PipelineKind: kind,
		// The teams a hypothesis may name (X4c), as the room service fills them.
		Teams: alertTeams(own, pipeline),
	}

	runCtx := ctx
	if havePipe && len(pipe.Repos) > 0 {
		// A pipeline with repos: gets the fix PR as in a room (Q7f): the
		// action reads the repos from the whole pipeline, which the room
		// service puts in the run ctx. Other pipelines keep the id-only scope.
		runCtx = tools.WithPipeline(ctx, pipe)
	}
	err = ep.Run(runCtx, alert, agent.Deps{
		Tools:     reg,
		Actions:   areg,
		Inbox:     agentInbox,
		Sink:      sink,
		Proposals: proposals,
	}, opts)
	if err != nil {
		return fmt.Errorf("investigate: %w", err)
	}
	return nil
}

// alertTeams is agent.Alert.Teams: the owners.yaml teams of the pipeline's
// tenant, ids and owned services only. nil without an owners file or teams.
func alertTeams(own *owners.Owners, pipeline string) []agent.Team {
	var out []agent.Team
	for _, t := range own.TeamsFor(pipeline) {
		out = append(out, agent.Team{ID: t.ID, Owns: t.Owns})
	}
	return out
}
