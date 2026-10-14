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
	"os"
	"os/signal"
	"syscall"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/agent/fake"
	"piperoom/internal/config"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
)

func main() {
	var (
		pipeline  = flag.String("pipeline", "orders", "pipeline id to investigate")
		title     = flag.String("title", "orders stale", "alert title")
		fakeTools = flag.Bool("fake-tools", false, "use the built-in schema-change replay tools instead of real clients")
		model     = flag.String("model", "", "model override (default from config)")
		effort    = flag.String("effort", "", "effort override (default from config)")
		maxTurns  = flag.Int("max-turns", 0, "max turns (default 40)")
	)
	flag.Parse()

	os.Exit(exitCode(run(*pipeline, *title, *fakeTools, *model, *effort, *maxTurns), os.Stderr))
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

func run(pipeline, title string, fakeTools bool, model, effort string, maxTurns int) error {
	cfg := config.Load()

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

	reg := tools.NewRegistry()
	areg := actions.NewRegistry()
	if fakeTools {
		if err := fake.Register(reg); err != nil {
			return fmt.Errorf("investigate: register fake tools: %w", err)
		}
	} else {
		w, err := wiring.Open(ctx, cfg, awsCfg)
		if err != nil {
			return err
		}
		defer w.Close()
		if err := wiring.RegisterTools(reg, areg, w); err != nil {
			return err
		}
	}

	opts := agent.Options{Model: model, Effort: effort, MaxTurns: maxTurns}
	if opts.Model == "" {
		opts.Model = cfg.Model
	}
	if opts.Effort == "" {
		opts.Effort = cfg.Effort
	}

	inbox := newStdinInbox()
	sink := newTerminalSink(os.Stdout)
	proposals := &rejectProposals{}

	alert := agent.Alert{
		Title:      title,
		PipelineID: pipeline,
		PipelineMap: fmt.Sprintf(`pipeline %s
postgres: public.orders  ->  connect: pg-orders-connector  ->  topic: pg.public.orders  ->  consumer: order-sync  ->  warehouse: analytics.orders`, pipeline),
	}

	err = ep.Run(ctx, alert, agent.Deps{
		Tools:     reg,
		Actions:   areg,
		Inbox:     inbox,
		Sink:      sink,
		Proposals: proposals,
	}, opts)
	if err != nil {
		return fmt.Errorf("investigate: %w", err)
	}
	return nil
}
