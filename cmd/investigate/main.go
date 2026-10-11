// Command investigate runs the investigation agent loop from the terminal:
// it builds tools and actions from config.Load(), prints room events one line
// each (colored by type), reads stdin lines as driver steers, and auto-rejects
// proposals.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/agent/fake"
	"piperoom/internal/config"
	"piperoom/internal/tools"
)

func main() {
	var (
		pipeline  = flag.String("pipeline", "orders", "pipeline id to investigate")
		title     = flag.String("title", "orders stale", "alert title")
		fakeTools = flag.Bool("fake-tools", true, "use the built-in schema-change replay tools instead of real clients")
		model     = flag.String("model", "", "model override (default from config)")
		effort    = flag.String("effort", "", "effort override (default from config)")
		maxTurns  = flag.Int("max-turns", 0, "max turns (default 40)")
	)
	flag.Parse()

	if err := run(*pipeline, *title, *fakeTools, *model, *effort, *maxTurns); err != nil {
		fmt.Fprintln(os.Stderr, "investigate:", err)
		os.Exit(1)
	}
}

func run(pipeline, title string, fakeTools bool, model, effort string, maxTurns int) error {
	cfg := config.Load()

	reg := tools.NewRegistry()
	areg := actions.NewRegistry()
	if fakeTools {
		if err := fake.Register(reg); err != nil {
			return fmt.Errorf("investigate: register fake tools: %w", err)
		}
	} else {
		if err := wireRealTools(cfg, reg, areg); err != nil {
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	inbox := newStdinInbox()
	sink := newTerminalSink(os.Stdout)
	proposals := &rejectProposals{}

	alert := agent.Alert{
		Title:      title,
		PipelineID: pipeline,
		PipelineMap: fmt.Sprintf(`pipeline %s
postgres: public.orders  ->  connect: pg-orders-connector  ->  topic: pg.public.orders  ->  consumer: order-sync  ->  warehouse: analytics.orders`, pipeline),
	}

	err := agent.Run(ctx, alert, agent.Deps{
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
