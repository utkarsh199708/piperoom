// Command devctl starts, breaks and fixes the local CDC pipeline used to
// develop and test Piperoom. Run it from anywhere inside the repo.
//
//	devctl up | down | reset | status
//	devctl load [--rate 5]
//	devctl break <scenario> | fix <scenario>
//
// Scenarios: schema-change, connector-down, consumer-stuck, long-transaction.
// See dev/scenarios.md for what each one does and what it looks like.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
)

const usage = `devctl — local pipeline control

  devctl up                 start the stack, register the connector, start devconsumer
  devctl down               stop the stack
  devctl reset              remove all data and start clean
  devctl status             one-screen pipeline health
  devctl load [--rate N]    insert/update orders N per second until Ctrl+C
  devctl break <scenario>   cause a failure
  devctl fix <scenario>     apply the correct manual fix

scenarios: %s
`

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, strings.Join(scenarioNames(), ", "))
		os.Exit(2)
	}
	st, err := newStack(log)
	if err != nil {
		log.Error("devctl", "err", err)
		os.Exit(1)
	}
	if err := runCommand(ctx, st, os.Args[1], os.Args[2:]); err != nil {
		log.Error("devctl "+os.Args[1]+" failed", "err", err)
		os.Exit(1)
	}
}

func runCommand(ctx context.Context, st *stack, cmd string, args []string) error {
	switch cmd {
	case "up":
		return st.up(ctx)
	case "down":
		return st.compose(ctx, nil, "down")
	case "reset":
		return st.reset(ctx)
	case "status":
		return st.status(ctx, os.Stdout)
	case "load":
		fs := flag.NewFlagSet("load", flag.ExitOnError)
		rate := fs.Int("rate", 5, "changes per second")
		_ = fs.Parse(args)
		return st.load(ctx, *rate)
	case "break", "fix":
		if len(args) != 1 {
			return fmt.Errorf("usage: devctl %s <scenario> (%s)", cmd, strings.Join(scenarioNames(), ", "))
		}
		sc, ok := scenarios[args[0]]
		if !ok {
			return fmt.Errorf("unknown scenario %q (%s)", args[0], strings.Join(scenarioNames(), ", "))
		}
		if cmd == "break" {
			return sc.breakIt(ctx, st)
		}
		return sc.fix(ctx, st)
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func scenarioNames() []string {
	names := make([]string, 0, len(scenarios))
	for n := range scenarios {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
