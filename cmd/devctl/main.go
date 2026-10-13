// Command devctl starts, breaks and fixes the local CDC pipeline used to
// develop and test Piperoom. Run it from anywhere inside the repo.
//
//	devctl up | down | reset | status
//	devctl health                     read-only; exit 1 if a layer is unhealthy
//	devctl load [--rate 5]
//	devctl break <scenario> | fix <scenario>
//	devctl demo [--fix] <scenario>
//	devctl identities unbind <user>   move a user to a new IdP account
//
// Scenarios: schema-change, connector-down, consumer-stuck, long-transaction,
// slot-inactive, consumer-lag, connector-auth.
// See dev/scenarios.md for what each one does and what it looks like.
package main

import (
	"context"
	"errors"
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
  devctl status             one-screen pipeline health (for humans)
  devctl health             read-only check per layer; exit 1 unhealthy, 2 setup error
  devctl load [--rate N]    insert/update orders N per second until Ctrl+C
  devctl break <scenario>   cause a failure
  devctl fix <scenario>     apply the correct manual fix
  devctl demo [--fix] <scenario>   one-command live demo (locks, resets, breaks)
  devctl lock --owner <tag> take the shared stack lock
  devctl unlock --owner <tag>  release the shared stack lock
  devctl lock status        show who holds the lock
  devctl identities unbind <user>   delete the user's IdP binding (room database)
  devctl lock --steal       steal a stale (>15m) lock

up/down/reset/load/break/fix mutate the shared stack and require --owner <tag>
(or PIPEROOM_STACK_OWNER) to match the current lock holder.

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
		os.Exit(exitCode(os.Args[1]))
	}
	if err := runCommand(ctx, st, os.Args[1], os.Args[2:]); err != nil {
		if errors.Is(err, errUnhealthy) {
			// The report already names the layers; exit code is the answer.
			os.Exit(1)
		}
		log.Error("devctl "+os.Args[1]+" failed", "err", err)
		os.Exit(exitCode(os.Args[1]))
	}
}

// exitCode is 2 for a failed `health` run (setup: not in the repo, owners.yaml
// unreadable, bad arguments) so it is never confused with "unhealthy" (1). Other
// commands keep 1.
func exitCode(cmd string) int {
	if cmd == "health" {
		return 2
	}
	return 1
}

func runCommand(ctx context.Context, st *stack, cmd string, args []string) error {
	switch cmd {
	case "up":
		owner, rest, err := splitOwner(args)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return fmt.Errorf("usage: devctl up")
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		return st.up(ctx)
	case "down":
		owner, rest, err := splitOwner(args)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return fmt.Errorf("usage: devctl down")
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		return st.compose(ctx, nil, "down")
	case "reset":
		owner, rest, err := splitOwner(args)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return fmt.Errorf("usage: devctl reset")
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		return st.reset(ctx)
	case "status":
		return st.status(ctx, os.Stdout)
	case "health":
		if len(args) != 0 {
			return fmt.Errorf("usage: devctl health")
		}
		return st.health(ctx, os.Stdout)
	case "load":
		owner, rest, err := splitOwner(args)
		if err != nil {
			return err
		}
		fs := flag.NewFlagSet("load", flag.ContinueOnError)
		fs.SetOutput(os.Stderr)
		rate := fs.Int("rate", 5, "changes per second")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return fmt.Errorf("usage: devctl load [--rate N] [--owner <tag>]")
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		return st.load(ctx, *rate)
	case "lock":
		return lockCmd(args)
	case "unlock":
		return unlockCmd(args)
	case "break", "fix":
		owner, rest, err := splitOwner(args)
		if err != nil {
			return err
		}
		if len(rest) != 1 {
			return fmt.Errorf("usage: devctl %s <scenario> (%s)", cmd, strings.Join(scenarioNames(), ", "))
		}
		sc, ok := scenarios[rest[0]]
		if !ok {
			return fmt.Errorf("unknown scenario %q (%s)", rest[0], strings.Join(scenarioNames(), ", "))
		}
		if err := requireLock(owner); err != nil {
			return err
		}
		if cmd == "break" {
			return sc.breakIt(ctx, st)
		}
		return sc.fix(ctx, st)
	case "demo":
		return demoCmd(ctx, st, args)
	case "identities":
		return identitiesCmd(ctx, st, args, os.Stdout)
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
