package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// demoCmd implements `devctl demo [--fix] <scenario>`. It is a one-command live
// demo: it takes the shared stack lock (owner a1 by default), resets to a clean
// seeded state, loads a short burst of traffic so the healthy pipeline is
// visible, breaks the scenario, and prints what to watch. `--fix` repairs the
// scenario and releases the lock.
func demoCmd(ctx context.Context, s *stack, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fix := fs.Bool("fix", false, "repair the scenario and release the stack lock")
	owner := fs.String("owner", "a1", "lock owner tag")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("usage: devctl demo [--fix] <scenario> (%s)", strings.Join(scenarioNames(), ", "))
	}
	sc, ok := scenarios[rest[0]]
	if !ok {
		return fmt.Errorf("unknown scenario %q (%s)", rest[0], strings.Join(scenarioNames(), ", "))
	}

	if *fix {
		if err := requireLock(*owner); err != nil {
			return err
		}
		if err := sc.fix(ctx, s); err != nil {
			return err
		}
		if err := releaseLock(*owner); err != nil {
			return err
		}
		fmt.Printf("repaired %q and released the stack lock\n", rest[0])
		return nil
	}

	// Take the lock; release it again on any failure, but keep it on success so
	// the caller can watch the live break and run `devctl demo --fix`.
	if err := acquireLock(*owner); err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			_ = releaseLock(*owner)
		}
	}()

	s.log.Info("demo: reset to a clean seeded state", "scenario", rest[0])
	if err := s.reset(ctx); err != nil {
		return err
	}
	s.log.Info("demo: loading a short burst of traffic")
	loadCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	err := s.load(loadCtx, 5)
	cancel()
	if err != nil {
		return err
	}
	s.log.Info("demo: breaking", "scenario", rest[0])
	if err := sc.breakIt(ctx, s); err != nil {
		return err
	}
	keep = true

	fmt.Printf(`
Demo ready — %[1]s is broken. Watch it with:

  devctl status                      one-screen pipeline health
  devctl load --rate 20              add traffic and watch the symptoms deepen

To repair and release the lock:

  devctl demo --fix %[1]s
`, rest[0])
	return nil
}
