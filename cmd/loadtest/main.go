// Command loadtest drives capacity and isolation scenarios against real
// Piperoom server processes (room service, HTTP API, WebSocket gateway and
// the real agent loop on Postgres) with a fake model and fake tools, so no
// API key is needed. See reviews/capacity-2026-10.md.
//
//	loadtest run -profile all [-short] [-p key=value]... [-replicas N] [-out DIR] [-db-url URL]
//	loadtest list
//	loadtest serve   (internal: one server process, started by run)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"piperoom/internal/loadtest"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "list":
		list()
	case "serve":
		err = loadtest.ServeMain(os.Stdin, os.Stdout)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		var ce checksFailed
		if errors.As(err, &ce) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		slog.Error("loadtest: failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: loadtest run [-profile all|name,...] [-short] [-p key=value]... [-replicas N] [-out DIR] [-db-url URL]\n       loadtest list")
}

func list() {
	for _, p := range loadtest.Profiles() {
		fmt.Printf("%s: %s\n", p.Name, p.Help)
		for _, s := range p.Params {
			short := s.Short
			if short == "" {
				short = s.Full
			}
			fmt.Printf("  %-22s %-9s full=%-18s short=%-10s %s\n", s.Name, s.Kind, s.Full, short, s.Help)
		}
	}
}

// multi collects a repeated flag.
type multi []string

func (m *multi) String() string     { return strings.Join(*m, " ") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

type checksFailed struct{ n int }

func (c checksFailed) Error() string {
	return fmt.Sprintf("loadtest: %d check(s) failed; see the reports", c.n)
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	profile := fs.String("profile", "all", "profile names, comma-separated, or all")
	short := fs.Bool("short", false, "use the small (CI smoke) defaults")
	replicas := fs.Int("replicas", 1, "server processes on one database")
	out := fs.String("out", "", "output directory (default loadtest-out/<time>)")
	dbURL := fs.String("db-url", "", "existing Postgres admin URL: ALL load goes to that server (the harness only creates and drops its own lt_<hex> databases there); default starts a throwaway postgres:16 container")
	prefix := fs.String("prefix", "s2lt", "docker container name prefix")
	mutex := fs.Int("mutex-profile", 0, "mutex profile fraction for the servers (0 = off); read /debug/pprof/mutex on a server URL")
	dbParams := fs.String("server-db-params", "", "extra k=v&k=v parameters for the servers' database URL, e.g. pool_max_conns=20")
	poolMax := fs.Int("pool-max-conns", 0, "experiment: set the servers' pgxpool size directly (0 = the default cmd/server gets)")
	var params multi
	fs.Var(&params, "p", "parameter override key=value or profile.key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	profiles, err := loadtest.SelectProfiles(*profile)
	if err != nil {
		return err
	}
	overrides, err := loadtest.ParseOverrides(params)
	if err != nil {
		return err
	}
	if err := loadtest.CheckOverrides(profiles, overrides); err != nil {
		return err
	}
	if *out == "" {
		*out = filepath.Join("loadtest-out", time.Now().UTC().Format("20060102T150405Z"))
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return fmt.Errorf("loadtest: out dir: %w", err)
	}
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("loadtest: find own binary: %w", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	env, err := loadtest.NewEnv(ctx, loadtest.EnvConfig{
		DBURL: *dbURL, Prefix: *prefix, Replicas: *replicas, Short: *short, LogDir: filepath.Join(*out, "logs"), Log: log,
		MutexProfile: *mutex, ServerDBParams: *dbParams, PoolMaxConns: *poolMax,
		Command: func() (string, []string, []string) { return self, []string{"serve"}, nil },
	})
	if err != nil {
		return err
	}
	defer env.Close()
	results, runErr := loadtest.RunProfiles(ctx, env, profiles, overrides, *out)
	failed := 0
	for _, r := range results {
		fmt.Println(r.Markdown)
		failed += len(r.Failed())
	}
	fmt.Printf("reports in %s\n", *out)
	if runErr != nil {
		return runErr
	}
	if failed > 0 {
		return checksFailed{failed}
	}
	return nil
}
