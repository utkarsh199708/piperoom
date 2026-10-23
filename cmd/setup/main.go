// Command setup is the Piperoom setup runner: piperoom-setup pairs with a
// Piperoom server over one WebSocket and executes the setup operations the
// server sends. With --standalone it runs the server for one machine itself:
// the quickstart server with write mode and setup on, loopback only, every
// room event appended to setup-log.jsonl, and --watch to restart it when the
// setup room writes owners.yaml (U1-11, docs/design/setup-agent.md section
// 2.7).
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

	"piperoom/internal/config"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/runner"
	"piperoom/internal/setup/standalone"
)

func main() {
	if err := runWith(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	return runWith(args, os.Stdout)
}

func runWith(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("piperoom-setup", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	server := fs.String("server", "", "server origin (default $PIPEROOM_SETUP_SERVER_URL)")
	code := fs.String("code", "", "pairing code (default $PIPEROOM_SETUP_CODE)")
	yes := fs.Bool("yes", false, "skip the pairing confirmation (loopback only)")
	project := fs.String("project", ".", "project directory")
	label := fs.String("label", "", "runner label (never the raw hostname)")
	standaloneMode := fs.Bool("standalone", false, "run the Piperoom server for this machine in-process (write mode and setup on, loopback only) and keep serving; prints the URL to open")
	watch := fs.Bool("watch", false, "with --standalone: restart the server whenever owners.yaml is written")
	watchInterval := fs.Duration("watch-interval", standalone.DefaultWatchInterval, "with --watch: how often to poll owners.yaml")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("setup: unexpected arguments: %v", fs.Args())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *standaloneMode {
		// Standalone means this process is the server: a server origin or a
		// pairing code would name a different one.
		if *server != "" || *code != "" || *label != "" {
			return errors.New("setup: --standalone runs its own server; --server, --code and --label pair with an existing one")
		}
		return standalone.Run(ctx, standalone.Options{
			Config:        config.Load(),
			ProjectDir:    *project,
			Watch:         *watch,
			WatchInterval: *watchInterval,
			Stdout:        stdout,
			Log:           newLogger(),
			// --yes is accepted here because the server is loopback: the
			// pairing confirmation is skipped only where the address proves the
			// person's own machine (U1-1's loopback rule).
		})
	}
	if *watch {
		return errors.New("setup: --watch needs --standalone: the standalone server is the one that restarts")
	}

	cfg, err := config.ParseSetupRunner(config.Load())
	if err != nil {
		return err
	}
	serverURL := *server
	if serverURL == "" {
		serverURL = cfg.ServerURL
	}
	codeStr := *code
	if codeStr == "" {
		codeStr = cfg.Code
	}

	log := newLogger()
	// The real catalogue (U1-2). Targets arrive with the Welcome, and the
	// runner's target registry is a later row: until it hands them over,
	// http_get and net_probe refuse every host and the file and env ops work.
	catalogue, err := ops.New(ops.Config{ProjectDir: *project})
	if err != nil {
		return err
	}
	defer catalogue.Close()
	r, err := runner.New(runner.Options{
		ServerURL:        serverURL,
		Code:             codeStr,
		Yes:              *yes,
		ProjectDir:       *project,
		Label:            *label,
		Log:              log,
		Catalogue:        catalogue,
		CatalogueVersion: ops.CatalogueVersion,
	})
	if err != nil {
		return err
	}

	if err := r.Run(ctx); err != nil {
		if errors.Is(err, runner.ErrDeclined) {
			fmt.Fprintln(os.Stderr, "setup: pairing declined")
			return nil
		}
		return err
	}
	return nil
}

// newLogger is the stderr logger every mode shares.
func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}
