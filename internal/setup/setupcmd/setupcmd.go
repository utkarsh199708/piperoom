// Package setupcmd is the piperoom-setup command: it pairs with a Piperoom
// server over one WebSocket and executes the setup operations the server
// sends, or with --standalone runs the server for one machine itself: the
// quickstart server with write mode and setup on, loopback only, every room
// event appended to setup-log.jsonl, and --watch to restart it when the setup
// room writes owners.yaml (U1-11, docs/design/setup-agent.md section 2.7).
//
// It lives outside package main so that cmd/setup (piperoom-setup) and
// cmd/piperoom (piperoom setup, U1-21) run the same code with the same flags.
package setupcmd

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
	"piperoom/internal/setup/targets/host"
)

// Run parses args as piperoom-setup's flags and runs the mode they name until
// it ends or the process gets SIGINT or SIGTERM. Usage, parse errors and logs
// go to stderr; the standalone URL goes to stdout.
//
// The one argument form parsed here first is the host target's run-service
// subcommand (row U8b): the Windows service control manager starts
// `piperoom-setup run-service --release ...`, and that process is the service
// host, not a runner. It is dispatched in this package rather than in cmd/setup
// so that every build of the runner — piperoom-setup and the umbrella
// `piperoom setup` — answers it the same way; the service's own binary must be
// piperoom-setup either way, and service_install refuses a binary that is not.
func Run(args []string, stdout, stderr io.Writer) error {
	if handled, err := host.RunServiceCommand(args, stderr); handled {
		return err
	}
	fs := flag.NewFlagSet("piperoom-setup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", "", "server origin (default $PIPEROOM_SETUP_SERVER_URL)")
	code := fs.String("code", "", "pairing code (default $PIPEROOM_SETUP_CODE)")
	yes := fs.Bool("yes", false, "skip the pairing confirmation (loopback only)")
	project := fs.String("project", ".", "project directory")
	label := fs.String("label", "", "runner label (never the raw hostname)")
	standaloneMode := fs.Bool("standalone", false, "run the Piperoom server for this machine in-process (write mode and setup on, loopback only) and keep serving; prints the URL to open")
	watch := fs.Bool("watch", false, "with --standalone: restart the server whenever owners.yaml is written")
	watchInterval := fs.Duration("watch-interval", standalone.DefaultWatchInterval, "with --watch: how often to poll owners.yaml")
	embedded := fs.Bool("embedded", false, "the room's target is embedded (the Debezium Engine in your own application): register only the reference project and log reads, and refuse every other write")
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
		if *server != "" || *code != "" || *label != "" || *embedded {
			return errors.New("setup: --standalone runs its own server; --server, --code, --label and --embedded pair a runner with an existing one")
		}
		return standalone.Run(ctx, standalone.Options{
			Config:        config.Load(),
			ProjectDir:    *project,
			Watch:         *watch,
			WatchInterval: *watchInterval,
			Stdout:        stdout,
			Log:           newLogger(stderr),
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

	log := newLogger(stderr)
	// The real catalogue (U1-2). Targets arrive with the Welcome: the target
	// driver (compose or host, row U8f) registers then, through the
	// registrar; until the handshake hands them over, http_get and net_probe
	// refuse every host and the file and env ops work.
	catalogue, err := ops.New(ops.Config{ProjectDir: *project})
	if err != nil {
		return err
	}
	defer catalogue.Close()
	registrar := newTargetRegistrar(catalogue, parsePluginHosts(config.Load().SetupPluginHosts))
	if *embedded {
		registrar.embedded = true
	}
	r, err := runner.New(runner.Options{
		ServerURL:        serverURL,
		Code:             codeStr,
		Yes:              *yes,
		ProjectDir:       *project,
		Label:            *label,
		Log:              log,
		Catalogue:        catalogue,
		CatalogueVersion: ops.CatalogueVersion,
		RegisterTargets:  registrar.Apply,
	})
	if err != nil {
		return err
	}

	if err := r.Run(ctx); err != nil {
		if errors.Is(err, runner.ErrDeclined) {
			fmt.Fprintln(stderr, "setup: pairing declined")
			return nil
		}
		return err
	}
	return nil
}

// ExitCode maps Run's error to the process exit code and prints a failure to
// stderr: 0 for success and for --help (the flag package already printed the
// usage), 1 otherwise.
func ExitCode(err error, stderr io.Writer) int {
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	fmt.Fprintln(stderr, "setup:", err)
	return 1
}

// newLogger is the stderr logger every mode shares.
func newLogger(stderr io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(stderr, nil))
}
