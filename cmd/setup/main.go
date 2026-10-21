// Command setup is the Piperoom setup runner: piperoom-setup pairs with a
// Piperoom server over one WebSocket and, in later waves, executes the setup
// operations the server sends. This wave (U1-1a) implements pairing with
// local confirmation, the Hello handshake and the transport.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"piperoom/internal/config"
	"piperoom/internal/setup/runner"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "setup:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("piperoom-setup", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	server := fs.String("server", "", "server origin (default $PIPEROOM_SETUP_SERVER_URL)")
	code := fs.String("code", "", "pairing code (default $PIPEROOM_SETUP_CODE)")
	yes := fs.Bool("yes", false, "skip the pairing confirmation (loopback only)")
	project := fs.String("project", ".", "project directory")
	label := fs.String("label", "", "runner label (never the raw hostname)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("setup: unexpected arguments: %v", fs.Args())
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

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r, err := runner.New(runner.Options{
		ServerURL:  serverURL,
		Code:       codeStr,
		Yes:        *yes,
		ProjectDir: *project,
		Label:      *label,
		Log:        log,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := r.Run(ctx); err != nil {
		if errors.Is(err, runner.ErrDeclined) {
			fmt.Fprintln(os.Stderr, "setup: pairing declined")
			return nil
		}
		return err
	}
	return nil
}
