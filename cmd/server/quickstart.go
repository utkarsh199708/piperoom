package main

// --quickstart (tasks/P1-quickstart-readonly.md, P1a): the real agent and the
// real read-only tools on your pipelines, with the in-memory room store, dev
// login on loopback only, write mode off, and a one-user users file written
// for this run, so `discover -write-owners` plus this flag is a whole setup.
//
// The start itself lives in internal/quickstart (U1-5c): this file keeps the
// flag checks and hands the package the server composition every mode shares.

import (
	"context"
	"errors"
	"log/slog"

	"piperoom/internal/config"
	"piperoom/internal/quickstart"
)

// checkStartFlags refuses flag combinations that cannot mean what they say,
// before anything is read or opened.
func checkStartFlags(demo, quickstartMode, solo, allowRemote bool, ownersPath string) error {
	switch {
	case quickstartMode && demo:
		return errors.New("server: --quickstart and --demo cannot be combined: --quickstart runs the real agent on your pipelines, --demo a scripted one on fake data")
	case quickstartMode && allowRemote:
		return errors.New("server: --quickstart listens on loopback only; --allow-remote cannot be combined with it")
	case ownersPath != "" && !quickstartMode:
		return errors.New("server: --owners needs --quickstart; without it, set OWNERS_FILE")
	case solo && !demo && !quickstartMode:
		return errors.New("server: --solo needs --demo or --quickstart: it gives the demo presenter the solo ceremony; the full server takes the solo user from PIPEROOM_SOLO_USER")
	}
	return nil
}

// runQuickstart runs cmd/server's --quickstart start through
// internal/quickstart: the package forces loopback, dev login and write mode
// off, resolves the users file and then calls serve, the composition every
// mode uses. cmd/server's quickstart is read-only by design, so WriteMode and
// SetupEnabled are false here; the package's options exist for the standalone
// server of docs/design/setup-agent.md section 2.7.
func runQuickstart(ctx context.Context, d startDeps, cfg config.Config, ownersPath string, solo bool, log *slog.Logger) error {
	m := serveMode{quickstart: true, solo: solo, log: log}
	return quickstart.Run(ctx, quickstart.Options{
		Config:       cfg,
		WriteMode:    false,
		SetupEnabled: false,
		OwnersPath:   ownersPath,
		Solo:         solo,
		Stdout:       d.stdout,
		Log:          log,
		Serve: func(ctx context.Context, s quickstart.Settings) error {
			m.stdout = s.Stdout
			settings, err := parseServer(s.Config, m)
			if err != nil {
				return err
			}
			return serve(ctx, d, s.Config, s.Owners, s.Users, m, settings)
		},
	})
}
