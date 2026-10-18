package main

import (
	"fmt"
	"log/slog"

	"piperoom/internal/config"
)

// writeModeSetup reads PIPEROOM_WRITE_MODE (config.ParseWriteMode, P0 item 1)
// and reports whether fully approved proposals run. A value other than off or
// on stops the start, in --demo too: a typo is a typo. --demo then forces on,
// because its actions run on in-memory fake clients only
// (demo.FakeActionDeps) and the demo exists to show the whole path to
// action.executed. Off is logged once as a warning, so whoever reads the
// start-up log knows why an approved fix does nothing.
func writeModeSetup(cfg config.Config, demoMode bool, log *slog.Logger) (bool, error) {
	on, err := config.ParseWriteMode(cfg)
	if err != nil {
		return false, fmt.Errorf("server: %w", err)
	}
	switch {
	case demoMode:
		log.Info("server: write mode on: --demo runs fixes on fake clients only")
		return true, nil
	case on:
		log.Info("server: write mode on: fully approved fixes run")
	default:
		log.Warn("server: write mode off: fixes will be approved but not executed; set PIPEROOM_WRITE_MODE=on to run them")
	}
	return on, nil
}
