package main

import (
	"fmt"
	"io"
	"log/slog"

	"piperoom/internal/config"
)

// newLogger builds the process logger from PIPEROOM_LOG_LEVEL and
// PIPEROOM_LOG_FORMAT (config.ParseLog): text for a terminal, JSON for a log
// collector. The error names the bad variable.
func newLogger(cfg config.Config, w io.Writer) (*slog.Logger, error) {
	l, err := config.ParseLog(cfg)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	opts := &slog.HandlerOptions{Level: l.Level}
	if l.Format == config.LogFormatJSON {
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	}
	return slog.New(slog.NewTextHandler(w, opts)), nil
}
