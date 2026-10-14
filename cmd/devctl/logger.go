package main

import (
	"io"
	"log/slog"

	"piperoom/internal/config"
)

// newLogger builds devctl's logger from PIPEROOM_LOG_LEVEL and
// PIPEROOM_LOG_FORMAT, like the server. A bad value keeps the defaults and is
// reported once on the new logger, so a typo never stops a dev command.
func newLogger(w io.Writer, c config.Config) *slog.Logger {
	l, err := config.ParseLog(c)
	if err != nil {
		l, _ = config.ParseLog(config.Config{})
	}
	opts := &slog.HandlerOptions{Level: l.Level}
	var h slog.Handler = slog.NewTextHandler(w, opts)
	if l.Format == config.LogFormatJSON {
		h = slog.NewJSONHandler(w, opts)
	}
	log := slog.New(h)
	if err != nil {
		log.Warn("devctl: ignoring log settings", "err", err)
	}
	return log
}
