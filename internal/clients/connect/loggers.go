package connect

import (
	"context"
	"net/http"
)

var _ LoggerLevelReader = (*Client)(nil)

// LoggerLevelReader reads the worker's configured logger levels. It is
// optional like PluginReader: a client without it does not break, and the
// receiving tool says the levels cannot be read from here. It is a new
// interface rather than a method on ConnectInfoReader so that existing fakes
// and eval replays compile unchanged (the Q0 clients.ConnectInfoReader adds
// LoggerLevels; contract change request in this PR).
type LoggerLevelReader interface {
	// LoggerLevels returns each configured logger's level (ERROR, WARN, INFO,
	// DEBUG, TRACE, ...), keyed by logger name. Only levels the worker
	// reports are present; an empty map means it configured none.
	LoggerLevels(ctx context.Context) (map[string]string, error)
}

// LoggerLevels reads GET /admin/loggers: a map of logger name to
// {"level": "..."} (apache/kafka ConnectLoggersResource). It reads only; it
// never changes a level. Logger names and levels are untrusted Connect text,
// so the caller sanitizes them before they reach a summary or Data.
func (c *Client) LoggerLevels(ctx context.Context) (map[string]string, error) {
	var raw map[string]struct {
		Level string `json:"level"`
	}
	if err := c.do(ctx, http.MethodGet, "/admin/loggers", &raw); err != nil {
		// Fold, redact and cap a non-2xx body before it can reach a log or a
		// prompt, as ServerInfo and Plugins do.
		return nil, safeErr(err)
	}
	levels := make(map[string]string, len(raw))
	for name, l := range raw {
		if name == "" {
			continue
		}
		levels[name] = l.Level
	}
	return levels, nil
}
