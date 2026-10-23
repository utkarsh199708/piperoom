package connect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"piperoom/internal/clients"
)

var _ LoggerReader = (*Client)(nil)

// LoggerLevel is one logger of GET /admin/loggers: its name and level.
// LastModified (Kafka 3.7+, KIP-976) is when the level was last changed
// through the API, in Unix milliseconds; 0 when Connect does not report it.
type LoggerLevel struct {
	Logger       string `json:"logger"`
	Level        string `json:"level"`
	LastModified int64  `json:"last_modified,omitempty"`
}

// LoggerReader reads the worker's log levels. It is optional (Q3a): a client
// without it gives connect_worker_info no logger section, and the tool says
// so. It is read-only; setting a level is a write and is not offered.
type LoggerReader interface {
	Loggers(ctx context.Context) ([]LoggerLevel, error)
}

// loggersVersion is the Kafka release that adds GET /admin/loggers (KIP-495).
const loggersVersion = "2.4"

// maxLoggers caps the loggers kept from one answer.
const maxLoggers = 500

// Loggers reads GET /admin/loggers on the worker the client points at: a JSON
// object of logger name to {"level", "last_modified"}. A Connect without the
// endpoint (before Kafka 2.4) answers 404, which is clients.ErrNotSupported.
// The levels are for that worker only: KIP-976 changes can be cluster-wide,
// but the read is not.
func (c *Client) Loggers(ctx context.Context) ([]LoggerLevel, error) {
	var raw map[string]struct {
		Level        string `json:"level"`
		LastModified *int64 `json:"last_modified"`
	}
	if err := c.do(ctx, http.MethodGet, "/admin/loggers", &raw); err != nil {
		if errors.Is(err, clients.ErrNotFound) {
			return nil, fmt.Errorf("connect: GET /admin/loggers needs Kafka Connect %s+: %w", loggersVersion, clients.ErrNotSupported)
		}
		return nil, safeErr(err)
	}
	out := make([]LoggerLevel, 0, len(raw))
	for name, v := range raw {
		l := LoggerLevel{Logger: name, Level: strings.ToUpper(strings.TrimSpace(v.Level))}
		if v.LastModified != nil {
			l.LastModified = *v.LastModified
		}
		out = append(out, l)
	}
	// Sort before the cap so the same answer always keeps the same loggers;
	// "root" first because every other level inherits from it.
	sort.Slice(out, func(i, j int) bool {
		if (out[i].Logger == "root") != (out[j].Logger == "root") {
			return out[i].Logger == "root"
		}
		return out[i].Logger < out[j].Logger
	})
	if len(out) > maxLoggers {
		out = out[:maxLoggers]
	}
	return out, nil
}
