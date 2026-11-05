package wiring

import (
	"piperoom/internal/clients"
	"piperoom/internal/config"
)

// The two accessors below are what internal/wiring/netwire needs to build
// net_probe over a set without this package importing nettools (see
// Wired.ProbeTools for the import cycle).

// ReadOnlyConnect returns the set's Connect client as the read tools get it:
// nil when none is configured, else the read-only value (readConnect), which
// keeps Connect's config validate. It never returns the writing client.
func (w *Wired) ReadOnlyConnect() clients.ConnectClient { return readConnect(w) }

// ConnSettings returns the connection settings the set was opened with (the
// process-wide settings, or one pipeline's resolved connections: block); zero
// for a Wired built by hand.
func (w *Wired) ConnSettings() config.Config { return w.conn }
