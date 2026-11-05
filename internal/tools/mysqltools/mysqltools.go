// Package mysqltools holds the read-only tools that only a MySQL pipeline's
// room offers (docs/design/mysql-source.md §3, §7.2): today
// mysql_binlog_status. The source-neutral source_* tools live in dbtools.
//
// Wiring registers the package for pipelines whose source_kind is mysql
// (Q8g); a scoped call on any other pipeline is refused before any I/O.
package mysqltools

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// NameBinlogStatus is the tool name of mysql_binlog_status.
const NameBinlogStatus = "mysql_binlog_status"

// callTimeout bounds one tool call: the binlog read and every Connect read
// share it (AGENTS.md §4 rule 4).
const callTimeout = 10 * time.Second

// Deps are the collaborators the tools read from.
type Deps struct {
	// Src is the pipeline's source client. mysql_binlog_status asserts
	// mysql.BinlogReader on it at call time; a client without it (Postgres,
	// a fake, the eval replay) answers clients.ErrNotSupported.
	Src clients.SourceDB
	// Connect reads the connectors' config and status, and their stored
	// offsets through connect.SourceOffsetReader (asserted at call time).
	// Nil-able: the tool then reports the server's binlog state alone.
	Connect clients.ConnectReader
	// Owners resolves the room's pipeline (tools.ScopeFor). Required.
	Owners *owners.Owners
	// Now is the clock of the retention check; nil is time.Now.
	Now func() time.Time
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Register adds every mysqltools tool to reg. A nil Owners is a wiring fault
// and fails fast.
func Register(reg *tools.Registry, d Deps) error {
	if d.Owners == nil {
		return fmt.Errorf("mysqltools: Owners is nil (wiring error)")
	}
	for _, t := range []tools.Tool{&binlogStatusTool{d: d}} {
		if err := reg.Register(t); err != nil {
			return fmt.Errorf("mysqltools: register %s: %w", t.Name(), err)
		}
	}
	return nil
}

// withTimeout bounds one call.
func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, callTimeout)
}
