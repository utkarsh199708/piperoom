package wiring

import (
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// The Q2 source admin actions (Q2c, Q2d): pg_set_replica_identity,
// source_create_heartbeat_table, source_grant_select and source_end_session.
// Like the P3 actions they are registered in both write modes, so the agent
// may propose them and the room previews them: Validate and Preview read
// only, through the read-only Connect adapter and the source reader. Execute
// alone needs the admin writer, and that exists only with write mode on and
// a source write URL: it is the set's own source writer (*postgres.Writer on
// SOURCE_DB_WRITE_URL, or the pipeline's connections.source_db_write_url),
// which implements postgres.SourceAdminWriter (Q2b). Off, every writer below
// is an untyped nil, so Validate refuses ("no write credentials") and
// Execute refuses before any I/O.

// adminWriter returns w's source admin writer: nil unless w was opened with
// write mode on and its source writer has the Q2b admin methods. It is never
// an interface holding a typed nil.
func adminWriter(w *Wired) postgres.SourceAdminWriter {
	if !w.write || w.writers.source == nil {
		return nil
	}
	if a, ok := w.writers.source.(postgres.SourceAdminWriter); ok && !isNilIface(a) {
		return a
	}
	return nil
}

// SourceActionDeps are the clients the four Q2 source admin actions read and
// write through (RegisterSourceActions). Any field may be nil.
type SourceActionDeps struct {
	// Connect reads connector configs (connector.class, database.user,
	// publication.name). It should hold no Connect write: none of the four
	// needs one.
	Connect tools.ConfigReader
	// Source is the read-only source client with the Q2a reads.
	Source clients.SourceDB
	// Admin executes the four actions. In wiring it is nil unless write mode
	// is on and a source write URL is set.
	Admin postgres.SourceAdminWriter
}

// RegisterSourceActions registers pg_set_replica_identity,
// source_create_heartbeat_table, source_grant_select and source_end_session
// over d, in that order. Each action checks Writer == nil, so a missing (or
// typed-nil) admin writer is handed to every one as an untyped nil: an
// interface holding a typed nil would pass Validate and fail at Execute.
func RegisterSourceActions(areg *actions.Registry, d SourceActionDeps) error {
	if isNilIface(d.Connect) {
		d.Connect = nil
	}
	if isNilIface(d.Source) {
		d.Source = nil
	}
	var (
		ri sourceactions.ReplicaIdentityWriter
		hb sourceactions.HeartbeatWriter
		gs sourceactions.GrantWriter
		es sourceactions.SessionEnder
	)
	if !isNilIface(d.Admin) {
		ri, hb, gs, es = d.Admin, d.Admin, d.Admin, d.Admin
	}
	for _, a := range []actions.Action{
		sourceactions.NewSetReplicaIdentity(sourceactions.ReplicaIdentityDeps{Source: d.Source, Writer: ri}),
		sourceactions.NewCreateHeartbeatTable(sourceactions.HeartbeatDeps{Connect: d.Connect, Source: d.Source, Writer: hb}),
		sourceactions.NewGrantSelect(sourceactions.GrantSelectDeps{Connect: d.Connect, Source: d.Source, Writer: gs}),
		sourceactions.NewEndSession(sourceactions.EndSessionDeps{Connect: d.Connect, Source: d.Source, Writer: es}),
	} {
		if err := areg.Register(a); err != nil {
			return fmt.Errorf("wiring: register %s: %w", a.Name(), err)
		}
	}
	return nil
}

// registerSourceActions registers the Q2 source admin actions over w's
// clients: the read-only Connect adapter in both write modes, and the admin
// writer only with write mode on (adminWriter).
func registerSourceActions(areg *actions.Registry, w *Wired) error {
	// readConnect is nil (a nil interface) without a Connect client, and an
	// interface conversion keeps it nil.
	var conn tools.ConfigReader = readConnect(w)
	return RegisterSourceActions(areg, SourceActionDeps{Connect: conn, Source: w.Source, Admin: adminWriter(w)})
}
