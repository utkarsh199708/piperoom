package wiring

import (
	"fmt"

	"piperoom/internal/actions"
	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// The source database admin actions (Q2c, Q2d) are registered in both write
// modes, like the P3 actions: GET /api/actions lists them, the agent may
// propose them and the room previews them. Validate and Preview read only —
// through the read-only Connect adapter and the source reader — so with write
// mode off they still say what the step would do. Execute alone needs the
// admin writer, and that exists only with write mode on and
// SOURCE_DB_WRITE_URL set (writemode.go); off, Deps.Writer is nil and every
// Execute refuses before any I/O.

// SourceActionDeps are the clients the four Q2c/Q2d actions read and write
// through (RegisterSourceActions). Any field may be nil.
type SourceActionDeps struct {
	// Connect reads the connector config (database.user, publication,
	// heartbeat settings) in Validate and Preview. It should hold no Connect
	// write: none of the four needs one.
	Connect tools.ConfigReader
	// Source is the read-only source Postgres. The actions find the Q2a reads
	// they need by type assertion (TableReplication, RoleInfo, Blockers); a
	// client without them refuses at Validate with a clear error.
	Source clients.SourceDB
	// Writer executes the steps; it is the admin half of the
	// SOURCE_DB_WRITE_URL writer (postgres.SourceAdminWriter). In wiring it is
	// nil unless write mode is on; a harness may pass a writer that records
	// and changes nothing.
	Writer postgres.SourceAdminWriter
}

// RegisterSourceActions registers pg_set_replica_identity,
// source_create_heartbeat_table, source_grant_select and source_end_session
// over d, in that order. It is the one place the four are put together with
// their readers and writer: wiring calls it for each client set
// (registerSourceActions), and a harness can call it over its replay clients,
// so both register the same actions the same way. A field holding a typed nil
// is turned into a nil interface first, so an action refuses cleanly at
// Execute instead of panicking.
func RegisterSourceActions(areg *actions.Registry, d SourceActionDeps) error {
	if isNilIface(d.Connect) {
		d.Connect = nil
	}
	if isNilIface(d.Source) {
		d.Source = nil
	}
	if isNilIface(d.Writer) {
		d.Writer = nil
	}
	list := []actions.Action{
		sourceactions.NewSetReplicaIdentity(sourceactions.ReplicaIdentityDeps{Source: d.Source, Writer: d.Writer}),
		sourceactions.NewCreateHeartbeatTable(sourceactions.HeartbeatDeps{Connect: d.Connect, Source: d.Source, Writer: d.Writer}),
		sourceactions.NewGrantSelect(sourceactions.GrantSelectDeps{Connect: d.Connect, Source: d.Source, Writer: d.Writer}),
		sourceactions.NewEndSession(sourceactions.EndSessionDeps{Connect: d.Connect, Source: d.Source, Writer: d.Writer}),
	}
	for _, a := range list {
		if err := areg.Register(a); err != nil {
			return fmt.Errorf("wiring: register %s: %w", a.Name(), err)
		}
	}
	return nil
}

// registerSourceActions registers the source admin actions over w's clients:
// the read-only Connect adapter and source reader in both write modes, and
// the admin writer only when w was opened with write mode on
// (actionAdminWriter).
func registerSourceActions(areg *actions.Registry, w *Wired) error {
	var connReader tools.ConfigReader
	if c := readConnect(w); c != nil {
		connReader = c
	}
	return RegisterSourceActions(areg, SourceActionDeps{
		Connect: connReader,
		Source:  w.Source,
		Writer:  actionAdminWriter(w),
	})
}

// actionAdminWriter returns the source admin writer of w: nil unless w was
// opened with write mode on and a source writer was built (SOURCE_DB_WRITE_URL
// set). The value is converted from the field only when the field holds
// something, so the actions see a nil interface, never a typed nil.
func actionAdminWriter(w *Wired) postgres.SourceAdminWriter {
	if !w.write || w.writers.admin == nil {
		return nil
	}
	return w.writers.admin
}
