package wiring

import (
	"piperoom/internal/actions"
	"piperoom/internal/clients"
)

// The Q5 connector lifecycle actions (Q5b, Q5c): connect_stop_connector,
// connect_reset_source_offsets, connect_create_connector and
// connect_delete_connector. Like the P3 and Q2 actions they are registered in
// both write modes (P0 ruling #606): with write mode off the agent must still
// be able to propose and preview a stop, an offsets reset, a create or a
// delete, and the room's executor runs nothing. With write mode on they get
// the whole Connect client, which implements connect.ConnectLifecycleWriter
// and connect.SourceOffsetReader. With write mode off they get readConnect's
// read-only value (the stored offsets, version and plugin reads and config
// validation, no write method): Validate and Preview only read, and Execute
// asserts connect.ConnectLifecycleWriter before any I/O, so it refuses there
// and no value in the action registry holds a lifecycle writer.

// registerLifecycle registers the Q5 actions over w's clients. Source is the
// read-only source client: only connect_reset_source_offsets' Preview reads
// it, for the size of the snapshot a reset starts. AllowedHosts are the
// set's own Kafka brokers, Schema Registry and source database (in both write
// modes, so Preview answers the same), so a pipeline's first connector can
// name them; any other host must already be in a registered connector's
// config.
func registerLifecycle(areg *actions.Registry, w *Wired) error {
	d := actions.LifecycleDeps{AllowedHosts: w.lifecycleHosts}
	if w.write {
		d.Connect = w.Connect
	} else {
		d.Connect = readConnect(w)
	}
	if isNilIface(d.Connect) {
		d.Connect = nil
	}
	var src clients.SourceReader = w.Source
	if !isNilIface(src) {
		d.Source = src
	}
	return actions.RegisterLifecycle(areg, d)
}
