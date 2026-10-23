package wiring

import (
	"piperoom/internal/actions"
	"piperoom/internal/clients"
)

// The Q5 connector lifecycle actions (Q5b, Q5c): connect_stop_connector,
// connect_reset_source_offsets, connect_create_connector and
// connect_delete_connector. Unlike the P3 and Q2 actions they are registered
// only with write mode on, as package actions asks (RegisterLifecycle): they
// stop, delete, re-register a connector or move where it reads from, so with
// writes off none of them is listed, the agent cannot propose one, and no
// value in the action registry holds a lifecycle writer. With write mode on
// they get the whole Connect client, which implements
// connect.ConnectLifecycleWriter and connect.SourceOffsetReader; the actions
// assert for both at use time, so a client without them refuses at Validate
// or Execute before any write.

// registerLifecycle registers the Q5 actions over w's clients when w was
// opened with write mode on, and nothing otherwise. Source is the read-only
// source client: only connect_reset_source_offsets' Preview reads it, for the
// size of the snapshot a reset starts. AllowedHosts stays empty: no setting
// names extra endpoints yet, so a created connector may point only at hosts
// the pipeline's connections or its registered connectors already reach.
func registerLifecycle(areg *actions.Registry, w *Wired) error {
	if !w.write {
		return nil
	}
	d := actions.LifecycleDeps{Connect: w.Connect}
	if isNilIface(d.Connect) {
		d.Connect = nil
	}
	var src clients.SourceReader = w.Source
	if !isNilIface(src) {
		d.Source = src
	}
	return actions.RegisterLifecycle(areg, d)
}
