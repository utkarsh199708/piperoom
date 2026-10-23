// Package netwire registers net_probe (Q6a, internal/tools/nettools) over a
// wiring client set. It lives apart from package wiring because nettools
// imports internal/connprobe, which imports wiring: wiring calls Register
// through the Wired.ProbeTools hook instead of importing nettools.
package netwire

import (
	"piperoom/internal/tools"
	"piperoom/internal/tools/nettools"
	"piperoom/internal/wiring"
)

// Register adds net_probe to reg over w's clients: the read-only Connect value
// (its config validate gives the probe Connect's own view of the source), the
// Kafka admin for the advertised listeners, and the set's own connection
// settings, so a set never names another set's host. It has the
// Wired.ProbeTools signature.
func Register(reg *tools.Registry, w *wiring.Wired) error {
	d := nettools.Deps{Connect: w.ReadOnlyConnect(), Owners: w.Owners, Config: w.ConnSettings()}
	if w.Kafka != nil {
		d.Kafka = w.Kafka
	}
	return nettools.Register(reg, d)
}
