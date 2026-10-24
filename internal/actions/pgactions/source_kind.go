package pgactions

import (
	"fmt"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// refuseNonPostgres is every pgactions action's refusal of a pipeline whose
// source is not Postgres (Q8n, docs/design/mysql-source.md §3 and §8.1;
// KAN-541 for sqlserver, mongodb and oracle): publications, replication slots
// and the signal table are Postgres things. Wiring does not register these
// actions for a MySQL set (Q8g); this is the action's own guard, in
// CheckScope, so a step on such a pipeline never reaches an approver. The
// check is positive (the source kind is postgres), so a kind added later is
// refused until it is listed here.
func refuseNonPostgres(name string, pl owners.Pipeline) error {
	if k := pl.SourceKindOrDefault(); k != owners.SourceKindPostgres {
		return fmt.Errorf("pgactions: %s: %w: pipeline %q has source_kind %s and %s is a Postgres-only action (a %s source has no publications, replication slots or signal table)", name, tools.ErrOutOfScope, pl.ID, k, name, k)
	}
	return nil
}
