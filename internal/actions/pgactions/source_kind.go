package pgactions

import (
	"fmt"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// refuseMySQL is every pgactions action's refusal of a MySQL pipeline (Q8n,
// docs/design/mysql-source.md §3 and §8.1): publications, replication slots
// and the signal table are Postgres things. Wiring does not register these
// actions for a MySQL set (Q8g); this is the action's own guard, in
// CheckScope, so a step on a MySQL pipeline never reaches an approver.
func refuseMySQL(name string, pl owners.Pipeline) error {
	if pl.IsMySQL() {
		return fmt.Errorf("pgactions: %s: %w: pipeline %q has source_kind mysql and %s is a Postgres-only action (MySQL has no publications, replication slots or signal table)", name, tools.ErrOutOfScope, pl.ID, name)
	}
	return nil
}
