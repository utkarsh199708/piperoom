package pgactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Every pgactions action refuses a MySQL pipeline at CheckScope, and its
// Preview and Execute (which run CheckScope's rule on the room pipeline in
// ctx) refuse it before any I/O: the deps are empty, so any read would fail
// with another error. The same params pass on the Postgres pipeline.
func TestPostgresOnlyActionsRefuseMySQLPipeline(t *testing.T) {
	mysqlOf := func(pl owners.Pipeline) owners.Pipeline {
		pl.SourceKind = owners.SourceKindMySQL
		pl.WatchSpec = nil
		return pl
	}
	cases := []struct {
		name string
		a    actions.Action
		pl   owners.Pipeline
		raw  json.RawMessage
	}{
		{AddPublicationTableName, NewAddPublicationTable(AddPublicationTableDeps{}), addPubPipeline(), addPubGood},
		{DropSlotName, NewDropReplicationSlot(DropSlotDeps{}), pipeline(), good(t)},
		{ResnapshotRowsName, NewResnapshotRows(ResnapshotRowsDeps{}), resnapPipeline(), resnapKeys("1", "2")},
		{SnapshotSignalName, NewSnapshotSignal(SnapshotSignalDeps{}), snapSigPipeline(), snapSigGood},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := tc.a.(actions.Scoped)
			if err := sc.CheckScope(tc.pl, tc.raw); err != nil {
				t.Fatalf("Postgres pipeline: CheckScope = %v, want nil", err)
			}
			my := mysqlOf(tc.pl)
			err := sc.CheckScope(my, tc.raw)
			if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "Postgres-only") {
				t.Fatalf("MySQL pipeline: CheckScope = %v, want ErrOutOfScope naming Postgres-only", err)
			}
			ctx := tools.WithPipeline(context.Background(), my)
			if p, ok := tc.a.(actions.Preview); ok {
				if _, err := p.Preview(ctx, tc.raw); !errors.Is(err, tools.ErrOutOfScope) {
					t.Errorf("MySQL pipeline: Preview = %v, want ErrOutOfScope", err)
				}
			}
			if _, err := tc.a.Execute(ctx, tc.raw); !errors.Is(err, tools.ErrOutOfScope) {
				t.Errorf("MySQL pipeline: Execute = %v, want ErrOutOfScope", err)
			}
		})
	}
}
