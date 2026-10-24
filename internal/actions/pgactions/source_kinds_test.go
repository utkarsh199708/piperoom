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

// Every pgactions action runs on a Postgres pipeline (source_kind unset or
// postgres) and refuses every other source kind at CheckScope, Preview and
// Execute, with an error that names the kind (KAN-541).
func TestPostgresOnlyActionsBySourceKind(t *testing.T) {
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
	kinds := []struct {
		name, kind string
		ok         bool
	}{
		{"default", "", true},
		{"postgres", owners.SourceKindPostgres, true},
		{"mysql", owners.SourceKindMySQL, false},
		{"sqlserver", owners.SourceKindSQLServer, false},
		{"mongodb", owners.SourceKindMongoDB, false},
		{"oracle", owners.SourceKindOracle, false},
	}
	for _, tc := range cases {
		for _, k := range kinds {
			t.Run(tc.name+"/"+k.name, func(t *testing.T) {
				pl := tc.pl
				pl.SourceKind = k.kind
				pl.WatchSpec = nil
				err := tc.a.(actions.Scoped).CheckScope(pl, tc.raw)
				if k.ok {
					if err != nil {
						t.Fatalf("CheckScope = %v, want nil", err)
					}
					return
				}
				if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "Postgres-only") || !strings.Contains(err.Error(), "source_kind "+k.kind) {
					t.Fatalf("CheckScope = %v, want ErrOutOfScope naming Postgres-only and source_kind %s", err, k.kind)
				}
				ctx := tools.WithPipeline(context.Background(), pl)
				if p, ok := tc.a.(actions.Preview); ok {
					if _, err := p.Preview(ctx, tc.raw); !errors.Is(err, tools.ErrOutOfScope) {
						t.Errorf("Preview = %v, want ErrOutOfScope", err)
					}
				}
				if _, err := tc.a.Execute(ctx, tc.raw); !errors.Is(err, tools.ErrOutOfScope) {
					t.Errorf("Execute = %v, want ErrOutOfScope", err)
				}
			})
		}
	}
}
