package sourceactions

import (
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// CheckScope accepts only the source kinds an action supports (KAN-541): the
// three source-neutral actions take postgres and mysql, set_replica_identity
// only postgres, and a sqlserver, mongodb or oracle pipeline is refused with
// ErrOutOfScope naming its kind instead of being handled as Postgres.
func TestCheckScopeBySourceKind(t *testing.T) {
	_, _, _, grant := grantFixture()
	_, _, _, hb := hbFixture()
	_, _, _, es := esFixture()
	ri := &setReplicaIdentity{}

	actionsUnderTest := []struct {
		name string
		a    interface {
			CheckScope(owners.Pipeline, []byte) error
		}
		mysqlOK    bool
		wantPhrase string
	}{
		{GrantSelectName, scopeAdapter(func(pl owners.Pipeline) error {
			return grant.CheckScope(pl, grantParamsJSON("debezium", "public.orders"))
		}), true, "does not support"},
		{CreateHeartbeatTableName, scopeAdapter(func(pl owners.Pipeline) error { return hb.CheckScope(pl, hbParams) }), true, "does not support"},
		{EndSessionName, scopeAdapter(func(pl owners.Pipeline) error {
			return es.CheckScope(pl, esParams(4242, time.Unix(0, 0)))
		}), true, "does not support"},
		{SetReplicaIdentityName, scopeAdapter(func(pl owners.Pipeline) error {
			return ri.CheckScope(pl, riParams("full", ""))
		}), false, "Postgres-only"},
	}
	kinds := []struct {
		name, kind string
	}{
		{"default", ""},
		{"postgres", owners.SourceKindPostgres},
		{"mysql", owners.SourceKindMySQL},
		{"sqlserver", owners.SourceKindSQLServer},
		{"mongodb", owners.SourceKindMongoDB},
		{"oracle", owners.SourceKindOracle},
	}
	for _, act := range actionsUnderTest {
		for _, k := range kinds {
			t.Run(act.name+"/"+k.name, func(t *testing.T) {
				pl := testPipeline()
				pl.SourceKind = k.kind
				err := act.a.CheckScope(pl, nil)
				ok := k.kind == "" || k.kind == owners.SourceKindPostgres || (k.kind == owners.SourceKindMySQL && act.mysqlOK)
				if ok {
					if err != nil {
						t.Fatalf("CheckScope = %v, want nil", err)
					}
					return
				}
				if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), act.wantPhrase) || !strings.Contains(err.Error(), "source_kind "+k.kind) {
					t.Fatalf("CheckScope = %v, want ErrOutOfScope naming source_kind %s and %q", err, k.kind, act.wantPhrase)
				}
			})
		}
	}
}

// scopeAdapter wraps a closure over a pipeline as the CheckScope shape above.
type scopeAdapter func(owners.Pipeline) error

func (f scopeAdapter) CheckScope(pl owners.Pipeline, _ []byte) error { return f(pl) }
