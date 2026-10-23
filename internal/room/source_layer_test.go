package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// sourceLayeredAction is a source-neutral source action (source_grant_select
// after Q8n): it carries the SourceLayered marker and its Layer() answers
// postgres, the value a caller without a pipeline shows.
type sourceLayeredAction struct{ fakeAction }

func (*sourceLayeredAction) SourceLayered() {}

const sourceLayerOwners = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
  - id: orders-mysql
    source_kind: mysql
    topic_prefix: shop
    source_tables: [shop.orders]
    connectors: [orders-mysql]
    owners:
      mysql: {approvers: arjun}
layers:
  postgres: priya
  kafka-connect: arjun
`

func sourceLayerService(t *testing.T) *room.Service {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(sourceLayerOwners), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{
		&sourceLayeredAction{fakeAction{name: "source_grant_select", layer: "postgres"}},
		&fakeAction{name: "pg_set_replica_identity", layer: "postgres"},
		&fakeAction{name: "connect_restart_task", layer: "kafka-connect"},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	svc := room.New(room.Deps{
		Store: memstore.New(), Actions: reg, Users: room.DefaultUsers(), Owners: own,
		Run: fakeRun, WriteHandoff: fakeWrite,
	})
	t.Cleanup(svc.Close)
	return svc
}

// A step's approving layer comes from the room pipeline's source kind for a
// SourceLayered action, and from Layer() for every other action (Q8o).
func TestStepLayerFollowsThePipelineSourceKind(t *testing.T) {
	cases := []struct {
		name, pipeline, action, layer string
		ok                            bool
	}{
		{"mysql pipeline, source action, mysql layer", "orders-mysql", "source_grant_select", "mysql", true},
		{"mysql pipeline, source action, postgres layer", "orders-mysql", "source_grant_select", "postgres", false},
		{"postgres pipeline, source action, postgres layer", "orders", "source_grant_select", "postgres", true},
		{"postgres pipeline, source action, mysql layer", "orders", "source_grant_select", "mysql", false},
		{"mysql pipeline, unmarked action keeps Layer()", "orders-mysql", "pg_set_replica_identity", "postgres", true},
		{"mysql pipeline, connect action unchanged", "orders-mysql", "connect_restart_task", "kafka-connect", true},
		{"mysql pipeline, connect action, mysql layer", "orders-mysql", "connect_restart_task", "mysql", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := sourceLayerService(t)
			snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "Incident", "sev2", tc.pipeline)
			if err != nil {
				t.Fatalf("create room: %v", err)
			}
			_, err = svc.CreateProposal(context.Background(), snap.ID, events.ProposalCreatedPayload{
				Title: "fix", Risk: "low", Rollback: "undo",
				Steps: []events.ProposalStep{{Action: tc.action, Layer: tc.layer, Params: json.RawMessage(`{}`)}},
			}, events.AgentActor)
			var re *room.Error
			switch {
			case tc.ok && err != nil:
				t.Fatalf("create proposal: %v", err)
			case !tc.ok && !errors.As(err, &re):
				t.Fatalf("create proposal = %v, want a validation error", err)
			}
		})
	}
}

// A source_grant_select step on a MySQL pipeline needs the mysql owner: the
// postgres owner may not approve it.
func TestMySQLSourceStepNeedsTheMySQLOwner(t *testing.T) {
	svc := sourceLayerService(t)
	ctx := context.Background()
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "Incident", "sev2", "orders-mysql")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	pid := propose(t, svc, snap.ID, events.ProposalStep{Action: "source_grant_select", Layer: "mysql", Params: json.RawMessage(`{}`)})
	got, err := svc.GetRoom(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OpenProposals) != 1 || !slices.Equal(got.OpenProposals[0].RequiredLayers, []string{"mysql"}) {
		t.Fatalf("open proposals %+v, want one requiring [mysql]", got.OpenProposals)
	}
	hash := proposalHash(t, svc, pid)
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "mysql", hash); err == nil {
		t.Fatal("the postgres owner approved the mysql layer")
	}
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "arjun"), pid, "mysql", hash); err != nil {
		t.Fatalf("the mysql owner's approval: %v", err)
	}
}
