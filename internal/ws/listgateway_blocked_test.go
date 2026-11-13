package ws_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// blockedAct is an action that is never run: write mode is off.
type blockedAct struct{ name, layer string }

func (a blockedAct) Name() string                                           { return a.name }
func (a blockedAct) Layer() string                                          { return a.layer }
func (a blockedAct) Describe() string                                       { return a.name }
func (blockedAct) ParamsSchema() map[string]any                             { return map[string]any{"type": "object"} }
func (blockedAct) Validate(context.Context, json.RawMessage) error          { return nil }
func (blockedAct) CheckScope(owners.Pipeline, json.RawMessage) error        { return nil }
func (blockedAct) Execute(context.Context, json.RawMessage) (string, error) { return "", nil }

// KAN-596: with write mode off the list socket's first frame and the "room"
// frame the hub pushes after the last approval carry blocked, GET /api/rooms
// agrees with the first frame, and the shared published snapshot is untouched.
func TestListGatewayCarriesBlockedWithWriteModeOff(t *testing.T) {
	st := memstore.New()
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{blockedAct{"pg_act", "postgres"}, blockedAct{"wh_act", "warehouse"}} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Run: fakeRun, WriteModeOff: true,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
	})
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	t.Cleanup(svc.Close)
	ctx := context.Background()
	snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "orders down", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "wh_act", Layer: "warehouse", Params: json.RawMessage("{}")},
		},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	hash := func() string {
		rooms, _ := svc.ListRooms(ctx)
		return rooms[0].OpenProposals[0].ParamsHash
	}
	approve := func(user, layer string) {
		t.Helper()
		if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, user), pid, layer, hash()); err != nil {
			t.Fatalf("%s approves %s: %v", user, layer, err)
		}
	}
	approve("priya", "postgres")

	conn := dialList(t, srv, "arjun")
	first, ok := readList(t, conn, 5*time.Second)
	if !ok || first.Type != "rooms" || len(first.Rooms) != 1 || first.Rooms[0].OpenProposals[0].Blocked != "" {
		t.Fatalf("first frame, a layer pending = %+v", first)
	}

	approve("meera", "warehouse")
	f, ok := readList(t, conn, 5*time.Second)
	if !ok || f.Type != "room" || len(f.Room.OpenProposals) != 1 || f.Room.OpenProposals[0].Blocked != room.BlockedWriteModeOff {
		t.Fatalf("frame after the last approval = %+v", f)
	}
	// The published snapshot the hub shares is still the fold of the log.
	pub, err := svc.GetRoom(ctx, snap.ID)
	if err != nil || pub.OpenProposals[0].Blocked != "" {
		t.Fatalf("published snapshot: %+v %v", pub.OpenProposals, err)
	}

	// A fresh socket and GET /api/rooms say the same.
	again := dialList(t, srv, "arjun")
	g, ok := readList(t, again, 5*time.Second)
	if !ok || g.Type != "rooms" || g.Rooms[0].OpenProposals[0].Blocked != room.BlockedWriteModeOff {
		t.Fatalf("first frame, all approved = %+v", g)
	}
	if got, want := frameRooms(t, g), getRoomsJSON(t, srv, "arjun"); got != want {
		t.Fatalf("first frame != GET /api/rooms:\n%s\n%s", got, want)
	}
}
