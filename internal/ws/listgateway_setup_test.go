package ws_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

const setupListUsers = `users:
  - {id: alice, name: Alice}
  - {id: ann, name: Ann}
  - {id: bob, name: Bob}
`

const setupListOwners = `tenants:
  a: {members: [user:alice, user:ann]}
  b: {members: [user:bob]}
pipelines:
  - id: existing
    tenant: a
    topic_prefix: ex
    source_tables: [public.e]
    connectors: [ex-c]
    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}
    owners:
      postgres: {approvers: [user:alice]}
`

// setupListServer is a server with two tenants (a: alice, ann; b: bob) and
// setup rooms on.
func setupListServer(t *testing.T) (*httptest.Server, *room.Service) {
	t.Helper()
	users, err := room.ParseUsers([]byte(setupListUsers))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(setupListOwners), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own, Run: fakeRun,
		Setup: config.Setup{Enabled: true, SecretsDir: t.TempDir()},
	})
	srv := httptest.NewServer(httpapi.New(svc, st, nil))
	t.Cleanup(srv.Close)
	t.Cleanup(svc.Close)
	return srv, svc
}

// frameMember returns one member of a room frame's room, exactly as sent, and
// whether the frame has it.
func frameMember(t *testing.T, f listFrame, key string) (string, bool) {
	t.Helper()
	var env struct {
		Room map[string]json.RawMessage `json:"room"`
	}
	if err := json.Unmarshal([]byte(f.raw), &env); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	v, ok := env.Room[key]
	return string(v), ok
}

// KAN-602: with tenancy on, a setup room's live frames carry the list form of
// its setup member (as a GET /api/rooms row does) and reach its own tenant,
// never another one; an incident room's frames carry no setup member.
func TestListGatewaySetupRoomFrames(t *testing.T) {
	srv, svc := setupListServer(t)
	ctx := context.Background()
	alice := mustUser(t, svc, "alice")
	res, err := svc.CreateSetupRoom(ctx, alice, events.SetupRequest{
		PipelineID: "orders", Tenant: "a", Title: "Orders setup", SourceKind: "postgres", Target: "compose",
		TopicPrefix: "orders", Tables: []string{"public.orders"}, Connectors: []string{"orders-src"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	})
	if err != nil {
		t.Fatalf("CreateSetupRoom: %v", err)
	}
	id := res.Room.ID

	mine, mate, other := dialList(t, srv, "alice"), dialList(t, srv, "ann"), dialList(t, srv, "bob")
	for name, c := range map[string]*listClient{"alice": mine, "ann": mate, "bob": other} {
		f, ok := readList(t, c, 5*time.Second)
		if !ok || f.Type != "rooms" {
			t.Fatalf("%s first frame = %+v", name, f)
		}
		if want := name != "bob"; (len(f.Rooms) == 1) != want {
			t.Fatalf("%s first frame has %d rooms", name, len(f.Rooms))
		}
	}

	// The runner pairs: runner.connected changes, and the published snapshot
	// gets a full setup member.
	if err := svc.AttachRunner(ctx, res.Code, room.Runner{
		Tenant: "a", RoomID: id, Creator: events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"},
		Label: "laptop", OS: "linux", Arch: "amd64", Token: "tok-1", TokenExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("attach runner: %v", err)
	}
	const wantSetup = `{"mode":true,"runner":{"connected":true},"phase":""}`
	for name, c := range map[string]*listClient{"alice": mine, "ann": mate} {
		f, ok := readList(t, c, 5*time.Second)
		if !ok || f.Type != "room" || f.Room == nil || f.Room.ID != id {
			t.Fatalf("%s: no live frame for the setup room: %+v", name, f)
		}
		if got, _ := frameMember(t, f, "setup"); got != wantSetup {
			t.Errorf("%s: setup member = %s, want %s", name, got, wantSetup)
		}
	}
	if f, ok := readList(t, other, 700*time.Millisecond); ok {
		t.Fatalf("another tenant got a frame for the setup room: %s", f.raw)
	}

	// GET /api/rooms agrees, and the shared snapshot still has the full member.
	var rows []struct {
		Setup json.RawMessage `json:"setup"`
	}
	if err := json.Unmarshal([]byte(getRoomsJSON(t, srv, "alice")), &rows); err != nil || len(rows) != 1 || string(rows[0].Setup) != wantSetup {
		t.Fatalf("GET /api/rooms setup = %+v %v", rows, err)
	}
	pub, err := svc.GetRoom(ctx, id)
	if err != nil || pub.Setup == nil || pub.Setup.Runner == nil || pub.Setup.Runner.Label != "laptop" || len(pub.Setup.Targets) != 1 {
		t.Fatalf("published snapshot lost its full setup member: %+v %v", pub.Setup, err)
	}

	// The runner drops: a digest change sends the row again, in list form.
	svc.RunnerLost(ctx, "a", id, "alice", "tok-1")
	for name, c := range map[string]*listClient{"alice": mine, "ann": mate} {
		f, ok := readList(t, c, 5*time.Second)
		got, _ := frameMember(t, f, "setup")
		if !ok || f.Type != "room" || got != `{"mode":true,"runner":{"connected":false},"phase":""}` {
			t.Fatalf("%s: frame after the runner dropped = %s", name, f.raw)
		}
	}

	// An incident room on a registered pipeline: frames as before, no setup.
	inc, err := svc.CreateRoom(ctx, alice, "orders down", "sev2", "existing")
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]*listClient{"alice": mine, "ann": mate} {
		f, ok := readList(t, c, 5*time.Second)
		if !ok || f.Type != "room" || f.Room.ID != inc.ID {
			t.Fatalf("%s: incident frame = %+v", name, f)
		}
		if v, has := frameMember(t, f, "setup"); has && v != "null" {
			t.Errorf("%s: incident frame has setup %s", name, v)
		}
	}
	if f, ok := readList(t, other, 700*time.Millisecond); ok {
		t.Fatalf("another tenant got an incident frame: %s", f.raw)
	}
}
