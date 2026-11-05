package ws_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// P2b-2: the first member to open a driverless alert room's WebSocket becomes
// its driver; the grant reaches that connection (and the log) as the next
// event after the replay, and /metrics counts it.
func TestSoleMemberJoinGrantsOverWebSocket(t *testing.T) {
	srv, svc := newRoomServer(t)
	ctx := context.Background()
	snap, created, err := svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", json.RawMessage(`{"alert":"lag"}`))
	if err != nil || !created {
		t.Fatalf("alert room: created=%v err=%v", created, err)
	}

	priya := dialWS(t, srv, snap.ID, "priya", 0)
	if e := readEvent(t, priya); e.Type != events.RoomCreated {
		t.Fatalf("first event = %s, want room.created", e.Type)
	}
	e := readEvent(t, priya)
	p, err := events.Decode[events.ControlGrantedPayload](e)
	if err != nil || e.Type != events.ControlGranted || p.Reason != events.ControlReasonSoleMember || p.To.ID != "priya" ||
		e.Actor.Kind != events.ActorSystem {
		t.Fatalf("event after the join = %s %+v by %+v (err %v), want control.granted sole_member to priya by the system", e.Type, p, e.Actor, err)
	}

	// A second member joining changes nothing: the service tests cover it
	// (internal/room solemember_internal_test.go); the gateway runs the
	// join after its handshake, so a test here could not tell "nothing
	// happened" from "not yet".
	got, err := svc.GetRoom(ctx, snap.ID)
	if err != nil || got.Driver == nil || got.Driver.ID != "priya" {
		t.Fatalf("driver %+v (err %v), want priya", got.Driver, err)
	}

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if want := "piperoom_sole_member_grants_total 1\n"; !strings.Contains(string(b), want) {
		t.Fatalf("metrics lack %q:\n%s", want, b)
	}
}
