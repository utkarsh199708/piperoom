//go:build integration

package room

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// A room deleted on one replica closes its WebSockets on the others: the
// replica that notices it gone (here B's re-list) calls OnRoomDeleted with the
// frame built from the shared audit row.
func TestReplicasDeleteTellsOtherReplicas(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	users, err := ParseUsers([]byte("users:\n  - id: arjun\n    name: Arjun\n    layers: [kafka]\n  - id: ada\n    name: Ada\n    admin: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	withUsers := func(d *Deps) { d.Users = users }
	a := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r0", log, nil, withUsers)
	calls := make(chan hookCall, 4)
	b := newReplica(t, pgReplicaStore(t, schema, store.Options{NoListen: true, PollInterval: time.Hour}), "r1", log, nil, withUsers,
		func(d *Deps) {
			d.OnRoomDeleted = func(roomID string, ev events.Event) { calls <- hookCall{roomID, ev} }
		})
	ctx := context.Background()
	snap, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetRoom(ctx, snap.ID); err != nil {
		t.Fatal(err)
	}
	d, err := a.DeleteRoom(ctx, user(t, a, "ada"), snap.ID, "test data", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ListRooms(ctx); err != nil {
		t.Fatal(err)
	}
	c := nextHookCall(t, calls)
	var p events.RoomDeletedPayload
	_ = json.Unmarshal(c.ev.Payload, &p)
	if c.roomID != snap.ID || c.ev.Type != events.RoomDeleted || c.ev.Seq != d.EventCount+1 || c.ev.Actor.ID != "ada" || p.Reason != "test data" {
		t.Fatalf("hook call on B = %+v (%s)", c, c.ev.Payload)
	}
}
