package main

import (
	"testing"

	"piperoom/internal/events"
)

// Before the handler exists the hook drops the call (no socket can be open);
// after set it passes the room and its frame through unchanged.
func TestRoomDeletedHook(t *testing.T) {
	h := &roomDeletedHook{}
	h.onDeleted("r_early", events.Event{Type: events.RoomDeleted}) // must not panic

	var gotID string
	var gotEv events.Event
	calls := 0
	h.set(func(roomID string, deleted events.Event) {
		calls++
		gotID, gotEv = roomID, deleted
	})
	want := events.Event{RoomID: "r_1", Seq: 4, Type: events.RoomDeleted, Payload: []byte(`{"reason":"x"}`)}
	h.onDeleted("r_1", want)
	if calls != 1 || gotID != "r_1" || gotEv.Seq != 4 || gotEv.Type != events.RoomDeleted || string(gotEv.Payload) != `{"reason":"x"}` {
		t.Fatalf("calls %d, got %s %+v", calls, gotID, gotEv)
	}
}
