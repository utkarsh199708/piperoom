package watch

import (
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// closeOpenRoom closes the watcher room roomID the way the room service does.
func closeOpenRoom(h *harness, pipe, roomID string) {
	h.rooms.closeRoom(roomID, h.clk.Now())
	h.w.OnRoomClosed(roomID, pipe, DedupKey(pipe))
}

// failedAndClosed breaks the connector, waits for the first room and closes it.
func failedAndClosed(t *testing.T, h *harness) string {
	t.Helper()
	h.breakConnector("orders", "org.apache.kafka.connect.errors.ConnectException: boom")
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("setup: want one room, got %d\n%s", len(rooms), h.logs)
	}
	closeOpenRoom(h, "orders", rooms[0].roomID)
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatal("setup: not acked after the close")
	}
	return rooms[0].roomID
}

// A person closing a room does not fix the fault: one cooldown later the
// signal that still fires opens a new room that says so (2026-10-25: the demo
// stayed silent until the stack was reset).
func TestClosedRoomStillFiringReopens(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute})
	failedAndClosed(t, h)

	h.steps(25, time.Minute) // inside the cooldown
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("re-opened inside the cooldown: %d rooms", n)
	}
	h.steps(10, time.Minute) // past it
	rooms := h.rooms.created()
	if len(rooms) != 2 {
		t.Fatalf("a signal still firing after the cooldown opened no room: %d\n%s", len(rooms), h.logs)
	}
	a := rooms[1].alert
	if !strings.Contains(a.Title, "reopened after close") {
		t.Fatalf("title lacks the marker: %q", a.Title)
	}
	ev := evidenceOf(t, a.Raw)
	if ev["reopened_after_close"] != true || ev["previous_room_closed_at"] == nil ||
		!strings.Contains(ev["reason"].(string), "reopened after close") {
		t.Fatalf("evidence lacks the marker: %s", a.Raw)
	}
	if a.DedupKey != DedupKey("orders") {
		t.Fatalf("dedup key = %q", a.DedupKey)
	}
	// The new room is open: later ticks join it, they do not open more.
	h.steps(60, time.Minute)
	if n := len(h.rooms.created()); n != 2 {
		t.Fatalf("rooms = %d, want 2 while the second room stays open", n)
	}
	if h.logs.count("opening a new room") != 1 {
		t.Fatalf("re-arm logged %d times\n%s", h.logs.count("opening a new room"), h.logs)
	}
}

// Flapping by design: while the fault persists, every closed room is followed
// by another one cooldown later.
func TestReopenedRoomClosedAgainReopensAgain(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute})
	failedAndClosed(t, h)
	h.steps(35, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 {
		t.Fatalf("first reopen: %d rooms", len(rooms))
	}
	closeOpenRoom(h, "orders", rooms[1].roomID)
	h.steps(25, time.Minute)
	if n := len(h.rooms.created()); n != 2 {
		t.Fatalf("second reopen inside the cooldown: %d rooms", n)
	}
	h.steps(10, time.Minute)
	if n := len(h.rooms.created()); n != 3 {
		t.Fatalf("second close: %d rooms, want 3\n%s", n, h.logs)
	}
}

// A signal that clears before the cooldown is over opens nothing, and a normal
// refire later is a normal new incident (no reopen marker).
func TestClosedRoomSignalClearsBeforeCooldownOpensNothing(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute})
	failedAndClosed(t, h)
	h.steps(10, time.Minute)
	h.fixConnector("orders")
	h.steps(4, time.Minute)
	if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
		t.Fatal("did not clear")
	}
	h.steps(60, time.Minute)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("a cleared signal opened %d rooms", n)
	}
	h.breakConnector("orders", "boom again")
	h.steps(5, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 || strings.Contains(rooms[1].alert.Title, "reopened") {
		t.Fatalf("a refire after a clear: %+v", rooms)
	}
}

// A signal that is clearing when the cooldown ends is not re-armed.
func TestClosedRoomSignalClearingAtCooldownStaysAcked(t *testing.T) {
	h := newHarness(t, opts{cooldown: 10 * time.Minute})
	failedAndClosed(t, h)
	h.steps(8, time.Minute)
	h.fixConnector("orders")
	h.step(2 * time.Minute) // past the cooldown, first clear reading
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatalf("phase = %v, want acked while clearing", h.phase("orders", owners.SignalConnectorDown))
	}
	h.steps(5, time.Minute)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("a clearing signal opened %d rooms", n)
	}
}

// Several signals acked by one close come back as one room.
func TestClosedRoomTwoSignalsReopenAsOneRoom(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute})
	h.breakConnector("orders", "boom")
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(8, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("setup: %d rooms\n%s", len(rooms), h.logs)
	}
	joinsBefore := len(h.rooms.postsContaining("new signal"))
	closeOpenRoom(h, "orders", rooms[0].roomID)
	for _, s := range []string{owners.SignalConnectorDown, owners.SignalSlotInactive} {
		if h.phase("orders", s) != phaseAcked {
			t.Fatalf("setup: %s not acked", s)
		}
	}
	h.steps(35, time.Minute)
	rooms = h.rooms.created()
	if len(rooms) != 2 {
		t.Fatalf("two acked signals made %d new rooms, want 1\n%s", len(rooms)-1, h.logs)
	}
	ev := evidenceOf(t, rooms[1].alert.Raw)
	for _, s := range []string{owners.SignalConnectorDown, owners.SignalSlotInactive} {
		signalEv(t, ev, s)
	}
	if n := len(h.rooms.postsContaining("new signal")); n != joinsBefore {
		t.Fatalf("a second signal joined separately: %d posts", n)
	}
}

// The open-rooms cap holds the reopen back, with one log line, until a room
// frees up.
func TestClosedRoomReopenRespectsTheCap(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute, maxRooms: 1,
		pipes: []pipeSpec{{id: "orders"}, {id: "billing"}}})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	first := h.rooms.created()
	if len(first) != 1 {
		t.Fatalf("setup: %d rooms", len(first))
	}
	closeOpenRoom(h, "orders", first[0].roomID)
	h.breakConnector("billing", "boom")
	h.steps(4, time.Minute) // billing takes the only slot
	if len(h.rooms.created()) != 2 {
		t.Fatalf("setup: billing room missing\n%s", h.logs)
	}
	h.steps(40, time.Minute) // orders' cooldown is over, the cap is full
	if n := len(h.rooms.created()); n != 2 {
		t.Fatalf("the cap was exceeded: %d rooms", n)
	}
	if h.logs.count("max_open_watcher_rooms reached") != 1 {
		t.Fatalf("cap not logged once\n%s", h.logs)
	}
	billing := h.rooms.created()[1].roomID
	closeOpenRoom(h, "billing", billing)
	h.steps(3, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 3 || rooms[2].alert.PipelineID != "orders" || !strings.Contains(rooms[2].alert.Title, "reopened") {
		t.Fatalf("orders did not get its room once a slot was free: %+v", rooms)
	}
}

// Closed at sev3, still sev3 one cooldown later: the room reopens at the
// severity the signal has now.
func TestClosedRoomReopensAtCurrentSeverity(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute})
	h.conn.setStatus("pg-orders", clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED",
		Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}})
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev3" {
		t.Fatalf("setup: %+v", rooms)
	}
	closeOpenRoom(h, "orders", rooms[0].roomID)
	h.steps(35, time.Minute)
	rooms = h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.Severity != "sev3" || !strings.Contains(rooms[1].alert.Title, "reopened") {
		t.Fatalf("PAUSED connector after the cooldown: %+v", rooms)
	}
}

// After a server restart the in-memory acked state is gone. The signal fires
// again, and openRoom waits out the cooldown from the room service's closed
// time, then opens a plain room (the marker needs the acked memory).
func TestClosedRoomAcrossRestart(t *testing.T) {
	h := newHarness(t, opts{cooldown: 30 * time.Minute})
	failedAndClosed(t, h)
	w2, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w2
	h.steps(10, time.Minute)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("a restart re-opened inside the cooldown: %d rooms", n)
	}
	h.steps(30, time.Minute)
	if n := len(h.rooms.created()); n != 2 {
		t.Fatalf("after the restart and the cooldown: %d rooms\n%s", n, h.logs)
	}
}
