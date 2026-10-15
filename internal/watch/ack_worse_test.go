package watch

import (
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// A connector the driver closed the room on while it was PAUSED (sev3) that
// then goes FAILED (sev2) is worse than what the driver saw: it must reach a
// room, even inside the cooldown. Before, only a rise to sev1 re-opened, so
// the FAILED connector stayed acked until it cleared.
func TestWorseningAfterCloseReopens(t *testing.T) {
	paused := clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED", Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}}
	h := newHarness(t, opts{})
	h.conn.setStatus("pg-orders", paused)
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev3" {
		t.Fatalf("setup: want one sev3 room, got %+v", rooms)
	}
	r1 := rooms[0].roomID
	h.rooms.closeRoom(r1, h.clk.Now())
	h.w.OnRoomClosed(r1, "orders", DedupKey("orders"))
	h.step(20 * time.Second)
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked || len(h.rooms.created()) != 1 {
		t.Fatal("setup: the same PAUSED connector must stay acked")
	}

	h.breakConnector("orders", "org.postgresql.util.PSQLException: ERROR: must be owner of table orders")
	h.steps(3, 20*time.Second)
	rooms = h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.Severity != "sev2" {
		t.Fatalf("PAUSED -> FAILED after the close opened no sev2 room: %+v\n%s", rooms, h.logs)
	}
	ev := signalEv(t, evidenceOf(t, rooms[1].alert.Raw), owners.SignalConnectorDown)
	if !strings.Contains(string(rooms[1].alert.Raw), "must be owner of table orders") || ev["severity"] != "sev2" {
		t.Fatalf("evidence lacks the FAILED trace: %s", rooms[1].alert.Raw)
	}

	// Closed again at sev2: the same severity stays acked.
	r2 := rooms[1].roomID
	h.rooms.closeRoom(r2, h.clk.Now())
	h.w.OnRoomClosed(r2, "orders", DedupKey("orders"))
	h.steps(5, time.Minute)
	if len(h.rooms.created()) != 2 || h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatalf("an unchanged severity re-opened: %+v", h.rooms.created())
	}
}

// A room closed while the connector is RUNNING again but the signal still
// clears keeps the severity the driver saw (sev2), not the clear reading's "".
// Before, ackSev became "" (rank 9), so the same sev2 flap inside the clear
// window counted as a rise and re-opened a room past the cooldown (#391).
func TestCloseWhileClearingKeepsBreachSeverity(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "org.apache.kafka.connect.errors.ConnectException: boom")
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev2" {
		t.Fatalf("setup: want one sev2 room, got %+v", rooms)
	}
	h.fixConnector("orders")
	h.step(20 * time.Second)
	if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
		t.Fatal("setup: the signal must still be clearing (firing) when the room closes")
	}
	r1 := rooms[0].roomID
	h.rooms.closeRoom(r1, h.clk.Now())
	h.w.OnRoomClosed(r1, "orders", DedupKey("orders"))

	h.breakConnector("orders", "org.apache.kafka.connect.errors.ConnectException: boom")
	h.steps(3, 20*time.Second)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("a same-severity flap after the close re-opened inside the cooldown: %d rooms\n%s", n, h.logs)
	}
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatalf("the flap must stay acked, phase %v", h.phase("orders", owners.SignalConnectorDown))
	}
}

// The same for a storm follower: the storm room closes while b clears, and b
// flaps back at the same severity. The close acked b at its breach severity
// (assignFollowers/ackLocked), so no room opens inside the cooldown (#391).
func TestStormFollowerClosedWhileClearingStaysAcked(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	room := stormOf(t, h)
	h.fixConnector("b")
	h.step(20 * time.Second)
	if h.phase("b", owners.SignalConnectorDown) != phaseFiring {
		t.Fatal("setup: b must still be clearing when the room closes")
	}
	h.rooms.closeRoom(room, h.clk.Now())
	h.w.OnRoomClosed(room, "a", "a|watcher:a")

	h.breakConnector("b", "worker died")
	h.steps(3, 20*time.Second)
	if n := len(h.rooms.created()); n != 1 {
		t.Fatalf("a same-severity flap of a storm follower re-opened: %d rooms\n%s", n, h.logs)
	}
}

// A close while clearing must not hide a real rise: PAUSED (sev3) acked while
// RUNNING again, then FAILED (sev2) is worse and re-opens past the cooldown.
func TestCloseWhileClearingStillReopensOnEscalation(t *testing.T) {
	paused := clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED", Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}}
	h := newHarness(t, opts{})
	h.conn.setStatus("pg-orders", paused)
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev3" {
		t.Fatalf("setup: want one sev3 room, got %+v", rooms)
	}
	h.fixConnector("orders")
	h.step(20 * time.Second)
	h.rooms.closeRoom(rooms[0].roomID, h.clk.Now())
	h.w.OnRoomClosed(rooms[0].roomID, "orders", DedupKey("orders"))

	h.breakConnector("orders", "org.apache.kafka.connect.errors.ConnectException: boom")
	h.steps(3, 20*time.Second)
	rooms = h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.Severity != "sev2" {
		t.Fatalf("PAUSED -> FAILED after a close while clearing opened no sev2 room: %+v\n%s", rooms, h.logs)
	}
}
