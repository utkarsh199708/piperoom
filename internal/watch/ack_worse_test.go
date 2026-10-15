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
