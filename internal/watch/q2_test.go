package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Q2 F1: an escalation that opened no room (cap) must not spend the
// once-per-episode budget; a later room closed while the signal fires
// re-opens once.
func TestQ2EscalationBudgetNotSpentByRoomlessEscalation(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}}, maxRooms: 1, escalateAfter: 10 * time.Minute})
	h.step(0)
	h.breakConnector("a", "x")
	h.steps(4, time.Minute)
	h.breakConnector("b", "x")
	h.steps(4, time.Minute)
	h.steps(30, time.Minute) // escalation at the cap: nothing opens
	h.fixConnector("a")
	a := h.rooms.created()[0].roomID
	h.rooms.closeRoom(a, h.clk.Now())
	h.w.OnRoomClosed(a, "a", "a|watcher:a")
	h.step(time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.PipelineID != "b" {
		t.Fatalf("setup: rooms=%d", len(rooms))
	}
	b := rooms[1].roomID
	h.rooms.closeRoom(b, h.clk.Now())
	h.w.OnRoomClosed(b, "b", "b|watcher:b")
	h.steps(120, time.Minute) // b's connector still FAILED
	if got := len(h.rooms.created()); got != 3 {
		t.Fatalf("rooms=%d, want 3 (b re-opened once after close while firing)\n%s", got, h.logs)
	}
	h.steps(120, time.Minute)
	if got := len(h.rooms.created()); got != 3 {
		t.Fatalf("rooms=%d, want still 3: the re-open happens once\n%s", got, h.logs)
	}
}

// Q2 F2: PAUSED (sev3) to FAILED (sev2) on the same connector after the room
// was closed is a new incident.
func TestQ2SeverityRiseAfterCloseIsNewIncident(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	h.conn.setStatus("pg-orders", clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED", Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}})
	h.steps(4, time.Minute)
	if len(h.rooms.created()) != 1 {
		t.Fatalf("setup: rooms=%d", len(h.rooms.created()))
	}
	r1 := h.rooms.created()[0].roomID
	h.closeNow(r1)
	h.step(19 * time.Second)
	h.breakConnector("orders", "must be owner of table orders")
	h.steps(5, 20*time.Second)
	if len(h.rooms.created()) != 2 {
		t.Fatalf("worsening to FAILED opened no room\n%s", h.logs)
	}
}

// Q2 F3: a close that lands while a new signal is being posted acks only the
// signals the room carried.
func TestQ2CloseDoesNotAckSignalTheRoomNeverHeard(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	r1 := h.rooms.created()[0].roomID
	h.rooms.closeRoom(r1, h.clk.Now()) // closed elsewhere; the watcher has not heard
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(12, time.Minute)
	if got := len(h.rooms.created()); got < 2 {
		t.Fatalf("slot_inactive never got a room: rooms=%d slot phase=%v\n%s", got, h.phase("orders", owners.SignalSlotInactive), h.logs)
	}
}
