package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
)

// Q4 R1: a target the closed incident did not carry, on an acked signal, is
// posted at once, also after the escalation room was closed.
func TestQ4R1NewTargetOfAckedSignalOpensARoom(t *testing.T) {
	for _, escalated := range []bool{false, true} {
		name := "before escalation"
		if escalated {
			name = "after the escalation room was closed"
		}
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, opts{q1Sink: true, escalateAfter: 10 * time.Minute,
				pipes: []pipeSpec{{id: "orders", extraConn: []string{"wh-sink"}}}})
			h.conn.cfg["wh-sink"] = map[string]string{"connector.class": "x.Sink"}
			h.conn.setStatus("wh-sink", running("wh-sink"))
			h.breakConnector("orders", "boom")
			h.steps(4, time.Minute)
			h.closeNow(h.rooms.created()[0].roomID)
			if escalated {
				h.steps(12, time.Minute)
				rs := h.rooms.created()
				if len(rs) != 2 {
					t.Fatalf("rooms = %d, want the escalation room", len(rs))
				}
				h.closeNow(rs[1].roomID)
			}
			before := len(h.rooms.created())
			h.conn.setStatus("wh-sink", failed("wh-sink", "unrelated"))
			h.steps(8, time.Minute)
			if got := len(h.rooms.created()); got <= before {
				t.Fatalf("second connector failure opened no room (%d rooms)\n%s", got, h.logs)
			}
		})
	}
}

// Q4 R2: after a restart the re-attach to the watcher's own open room uses
// the room's carried keys: a new key wakes the agent, a carried one does not.
func TestQ4R2ReattachUsesRoomKeys(t *testing.T) {
	cases := []struct {
		name string
		keys string
		via  string
	}{
		{"new signal", "connector_down:pg-orders", "incident"},
		{"already carried", "slot_inactive:debezium_orders", "evidence"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{q3Sink: true})
			h.rooms.byKey["orders|watcher:orders"] = "r_before"
			h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_before"}
			h.q3.setKeys("r_before", tc.keys)
			h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
			h.steps(6, time.Minute)
			posts := h.rooms.postsContaining("re-attached after a restart")
			if len(posts) == 0 || posts[0].via != tc.via {
				t.Fatalf("posts = %+v, want via %s\n%s", posts, tc.via, h.logs)
			}
		})
	}
}

// Q4 R2: a re-attached room older than JoinMaxAge is told as a new incident.
func TestQ4R2ReattachStaleRoomWakes(t *testing.T) {
	h := newHarness(t, opts{q3Sink: true, joinMaxAge: time.Hour})
	h.rooms.byKey["orders|watcher:orders"] = "r_before"
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_before"}
	h.q3.setKeys("r_before", "slot_inactive:debezium_orders")
	h.q3.setInfo("orders", RoomInfo{RoomID: "r_before", AlertSource: "watcher", LastEventAt: h.clk.Now().Add(-24 * time.Hour)})
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	posts := h.rooms.postsContaining("re-attached after a restart")
	if len(posts) == 0 || posts[0].via != "incident" {
		t.Fatalf("posts = %+v, want via incident", posts)
	}
}
