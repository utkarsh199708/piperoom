package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
)

// Q6 U2: the cooldown only stops flaps. A signal that stayed clear for the
// sustained window after its room closed is a new incident and opens at once;
// a clear gap shorter than the window stays quiet.
func TestU2SustainedClearEndsTheCooldown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		healthy   int // minutes the pipeline stays healthy after the close
		wantRooms int
	}{
		{"short gap stays quiet", 5, 1},
		{"sustained clear opens", 12, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{q1Sink: true}) // cooldown 60m
			h.breakConnector("orders", "boom")
			h.steps(4, time.Minute)
			h.fixConnector("orders")
			h.steps(4, time.Minute) // resolved
			h.closeNow(h.rooms.created()[0].roomID)
			h.steps(tc.healthy, time.Minute)
			h.breakConnector("orders", "boom")
			h.steps(5, time.Minute)
			if got := len(h.rooms.created()); got != tc.wantRooms {
				t.Fatalf("rooms = %d, want %d\n%s", got, tc.wantRooms, h.logs)
			}
		})
	}
}

func TestU2SustainedClearWindow(t *testing.T) {
	if got := sustainedClear(60 * time.Minute); got != clearWindow {
		t.Fatalf("60m cooldown = %v", got)
	}
	if got := sustainedClear(20 * time.Minute); got != 5*time.Minute {
		t.Fatalf("20m cooldown = %v", got)
	}
}

// Q6 U7: a restart that sees the state already stored as the newest row writes
// nothing, even on another day; a different state is still recorded once.
func TestU7RestartDoesNotRecordAPhantomFirstSighting(t *testing.T) {
	h := newChangesHarness(t)
	h.conn.setStatus("pg-orders", clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED",
		Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}})
	h.step(0)
	if len(h.ch.all()) != 2 {
		t.Fatalf("setup rows = %v", kinds(h.ch.all()))
	}
	restart := func() {
		d := h.deps
		d.Changes, d.Registry, d.DDL = h.ch, h.reg, h.ddl
		w, err := New(d)
		if err != nil {
			t.Fatal(err)
		}
		h.w = w
	}
	restart()
	h.step(25 * time.Hour) // the day-keyed fingerprint no longer dedups
	if len(h.ch.all()) != 2 {
		t.Fatalf("restart wrote a phantom change: %v", kinds(h.ch.all()))
	}
	// Stored PAUSED, but the connector is FAILED now (a change the watcher
	// missed): recorded once.
	h.conn.setStatus("pg-orders", failed("pg-orders", "boom"))
	restart()
	h.step(time.Minute)
	if got := len(h.ch.all()); got < 3 {
		t.Fatalf("a missed change was not recorded: %v", kinds(h.ch.all()))
	}
}
