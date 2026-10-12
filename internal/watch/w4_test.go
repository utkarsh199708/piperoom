package watch

import (
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// stormOf breaks a, b and c and runs until the storm room exists. It returns
// the room id.
func stormOf(t *testing.T, h *harness) string {
	t.Helper()
	h.step(0)
	for _, p := range []string{"a", "b", "c"} {
		h.breakConnector(p, "worker died")
	}
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("storm rooms = %d, want 1\n%s", len(rooms), h.logs)
	}
	return rooms[0].roomID
}

func TestStormFollowerHonoursTheCooldownAfterTheStormRoomCloses(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}}) // cooldown 60 min
	room := stormOf(t, h)
	h.rooms.closeRoom(room, h.clk.Now())
	h.w.OnRoomClosed(room, "a", "a|watcher:a")

	// The follower b recovers and fails again inside the cooldown.
	h.fixConnector("b")
	h.steps(6, time.Minute)
	if h.phase("b", owners.SignalConnectorDown) != phaseOK {
		t.Fatal("b did not clear")
	}
	h.breakConnector("b", "worker died")
	h.steps(8, time.Minute)
	if h.phase("b", owners.SignalConnectorDown) != phaseFiring {
		t.Fatal("b did not fire again")
	}
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("a follower opened a room inside the cooldown: %d rooms\n%s", got, h.logs)
	}
	if h.logs.count("cooldown after a closed watcher room") != 1 {
		t.Fatalf("cooldown not logged once\n%s", h.logs)
	}

	// After the cooldown it opens.
	h.step(61 * time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.PipelineID != "b" {
		t.Fatalf("rooms after the cooldown = %+v, want a second one on b", rooms)
	}
}

func TestStormFollowersStillFiringAfterTheLeaderResolved(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	room := stormOf(t, h)
	countStill := func(p string) int {
		n := 0
		for _, post := range h.rooms.postsContaining("(still firing)") {
			if post.roomID == room && strings.HasPrefix(post.text, "watcher: "+p+":") {
				n++
			}
		}
		return n
	}

	// Only the leader recovers.
	h.fixConnector("a")
	for i := 0; i < 30 && len(h.rooms.resolved()) == 0; i++ {
		h.step(time.Minute)
	}
	if len(h.rooms.resolved()) != 1 {
		t.Fatalf("the leader did not resolve\n%s", h.logs)
	}
	if countStill("b") != 0 || countStill("c") != 0 {
		t.Fatal("a follower posted still-firing while the leader was in the room")
	}

	// Now the followers remind, every 15 minutes at most.
	for i := 0; i < 40 && countStill("b") == 0; i++ {
		h.step(time.Minute)
	}
	if countStill("b") != 1 {
		t.Fatalf("follower b never posted still-firing\n%s", h.logs)
	}
	h.steps(10, time.Minute)
	if countStill("b") != 1 {
		t.Fatalf("follower b posted again inside 15 minutes: %d", countStill("b"))
	}
	h.steps(10, time.Minute)
	if countStill("b") != 2 {
		t.Fatalf("follower b posts = %d after 20 minutes, want 2", countStill("b"))
	}
	if countStill("c") < 2 {
		t.Fatalf("follower c posts = %d, want at least 2", countStill("c"))
	}
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}

	// A closed room ends the reminders.
	h.rooms.closeRoom(room, h.clk.Now())
	h.w.OnRoomClosed(room, "", "")
	before := countStill("b")
	h.steps(40, time.Minute)
	if countStill("b") != before {
		t.Fatal("a follower kept posting into a closed room")
	}
}

func TestStripTracesDoesNotChangeTheStoredReading(t *testing.T) {
	h := newHarness(t, opts{})
	// "<" is escaped to six bytes in JSON, so three traces pass 4 KiB.
	trace := strings.Repeat("<", 400)
	h.conn.setStatus("pg-orders", clients.ConnectorStatus{Name: "pg-orders", State: "FAILED", Tasks: []clients.TaskStatus{
		{ID: 0, State: "FAILED", Trace: trace}, {ID: 1, State: "FAILED", Trace: trace}, {ID: 2, State: "FAILED", Trace: trace},
	}})
	h.steps(5, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	raw := string(rooms[0].alert.Raw)
	if strings.Contains(raw, `"trace"`) {
		t.Fatalf("the evidence still has traces (%d bytes)", len(raw))
	}
	if len(raw) > maxEvidenceBytes {
		t.Fatalf("evidence is %d bytes", len(raw))
	}
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	ev := h.w.pipes["orders"].sigs[owners.SignalConnectorDown].last.ev
	conns := ev["connectors"].([]map[string]any)
	tasks := conns[0]["tasks"].([]map[string]any)
	if len(tasks) != 3 {
		t.Fatalf("stored tasks = %d, want 3", len(tasks))
	}
	for _, tk := range tasks {
		if _, ok := tk["trace"]; !ok {
			t.Fatalf("the stored reading lost a trace: %v", tk)
		}
	}
}

func TestResolvedAlertFailureWarnsOncePerEpisode(t *testing.T) {
	h := newHarness(t, opts{})
	failResolve := func() {
		h.rooms.openErr = func(a alertin.Alert) error {
			if a.Status == alertin.StatusResolved {
				return errors.New("store: down")
			}
			return nil
		}
	}
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	failResolve()
	h.fixConnector("orders")
	h.steps(12, time.Minute)
	warn := `level=WARN msg="watch: resolved alert failed`
	if n := h.logs.count(warn); n != 1 {
		t.Fatalf("resolved-failed warnings = %d, want 1\n%s", n, h.logs)
	}
	if h.logs.count("watch: resolved alert failed") < 3 {
		t.Fatal("the retries left no debug line")
	}

	// The retry goes through; the next episode warns again.
	h.rooms.openErr = nil
	h.steps(2, time.Minute)
	if len(h.rooms.resolved()) != 1 {
		t.Fatal("the resolve did not go through")
	}
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	failResolve()
	h.fixConnector("orders")
	h.steps(12, time.Minute)
	if n := h.logs.count(warn); n != 2 {
		t.Fatalf("resolved-failed warnings after a second episode = %d, want 2", n)
	}
}

func TestTargetsCountsWatchedPipelines(t *testing.T) {
	pipes := []pipeSpec{
		{id: "off", watch: "    watch:\n      enabled: false\n"},
		{id: "none", watch: "    watch:\n      signals:\n        connector_down: {enabled: false}\n        slot_inactive: {enabled: false}\n        slot_wal: {enabled: false}\n"},
		{id: "some", watch: "    watch:\n      signals:\n        connector_down: {enabled: false}\n"},
		{id: "all"},
	}
	h := newHarness(t, opts{pipes: pipes})
	if got := h.w.Targets(); got != 2 {
		t.Fatalf("Targets = %d, want 2", got)
	}
}
