package watch

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

func (h *harness) roomOf(pipe string) string {
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	if ps := h.w.pipes[pipe]; ps != nil {
		return ps.roomID
	}
	return ""
}

// ---- closed rooms are never revived ----

func TestRoomClosedWhilePostingIsNotRevived(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	room := h.rooms.created()[0].roomID

	// The driver closes the room just after the next post went through.
	h.rooms.onPosted = func(id string) {
		h.rooms.closeRoom(id, h.clk.Now())
		h.w.OnRoomClosed(id, "orders", "orders|watcher:orders")
	}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute) // slot_inactive fires: the post is the one in flight
	h.rooms.onPosted = nil

	if got := h.roomOf("orders"); got != "" {
		t.Fatalf("the pipeline took the closed room %s as its own again", got)
	}
	for _, sig := range []string{owners.SignalConnectorDown, owners.SignalSlotInactive} {
		if h.phase("orders", sig) != phaseAcked {
			t.Fatalf("%s phase = %v, want acked", sig, h.phase("orders", sig))
		}
	}
	posts := len(h.rooms.postsContaining("watcher:"))
	h.steps(60, time.Minute)
	if got := len(h.rooms.postsContaining("watcher:")); got != posts {
		t.Fatalf("posts to a closed room: %d more", got-posts)
	}
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1 (same incident, acked)", got)
	}
	_ = room
}

func TestRoomClosedWhileOpeningIsAcked(t *testing.T) {
	h := newHarness(t, opts{})
	h.rooms.onOpened = func(id string) {
		h.rooms.closeRoom(id, h.clk.Now())
		h.w.OnRoomClosed(id, "orders", "orders|watcher:orders")
	}
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	h.rooms.onOpened = nil
	if got := h.roomOf("orders"); got != "" {
		t.Fatalf("room %s kept after it closed during the open", got)
	}
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatalf("phase = %v, want acked", h.phase("orders", owners.SignalConnectorDown))
	}
	h.steps(30, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}
	if got := len(h.rooms.postsContaining("watcher:")); got != 0 {
		t.Fatalf("posts to a closed room: %d", got)
	}
}

func TestMissedCloseEventSelfHeals(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	room := h.rooms.created()[0].roomID
	// The room closes but OnRoomClosed never arrives.
	h.rooms.closeRoom(room, h.clk.Now())
	h.steps(16, time.Minute) // the 15-minute "still firing" post is refused: room closed
	if got := h.roomOf("orders"); got != "" {
		t.Fatalf("still posting to %s", got)
	}
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatalf("phase = %v, want acked", h.phase("orders", owners.SignalConnectorDown))
	}
	h.steps(30, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}
}

// ---- error paths ----

func TestOpenAlertRoomFailureWarnsOncePerEpisodeAndRetries(t *testing.T) {
	h := newHarness(t, opts{})
	var mu sync.Mutex
	fail := true
	h.rooms.openErr = func(alertin.Alert) error {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			return errors.New("store: connection refused")
		}
		return nil
	}
	h.breakConnector("orders", "boom")
	h.steps(10, time.Minute)
	if got := len(h.rooms.created()); got != 0 {
		t.Fatalf("rooms = %d while the store is down", got)
	}
	if n := strings.Count(h.logs.String(), `level=WARN msg="watch: could not open a room`); n != 1 {
		t.Fatalf("warnings = %d, want 1 per episode\n%s", n, h.logs)
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	h.step(time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms after the store came back = %d, want 1", got)
	}
}

func TestPostEvidenceFailureIsRetried(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	var mu sync.Mutex
	failures := 2
	h.rooms.postErr = func(string) error {
		mu.Lock()
		defer mu.Unlock()
		if failures > 0 {
			failures--
			return errors.New("store: timeout")
		}
		return nil
	}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(5, time.Minute) // slot_inactive fires at 5 min; two posts fail
	h.steps(3, time.Minute)
	got := h.rooms.postsContaining("slot debezium_orders inactive")
	if len(got) != 1 {
		t.Fatalf("slot_inactive reached the room %d times, want exactly 1 after retries\n%s", len(got), h.logs)
	}
	if h.logs.count("could not post to the room") < 2 {
		t.Fatal("failed posts were not logged")
	}
}

func TestResolveRetriesTransientErrorsAndStopsOnNoRoom(t *testing.T) {
	t.Run("transient error is retried", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.breakConnector("orders", "boom")
		h.steps(4, time.Minute)
		var mu sync.Mutex
		attempts := 0
		h.rooms.openErr = func(a alertin.Alert) error {
			if a.Status != alertin.StatusResolved {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts <= 2 {
				return errors.New("store: timeout")
			}
			return nil
		}
		h.fixConnector("orders")
		h.steps(8, time.Minute)
		if got := len(h.rooms.resolved()); got != 1 {
			t.Fatalf("resolved alerts delivered = %d, want 1 after retries (attempts %d)\n%s", got, attempts, h.logs)
		}
		mu.Lock()
		defer mu.Unlock()
		if attempts != 3 {
			t.Fatalf("attempts = %d, want 3", attempts)
		}
	})
	t.Run("ErrNoRoom ends the incident", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.breakConnector("orders", "boom")
		h.steps(4, time.Minute)
		attempts := 0
		h.rooms.openErr = func(a alertin.Alert) error {
			if a.Status == alertin.StatusResolved {
				attempts++
				return fmt.Errorf("room: %w", ErrNoRoom)
			}
			return nil
		}
		h.fixConnector("orders")
		h.steps(12, time.Minute)
		if attempts != 1 {
			t.Fatalf("attempts = %d, want 1: ErrNoRoom must not be retried", attempts)
		}
		h.w.mu.Lock()
		episode := h.w.pipes["orders"].episode
		h.w.mu.Unlock()
		if episode {
			t.Fatal("the incident is still open after ErrNoRoom")
		}
	})
}

// ---- restart ----

func TestRestartJoinsTheOpenWatcherRoomWithoutASecondOne(t *testing.T) {
	h := newHarness(t, opts{})
	// A watcher room from before the restart is still open.
	h.rooms.byKey["orders|watcher:orders"] = "r_before"
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_before"}
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)

	if got := len(h.rooms.created()); got != 0 {
		t.Fatalf("created %d rooms next to the open watcher room", got)
	}
	var joins int
	for _, o := range h.rooms.opens {
		if o.roomID == "r_before" && !o.created {
			joins++
		}
	}
	if joins != 1 {
		t.Fatalf("joins by OpenAlertRoom = %d, want 1", joins)
	}
	if got := h.rooms.postsContaining("re-attached after a restart"); len(got) != 1 || got[0].roomID != "r_before" {
		t.Fatalf("evidence not posted after the join: %+v", got)
	}
	if got := h.roomOf("orders"); got != "r_before" {
		t.Fatalf("room = %q, want r_before", got)
	}
	// And it counts as one of ours for the cap, and is resolved normally.
	h.fixConnector("orders")
	h.steps(5, time.Minute)
	if got := len(h.rooms.resolved()); got != 1 {
		t.Fatalf("resolved alerts = %d, want 1", got)
	}
}

// ---- storms ----

func TestStormFollowersPostIntoTheLeadersRoom(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	h.step(0)
	for _, p := range []string{"a", "b", "c"} {
		h.breakConnector(p, "worker died")
	}
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	room := rooms[0].roomID
	for _, p := range []string{"a", "b", "c"} {
		if got := h.roomOf(p); got != room {
			t.Fatalf("pipeline %s room = %q, want the storm room %s", p, got, room)
		}
	}

	// A later signal on a follower lands in the storm room, not in a new one.
	h.src.update("debezium_b", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("a follower opened its own room: %d rooms", got)
	}
	if got := h.rooms.postsContaining("slot debezium_b inactive"); len(got) != 1 || got[0].roomID != room {
		t.Fatalf("follower update = %+v, want one post into %s", got, room)
	}

	// Followers do not repeat "still firing"; the leader does.
	h.steps(20, time.Minute)
	for _, p := range h.rooms.postsContaining("still firing") {
		if !strings.Contains(p.text, "watcher: a:") {
			t.Fatalf("a follower repeated still-firing: %s", p.text)
		}
	}

	// Their clears land there too.
	for _, p := range []string{"a", "b", "c"} {
		h.fixConnector(p)
	}
	h.src.update("debezium_b", func(r *clients.ReplicationSlot) { r.Active = true })
	h.steps(7, time.Minute)
	for _, p := range []string{"b", "c"} {
		if got := h.rooms.postsContaining(p + ": all watcher signals are clear"); len(got) != 1 || got[0].roomID != room {
			t.Fatalf("clear of %s = %+v, want one post into %s", p, got, room)
		}
	}
	if got := len(h.rooms.resolved()); got != 1 {
		t.Fatalf("resolved alerts = %d, want 1 (the leader's)", got)
	}
}

func TestOnRoomClosedAcksEveryPipelineInTheStormRoom(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	h.step(0)
	for _, p := range []string{"a", "b", "c"} {
		h.breakConnector(p, "x")
	}
	h.steps(4, time.Minute)
	room := h.rooms.created()[0].roomID
	h.rooms.closeRoom(room, h.clk.Now())
	h.w.OnRoomClosed(room, "a", "a|watcher:a")
	for _, p := range []string{"a", "b", "c"} {
		if h.phase(p, owners.SignalConnectorDown) != phaseAcked || h.roomOf(p) != "" {
			t.Fatalf("pipeline %s not acked after its room closed", p)
		}
	}
	h.steps(20, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}
}

func TestStormLeaderFailureDoesNotLetFollowersOpenTheirOwn(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	var mu sync.Mutex
	attempted := map[string]int{}
	fail := true
	h.rooms.openErr = func(a alertin.Alert) error {
		mu.Lock()
		defer mu.Unlock()
		attempted[a.PipelineID]++
		if fail {
			return errors.New("store: down")
		}
		return nil
	}
	h.step(0)
	for _, p := range []string{"a", "b", "c"} {
		h.breakConnector(p, "worker died")
	}
	h.steps(6, time.Minute) // the storm fires at tick 4; the leader fails on every tick after
	mu.Lock()
	if attempted["b"] != 0 || attempted["c"] != 0 || attempted["a"] == 0 {
		t.Fatalf("open attempts = %v, want only the leader a", attempted)
	}
	fail = false
	mu.Unlock()
	h.step(time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.PipelineID != "a" {
		t.Fatalf("rooms after recovery = %+v, want one on the leader", rooms)
	}
	aff, _ := evidenceOf(t, rooms[0].alert.Raw)["affected_pipelines"].([]any)
	if len(aff) != 3 {
		t.Fatalf("the carried storm lost its members: %v", aff)
	}
	if h.roomOf("b") != rooms[0].roomID || h.roomOf("c") != rooms[0].roomID {
		t.Fatal("followers were not put into the storm room")
	}
}

func TestStormLeaderIsNotInCooldown(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	closed := h.clk.Now().Add(-10 * time.Minute)
	h.rooms.last["a|watcher:a"] = lastRoom{roomID: "r_old", closedAt: &closed}
	h.step(0)
	for _, p := range []string{"a", "b", "c"} {
		h.breakConnector(p, "worker died")
	}
	h.steps(5, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.PipelineID != "b" {
		t.Fatalf("rooms = %+v, want one room led by b (a is in cooldown)", rooms)
	}
	if h.roomOf("a") != "" {
		t.Fatal("a pipeline in cooldown was pulled into the storm room")
	}
	if h.roomOf("c") != rooms[0].roomID {
		t.Fatal("c was not put into the storm room")
	}
}

// ---- severity rules from the review ----

func TestETASev1IsNotCappedByMissingHeartbeat(t *testing.T) {
	h := newHarness(t, opts{noHeartbeat: true})
	safe := gib
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.SafeWALBytes = &safe })
	for minute := 0; minute <= 55; minute++ {
		h.src.setRetained("debezium_orders", 2*gib+int64(minute)*10*mib)
		h.step(time.Minute)
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev3" {
		t.Fatalf("want one room opened at the capped sev3, got %+v", rooms)
	}
	if got := len(h.rooms.postsContaining("severity raised to sev1")); got != 1 {
		t.Fatalf("sev1 raises = %d, want 1: the cap must not hide an ETA sev1", got)
	}
}

func TestRiseToSev1ReopensAClosedRoomOncePerEpisode(t *testing.T) {
	h := newHarness(t, opts{}) // cooldown 60 min
	safe := gib
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.SafeWALBytes = &safe })
	closeNow := func() {
		id := h.roomOf("orders")
		h.rooms.closeRoom(id, h.clk.Now())
		h.w.OnRoomClosed(id, "orders", "orders|watcher:orders")
	}
	var closedAt20 bool
	for minute := 0; minute <= 90; minute++ {
		h.src.setRetained("debezium_orders", 2*gib+int64(minute)*10*mib)
		h.step(time.Minute)
		if minute == 20 {
			closeNow() // the driver closes the sev2 room
			closedAt20 = true
		}
		if minute >= 12 && minute < 45 {
			if got := len(h.rooms.created()); got != 1 {
				t.Fatalf("minute %d: rooms = %d, want 1 (acked, cooldown running)", minute, got)
			}
		}
		if minute == 55 && len(h.rooms.created()) == 2 {
			closeNow() // closed again: this must not come back
		}
	}
	if !closedAt20 {
		t.Fatal("setup")
	}
	rooms := h.rooms.created()
	if len(rooms) != 2 {
		t.Fatalf("rooms = %d, want 2 (one re-open on the rise to sev1)\n%s", len(rooms), h.logs)
	}
	if rooms[1].alert.Severity != "sev1" {
		t.Fatalf("re-opened at %s, want sev1", rooms[1].alert.Severity)
	}
}

// ---- slot name, evidence size ----

func TestInvalidSlotNameIsNeverReplacedByAnotherSlot(t *testing.T) {
	// A blank slot.name is set, so it is invalid too, not "use the default" (T2).
	for _, name := range []string{"Bad-Name", "", "   "} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			h := newHarness(t, opts{})
			h.conn.mu.Lock()
			h.conn.cfg["pg-orders"] = map[string]string{
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
				"slot.name":       name,
			}
			h.conn.mu.Unlock()
			// Another pipeline's slot, named like Debezium's default, is breaching.
			h.src.update("debezium", func(r *clients.ReplicationSlot) { r.Active, r.RetainedWALBytes = false, 5*gib })
			h.steps(15, time.Minute)
			if h.phase("orders", owners.SignalSlotInactive) != phaseOK || h.phase("orders", owners.SignalSlotWAL) != phaseOK {
				t.Fatal("the pipeline took another slot")
			}
			if got := len(h.rooms.created()); got != 0 {
				t.Fatalf("rooms = %d", got)
			}
			if !strings.Contains(h.logs.String(), "invalid slot.name") {
				t.Fatalf("no warning\n%s", h.logs)
			}
		})
	}
}

func TestSlotNameRule(t *testing.T) {
	for name, want := range map[string]bool{
		"debezium_orders": true, "9slot": true, "a": true, strings.Repeat("a", 63): true,
		"": false, "Upper": false, "has-dash": false, "dot.name": false, strings.Repeat("a", 64): false,
	} {
		if slotNameRE.MatchString(name) != want {
			t.Errorf("slotNameRE(%q) = %v, want %v", name, !want, want)
		}
	}
}

func TestEvidenceStaysUnder4KiBWithManyFailedTasks(t *testing.T) {
	h := newHarness(t, opts{})
	st := clients.ConnectorStatus{Name: "pg-orders", State: "RUNNING"}
	for i := range 80 {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: i, State: "FAILED", Trace: strings.Repeat("e", 1000) + "\nat x"})
	}
	h.conn.setStatus("pg-orders", st)
	h.steps(5, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d", len(rooms))
	}
	if n := len(rooms[0].alert.Raw); n > maxEvidenceBytes {
		t.Fatalf("evidence is %d bytes, want <= %d", n, maxEvidenceBytes)
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), owners.SignalConnectorDown)
	conn := ev["connectors"].([]any)[0].(map[string]any)
	if got := len(conn["tasks"].([]any)); got != maxEvidenceTasks {
		t.Fatalf("tasks listed = %d, want %d", got, maxEvidenceTasks)
	}
	if conn["tasks_omitted"] == nil {
		t.Fatal("omitted tasks are not counted")
	}
}
