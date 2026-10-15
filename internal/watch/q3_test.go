package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Q3 (real-stack QA round 1): S1 unknown incidents after a restart, S2 join
// rules on every join, S7 Connect unreachable, S8 first-seen connector state.

// ---- S1 ----

// Without a ClosedRoomKeysSink, a close the watcher never saw cools only the
// signals that were already firing at the close: a signal that started after
// it (here: after a restart) opens a room at once.
func TestS1UnknownCloseDoesNotSilenceANewSignal(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	closed := h.clk.Now().Add(-10 * time.Minute)
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_old", closedAt: &closed}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute) // slot_inactive fires at t0+6
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1: a signal that fired after the close must not wait out its cooldown\n%s", got, h.logs)
	}
	if h.logs.count("cooldown after a closed watcher room") != 0 {
		t.Fatalf("cooldown logged\n%s", h.logs)
	}
}

// The fallback rule itself: a pending signal that fired before the unseen
// close is cooled, one that fired after it is not.
func TestS1UnknownCloseCoolsOnlySignalsFiringAtTheClose(t *testing.T) {
	h := newHarness(t, opts{})
	now := t0.Add(time.Hour)
	closed := now.Add(-10 * time.Minute)
	h.w.mu.Lock()
	h.w.pipe("orders")
	h.w.mu.Unlock()
	before := sigSnap{id: owners.SignalConnectorDown, keys: []string{"connector_down:pg-orders"}, fired: closed.Add(-time.Minute)}
	after := sigSnap{id: owners.SignalSlotInactive, keys: []string{"slot_inactive:debezium_orders"}, fired: closed.Add(time.Minute)}
	cases := []struct {
		name string
		pend []sigSnap
		want bool
	}{
		{"fired before the close", []sigSnap{before}, true},
		{"fired after the close", []sigSnap{after}, false},
		{"both", []sigSnap{before, after}, false},
		{"fired exactly at the close", []sigSnap{{id: "x", keys: []string{"x"}, fired: closed}}, true},
	}
	for _, tc := range cases {
		h.w.mu.Lock()
		q := h.w.cooldownOf(now, "orders", tc.pend, "r_old", &closed, true)
		h.w.mu.Unlock()
		if q.on != tc.want {
			t.Errorf("%s: quiet = %v, want %v", tc.name, q.on, tc.want)
		}
		if q.on && (!q.unknown || q.room != "r_old") {
			t.Errorf("%s: quiet = %+v, want the unknown incident of r_old", tc.name, q)
		}
	}
}

// With a ClosedRoomKeysSink the closed room's incident is recovered: its own
// signal stays quiet for the cooldown, another one opens a room at once.
func TestS1RecoveredKeysKeyTheCooldownAfterARestart(t *testing.T) {
	h := newHarness(t, opts{q3Sink: true, escalateAfter: 30 * time.Minute})
	closed := h.clk.Now().Add(-10 * time.Minute)
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_old", closedAt: &closed}
	h.q3.setKeys("r_old", "connector_down:pg-orders")
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute) // connector_down fires at t0+4: the same incident
	if got := len(h.rooms.created()); got != 0 {
		t.Fatalf("rooms = %d, the closed room's own signal must stay quiet\n%s", got, h.logs)
	}
	if !strings.Contains(h.logs.String(), `incident="same signals"`) {
		t.Fatalf("cooldown line does not name the recovered incident\n%s", h.logs)
	}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute) // slot_inactive fires: outside the incident
	rooms := h.rooms.created()
	if len(rooms) != 1 || !strings.Contains(rooms[0].alert.Title, "slot debezium_orders inactive") {
		t.Fatalf("rooms = %+v, want one for slot_inactive\n%s", rooms, h.logs)
	}
}

// A recovered room with no keys (an older watcher wrote its evidence) falls
// back to the firing-at-close rule.
func TestS1RecoveryWithoutKeysFallsBack(t *testing.T) {
	h := newHarness(t, opts{q3Sink: true})
	closed := h.clk.Now().Add(-10 * time.Minute)
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_old", closedAt: &closed}
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1\n%s", got, h.logs)
	}
}

// The evidence names its incident keys, so the room service can give them back.
func TestS1EvidenceCarriesKeys(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d", len(rooms))
	}
	if got := EvidenceKeys(rooms[0].alert.Raw); !slices.Equal(got, []string{"connector_down:pg-orders"}) {
		t.Fatalf("EvidenceKeys = %v\n%s", got, rooms[0].alert.Raw)
	}
	for _, raw := range []string{``, `{`, `{"source":"webhook","keys":["a"]}`, `{"source":"watcher"}`} {
		if got := EvidenceKeys(json.RawMessage(raw)); got != nil {
			t.Errorf("EvidenceKeys(%q) = %v, want nil", raw, got)
		}
	}
	if got := EvidenceKeys(json.RawMessage(`{"source":"watcher","keys":["b","a","b",""]}`)); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("EvidenceKeys dedup = %v", got)
	}
}

// ---- S2 ----

// A recent alert room whose title is flagged as a prompt injection is not
// joined; the watcher opens its own.
func TestS2FlaggedTitleIsNotJoined(t *testing.T) {
	h := newHarness(t, opts{q3Sink: true})
	h.q1.setInfo("orders", RoomInfo{RoomID: "r_inj", AlertSource: "webhook", OpenedAt: t0, LastEventAt: t0})
	h.q3.setFlags("r_inj", "instruction_override")
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if got := len(h.rooms.postsTo("r_inj")); got != 0 {
		t.Fatalf("posted %d times into a flagged room", got)
	}
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want the watcher's own\n%s", got, h.logs)
	}
	if h.logs.count("title is flagged") != 1 {
		t.Fatalf("passing over the flagged room not logged once\n%s", h.logs)
	}
	// A clean recent alert room is still joined.
	h2 := newHarness(t, opts{q3Sink: true})
	h2.q1.setInfo("orders", RoomInfo{RoomID: "r_ok", AlertSource: "webhook", OpenedAt: t0, LastEventAt: t0})
	h2.q3.setFlags("r_ok")
	h2.breakConnector("orders", "boom")
	h2.steps(4, time.Minute)
	if len(h2.rooms.postsTo("r_ok")) != 1 || len(h2.rooms.created()) != 0 {
		t.Fatalf("clean room not joined\n%s", h2.logs)
	}
}

// The escalation (closed while firing, kept firing) opens under the same
// join rules: an old manual room and a flagged room are passed over.
func TestS2EscalationAppliesTheJoinRules(t *testing.T) {
	cases := []struct {
		name  string
		info  func(now time.Time) RoomInfo
		flags []string
	}{
		{"old manual room", func(now time.Time) RoomInfo {
			return RoomInfo{RoomID: "r_x", OpenedAt: now.Add(-3 * time.Hour), LastEventAt: now.Add(-time.Minute)}
		}, nil},
		{"flagged alert room", func(now time.Time) RoomInfo {
			return RoomInfo{RoomID: "r_x", AlertSource: "webhook", OpenedAt: now.Add(-time.Minute), LastEventAt: now}
		}, []string{"role_play"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{q3Sink: true, escalateAfter: 10 * time.Minute})
			h.breakConnector("orders", "boom")
			h.steps(4, time.Minute)
			r1 := h.rooms.created()[0].roomID
			h.closeNow(r1)
			h.q1.setInfo("orders", tc.info(h.clk.Now()))
			if tc.flags != nil {
				h.q3.setFlags("r_x", tc.flags...)
			}
			h.steps(11, time.Minute)
			if got := len(h.rooms.postsTo("r_x")); got != 0 {
				t.Fatalf("the escalation joined r_x\n%s", h.logs)
			}
			rooms := h.rooms.created()
			if len(rooms) != 2 || !strings.Contains(rooms[1].alert.Title, "kept firing") {
				t.Fatalf("rooms = %+v, want the escalation's own room\n%s", rooms, h.logs)
			}
		})
	}
}

// A new incident in a room the watcher only joined is a join too: once that
// room is no longer joinable, the new signal goes into the watcher's own room.
func TestS2NewIncidentInAJoinedRoomRechecksTheRules(t *testing.T) {
	h := newHarness(t, opts{q3Sink: true})
	h.q1.setInfo("orders", RoomInfo{RoomID: "r_p", OpenedAt: t0, LastEventAt: t0})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute) // joins r_p (a person's room, 4 minutes old)
	if len(h.rooms.postsTo("r_p")) != 1 {
		t.Fatalf("setup: not joined\n%s", h.logs)
	}
	h.steps(120, time.Minute) // r_p was opened over 2 h ago now
	h.q1.setInfo("orders", RoomInfo{RoomID: "r_p", OpenedAt: t0, LastEventAt: h.clk.Now()})
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	for _, p := range h.rooms.postsTo("r_p") {
		if strings.Contains(p.text, "inactive") {
			t.Fatalf("the new incident went into the old person's room: %q", p.text)
		}
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 || !strings.Contains(rooms[0].alert.Title, "inactive") {
		t.Fatalf("rooms = %+v, want the watcher's own room with slot_inactive\n%s", rooms, h.logs)
	}
	// A repeat ("still firing") of what r_p already carries kept going there
	// while it was joined.
	if len(h.rooms.postsContaining("(still firing)")) == 0 {
		t.Fatal("no still-firing post")
	}
}

// ---- S7 ----

func connRefused() error {
	return fmt.Errorf("connect: GET /connectors/pg-orders/status: %w", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})
}

func TestS7ConnectUnreachableFiresAfterNFailedTicks(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0)
	h.conn.setStatusErr("pg-orders", connRefused())
	h.steps(connectUnreachableTicks-1, time.Minute)
	if h.phase("orders", SignalConnectUnreachable) != phaseOK {
		t.Fatalf("fired before %d failed ticks", connectUnreachableTicks)
	}
	h.step(time.Minute)
	if h.phase("orders", SignalConnectUnreachable) != phaseFiring {
		t.Fatalf("not firing after %d failed ticks\n%s", connectUnreachableTicks, h.logs)
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 || !strings.Contains(rooms[0].alert.Title, "Kafka Connect not answering") || rooms[0].alert.Severity != "sev2" {
		t.Fatalf("rooms = %+v", rooms)
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), SignalConnectUnreachable)
	if ev["layer"] != "kafka-connect" || ev["error_kind"] != "network" || ev["failed_checks"] != float64(connectUnreachableTicks) {
		t.Fatalf("evidence = %v", ev)
	}
	if strings.Contains(string(rooms[0].alert.Raw), "connection refused") {
		t.Fatal("raw error text in the evidence")
	}
	// Connect answers again: the signal clears after connector_down's `for`.
	h.conn.setStatus("pg-orders", running("pg-orders"))
	h.steps(4, time.Minute)
	if h.phase("orders", SignalConnectUnreachable) != phaseOK {
		t.Fatalf("did not clear\n%s", h.logs)
	}
}

func TestS7ConnectUnreachableIgnoresNotFoundAndMissingClient(t *testing.T) {
	t.Run("404", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.conn.setStatusErr("pg-orders", fmt.Errorf("connect: %w", clients.ErrNotFound))
		h.steps(5, time.Minute)
		if h.phase("orders", SignalConnectUnreachable) != phaseOK {
			t.Fatal("a 404 counted as unreachable")
		}
	})
	t.Run("one failed tick between good ones", func(t *testing.T) {
		h := newHarness(t, opts{})
		for range 5 {
			h.conn.setStatusErr("pg-orders", connRefused())
			h.step(time.Minute)
			h.conn.setStatus("pg-orders", running("pg-orders"))
			h.step(time.Minute)
		}
		if h.phase("orders", SignalConnectUnreachable) != phaseOK {
			t.Fatal("non-consecutive failures fired")
		}
	})
	t.Run("no Connect client", func(t *testing.T) {
		h := newHarness(t, opts{})
		d := h.deps
		d.Connect = nil
		w, err := New(d)
		if err != nil {
			t.Fatal(err)
		}
		h.w = w
		h.steps(5, time.Minute)
		if h.phase("orders", SignalConnectUnreachable) != phaseOK {
			t.Fatal("a missing client counted as unreachable")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		if k := connectErrKind(fmt.Errorf("x: %w", context.DeadlineExceeded)); k != "timeout" {
			t.Fatalf("kind = %q", k)
		}
		if k := connectErrKind(errors.New("HTTP 500")); k != "error" {
			t.Fatalf("kind = %q", k)
		}
	})
}

// With Connect down from the start, watch.slot keeps the slot signals working.
func TestS7SlotSignalsUseWatchSlotWhileConnectIsDown(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders", watch: "    watch:\n      slot: debezium_orders\n"}}})
	h.conn.setStatusErr("pg-orders", connRefused())
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	if h.phase("orders", owners.SignalSlotInactive) != phaseFiring {
		t.Fatalf("slot_inactive not firing with Connect down\n%s", h.logs)
	}
}

// Without watch.slot the error says how to fix it.
func TestS7SlotUnknownNamesWatchSlot(t *testing.T) {
	h := newHarness(t, opts{})
	h.conn.setStatusErr("pg-orders", connRefused())
	h.step(0)
	if !strings.Contains(h.logs.String(), "set watch.slot") {
		t.Fatalf("no hint\n%s", h.logs)
	}
}

// ---- S8 ----

// A connector first seen in a non-RUNNING state (the watcher started after
// the pause) records that state; a RUNNING first sighting records nothing.
func TestS8FirstSightingRecordsANonRunningState(t *testing.T) {
	h := newChangesHarness(t)
	h.conn.setStatus("pg-orders", clients.ConnectorStatus{Name: "pg-orders", State: "PAUSED",
		Tasks: []clients.TaskStatus{{ID: 0, State: "PAUSED"}}})
	h.step(0)
	rows := h.ch.all()
	if got := kinds(rows); !slices.Equal(got, []string{"connector_state pg-orders", "connector_state pg-orders/0"}) {
		t.Fatalf("rows = %v", got)
	}
	if a := jsonMap(t, rows[0].After); a["state"] != "PAUSED" {
		t.Fatalf("after = %s", rows[0].After)
	}
	if !strings.Contains(rows[0].Summary, "PAUSED") || !strings.Contains(rows[0].Summary, "first seen") {
		t.Fatalf("summary = %q", rows[0].Summary)
	}
	// Same state again: nothing. A restarted watcher seeing it again the same
	// day writes the same fingerprint (no duplicate row).
	h.step(time.Minute)
	if len(h.ch.all()) != 2 {
		t.Fatalf("repeat wrote rows: %v", kinds(h.ch.all()))
	}
	d := h.deps
	d.Changes, d.Registry, d.DDL = h.ch, h.reg, h.ddl
	w, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	h.step(time.Minute)
	if len(h.ch.all()) != 2 {
		t.Fatalf("restart duplicated the first-seen rows: %v", kinds(h.ch.all()))
	}
	// The way back is a normal transition.
	h.conn.setStatus("pg-orders", running("pg-orders"))
	h.step(time.Minute)
	if rows := h.ch.all(); len(rows) != 4 || !strings.Contains(rows[2].Summary, "PAUSED → RUNNING") {
		t.Fatalf("rows = %v", kinds(rows))
	}
}

// A failed first-seen write is retried on the next tick.
func TestS8FirstSightingWriteIsRetried(t *testing.T) {
	h := newChangesHarness(t)
	h.conn.setStatus("pg-orders", failed("pg-orders", "boom"))
	h.ch.setErr(errors.New("db down"))
	h.step(0)
	h.ch.setErr(nil)
	h.step(time.Minute)
	if got := kinds(h.ch.all()); !slices.Equal(got, []string{"connector_state pg-orders", "connector_state pg-orders/0"}) {
		t.Fatalf("rows = %v", got)
	}
}
