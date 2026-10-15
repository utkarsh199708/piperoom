package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

const (
	gib = int64(1 << 30)
	mib = int64(1 << 20)
)

// ---- the time-based `for` ----

func TestForIsTimeBased(t *testing.T) {
	t.Run("for minus one second does not fire, for fires", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.step(0) // healthy first tick
		h.breakConnector("orders", "boom")
		h.step(time.Second)                     // breaching_since = this tick
		h.step(3*time.Minute - time.Second - 0) // for - 1s after breaching_since
		if got := len(h.rooms.created()); got != 0 {
			t.Fatalf("rooms after for-1s = %d, want 0", got)
		}
		if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
			t.Fatal("fired too early")
		}
		h.step(time.Second)
		if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
			t.Fatal("did not fire at for")
		}
		if got := len(h.rooms.created()); got != 1 {
			t.Fatalf("rooms at for = %d, want 1", got)
		}
	})

	t.Run("an error tick in the middle still fires on time", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.breakConnector("orders", "boom")
		h.step(0)
		h.conn.setStatusErr("pg-orders", errors.New("connect: timeout"))
		h.step(90 * time.Second) // unknown: timers untouched
		if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
			t.Fatal("fired on an error tick")
		}
		h.breakConnector("orders", "boom")
		h.step(89 * time.Second) // 179 s since the first breach
		if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
			t.Fatal("fired before for")
		}
		h.step(time.Second) // 180 s
		if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
			t.Fatal("an error tick delayed the signal")
		}
	})

	t.Run("an error tick does not reset a clear timer", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.breakConnector("orders", "boom")
		h.steps(4, time.Minute) // firing
		h.fixConnector("orders")
		h.step(time.Minute) // clearing_since
		h.conn.setStatusErr("pg-orders", errors.New("connect: timeout"))
		h.step(time.Minute)
		h.fixConnector("orders")
		h.step(time.Minute)
		h.step(time.Minute) // 3 min since the first clear tick
		if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
			t.Fatal("did not resolve on time")
		}
	})
}

func TestSlotWALHysteresisNoFlap(t *testing.T) {
	// defaults: fire 1 GiB, clear 768 MiB, for 10 m
	slot := "debezium_orders"
	newFiring := func(t *testing.T) *harness {
		h := newHarness(t, opts{})
		h.src.setRetained(slot, 2*gib)
		h.steps(11, time.Minute)
		if h.phase("orders", owners.SignalSlotWAL) != phaseFiring {
			t.Fatal("setup: slot_wal not firing")
		}
		return h
	}

	t.Run("hovering between clear and fire never resolves", func(t *testing.T) {
		h := newFiring(t)
		for i := range 40 {
			v := 800 * mib // between clear and fire
			if i%2 == 1 {
				v = gib + 10*mib
			}
			h.src.setRetained(slot, v)
			h.step(time.Minute)
			if h.phase("orders", owners.SignalSlotWAL) != phaseFiring {
				t.Fatalf("flapped at step %d", i)
			}
		}
		if n := h.logs.count("signal resolved"); n != 0 {
			t.Fatalf("resolved lines = %d, want 0", n)
		}
	})

	t.Run("below clear for for-1 then back in between does not resolve", func(t *testing.T) {
		h := newFiring(t)
		h.src.setRetained(slot, 500*mib)
		h.step(time.Minute)     // clearing_since
		h.step(9 * time.Minute) // 9 min below clear, for = 10 min
		h.src.setRetained(slot, 800*mib)
		h.step(time.Minute) // between: clear timer restarts
		h.src.setRetained(slot, 500*mib)
		h.step(time.Minute)
		h.step(9 * time.Minute)
		if h.phase("orders", owners.SignalSlotWAL) != phaseFiring {
			t.Fatal("resolved although the clear timer was reset")
		}
		h.step(time.Minute)
		if h.phase("orders", owners.SignalSlotWAL) != phaseOK {
			t.Fatal("did not resolve after for below clear")
		}
	})

	t.Run("while ok a value that dips below fire restarts the fire timer", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.src.setRetained(slot, 2*gib)
		h.steps(6, time.Minute) // 5 min over fire
		h.src.setRetained(slot, 900*mib)
		h.step(time.Minute) // hold: timer reset
		h.src.setRetained(slot, 2*gib)
		h.steps(9, time.Minute) // 8 min over fire since the restart
		if h.phase("orders", owners.SignalSlotWAL) != phaseOK {
			t.Fatal("fired although the breach was interrupted")
		}
		h.steps(3, time.Minute)
		if h.phase("orders", owners.SignalSlotWAL) != phaseFiring {
			t.Fatal("did not fire 10 min after the restart")
		}
	})
}

// ---- severity ----

func TestConnectorSeverity(t *testing.T) {
	cases := []struct {
		name string
		st   clients.ConnectorStatus
		err  error
		want string
	}{
		{"FAILED", failed("pg-orders", "x"), nil, "sev2"},
		{"missing (404)", clients.ConnectorStatus{}, fmt.Errorf("connect: GET: %w", clients.ErrNotFound), "sev2"},
		{"PAUSED", clients.ConnectorStatus{State: "PAUSED", Tasks: []clients.TaskStatus{{State: "PAUSED"}}}, nil, "sev3"},
		{"STOPPED", clients.ConnectorStatus{State: "STOPPED"}, nil, "sev3"},
		{"UNASSIGNED", clients.ConnectorStatus{State: "UNASSIGNED"}, nil, "sev2"},
		{"RUNNING with zero tasks", clients.ConnectorStatus{State: "RUNNING"}, nil, "sev2"},
		{"RUNNING with a failed task", clients.ConnectorStatus{State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}, {ID: 1, State: "FAILED"}}}, nil, "sev2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, opts{})
			h.conn.setStatus("pg-orders", c.st)
			if c.err != nil {
				h.conn.setStatusErr("pg-orders", c.err)
			}
			h.steps(5, time.Minute)
			rooms := h.rooms.created()
			if len(rooms) != 1 {
				t.Fatalf("rooms = %d, want 1\n%s", len(rooms), h.logs)
			}
			if got := rooms[0].alert.Severity; got != c.want {
				t.Fatalf("severity = %s, want %s", got, c.want)
			}
		})
	}
}

func TestHeartbeatCapsSlotWALAtSev3(t *testing.T) {
	cases := []struct {
		name     string
		noHB     bool
		active   bool
		cfgErr   bool
		wantSev  string
		wantNote bool
	}{
		{"no heartbeat, active slot, running connector", true, true, false, "sev3", true},
		{"heartbeat configured", false, true, false, "sev2", false},
		{"slot inactive: no cap", true, false, false, "sev2", false},
		{"config unreadable: no cap", true, true, true, "sev2", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pipes := []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        slot_inactive: {enabled: false}\n"}}
			h := newHarness(t, opts{pipes: pipes, noHeartbeat: c.noHB})
			h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active, r.RetainedWALBytes = c.active, 2*gib })
			if c.cfgErr {
				h.conn.mu.Lock()
				h.conn.cfgErr["pg-orders"] = errors.New("config: timeout")
				h.conn.mu.Unlock()
			}
			h.steps(12, time.Minute)
			rooms := h.rooms.created()
			if len(rooms) != 1 {
				t.Fatalf("rooms = %d, want 1\n%s", len(rooms), h.logs)
			}
			if got := rooms[0].alert.Severity; got != c.wantSev {
				t.Fatalf("severity = %s, want %s", got, c.wantSev)
			}
			note, has := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), owners.SignalSlotWAL)["note"].(string)
			if has != c.wantNote || (has && !strings.Contains(note, "no heartbeat configured")) {
				t.Fatalf("note = %q (present %v), want present %v", note, has, c.wantNote)
			}
		})
	}
}

func TestLostSlotIsSev1AtOnce(t *testing.T) {
	for _, status := range []string{"lost", "unreserved"} {
		t.Run(status, func(t *testing.T) {
			h := newHarness(t, opts{noHeartbeat: true}) // the cap must not hide it
			h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.WALStatus, r.RetainedWALBytes = status, 10*mib })
			h.step(0) // the very first tick: no waiting for `for`
			rooms := h.rooms.created()
			if len(rooms) != 1 {
				t.Fatalf("rooms = %d, want 1", len(rooms))
			}
			if got := rooms[0].alert.Severity; got != "sev1" {
				t.Fatalf("severity = %s, want sev1", got)
			}
			if !strings.Contains(rooms[0].alert.Title, status) {
				t.Fatalf("title %q does not name wal_status %s", rooms[0].alert.Title, status)
			}
		})
	}
}

func TestNegativeSafeWALIsSev1AtOnceWithoutETA(t *testing.T) {
	h := newHarness(t, opts{noHeartbeat: true})
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) {
		r.SafeWALBytes, r.RetainedWALBytes = ptr(-100*mib), 10*mib // wal_status still "reserved" in the row
	})
	h.step(0)
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev1" {
		t.Fatalf("want one sev1 room at the first tick, got %+v", rooms)
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), owners.SignalSlotWAL)
	if _, has := ev["eta_hours"]; has || ev["eta_note"] == nil {
		t.Fatalf("want no ETA and a reason, got %v", ev)
	}
}

func TestETASev1NeedsTwiceFor(t *testing.T) {
	const slot = "debezium_orders"
	h := newHarness(t, opts{})
	safe := gib
	h.src.update(slot, func(r *clients.ReplicationSlot) { r.SafeWALBytes = &safe })

	// +600 MiB/h from 2 GiB: with 1 GiB safe, the slot is lost in about 1.7 h < 6 h.
	// The rate needs 30 min of samples; the estimate then has to hold 2×for = 20 min.
	for minute := 0; minute <= 60; minute++ {
		h.src.setRetained(slot, 2*gib+int64(minute)*10*mib)
		h.step(map[bool]time.Duration{true: 0, false: time.Minute}[minute == 0])
		raised := len(h.rooms.postsContaining("severity raised to sev1"))
		switch {
		case minute < 50 && raised > 0:
			t.Fatalf("sev1 raised at minute %d, before 30 min of samples + 2×for", minute)
		case minute == 50 && raised != 1:
			t.Fatalf("sev1 not raised at minute 50 (posts %d)\n%s", raised, h.logs)
		}
	}
	rooms := h.rooms.created()
	if len(rooms) != 1 || rooms[0].alert.Severity != "sev2" {
		t.Fatalf("opened %d rooms, first severity %v; want one room opened at sev2", len(rooms), rooms)
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), owners.SignalSlotWAL)
	if _, ok := ev["rate_1h_bytes_per_hour"]; ok {
		t.Fatalf("the room opened at minute 10 must not carry a rate yet: %v", ev)
	}
	if ev["eta_note"] == nil {
		t.Fatalf("warm-up evidence should say why there is no ETA: %v", ev)
	}
}

func TestEvidenceETABasis(t *testing.T) {
	run := func(t *testing.T, headroom string, safe *int64) map[string]any {
		pipes := []pipeSpec{{id: "orders", watch: "    watch:\n" + headroom}}
		h := newHarness(t, opts{pipes: pipes})
		h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.SafeWALBytes = safe })
		for minute := 0; minute <= 45; minute++ {
			h.src.setRetained("debezium_orders", 2*gib+int64(minute)*10*mib)
			h.step(time.Minute)
		}
		// The slot_wal evidence of the latest "still firing" or severity post.
		posts := h.rooms.postsContaining("evidence:")
		if len(posts) == 0 {
			t.Fatalf("no evidence post\n%s", h.logs)
		}
		text := posts[len(posts)-1].text
		raw := text[strings.Index(text, "evidence: ")+len("evidence: "):]
		return signalEv(t, evidenceOf(t, []byte(raw)), owners.SignalSlotWAL)
	}
	t.Run("safe_wal_size", func(t *testing.T) {
		ev := run(t, "      wal_headroom_bytes: 50GiB\n", ptr(gib))
		if ev["eta_basis"] != basisSafeWAL {
			t.Fatalf("eta_basis = %v, want %s", ev["eta_basis"], basisSafeWAL)
		}
	})
	t.Run("configured headroom", func(t *testing.T) {
		ev := run(t, "      wal_headroom_bytes: 50GiB\n", nil)
		if ev["eta_basis"] != basisHeadroom || ev["eta_note"] == nil {
			t.Fatalf("want headroom basis with a note, got %v", ev)
		}
	})
	t.Run("neither", func(t *testing.T) {
		ev := run(t, "      enabled: true\n", nil)
		if _, has := ev["eta_hours"]; has || ev["eta_note"] == nil {
			t.Fatalf("want no ETA and a reason, got %v", ev)
		}
		if _, ok := ev["rate_6h_bytes_per_hour"]; !ok {
			t.Fatalf("the rate is still reported: %v", ev)
		}
	})
}

// ---- rooms ----

func TestOpensOneRoomAndJoinsLaterSignals(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute) // connector_down fires at 3 min
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}
	r := h.rooms.created()[0]
	if r.alert.Source != alertin.Watcher || r.alert.Status != alertin.StatusFiring ||
		r.alert.DedupKey != "watcher:orders" || r.alert.PipelineID != "orders" {
		t.Fatalf("bad alert: %+v", r.alert)
	}
	if !strings.HasPrefix(r.alert.Title, "orders: connector pg-orders FAILED") {
		t.Fatalf("title = %q", r.alert.Title)
	}
	// The slot goes inactive too: it fires 5 min after it started, in the same room.
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("a second signal opened a room: %d", got)
	}
	joins := h.rooms.postsContaining("slot debezium_orders inactive")
	if len(joins) != 1 || joins[0].roomID != r.roomID {
		t.Fatalf("joins = %+v, want one post into %s", joins, r.roomID)
	}
}

func TestJoinsAnOpenRoomFromAnotherSource(t *testing.T) {
	h := newHarness(t, opts{})
	h.rooms.foreign["orders"] = "r_alertmanager"
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if got := len(h.rooms.created()); got != 0 {
		t.Fatalf("opened %d rooms next to an open Alertmanager room", got)
	}
	posts := h.rooms.postsContaining("evidence:")
	if len(posts) != 1 || posts[0].roomID != "r_alertmanager" {
		t.Fatalf("posts = %+v, want one into r_alertmanager", posts)
	}
	// It clears: a joined room gets a plain message, not a resolved alert.
	h.fixConnector("orders")
	h.steps(4, time.Minute)
	if got := len(h.rooms.resolved()); got != 0 {
		t.Fatalf("resolved alerts = %d for a joined room", got)
	}
	if got := len(h.rooms.postsContaining("all watcher signals are clear")); got != 1 {
		t.Fatalf("clear posts = %d, want 1", got)
	}
}

func TestStillFiringJoinsAtMostEvery15Minutes(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute) // opens at 3 min (tick 4, since the first tick is at +1 min)
	opened := h.clk.Now()
	var times []time.Time
	for range 50 {
		before := len(h.rooms.postsContaining("still firing"))
		h.step(time.Minute)
		if len(h.rooms.postsContaining("still firing")) > before {
			times = append(times, h.clk.Now())
		}
	}
	if len(times) != 3 {
		t.Fatalf("still-firing posts = %d over 50 min, want 3 (at 15, 30 and 45 min)", len(times))
	}
	prev := opened
	for _, ts := range times {
		if ts.Sub(prev) < 15*time.Minute {
			t.Fatalf("posts %v apart, want >= 15 min", ts.Sub(prev))
		}
		prev = ts
	}
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}
}

func TestResolvedAlertWhenAllSignalsClear(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(7, time.Minute) // both signals fire
	if h.phase("orders", owners.SignalConnectorDown) != phaseFiring || h.phase("orders", owners.SignalSlotInactive) != phaseFiring {
		t.Fatal("setup: both signals should fire")
	}
	h.fixConnector("orders")
	h.steps(5, time.Minute) // connector clear after 3 min; the slot is still inactive
	if got := len(h.rooms.resolved()); got != 0 {
		t.Fatal("resolved while a signal still fires")
	}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = true })
	h.steps(6, time.Minute)
	res := h.rooms.resolved()
	if len(res) != 1 {
		t.Fatalf("resolved alerts = %d, want 1\n%s", len(res), h.logs)
	}
	if a := res[0].alert; a.DedupKey != "watcher:orders" || a.Source != alertin.Watcher || res[0].created {
		t.Fatalf("bad resolved alert: %+v", a)
	}
	// And it is sent once.
	h.steps(10, time.Minute)
	if got := len(h.rooms.resolved()); got != 1 {
		t.Fatalf("resolved alerts after more ticks = %d, want 1", got)
	}
}

func TestAckedAfterRoomClosedAndResetOnClear(t *testing.T) {
	// Cooldown is tiny so that only the acked state can hold the watcher back.
	h := newHarness(t, opts{cooldown: time.Millisecond})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1", len(rooms))
	}
	id := rooms[0].roomID
	h.rooms.closeRoom(id, h.clk.Now())
	h.w.OnRoomClosed(id, "orders", "watcher:orders")
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatalf("phase = %v, want acked", h.phase("orders", owners.SignalConnectorDown))
	}
	h.steps(40, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("a closed room was re-opened by the same incident: %d rooms", got)
	}
	if got := len(h.rooms.postsContaining("still firing")); got != 0 {
		t.Fatalf("posts to a closed room: %d", got)
	}
	// It clears for `for`: back to ok, and no resolved alert for a closed room.
	h.fixConnector("orders")
	h.steps(4, time.Minute)
	if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
		t.Fatal("acked did not reset on clear")
	}
	if got := len(h.rooms.resolved()); got != 0 {
		t.Fatalf("resolved alerts = %d for a closed room", got)
	}
	// A new incident opens a new room.
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	if got := len(h.rooms.created()); got != 2 {
		t.Fatalf("rooms after a new incident = %d, want 2", got)
	}
}

func TestOnRoomClosedIgnoresOtherRooms(t *testing.T) {
	h := newHarness(t, opts{})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	h.w.OnRoomClosed("r_other", "orders", "alertmanager:x")
	h.w.OnRoomClosed("r_other", "payments", "watcher:payments")
	if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
		t.Fatal("a room that is not ours acked the signal")
	}
	// The service form of the key works too.
	id := h.rooms.created()[0].roomID
	h.w.OnRoomClosed(id, "orders", "orders|watcher:orders")
	if h.phase("orders", owners.SignalConnectorDown) != phaseAcked {
		t.Fatal("did not ack")
	}
}

func TestCooldownSkipsOpening(t *testing.T) {
	// A close from before the start whose incident the room service gives
	// back (ClosedRoomKeysSink, QA S1): the same signal waits out the cooldown.
	h := newHarness(t, opts{q3Sink: true})
	closed := h.clk.Now().Add(-10 * time.Minute)
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_old", closedAt: &closed}
	h.q3.setKeys("r_old", "connector_down:pg-orders")
	h.breakConnector("orders", "boom")
	h.steps(10, time.Minute)
	if got := len(h.rooms.created()); got != 0 {
		t.Fatalf("opened a room during cooldown: %d", got)
	}
	if n := h.logs.count("staying quiet"); n != 1 {
		t.Fatalf("cooldown log lines = %d, want 1\n%s", n, h.logs)
	}
	// 60 min after the close, the same incident may open a room.
	h.step(52 * time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms after cooldown = %d, want 1", got)
	}
}

func TestStormOpensOneRoom(t *testing.T) {
	pipes := []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}, {id: "d"}}
	h := newHarness(t, opts{pipes: pipes})
	h.step(0)
	for _, p := range []string{"a", "b", "c"} {
		h.breakConnector(p, "worker died")
	}
	h.steps(5, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d, want 1\n%s", len(rooms), h.logs)
	}
	if rooms[0].alert.PipelineID != "a" {
		t.Fatalf("storm room on %s, want the first pipeline a", rooms[0].alert.PipelineID)
	}
	ev := evidenceOf(t, rooms[0].alert.Raw)
	aff, _ := ev["affected_pipelines"].([]any)
	if len(aff) != 3 || aff[0] != "a" || aff[1] != "b" || aff[2] != "c" {
		t.Fatalf("affected_pipelines = %v, want [a b c]", aff)
	}
	if !strings.Contains(rooms[0].alert.Title, "2 other pipelines") {
		t.Fatalf("title = %q", rooms[0].alert.Title)
	}
	if h.logs.count("storm") == 0 {
		t.Fatal("the storm was not logged")
	}
	// Later the signal clears everywhere without new rooms.
	h.steps(10, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms later = %d, want 1", got)
	}
}

func TestTwoPipelinesIsNotAStorm(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	h.step(0)
	h.breakConnector("a", "x")
	h.breakConnector("b", "x")
	h.steps(5, time.Minute)
	if got := len(h.rooms.created()); got != 2 {
		t.Fatalf("rooms = %d, want 2", got)
	}
}

func TestMaxOpenWatcherRooms(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}, maxRooms: 2})
	h.step(0)
	// Staggered, so they are not a storm.
	h.breakConnector("a", "x")
	h.step(time.Minute)
	h.breakConnector("b", "x")
	h.step(time.Minute)
	h.breakConnector("c", "x")
	h.steps(8, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 {
		t.Fatalf("rooms = %d, want the cap of 2\n%s", len(rooms), h.logs)
	}
	if n := h.logs.count("max_open_watcher_rooms"); n != 1 {
		t.Fatalf("cap warnings = %d, want 1", n)
	}
	// Closing one makes room for the third.
	h.rooms.closeRoom(rooms[0].roomID, h.clk.Now())
	h.w.OnRoomClosed(rooms[0].roomID, rooms[0].alert.PipelineID, rooms[0].alert.DedupKey)
	h.step(time.Minute)
	if got := len(h.rooms.created()); got != 3 {
		t.Fatalf("rooms after one closed = %d, want 3", got)
	}
}

// ---- modes ----

func TestModeLogOpensNothingButLogsTransitions(t *testing.T) {
	h := newHarness(t, opts{mode: config.WatchLog})
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	h.fixConnector("orders")
	h.steps(5, time.Minute)
	if got := h.rooms.callCount(); got != 0 {
		t.Fatalf("log mode called the room service %d times", got)
	}
	if h.logs.count("watch: signal fired") != 1 || h.logs.count("watch: signal resolved") != 1 {
		t.Fatalf("want one fired and one resolved line\n%s", h.logs)
	}
	for _, f := range []string{"mode=log", "pipeline=orders", "signal=connector_down", "threshold=", "since=", "value="} {
		if !strings.Contains(h.logs.String(), f) {
			t.Fatalf("log lacks %q\n%s", f, h.logs)
		}
	}
}

func TestActionLogSignalNeverOpensARoom(t *testing.T) {
	pipes := []pipeSpec{{id: "orders", watch: "    watch:\n      signals:\n        connector_down: {action: log}\n"}}
	h := newHarness(t, opts{pipes: pipes})
	h.breakConnector("orders", "boom")
	h.steps(6, time.Minute)
	if h.rooms.callCount() != 0 {
		t.Fatal("an action: log signal reached the room service")
	}
	if h.logs.count("watch: signal fired") != 1 {
		t.Fatalf("not logged\n%s", h.logs)
	}
}

func TestModeOffReturnsAtOnce(t *testing.T) {
	h := newHarness(t, opts{mode: config.WatchOff})
	done := make(chan error, 1)
	go func() { done <- h.w.Run(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run in mode off did not return")
	}
	if h.src.callCount() != 0 {
		t.Fatal("mode off read the source")
	}
}

func TestNewRequiresRoomsInRoomsMode(t *testing.T) {
	o := loadOwners(t, pipeSpec{id: "orders"})
	if _, err := New(Deps{Owners: o, Config: config.Watch{Mode: config.WatchRooms}}); err == nil {
		t.Fatal("want an error without Rooms in mode rooms")
	}
	if _, err := New(Deps{Config: config.Watch{Mode: config.WatchLog}}); err == nil {
		t.Fatal("want an error without Owners")
	}
	if _, err := New(Deps{Owners: o, Config: config.Watch{Mode: config.WatchLog}}); err != nil {
		t.Fatalf("log mode needs no Rooms: %v", err)
	}
}

// ---- isolation and scope ----

func TestSourceErrorDoesNotHideConnectorDown(t *testing.T) {
	h := newHarness(t, opts{})
	h.src.mu.Lock()
	h.src.err = errors.New("pq: the database system is in recovery mode: recovery is in progress")
	h.src.mu.Unlock()
	h.breakConnector("orders", "boom")
	h.steps(5, time.Minute)
	if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
		t.Fatal("connector_down did not fire while the source was failing")
	}
	if h.phase("orders", owners.SignalSlotWAL) != phaseOK || h.phase("orders", owners.SignalSlotInactive) != phaseOK {
		t.Fatal("slot signals changed on a failing source")
	}
	if !strings.Contains(h.logs.String(), "must be the primary") {
		t.Fatalf("no hint for the recovery error\n%s", h.logs)
	}
	if n := h.logs.count("watch: check failed"); n != 2 { // once per slot signal, not every tick
		t.Fatalf("check failed warnings = %d, want 2", n)
	}
}

func TestSourceIsReadOncePerTick(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}, {id: "c"}}})
	h.steps(3, time.Minute)
	if got := h.src.callCount(); got != 3 {
		t.Fatalf("ReplicationSlots calls = %d over 3 ticks, want 3", got)
	}
}

func TestSlotNameFromConfigAndExplicit(t *testing.T) {
	t.Run("debezium default slot name", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.conn.mu.Lock()
		h.conn.cfg["pg-orders"] = map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}
		h.conn.mu.Unlock()
		h.src.update("debezium", func(r *clients.ReplicationSlot) { r.Active, r.RetainedWALBytes = true, 2*gib })
		h.steps(12, time.Minute)
		if h.phase("orders", owners.SignalSlotWAL) != phaseFiring {
			t.Fatalf("slot \"debezium\" not found\n%s", h.logs)
		}
	})
	t.Run("watch.slot wins over the config", func(t *testing.T) {
		pipes := []pipeSpec{{id: "orders", watch: "    watch:\n      slot: my_slot\n"}}
		h := newHarness(t, opts{pipes: pipes})
		h.src.update("my_slot", func(r *clients.ReplicationSlot) { r.Active, r.RetainedWALBytes = false, 1 })
		h.steps(7, time.Minute)
		if h.phase("orders", owners.SignalSlotInactive) != phaseFiring {
			t.Fatalf("explicit slot not used\n%s", h.logs)
		}
	})
	t.Run("missing slot is unknown, not a breach", func(t *testing.T) {
		h := newHarness(t, opts{})
		h.src.mu.Lock()
		h.src.rows = nil
		h.src.mu.Unlock()
		h.steps(12, time.Minute)
		if h.phase("orders", owners.SignalSlotInactive) != phaseOK || len(h.rooms.created()) != 0 {
			t.Fatal("a missing slot fired a signal")
		}
		if !strings.Contains(h.logs.String(), "not found in pg_replication_slots") {
			t.Fatalf("no warning\n%s", h.logs)
		}
	})
}

func TestDisabledPipelineAndSignal(t *testing.T) {
	pipes := []pipeSpec{
		{id: "off", watch: "    watch:\n      enabled: false\n"},
		{id: "on", watch: "    watch:\n      signals:\n        connector_down: {enabled: false}\n"},
	}
	h := newHarness(t, opts{pipes: pipes})
	h.breakConnector("off", "x")
	h.breakConnector("on", "x")
	h.steps(6, time.Minute)
	if h.rooms.callCount() != 0 {
		t.Fatalf("opened rooms for disabled watches: %+v", h.rooms.opens)
	}
	if h.w.pipes["off"] != nil {
		t.Fatal("a disabled pipeline was evaluated")
	}
}

func TestEvidenceHasNoUsernamesOrQueriesAndTraceIsOneLine(t *testing.T) {
	h := newHarness(t, opts{})
	long := strings.Repeat("x", 500)
	trace := "org.apache.kafka.connect.errors.ConnectException: " + long + "\n\tat io.debezium.Foo.bar(Foo.java:12)\n" +
		"Caused by: ERROR: password for user alice is wrong\n"
	h.breakConnector("orders", trace)
	h.steps(5, time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 1 {
		t.Fatalf("rooms = %d", len(rooms))
	}
	raw := string(rooms[0].alert.Raw)
	for _, banned := range []string{"username", "query", "alice", "Foo.java", "Caused by"} {
		if strings.Contains(raw, banned) {
			t.Fatalf("evidence contains %q:\n%s", banned, raw)
		}
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), owners.SignalConnectorDown)
	conns := ev["connectors"].([]any)
	tasks := conns[0].(map[string]any)["tasks"].([]any)
	line := tasks[0].(map[string]any)["trace"].(string)
	if len([]rune(line)) != maxTraceChars || strings.Contains(line, "\n") {
		t.Fatalf("trace line has %d runes, want exactly %d on one line", len([]rune(line)), maxTraceChars)
	}
	if len(raw) > 4096 {
		t.Fatalf("evidence is %d bytes, want small", len(raw))
	}
	if !json.Valid(rooms[0].alert.Raw) {
		t.Fatal("evidence is not valid JSON")
	}
	if n := len([]rune(rooms[0].alert.Title)); n > alertin.MaxTitleRunes {
		t.Fatalf("title has %d runes, max %d", n, alertin.MaxTitleRunes)
	}
}

// ---- helpers ----

func TestFirstLine(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"\n\n  \nreal line\nsecond", "real line"},
		{"a\x00b\x1b[31m\nrest", "ab[31m"},
		{strings.Repeat("é", 400), strings.Repeat("é", 300)},
	}
	for _, c := range cases {
		if got := firstLine(c.in, 300); got != c.want {
			t.Errorf("firstLine(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStateMachineDirect(t *testing.T) {
	var s sigState
	now := t0
	if tr := s.step(now, reading{v: vBreach}, time.Minute); tr != tNone {
		t.Fatal("fired at once without BreachNow")
	}
	if tr := s.step(now.Add(30*time.Second), reading{v: vUnknown}, time.Minute); tr != tNone || s.breachingSince != now {
		t.Fatal("an unknown reading touched the timer")
	}
	if tr := s.step(now.Add(time.Minute), reading{v: vBreach}, time.Minute); tr != tFired {
		t.Fatal("did not fire at for")
	}
	if tr := s.step(now.Add(2*time.Minute), reading{v: vBreachNow}, time.Minute); tr != tNone {
		t.Fatal("fired twice")
	}
	if tr := s.step(now.Add(3*time.Minute), reading{v: vClear}, time.Minute); tr != tNone || s.clearingSince.IsZero() {
		t.Fatal("clear timer not started")
	}
	if tr := s.step(now.Add(4*time.Minute), reading{v: vClear}, time.Minute); tr != tResolved || s.phase != phaseOK {
		t.Fatal("did not resolve")
	}
}
