package watch

import (
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// Q1 (QA G1, G18): incident-keyed cooldown and ack, escalation, join rules,
// slot_stall.

// closeNow closes room the way the service does and tells the watcher.
func (h *harness) closeNow(room string) {
	h.t.Helper()
	h.rooms.closeRoom(room, h.clk.Now())
	h.w.OnRoomClosed(room, "orders", "orders|watcher:orders")
}

// (a) A different signal right after a room closed opens a room at once; the
// same signal again stays quiet (the closed incident's cooldown).
func TestCooldownIsKeyedToTheIncident(t *testing.T) {
	cases := []struct {
		name       string
		keepFiring bool   // connector_down still fires at the close (acked)
		second     string // "slot": another signal fires; "connector": the same one again
		wantRooms  int
	}{
		{"other signal 19s after a resolved room closed", false, "slot", 2},
		{"other signal 19s after a room closed while the first fired", true, "slot", 2},
		{"same signal 19s after a resolved room closed", false, "connector", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{q1Sink: true})
			h.breakConnector("orders", "boom")
			h.steps(4, time.Minute) // t0+4: connector_down opens r1
			if len(h.rooms.created()) != 1 {
				t.Fatalf("setup: rooms = %d", len(h.rooms.created()))
			}
			r1 := h.rooms.created()[0].roomID
			if !tc.keepFiring {
				h.fixConnector("orders")
			}
			h.steps(4, time.Minute) // t0+8 (resolved when fixed; r1 stays open)
			switch tc.second {
			case "slot":
				h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
				h.steps(5, time.Minute) // breaching from t0+9, fires at t0+14
			case "connector":
				h.steps(2, time.Minute)
				h.breakConnector("orders", "auth failed")
				h.steps(3, time.Minute) // breaching from t0+11, fires at t0+14
			}
			h.clk.Advance(41 * time.Second)
			h.closeNow(r1)
			h.step(19 * time.Second) // t0+14: the second signal fires 19 s after the close

			sig := owners.SignalSlotInactive
			if tc.second == "connector" {
				sig = owners.SignalConnectorDown
			}
			if h.phase("orders", sig) != phaseFiring {
				t.Fatalf("setup: %s phase = %v, want firing", sig, h.phase("orders", sig))
			}
			rooms := h.rooms.created()
			if len(rooms) != tc.wantRooms {
				t.Fatalf("rooms = %d, want %d\n%s", len(rooms), tc.wantRooms, h.logs)
			}
			quiet := h.logs.count("cooldown after a closed watcher room") > 0
			if quiet != (tc.wantRooms == 1) {
				t.Fatalf("cooldown logged = %v, want %v\n%s", quiet, tc.wantRooms == 1, h.logs)
			}
			if tc.wantRooms == 2 {
				r2 := rooms[1]
				if !strings.Contains(r2.alert.Title, "slot debezium_orders inactive") {
					t.Fatalf("second room title = %q", r2.alert.Title)
				}
				ev := evidenceOf(t, r2.alert.Raw)
				if sigs, _ := ev["signals"].([]any); len(sigs) != 1 {
					t.Fatalf("second room carries %d signals, want only the new one: %v", len(sigs), ev)
				}
				if got := signalEv(t, ev, owners.SignalSlotInactive)["layer"]; got != "postgres" {
					t.Fatalf("slot_inactive layer = %v", got)
				}
			}
			for _, p := range h.rooms.postsTo(r1) {
				if strings.Contains(p.text, "inactive") || strings.Contains(p.text, "auth") {
					t.Fatalf("the second signal went into the closed room: %q", p.text)
				}
			}
		})
	}
}

// (b) Join rules with a RoomInfoSink.
func TestJoinRules(t *testing.T) {
	open := t0.Add(4 * time.Minute) // when connector_down fires
	cases := []struct {
		name string
		info RoomInfo
		join bool
	}{
		{"stale manual room", RoomInfo{RoomID: "r_x", OpenedAt: open.Add(-3 * time.Hour), LastEventAt: open.Add(-3 * time.Hour)}, false},
		{"busy room a person opened 3h ago", RoomInfo{RoomID: "r_x", OpenedAt: open.Add(-3 * time.Hour), LastEventAt: open.Add(-time.Minute)}, false},
		{"person's room opened 30m ago", RoomInfo{RoomID: "r_x", OpenedAt: open.Add(-30 * time.Minute), LastEventAt: open.Add(-time.Minute)}, true},
		{"recent alert room", RoomInfo{RoomID: "r_x", AlertSource: "alertmanager", OpenedAt: open.Add(-5 * time.Hour), LastEventAt: open.Add(-5 * time.Minute)}, true},
		{"recent watcher room", RoomInfo{RoomID: "r_x", AlertSource: "watcher", OpenedAt: open.Add(-time.Hour), LastEventAt: open.Add(-time.Minute)}, true},
		{"alert room gone quiet", RoomInfo{RoomID: "r_x", AlertSource: "alertmanager", OpenedAt: open.Add(-5 * time.Hour), LastEventAt: open.Add(-3 * time.Hour)}, false},
		{"alert room with no times", RoomInfo{RoomID: "r_x", AlertSource: "alertmanager"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{q1Sink: true})
			h.q1.setInfo("orders", tc.info)
			h.breakConnector("orders", "boom")
			h.steps(4, time.Minute)
			created, posts := len(h.rooms.created()), h.rooms.postsTo("r_x")
			if tc.join {
				if created != 0 || len(posts) != 1 || !strings.Contains(posts[0].text, "joined an open room") {
					t.Fatalf("want one join into r_x: created %d, posts %+v\n%s", created, posts, h.logs)
				}
				if posts[0].via != "incident" {
					t.Fatalf("join via %q, want incident (new signals wake the agent)", posts[0].via)
				}
				return
			}
			if created != 1 || len(posts) != 0 {
				t.Fatalf("want the watcher's own room: created %d, posts into r_x %d\n%s", created, len(posts), h.logs)
			}
			if h.logs.count("not joining an old or manual room") != 1 {
				t.Fatalf("passing over the room not logged once\n%s", h.logs)
			}
		})
	}
}

func TestJoinableBoundaries(t *testing.T) {
	now := t0
	max := 2 * time.Hour
	cases := []struct {
		name string
		info RoomInfo
		want bool
	}{
		{"last event just inside", RoomInfo{AlertSource: "watcher", LastEventAt: now.Add(-max + time.Second)}, true},
		{"last event exactly at the max", RoomInfo{AlertSource: "watcher", LastEventAt: now.Add(-max)}, false},
		{"person, opened just inside", RoomInfo{OpenedAt: now.Add(-max + time.Second), LastEventAt: now}, true},
		{"person, opened at the max", RoomInfo{OpenedAt: now.Add(-max), LastEventAt: now}, false},
		{"person, no opened time", RoomInfo{LastEventAt: now}, false},
	}
	for _, tc := range cases {
		if got := joinable(now, tc.info, max); got != tc.want {
			t.Errorf("%s: joinable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// (b) A joined alert room hears about new signals through PostIncidentJSON and
// about repeats through PostEvidenceJSON.
func TestJoinPostsNewSignalsAsIncidentsAndRepeatsAsEvidence(t *testing.T) {
	h := newHarness(t, opts{q1Sink: true})
	h.q1.setInfo("orders", RoomInfo{RoomID: "r_am", AlertSource: "alertmanager", OpenedAt: t0, LastEventAt: t0})
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)  // t0+4: join r_am
	h.steps(15, time.Minute) // t0+19: still firing
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(6, time.Minute)  // t0+25: slot_inactive is a new signal
	h.steps(15, time.Minute) // t0+40: still firing
	posts := h.rooms.postsTo("r_am")
	type want struct{ text, via string }
	wants := []want{
		{"joined an open room", "incident"},
		{"(still firing)", "evidence"},
		{"(new signal)", "incident"},
		{"(still firing)", "evidence"},
	}
	if len(posts) != len(wants) {
		t.Fatalf("posts = %d, want %d: %+v", len(posts), len(wants), posts)
	}
	for i, w := range wants {
		if !strings.Contains(posts[i].text, w.text) || posts[i].via != w.via {
			t.Fatalf("post %d = %q via %q, want %q via %q", i, posts[i].text, posts[i].via, w.text, w.via)
		}
	}
	if len(h.rooms.created()) != 0 {
		t.Fatal("opened a room next to a recent alert room")
	}
}

// After a restart the watcher cannot know what its own open room carried: the
// re-attach is a repeat. In-process, a new signal in that room is an incident.
func TestReattachToTheOwnOpenRoom(t *testing.T) {
	t.Run("after a restart", func(t *testing.T) {
		h := newHarness(t, opts{q1Sink: true})
		h.rooms.byKey["orders|watcher:orders"] = "r_before"
		h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_before"}
		h.breakConnector("orders", "boom")
		h.steps(4, time.Minute)
		posts := h.rooms.postsContaining("re-attached after a restart")
		if len(posts) != 1 || posts[0].via != "evidence" {
			t.Fatalf("re-attach posts = %+v, want one repeat via evidence", posts)
		}
	})
	t.Run("resolved room still open, another signal fires", func(t *testing.T) {
		h := newHarness(t, opts{q1Sink: true})
		h.breakConnector("orders", "boom")
		h.steps(4, time.Minute) // r1
		h.fixConnector("orders")
		h.steps(4, time.Minute) // resolved, r1 stays open
		h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
		h.steps(6, time.Minute)
		posts := h.rooms.postsContaining("fired again in the open room")
		if len(posts) != 1 || posts[0].via != "incident" || !strings.Contains(posts[0].text, "inactive") {
			t.Fatalf("posts = %+v, want one incident post of slot_inactive", posts)
		}
		if len(h.rooms.created()) != 1 {
			t.Fatalf("rooms = %d, want 1", len(h.rooms.created()))
		}
	})
}

// (c), (d) Closed while it fires and it keeps firing: one re-open after
// EscalateAfter, never with 0. Closed again, it stays quiet until it clears.
func TestCloseWhileFiringEscalatesOnce(t *testing.T) {
	cases := []struct {
		name     string
		escalate time.Duration
		reopenAt time.Duration // after the close; 0 = never
	}{
		{"escalate off", 0, 0},
		{"escalate after 10m", 10 * time.Minute, 10 * time.Minute},
		{"escalate after 30m", 30 * time.Minute, 30 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, opts{escalateAfter: tc.escalate, q1Sink: true}) // cooldown 60m
			h.breakConnector("orders", "boom")
			h.steps(4, time.Minute)
			r1 := h.rooms.created()[0].roomID
			closedAt := h.clk.Now()
			h.closeNow(r1)
			var reopened time.Duration
			for range 120 {
				h.step(time.Minute)
				rooms := h.rooms.created()
				if len(rooms) == 2 && reopened == 0 {
					reopened = h.clk.Now().Sub(closedAt)
					r2 := rooms[1]
					if !strings.Contains(r2.alert.Title, "kept firing "+fmtDur(tc.escalate)+" after its room was closed") {
						t.Fatalf("re-open title = %q", r2.alert.Title)
					}
					if ev := evidenceOf(t, r2.alert.Raw); ev["escalated"] != true {
						t.Fatalf("re-open evidence not marked escalated: %v", ev)
					}
					h.closeNow(r2.roomID) // closed again while it fires: must not come back
				}
			}
			if reopened != tc.reopenAt {
				t.Fatalf("re-opened %v after the close, want %v\n%s", reopened, tc.reopenAt, h.logs)
			}
			want := 1
			if tc.reopenAt > 0 {
				want = 2
			}
			if got := len(h.rooms.created()); got != want {
				t.Fatalf("rooms = %d, want %d", got, want)
			}
			if got := h.logs.count(`level=WARN msg="watch: signal firing with no room"`); got != want-1 {
				t.Fatalf("no-room warnings = %d, want %d\n%s", got, want-1, h.logs)
			}
			// It clears and fires again later: a new episode may open again.
			h.fixConnector("orders")
			h.steps(4, time.Minute)
			if h.phase("orders", owners.SignalConnectorDown) != phaseOK {
				t.Fatal("did not clear")
			}
		})
	}
}

// (c) Flapping (fire, clear, fire again within the cooldown) stays quiet when
// each burst is shorter than EscalateAfter; one that lasts longer escalates.
func TestFlappingWithinTheCooldownStaysQuiet(t *testing.T) {
	h := newHarness(t, opts{escalateAfter: 10 * time.Minute, q1Sink: true}) // cooldown 60m
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute) // r1
	h.fixConnector("orders")
	h.steps(4, time.Minute) // resolved
	h.closeNow(h.rooms.created()[0].roomID)
	for range 3 { // three 9-minute flaps, each firing about 2 minutes
		h.breakConnector("orders", "boom")
		h.steps(5, time.Minute)
		if h.phase("orders", owners.SignalConnectorDown) != phaseFiring {
			t.Fatal("setup: the flap did not fire")
		}
		h.fixConnector("orders")
		h.steps(4, time.Minute)
	}
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("flapping opened rooms: %d\n%s", got, h.logs)
	}
	if h.logs.count("signal firing with no room") != 0 {
		t.Fatalf("a short flap escalated\n%s", h.logs)
	}
	// A burst that keeps firing for EscalateAfter re-opens, cooldown or not.
	h.breakConnector("orders", "boom")
	h.steps(13, time.Minute) // breaching from the 1st tick, fires at the 4th; 9 minutes later
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("escalated before EscalateAfter: %d rooms", got)
	}
	h.step(time.Minute) // fired 10 minutes ago
	rooms := h.rooms.created()
	if len(rooms) != 2 || !strings.Contains(rooms[1].alert.Title, "firing 10m with no room") {
		t.Fatalf("rooms = %+v, want a second one with the no-room note\n%s", rooms, h.logs)
	}
}

// A close the watcher never saw (before a restart) has no known incident: the
// whole pipeline waits, until a signal has fired EscalateAfter.
func TestUnknownIncidentCooldownEndsWithAnEscalation(t *testing.T) {
	h := newHarness(t, opts{escalateAfter: 10 * time.Minute})
	closed := h.clk.Now().Add(-10 * time.Minute)
	h.rooms.last["orders|watcher:orders"] = lastRoom{roomID: "r_old", closedAt: &closed}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.Active = false })
	h.steps(15, time.Minute) // slot_inactive fires at t0+6
	if got := len(h.rooms.created()); got != 0 {
		t.Fatalf("rooms = %d during an unknown incident's cooldown", got)
	}
	if !strings.Contains(h.logs.String(), `incident="unknown: closed before the watcher saw it"`) {
		t.Fatalf("cooldown line does not say the incident is unknown\n%s", h.logs)
	}
	h.step(time.Minute) // t0+16: fired 10 minutes ago
	rooms := h.rooms.created()
	if len(rooms) != 1 || !strings.Contains(rooms[0].alert.Title, "with no room") {
		t.Fatalf("rooms = %+v, want one escalated room\n%s", rooms, h.logs)
	}
	if !strings.Contains(h.logs.String(), `reason="cooldown after a closed room"`) {
		t.Fatalf("the no-room warning does not name the cooldown\n%s", h.logs)
	}
}

// Item 5: a signal held back by the room cap logs a warning and one error
// after EscalateAfter, and opens once the cap frees up.
func TestEscalationAtTheCapLogsAnErrorOnce(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "a"}, {id: "b"}}, maxRooms: 1, escalateAfter: 10 * time.Minute})
	h.step(0)
	h.breakConnector("a", "x")
	h.steps(4, time.Minute) // a's room
	h.breakConnector("b", "x")
	h.steps(4, time.Minute) // b fires: cap
	h.steps(30, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d over the cap", got)
	}
	if n := h.logs.count(`level=WARN msg="watch: signal firing with no room"`); n != 1 {
		t.Fatalf("no-room warnings = %d, want 1\n%s", n, h.logs)
	}
	if n := h.logs.count(`level=ERROR msg="watch: signal firing with no room, max_open_watcher_rooms reached"`); n != 1 {
		t.Fatalf("cap errors = %d, want 1\n%s", n, h.logs)
	}
	a := h.rooms.created()[0].roomID
	h.rooms.closeRoom(a, h.clk.Now())
	h.w.OnRoomClosed(a, "a", "a|watcher:a")
	h.step(time.Minute)
	rooms := h.rooms.created()
	if len(rooms) != 2 || rooms[1].alert.PipelineID != "b" || !strings.Contains(rooms[1].alert.Title, "with no room") {
		t.Fatalf("rooms after the cap freed = %+v", rooms)
	}
}

// ---- slot_stall ----

func TestSlotStallReading(t *testing.T) {
	const slot = "debezium_orders"
	type obs struct {
		at                 time.Duration
		restart, confirmed string
		retained           int64
		inactive           bool
	}
	cases := []struct {
		name    string
		hb      heartbeat
		rows    []obs
		want    verdict
		wantSev string
	}{
		{"first sample", heartbeat{}, []obs{{0, "0/10", "0/10", 100 * mib, false}}, vClear, ""},
		{"still for 29m", heartbeat{}, []obs{{0, "0/10", "0/10", 100 * mib, false}, {29 * time.Minute, "0/10", "0/10", 120 * mib, false}}, vClear, ""},
		{"still for 30m, large and growing", heartbeat{known: true}, []obs{{0, "0/10", "0/10", 100 * mib, false}, {30 * time.Minute, "0/10", "0/10", 110 * mib, false}}, vBreach, "sev3"},
		{"heartbeats configured", heartbeat{known: true, configured: true}, []obs{{0, "0/10", "0/10", 100 * mib, false}, {30 * time.Minute, "0/10", "0/10", 110 * mib, false}}, vBreach, "sev2"},
		{"under 64 MiB", heartbeat{}, []obs{{0, "0/10", "0/10", 10 * mib, false}, {40 * time.Minute, "0/10", "0/10", 60 * mib, false}}, vHold, "sev3"},
		{"not growing", heartbeat{}, []obs{{0, "0/10", "0/10", 100 * mib, false}, {40 * time.Minute, "0/10", "0/10", 100 * mib, false}}, vHold, "sev3"},
		{"inactive slot", heartbeat{}, []obs{{0, "0/10", "0/10", 100 * mib, true}, {40 * time.Minute, "0/10", "0/10", 120 * mib, true}}, vHold, "sev3"},
		{"restart_lsn moved", heartbeat{}, []obs{{0, "0/10", "0/10", 100 * mib, false}, {40 * time.Minute, "0/20", "0/10", 120 * mib, false}}, vClear, ""},
		{"confirmed_flush_lsn moved", heartbeat{}, []obs{{0, "0/10", "0/10", 100 * mib, false}, {40 * time.Minute, "0/10", "0/30", 120 * mib, false}}, vClear, ""},
		{"moved, then still for 30m", heartbeat{}, []obs{{0, "0/10", "0/10", 90 * mib, false}, {10 * time.Minute, "0/20", "0/20", 100 * mib, false}, {40 * time.Minute, "0/20", "0/20", 102 * mib, false}}, vBreach, "sev3"},
		{"no restart_lsn", heartbeat{}, []obs{{0, "", "", 0, false}, {40 * time.Minute, "", "", 200 * mib, false}}, vClear, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m stallMem
			var r reading
			for _, o := range tc.rows {
				row := clients.ReplicationSlot{Name: slot, Active: !o.inactive, RestartLSN: o.restart, ConfirmedFlushLSN: o.confirmed, RetainedWALBytes: o.retained}
				r = slotStallReading(t0.Add(o.at), owners.DefaultWatch().SlotStall, slot, &m, row, tc.hb)
			}
			if r.v != tc.want {
				t.Fatalf("verdict = %v, want %v (%+v)", r.v, tc.want, r)
			}
			if tc.wantSev != "" && r.sev != tc.wantSev {
				t.Fatalf("severity = %q, want %q", r.sev, tc.wantSev)
			}
			if r.v != vBreach {
				return
			}
			if r.ev["slot"] != slot || r.ev["signal"] != SignalSlotStall || r.ev["stalled_seconds"] == nil || r.ev["retained_bytes"] == nil {
				t.Fatalf("evidence = %v", r.ev)
			}
			for k, v := range r.ev {
				if s, ok := v.(string); ok && strings.Contains(s, "0/") {
					t.Fatalf("evidence %s carries an LSN: %q", k, s)
				}
			}
			if !slices.Equal(r.targets, []string{slot}) {
				t.Fatalf("targets = %v", r.targets)
			}
		})
	}
}

// (e) slot_stall fires after 30 minutes without movement and clears on the
// first tick after restart_lsn moves.
func TestSlotStallFiresAndClearsWhenTheSlotMoves(t *testing.T) {
	h := newHarness(t, opts{})
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) {
		r.RestartLSN, r.ConfirmedFlushLSN, r.RetainedWALBytes = "0/1000", "0/1000", 100*mib
	})
	grow := func(n int) {
		for range n {
			h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.RetainedWALBytes += mib })
			h.step(time.Minute)
		}
	}
	grow(30) // t0+30: still for 29 minutes (first seen at t0+1)
	if h.phase("orders", SignalSlotStall) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatalf("fired before 30 minutes\n%s", h.logs)
	}
	grow(1) // t0+31
	rooms := h.rooms.created()
	if h.phase("orders", SignalSlotStall) != phaseFiring || len(rooms) != 1 {
		t.Fatalf("phase %v, rooms %d: want firing and one room\n%s", h.phase("orders", SignalSlotStall), len(rooms), h.logs)
	}
	if !strings.Contains(rooms[0].alert.Title, "slot debezium_orders stalled 30m") || rooms[0].alert.Severity != "sev2" {
		t.Fatalf("room = %q %s, want the stall at sev2 (heartbeats configured)", rooms[0].alert.Title, rooms[0].alert.Severity)
	}
	ev := signalEv(t, evidenceOf(t, rooms[0].alert.Raw), SignalSlotStall)
	if ev["slot"] != "debezium_orders" || ev["layer"] != "postgres" || ev["heartbeat_configured"] != true {
		t.Fatalf("evidence = %v", ev)
	}
	if h.phase("orders", owners.SignalSlotWAL) != phaseOK {
		t.Fatal("slot_wal fired at 130 MB")
	}
	h.src.update("debezium_orders", func(r *clients.ReplicationSlot) { r.RestartLSN = "0/2000" })
	h.step(time.Minute)
	if h.phase("orders", SignalSlotStall) != phaseOK {
		t.Fatal("did not clear when restart_lsn moved")
	}
	if got := len(h.rooms.resolved()); got != 1 {
		t.Fatalf("resolved alerts = %d, want 1", got)
	}
}

func TestSignalLayerCoversEverySignal(t *testing.T) {
	for _, id := range signalIDs {
		if l := SignalLayer(id); !slices.Contains(events.Layers, l) {
			t.Errorf("SignalLayer(%s) = %q, not in events.Layers", id, l)
		}
	}
	if SignalLayer(SignalSlotStall) != "postgres" {
		t.Errorf("slot_stall layer = %q", SignalLayer(SignalSlotStall))
	}
	if SignalLayer("nope") != "" {
		t.Error("unknown signal has a layer")
	}
}

func TestIncidentKeys(t *testing.T) {
	var s sigState
	if got := s.keys("dlq_growth"); !slices.Equal(got, []string{"dlq_growth"}) {
		t.Fatalf("no targets: %v", got)
	}
	s.addTargets([]string{"pg-b", "pg-a", "pg-a", ""})
	if got := s.keys(owners.SignalConnectorDown); !slices.Equal(got, []string{"connector_down:pg-a", "connector_down:pg-b"}) {
		t.Fatalf("keys = %v", got)
	}
	for i := range 2 * maxSignalTargets {
		s.addTargets([]string{strings.Repeat("x", i+1)})
	}
	if len(s.targets) != maxSignalTargets {
		t.Fatalf("targets = %d, want the cap %d", len(s.targets), maxSignalTargets)
	}

	var ps pipeState
	for i := range maxToldRooms + 2 {
		ps.tell("r"+strings.Repeat("x", i), []string{"k"})
	}
	if len(ps.told) != maxToldRooms {
		t.Fatalf("told rooms = %d, want the cap %d", len(ps.told), maxToldRooms)
	}
	ps.tell("r_new", []string{"a", "b"})
	if fresh, known := ps.untold("r_new", []string{"a", "c"}); !known || !slices.Equal(fresh, []string{"c"}) {
		t.Fatalf("untold = %v %v", fresh, known)
	}
	if fresh, known := ps.untold("r_other", []string{"a"}); known || !slices.Equal(fresh, []string{"a"}) {
		t.Fatalf("untold of an unknown room = %v %v", fresh, known)
	}
	if !ps.coolRoom("r_new", t0) || ps.coolRoom("r_new", t0) {
		t.Fatal("coolRoom should report the room once")
	}
	if c, ok := ps.cooled["b"]; !ok || c.room != "r_new" || !c.at.Equal(t0) {
		t.Fatalf("cooled = %v", ps.cooled)
	}
}
