package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// Forecast rooms (X3c): a forecast signal opens a quiet room, the first steer
// or @agent comment starts its agent, and the metric's real signal escalates
// it.

const fcETA = "2026-10-14T03:00:00Z"

// forecastEvidence is watcher evidence (watch.compose) holding one forecast.
func forecastEvidence(pipeline string) json.RawMessage {
	return json.RawMessage(`{"source":"watcher","pipeline":"` + pipeline + `","severity":"sev4","at":"2026-10-13T09:00:00Z",` +
		`"reason":"opened","signals":[{"signal":"forecast","severity":"sev4","metric":"slot_wal_bytes","object":"orders_slot",` +
		`"current":64424509440,"limit":107374182400,"eta":"` + fcETA + `","slope_per_hour":1073741824,"r2":0.97,"samples":96}]}`)
}

// slotWALEvidence is the watcher's join once the slot really fills: the
// forecast is still firing beside slot_wal at sev.
func slotWALEvidence(pipeline, sev string) json.RawMessage {
	return json.RawMessage(`{"source":"watcher","pipeline":"` + pipeline + `","severity":"` + sev + `","reason":"new signal","signals":[` +
		`{"signal":"slot_wal","severity":"` + sev + `","slot":"orders_slot"},` +
		`{"signal":"forecast","severity":"sev4","metric":"slot_wal_bytes","object":"orders_slot","current":96636764160,` +
		`"limit":107374182400,"eta":"2026-10-13T20:00:00Z","slope_per_hour":1073741824,"r2":0.98,"samples":100}]}`)
}

// postWatcher posts evidence the way the watcher joins a room
// (watch.updateRoom): the evidence JSON in the note text and beside it.
func postWatcher(ctx context.Context, svc *room.Service, roomID string, ev json.RawMessage) error {
	return svc.PostRoomEvidence(ctx, roomID, "watcher: orders: signals (new signal)\nevidence: "+string(ev), ev)
}

func forecastAlert(pipeline string) alertin.Alert {
	return alertin.Alert{
		Source: alertin.Watcher, Status: alertin.StatusFiring, Severity: "sev4", PipelineID: pipeline,
		Title:    pipeline + ": slot_wal_bytes reaches its limit in 18h",
		DedupKey: "watcher:" + pipeline + ":forecast:slot_wal_bytes",
		Raw:      forecastEvidence(pipeline),
	}
}

// openForecast opens a forecast room on svc and checks it opened quietly.
func openForecast(t *testing.T, h *watchHarness, svc *room.Service) room.RoomSnapshot {
	t.Helper()
	snap, created, err := svc.OpenAlertRoom(context.Background(), forecastAlert("orders"))
	if err != nil || !created {
		t.Fatalf("open forecast room: created=%v err=%v", created, err)
	}
	return snap
}

// agentEvents are the events of the agent actor, and every agent.* event that
// is not the system's note.
func agentEvents(evs []events.Event) []events.Event {
	var out []events.Event
	for _, e := range evs {
		if e.Actor.Kind == events.ActorAgent || (strings.HasPrefix(string(e.Type), "agent.") && e.Actor.Kind != events.ActorSystem) {
			out = append(out, e)
		}
	}
	return out
}

func escalations(t *testing.T, evs []events.Event) []events.RoomEscalatedPayload {
	t.Helper()
	var out []events.RoomEscalatedPayload
	for _, e := range evs {
		if e.Type != events.RoomEscalated {
			continue
		}
		p, err := events.Decode[events.RoomEscalatedPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		if e.Actor.Kind != events.ActorSystem {
			t.Errorf("room.escalated actor = %+v, want system", e.Actor)
		}
		out = append(out, p)
	}
	return out
}

// TC1, TC5: the room opens with origin forecast, sev4, the trend note first,
// no agent event, no usage, no run; the snapshot shows origin and ETA.
func TestForecastRoomOpensQuietly(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	snap := openForecast(t, h, svc)

	if snap.Origin != events.RoomOriginForecast || snap.Severity != "sev4" {
		t.Fatalf("snapshot origin=%q severity=%q, want forecast sev4", snap.Origin, snap.Severity)
	}
	f := snap.Forecast
	if f == nil || f.Metric != "slot_wal_bytes" || f.Object != "orders_slot" || f.Limit != 107374182400 ||
		f.ETA.Format(time.RFC3339) != fcETA || f.R2 != 0.97 || f.At.IsZero() {
		t.Fatalf("snapshot forecast = %+v", f)
	}
	evs := roomEvents(t, svc, snap.ID)
	p, err := events.Decode[events.RoomCreatedPayload](evs[0])
	if err != nil || p.Origin != events.RoomOriginForecast {
		t.Fatalf("room.created origin = %q (%v)", p.Origin, err)
	}
	texts := systemTexts(t, svc, snap.ID)
	if len(texts) == 0 || !strings.HasPrefix(texts[0], "forecast: slot_wal_bytes of orders_slot is 60.0 GiB of its 100.0 GiB limit") ||
		!strings.Contains(texts[0], "2026-10-14 03:00 UTC") || !strings.Contains(texts[0], "@agent") {
		t.Fatalf("first system message = %q", texts)
	}
	time.Sleep(50 * time.Millisecond) // a run would start in its own goroutine
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("agent runs = %d, want 0", n)
	}
	if got := agentEvents(roomEvents(t, svc, snap.ID)); len(got) != 0 {
		t.Fatalf("agent events in a quiet room: %+v", got)
	}

	// The list rows carry origin and forecast too.
	rows, err := svc.ListRooms(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Origin != events.RoomOriginForecast || rows[0].Forecast == nil {
		t.Fatalf("list rows = %+v, %v", rows, err)
	}
	b, _ := json.Marshal(rows[0])
	if !strings.Contains(string(b), `"origin":"forecast"`) || !strings.Contains(string(b), `"eta":"`+fcETA+`"`) {
		t.Fatalf("row JSON lacks origin or eta: %s", b)
	}

	// Another watcher post that does not raise the room (the forecast still
	// firing) starts nothing either.
	if err := postWatcher(context.Background(), svc, snap.ID, forecastEvidence("orders")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("agent runs after a forecast post = %d, want 0", n)
	}
}

// TC2: the first steer starts the agent once; later steers and @agent
// comments do not start another.
func TestForecastRoomFirstSteerStartsAgentOnce(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	snap := openForecast(t, h, svc)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	if _, err := svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("taking control started the agent (%d runs)", n)
	}
	if _, err := svc.Steer(ctx, arjun, snap.ID, "is the slot really growing?"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
	if _, err := svc.Steer(ctx, arjun, snap.ID, "and the consumer?"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddComment(ctx, arjun, snap.ID, "", "@agent check the lag too"); err != nil {
		t.Fatal(err)
	}
	// The escalation finds the agent running: it raises the room only.
	if err := postWatcher(ctx, svc, snap.ID, slotWALEvidence("orders", "sev2")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("agent runs = %d, want exactly 1", n)
	}
}

// TC2: an @agent comment by anyone (not only the driver) starts it too.
func TestForecastRoomAgentMentionStartsAgent(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	snap := openForecast(t, h, svc)
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "arjun"), snap.ID, "", "no mention"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("a plain comment started the agent (%d runs)", n)
	}
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "arjun"), snap.ID, "", "@agent have a look"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
}

// TC3: the real signal on the same pipeline joins the forecast room, raises
// its severity (room.escalated) and starts the agent; a later raise starts
// nothing more, and a room is never lowered.
func TestForecastRoomEscalation(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	snap := openForecast(t, h, svc)
	ctx := context.Background()

	// Evidence of another pipeline (a storm follower) raises nothing.
	if err := postWatcher(ctx, svc, snap.ID, slotWALEvidence("payments", "sev1")); err != nil {
		t.Fatal(err)
	}
	if err := postWatcher(ctx, svc, snap.ID, slotWALEvidence("orders", "sev2")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
	got, err := svc.GetRoom(ctx, snap.ID)
	if err != nil || got.Severity != "sev2" || got.Origin != events.RoomOriginForecast {
		t.Fatalf("after escalation: severity=%q origin=%q err=%v", got.Severity, got.Origin, err)
	}
	esc := escalations(t, roomEvents(t, svc, snap.ID))
	if len(esc) != 1 || esc[0] != (events.RoomEscalatedPayload{Severity: "sev2", From: "sev4", Signal: "slot_wal"}) {
		t.Fatalf("escalations = %+v", esc)
	}
	// The new run got the join's numbers as its first input.
	in, err := h.inbox(0).Drain(ctx)
	if err != nil || len(in) == 0 || in[0].Kind != "alert_update" || !strings.Contains(in[0].Text, "slot_wal") {
		t.Fatalf("first input = %+v, %v", in, err)
	}

	// Lower or equal severity: no event. Higher: raised again, no new run.
	for _, sev := range []string{"sev3", "sev2"} {
		if err := postWatcher(ctx, svc, snap.ID, slotWALEvidence("orders", sev)); err != nil {
			t.Fatal(err)
		}
	}
	if err := postWatcher(ctx, svc, snap.ID, slotWALEvidence("orders", "sev1")); err != nil {
		t.Fatal(err)
	}
	esc = escalations(t, roomEvents(t, svc, snap.ID))
	if len(esc) != 2 || esc[1] != (events.RoomEscalatedPayload{Severity: "sev1", From: "sev2", Signal: "slot_wal"}) {
		t.Fatalf("escalations = %+v", esc)
	}
	time.Sleep(20 * time.Millisecond)
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("agent runs = %d, want 1", n)
	}
	// The newest forecast of the evidence is the snapshot's.
	got, _ = svc.GetRoom(ctx, snap.ID)
	if got.Severity != "sev1" || got.Forecast == nil || got.Forecast.Current != 96636764160 {
		t.Fatalf("snapshot after joins: severity=%q forecast=%+v", got.Severity, got.Forecast)
	}
}

// A non-forecast room is raised the same way, but its agent is not started
// again (the room never was quiet).
func TestEscalationOfWatcherRoom(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	a := watcherAlert("orders", alertin.StatusFiring)
	a.Severity = "sev3"
	snap, _, err := svc.OpenAlertRoom(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
	if snap.Origin != "" {
		t.Fatalf("origin = %q, want none", snap.Origin)
	}
	// Plain notes and non-watcher evidence raise nothing.
	if err := svc.PostRoomNote(context.Background(), snap.ID, "watcher: x\nevidence: {}"); err != nil {
		t.Fatal(err)
	}
	if err := svc.PostRoomEvidence(context.Background(), snap.ID, "note", json.RawMessage(`{"source":"alertmanager","pipeline":"orders","severity":"sev1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := svc.PostRoomEvidence(context.Background(), snap.ID, "note", json.RawMessage(`not json`)); err != nil {
		t.Fatal(err)
	}
	if err := postWatcher(context.Background(), svc, snap.ID, slotWALEvidence("orders", "sev2")); err != nil {
		t.Fatal(err)
	}
	esc := escalations(t, roomEvents(t, svc, snap.ID))
	if len(esc) != 1 || esc[0] != (events.RoomEscalatedPayload{Severity: "sev2", From: "sev3", Signal: "slot_wal"}) {
		t.Fatalf("escalations = %+v", esc)
	}
	time.Sleep(20 * time.Millisecond)
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("agent runs = %d, want 1", n)
	}
}

// TC4: closing a forecast room ends only the forecast's episode: the
// pipeline's own key has no closed room, so a later real signal opens a normal
// room with its agent.
func TestForecastRoomCloseDoesNotMuteRealSignal(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	snap := openForecast(t, h, svc)
	h.closeRoom(svc, snap.ID)

	fcKey := "watcher:orders:forecast:slot_wal_bytes"
	if id, at, ok := svc.LastRoomForKey("orders", fcKey); !ok || id != snap.ID || at == nil {
		t.Fatalf("forecast key: %q %v %v, want the closed room", id, at, ok)
	}
	if id, _, ok := svc.LastRoomForKey("orders", "watcher:orders"); ok {
		t.Fatalf("the pipeline key has a room %q after a forecast close", id)
	}
	waitFor(t, 2*time.Second, func() bool { return len(h.closedCalls()) == 1 })
	if c := h.closedCalls()[0]; c.dedupKey != fcKey || c.pipelineID != "orders" {
		t.Fatalf("OnRoomClosed = %+v", c)
	}
	real, created, err := svc.OpenAlertRoom(context.Background(), watcherAlert("orders", alertin.StatusFiring))
	if err != nil || !created || real.ID == snap.ID || real.Origin != "" {
		t.Fatalf("real signal: %+v created=%v err=%v", real, created, err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
}

// Only the watcher's own forecast key opens a quiet room: the same key from
// another source, or a malformed key, opens a normal room.
func TestForecastOriginOnlyForWatcherKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    func(alertin.Alert) alertin.Alert
	}{
		{"generic source", func(a alertin.Alert) alertin.Alert { a.Source = alertin.Generic; return a }},
		{"unknown metric", func(a alertin.Alert) alertin.Alert { a.DedupKey = "watcher:orders:forecast:cpu"; return a }},
		{"other pipeline's key", func(a alertin.Alert) alertin.Alert {
			a.DedupKey = "watcher:payments:forecast:slot_wal_bytes"
			return a
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newWatchHarness(t)
			svc := h.build()
			snap, _, err := svc.OpenAlertRoom(context.Background(), tc.a(forecastAlert("orders")))
			if err != nil {
				t.Fatal(err)
			}
			if snap.Origin != "" {
				t.Fatalf("origin = %q, want none", snap.Origin)
			}
			waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
		})
	}
}

// A restart keeps the room quiet: no "agent paused" notice (no agent event),
// origin and forecast rebuilt from the log; the escalation still starts the
// agent.
func TestForecastRoomQuietAcrossRestart(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	snap := openForecast(t, h, svc)
	svc.Close()

	svc2 := h.build()
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := agentEvents(roomEvents(t, svc2, snap.ID)); len(got) != 0 {
		t.Fatalf("restart wrote agent events: %+v", got)
	}
	got, err := svc2.GetRoom(context.Background(), snap.ID)
	if err != nil || got.Origin != events.RoomOriginForecast || got.Forecast == nil {
		t.Fatalf("reloaded snapshot: %+v %v", got, err)
	}
	if err := postWatcher(context.Background(), svc2, snap.ID, slotWALEvidence("orders", "sev3")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
}

// Closing a quiet forecast room drafts its post-mortem from the events: no
// model call.
func TestForecastRoomPostmortemSpendsNothing(t *testing.T) {
	var calls atomic.Int32
	svc, st, _ := newPMService(t, func(context.Context, []events.Event, agent.Options) (agent.PostmortemDraft, error) {
		calls.Add(1)
		return agent.PostmortemDraft{Summary: events.HandoffSummary{WhatBroke: "x"}}, nil
	})
	snap, _, err := svc.OpenAlertRoom(context.Background(), forecastAlert("orders"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GrantControl(context.Background(), mustUser(t, svc, "arjun"), snap.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	closeAs(t, svc, snap.ID)
	if p := drafted(t, waitPM(t, st, snap.ID)); p.Source != "events" {
		t.Fatalf("postmortem source = %q, want events", p.Source)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("postmortem model calls = %d, want 0", n)
	}
}
