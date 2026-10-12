package room_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// alertBody is a real Alertmanager group. Sent to the dev webhook it is just
// another JSON body; sent to the signed route it is parsed by alertin.
var alertBody = []byte(`{"groupKey":"{}:{alertname=\"Lag\"}","status":"firing","commonLabels":{"alertname":"Lag","severity":"critical","pipeline":"orders"},` +
	`"alerts":[{"status":"firing","labels":{"alertname":"Lag"}}]}`)

func alertDeps(st *memstore.Store, c *clock) func() room.Deps {
	return func() room.Deps {
		return room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, Now: c.Now}
	}
}

// A dev-webhook room and a signed room opened from the same body are two
// rooms, and each route's repeats join only its own room, before and after a
// restart. An X-User caller therefore cannot capture a signed source's later
// alerts. The route that opened each room is recorded in room.created.
func TestDevWebhookCannotTakeSignedAlertKey(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	deps := alertDeps(st, c)
	svc := room.New(deps())
	t.Cleanup(svc.Close)
	ctx := context.Background()

	signed, err := alertin.Config{}.Parse(alertin.Alertmanager, alertBody)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	signedRoom, created, err := svc.OpenAlertRoom(ctx, signed)
	if err != nil || !created {
		t.Fatalf("signed alert: created=%v err=%v", created, err)
	}
	devRoom, created, err := svc.CreateRoomFromAlert(ctx, "Lag", "sev1", "orders", alertBody)
	if err != nil || !created || devRoom.ID == signedRoom.ID {
		t.Fatalf("dev webhook: room %s created=%v err=%v, want its own room", devRoom.ID, created, err)
	}

	for id, want := range map[string]string{signedRoom.ID: "alertmanager", devRoom.ID: "webhook"} {
		p, err := events.Decode[events.RoomCreatedPayload](allEvents(t, st, id)[0])
		if err != nil || p.AlertSource != want || p.DedupKey == "" {
			t.Fatalf("room %s: alert_source %q dedup_key %q (err %v), want source %q and a key", id, p.AlertSource, p.DedupKey, err, want)
		}
	}

	check := func(label string, s *room.Service) {
		t.Helper()
		got, created, err := s.OpenAlertRoom(ctx, signed)
		if err != nil || created || got.ID != signedRoom.ID {
			t.Fatalf("%s: signed repeat joined %s created=%v err=%v, want %s", label, got.ID, created, err, signedRoom.ID)
		}
		got, created, err = s.CreateRoomFromAlert(ctx, "Lag", "sev1", "orders", alertBody)
		if err != nil || created || got.ID != devRoom.ID {
			t.Fatalf("%s: dev repeat joined %s created=%v err=%v, want %s", label, got.ID, created, err, devRoom.ID)
		}
	}
	check("before restart", svc)

	svc.Close()
	svc2 := room.New(deps())
	t.Cleanup(svc2.Close)
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	check("after restart", svc2)
	if rooms, _ := svc2.ListRooms(ctx); len(rooms) != 2 {
		t.Fatalf("want 2 rooms, got %d", len(rooms))
	}
}

// room.created is stored in a Postgres jsonb column, which rejects \u0000, so
// the stored dedup key (and the rest of the payload) must be free of NUL for
// both routes.
func TestRoomCreatedPayloadHasNoNUL(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(alertDeps(st, c)())
	t.Cleanup(svc.Close)
	ctx := context.Background()

	signed, err := alertin.Config{}.Parse(alertin.Alertmanager, alertBody)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	a, _, err := svc.OpenAlertRoom(ctx, signed)
	if err != nil {
		t.Fatalf("signed: %v", err)
	}
	b, _, err := svc.CreateRoomFromAlert(ctx, "Lag", "sev1", "orders", alertBody)
	if err != nil {
		t.Fatalf("dev: %v", err)
	}
	for _, id := range []string{a.ID, b.ID} {
		e := allEvents(t, st, id)[0]
		enc, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if bytes.Contains(enc, []byte(`\u0000`)) || bytes.IndexByte(e.Payload, 0) >= 0 {
			t.Fatalf("room %s: room.created contains NUL: %s", id, e.Payload)
		}
		p, err := events.Decode[events.RoomCreatedPayload](e)
		if err != nil || !strings.HasPrefix(p.DedupKey, "orders|") {
			t.Fatalf("room %s: dedup_key %q (err %v), want orders|...", id, p.DedupKey, err)
		}
	}
}

// seedLegacyRoom writes an open alert room the way the server did before
// room.created recorded alert_source and dedup_key.
func seedLegacyRoom(t *testing.T, st *memstore.Store, id string, raw []byte) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateRoom(ctx, store.Room{ID: id, Title: "Lag", Severity: "sev1", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("seed room: %v", err)
	}
	actor := events.Actor{ID: "system", Name: "System"}
	if _, err := st.Append(ctx, id, events.RoomCreated, actor, events.RoomCreatedPayload{
		Title: "Lag", Severity: "sev1", PipelineID: "orders", Alert: raw,
	}); err != nil {
		t.Fatalf("seed room.created: %v", err)
	}
}

// Rooms from before alert_source was recorded can only have come from the dev
// webhook: after a restart they are found by a dev-webhook repeat, never by a
// signed alert with the same body.
func TestLegacyRoomsAreWebhookRoomsOnly(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	seedLegacyRoom(t, st, "r_legacy", alertBody)
	svc := room.New(alertDeps(st, c)())
	t.Cleanup(svc.Close)
	ctx := context.Background()
	if err := svc.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}

	signed, err := alertin.Config{}.Parse(alertin.Alertmanager, alertBody)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, created, err := svc.OpenAlertRoom(ctx, signed)
	if err != nil || !created || got.ID == "r_legacy" {
		t.Fatalf("signed alert: room %s created=%v err=%v, want a new room", got.ID, created, err)
	}
	got, created, err = svc.CreateRoomFromAlert(ctx, "Lag", "sev1", "orders", alertBody)
	if err != nil || created || got.ID != "r_legacy" {
		t.Fatalf("dev repeat: room %s created=%v err=%v, want r_legacy", got.ID, created, err)
	}
}

// Two open rooms that hold the same key (only possible for rooms opened before
// dedup existed): repeats join one; when it closes the other takes over the key
// instead of a repeat opening a third room; only when both are closed does a
// repeat open a new room.
func TestClosingKeyHolderHandsKeyToOtherOpenRoom(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	raw := []byte(`{"title":"Lag","pipeline_id":"orders","dedup_key":"k"}`)
	seedLegacyRoom(t, st, "r_legacy1", raw)
	seedLegacyRoom(t, st, "r_legacy2", raw)
	svc := room.New(alertDeps(st, c)())
	t.Cleanup(svc.Close)
	ctx := context.Background()
	if err := svc.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	arjun := mustUser(t, svc, "arjun")
	repeat := func() (string, bool) {
		t.Helper()
		snap, created, err := svc.CreateRoomFromAlert(ctx, "Lag", "sev1", "orders", raw)
		if err != nil {
			t.Fatalf("repeat: %v", err)
		}
		return snap.ID, created
	}
	closeRoom := func(id string) {
		t.Helper()
		if _, err := svc.GrantControl(ctx, arjun, id, "arjun"); err != nil {
			t.Fatalf("take control: %v", err)
		}
		if _, err := svc.CloseRoom(ctx, arjun, id, "fixed"); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	winner, created := repeat()
	if created || (winner != "r_legacy1" && winner != "r_legacy2") {
		t.Fatalf("first repeat: room %s created=%v, want one of the open rooms", winner, created)
	}
	loser := "r_legacy1"
	if winner == loser {
		loser = "r_legacy2"
	}
	closeRoom(winner)
	if got, created := repeat(); created || got != loser {
		t.Fatalf("after closing the key holder: room %s created=%v, want the other open room %s", got, created, loser)
	}
	closeRoom(loser)
	if got, created := repeat(); !created || got == winner || got == loser {
		t.Fatalf("after closing both: room %s created=%v, want a new room", got, created)
	}
}
