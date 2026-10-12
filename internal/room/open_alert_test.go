package room_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// OpenAlertRoom takes the alert's own fields and its DedupKey: a signed source
// opens a room, a repeat of the same key joins it, a resolved repeat joins and
// leaves it open, a resolved alert with no room opens nothing, and the same key
// on another pipeline is another incident. After a restart the room is found
// again from its stored body alone, by the source's own key.
func TestOpenAlertRoomDedupAndRestart(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	deps := func() room.Deps {
		return room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, Now: c.Now}
	}
	svc := room.New(deps())
	t.Cleanup(svc.Close)
	ctx := context.Background()

	// A real Alertmanager body, parsed by alertin, as the signed route does.
	body := []byte(`{"groupKey":"{}:{alertname=\"Lag\"}","status":"firing","commonLabels":{"alertname":"Lag","severity":"critical","pipeline":"orders"},` +
		`"alerts":[{"status":"firing","labels":{"alertname":"Lag"}}]}`)
	firing, err := alertin.Config{}.Parse(alertin.Alertmanager, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	first, created, err := svc.OpenAlertRoom(ctx, firing)
	if err != nil || !created {
		t.Fatalf("first alert: created=%v err=%v", created, err)
	}
	if first.Title != "Lag" || first.Severity != "sev1" || first.PipelineID != "orders" {
		t.Fatalf("room = title %q severity %q pipeline %q", first.Title, first.Severity, first.PipelineID)
	}

	again, created, err := svc.OpenAlertRoom(ctx, firing)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("repeat: room %s created=%v err=%v, want join of %s", again.ID, created, err, first.ID)
	}

	resolved := firing
	resolved.Status = alertin.StatusResolved
	joined, created, err := svc.OpenAlertRoom(ctx, resolved)
	if err != nil || created || joined.ID != first.ID || joined.Status != "open" {
		t.Fatalf("resolved repeat: room %s (%s) created=%v err=%v", joined.ID, joined.Status, created, err)
	}

	other := firing
	other.PipelineID = "billing"
	if snap, created, err := svc.OpenAlertRoom(ctx, other); err != nil || !created || snap.ID == first.ID {
		t.Fatalf("same key, other pipeline: room %s created=%v err=%v, want a new room", snap.ID, created, err)
	}

	noRoom := resolved
	noRoom.DedupKey = "alertmanager:never-fired"
	if _, _, err := svc.OpenAlertRoom(ctx, noRoom); !errors.Is(err, room.ErrResolvedAlertNoRoom) {
		t.Fatalf("resolved with no room: err = %v, want ErrResolvedAlertNoRoom", err)
	}

	// Restart: the log keeps only the raw body, yet the source's key is rebuilt.
	svc.Close()
	svc2 := room.New(deps())
	t.Cleanup(svc2.Close)
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	after, created, err := svc2.OpenAlertRoom(ctx, firing)
	if err != nil || created || after.ID != first.ID {
		t.Fatalf("after restart: room %s created=%v err=%v, want join of %s", after.ID, created, err, first.ID)
	}
	if rooms, _ := svc2.ListRooms(ctx); len(rooms) != 2 {
		t.Fatalf("want 2 rooms (orders, billing), got %d", len(rooms))
	}
}
