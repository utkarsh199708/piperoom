package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// resolvedSetup returns a service that counts agent starts and OnRoomOpened
// calls, so a test can prove a resolved alert started nothing.
func resolvedSetup(t *testing.T) (*room.Service, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	runs, hooks := new(atomic.Int32), new(atomic.Int32)
	svc := room.New(room.Deps{
		Store:  memstore.New(),
		Users:  room.DefaultUsers(),
		Owners: testOwners(),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			runs.Add(1)
			<-ctx.Done()
			return nil
		},
		WriteHandoff: fakeWrite,
		OnRoomOpened: func(context.Context, string) { hooks.Add(1) },
	})
	t.Cleanup(svc.Close)
	return svc, runs, hooks
}

// A resolved notice with no open room is ignored: no room, no agent, no hook.
// A generic body has no dedup_key, so its key is a hash of the body and the
// resolved body never matches the firing one.
func TestResolvedGenericAlertWithoutRoomIsIgnored(t *testing.T) {
	svc, runs, hooks := resolvedSetup(t)
	ctx := context.Background()

	snap, created, err := svc.CreateRoomFromAlert(ctx, "disk full", "sev2", "orders",
		json.RawMessage(`{"title":"disk full","pipeline_id":"orders","status":"resolved"}`))
	if !errors.Is(err, room.ErrResolvedAlertNoRoom) {
		t.Fatalf("want ErrResolvedAlertNoRoom, got %v", err)
	}
	if created || snap.ID != "" {
		t.Fatalf("ignored alert returned room %q (created %v)", snap.ID, created)
	}
	if rooms, _ := svc.ListRooms(ctx); len(rooms) != 0 {
		t.Fatalf("ignored alert stored %d rooms, want 0", len(rooms))
	}
	time.Sleep(50 * time.Millisecond)
	if runs.Load() != 0 || hooks.Load() != 0 {
		t.Fatalf("ignored alert started %d agents and fired %d hooks, want 0 and 0", runs.Load(), hooks.Load())
	}
}

// A resolved notice whose room was already closed is ignored too: a closed
// room is not an open match, and the resolved notice must not open a new one.
func TestResolvedAlertForClosedRoomIsIgnored(t *testing.T) {
	svc, runs, hooks := resolvedSetup(t)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	firing := json.RawMessage(`{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag"}`)
	resolved := json.RawMessage(`{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag","status":"resolved"}`)

	first, created, err := svc.CreateRoomFromAlert(ctx, "consumer lag", "sev2", "orders", firing)
	if err != nil || !created {
		t.Fatalf("firing alert: created %v, err %v", created, err)
	}
	waitFor(t, 5*time.Second, func() bool { return hooks.Load() == 1 })
	if _, err := svc.GrantControl(ctx, arjun, first.ID, "arjun"); err != nil {
		t.Fatalf("take control: %v", err)
	}
	if _, err := svc.CloseRoom(ctx, arjun, first.ID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, created, err = svc.CreateRoomFromAlert(ctx, "consumer lag", "sev2", "orders", resolved)
	if !errors.Is(err, room.ErrResolvedAlertNoRoom) {
		t.Fatalf("resolved alert after close: want ErrResolvedAlertNoRoom, got %v", err)
	}
	if created {
		t.Fatal("resolved alert after close reported created")
	}
	if rooms, _ := svc.ListRooms(ctx); len(rooms) != 1 {
		t.Fatalf("want only the closed room, got %d rooms", len(rooms))
	}
	time.Sleep(50 * time.Millisecond)
	if runs.Load() != 1 || hooks.Load() != 1 {
		t.Fatalf("after ignored resolved alert: %d agent starts, %d hooks; want 1 and 1", runs.Load(), hooks.Load())
	}
}

// A resolved notice for an open room joins it and leaves the room open.
func TestResolvedAlertJoinsOpenRoomAndKeepsItOpen(t *testing.T) {
	svc, runs, hooks := resolvedSetup(t)
	ctx := context.Background()
	firing := json.RawMessage(`{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag"}`)
	resolved := json.RawMessage(`{"title":"consumer lag","pipeline_id":"orders","dedup_key":"lag","status":"resolved"}`)

	first, _, err := svc.CreateRoomFromAlert(ctx, "consumer lag", "sev2", "orders", firing)
	if err != nil {
		t.Fatalf("firing alert: %v", err)
	}
	joined, created, err := svc.CreateRoomFromAlert(ctx, "consumer lag", "sev2", "orders", resolved)
	if err != nil {
		t.Fatalf("resolved alert with open room: %v", err)
	}
	if created {
		t.Fatal("resolved alert with open room reported created")
	}
	if joined.ID != first.ID || joined.Status != "open" {
		t.Fatalf("resolved alert: room %s status %s, want open room %s", joined.ID, joined.Status, first.ID)
	}
	if got, err := svc.GetRoom(ctx, first.ID); err != nil || got.Status != "open" {
		t.Fatalf("room after resolved join: status %q, err %v; want open", got.Status, err)
	}
	if rooms, _ := svc.ListRooms(ctx); len(rooms) != 1 {
		t.Fatalf("want one room, got %d", len(rooms))
	}
	waitFor(t, 5*time.Second, func() bool { return hooks.Load() == 1 })
	if runs.Load() != 1 {
		t.Fatalf("resolved join started an agent: %d starts, want 1", runs.Load())
	}
}

// A firing alert with no open room still opens one (created is true).
func TestFiringAlertOpensRoom(t *testing.T) {
	svc, _, hooks := resolvedSetup(t)
	snap, created, err := svc.CreateRoomFromAlert(context.Background(), "disk full", "sev2", "orders",
		json.RawMessage(`{"title":"disk full","pipeline_id":"orders"}`))
	if err != nil {
		t.Fatalf("firing alert: %v", err)
	}
	if !created || snap.ID == "" || snap.Status != "open" {
		t.Fatalf("firing alert: created %v, room %+v; want a new open room", created, snap)
	}
	waitFor(t, 5*time.Second, func() bool { return hooks.Load() == 1 })
}
