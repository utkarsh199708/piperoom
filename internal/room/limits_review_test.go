package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// fillLog has the agent emit until the store refuses it, then people comment
// until the reserve is used up too, so that nothing but room.closed fits.
func fillLog(t *testing.T, e *limitsEnv, roomID string) {
	t.Helper()
	ctx := context.Background()
	sink := e.runs.sink(t)
	var err error
	for i := 0; i < 10000 && err == nil; i++ {
		err = sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "step"})
	}
	if !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("agent emit at the cap: %v", err)
	}
	err = nil
	for i := 0; i < 1000 && err == nil; i++ {
		_, err = e.svc.AddComment(ctx, mustUser(t, e.svc, "priya"), roomID, "", "comment")
	}
	if !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("comment past the reserve: %v", err)
	}
}

// TestNoteIntoFullRoomIsDropped: the watcher's note into a room whose log
// (reserve included) is full is dropped with nil, so the watcher does not
// retry it every tick, and nothing is stored.
func TestNoteIntoFullRoomIsDropped(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{RoomMaxEvents: 100})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	fillLog(t, e, snap.ID)
	before := len(typedEvents(t, e.st, snap.ID, ""))
	if err := e.svc.PostRoomNote(ctx, snap.ID, "lag still rising"); err != nil {
		t.Fatalf("note into a full room: %v, want nil", err)
	}
	if after := len(typedEvents(t, e.st, snap.ID, "")); after != before {
		t.Fatalf("events %d -> %d: the note was stored", before, after)
	}
}

// TestRepeatAlertJoinsFullRoom: a repeat of the alert finds its full room and
// returns it (created false, no error) instead of opening a second room or
// failing the webhook; the join message is dropped.
func TestRepeatAlertJoinsFullRoom(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{RoomMaxEvents: 100})
	ctx := context.Background()
	raw := json.RawMessage(`{"title":"lag","pipeline_id":"orders","dedup_key":"full-1"}`)
	first, created, err := e.svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", raw)
	if err != nil || !created {
		t.Fatalf("first alert: created=%v err=%v", created, err)
	}
	fillLog(t, e, first.ID)
	before := len(typedEvents(t, e.st, first.ID, ""))
	again, created, err := e.svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", raw)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("repeat alert: id=%s created=%v err=%v, want the full room %s", again.ID, created, err, first.ID)
	}
	if after := len(typedEvents(t, e.st, first.ID, "")); after != before {
		t.Fatalf("events %d -> %d into a full room", before, after)
	}
	if list, _ := e.svc.ListRooms(ctx); len(list) != 1 {
		t.Fatalf("rooms = %d, want 1", len(list))
	}
}

// TestRestartReadsByteCount: a restarted service takes the room's byte count
// from the store (loadUsageLocked), so the 90% warning fires when the log
// crosses 90%, not 90% of what this process appended.
func TestRestartReadsByteCount(t *testing.T) {
	l := config.Limits{RoomMaxEvents: 100000, RoomMaxBytes: 20000}
	e := newLimitsEnv(t, l)
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	sink := e.runs.sink(t)
	text := strings.Repeat("x", 500)
	for {
		u, err := e.st.RoomUsage(ctx, snap.ID)
		if err != nil {
			t.Fatal(err)
		}
		if u.Bytes >= 16500 {
			break
		}
		if err := sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: text}); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	if w := typedEvents(t, e.st, snap.ID, events.RoomLogCapWarning); len(w) != 0 {
		t.Fatalf("warned before 90%%: %+v", w)
	}
	e.svc.Close()

	svc2 := room.New(room.Deps{Store: e.st, Users: adminUsers(t), Owners: testOwners(), Run: newRunRecorder().run, Now: e.clock.Now, Limits: l})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	// ~1.6 KB more passes 18000 bytes (90%) only when counted on top of the
	// stored ~16.5 KB; on its own it is far below.
	if _, err := svc2.AddComment(ctx, mustUser(t, svc2, "priya"), snap.ID, "", strings.Repeat("y", 1600)); err != nil {
		t.Fatalf("comment: %v", err)
	}
	w := typedEvents(t, e.st, snap.ID, events.RoomLogCapWarning)
	if len(w) != 1 {
		t.Fatalf("cap warnings after restart = %d, want 1", len(w))
	}
	p, _ := events.Decode[events.RoomLogCapWarningPayload](w[0])
	if p.Bytes < 18000 || p.MaxBytes != 20000 {
		t.Fatalf("warning payload = %+v, want the stored bytes counted", p)
	}
}

// TestEraseRemovesStaleArchive: a plain delete (an erase) removes an export of
// the room an earlier archive=true attempt left in the archive directory, and
// says so; with no such file it reports false.
func TestEraseRemovesStaleArchive(t *testing.T) {
	dir := t.TempDir()
	e := newLimitsEnv(t, config.Limits{ArchiveDir: dir})
	ctx := context.Background()
	ada := mustUser(t, e.svc, "ada")

	stale := createRoom(t, e.svc, "arjun")
	path := filepath.Join(dir, "orders", stale.ID+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"room":"old copy"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := e.svc.DeleteRoom(ctx, ada, stale.ID, "erase", false)
	if err != nil {
		t.Fatal(err)
	}
	if d.Archived || !d.ArchiveRemoved {
		t.Fatalf("deletion = %+v, want archived false, archive_removed true", d)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale archive still there: %v", err)
	}
	if _, err := e.st.GetRoom(ctx, stale.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("room left: %v", err)
	}

	clean := createRoom(t, e.svc, "arjun")
	d, err = e.svc.DeleteRoom(ctx, ada, clean.ID, "erase", false)
	if err != nil || d.ArchiveRemoved {
		t.Fatalf("no stale file: %+v %v, want archive_removed false", d, err)
	}

	// archive=true keeps the copy it just wrote.
	kept := createRoom(t, e.svc, "arjun")
	d, err = e.svc.DeleteRoom(ctx, ada, kept.ID, "keep", true)
	if err != nil || !d.Archived || d.ArchiveRemoved {
		t.Fatalf("archive delete: %+v %v", d, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "orders", kept.ID+".json")); err != nil {
		t.Fatalf("archive missing: %v", err)
	}
}

// TestDeleteTenancy: with owners.yaml tenants (#112), an admin outside the
// room's tenant gets 404, for a live room and for one deleted before (the
// audit row); a non-admin still gets 403 first; an admin in the tenant
// deletes.
func TestDeleteTenancy(t *testing.T) {
	ctx := context.Background()
	users, err := room.ParseUsers([]byte(`
users:
  - {id: arjun, name: Arjun}
  - {id: ada, name: Ada, admin: true}
  - {id: eve, name: Eve, admin: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(`tenants:
  t1: {members: [user:arjun, user:eve]}
  t2: {members: [user:ada]}
pipelines:
  - id: orders
    tenant: t1
    topic_prefix: orders
    source_tables: [public.orders]
    connectors: [orders-c]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: users, Owners: own, Run: newRunRecorder().run})
	t.Cleanup(svc.Close)
	if !svc.TenancyOn() {
		t.Fatal("tenancy is off")
	}

	live := createRoom(t, svc, "arjun")
	_, err = svc.DeleteRoom(ctx, mustUser(t, svc, "arjun"), live.ID, "cleanup", false)
	wantCode(t, err, "not_allowed")
	_, err = svc.DeleteRoom(ctx, mustUser(t, svc, "ada"), live.ID, "cleanup", false)
	wantCode(t, err, "not_found")
	if _, err := st.GetRoom(ctx, live.ID); err != nil {
		t.Fatalf("hidden delete removed the room: %v", err)
	}

	gone := createRoom(t, svc, "arjun")
	if _, err := svc.DeleteRoom(ctx, mustUser(t, svc, "eve"), gone.ID, "cleanup", false); err != nil {
		t.Fatal(err)
	}
	_, err = svc.DeleteRoom(ctx, mustUser(t, svc, "ada"), gone.ID, "again", false)
	wantCode(t, err, "not_found")
	// The admin who can see it gets the row.
	if d, err := svc.DeleteRoom(ctx, mustUser(t, svc, "eve"), gone.ID, "again", false); err != nil || d.RoomID != gone.ID {
		t.Fatalf("visible repeat: %+v %v", d, err)
	}
}
