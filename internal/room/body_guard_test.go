package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// guardSetup returns a service over st that counts agent starts and
// OnRoomOpened calls, so a test can prove a refused body started nothing.
func guardSetup(t *testing.T, st store.Store) (*room.Service, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	runs, hooks := new(atomic.Int32), new(atomic.Int32)
	svc := room.New(room.Deps{
		Store:  st,
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

func wantValidation400(t *testing.T, err error) {
	t.Helper()
	var re *room.Error
	if !errors.As(err, &re) || re.Status != http.StatusBadRequest || re.Code != "validation" {
		t.Fatalf("err = %v, want a 400 validation room.Error", err)
	}
}

// Bodies Postgres jsonb cannot store are refused with 400 before anything is
// written, on both the dev webhook and the shared signed entry point: no room
// row, no event, no agent, no hook. Bodies that merely look similar are fine.
func TestAlertBodyGuard(t *testing.T) {
	bad := map[string]string{
		"nul escape":                  `{"title":"x\u0000y","pipeline_id":"orders"}`,
		"nul escape uppercase hex":    `{"title":"\u0000"}`,
		"invalid utf-8":               "{\"title\":\"x\xffy\"}",
		"raw nul byte":                "{\"title\":\"x\x00y\"}",
		"lone high surrogate":         `{"title":"\ud83d"}`,
		"lone low surrogate":          `{"title":"\ude00"}`,
		"high then plain char":        `{"title":"\ud83dA"}`,
		"high then non-low escape":    `{"title":"\ud83dA"}`,
		"high then high":              `{"title":"\ud83d\ud83d"}`,
		"low then high":               `{"title":"\ude00\ud83d"}`,
		"high at end of string":       `{"title":"x\uD83D"}`,
		"nul after escaped backslash": `{"title":"\\\u0000"}`,
		"nul in a key":                `{"a\u0000":1}`,
		"nul inside a nested value":   `{"labels":{"k":["a","b\u0000"]}}`,
	}
	for name, body := range bad {
		t.Run(name+"/webhook", func(t *testing.T) {
			svc, runs, hooks := guardSetup(t, memstore.New())
			_, created, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", json.RawMessage(body))
			wantValidation400(t, err)
			assertNothingCreated(t, svc, runs, hooks, created)
		})
		t.Run(name+"/signed", func(t *testing.T) {
			svc, runs, hooks := guardSetup(t, memstore.New())
			_, created, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
				Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2",
				PipelineID: "orders", DedupKey: "generic:k", Raw: json.RawMessage(body),
			})
			wantValidation400(t, err)
			assertNothingCreated(t, svc, runs, hooks, created)
		})
	}

	good := map[string]string{
		"valid surrogate pair":     `{"title":"😀"}`,
		"escaped backslash then u": `{"title":"\\u0000"}`,
		"plain unicode":            `{"title":"café 中"}`,
		"escaped quote before u":   `{"title":"\"A"}`,
		"literal utf-8":            "{\"title\":\"café 中\"}",
	}
	for name, body := range good {
		t.Run(name, func(t *testing.T) {
			svc, _, _ := guardSetup(t, memstore.New())
			_, created, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", json.RawMessage(body))
			if err != nil || !created {
				t.Fatalf("created=%v err=%v, want the room opened", created, err)
			}
		})
	}
}

func assertNothingCreated(t *testing.T, svc *room.Service, runs, hooks *atomic.Int32, created bool) {
	t.Helper()
	if created {
		t.Fatal("a refused body reported a created room")
	}
	if rooms, _ := svc.ListRooms(context.Background()); len(rooms) != 0 {
		t.Fatalf("refused body left %d rooms, want 0", len(rooms))
	}
	time.Sleep(50 * time.Millisecond)
	if runs.Load() != 0 || hooks.Load() != 0 {
		t.Fatalf("refused body started %d agents and fired %d hooks, want 0 and 0", runs.Load(), hooks.Load())
	}
}

// A refused body that repeats an open alert's key must not write to that room
// either (a join appends a system message).
func TestAlertBodyGuardWritesNothingToAnOpenRoom(t *testing.T) {
	st := memstore.New()
	svc, _, _ := guardSetup(t, st)
	ctx := context.Background()
	first, _, err := svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", json.RawMessage(`{"title":"lag","pipeline_id":"orders","dedup_key":"k"}`))
	if err != nil {
		t.Fatalf("first alert: %v", err)
	}
	before := len(allEvents(t, st, first.ID))
	_, _, err = svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", json.RawMessage(`{"title":"lag\u0000","pipeline_id":"orders","dedup_key":"k"}`))
	wantValidation400(t, err)
	if after := len(allEvents(t, st, first.ID)); after != before {
		t.Fatalf("refused body appended %d events to the open room", after-before)
	}
}

// POST /api/rooms stores a client title in a text column and in jsonb: NUL and
// invalid UTF-8 are refused before any write.
func TestCreateRoomTitleGuard(t *testing.T) {
	for name, title := range map[string]string{"nul": "bad\x00title", "invalid utf-8": "bad\xfftitle"} {
		t.Run(name, func(t *testing.T) {
			svc, runs, hooks := guardSetup(t, memstore.New())
			_, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), title, "sev2", "orders")
			wantValidation400(t, err)
			assertNothingCreated(t, svc, runs, hooks, false)
		})
	}
}

// failAppendStore refuses to append room.created, as a real store would for a
// payload it cannot persist (or a database that just went away).
type failAppendStore struct {
	*memstore.Store
}

func (f failAppendStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if typ == events.RoomCreated {
		return events.Event{}, errors.New("simulated append failure")
	}
	return f.Store.Append(ctx, roomID, typ, actor, payload)
}

// If room.created cannot be appended, no room is left behind: not in the cache
// and not as a row in the store, for both creation paths.
func TestFailedRoomCreatedLeavesNoOrphanRow(t *testing.T) {
	ctx := context.Background()
	tests := map[string]func(*room.Service) error{
		"POST /api/rooms": func(svc *room.Service) error {
			_, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "t", "sev2", "orders")
			return err
		},
		"alert webhook": func(svc *room.Service) error {
			_, _, err := svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", json.RawMessage(`{"title":"lag"}`))
			return err
		},
		"signed alert": func(svc *room.Service) error {
			_, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2", PipelineID: "orders", DedupKey: "generic:k", Raw: json.RawMessage(`{}`)})
			return err
		},
	}
	for name, create := range tests {
		t.Run(name, func(t *testing.T) {
			mem := memstore.New()
			svc, runs, hooks := guardSetup(t, failAppendStore{mem})
			if err := create(svc); err == nil {
				t.Fatal("want the append failure returned")
			}
			assertNothingCreated(t, svc, runs, hooks, false)
			if rows, _ := mem.ListRooms(ctx); len(rows) != 0 {
				t.Fatalf("store keeps %d orphan room rows, want 0", len(rows))
			}
			// The same alert can open a room later: nothing is left in the key index.
			if name != "POST /api/rooms" {
				svc2, _, _ := guardSetup(t, mem)
				if _, created, err := svc2.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", json.RawMessage(`{"title":"lag"}`)); err != nil || !created {
					t.Fatalf("retry after failure: created=%v err=%v", created, err)
				}
			}
		})
	}
}

// An empty row left by a crash (or a failed delete) is skipped by Load: it is
// not a room.
func TestLoadSkipsRoomRowWithNoEvents(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	if err := mem.CreateRoom(ctx, store.Room{ID: "r_orphan", Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	svc, _, _ := guardSetup(t, mem)
	if err := svc.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	if rooms, _ := svc.ListRooms(ctx); len(rooms) != 0 {
		t.Fatalf("Load listed %d rooms, want the empty row skipped", len(rooms))
	}
}

// DeleteEmptyRoom only deletes a room that has no events.
func TestMemstoreDeleteEmptyRoom(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	row := store.Room{ID: "r_a", Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}
	if err := mem.CreateRoom(ctx, row); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mem.Append(ctx, "r_a", events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}, events.RoomCreatedPayload{Title: "t"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := mem.DeleteEmptyRoom(ctx, "r_a"); !errors.Is(err, store.ErrRoomNotEmpty) {
		t.Fatalf("room with events: err = %v, want ErrRoomNotEmpty", err)
	}
	row.ID = "r_b"
	if err := mem.CreateRoom(ctx, row); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := mem.DeleteEmptyRoom(ctx, "r_b"); err != nil {
		t.Fatalf("empty room: %v", err)
	}
	if err := mem.DeleteEmptyRoom(ctx, "r_b"); err != nil {
		t.Fatalf("missing room must be a no-op: %v", err)
	}
	if rows, _ := mem.ListRooms(ctx); len(rows) != 1 || rows[0].ID != "r_a" {
		t.Fatalf("rows = %+v, want only r_a", rows)
	}
}
