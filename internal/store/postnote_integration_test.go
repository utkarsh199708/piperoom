//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
)

// The room service on real Postgres: a watcher note with NUL bytes and invalid
// UTF-8 is stored with U+FFFD in their place (Postgres text and jsonb refuse
// the raw bytes, with SQLSTATE 22P05 for a NUL), and LastRoomForKey finds the
// room through the stored dedup_key, open and closed, from a fresh service.
func TestServicePostRoomNoteAndLastRoomForKey(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	newSvc := func() *room.Service {
		svc := room.New(room.Deps{
			Store: s, Users: room.DefaultUsers(),
			Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
				<-ctx.Done()
				return nil
			},
		})
		t.Cleanup(svc.Close)
		return svc
	}
	svc := newSvc()
	pipeline := "orders"
	key := "watcher:" + strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_")
	alert := alertin.Alert{
		Source: alertin.Watcher, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: pipeline,
		Title: "orders: connector pg-orders FAILED 4m", DedupKey: key, Raw: json.RawMessage(`{"signal":"connector_down"}`),
	}
	snap, created, err := svc.OpenAlertRoom(ctx, alert)
	if err != nil || !created {
		t.Fatalf("open: created=%v err=%v", created, err)
	}

	rep := string(rune(0xFFFD))
	nul := string(rune(0))
	if err := svc.PostRoomNote(ctx, snap.ID, "task 0: "+nul+"trace"+nul+" \xff\xfe end"); err != nil {
		t.Fatalf("note with NUL and invalid UTF-8 on Postgres: %v", err)
	}
	if err := svc.PostRoomNote(ctx, snap.ID, strings.Repeat(nul, 3000)); err != nil {
		t.Fatalf("long note of NULs on Postgres: %v", err)
	}

	evs, err := s.Since(ctx, snap.ID, 0, 100)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	var notes []string
	for _, e := range evs {
		if e.Type != events.AgentMessage || e.Actor.Kind != events.ActorSystem {
			continue
		}
		p, err := events.Decode[events.AgentMessagePayload](e)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		notes = append(notes, p.Text)
	}
	if len(notes) != 2 {
		t.Fatalf("stored notes = %q, want 2", notes)
	}
	if want := "task 0: " + rep + "trace" + rep + " " + rep + " end"; notes[0] != want {
		t.Fatalf("note 0 = %q, want %q", notes[0], want)
	}
	if len(notes[1]) > 4096 || !utf8.ValidString(notes[1]) || !strings.HasSuffix(notes[1], "…") {
		t.Fatalf("note 1: %d bytes, want <= 4096 valid UTF-8 ending in …", len(notes[1]))
	}

	// A fresh service finds the open room through the store.
	svc2 := newSvc()
	id, closedAt, ok := svc2.LastRoomForKey(pipeline, key)
	if !ok || id != snap.ID || closedAt != nil {
		t.Fatalf("open room: %q %v %v, want %q nil true", id, closedAt, ok, snap.ID)
	}
	if _, _, ok := svc2.LastRoomForKey(pipeline, key+"-other"); ok {
		t.Fatal("another key matched")
	}
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	arjun, _ := svc2.LookupUser("arjun")
	if _, err := svc2.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
		t.Fatalf("take control: %v", err)
	}
	if _, err := svc2.CloseRoom(ctx, arjun, snap.ID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
	id, closedAt, ok = newSvc().LastRoomForKey(pipeline, key)
	if !ok || id != snap.ID || closedAt == nil {
		t.Fatalf("closed room: %q %v %v, want %q <time> true", id, closedAt, ok, snap.ID)
	}
}
