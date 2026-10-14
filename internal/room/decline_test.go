package room_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// suggest has priya (not the driver of arjun's room) suggest text and returns
// the suggestion id.
func suggest(t *testing.T, svc *room.Service, roomID, text string) string {
	t.Helper()
	ev, err := svc.Steer(context.Background(), mustUser(t, svc, "priya"), roomID, text)
	if err != nil {
		t.Fatalf("steer: %v", err)
	}
	if ev.Type != events.SteerSuggested {
		t.Fatalf("want steer.suggested, got %s", ev.Type)
	}
	p, err := events.Decode[events.SteerSuggestedPayload](ev)
	if err != nil {
		t.Fatalf("decode suggestion: %v", err)
	}
	return p.SuggestionID
}

func eventCount(t *testing.T, st *memstore.Store, roomID string) int {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	return len(evs)
}

func TestDeclineSuggestionByDriver(t *testing.T) {
	svc, st, c := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "restart the connector")
	keep := suggest(t, svc, roomID, "check the registry")

	c.Advance(20 * time.Minute)
	ev, err := svc.DeclineSuggestion(ctx, mustUser(t, svc, "arjun"), roomID, sid, "not yet")
	if err != nil {
		t.Fatalf("decline: %v", err)
	}
	if ev.Type != events.SteerDeclined {
		t.Fatalf("want steer.declined, got %s", ev.Type)
	}
	if ev.Actor.Kind != events.ActorHuman || ev.Actor.ID != "arjun" {
		t.Fatalf("actor should be the driver as a human: %+v", ev.Actor)
	}
	p, err := events.Decode[events.SteerDeclinedPayload](ev)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.SuggestionID != sid || p.Reason != "not yet" {
		t.Fatalf("bad payload: %+v", p)
	}
	snap, err := svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if len(snap.PendingSuggestions) != 1 || snap.PendingSuggestions[0].SuggestionID != keep {
		t.Fatalf("only the other suggestion should stay pending: %+v", snap.PendingSuggestions)
	}
	// A driver action renews the lease.
	if want := c.Now().UTC().Add(room.DriverLease); snap.Driver == nil || !snap.Driver.LeaseUntil.Equal(want) {
		t.Fatalf("lease not renewed: %+v, want %v", snap.Driver, want)
	}
	row, err := st.GetSuggestion(ctx, sid)
	if err != nil {
		t.Fatalf("get suggestion: %v", err)
	}
	if row.Status != "declined" {
		t.Fatalf("row status = %q, want declined", row.Status)
	}
}

func TestDeclineSuggestionErrors(t *testing.T) {
	svc, st, _ := newTestService(t)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	other := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "look at lag")

	// Non-driver.
	_, err := svc.DeclineSuggestion(ctx, mustUser(t, svc, "priya"), roomID, sid, "")
	wantCode(t, err, "not_driver")
	// Unknown id, and an id of another room.
	_, err = svc.DeclineSuggestion(ctx, arjun, roomID, "g_missing", "")
	wantCode(t, err, "not_found")
	_, err = svc.DeclineSuggestion(ctx, arjun, other, sid, "")
	wantCode(t, err, "not_found")
	// Unknown room.
	_, err = svc.DeclineSuggestion(ctx, arjun, "r_missing", sid, "")
	wantCode(t, err, "not_found")

	// reason validation, before anything is written.
	before := eventCount(t, st, roomID)
	for name, reason := range map[string]string{
		"too long":     strings.Repeat("a", 501),
		"too long mb":  strings.Repeat("é", 501),
		"nul":          "a\x00b",
		"invalid utf8": "a\xffb",
	} {
		_, err = svc.DeclineSuggestion(ctx, arjun, roomID, sid, reason)
		if err == nil {
			t.Fatalf("%s: want validation error", name)
		}
		wantCode(t, err, "validation")
	}
	if got := eventCount(t, st, roomID); got != before {
		t.Fatalf("failed declines appended events: %d -> %d", before, got)
	}
	snap, _ := svc.GetRoom(ctx, roomID)
	if len(snap.PendingSuggestions) != 1 {
		t.Fatalf("suggestion should still be pending: %+v", snap.PendingSuggestions)
	}
}

func TestDeclineSuggestionReasonLimitIsRunes(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "x")
	reason := strings.Repeat("ü", 500) // 1000 bytes, 500 characters
	ev, err := svc.DeclineSuggestion(context.Background(), mustUser(t, svc, "arjun"), roomID, sid, reason)
	if err != nil {
		t.Fatalf("500 multibyte characters must be accepted: %v", err)
	}
	p, _ := events.Decode[events.SteerDeclinedPayload](ev)
	if p.Reason != reason {
		t.Fatalf("reason not kept")
	}
}

func TestDeclineSuggestionNoReason(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "x")
	ev, err := svc.DeclineSuggestion(context.Background(), mustUser(t, svc, "arjun"), roomID, sid, "")
	if err != nil {
		t.Fatalf("decline: %v", err)
	}
	if strings.Contains(string(ev.Payload), "reason") {
		t.Fatalf("empty reason should be omitted: %s", ev.Payload)
	}
}

func TestDeclineSuggestionClosedRoom(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "x")
	if _, err := svc.CloseRoom(ctx, arjun, roomID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := svc.DeclineSuggestion(ctx, arjun, roomID, sid, "")
	wantCode(t, err, "room_closed")
}

// TestDeclineSuggestionAlreadyDecided: sent then declined, declined twice, and
// declined then sent all answer 409 already_decided and append nothing.
func TestDeclineSuggestionAlreadyDecided(t *testing.T) {
	svc, st, _ := newTestService(t)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID

	sent := suggest(t, svc, roomID, "send me")
	if _, err := svc.SendSuggestion(ctx, arjun, roomID, sent); err != nil {
		t.Fatalf("send: %v", err)
	}
	n := eventCount(t, st, roomID)
	_, err := svc.DeclineSuggestion(ctx, arjun, roomID, sent, "")
	wantCode(t, err, "already_decided")

	declined := suggest(t, svc, roomID, "decline me")
	if _, err := svc.DeclineSuggestion(ctx, arjun, roomID, declined, "no"); err != nil {
		t.Fatalf("decline: %v", err)
	}
	n = eventCount(t, st, roomID)
	_, err = svc.DeclineSuggestion(ctx, arjun, roomID, declined, "no")
	wantCode(t, err, "already_decided")
	_, err = svc.SendSuggestion(ctx, arjun, roomID, declined)
	wantCode(t, err, "already_decided")
	if got := eventCount(t, st, roomID); got != n {
		t.Fatalf("refused decisions appended events: %d -> %d", n, got)
	}
	row, _ := st.GetSuggestion(ctx, declined)
	if row.Status != "declined" {
		t.Fatalf("row status = %q, want declined", row.Status)
	}
}

// TestSendRefusesSuggestionDeclinedInLog: when the status write after a
// decline was lost (the row still says pending), send still answers
// already_decided from the log and leaves no steer.sent.
func TestSendRefusesSuggestionDeclinedInLog(t *testing.T) {
	svc, st, _ := newTestService(t)
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	sid := suggest(t, svc, roomID, "x")
	if _, err := svc.DeclineSuggestion(ctx, arjun, roomID, sid, ""); err != nil {
		t.Fatalf("decline: %v", err)
	}
	if err := st.SetSuggestionStatus(ctx, sid, "pending"); err != nil {
		t.Fatalf("reset status: %v", err)
	}
	n := eventCount(t, st, roomID)
	_, err := svc.SendSuggestion(ctx, arjun, roomID, sid)
	wantCode(t, err, "already_decided")
	_, err = svc.DeclineSuggestion(ctx, arjun, roomID, sid, "")
	wantCode(t, err, "already_decided")
	if got := eventCount(t, st, roomID); got != n {
		t.Fatalf("refused decisions appended events: %d -> %d", n, got)
	}
}
