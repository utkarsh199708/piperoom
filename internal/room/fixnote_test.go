package room_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

func lastComment(t *testing.T, st *memstore.Store, roomID string) events.CommentAddedPayload {
	t.Helper()
	evs := typesOf(allEvents(t, st, roomID), events.CommentAdded)
	if len(evs) == 0 {
		t.Fatal("no comment.added")
	}
	p, err := events.Decode[events.CommentAddedPayload](evs[len(evs)-1])
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Kind is "" or "fix_note"; anything else is 400 and writes nothing.
func TestCommentKindValidation(t *testing.T) {
	cases := []struct {
		kind    string
		wantErr bool
	}{
		{"", false},
		{"fix_note", false},
		{"FIX_NOTE", true},
		{"fix note", true},
		{"note", true},
		{" fix_note", true},
	}
	for _, c := range cases {
		t.Run("kind="+c.kind, func(t *testing.T) {
			svc, st, _ := newTestService(t)
			roomID := createRoom(t, svc, "arjun").ID
			_, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "restarted by hand", c.kind)
			if c.wantErr {
				wantCode(t, err, "validation")
				if n := countType(t, st, roomID, events.CommentAdded); n != 0 {
					t.Fatalf("%d comments after a 400", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("kind %q refused: %v", c.kind, err)
			}
			if got := lastComment(t, st, roomID).Kind; got != c.kind {
				t.Fatalf("stored kind = %q, want %q", got, c.kind)
			}
		})
	}
}

// A fix note takes the same text rules as a comment.
func TestFixNoteTextRules(t *testing.T) {
	cases := []struct {
		name, text string
	}{
		{"empty", ""},
		{"blank", "  \n "},
		{"too long", strings.Repeat("a", room.MaxMessageChars+1)},
		{"NUL", "restarted\x00"},
		{"invalid UTF-8", "restarted\xff"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc, st, _ := newTestService(t)
			roomID := createRoom(t, svc, "arjun").ID
			_, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", c.text, events.CommentKindFixNote)
			wantCode(t, err, "validation")
			if n := countType(t, st, roomID, events.CommentAdded); n != 0 {
				t.Fatalf("%d comments after a 400", n)
			}
		})
	}
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", strings.Repeat("a", room.MaxMessageChars), events.CommentKindFixNote); err != nil {
		t.Fatalf("a note at the limit: %v", err)
	}
}

// In an open room anyone who can comment can log a fix note; "@agent" still
// wakes the agent.
func TestFixNoteInOpenRoom(t *testing.T) {
	svc, st, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "restarted order-sync by hand", events.CommentKindFixNote); err != nil {
		t.Fatal(err)
	}
	p := lastComment(t, st, roomID)
	if p.Kind != events.CommentKindFixNote || p.MentionsAgent || p.Text != "restarted order-sync by hand" {
		t.Fatalf("payload = %+v", p)
	}
	if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "@agent please check the lag", events.CommentKindFixNote); err != nil {
		t.Fatal(err)
	}
	if p := lastComment(t, st, roomID); !p.MentionsAgent {
		t.Fatalf("an @agent mention in an open room is lost: %+v", p)
	}
}

// A plain comment is unchanged: no kind, closed room refuses.
func TestPlainCommentUnchanged(t *testing.T) {
	svc, st, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "hello"); err != nil {
		t.Fatal(err)
	}
	if p := lastComment(t, st, roomID); p.Kind != "" {
		t.Fatalf("kind = %q on a plain comment", p.Kind)
	}
	closeRoomAs(t, svc, roomID, "arjun")
	_, err := svc.AddComment(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "late")
	wantCode(t, err, "room_closed")
	_, err = svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "late", "")
	wantCode(t, err, "room_closed")
}

// Fix notes on a closed room: the closer or a member of the pipeline, within
// 24 hours of the close.
func TestFixNoteClosedRoomRule(t *testing.T) {
	stranger := room.User{ID: "stranger", Name: "Stranger"}
	other := room.User{ID: "other", Name: "Other"}
	cases := []struct {
		name     string
		closer   string // who creates (so drives) and closes the room
		author   func(*room.Service) room.User
		after    time.Duration // time since the close
		wantCode string        // "" = accepted
	}{
		{"member, just closed", "arjun", func(s *room.Service) room.User { return mustUser(t, s, "kabir") }, time.Minute, ""},
		{"closer who is a member", "arjun", func(s *room.Service) room.User { return mustUser(t, s, "arjun") }, time.Hour, ""},
		{"closer who is not a member", "stranger", func(*room.Service) room.User { return stranger }, time.Hour, ""},
		{"member, exactly 24 h", "arjun", func(s *room.Service) room.User { return mustUser(t, s, "priya") }, 24 * time.Hour, ""},
		{"stranger, not the closer", "arjun", func(*room.Service) room.User { return other }, time.Hour, "not_allowed"},
		{"another stranger, not the closer", "stranger", func(*room.Service) room.User { return other }, time.Hour, "not_allowed"},
		{"member, after 24 h", "arjun", func(s *room.Service) room.User { return mustUser(t, s, "kabir") }, 24*time.Hour + time.Second, "room_closed"},
		{"closer, after 24 h", "stranger", func(*room.Service) room.User { return stranger }, 25 * time.Hour, "room_closed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc, st, clk := newTestService(t)
			var opener room.User
			if c.closer == "stranger" {
				opener = stranger
			} else {
				opener = mustUser(t, svc, c.closer)
			}
			snap, err := svc.CreateRoom(context.Background(), opener, "Incident", "sev2", "orders")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := svc.CloseRoom(context.Background(), opener, snap.ID, "fixed by hand"); err != nil {
				t.Fatalf("close: %v", err)
			}
			clk.Advance(c.after)
			before := countType(t, st, snap.ID, events.CommentAdded)

			ev, err := svc.AddCommentKind(context.Background(), c.author(svc), snap.ID, "", "restarted order-sync by hand", events.CommentKindFixNote)
			if c.wantCode != "" {
				wantCode(t, err, c.wantCode)
				if n := countType(t, st, snap.ID, events.CommentAdded); n != before {
					t.Fatal("a refused fix note was written")
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if ev.Type != events.CommentAdded {
				t.Fatalf("event = %s", ev.Type)
			}
			p := lastComment(t, st, snap.ID)
			if p.Kind != events.CommentKindFixNote {
				t.Fatalf("kind = %q", p.Kind)
			}
		})
	}
}

// A plain comment never goes to a closed room, whoever sends it; a note to a
// closed room promises no agent answer.
func TestFixNoteClosedRoomOnlyNotes(t *testing.T) {
	svc, st, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	closeRoomAs(t, svc, roomID, "arjun")
	_, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "arjun"), roomID, "", "plain", "")
	wantCode(t, err, "room_closed")
	if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "@agent did it help?", events.CommentKindFixNote); err != nil {
		t.Fatal(err)
	}
	if p := lastComment(t, st, roomID); p.MentionsAgent {
		t.Fatalf("mentions_agent set in a closed room: %+v", p)
	}
}

// The closer is found even when the close is far back in a long log.
func TestFixNoteCloserFoundInLongLog(t *testing.T) {
	svc, st, _ := newTestService(t)
	stranger := room.User{ID: "stranger", Name: "Stranger"}
	snap, err := svc.CreateRoom(context.Background(), stranger, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(context.Background(), stranger, snap.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	// More events after the close than the first lookup reads.
	for i := 0; i < 450; i++ {
		if _, err := st.Append(context.Background(), snap.ID, events.AgentUsage, events.AgentActor, events.AgentUsagePayload{Model: "m", Role: "main"}); err != nil {
			t.Fatal(err)
		}
	}
	// The service folds events another writer appended on its next load; a
	// fresh service over the same store sees the long log.
	fresh := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite, Now: func() time.Time { return time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC) }})
	defer fresh.Close()
	if err := fresh.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.AddCommentKind(context.Background(), stranger, snap.ID, "", "restarted by hand", events.CommentKindFixNote); err != nil {
		t.Fatalf("the closer was not found in a long log: %v", err)
	}
	_, err = fresh.AddCommentKind(context.Background(), room.User{ID: "other", Name: "Other"}, snap.ID, "", "me too", events.CommentKindFixNote)
	wantCode(t, err, "not_allowed")
}

// A store failure while looking for the closer is a plain error (500), not a
// verdict on the person.
func TestFixNoteCloserLookupStoreFailure(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	mem := memstore.NewWithClock(c.Now)
	fs := &failingStore{Store: mem}
	svc := room.New(room.Deps{Store: fs, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite, Now: c.Now})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID
	closeRoomAs(t, svc, roomID, "arjun")

	fs.failSince.Store(true)
	_, err := svc.AddCommentKind(context.Background(), room.User{ID: "other", Name: "Other"}, roomID, "", "x", events.CommentKindFixNote)
	var re *room.Error
	if err == nil || errors.As(err, &re) {
		t.Fatalf("err = %v; want a plain error", err)
	}
	// A member needs no lookup.
	if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "x", events.CommentKindFixNote); err != nil {
		t.Fatalf("member refused while the log is unreadable: %v", err)
	}
}

// A fix note takes an idempotency key like a plain comment (docs/api.md,
// Idempotent posts): the same key and body answer the stored event and append
// nothing; the same key with another kind is another body, 409
// client_id_reused.
func TestFixNoteClientID(t *testing.T) {
	ctx := context.Background()
	svc, st, _ := newTestService(t)
	kabir := mustUser(t, svc, "kabir")
	roomID := createRoom(t, svc, "kabir").ID

	first, err := svc.AddCommentKindKeyed(ctx, kabir, roomID, "", "restarted by hand", events.CommentKindFixNote, "k1")
	if err != nil {
		t.Fatalf("fix note: %v", err)
	}
	if p := lastComment(t, st, roomID); p.Kind != events.CommentKindFixNote || p.ClientID != "k1" {
		t.Fatalf("stored comment = %+v, want a fix note with client_id k1", p)
	}
	before := lastSeq(t, st, roomID)
	again, err := svc.AddCommentKindKeyed(ctx, kabir, roomID, "", "restarted by hand", events.CommentKindFixNote, "k1")
	if err != nil || again.Seq != first.Seq {
		t.Fatalf("repeat = seq %d, %v; want the stored seq %d", again.Seq, err, first.Seq)
	}
	if _, err := svc.AddCommentKindKeyed(ctx, kabir, roomID, "", "restarted by hand", "", "k1"); !isCode(err, "client_id_reused") {
		t.Fatalf("same key as a plain comment: err = %v, want 409 client_id_reused", err)
	}
	if after := lastSeq(t, st, roomID); after != before {
		t.Fatalf("last_seq %d -> %d, want no write", before, after)
	}
}
