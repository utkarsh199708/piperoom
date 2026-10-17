package room_test

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/alertin"
	"piperoom/internal/room"
)

// TestTypedTextRefusesBidi (Q5 T8): a person's text with a bidi embedding,
// override or isolate character is refused with a 400 before any write, like
// a NUL; the message names the characters, never the text.
func TestTypedTextRefusesBidi(t *testing.T) {
	ctx := context.Background()
	svc, st, _ := newTestService(t)
	arjun := mustUser(t, svc, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	for _, bad := range []string{"orders ‮missing lag", "a⁦b⁩"} {
		before := lastSeq(t, st, roomID)
		calls := map[string]func() error{
			"comment": func() error { _, err := svc.AddComment(ctx, arjun, roomID, "", bad); return err },
			"steer":   func() error { _, err := svc.Steer(ctx, arjun, roomID, bad); return err },
			"title":   func() error { _, err := svc.CreateRoom(ctx, arjun, bad, "sev2", "orders"); return err },
		}
		for name, call := range calls {
			err := call()
			var re *room.Error
			if !errors.As(err, &re) || re.Status != http.StatusBadRequest || re.Code != "validation" {
				t.Fatalf("%s %q: err = %v, want 400 validation", name, bad, err)
			}
			if !strings.Contains(re.Msg, "U+202E") || strings.Contains(re.Msg, "orders") {
				t.Fatalf("%s: message %q must name the characters and not quote the text", name, re.Msg)
			}
		}
		if got := lastSeq(t, st, roomID); got != before {
			t.Fatalf("last_seq moved %d -> %d", before, got)
		}
	}
	// A plain direction mark is ordinary text.
	if _, err := svc.AddComment(ctx, arjun, roomID, "", "rtl ‏ mark"); err != nil {
		t.Fatalf("comment with U+200F: %v", err)
	}
}

// TestAlertTitleBidiFlagged (Q5 T8): an alert's title comes from a machine; it
// is not refused (the alert would be lost) but flagged for every viewer.
func TestAlertTitleBidiFlagged(t *testing.T) {
	svc, _, _ := newTestService(t)
	snap, _, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "orders ‮missing lag", Severity: "sev2", PipelineID: "orders",
	})
	if err != nil {
		t.Fatalf("open alert room: %v", err)
	}
	if !slices.Contains(snap.TitleFlags, "bidi_override") {
		t.Fatalf("title_flags = %v, want bidi_override", snap.TitleFlags)
	}
}
