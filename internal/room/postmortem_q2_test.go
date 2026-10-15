package room_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// QA F11: the open-room limit's close carries a resolution, but nobody fixed
// anything. The draft must not claim a fix, and the hand-off's open items and
// risks stay current.
func TestPostmortemSystemCloseIsNotAFix(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	handoffLog(t, st, c, r.ID, "h_1", staleSummary())
	app(t, st, c, r.ID, events.RoomClosed, pmSystem, events.RoomClosedPayload{Resolution: "retention: open longer than 30 days"})
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})

	md := renderRoom(t, svc, r.ID)
	for _, bad := range []string{"fix was made outside Piperoom", "before the fix", "none recorded after the fix"} {
		if strings.Contains(md, bad) {
			t.Errorf("a system close reads as a fix (%q):\n%s", bad, md)
		}
	}
	if !strings.HasPrefix(section(t, md, "## Still open"), "- Restart the consumer") {
		t.Errorf("open items of the hand-off are not current:\n%s", md)
	}
	if !strings.HasPrefix(section(t, md, "## Risks"), "- lag keeps growing") {
		t.Errorf("risks of the hand-off are not current:\n%s", md)
	}
	if !strings.Contains(section(t, md, "## Verification"), "No verification recorded.") {
		t.Errorf("verification:\n%s", section(t, md, "## Verification"))
	}
}

// QA F11: a fix note is fix evidence whoever closed the room.
func TestPostmortemSystemCloseAfterFixNote(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	investigation(t, st, c, r.ID)
	handoffLog(t, st, c, r.ID, "h_1", staleSummary())
	app(t, st, c, r.ID, events.CommentAdded, pmHuman("kabir", "Kabir"), events.CommentAddedPayload{
		CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "restarted order-sync",
	})
	app(t, st, c, r.ID, events.RoomClosed, pmSystem, events.RoomClosedPayload{Resolution: "retention: open longer than 30 days"})
	app(t, st, c, r.ID, events.PostmortemDrafted, pmSystem, events.PostmortemDraftedPayload{Source: "handoff", SummaryRef: "h_1"})

	md := renderRoom(t, svc, r.ID)
	for _, want := range []string{"per the fix notes.", "(before the fix: fix note at "} {
		if !strings.Contains(md, want) {
			t.Errorf("draft lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "per the fix notes and the resolution") {
		t.Errorf("a retention resolution is not fix evidence:\n%s", md)
	}
}

// QA F13: a fix note logged while the model writes the closing summary is
// newer than what the model read, so the summary's lists are labelled.
func TestPostmortemFixNoteDuringModelDraftIsStale(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	svc, st, _ := newPMService(t, func(ctx context.Context, evs []events.Event, _ agent.Options) (agent.PostmortemDraft, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return agent.PostmortemDraft{Summary: events.HandoffSummary{
			WhatBroke: "w", Why: "stuck", Open: []string{"Restart the consumer"}, Risks: []string{"lag keeps growing"},
		}}, nil
	})
	r := createRoom(t, svc, "arjun")
	closeAs(t, svc, r.ID)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the model writer was not called")
	}
	if _, err := svc.AddCommentKind(context.Background(), mustUser(t, svc, "arjun"), r.ID, "", "restarted the consumer by hand", events.CommentKindFixNote); err != nil {
		t.Fatal(err)
	}
	close(release)
	if p := drafted(t, waitPM(t, st, r.ID)); p.Source != "model" {
		t.Fatalf("source = %q, want model", p.Source)
	}
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "Earlier, from the closing summary") {
		t.Fatalf("not marked stale:\n%s", md)
	}
	if strings.HasPrefix(section(t, md, "## Still open"), "- Restart the consumer") {
		t.Fatalf("a stale open item is shown as current:\n%s", md)
	}
}
