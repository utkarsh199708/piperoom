package room_test

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"piperoom/internal/alertin"
	"piperoom/internal/room"
)

func TestTextCaps(t *testing.T) {
	ctx := context.Background()
	svc, st, _ := newTestService(t)
	priya := mustUser(t, svc, "priya")
	snap := createRoom(t, svc, "priya")

	// Multi-byte characters: the cap counts what the person sees, not bytes.
	atTitle := strings.Repeat("é", room.MaxTitleChars)
	atText := strings.Repeat("é", room.MaxMessageChars)
	over := func(s string) string { return s + "x" }

	cases := []struct {
		name string
		call func(text string) error
		at   string
		want string // part of the 400 message
	}{
		{"title", func(s string) error {
			_, err := svc.CreateRoom(ctx, priya, s, "sev2", "orders")
			return err
		}, atTitle, "title is too long"},
		{"comment", func(s string) error {
			_, err := svc.AddComment(ctx, priya, snap.ID, "", s)
			return err
		}, atText, "text is too long"},
		{"steer", func(s string) error {
			_, err := svc.Steer(ctx, priya, snap.ID, s)
			return err
		}, atText, "text is too long"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.call(c.at); err != nil {
				t.Fatalf("at the limit: %v", err)
			}
			before := len(allEvents(t, st, snap.ID))
			err := c.call(over(c.at))
			wantCode(t, err, "validation")
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("message %q does not say %q", err.Error(), c.want)
			}
			if strings.Contains(err.Error(), "é") {
				t.Fatalf("message quotes the text: %q", err.Error())
			}
			if after := len(allEvents(t, st, snap.ID)); after != before {
				t.Fatalf("a refused request wrote %d events", after-before)
			}
		})
	}
}

func TestAlertTitleIsClippedNotRefused(t *testing.T) {
	svc, _, _ := newTestService(t)
	long := strings.Repeat("ü", room.MaxTitleChars+50)
	snap, created, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: long,
		Severity: "sev2", PipelineID: "orders", Raw: []byte(`{}`),
	})
	if err != nil || !created {
		t.Fatalf("open alert room: created=%v err=%v", created, err)
	}
	if n := utf8.RuneCountInString(snap.Title); n != room.MaxTitleChars {
		t.Fatalf("title has %d characters, want %d", n, room.MaxTitleChars)
	}
	if !strings.HasSuffix(snap.Title, "…") || !utf8.ValidString(snap.Title) {
		t.Fatalf("clipped title %q", snap.Title)
	}
}
