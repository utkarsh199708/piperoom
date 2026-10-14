package room

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/agent"
)

func TestQuietInputDoesNotWake(t *testing.T) {
	in := newInbox()
	in.pushQuiet(agent.HumanInput{Kind: "alert_update", Text: "old"})
	in.pushQuiet(agent.HumanInput{Kind: "alert_update", Text: "new"})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := in.Wait(ctx); err == nil {
		t.Fatal("Wait returned for a quiet input")
	}
	in.push(agent.HumanInput{Kind: "steer", Text: "go"})
	if err := in.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after a steer: %v", err)
	}
	got, _ := in.Drain(context.Background())
	if len(got) != 2 || got[0].Text != "new" || got[1].Kind != "steer" {
		t.Fatalf("drained = %+v; want the newest update first, then the steer", got)
	}
	if got, _ := in.Drain(context.Background()); len(got) != 0 {
		t.Fatalf("second drain = %+v", got)
	}
	closed := newClosedInbox()
	closed.pushQuiet(agent.HumanInput{Kind: "alert_update"})
	if got, _ := closed.Drain(context.Background()); len(got) != 0 {
		t.Fatal("a shadow inbox took an alert update")
	}
}
