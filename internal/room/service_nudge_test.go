package room_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

func TestNudge(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, clk := newTestService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	priya := mustUser(t, svc, "priya")

	// Validation and unknown targets.
	_, err = svc.Nudge(ctx, priya, roomID, "", "postgres")
	wantCode(t, err, "validation")
	_, err = svc.Nudge(ctx, priya, roomID, pid, "nope")
	wantCode(t, err, "validation")
	_, err = svc.Nudge(ctx, priya, roomID, pid, "kafka") // not required by this proposal
	wantCode(t, err, "validation")
	_, err = svc.Nudge(ctx, priya, roomID, "p_missing", "postgres")
	wantCode(t, err, "already_decided")
	_, err = svc.Nudge(ctx, priya, "r_missing", pid, "postgres")
	wantCode(t, err, "not_found")

	ev, err := svc.Nudge(ctx, priya, roomID, pid, "postgres")
	if err != nil {
		t.Fatalf("nudge: %v", err)
	}
	if ev.Type != events.ApprovalNudged || ev.Actor.ID != "priya" {
		t.Fatalf("bad event: %+v", ev)
	}
	// Ids only: no title or free text in the payload.
	if got := string(ev.Payload); !strings.Contains(got, pid) || strings.Contains(got, "widen") {
		t.Fatalf("payload should carry ids only: %s", got)
	}

	// 10-minute window per (proposal, layer), measured with the fake clock.
	clk.Advance(4 * time.Minute)
	_, err = svc.Nudge(ctx, mustUser(t, svc, "kabir"), roomID, pid, "postgres")
	wantCode(t, err, "already_nudged")
	if !strings.Contains(err.Error(), "nudged 4 min ago") {
		t.Fatalf("message: %v", err)
	}
	clk.Advance(6 * time.Minute)
	if _, err = svc.Nudge(ctx, priya, roomID, pid, "postgres"); err != nil {
		t.Fatalf("after the window: %v", err)
	}

	// A decided layer cannot be nudged.
	if _, err = svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	clk.Advance(time.Hour)
	_, err = svc.Nudge(ctx, priya, roomID, pid, "postgres")
	wantCode(t, err, "already_decided")
}

func TestNudgeClosedRoom(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), roomID, "resolved"); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Nudge(ctx, mustUser(t, svc, "priya"), roomID, pid, "postgres")
	wantCode(t, err, "room_closed")
}
