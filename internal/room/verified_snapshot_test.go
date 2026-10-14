package room_test

import (
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

func mkEvent(t *testing.T, seq int64, typ events.Type, actor events.Actor, payload any) events.Event {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return events.Event{RoomID: "r_1", Seq: seq, Type: typ, Actor: actor, Payload: b, At: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
}

func member(t *testing.T, snap room.RoomSnapshot, id string) room.Member {
	t.Helper()
	for _, m := range snap.Members {
		if m.ID == id {
			return m
		}
	}
	t.Fatalf("no member %s in %+v", id, snap.Members)
	return room.Member{}
}

// A room open across the upgrade (design section 2.1): priya's events from
// before the verified field read as unverified. Her next event marks her
// member entry verified without touching her driver role, and the next grant
// marks the driver verified. The flag follows the latest event either way.
func TestSnapshotVerifiedFollowsTheLatestEvent(t *testing.T) {
	old := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"} // written before the field
	now := old
	now.Verified = true
	lease := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

	snap := room.NewSnapshot()
	snap = room.Apply(snap, mkEvent(t, 1, events.RoomCreated, old, events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders"}))
	snap = room.Apply(snap, mkEvent(t, 2, events.ControlGranted, old, events.ControlGrantedPayload{To: old, LeaseUntil: lease}))
	if m := member(t, snap, "priya"); m.Verified || m.Role != "driver" {
		t.Fatalf("before: member %+v, want unverified driver", m)
	}
	if snap.Driver == nil || snap.Driver.Verified {
		t.Fatalf("before: driver %+v, want unverified", snap.Driver)
	}

	snap = room.Apply(snap, mkEvent(t, 3, events.CommentAdded, now, events.CommentAddedPayload{CommentID: "c_1", Text: "back"}))
	if m := member(t, snap, "priya"); !m.Verified || m.Role != "driver" {
		t.Fatalf("after her comment: member %+v, want verified, still driver", m)
	}
	snap = room.Apply(snap, mkEvent(t, 4, events.ControlGranted, now, events.ControlGrantedPayload{To: now, LeaseUntil: lease}))
	if snap.Driver == nil || !snap.Driver.Verified {
		t.Fatalf("after the grant: driver %+v, want verified", snap.Driver)
	}

	// Removed from users.yaml later: her next event is unverified again.
	snap = room.Apply(snap, mkEvent(t, 5, events.CommentAdded, old, events.CommentAddedPayload{CommentID: "c_2", Text: "x"}))
	if m := member(t, snap, "priya"); m.Verified {
		t.Fatalf("after an unverified event: member %+v, want unverified", m)
	}
}
