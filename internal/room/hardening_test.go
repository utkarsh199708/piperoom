package room_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/topology"
)

// badStagesTopo returns trace stages whose text carries NUL bytes, as an
// outside system (a Connect task trace, a warehouse row) could.
type badStagesTopo struct{ fakeTopo }

func (badStagesTopo) Trace(_ context.Context, _, table, _ string) ([]events.TraceStage, error) {
	return []events.TraceStage{{
		Layer: "postgres", Name: table + "\x00", Status: "seen\x00",
		Facts: map[string]string{"lsn": "0/16\x00B3", "bad\x00key": "v"},
	}}, nil
}

var _ topology.Builder = badStagesTopo{}

// Trace stage facts come from outside systems and are not the person's text. A
// NUL in them becomes U+FFFD (the agent-text policy); the pin is stored, not
// refused with a 400 that blames the person.
func TestTraceStageFactsAreSanitized(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, Now: c.Now,
		Topology: badStagesTopo{},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	ev, err := svc.Trace(ctx, mustUser(t, svc, "arjun"), roomID, "public.orders", "1")
	if err != nil {
		t.Fatalf("trace with NUL in stage text was refused: %v", err)
	}
	p, err := events.Decode[events.TracePinnedPayload](ev)
	if err != nil || len(p.Stages) != 1 {
		t.Fatalf("payload = %+v, %v", p, err)
	}
	s := p.Stages[0]
	if s.Name != "public.orders�" || s.Status != "seen�" || s.Facts["lsn"] != "0/16�B3" || s.Facts["bad�key"] != "v" {
		t.Fatalf("stage = %+v, want U+FFFD in place of each NUL", s)
	}
	for _, e := range allEvents(t, st, roomID) {
		if err := store.CheckJSON(e.Payload); err != nil {
			t.Fatalf("event %d (%s) is stored with unstorable content: %v", e.Seq, e.Type, err)
		}
	}
	// The person's own key is still refused, not rewritten.
	if _, err := svc.Trace(ctx, mustUser(t, svc, "arjun"), roomID, "public.orders", "k\x00"); err == nil {
		t.Fatal("a NUL in the person's key must still be refused")
	}
}
