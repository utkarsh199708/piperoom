package memstore

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

func TestClosedRoomReader(t *testing.T) {
	ctx := context.Background()
	s := New()
	at := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	mk := func(id, pipeline string, closed *time.Time) {
		t.Helper()
		if err := s.CreateRoom(ctx, store.Room{ID: id, Title: id, Severity: "sev2", PipelineID: pipeline, Status: "open", OpenedAt: at}); err != nil {
			t.Fatal(err)
		}
		if closed != nil {
			if err := s.CloseRoom(ctx, id, *closed); err != nil {
				t.Fatal(err)
			}
		}
	}
	d1, d2 := at.Add(time.Hour), at.Add(2*time.Hour)
	mk("a", "orders", &d1)
	mk("b", "orders", &d2)
	mk("c", "orders", &d2) // same close time as b: id order
	mk("d", "orders", nil) // open
	mk("e", "payments", &d1)

	ids := func(p string, n int) []string {
		t.Helper()
		rs, err := s.ListClosedRooms(ctx, p, n)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range rs {
			out = append(out, r.ID)
		}
		return out
	}
	if got := ids("orders", 10); !slices.Equal(got, []string{"b", "c", "a"}) {
		t.Errorf("orders = %v", got)
	}
	if got := ids("orders", 2); !slices.Equal(got, []string{"b", "c"}) {
		t.Errorf("limit 2 = %v", got)
	}
	if got := ids("", 10); !slices.Equal(got, []string{"b", "c", "a", "e"}) {
		t.Errorf("all = %v", got)
	}
	if got := ids("orders", 0); len(got) != 0 {
		t.Errorf("limit 0 = %v", got)
	}

	app := func(typ events.Type, p any) {
		t.Helper()
		if _, err := s.Append(ctx, "a", typ, events.AgentActor, p); err != nil {
			t.Fatal(err)
		}
	}
	app(events.AgentStatus, events.AgentStatusPayload{State: "working"})
	app(events.AgentToolResult, events.AgentToolResultPayload{Tool: "x", Summary: "sum", Data: []byte(`{"big":1}`)})
	app(events.RoomClosed, events.RoomClosedPayload{Resolution: "done"})

	evs, err := s.EventsOfTypes(ctx, "a", []events.Type{events.RoomClosed, events.AgentToolResult}, 10)
	if err != nil || len(evs) != 2 || evs[0].Type != events.AgentToolResult {
		t.Fatalf("events = %+v, %v", evs, err)
	}
	if strings.Contains(string(evs[0].Payload), "big") {
		t.Errorf("data not dropped: %s", evs[0].Payload)
	}
	// The stored log still has the data.
	all, _ := s.Since(ctx, "a", 0, 10)
	if !strings.Contains(string(all[1].Payload), "big") {
		t.Errorf("EventsOfTypes changed the stored event: %s", all[1].Payload)
	}
	// Over the limit the newest events are kept, in seq order.
	for i := range 5 {
		app(events.AgentToolResult, events.AgentToolResultPayload{Tool: "x", Summary: string(rune('a' + i))})
	}
	last, err := s.EventsOfTypes(ctx, "a", []events.Type{events.AgentToolResult}, 2)
	if err != nil || len(last) != 2 || last[0].Seq >= last[1].Seq || last[1].Seq != int64(len(all)+5) {
		t.Errorf("newest 2 = %+v, %v", last, err)
	}
	// A room deleted since it was listed has no events, as in Postgres.
	if got, err := s.EventsOfTypes(ctx, "zzz", []events.Type{events.RoomClosed}, 1); err != nil || len(got) != 0 {
		t.Errorf("missing room: %v, %v; want no events and no error", got, err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ListClosedRooms(cctx, "", 1); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled list: err = %v", err)
	}
	if _, err := s.EventsOfTypes(cctx, "a", nil, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled events: err = %v", err)
	}
}
