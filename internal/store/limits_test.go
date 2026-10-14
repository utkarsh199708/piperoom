package store

import (
	"testing"

	"piperoom/internal/events"
)

func TestReserve(t *testing.T) {
	for _, tc := range []struct{ n, want int64 }{
		{0, 0}, {1, 1}, {49, 1}, {50, 1}, {51, 2}, {100, 2}, {101, 3}, {20000, 400}, {256 << 20, 5368710},
	} {
		if got := reserve(tc.n); got != tc.want {
			t.Errorf("reserve(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

// TestFitsByClass pins the three classes at the edges: agent events stop at
// the cap, human and system events at cap + 2%, room.closed never.
func TestFitsByClass(t *testing.T) {
	human := events.Actor{Kind: events.ActorHuman, ID: "arjun"}
	system := events.Actor{Kind: events.ActorSystem, ID: "system"}
	l := Limits{MaxEvents: 100, MaxBytes: 10000}
	tests := []struct {
		name          string
		typ           events.Type
		actor         events.Actor
		events, bytes int64
		size          int64
		want          bool
	}{
		{"agent below the event cap", events.AgentToolResult, events.AgentActor, 98, 0, 10, true},
		{"agent reaches the event cap", events.AgentToolResult, events.AgentActor, 99, 0, 10, true},
		{"agent past the event cap", events.AgentToolResult, events.AgentActor, 100, 0, 10, false},
		{"agent byte cap exactly", events.AgentMessage, events.AgentActor, 0, 9990, 10, true},
		{"agent byte cap passed by one", events.AgentMessage, events.AgentActor, 0, 9991, 10, false},
		{"human inside the reserve", events.CommentAdded, human, 101, 0, 10, true},
		{"human at the reserve edge", events.CommentAdded, human, 101, 0, 10, true},
		{"human past the reserve", events.CommentAdded, human, 102, 0, 10, false},
		{"system past the byte reserve", events.AgentMessage, system, 0, 10200, 1, false},
		{"system inside the byte reserve", events.AgentMessage, system, 0, 10199, 1, true},
		{"agent status by the system uses the reserve", events.AgentStatus, system, 100, 0, 10, true},
		{"room.closed past everything", events.RoomClosed, human, 1 << 40, 1 << 40, 10, true},
		{"room.closed by the system", events.RoomClosed, system, 1 << 40, 1 << 40, 10, true},
	}
	for _, tc := range tests {
		if got := l.Fits(tc.typ, tc.actor, tc.events, tc.bytes, tc.size); got != tc.want {
			t.Errorf("%s: Fits = %v, want %v", tc.name, got, tc.want)
		}
	}
	// Zero limits are no cap.
	if !(Limits{}).Fits(events.AgentToolResult, events.AgentActor, 1<<40, 1<<40, 1<<20) {
		t.Error("zero limits must not cap")
	}
}

func TestUsageThresholds(t *testing.T) {
	tests := []struct {
		u           Usage
		warn, full  bool
		description string
	}{
		{Usage{Events: 89, MaxEvents: 100}, false, false, "89% events"},
		{Usage{Events: 90, MaxEvents: 100}, true, false, "90% events"},
		{Usage{Events: 100, MaxEvents: 100}, true, true, "100% events"},
		{Usage{Bytes: 899, MaxBytes: 1000}, false, false, "89.9% bytes"},
		{Usage{Bytes: 900, MaxBytes: 1000}, true, false, "90% bytes"},
		{Usage{Bytes: 1000, MaxBytes: 1000}, true, true, "100% bytes"},
		{Usage{Events: 1 << 40, Bytes: 1 << 40}, false, false, "no caps"},
	}
	for _, tc := range tests {
		if got := tc.u.AtWarning(); got != tc.warn {
			t.Errorf("%s: AtWarning = %v, want %v", tc.description, got, tc.warn)
		}
		if got := tc.u.Full(); got != tc.full {
			t.Errorf("%s: Full = %v, want %v", tc.description, got, tc.full)
		}
	}
}
