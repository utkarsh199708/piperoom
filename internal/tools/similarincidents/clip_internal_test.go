package similarincidents

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/tools/similarincidents/fake"
)

// TestLoadRoomClipsEveryFieldToItsCap: the cache holds rooms for rooms x
// pipelines, so what a room contributes is bounded when it is read.
func TestLoadRoomClipsEveryFieldToItsCap(t *testing.T) {
	ctx := context.Background()
	huge := strings.Repeat("x", 1<<20)
	many := make([]string, 50)
	for i := range many {
		many[i] = huge
	}
	var results []fake.ToolResult
	for i := range 30 {
		results = append(results, fake.ToolResult{Tool: "connect_status",
			Summary: "org.a.b.FooException: m" + strings.Repeat("y", i) + "\nCaused by: org.a.b.Bar" + strings.Repeat("z", i) + "Exception: " + huge[:3000]})
	}
	st, err := fake.NewStore(ctx, fake.Room{ID: "r-1", Title: huge, ClosedAt: time.Now().Add(-time.Hour), Resolution: huge,
		Handoff:     &events.HandoffSummary{WhatBroke: huge, Why: huge, Open: many, Risks: many},
		ToolResults: results})
	if err != nil {
		t.Fatal(err)
	}
	rooms, err := st.ListClosedRooms(ctx, "orders", 5)
	if err != nil || len(rooms) != 1 {
		t.Fatalf("rooms = %v, %v", rooms, err)
	}
	pr, err := loadRoom(ctx, FromStore(st), rooms[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	total := len(pr.Title) + len(pr.Resolution) + len(pr.WhatBroke) + len(pr.Why) + len(pr.traceText)
	for _, x := range append(append([]string{}, pr.Open...), pr.Risks...) {
		total += len(x)
	}
	if len(pr.Title) > maxTitleBytes+4 || len(pr.Resolution) > maxFieldBytes || len(pr.WhatBroke) > maxSummaryField ||
		len(pr.Why) > maxSummaryField || len(pr.Open) > maxListItems || len(pr.Risks) > maxListItems ||
		len(pr.traceText) > maxTraceTextKeep || len(pr.Traces) > maxFingerprints {
		t.Fatalf("a field is over its cap: title %d resolution %d what %d why %d open %d risks %d trace %d fps %d",
			len(pr.Title), len(pr.Resolution), len(pr.WhatBroke), len(pr.Why), len(pr.Open), len(pr.Risks), len(pr.traceText), len(pr.Traces))
	}
	if total > 16<<10 {
		t.Errorf("one room keeps %d bytes", total)
	}
}
