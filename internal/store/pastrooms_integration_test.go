//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

func pastRoomsFixture() []fake.Room {
	at := func(d int) time.Time { return time.Date(2026, 9, d, 10, 0, 0, 0, time.UTC) }
	return []fake.Room{
		{ID: "pr-old", Title: "orders stale", ClosedAt: at(1), Resolution: "restarted task 0", Layer: "kafka-connect", LabelTerms: []string{"avro"},
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: "org.apache.kafka.connect.errors.ConnectException: boom\nCaused by: org.postgresql.util.PSQLException: ERROR: must be owner of table orders"}}},
		{ID: "pr-new", Title: "orders stale again", ClosedAt: at(9), Resolution: "set compatibility NONE", Handoff: &events.HandoffSummary{WhatBroke: "schema", Open: []string{"migration policy"}}},
		{ID: "pr-pay", PipelineID: "payments", Title: "payments stale", ClosedAt: at(5), Resolution: "other pipeline"},
	}
}

// TestClosedRoomReaderMatchesMemstore runs the same history through Postgres
// and memstore: same closed rooms in the same order, same event selection with
// tool-result data dropped, and the same tool answer on top.
func TestClosedRoomReaderMatchesMemstore(t *testing.T) {
	pg, _ := requireStore(t)
	mem := memstore.New()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, r := range pastRoomsFixture() {
		for _, st := range []store.Store{pg, mem} {
			if err := fake.Seed(ctx, st, r); err != nil {
				t.Fatalf("seed %s: %v", r.ID, err)
			}
		}
	}
	// An open room must never be listed.
	if err := pg.CreateRoom(ctx, store.Room{ID: "pr-open", Title: "open", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		pipeline string
		limit    int
		want     []string
	}{
		{"orders", 10, []string{"pr-new", "pr-old"}},
		{"orders", 1, []string{"pr-new"}},
		{"", 10, []string{"pr-new", "pr-pay", "pr-old"}},
		{"nope", 10, nil},
		{"orders", 0, nil},
	} {
		for name, r := range map[string]store.ClosedRoomReader{"pg": pg, "mem": mem} {
			got, err := r.ListClosedRooms(ctx, tc.pipeline, tc.limit)
			if err != nil {
				t.Fatalf("%s list(%q,%d): %v", name, tc.pipeline, tc.limit, err)
			}
			var ids []string
			for _, room := range got {
				ids = append(ids, room.ID)
				if room.Status != "closed" || room.ClosedAt == nil || room.ClosedAt.Location() != time.UTC {
					t.Errorf("%s: room %+v is not a UTC closed room", name, room)
				}
			}
			if !slices.Equal(ids, tc.want) {
				t.Errorf("%s list(%q,%d) = %v, want %v", name, tc.pipeline, tc.limit, ids, tc.want)
			}
		}
	}

	types := []events.Type{events.RoomClosed, events.AgentToolResult}
	for name, r := range map[string]store.ClosedRoomReader{"pg": pg, "mem": mem} {
		evs, err := r.EventsOfTypes(ctx, "pr-old", types, 10)
		if err != nil {
			t.Fatalf("%s events: %v", name, err)
		}
		if len(evs) != 2 || evs[0].Type != events.AgentToolResult || evs[1].Type != events.RoomClosed || evs[0].Seq >= evs[1].Seq {
			t.Fatalf("%s: events = %+v, want tool result then room.closed in seq order", name, evs)
		}
		if strings.Contains(string(evs[0].Payload), "bulky") || strings.Contains(string(evs[0].Payload), `"data"`) {
			t.Errorf("%s: tool result data was not dropped: %s", name, evs[0].Payload)
		}
		res, err := events.Decode[events.AgentToolResultPayload](evs[0])
		if err != nil || res.Tool != "connect_status" || !strings.Contains(res.Summary, "must be owner") {
			t.Errorf("%s: tool result lost its summary: %+v, %v", name, res, err)
		}
		if one, err := r.EventsOfTypes(ctx, "pr-old", types, 1); err != nil || len(one) != 1 {
			t.Errorf("%s: limit 1: %d events, %v", name, len(one), err)
		}
		if none, err := r.EventsOfTypes(ctx, "pr-old", nil, 10); err != nil || len(none) != 0 {
			t.Errorf("%s: no types: %d events, %v", name, len(none), err)
		}
	}
	// A cancelled context is an error on Postgres.
	cctx, ccancel := context.WithCancel(ctx)
	ccancel()
	if _, err := pg.ListClosedRooms(cctx, "orders", 5); err == nil {
		t.Error("cancelled ctx: ListClosedRooms succeeded")
	}
	if _, err := pg.EventsOfTypes(cctx, "pr-old", types, 5); err == nil {
		t.Error("cancelled ctx: EventsOfTypes succeeded")
	}

	// The tool gives the same answer over both stores.
	tctx := tools.WithPipeline(ctx, owners.Pipeline{ID: "orders"})
	in := json.RawMessage(`{"query":"orders stale, must be owner of table orders","limit":3}`)
	var answers []string
	for _, st := range []store.Store{pg, mem} {
		res, err := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)}).Run(tctx, in)
		if err != nil {
			t.Fatalf("tool: %v", err)
		}
		answers = append(answers, res.Summary)
	}
	if answers[0] != answers[1] || !strings.Contains(answers[0], "pr-old") || !strings.Contains(answers[0], "pr-new") || strings.Contains(answers[0], "pr-pay") {
		t.Fatalf("tool answers differ or are wrong:\npg:\n%s\nmem:\n%s", answers[0], answers[1])
	}
}
