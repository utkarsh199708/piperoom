package similarincidents_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

// TestRootFingerprintThroughTheTool: the room's trace ends in a "Detail:" line
// and a stack frame. A query quoting only the PSQLException line must match by
// root fingerprint. With a "last cause" rule the room's root would be the
// Detail line and there would be no match.
func TestRootFingerprintThroughTheTool(t *testing.T) {
	trace := "org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\"\n" +
		"  Detail: Key (email)=(a@b.c) already exists.\n" +
		"\tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2713)"
	st := seeded(t, fake.Room{ID: "r-1", Title: "load failing", ClosedAt: day(9, 1), Resolution: "stopped duplicate writer",
		ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: trace}}})
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
	res, err := run(t, tl, scoped("orders"), input("Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\"", 3))
	if err != nil {
		t.Fatal(err)
	}
	d := res.Data.(similarincidents.Data)
	if len(d.Hits) != 1 || d.Hits[0].Fingerprint != "root" {
		t.Fatalf("hits = %+v, want one hit with fingerprint_match root", d.Hits)
	}
}

// TestOwnResultsAreNotIndexed: a similar_incidents result quotes other rooms.
// Indexing it would make this room "similar" to the rooms it merely searched.
func TestOwnResultsAreNotIndexed(t *testing.T) {
	quoted := "2 similar past incident(s) zzquotedword\norg.a.b.FooException: outer\nCaused by: org.a.b.BarException: quoted root"
	st := seeded(t, fake.Room{ID: "r-1", Title: "other", ClosedAt: day(9, 1), Resolution: "nothing relevant",
		ToolResults: []fake.ToolResult{{Tool: similarincidents.ToolName, Summary: quoted}}})
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
	for _, q := range []string{"zzquotedword", "org.a.b.FooException: outer\nCaused by: org.a.b.BarException: quoted root", "Caused by: org.a.b.BarException: quoted root"} {
		res, err := run(t, tl, scoped("orders"), input(q, 3))
		if err != nil {
			t.Fatal(err)
		}
		if ids := hitIDs(t, res); len(ids) != 0 {
			t.Errorf("query %q matched %v through a similar_incidents result", q, ids)
		}
	}
	// The same text from another tool is indexed, so the test can tell.
	st = seeded(t, fake.Room{ID: "r-2", Title: "other", ClosedAt: day(9, 1), Resolution: "nothing relevant",
		ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: quoted}}})
	tl = similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
	res, err := run(t, tl, scoped("orders"), input("Caused by: org.a.b.BarException: quoted root", 3))
	if err != nil {
		t.Fatal(err)
	}
	if ids := hitIDs(t, res); !slices.Equal(ids, []string{"r-2"}) {
		t.Errorf("control: a connect_status result was not indexed: %v", ids)
	}
}

// TestFingerprintHitsAreExemptFromTheScoreFloor: a very long matching query
// makes the top BM25 score large, so a root-fingerprint hit (+15) can fall
// below 15% of it. It must stay.
func TestFingerprintHitsAreExemptFromTheScoreFloor(t *testing.T) {
	var words []string
	for i := range 64 {
		words = append(words, fmt.Sprintf("tok%02d", i))
	}
	text := strings.Join(words, " ")
	rooms := []fake.Room{
		{ID: "r-top", Title: text, ClosedAt: day(9, 2), Resolution: text + " " + text},
		{ID: "r-fp", Title: "unrelated", ClosedAt: day(9, 1), Resolution: "nothing",
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: "org.a.b.FooException: outer\nCaused by: org.a.b.BarException: floor root"}}},
	}
	for i := range 30 { // more rooms make the terms rare, so the top score is high
		rooms = append(rooms, fake.Room{ID: fmt.Sprintf("r-filler-%02d", i), Title: "filler", ClosedAt: day(8, 1+i%28), Resolution: "filler text"})
	}
	st := seeded(t, rooms...)
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
	res, err := run(t, tl, scoped("orders"), input(text+"\nCaused by: org.a.b.BarException: floor root", 5))
	if err != nil {
		t.Fatal(err)
	}
	d := res.Data.(similarincidents.Data)
	if len(d.Hits) != 2 || d.Hits[0].RoomID != "r-top" || d.Hits[1].RoomID != "r-fp" || d.Hits[1].Fingerprint != "root" {
		t.Fatalf("hits = %+v, want r-top then the fingerprint hit r-fp", d.Hits)
	}
	// The test only means something if the fingerprint hit is under the floor.
	if d.Hits[1].Score >= 0.15*d.Hits[0].Score {
		t.Fatalf("precondition: fp hit %.1f is not below 15%% of the top %.1f", d.Hits[1].Score, d.Hits[0].Score)
	}
}

// TestLatestHandoffIsUsedBeyondTheReadLimit: with more hand-offs than the read
// limit (64) the newest one must still be the one indexed.
func TestLatestHandoffIsUsedBeyondTheReadLimit(t *testing.T) {
	var more []events.HandoffSummary
	for range 69 {
		more = append(more, events.HandoffSummary{WhatBroke: "zzoldhandoff"})
	}
	more = append(more, events.HandoffSummary{WhatBroke: "zzlatesthandoff"})
	st := seeded(t, fake.Room{ID: "r-1", Title: "lag", ClosedAt: day(9, 1), Resolution: "x", MoreHandoffs: more})
	for name, src := range map[string]similarincidents.Source{
		"store":    similarincidents.FromStore(st),
		"fallback": similarincidents.FromStore(plainStore{st}),
	} {
		tl := similarincidents.New(similarincidents.Deps{Source: src})
		res, err := run(t, tl, scoped("orders"), input("zzlatesthandoff", 3))
		if err != nil {
			t.Fatal(err)
		}
		if ids := hitIDs(t, res); !slices.Equal(ids, []string{"r-1"}) {
			t.Errorf("%s: latest hand-off not indexed: %v", name, ids)
		}
		res, _ = run(t, tl, scoped("orders"), input("zzoldhandoff", 3))
		if ids := hitIDs(t, res); len(ids) != 0 {
			t.Errorf("%s: an older hand-off was indexed: %v", name, ids)
		}
	}
}
