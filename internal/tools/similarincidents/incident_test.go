package similarincidents_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

// TestOnlyIncidentsAreIndexed: a closed room is offered as history only when
// it was closed with a resolution, has a hypothesis or a human label, and has
// at least eight events (G7). Each room is searched by its title, the only
// text it shares with the query, so a skipped room cannot pass by not matching.
// The event count is read back from the store, so the boundary cases are what
// their names say.
func TestOnlyIncidentsAreIndexed(t *testing.T) {
	cases := []struct {
		name   string
		room   fake.Room
		events int  // the room's event count in the store
		want   bool // indexed
	}{
		{"QA test room is skipped",
			fake.Room{ID: "r-qa", Title: "test", ClosedAt: day(10, 1), Resolution: "Closed by the QA run, not a real incident"},
			2, false},
		{"a worked room titled test is kept",
			fake.Room{ID: "r-test", Title: "test", ClosedAt: day(10, 1), Resolution: "Restarted the connector.", Hypothesis: true, Steps: 6},
			9, true},
		{"incident with a hypothesis is kept",
			fake.Room{ID: "r-hyp", Title: "orders stalled", ClosedAt: day(10, 1), Resolution: "Restarted the connector.", Hypothesis: true, Steps: 6},
			9, true},
		{"incident with a human label is kept",
			fake.Room{ID: "r-label", Title: "orders lagging", ClosedAt: day(10, 1), Resolution: "Restarted the connector.",
				Label: &events.RoomLabelledPayload{Cause: "stopped task"}, Steps: 6},
			9, true},
		{"shadow room with its blind label is kept",
			fake.Room{ID: "r-shadow", Title: "orders shadowed", ClosedAt: day(10, 1), Resolution: "Restarted the connector.",
				Shadow: true, Layer: "kafka-connect", Steps: 5},
			8, true},
		{"resolution from an unverified closer counts; its text is not indexed",
			fake.Room{ID: "r-unverified", Title: "orders unverified", ClosedAt: day(10, 1), Resolution: "Restarted the connector.",
				UnverifiedCloser: true, Hypothesis: true, Steps: 6},
			9, true},
		{"resolution without hypothesis or label is skipped",
			fake.Room{ID: "r-bare", Title: "orders bare", ClosedAt: day(10, 1), Resolution: "Restarted the connector.", Steps: 6},
			8, false},
		{"blank resolution is skipped",
			fake.Room{ID: "r-blank", Title: "orders blank", ClosedAt: day(10, 1), Resolution: "   ", Hypothesis: true, Steps: 6},
			9, false},
		{"no resolution is skipped",
			fake.Room{ID: "r-none", Title: "orders none", ClosedAt: day(10, 1), Hypothesis: true, Steps: 6},
			9, false},
		{"hypothesis but too few events is skipped",
			fake.Room{ID: "r-few", Title: "orders few", ClosedAt: day(10, 1), Resolution: "Restarted the connector.", Hypothesis: true},
			3, false},
		{"seven events is too few",
			fake.Room{ID: "r-seven", Title: "orders seven", ClosedAt: day(10, 1), Resolution: "Restarted the connector.", Hypothesis: true, Steps: 4},
			7, false},
		{"eight events is enough",
			fake.Room{ID: "r-eight", Title: "orders eight", ClosedAt: day(10, 1), Resolution: "Restarted the connector.", Hypothesis: true, Steps: 5},
			8, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, err := fake.NewStore(ctx, tc.room)
			if err != nil {
				t.Fatal(err)
			}
			room, err := st.GetRoom(ctx, tc.room.ID)
			if err != nil {
				t.Fatal(err)
			}
			if room.LastSeq != int64(tc.events) {
				t.Fatalf("fixture has %d events, the case says %d", room.LastSeq, tc.events)
			}
			tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
			res, err := run(t, tl, scoped("orders"), input(tc.room.Title, 3))
			if err != nil {
				t.Fatal(err)
			}
			if got := slices.Contains(hitIDs(t, res), tc.room.ID); got != tc.want {
				t.Fatalf("indexed = %v, want %v (hits %v)", got, tc.want, hitIDs(t, res))
			}
		})
	}
}

// TestSettledJunkRoomStaysOutOfTheIndex: a settled room is reused from the
// cache without being read again, so the reused copy must be filtered the same
// way as a fresh read.
func TestSettledJunkRoomStaysOutOfTheIndex(t *testing.T) {
	ctx := context.Background()
	st, err := fake.NewStore(ctx,
		fake.Room{ID: "r-qa", Title: "test", ClosedAt: day(9, 1), Resolution: "Closed by the QA run"},
		fake.Room{ID: "r-real", Title: "test", ClosedAt: day(9, 1), Resolution: "Restarted the connector.", Hypothesis: true, Steps: 6},
	)
	if err != nil {
		t.Fatal(err)
	}
	src := &countingSource{Source: similarincidents.FromStore(st)}
	// Built a day after both rooms closed, so both are settled on the next build.
	now := day(9, 2).Add(time.Hour)
	tl := similarincidents.New(similarincidents.Deps{Source: src, Now: func() time.Time { return now }, TTL: time.Minute})
	for build := 1; build <= 2; build++ {
		if build == 2 {
			now = now.Add(2 * time.Minute) // past the TTL: the index is rebuilt from the settled copies
		}
		before := src.events
		res, err := run(t, tl, scoped("orders"), input("test", 5))
		if err != nil {
			t.Fatal(err)
		}
		if ids := hitIDs(t, res); !slices.Equal(ids, []string{"r-real"}) {
			t.Fatalf("build %d: hits = %v, want only r-real", build, ids)
		}
		if build == 2 && src.events != before {
			t.Fatalf("settled rooms were read again: %d event reads", src.events-before)
		}
	}
}
