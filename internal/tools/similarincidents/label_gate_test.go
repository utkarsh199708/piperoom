package similarincidents_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

// gateRoom is a room every test query matches by title, so only the gate
// decides whether it is returned.
func gateRoom(id string, mut func(*fake.Room)) fake.Room {
	r := fake.Room{ID: id, Title: "zebrafish pipeline stalled", ClosedAt: day(9, 30), Resolution: "closing words"}
	if mut != nil {
		mut(&r)
	}
	return r
}

func TestOnlyLabelledRoomsAreReturned(t *testing.T) {
	label := &events.RoomLabelledPayload{ActualLayer: "kafka-connect", Cause: "slot dropped", FixThatWorked: "recreated slot"}
	tests := []struct {
		name        string
		rooms       []fake.Room
		wantIDs     []string
		wantSkipped int
	}{
		{"labelled is returned", []fake.Room{gateRoom("r-l", func(r *fake.Room) { r.Label = label })}, []string{"r-l"}, 0},
		{"unlabelled is excluded and counted", []fake.Room{gateRoom("r-u", nil)}, nil, 1},
		{"mixed", []fake.Room{
			gateRoom("r-l", func(r *fake.Room) { r.Label = label }),
			gateRoom("r-u1", nil),
			gateRoom("r-u2", nil),
		}, []string{"r-l"}, 2},
		{"only the shadow label does not count", []fake.Room{gateRoom("r-s", func(r *fake.Room) {
			r.Layer, r.LabelTerms = "kafka", []string{"zebrafish"}
		})}, nil, 1},
		{"dev webhook room is dropped even when labelled", []fake.Room{
			gateRoom("r-w", func(r *fake.Room) { r.Label, r.AlertSource = label, "webhook" }),
		}, nil, 0},
		{"smoke- and test- dedup keys are dropped", []fake.Room{
			gateRoom("r-s", func(r *fake.Room) { r.Label, r.DedupKey = label, "smoke-clear-1" }),
			gateRoom("r-t", func(r *fake.Room) { r.Label, r.DedupKey = label, "TEST-x" }),
			gateRoom("r-ok", func(r *fake.Room) { r.Label, r.DedupKey = label, "watcher:orders" }),
		}, []string{"r-ok"}, 0},
		{"signed source stays", []fake.Room{gateRoom("r-a", func(r *fake.Room) { r.Label, r.AlertSource = label, "alertmanager" })}, []string{"r-a"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(seededRaw(t, tc.rooms...))})
			res, err := run(t, tl, scoped("orders"), input("zebrafish pipeline stalled", 5))
			if err != nil {
				t.Fatal(err)
			}
			d := res.Data.(similarincidents.Data)
			ids := hitIDs(t, res)
			slices.Sort(ids)
			if !slices.Equal(ids, tc.wantIDs) || d.UnlabelledSkipped != tc.wantSkipped {
				t.Errorf("hits %v skipped %d, want %v skipped %d", ids, d.UnlabelledSkipped, tc.wantIDs, tc.wantSkipped)
			}
			if tc.wantSkipped > 0 && !strings.Contains(res.Summary, "no human label") {
				t.Errorf("summary does not say history was left out: %q", res.Summary)
			}
			if tc.wantSkipped == 0 && strings.Contains(res.Summary, "no human label") {
				t.Errorf("summary mentions skipped rooms: %q", res.Summary)
			}
		})
	}
}

// The case from the 2026-10-25 live test: throwaway closing words of an
// unlabelled room must not reach the model.
func TestThrowawayResolutionOfUnlabelledRoomNeverShown(t *testing.T) {
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(seededRaw(t,
		gateRoom("r-stale", func(r *fake.Room) {
			r.Resolution = "Stale room from the Oct 16 demo run; closed for the smoke test."
		})))})
	res, err := run(t, tl, scoped("orders"), input("zebrafish pipeline stalled", 3))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Summary, "Stale room") || len(hitIDs(t, res)) != 0 {
		t.Errorf("summary = %q", res.Summary)
	}
	if !strings.Contains(res.Summary, "No similar closed incident") {
		t.Errorf("summary = %q", res.Summary)
	}
}

func TestLabelledHitCarriesCauseAndMarksResolutionUnverified(t *testing.T) {
	label := &events.RoomLabelledPayload{Cause: "slot dropped", FixThatWorked: "recreated slot"}
	res, err := run(t, similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(seededRaw(t,
		gateRoom("r-1", func(r *fake.Room) { r.Label = label })))}), scoped("orders"), input("zebrafish pipeline stalled", 3))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"cause: slot dropped", "fix: recreated slot", "unverified closing note: closing words"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q: %q", want, res.Summary)
		}
	}
	h := res.Data.(similarincidents.Data).Hits[0]
	if h.Cause != "slot dropped" || h.LabelFix != "recreated slot" {
		t.Errorf("primary fields = %q, %q", h.Cause, h.LabelFix)
	}
}

func TestRelabelLastLabelWins(t *testing.T) {
	r := gateRoom("r-1", func(r *fake.Room) {
		r.Label = &events.RoomLabelledPayload{ActualLayer: "kafka", Cause: "first", FixThatWorked: "old fix"}
		r.Relabels = []events.RoomLabelledPayload{{Cause: "second cause"}}
	})
	hits, summary := hitsOf(t, r)
	if len(hits) != 1 {
		t.Fatalf("hits = %+v", hits)
	}
	h := hits[0]
	// The later label names no layer and no fix: nothing of the first survives.
	if h.Cause != "second cause" || h.LabelFix != "" || h.Layer != "" || strings.Contains(summary, "first") || strings.Contains(summary, "old fix") {
		t.Errorf("hit = %+v, summary = %q", h, summary)
	}
}

func TestGateWithFingerprintMatchStillSkipsUnlabelled(t *testing.T) {
	const trace = "ERROR task failed\norg.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler\n\tat org.apache.kafka.connect.runtime.WorkerSinkTask.deliverMessages(WorkerSinkTask.java:630)"
	res := []fake.ToolResult{{Tool: "connect_task_status", Summary: trace}}
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(seededRaw(t,
		fake.Room{ID: "r-u", Title: "sink down", ClosedAt: day(9, 30), Resolution: "x", ToolResults: res},
		fake.Room{ID: "r-l", Title: "sink down", ClosedAt: day(9, 29), Resolution: "y", ToolResults: res, Label: &events.RoomLabelledPayload{Cause: "c"}},
	))})
	out, err := run(t, tl, scoped("orders"), input(trace, 5))
	if err != nil {
		t.Fatal(err)
	}
	d := out.Data.(similarincidents.Data)
	if !slices.Equal(hitIDs(t, out), []string{"r-l"}) || d.UnlabelledSkipped != 1 {
		t.Errorf("hits %v skipped %d", hitIDs(t, out), d.UnlabelledSkipped)
	}
}

// failEvents fails the event read of one room; the rest must still answer.
type failEvents struct {
	similarincidents.Source
	room string
	err  error
}

func (f failEvents) EventsOfTypes(ctx context.Context, id string, ty []events.Type, n int) ([]events.Event, error) {
	if id == f.room {
		return nil, f.err
	}
	return f.Source.EventsOfTypes(ctx, id, ty, n)
}

func TestUnreadableRoomIsSkippedNotTrusted(t *testing.T) {
	boom := errors.New("db down")
	st := seeded(t,
		gateRoom("r-bad", nil),
		gateRoom("r-ok", func(r *fake.Room) { r.ClosedAt = day(9, 29) }),
	)
	tl := similarincidents.New(similarincidents.Deps{Source: failEvents{similarincidents.FromStore(st), "r-bad", boom}})
	res, err := run(t, tl, scoped("orders"), input("zebrafish pipeline stalled", 5))
	if err != nil {
		t.Fatal(err)
	}
	d := res.Data.(similarincidents.Data)
	if !slices.Equal(hitIDs(t, res), []string{"r-ok"}) || !d.Partial {
		t.Errorf("hits %v partial %v", hitIDs(t, res), d.Partial)
	}

	// Listing failure is an error, not "no labelled history".
	tl = similarincidents.New(similarincidents.Deps{Source: failList{similarincidents.FromStore(st), boom}})
	if _, err := run(t, tl, scoped("orders"), input("zebrafish", 5)); !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
	}
}

func TestDescriptionExplainsTheGate(t *testing.T) {
	d := similarincidents.New(similarincidents.Deps{}).Description()
	for _, w := range []string{"labelled", "unlabelled_skipped", "verified"} {
		if !strings.Contains(d, w) {
			t.Errorf("description lacks %q", w)
		}
	}
}

var _ store.ClosedRoomReader = failEvents{}
