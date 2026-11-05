package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

// FleetStore is what RunFleet needs: the fleet surface plus the change writer
// that fills pipeline_changes.
type FleetStore interface {
	store.FleetStore
	clients.ChangeWriter
}

// RunFleet checks the fleet view's store surface (X5a): watch status rows are
// replaced, read back only for the pipelines asked, and refused when they break
// the clients.WatchStatus rules with nothing written; fleet change pages are
// newest first by (at, id), stable under later writes, filtered by pipeline,
// since and kind, and bounded by clients.MaxFleetChangesPage; the 24-hour
// count counts per pipeline. prefix makes the pipeline ids of this run unique.
func RunFleet(t *testing.T, s FleetStore, prefix string) {
	t.Helper()
	runWatchStatus(t, s, prefix)
	runFleetChanges(t, s, prefix)
}

func runWatchStatus(t *testing.T, s FleetStore, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	pa, pb, never := prefix+"_sa", prefix+"_sb", prefix+"_never"
	now := time.Date(2026, 10, 22, 10, 1, 0, 123456789, time.UTC)
	eta := now.Add(18 * time.Hour)
	first := clients.WatchStatus{PipelineID: pa, CheckedAt: now.Add(-time.Minute), Signals: []clients.WatchSignal{
		{Signal: "connector_down", State: clients.WatchFiring, Severity: "sev2", Node: "connect:c1", Since: now.Add(-time.Hour)},
	}}
	if err := s.PutWatchStatus(ctx, first); err != nil {
		t.Fatalf("put first: %v", err)
	}
	// The second write of pa replaces the first.
	// clearing is kept, in UTC and microseconds, on the signal that has it only (H1).
	clearing := now.Add(-3*time.Minute + 123*time.Nanosecond)
	want := clients.WatchStatus{PipelineID: pa, CheckedAt: now, Signals: []clients.WatchSignal{
		{Signal: "slot_wal", State: clients.WatchAcked, Severity: "sev1", Since: now.Add(-2 * time.Hour), ClearingSince: &clearing},
		{Signal: "forecast", State: clients.WatchFiring, Severity: "sev4", Since: now.Add(-time.Hour), Metric: "slot_wal_bytes", ETA: &eta},
	}}
	if err := s.PutWatchStatus(ctx, want); err != nil {
		t.Fatalf("put replace: %v", err)
	}
	if err := s.PutWatchStatus(ctx, clients.WatchStatus{PipelineID: pb, CheckedAt: now}); err != nil {
		t.Fatalf("put empty: %v", err)
	}
	got, err := s.WatchStatuses(ctx, []string{pb, never, pa})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].PipelineID != pa || got[1].PipelineID != pb {
		t.Fatalf("statuses = %+v, want %s then %s (a pipeline never checked has no row)", got, pa, pb)
	}
	g := got[0]
	if !g.CheckedAt.Equal(now.Truncate(time.Microsecond)) || len(g.Signals) != 2 {
		t.Fatalf("replaced row = %+v", g)
	}
	if s0, s1 := g.Signals[0], g.Signals[1]; s0.Signal != "slot_wal" || s0.State != clients.WatchAcked || s0.Severity != "sev1" ||
		!s0.Since.Equal(now.Add(-2*time.Hour).Truncate(time.Microsecond)) || s0.ETA != nil ||
		s1.Metric != "slot_wal_bytes" || s1.ETA == nil || !s1.ETA.Equal(eta.Truncate(time.Microsecond)) {
		t.Fatalf("signals = %+v", g.Signals)
	}
	if cs := g.Signals[0].ClearingSince; cs == nil || !cs.Equal(clearing.Truncate(time.Microsecond)) || cs.Location() != time.UTC || g.Signals[1].ClearingSince != nil {
		t.Fatalf("clearing_since = %v / %v, want %v on slot_wal only", cs, g.Signals[1].ClearingSince, clearing.Truncate(time.Microsecond))
	}
	if got[1].Signals == nil || len(got[1].Signals) != 0 {
		t.Fatalf("no signals must read back as [], got %#v", got[1].Signals)
	}
	if none, err := s.WatchStatuses(ctx, nil); err != nil || len(none) != 0 {
		t.Fatalf("no pipelines: %v %v", none, err)
	}

	// Refused rows write nothing.
	many := make([]clients.WatchSignal, clients.MaxWatchSignals+1)
	for i := range many {
		many[i] = clients.WatchSignal{Signal: fmt.Sprintf("s%d", i), State: clients.WatchFiring, Severity: "sev3", Since: now}
	}
	ok := clients.WatchSignal{Signal: "dlq_growth", State: clients.WatchFiring, Severity: "sev3", Since: now}
	bad := map[string]clients.WatchStatus{
		"no pipeline":   {CheckedAt: now},
		"no checked_at": {PipelineID: pb},
		"too many":      {PipelineID: pb, CheckedAt: now, Signals: many},
		"bad state":     {PipelineID: pb, CheckedAt: now, Signals: []clients.WatchSignal{{Signal: "x", State: "ok", Severity: "sev3", Since: now}}},
		"bad severity":  {PipelineID: pb, CheckedAt: now, Signals: []clients.WatchSignal{{Signal: "x", State: clients.WatchFiring, Severity: "sev9", Since: now}}},
		"no name":       {PipelineID: pb, CheckedAt: now, Signals: []clients.WatchSignal{ok, {State: clients.WatchFiring, Severity: "sev3", Since: now}}},
		"no since":      {PipelineID: pb, CheckedAt: now, Signals: []clients.WatchSignal{{Signal: "x", State: clients.WatchFiring, Severity: "sev3"}}},
	}
	for name, st := range bad {
		if err := s.PutWatchStatus(ctx, st); !errors.Is(err, store.ErrBadWatchStatus) {
			t.Errorf("%s: err = %v, want ErrBadWatchStatus", name, err)
		}
	}
	if got, err := s.WatchStatuses(ctx, []string{pb}); err != nil || len(got) != 1 || len(got[0].Signals) != 0 {
		t.Fatalf("a refused row changed pb: %+v %v", got, err)
	}
	// Exactly the cap is accepted.
	if err := s.PutWatchStatus(ctx, clients.WatchStatus{PipelineID: pb, CheckedAt: now, Signals: many[:clients.MaxWatchSignals]}); err != nil {
		t.Fatalf("put %d signals: %v", clients.MaxWatchSignals, err)
	}
}

func runFleetChanges(t *testing.T, s FleetStore, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	pa, pb, other := prefix+"_ca", prefix+"_cb", prefix+"_other"
	base := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	record := func(pid, kind string, i int, at time.Time) {
		t.Helper()
		if _, err := s.RecordChange(ctx, clients.PipelineChange{
			PipelineID: pid, Kind: kind, Object: "obj", At: at, Source: clients.ChangeSourceWatcher,
			Fingerprint: fmt.Sprintf("fp-%d", i), Summary: "s",
		}); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	// 250 rows over two pipelines; groups of five share an at, so the id
	// breaks ties. One more pipeline's rows are never asked for.
	for i := range 250 {
		pid, kind := pa, clients.ChangeConnectorConfig
		if i%2 == 1 {
			pid, kind = pb, clients.ChangeDDL
		}
		record(pid, kind, i, base.Add(time.Duration(i/5)*time.Minute))
	}
	for i := range 5 {
		record(other, clients.ChangeDDL, 1000+i, base.Add(time.Hour))
	}
	ids := []string{pa, pb}

	var all []clients.PipelineChange
	var cur clients.ChangeCursor
	pages := 0
	for {
		page, err := s.ListFleetChanges(ctx, ids, time.Time{}, nil, cur, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		if pages == 1 {
			// Written after the first page, newer than everything: it
			// must not shift the later pages.
			record(pa, clients.ChangeDDL, 9999, base.Add(24*time.Hour))
		}
		all = append(all, page...)
		last := page[len(page)-1]
		cur = clients.ChangeCursor{At: last.At, ID: last.ID}
		if pages > 5 {
			t.Fatal("paging does not end")
		}
	}
	if pages != 3 || len(all) != 250 {
		t.Fatalf("pages = %d, rows = %d, want 3 pages of 250 rows", pages, len(all))
	}
	seen := map[int64]bool{}
	for i, c := range all {
		if seen[c.ID] {
			t.Fatalf("row %d repeated", c.ID)
		}
		seen[c.ID] = true
		if c.PipelineID != pa && c.PipelineID != pb {
			t.Fatalf("row of %s listed", c.PipelineID)
		}
		if i > 0 {
			p := all[i-1]
			if c.At.After(p.At) || (c.At.Equal(p.At) && c.ID >= p.ID) {
				t.Fatalf("rows %d and %d out of order: %v/%d then %v/%d", i-1, i, p.At, p.ID, c.At, c.ID)
			}
		}
	}

	// since is inclusive; kinds filter; one pipeline.
	since := base.Add(40 * time.Minute) // groups 40..49: 50 rows
	got, err := s.ListFleetChanges(ctx, ids, since, nil, clients.ChangeCursor{}, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 51 { // plus the row written while paging
		t.Fatalf("since: %d rows, want 51", len(got))
	}
	got, err = s.ListFleetChanges(ctx, ids, since, []string{clients.ChangeConnectorConfig}, clients.ChangeCursor{}, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 25 {
		t.Fatalf("since + kind: %d rows, want 25", len(got))
	}
	for _, c := range got {
		if c.Kind != clients.ChangeConnectorConfig || c.PipelineID != pa {
			t.Fatalf("kind filter let %+v through", c)
		}
	}
	got, err = s.ListFleetChanges(ctx, []string{pb}, time.Time{}, nil, clients.ChangeCursor{}, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 125 {
		t.Fatalf("one pipeline: %d rows, want 125", len(got))
	}

	// Bounds: a limit over the cap is the cap; 0 and no pipelines are no rows.
	if got, err := s.ListFleetChanges(ctx, ids, time.Time{}, nil, clients.ChangeCursor{}, 1000); err != nil || len(got) != clients.MaxFleetChangesPage {
		t.Fatalf("limit 1000: %d rows, %v", len(got), err)
	}
	if got, err := s.ListFleetChanges(ctx, ids, time.Time{}, nil, clients.ChangeCursor{}, 0); err != nil || len(got) != 0 {
		t.Fatalf("limit 0: %d rows, %v", len(got), err)
	}
	if got, err := s.ListFleetChanges(ctx, nil, time.Time{}, nil, clients.ChangeCursor{}, 100); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no pipelines: %v, %v", got, err)
	}

	counts, err := s.CountChangesSince(ctx, []string{pa, pb, prefix + "_none"}, since)
	if err != nil {
		t.Fatal(err)
	}
	if counts[pa] != 26 || counts[pb] != 25 || len(counts) != 2 {
		t.Fatalf("counts = %v, want %s 26, %s 25", counts, pa, pb)
	}
}
