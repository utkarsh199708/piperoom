package memstore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

var t0 = time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)

func change(pipeline, kind, fp string, at time.Time) clients.PipelineChange {
	return clients.PipelineChange{
		PipelineID: pipeline, Kind: kind, Object: "obj", At: at, Source: "watcher", Fingerprint: fp,
		After: json.RawMessage(`{"state":"FAILED"}`), Summary: "s " + fp,
	}
}

func TestRecordChangeDedups(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	c := change("orders", clients.ChangeConnectorState, "fp", t0)
	for i, want := range []bool{true, false, false} {
		got, err := s.RecordChange(ctx, c)
		if err != nil || got != want {
			t.Fatalf("call %d: inserted = %v, %v; want %v", i, got, err, want)
		}
	}
	// Same fingerprint on another object, kind or pipeline is a different row.
	for _, o := range []clients.PipelineChange{
		func() clients.PipelineChange { x := c; x.Object = "other"; return x }(),
		func() clients.PipelineChange { x := c; x.Kind = clients.ChangeDDL; return x }(),
		func() clients.PipelineChange { x := c; x.PipelineID = "payments"; return x }(),
	} {
		if ok, err := s.RecordChange(ctx, o); err != nil || !ok {
			t.Fatalf("distinct row: %v, %v", ok, err)
		}
	}
	if _, err := s.RecordChange(ctx, clients.PipelineChange{}); err == nil {
		t.Fatal("an empty change must be refused")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.RecordChange(cctx, change("orders", clients.ChangeDDL, "z", t0)); err == nil {
		t.Fatal("a cancelled context must be refused")
	}
}

func TestRecordChangeCapsPerPipeline(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	n := store.MaxChangesPerPipeline + 20
	for i := 0; i < n; i++ {
		if _, err := s.RecordChange(ctx, change("orders", clients.ChangeConnectorState, fmt.Sprint(i), t0.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.RecordChange(ctx, change("payments", clients.ChangeDDL, "p", t0)); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ListChanges(ctx, "orders", time.Time{}, time.Time{}, nil, 1000000)
	if err != nil {
		t.Fatal(err)
	}
	// ListChanges answers at most store.MaxListChanges; count through windows.
	if len(rows) != store.MaxListChanges {
		t.Fatalf("list cap: %d rows", len(rows))
	}
	oldest := t0.Add(20 * time.Second)
	all, _ := s.ListChanges(ctx, "orders", time.Time{}, oldest, nil, store.MaxListChanges)
	if len(all) != 0 {
		t.Fatalf("the 20 oldest rows must be gone, %d remain before %v", len(all), oldest)
	}
	kept, _ := s.ListChanges(ctx, "orders", oldest, time.Time{}, nil, store.MaxListChanges)
	if len(kept) != store.MaxListChanges {
		t.Fatalf("newest rows kept: %d", len(kept))
	}
	if other, _ := s.ListChanges(ctx, "payments", time.Time{}, time.Time{}, nil, 10); len(other) != 1 {
		t.Fatalf("another pipeline must not be capped: %d", len(other))
	}
}

func TestListChangesFilters(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	for i, k := range []string{clients.ChangeDDL, clients.ChangeConnectorState, clients.ChangeSchemaVersion, clients.ChangeDDL} {
		if _, err := s.RecordChange(ctx, change("orders", k, fmt.Sprint(i), t0.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	list := func(since, until time.Time, kinds []string, limit int) []string {
		rows, err := s.ListChanges(ctx, "orders", since, until, kinds, limit)
		if err != nil {
			t.Fatal(err)
		}
		var fps []string
		for _, r := range rows {
			fps = append(fps, r.Fingerprint)
		}
		return fps
	}
	eq := func(got []string, want ...string) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	eq(list(time.Time{}, time.Time{}, nil, 10), "3", "2", "1", "0") // newest first
	eq(list(time.Time{}, time.Time{}, nil, 2), "3", "2")
	eq(list(time.Time{}, time.Time{}, []string{clients.ChangeDDL}, 10), "3", "0")
	eq(list(t0.Add(time.Minute), t0.Add(3*time.Minute), nil, 10), "2", "1") // since inclusive, until exclusive
	eq(list(time.Time{}, time.Time{}, nil, 0))
	if rows, _ := s.ListChanges(ctx, "nobody", time.Time{}, time.Time{}, nil, 10); len(rows) != 0 {
		t.Fatal("unknown pipeline lists nothing")
	}
}

func TestChangesRetention(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if _, err := s.RecordChange(ctx, change("orders", clients.ChangeDDL, fmt.Sprint(i), t0.Add(time.Duration(i)*24*time.Hour))); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.PurgeChangesBefore(ctx, t0.Add(48*time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("purged %d, %v; want 2", n, err)
	}
	rows, _ := s.ListChanges(ctx, "orders", time.Time{}, time.Time{}, nil, 10)
	if len(rows) != 2 {
		t.Fatalf("%d rows left", len(rows))
	}
	// A purged fingerprint can be recorded again.
	if ok, _ := s.RecordChange(ctx, change("orders", clients.ChangeDDL, "0", t0)); !ok {
		t.Fatal("purged row should be insertable again")
	}
}
