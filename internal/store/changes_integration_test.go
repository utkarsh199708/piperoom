//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

func pgChange(pipeline, kind, fp string, at time.Time) clients.PipelineChange {
	return clients.PipelineChange{
		PipelineID: pipeline, Kind: kind, Object: "pg-orders-connector", At: at, Source: "watcher", Fingerprint: fp,
		Before: json.RawMessage(`{"state":"RUNNING"}`), After: json.RawMessage(`{"state":"FAILED"}`), Summary: "RUNNING → FAILED",
	}
}

func TestRecordChangeDedups(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	ctx := context.Background()
	at := time.Date(2026, 10, 14, 3, 4, 11, 123456789, time.UTC)
	c := pgChange("orders", clients.ChangeConnectorState, "fp", at)
	for i, want := range []bool{true, false} {
		if got, err := s.RecordChange(ctx, c); err != nil || got != want {
			t.Fatalf("call %d: inserted = %v, %v; want %v", i, got, err, want)
		}
	}
	rows, err := s.ListChanges(ctx, "orders", time.Time{}, time.Time{}, nil, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %d, %v", len(rows), err)
	}
	r := rows[0]
	if r.ID == 0 || !r.At.Equal(at.Truncate(time.Microsecond)) || r.Kind != c.Kind || r.Summary != c.Summary {
		t.Fatalf("row = %+v", r)
	}
	if string(r.Before) != `{"state": "RUNNING"}` && string(r.Before) != `{"state":"RUNNING"}` {
		t.Fatalf("before = %s", r.Before)
	}
	// A DDL row has no before/after: both come back empty, not "null".
	d := clients.PipelineChange{PipelineID: "orders", Kind: clients.ChangeDDL, Object: "public.orders", At: at, Source: "ddl_audit", Fingerprint: "d", Actor: "piperoom", Summary: "ALTER TABLE"}
	if _, err := s.RecordChange(ctx, d); err != nil {
		t.Fatal(err)
	}
	ddl, _ := s.ListChanges(ctx, "orders", time.Time{}, time.Time{}, []string{clients.ChangeDDL}, 10)
	if len(ddl) != 1 || len(ddl[0].Before) != 0 || len(ddl[0].After) != 0 || ddl[0].Actor != "piperoom" {
		t.Fatalf("ddl rows = %+v", ddl)
	}
	if _, err := s.RecordChange(ctx, clients.PipelineChange{}); err == nil {
		t.Fatal("an empty change must be refused")
	}
}

func TestRecordChangeCapsPerPipeline(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	n := store.MaxChangesPerPipeline + 10
	for i := 0; i < n; i++ {
		if _, err := s.RecordChange(ctx, pgChange("orders", clients.ChangeConnectorState, fmt.Sprint(i), t0.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.RecordChange(ctx, pgChange("payments", clients.ChangeConnectorState, "p", t0)); err != nil {
		t.Fatal(err)
	}
	oldest := t0.Add(10 * time.Second)
	if old, _ := s.ListChanges(ctx, "orders", time.Time{}, oldest, nil, store.MaxListChanges); len(old) != 0 {
		t.Fatalf("the 10 oldest rows must be gone, %d remain", len(old))
	}
	kept, _ := s.ListChanges(ctx, "orders", oldest, time.Time{}, nil, store.MaxListChanges)
	if len(kept) != store.MaxListChanges {
		t.Fatalf("newest rows kept: %d", len(kept))
	}
	if other, _ := s.ListChanges(ctx, "payments", time.Time{}, time.Time{}, nil, 10); len(other) != 1 {
		t.Fatalf("another pipeline must not be capped: %d", len(other))
	}
}

func TestChangesRetention(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		if _, err := s.RecordChange(ctx, pgChange("orders", clients.ChangeConnectorState, fmt.Sprint(i), t0.Add(time.Duration(i)*24*time.Hour))); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.PurgeChangesBefore(ctx, t0.Add(48*time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("purged %d, %v; want 2", n, err)
	}
	rows, _ := s.ListChanges(ctx, "orders", time.Time{}, time.Time{}, nil, 10)
	if len(rows) != 2 || rows[0].Fingerprint != "3" {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestListChangesFilters(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	for i, k := range []string{clients.ChangeDDL, clients.ChangeConnectorState, clients.ChangeSchemaVersion, clients.ChangeDDL} {
		if _, err := s.RecordChange(ctx, pgChange("orders", k, fmt.Sprint(i), t0.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	fps := func(since, until time.Time, kinds []string, limit int) string {
		rows, err := s.ListChanges(ctx, "orders", since, until, kinds, limit)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range rows {
			out = append(out, r.Fingerprint)
		}
		return fmt.Sprint(out)
	}
	for _, c := range []struct {
		got, want string
	}{
		{fps(time.Time{}, time.Time{}, nil, 10), "[3 2 1 0]"},
		{fps(time.Time{}, time.Time{}, nil, 2), "[3 2]"},
		{fps(time.Time{}, time.Time{}, []string{clients.ChangeDDL}, 10), "[3 0]"},
		{fps(t0.Add(time.Minute), t0.Add(3*time.Minute), nil, 10), "[2 1]"},
		{fps(time.Time{}, time.Time{}, nil, 0), "[]"},
	} {
		if c.got != c.want {
			t.Errorf("got %s, want %s", c.got, c.want)
		}
	}
}
