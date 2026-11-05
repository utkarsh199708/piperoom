package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools"
)

// freshnessTool builds the tool with d's fakes. The caller runs it with
// pipeline "orders" (testOwners) unless it wants the unscoped path.
func freshnessTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tt, ok := reg.Get("warehouse_freshness")
	if !ok {
		t.Fatal("warehouse_freshness not registered")
	}
	return tt
}

func runFreshness(t *testing.T, d Deps, ctx context.Context, input string) tools.Result {
	t.Helper()
	r, err := freshnessTool(t, d).Run(ctx, json.RawMessage(input))
	if err != nil {
		t.Fatalf("warehouse_freshness: %v", err)
	}
	return r
}

const freshnessInput = `{"table":"analytics.orders","ts_column":"updated_at"}`

// KAN-403: an old sink timestamp is only stale when the source has newer rows.
// A quiet source with a matching sink is idle, a source ahead of the sink is
// behind, and a sink ahead of the source is in sync with a non-negative lag.
func TestWarehouseFreshnessIdleVsStale(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name        string
		src, sink   time.Time
		srcErr      error
		wantVerdict string
		wantSummary []string
		notSummary  []string
		wantIdle    bool
		wantLagSign int
	}{
		{
			name: "TC1 idle source, old sink", src: now.Add(-time.Hour), sink: now.Add(-time.Hour),
			wantVerdict: "idle",
			wantSummary: []string{"idle: no writes at the source for", "sink in sync"},
			notSummary:  []string{"behind by", "stale:"},
			wantIdle:    true,
		},
		{
			name: "TC2 source newer than sink", src: now.Add(-time.Minute), sink: now.Add(-time.Hour),
			wantVerdict: "behind",
			wantSummary: []string{"stale: behind by", "source newest="},
			wantLagSign: 1,
		},
		{
			name: "TC3 sink ahead of source", src: now.Add(-5 * time.Minute), sink: now.Add(-3 * time.Minute),
			wantVerdict: "in sync",
			wantSummary: []string{"in sync (sink ahead by", "source newest="},
			notSummary:  []string{"behind by", "stale:"},
			wantLagSign: -1,
		},
		{
			name: "in sync within tolerance", src: now.Add(-90 * time.Second), sink: now.Add(-2 * time.Minute),
			wantVerdict: "in sync",
			wantSummary: []string{"in sync: lag", "source newest="},
			notSummary:  []string{"behind by", "idle", "stale:"},
			wantLagSign: 1,
		},
		{
			name: "TC4 source unreadable falls back to age", src: time.Time{}, sink: now.Add(-time.Hour),
			srcErr:      errors.New("postgres: connection refused"),
			wantVerdict: "unknown",
			wantSummary: []string{"newest=", "stale for 1.0h", "source unreadable, age only", "postgres: connection refused"},
			notSummary:  []string{"behind by", "idle"},
		},
		{
			name: "empty source table is idle", src: time.Time{}, sink: now.Add(-time.Hour),
			wantVerdict: "idle",
			wantSummary: []string{"idle: the source table has no rows"},
			notSummary:  []string{"stale:"},
			wantIdle:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := testDeps()
			src := d.Src.(*fakeSource)
			src.fresh, src.freshErr = tc.src, tc.srcErr
			d.Wh.(*fakeWarehouse).fresh = tc.sink
			r := runFreshness(t, d, tools.WithPipelineID(context.Background(), "orders"), freshnessInput)
			data := r.Data.(map[string]any)
			if got := data["verdict"]; got != tc.wantVerdict {
				t.Fatalf("verdict = %v, want %q (summary %q)", got, tc.wantVerdict, r.Summary)
			}
			for _, want := range tc.wantSummary {
				if !strings.Contains(r.Summary, want) {
					t.Errorf("summary %q lacks %q", r.Summary, want)
				}
			}
			for _, bad := range tc.notSummary {
				if strings.Contains(r.Summary, bad) {
					t.Errorf("summary %q must not contain %q", r.Summary, bad)
				}
			}
			if _, ok := data["idle"]; ok != tc.wantIdle {
				t.Errorf("data idle = %v, want present %v (%v)", data["idle"], tc.wantIdle, data)
			}
			if tc.wantLagSign != 0 {
				lag, ok := data["lag_seconds"].(int64)
				if !ok {
					t.Fatalf("lag_seconds = %v (%T), want int64", data["lag_seconds"], data["lag_seconds"])
				}
				switch {
				case tc.wantLagSign > 0 && lag <= 0:
					t.Errorf("lag_seconds = %d, want positive", lag)
				case tc.wantLagSign < 0 && lag >= 0:
					t.Errorf("lag_seconds = %d, want negative", lag)
				}
			}
			// The source read uses the pipeline's declared table mapping and
			// the canonical sink_ts_column, never an unvalidated input column.
			if src.freshCalls != 1 || src.freshTable != "public.orders" || src.freshColumn != "updated_at" {
				t.Fatalf("source read = %d call(s) table %q column %q, want public.orders/updated_at",
					src.freshCalls, src.freshTable, src.freshColumn)
			}
		})
	}
}

// A sink timestamp ahead of the clock is not comparable with the source; the
// tool keeps the old age-only answer and does not read the source.
func TestWarehouseFreshnessSkewReadsNoSource(t *testing.T) {
	d := testDeps()
	d.Wh.(*fakeWarehouse).fresh = time.Now().Add(30 * time.Second)
	r := runFreshness(t, d, tools.WithPipelineID(context.Background(), "orders"), freshnessInput)
	if !strings.Contains(r.Summary, "stale for 0s") {
		t.Fatalf("summary %q, want the old age-only answer", r.Summary)
	}
	if calls := d.Src.(*fakeSource).freshCalls; calls != 0 {
		t.Fatalf("source read %d time(s) for a sink ahead of the clock", calls)
	}
	if got := r.Data.(map[string]any)["verdict"]; got != "unknown" {
		t.Fatalf("verdict = %v, want unknown", got)
	}
}

// A source client without the optional read, and a pipeline without a source
// table, both make the verdict unknown with a clear reason instead of failing
// the call or guessing a table.
func TestWarehouseFreshnessUnknownSource(t *testing.T) {
	cases := []struct {
		name string
		deps func(*Deps)
		want string
	}{
		{"client without Freshness", func(d *Deps) { d.Src = plainSource{} }, "cannot read a newest timestamp"},
		{"pipeline without source_tables", func(d *Deps) { d.Owners.Pipelines[0].SourceTables = nil }, "lists no source_tables"},
		{"ambiguous source mapping", func(d *Deps) {
			d.Owners.Pipelines[0].SourceTables = []string{"public.orders", "other.orders"}
		}, "cannot tell which"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := testDeps()
			d.Wh.(*fakeWarehouse).fresh = time.Now().Add(-time.Hour)
			tc.deps(&d)
			r := runFreshness(t, d, tools.WithPipelineID(context.Background(), "orders"), freshnessInput)
			data := r.Data.(map[string]any)
			if data["verdict"] != "unknown" {
				t.Fatalf("verdict = %v, want unknown (%q)", data["verdict"], r.Summary)
			}
			if !strings.Contains(r.Summary, "source unknown, age only") || !strings.Contains(r.Summary, tc.want) {
				t.Fatalf("summary %q lacks the clear reason %q", r.Summary, tc.want)
			}
			if !strings.Contains(r.Summary, "stale for 1.0h") {
				t.Fatalf("summary %q should keep the sink's age", r.Summary)
			}
		})
	}
}

// Unscoped calls have no pipeline to map the sink to a source, so they keep
// the age-only answer instead of inventing a source table.
func TestWarehouseFreshnessUnscopedIsAgeOnly(t *testing.T) {
	d := testDeps()
	d.Wh.(*fakeWarehouse).fresh = time.Now().Add(-time.Hour)
	r := runFreshness(t, d, tools.WithUnscoped(context.Background()), freshnessInput)
	data := r.Data.(map[string]any)
	if data["verdict"] != "unknown" || !strings.Contains(r.Summary, "no pipeline scope") {
		t.Fatalf("summary %q data %v, want unknown with the no-scope reason", r.Summary, data)
	}
	if calls := d.Src.(*fakeSource).freshCalls; calls != 0 {
		t.Fatalf("source read %d time(s) without a pipeline scope", calls)
	}
}

// A source sentinel or a source clock far ahead is not comparable: unknown,
// never "in sync" from a negative lag.
func TestWarehouseFreshnessUnjudgeableSource(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		src  time.Time
		want string
	}{
		{"infinity", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), "source newest is infinity"},
		{"future", now.Add(3 * time.Hour), "IN THE FUTURE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := testDeps()
			d.Src.(*fakeSource).fresh = tc.src
			d.Wh.(*fakeWarehouse).fresh = now.Add(-time.Hour)
			r := runFreshness(t, d, tools.WithPipelineID(context.Background(), "orders"), freshnessInput)
			data := r.Data.(map[string]any)
			if data["verdict"] != "unknown" || !strings.Contains(r.Summary, tc.want) {
				t.Fatalf("summary %q data %v, want unknown with %q", r.Summary, data, tc.want)
			}
		})
	}
}
