package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

var rcSince = time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)

// countSrc / countWh are fakes with per-bucket counts keyed by bucket index.
type countSrc struct {
	fakeSource
	counts []int64
	err    error
	got    struct {
		table, col string
		since      time.Time
		until      time.Time
		width      time.Duration
	}
}

func (f *countSrc) CountByBucket(_ context.Context, table, col string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	f.got.table, f.got.col, f.got.since, f.got.until, f.got.width = table, col, since, until, width
	return toBuckets(f.counts, since, width), f.err
}

type countWh struct {
	fakeWarehouse
	counts []int64
	table  string
}

func (f *countWh) CountByBucket(_ context.Context, table, _ string, since, _ time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	f.table = table
	return toBuckets(f.counts, since, width), nil
}

func toBuckets(counts []int64, since time.Time, width time.Duration) []clients.MinuteCount {
	var out []clients.MinuteCount
	for i, c := range counts {
		if c > 0 { // empty buckets are not returned
			out = append(out, clients.MinuteCount{Bucket: since.Add(time.Duration(i) * width), Count: c})
		}
	}
	return out
}

type rateKafka struct {
	clients.KafkaReader
	perBoundary func(at time.Time) int64 // records since at
	calls       atomic.Int32
	err         error
}

func (f *rateKafka) PartitionRates(_ context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	return []clients.PartitionRate{{Topic: topics[0], Records: f.perBoundary(since)}}, nil
}

func rcDeps(src *countSrc, wh *countWh, k clients.KafkaReader) Deps {
	return Deps{Src: src, Wh: wh, Kafka: k, Builder: &fakeBuilder{}, Owners: &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, SinkTables: []string{"analytics.orders"}, SinkTSColumn: "updated_at",
	}, {
		ID: "multi", TopicPrefix: "pg", SourceTables: []string{"public.a"}, SinkTables: []string{"x.one", "x.two"}, SinkTSColumn: "updated_at",
	}, {
		ID: "nots", TopicPrefix: "pg", SourceTables: []string{"public.a"},
	}}}}
}

func rcRun(t *testing.T, d Deps, pipeline, in string) (tools.Result, error) {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, _ := reg.Get("reconcile")
	return tl.Run(tools.WithPipelineID(context.Background(), pipeline), json.RawMessage(in))
}

const rcIn = `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:30:00Z"}`

func TestReconcileWindowAndBuckets(t *testing.T) {
	// 30 min window: ceil(30/12 = 2.5 min) = 3 min buckets, 10 of them.
	src := &countSrc{counts: []int64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}}
	wh := &countWh{counts: []int64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}}
	res, err := rcRun(t, rcDeps(src, wh, nil), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	rows := res.Data.([]ReconcileRow)
	if len(rows) != 10 || src.got.width != 3*time.Minute || src.got.col != "updated_at" || wh.table != "analytics.orders" {
		t.Errorf("rows=%d width=%s col=%s sink=%s", len(rows), src.got.width, src.got.col, wh.table)
	}
	if !strings.Contains(res.Summary, "matches the source in every bucket") || !strings.Contains(res.Summary, "n/a") {
		t.Errorf("summary:\n%s", res.Summary)
	}
	if n := strings.Count(res.Summary, "\n"); n > 14 {
		t.Errorf("%d lines", n)
	}
	// A 6 h window is 12 buckets of 30 min; a 1 min window is one bucket.
	for _, c := range []struct {
		d time.Duration
		w time.Duration
		n int
	}{{6 * time.Hour, 30 * time.Minute, 12}, {time.Minute, time.Minute, 1}, {13 * time.Minute, 2 * time.Minute, 7}} {
		w, n := reconcileGrid(rcSince, rcSince.Add(c.d))
		if w != c.w || n != c.n || n > reconcileBuckets {
			t.Errorf("grid(%s) = %s, %d; want %s, %d", c.d, w, n, c.w, c.n)
		}
	}
}

func TestReconcileVerdictSide(t *testing.T) {
	// Buckets 3 and 4 lack 30 rows each in the sink (of 40).
	srcC := []int64{40, 40, 40, 40, 40, 40, 40, 40, 40, 40}
	snkC := []int64{40, 40, 40, 10, 10, 40, 40, 40, 40, 40}
	width := 3 * time.Minute
	topicWith := func(missing bool) *rateKafka {
		return &rateKafka{perBoundary: func(at time.Time) int64 {
			// records written since `at`: 40 per bucket, minus the lost ones in buckets 3 and 4.
			var total int64
			for i := range srcC {
				start := rcSince.Add(time.Duration(i) * width)
				if !start.Before(at) {
					v := srcC[i]
					if missing && (i == 3 || i == 4) {
						v = 0
					}
					total += v
				}
			}
			return total
		}}
	}
	cases := []struct {
		name    string
		k       clients.KafkaReader
		want    string
		notWant string
	}{
		// Enough events in the topic, but events are not rows: the side stays
		// open and the next check is named (#482).
		{"events cover the rows", topicWith(false), "the side is unproven, confirm one missing key with warehouse_row", "connector side"},
		// Each source row updated twice: 3 events per row. That must not read
		// as proof that the consumer dropped the rows (#482).
		{"updates inflate events", &rateKafka{perBoundary: func(at time.Time) int64 {
			var total int64
			for i := range srcC {
				if !rcSince.Add(time.Duration(i) * width).Before(at) {
					total += 3 * srcC[i]
				}
			}
			return total
		}}, "the side is unproven", "consumer side"},
		{"connector side", topicWith(true), "connector side", "consumer side"},
		{"no rate reader", nil, "topic counts are not available, so the side is unknown", "consumer side"},
		{"rates fail", &rateKafka{err: errors.New("broker down")}, "topic counts are not available", "consumer side"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := rcRun(t, rcDeps(&countSrc{counts: srcC}, &countWh{counts: snkC}, c.k), "orders", rcIn)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(res.Summary, c.want) || strings.Contains(res.Summary, c.notWant) {
				t.Errorf("summary:\n%s", res.Summary)
			}
		})
	}
	// n+1 offset reads, at most 13.
	k := topicWith(false)
	if _, err := rcRun(t, rcDeps(&countSrc{counts: srcC}, &countWh{counts: snkC}, k), "orders", rcIn); err != nil {
		t.Fatal(err)
	}
	if k.calls.Load() != 11 {
		t.Errorf("rate reads = %d, want 11", k.calls.Load())
	}
}

func TestReconcileTopicColumnValues(t *testing.T) {
	src := &countSrc{counts: []int64{5, 5, 5, 5, 5, 5, 5, 5, 5, 5}}
	k := &rateKafka{perBoundary: func(at time.Time) int64 {
		return int64(10-int(at.Sub(rcSince)/(3*time.Minute))) * 5
	}}
	res, err := rcRun(t, rcDeps(src, &countWh{counts: src.counts}, k), "orders", rcIn)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Data.([]ReconcileRow) {
		if r.Topic != 5 {
			t.Fatalf("topic = %d in %+v", r.Topic, r)
		}
	}
}

func TestReconcileInputErrors(t *testing.T) {
	d := rcDeps(&countSrc{}, &countWh{}, nil)
	cases := []struct {
		name, pipeline, in string
		bad                bool
		want               string
	}{
		{"window over 6h", "orders", `{"table":"public.orders","since":"2026-10-14T00:00:00Z","until":"2026-10-14T06:00:01Z"}`, true, "at most"},
		{"until before since", "orders", `{"table":"public.orders","since":"2026-10-14T03:00:00Z","until":"2026-10-14T02:00:00Z"}`, true, "after since"},
		{"bad since", "orders", `{"table":"public.orders","since":"noon"}`, true, "RFC 3339"},
		{"bad table", "orders", `{"table":"public.orders;--","since":"2026-10-14T03:00:00Z"}`, true, "schema.table"},
		{"no ts column", "nots", `{"table":"public.a","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:10:00Z"}`, true, "no timestamp column configured"},
		{"ambiguous sinks", "multi", `{"table":"public.a","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:10:00Z"}`, true, "sink_table"},
		{"other pipeline's table", "orders", `{"table":"public.a","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:10:00Z"}`, false, "not part of"},
	}
	for _, c := range cases {
		_, err := rcRun(t, d, c.pipeline, c.in)
		if err == nil || !strings.Contains(err.Error(), c.want) || errors.Is(err, tools.ErrBadInput) != c.bad && c.bad {
			t.Errorf("%s: err = %v", c.name, err)
		}
	}
	// Explicit sink_table resolves the ambiguity; client errors surface.
	if _, err := rcRun(t, rcDeps(&countSrc{}, &countWh{}, nil), "multi", `{"table":"public.a","sink_table":"x.two","since":"2026-10-14T03:00:00Z","until":"2026-10-14T03:10:00Z"}`); err != nil {
		t.Errorf("explicit sink_table: %v", err)
	}
	if _, err := rcRun(t, rcDeps(&countSrc{err: errors.New("pg down")}, &countWh{}, nil), "orders", rcIn); err == nil || !strings.Contains(err.Error(), "pg down") {
		t.Errorf("source error: %v", err)
	}
	// A client without CountReader answers instead of failing.
	res, err := rcRun(t, Deps{Src: &fakeSource{}, Wh: &fakeWarehouse{}, Builder: &fakeBuilder{}, Owners: d.Owners}, "orders", rcIn)
	if err != nil || !strings.Contains(res.Summary, "not supported") {
		t.Errorf("unsupported: %q %v", res.Summary, err)
	}
}

// The newest slice (settle margin) is cut off the window, not a whole bucket
// (#421). Loss in the rest of the newest bucket is still reported.
func TestReconcileSettleCutsOnlyTheNewestSlice(t *testing.T) {
	flat := []int64{600, 600, 600, 600, 600, 600, 600, 600, 600, 600, 600, 600}
	in := func(since time.Duration, extra string) string {
		return fmt.Sprintf(`{"table":"public.orders","since":%q%s}`, time.Now().Add(-since).Format(time.RFC3339Nano), extra)
	}
	run := func(src, snk []int64, input string) (tools.Result, *countSrc, error) {
		s := &countSrc{counts: src}
		res, err := rcRun(t, rcDeps(s, &countWh{counts: snk}, nil), "orders", input)
		return res, s, err
	}
	last := func(v int64) []int64 { c := append([]int64(nil), flat...); c[11] = v; return c }

	// 6 h window, 12 buckets of 30 min, nothing in the newest bucket.
	res, s, err := run(flat, last(0), in(6*time.Hour, ""))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "sink is missing 600 rows") {
		t.Errorf("loss in the newest bucket hidden:\n%s", res.Summary)
	}
	if want := time.Now().Add(-defaultSettle); s.got.until.After(want) || want.Sub(s.got.until) > time.Minute {
		t.Errorf("until = %s, want about now−2m", s.got.until)
	}
	if !strings.Contains(res.Summary, "last 2m0s) are in flight and not judged") {
		t.Errorf("margin not named:\n%s", res.Summary)
	}

	// A consumer that stalled: 200 of 600 in the newest bucket.
	if res, _, err = run(flat, last(200), in(6*time.Hour, "")); err != nil || !strings.Contains(res.Summary, "sink is missing 400 rows") {
		t.Errorf("stall: err=%v\n%s", err, res.Summary)
	}

	// Healthy: every bucket complete, the verdict says so and names the margin.
	if res, _, err = run(flat, flat, in(6*time.Hour, "")); err != nil || !strings.Contains(res.Summary, "matches the source in every bucket") {
		t.Errorf("healthy: err=%v\n%s", err, res.Summary)
	}

	// An explicit until in the future is cut too; settle widens the margin.
	if _, s, err = run(flat, flat, in(2*time.Hour, fmt.Sprintf(`,"until":%q,"settle":"10m"`, time.Now().Add(time.Hour).Format(time.RFC3339)))); err != nil {
		t.Fatal(err)
	} else if d := time.Since(s.got.until); d < 9*time.Minute || d > 11*time.Minute {
		t.Errorf("until is %s before now, want about 10m", d)
	}

	// A window that lies entirely inside the margin gives no verdict at all.
	_, _, err = run([]int64{600, 600, 0}, nil, in(2*time.Minute, ""))
	if !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "no verdict possible") {
		t.Errorf("window inside the margin: err = %v", err)
	}
	for _, bad := range []string{"2x", "-1m", "2h"} {
		if _, _, err = run(flat, flat, in(time.Hour, fmt.Sprintf(`,"settle":%q`, bad))); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("settle %q: err = %v", bad, err)
		}
	}

	// The margin is documented for the agent.
	reg := tools.NewRegistry()
	if err := Register(reg, rcDeps(&countSrc{}, &countWh{}, nil)); err != nil {
		t.Fatal(err)
	}
	tl, _ := reg.Get("reconcile")
	if !strings.Contains(tl.Description(), "settle margin (default 2m)") {
		t.Errorf("description lacks the margin: %s", tl.Description())
	}
}
