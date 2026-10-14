package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeSource answers every health read from fields. rows is the updated_at of
// every source row, so unsynced behaves like the real query. hang names one
// method that blocks until its context ends, to exercise the per-layer timeout;
// errs maps a method name to the error it returns.
type fakeSource struct {
	slotState slotHealth
	conn      connectorStatus
	topic     time.Time   // newest topic record; zero = no records
	warehouse time.Time   // newest analytics.orders updated_at; zero = empty
	rows      []time.Time // updated_at of every source row
	grp       groupHealth
	errs      map[string]error
	hang      string
}

var testBase = time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)

// connStatus builds a Connect status document with the given connector and task states.
func connStatus(t testing.TB, connState string, taskStates ...string) connectorStatus {
	t.Helper()
	var tasks []string
	for i, s := range taskStates {
		tasks = append(tasks, fmt.Sprintf(`{"id":%d,"state":%q}`, i, s))
	}
	doc := fmt.Sprintf(`{"connector":{"state":%q},"tasks":[%s]}`, connState, strings.Join(tasks, ","))
	var st connectorStatus
	if err := json.Unmarshal([]byte(doc), &st); err != nil {
		t.Fatalf("decode connector status: %v", err)
	}
	return st
}

// healthyFake is a pipeline that passes every layer.
func healthyFake(t testing.TB) *fakeSource {
	return &fakeSource{
		slotState: slotHealth{Exists: true, Active: true, RetainedBytes: 26 << 10},
		conn:      connStatus(t, "RUNNING", "RUNNING"),
		// One change is in flight (0.1 s old, not yet delivered): still healthy.
		topic:     testBase.Add(-1 * time.Second),
		warehouse: testBase.Add(-1 * time.Second),
		rows:      []time.Time{testBase.Add(-1 * time.Second), testBase.Add(-100 * time.Millisecond)},
		grp:       groupHealth{State: "Stable", Members: 1, Lag: 0},
	}
}

func (f *fakeSource) block(ctx context.Context, method string) error {
	if f.hang == method {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.errs[method]
}

func (f *fakeSource) slot(ctx context.Context) (slotHealth, error) {
	if err := f.block(ctx, "slot"); err != nil {
		return slotHealth{}, err
	}
	return f.slotState, f.errs["slot"]
}

func (f *fakeSource) connector(ctx context.Context) (connectorStatus, error) {
	if err := f.block(ctx, "connector"); err != nil {
		return connectorStatus{}, err
	}
	return f.conn, f.errs["connector"]
}

func (f *fakeSource) topicNewest(ctx context.Context, _ string) (time.Time, error) {
	if err := f.block(ctx, "topicNewest"); err != nil {
		return time.Time{}, err
	}
	return f.topic, f.errs["topicNewest"]
}

func (f *fakeSource) warehouseNewest(ctx context.Context) (time.Time, error) {
	if err := f.block(ctx, "warehouseNewest"); err != nil {
		return time.Time{}, err
	}
	return f.warehouse, f.errs["warehouseNewest"]
}

// unsynced mirrors the real query: the age of the oldest row with updated_at >= from.
func (f *fakeSource) unsynced(ctx context.Context, from time.Time) (time.Duration, error) {
	if err := f.block(ctx, "unsynced"); err != nil {
		return 0, err
	}
	if err := f.errs["unsynced"]; err != nil {
		return 0, err
	}
	var oldest time.Time
	for _, r := range f.rows {
		if (from.IsZero() || !r.Before(from)) && (oldest.IsZero() || r.Before(oldest)) {
			oldest = r
		}
	}
	if oldest.IsZero() {
		return 0, nil
	}
	return testBase.Sub(oldest), nil
}

func (f *fakeSource) group(ctx context.Context, _ string) (groupHealth, error) {
	if err := f.block(ctx, "group"); err != nil {
		return groupHealth{}, err
	}
	return f.grp, f.errs["group"]
}

// setTask replaces task 0's state in the fake connector status.
func (f *fakeSource) setTask(state string) {
	f.conn.Tasks[0].State = state
}

// dropTasks makes the fake connector report no tasks at all.
func (f *fakeSource) dropTasks() {
	f.conn.Tasks = nil
}

var testPlan = healthPlan{
	Topic:  "pg.public.orders",
	Group:  "order-sync",
	Owners: map[string]string{"postgres": "priya", "kafka-connect": "arjun", "kafka": "arjun", "consumer": "kabir", "warehouse": "meera"},
}

// runFake runs the health report against src and returns its output and error.
func runFake(t *testing.T, src healthSource, plan healthPlan) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	err := runHealth(context.Background(), healthChecks(src, plan), plan, &buf)
	return buf.String(), err
}

// lineFor returns the report line of one layer ("OK|FAIL <layer> <detail>"), or fails the test.
func lineFor(t *testing.T, out, layer string) string {
	t.Helper()
	for _, l := range strings.Split(out, "\n") {
		if f := strings.Fields(l); len(f) > 1 && f[1] == layer {
			return l
		}
	}
	t.Fatalf("no report line for layer %q in:\n%s", layer, out)
	return ""
}

func TestHealthAllHealthyExitsZero(t *testing.T) {
	out, err := runFake(t, healthyFake(t), testPlan)
	if err != nil {
		t.Fatalf("runHealth err = %v, want nil\n%s", err, out)
	}
	for _, layer := range []string{"postgres", "kafka-connect", "kafka", "consumer", "warehouse"} {
		if l := lineFor(t, out, layer); !strings.HasPrefix(l, "OK") {
			t.Errorf("layer %s not OK: %q", layer, l)
		}
	}
	if !strings.Contains(out, "pipeline healthy: 5/5 layers OK") {
		t.Errorf("missing healthy summary:\n%s", out)
	}
}

func TestHealthFailures(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(f *fakeSource)
		wantLayer string
		wantText  string
	}{
		{
			name:      "slot inactive",
			mutate:    func(f *fakeSource) { f.slotState.Active = false },
			wantLayer: "postgres",
			wantText:  "active=false",
		},
		{
			name:      "slot missing",
			mutate:    func(f *fakeSource) { f.slotState = slotHealth{} },
			wantLayer: "postgres",
			wantText:  "missing",
		},
		{
			name:      "retained WAL over limit",
			mutate:    func(f *fakeSource) { f.slotState.RetainedBytes = maxRetainedWAL + 1 },
			wantLayer: "postgres",
			wantText:  "limit 100.0 MB",
		},
		{
			name:      "task FAILED",
			mutate:    func(f *fakeSource) { f.setTask("FAILED") },
			wantLayer: "kafka-connect",
			wantText:  "tasks 0=FAILED",
		},
		{
			name:      "connector STOPPED with no tasks",
			mutate:    func(f *fakeSource) { f.conn.Connector.State = "STOPPED"; f.dropTasks() },
			wantLayer: "kafka-connect",
			wantText:  "STOPPED, tasks none",
		},
		{
			name:      "consumer lag over threshold",
			mutate:    func(f *fakeSource) { f.grp.Lag = maxConsumerLag + 1 },
			wantLayer: "consumer",
			wantText:  "total lag 101",
		},
		{
			name:      "consumer with no members",
			mutate:    func(f *fakeSource) { f.grp = groupHealth{State: "Empty", Members: 0, Lag: 1471} },
			wantLayer: "consumer",
			wantText:  "state Empty, members 0",
		},
		{
			// The consumer stopped: a change from 100 s ago never reached the warehouse.
			name: "warehouse stale",
			mutate: func(f *fakeSource) {
				f.warehouse = testBase.Add(-120 * time.Second)
				f.rows = []time.Time{f.warehouse, testBase.Add(-100 * time.Second)}
			},
			wantLayer: "warehouse",
			wantText:  "oldest unloaded change 100 s",
		},
		{
			// A single burst was never delivered and the source went idle since.
			// Newest-row lag would look fine here; the oldest pending change does not.
			name: "single burst never delivered, then idle",
			mutate: func(f *fakeSource) {
				f.warehouse = testBase.Add(-200 * time.Second)
				f.topic = testBase.Add(-200 * time.Second)
				f.rows = []time.Time{f.warehouse, testBase.Add(-90 * time.Second)}
			},
			wantLayer: "warehouse",
			wantText:  "oldest unloaded change 90 s",
		},
		{
			name:      "warehouse empty while source has rows",
			mutate:    func(f *fakeSource) { f.warehouse = time.Time{}; f.rows = []time.Time{testBase.Add(-90 * time.Second)} },
			wantLayer: "warehouse",
			wantText:  "empty, oldest unloaded change 90 s",
		},
		{
			name: "topic stopped while source keeps changing",
			mutate: func(f *fakeSource) {
				f.topic = testBase.Add(-120 * time.Second)
				f.rows = []time.Time{f.topic, testBase.Add(-70 * time.Second), testBase.Add(-1 * time.Second)}
			},
			wantLayer: "kafka",
			wantText:  "oldest unsent change 70 s",
		},
		{
			name:      "topic has no records while source has rows",
			mutate:    func(f *fakeSource) { f.topic = time.Time{}; f.rows = []time.Time{testBase.Add(-90 * time.Second)} },
			wantLayer: "kafka",
			wantText:  "no records, oldest unsent change 90 s",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := healthyFake(t)
			tt.mutate(f)
			out, err := runFake(t, f, testPlan)
			if !errors.Is(err, errUnhealthy) {
				t.Fatalf("runHealth err = %v, want errUnhealthy\n%s", err, out)
			}
			line := lineFor(t, out, tt.wantLayer)
			if !strings.HasPrefix(line, "FAIL") {
				t.Errorf("layer %s should FAIL: %q", tt.wantLayer, line)
			}
			if !strings.Contains(line, tt.wantText) {
				t.Errorf("layer %s line %q does not contain %q", tt.wantLayer, line, tt.wantText)
			}
			summary := out[strings.LastIndex(out, "pipeline UNHEALTHY"):]
			if !strings.Contains(summary, tt.wantLayer+" (") {
				t.Errorf("summary does not name %s with owner:\n%s", tt.wantLayer, out)
			}
		})
	}
}

func TestHealthThresholdBoundariesPass(t *testing.T) {
	f := healthyFake(t)
	f.grp.Lag = maxConsumerLag
	f.slotState.RetainedBytes = maxRetainedWAL
	// The oldest pending change is exactly at the limit for both sinks.
	f.warehouse = testBase.Add(-200 * time.Second)
	f.topic = testBase.Add(-200 * time.Second)
	f.rows = []time.Time{f.warehouse, testBase.Add(-maxSyncLag)}
	out, err := runFake(t, f, testPlan)
	if err != nil {
		t.Fatalf("values at the limit must pass, err = %v\n%s", err, out)
	}
}

func TestHealthIdleStackIsHealthy(t *testing.T) {
	// No traffic for an hour: every row is already in the topic and the warehouse.
	f := healthyFake(t)
	old := testBase.Add(-time.Hour)
	f.rows = []time.Time{old}
	f.topic = old
	f.warehouse = old
	out, err := runFake(t, f, testPlan)
	if err != nil {
		t.Fatalf("idle pipeline must be healthy, err = %v\n%s", err, out)
	}
}

func TestHealthOneErrorDoesNotStopOthers(t *testing.T) {
	f := healthyFake(t)
	f.errs = map[string]error{"slot": errors.New("psql: connection refused")}
	out, err := runFake(t, f, testPlan)
	if !errors.Is(err, errUnhealthy) {
		t.Fatalf("runHealth err = %v, want errUnhealthy", err)
	}
	if l := lineFor(t, out, "postgres"); !strings.HasPrefix(l, "FAIL") || !strings.Contains(l, "error: psql: connection refused") {
		t.Errorf("postgres line = %q, want FAIL with the error", l)
	}
	for _, layer := range []string{"kafka-connect", "kafka", "consumer", "warehouse"} {
		if l := lineFor(t, out, layer); !strings.HasPrefix(l, "OK") {
			t.Errorf("layer %s should still report OK: %q", layer, l)
		}
	}
	if !strings.Contains(out, "pipeline UNHEALTHY: postgres (priya)") {
		t.Errorf("summary does not name postgres:\n%s", out)
	}
}

func TestHealthTimeoutDoesNotStopOthers(t *testing.T) {
	allLayers := []string{"postgres", "kafka-connect", "kafka", "consumer", "warehouse"}
	tests := []struct {
		hang  string // fake method that never answers
		layer string // layer that must time out
	}{
		{"connector", "kafka-connect"},
		{"group", "consumer"},
		{"topicNewest", "kafka"},
		{"warehouseNewest", "warehouse"},
	}
	for _, tt := range tests {
		t.Run(tt.hang, func(t *testing.T) {
			f := healthyFake(t)
			f.hang = tt.hang
			plan := testPlan
			plan.Timeout = 50 * time.Millisecond

			start := time.Now()
			out, err := runFake(t, f, plan)
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("report took %s; a hung check must not hold up the report", elapsed)
			}
			if !errors.Is(err, errUnhealthy) {
				t.Fatalf("runHealth err = %v, want errUnhealthy", err)
			}
			if l := lineFor(t, out, tt.layer); !strings.HasPrefix(l, "FAIL") || !strings.Contains(l, "no answer within 50ms") {
				t.Errorf("layer %s line = %q, want FAIL with timeout", tt.layer, l)
			}
			for _, o := range allLayers {
				if o == tt.layer {
					continue
				}
				if l := lineFor(t, out, o); !strings.HasPrefix(l, "OK") {
					t.Errorf("layer %s should still report OK: %q", o, l)
				}
			}
		})
	}
}

func TestHealthErrorIsolation(t *testing.T) {
	allLayers := []string{"postgres", "kafka-connect", "kafka", "consumer", "warehouse"}
	tests := []struct {
		name   string
		errKey string   // fake method that returns an error
		layers []string // layers that must FAIL with the error
	}{
		// unsynced feeds both the kafka and the warehouse layer.
		{"unsynced", "unsynced", []string{"kafka", "warehouse"}},
		{"topicNewest", "topicNewest", []string{"kafka"}},
		{"warehouseNewest", "warehouseNewest", []string{"warehouse"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := healthyFake(t)
			f.errs = map[string]error{tt.errKey: errors.New("boom " + tt.errKey)}
			out, err := runFake(t, f, testPlan)
			if !errors.Is(err, errUnhealthy) {
				t.Fatalf("runHealth err = %v, want errUnhealthy", err)
			}
			failing := map[string]bool{}
			for _, l := range tt.layers {
				failing[l] = true
				if line := lineFor(t, out, l); !strings.HasPrefix(line, "FAIL") || !strings.Contains(line, "error: boom "+tt.errKey) {
					t.Errorf("layer %s line = %q, want FAIL with the error", l, line)
				}
			}
			for _, l := range allLayers {
				if failing[l] {
					continue
				}
				if line := lineFor(t, out, l); !strings.HasPrefix(line, "OK") {
					t.Errorf("layer %s should still report OK: %q", l, line)
				}
			}
		})
	}
}

// TestRunCheckCancelledParent: a parent cancelled by Ctrl+C must not be reported
// as a slow layer.
func TestRunCheckCancelledParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := healthCheck{layer: "consumer", run: func(ctx context.Context) (bool, string, error) {
		<-ctx.Done()
		return false, "", ctx.Err()
	}}
	got := runCheck(ctx, c, time.Second)
	if got.OK || got.Line != "cancelled" {
		t.Fatalf("runCheck with cancelled parent = %+v, want FAIL cancelled", got)
	}
}

// TestPendingFrom covers the boundary math that decides whether a row is still
// pending. Exact-microsecond rows must not look pending once they are loaded.
func TestPendingFrom(t *testing.T) {
	// 08:30:26.045449 is the value from the live false alarm: loaded, not pending.
	loaded := time.Date(2026, 10, 12, 8, 30, 26, 45449000, time.UTC)
	if got := warehousePendingFrom(loaded); got.Sub(loaded) != time.Microsecond {
		t.Fatalf("warehousePendingFrom(%v) = %v, want loaded+1us", loaded, got)
	}
	if got := warehousePendingFrom(time.Time{}); !got.IsZero() {
		t.Fatalf("warehousePendingFrom(zero) = %v, want zero (all rows)", got)
	}

	// Kafka timestamps are milliseconds: a row 400us after the newest record is in
	// the same millisecond, so it counts as delivered.
	topic := time.Date(2026, 10, 12, 8, 30, 26, 45000000, time.UTC)
	if got := topicPendingFrom(topic.Add(400 * time.Microsecond)); !got.Equal(topic.Add(time.Millisecond)) {
		t.Fatalf("topicPendingFrom(ms+400us) = %v, want the next millisecond", got)
	}
	if got := topicPendingFrom(time.Time{}); !got.IsZero() {
		t.Fatalf("topicPendingFrom(zero) = %v, want zero (all rows)", got)
	}
}

func TestPendingWhere(t *testing.T) {
	tests := []struct {
		name string
		from time.Time
		want string
	}{
		{"zero counts every row", time.Time{}, "TRUE"},
		{"microseconds kept", time.Date(2026, 10, 12, 8, 30, 26, 45450000, time.UTC),
			"updated_at >= '2026-10-12 08:30:26.045450+00'::timestamptz"},
		{"local zone converted to UTC", time.Date(2026, 10, 12, 14, 0, 26, 0, time.FixedZone("IST", 5*3600+1800)),
			"updated_at >= '2026-10-12 08:30:26.000000+00'::timestamptz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pendingWhere(tt.from); got != tt.want {
				t.Fatalf("pendingWhere = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLastRecordOffsets(t *testing.T) {
	ends := kadm.ListedOffsets{"pg.public.orders": {
		0: {Topic: "pg.public.orders", Partition: 0, Offset: 0},  // empty partition: excluded
		1: {Topic: "pg.public.orders", Partition: 1, Offset: 1},  // one record: last is offset 0
		2: {Topic: "pg.public.orders", Partition: 2, Offset: 42}, // last record is 41
	}}
	got, err := lastRecordOffsets(ends)
	if err != nil {
		t.Fatalf("lastRecordOffsets: %v", err)
	}
	want := map[int32]kgo.Offset{1: kgo.NewOffset().At(0), 2: kgo.NewOffset().At(41)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lastRecordOffsets = %v, want %v", got, want)
	}

	// A partition that could not be listed (the -1 entry for an unknown topic) is an error.
	bad := kadm.ListedOffsets{"nope": {-1: {Topic: "nope", Partition: -1, Offset: -1, Err: errors.New("unknown topic")}}}
	if _, err := lastRecordOffsets(bad); err == nil {
		t.Fatal("lastRecordOffsets with a listing error = nil, want error")
	}

	none, err := lastRecordOffsets(kadm.ListedOffsets{"t": {0: {Topic: "t", Offset: 0}}})
	if err != nil || len(none) != 0 {
		t.Fatalf("empty topic: got %v, %v; want no offsets and no error", none, err)
	}
}

func TestSettleHealth(t *testing.T) {
	ctx := context.Background()

	t.Run("settles to healthy and prints the last report", func(t *testing.T) {
		calls := 0
		run := func(w io.Writer) error {
			calls++
			fmt.Fprintf(w, "run %d\n", calls)
			if calls < 3 {
				return errUnhealthy
			}
			return nil
		}
		var out bytes.Buffer
		if err := settleHealth(ctx, run, time.Millisecond, time.Second, &out); err != nil {
			t.Fatalf("settleHealth err = %v, want nil", err)
		}
		if calls != 3 {
			t.Fatalf("calls = %d, want 3", calls)
		}
		if got := out.String(); got != "run 3\n" {
			t.Fatalf("printed %q, want only the last report", got)
		}
	})

	t.Run("gives up at the limit and reports unhealthy", func(t *testing.T) {
		var out bytes.Buffer
		err := settleHealth(ctx, func(w io.Writer) error { fmt.Fprint(w, "still bad\n"); return errUnhealthy }, time.Millisecond, 20*time.Millisecond, &out)
		if !errors.Is(err, errUnhealthy) {
			t.Fatalf("settleHealth err = %v, want errUnhealthy", err)
		}
		if out.String() != "still bad\n" {
			t.Fatalf("printed %q, want the last report", out.String())
		}
	})

	t.Run("stops at once on a run that could not happen", func(t *testing.T) {
		calls := 0
		boom := errors.New("cannot read owners")
		err := settleHealth(ctx, func(io.Writer) error { calls++; return boom }, time.Millisecond, time.Second, io.Discard)
		if !errors.Is(err, boom) || calls != 1 {
			t.Fatalf("err = %v calls = %d, want boom after 1 call", err, calls)
		}
	})
}

func TestParseSlotRow(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    slotHealth
		wantErr bool
	}{
		{"active", "active 26624", slotHealth{Exists: true, Active: true, RetainedBytes: 26624}, false},
		{"inactive", "inactive 1332000", slotHealth{Exists: true, Active: false, RetainedBytes: 1332000}, false},
		{"unknown restart_lsn", "active -1", slotHealth{Exists: true, Active: true, RetainedBytes: -1}, false},
		{"no slot", "", slotHealth{}, false},
		{"garbage", "active", slotHealth{}, true},
		{"bad number", "active x", slotHealth{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSlotRow(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSlotRow(%q) err = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("parseSlotRow(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseEpochMicros(t *testing.T) {
	zero, err := parseEpochMicros("0")
	if err != nil || !zero.IsZero() {
		t.Fatalf("parseEpochMicros(0) = %v, %v; want zero time", zero, err)
	}
	// 2026-10-12 08:30:26.045449 UTC, the live false-alarm value, exact to the microsecond.
	want := time.Date(2026, 10, 12, 8, 30, 26, 45449000, time.UTC)
	got, err := parseEpochMicros(fmt.Sprintf("%d\n", want.UnixMicro()))
	if err != nil {
		t.Fatalf("parseEpochMicros: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("parseEpochMicros = %v, want %v", got, want)
	}
	if _, err := parseEpochMicros("not-a-number"); err == nil {
		t.Fatal("parseEpochMicros(garbage) err = nil, want error")
	}
}

// TestHealthLoadedMicrosecondRowsNotPending is the live false alarm: the source and
// the warehouse hold the same exact-microsecond newest row, so nothing is pending.
func TestHealthLoadedMicrosecondRowsNotPending(t *testing.T) {
	f := healthyFake(t)
	newest := time.Date(2026, 10, 12, 8, 30, 26, 45449000, time.UTC)
	f.rows = []time.Time{newest}
	f.warehouse = newest
	f.topic = newest.Truncate(time.Millisecond)
	out, err := runFake(t, f, testPlan)
	if err != nil {
		t.Fatalf("loaded rows must not be pending, err = %v\n%s", err, out)
	}
}

// TestHealthOneMicrosecondAfterWarehouseIsPending: a row 1 µs newer than the newest
// loaded row is pending, so the warehouse layer ages it; the row at exactly the
// newest loaded time is not.
func TestHealthOneMicrosecondAfterWarehouseIsPending(t *testing.T) {
	loaded := testBase.Add(-200 * time.Second)
	tests := []struct {
		name     string
		rows     []time.Time
		wantOK   bool
		wantText string
	}{
		{"row at the newest loaded time", []time.Time{loaded}, true, ""},
		{"row 1 us after the newest loaded time", []time.Time{loaded, loaded.Add(time.Microsecond)}, false, "oldest unloaded change 200 s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := healthyFake(t)
			f.warehouse = loaded
			f.topic = loaded
			f.rows = tt.rows
			out, _ := runFake(t, f, testPlan)
			line := lineFor(t, out, "warehouse")
			if ok := strings.HasPrefix(line, "OK"); ok != tt.wantOK {
				t.Fatalf("warehouse OK = %v, want %v: %q", ok, tt.wantOK, line)
			}
			if tt.wantText != "" && !strings.Contains(line, tt.wantText) {
				t.Fatalf("warehouse line %q does not contain %q", line, tt.wantText)
			}
		})
	}
}

func TestExitCode(t *testing.T) {
	failed := errors.New("boom")
	tests := []struct {
		cmd  string
		err  error
		want int
	}{
		{"health", failed, 2}, // setup errors and cancelled runs
		{"status", failed, 1},
		{"demo", failed, 1},
		{"break", failed, 1},
		{"break", usagef("unknown scenario %q", "x"), 2},
		{"up", usagef("usage: devctl up"), 2},
		{"frobnicate", failed, 2}, // e.g. newStack failed outside the repo
		{"", failed, 2},
	}
	for _, tt := range tests {
		if got := exitCode(tt.cmd, tt.err); got != tt.want {
			t.Errorf("exitCode(%q, %v) = %d, want %d", tt.cmd, tt.err, got, tt.want)
		}
	}
	// runCommand's own unknown-command error is a usage error.
	err := runCommand(context.Background(), &stack{}, "frobnicate", nil)
	if got := exitCode("frobnicate", err); got != 2 {
		t.Errorf("unknown command exits %d, want 2", got)
	}
}
