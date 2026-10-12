package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
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

// unsynced mirrors the real query: the age of the oldest row newer than after.
func (f *fakeSource) unsynced(ctx context.Context, after time.Time) (time.Duration, error) {
	if err := f.block(ctx, "unsynced"); err != nil {
		return 0, err
	}
	var oldest time.Time
	for _, r := range f.rows {
		if r.After(after) && (oldest.IsZero() || r.Before(oldest)) {
			oldest = r
		}
	}
	if oldest.IsZero() {
		return 0, f.errs["unsynced"]
	}
	return testBase.Sub(oldest), f.errs["unsynced"]
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
	for _, hang := range []string{"connector", "group"} {
		t.Run(hang, func(t *testing.T) {
			f := healthyFake(t)
			f.hang = hang
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
			layer := "kafka-connect"
			if hang == "group" {
				layer = "consumer"
			}
			if l := lineFor(t, out, layer); !strings.HasPrefix(l, "FAIL") || !strings.Contains(l, "no answer within 50ms") {
				t.Errorf("layer %s line = %q, want FAIL with timeout", layer, l)
			}
			others := []string{"postgres", "kafka", "warehouse", "consumer", "kafka-connect"}
			for _, o := range others {
				if o == layer {
					continue
				}
				if l := lineFor(t, out, o); !strings.HasPrefix(l, "OK") {
					t.Errorf("layer %s should still report OK: %q", o, l)
				}
			}
		})
	}
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

func TestParseMillis(t *testing.T) {
	zero, err := parseMillis("0")
	if err != nil || !zero.IsZero() {
		t.Fatalf("parseMillis(0) = %v, %v; want zero time", zero, err)
	}
	got, err := parseMillis("1760263200123\n")
	if err != nil {
		t.Fatalf("parseMillis: %v", err)
	}
	if want := time.UnixMilli(1760263200123); !got.Equal(want) {
		t.Fatalf("parseMillis = %v, want %v", got, want)
	}
	if _, err := parseMillis("not-a-number"); err == nil {
		t.Fatal("parseMillis(garbage) err = nil, want error")
	}
}
