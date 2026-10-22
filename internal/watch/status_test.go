package watch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// fakeStatus records the status rows the watcher writes.
type fakeStatus struct {
	mu   sync.Mutex
	rows map[string]clients.WatchStatus
	puts int
	err  error
}

func (f *fakeStatus) PutWatchStatus(ctx context.Context, s clients.WatchStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("no deadline")
	}
	f.puts++
	if f.err != nil {
		return f.err
	}
	if f.rows == nil {
		f.rows = map[string]clients.WatchStatus{}
	}
	f.rows[s.PipelineID] = s
	return nil
}

func (f *fakeStatus) row(id string) (clients.WatchStatus, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rows[id]
	return r, ok
}

func withStatus(t *testing.T, h *harness, f *fakeStatus) {
	t.Helper()
	h.deps.Status = f
	w, err := New(h.deps)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
}

// X5a: one row per watched pipeline per tick, firing then acked, with the
// connector's topology node and the time the incident started.
func TestStatusRowsFollowTheSignals(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "orders"}, {id: "billing"}}})
	fs := &fakeStatus{}
	withStatus(t, h, fs)

	h.step(time.Minute)
	for _, id := range []string{"orders", "billing"} {
		r, ok := fs.row(id)
		if !ok || !r.CheckedAt.Equal(h.clk.Now()) || r.Signals == nil || len(r.Signals) != 0 {
			t.Fatalf("%s healthy row = %+v (%v)", id, r, ok)
		}
	}

	h.breakConnector("orders", "boom")
	start := h.clk.Now().Add(time.Minute)
	h.steps(4, time.Minute) // fires at 3 min
	r, _ := fs.row("orders")
	if len(r.Signals) != 1 {
		t.Fatalf("signals = %+v", r.Signals)
	}
	g := r.Signals[0]
	if g.Signal != owners.SignalConnectorDown || g.State != clients.WatchFiring || g.Node != "connect:pg-orders" ||
		!g.Since.Equal(start) || g.Severity == "" || g.Metric != "" || g.ETA != nil {
		t.Fatalf("connector_down entry = %+v (since want %v)", g, start)
	}
	if b, _ := fs.row("billing"); len(b.Signals) != 0 {
		t.Fatalf("billing caught orders' signal: %+v", b)
	}

	id := h.rooms.created()[0].roomID
	h.rooms.closeRoom(id, h.clk.Now())
	h.w.OnRoomClosed(id, "orders", DedupKey("orders"))
	h.step(time.Minute)
	if r, _ := fs.row("orders"); len(r.Signals) != 1 || r.Signals[0].State != clients.WatchAcked {
		t.Fatalf("after close = %+v, want acked", r.Signals)
	}

	h.fixConnector("orders")
	h.steps(4, time.Minute)
	if r, _ := fs.row("orders"); len(r.Signals) != 0 {
		t.Fatalf("after clear = %+v, want none", r.Signals)
	}
}

// Mode log writes status rows too: the fleet shows health with or without
// watcher rooms.
func TestStatusRowsInModeLog(t *testing.T) {
	h := newHarness(t, opts{mode: config.WatchLog})
	fs := &fakeStatus{}
	withStatus(t, h, fs)
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if r, ok := fs.row("orders"); !ok || len(r.Signals) != 1 {
		t.Fatalf("row = %+v (%v)", r, ok)
	}
}

// A failed write is logged once per tick and never stops it: the room still
// opens.
func TestStatusWriteFailureDoesNotStopTheTick(t *testing.T) {
	h := newHarness(t, opts{})
	fs := &fakeStatus{err: errors.New("db down")}
	withStatus(t, h, fs)
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if got := len(h.rooms.created()); got != 1 {
		t.Fatalf("rooms = %d, want 1", got)
	}
	if fs.puts != 4 {
		t.Fatalf("puts = %d, want one per tick", fs.puts)
	}
	if n := h.logs.count("watch: status rows not written"); n != 4 {
		t.Fatalf("warn lines = %d, want 4\n%s", n, h.logs.String())
	}
}

func TestSignalNodeAndName(t *testing.T) {
	cases := []struct {
		id       string
		ev       map[string]any
		name     string
		wantNode string
	}{
		{owners.SignalConnectorDown, map[string]any{"connectors": []map[string]any{{"name": "c1"}, {"name": "c2"}}}, owners.SignalConnectorDown, "connect:c1"},
		{owners.SignalConnectorDown, map[string]any{"connectors": []any{map[string]any{"name": "c9"}}}, owners.SignalConnectorDown, "connect:c9"},
		{owners.SignalConnectorDown, map[string]any{}, owners.SignalConnectorDown, ""},
		{owners.SignalConfigRisk, map[string]any{"risks": []map[string]any{{"connector": "sink"}}}, owners.SignalConfigRisk, "connect:sink"},
		{owners.SignalDLQGrowth, map[string]any{"topic": "dlq.orders"}, owners.SignalDLQGrowth, "dlq:dlq.orders"},
		{owners.SignalConsumerLag, map[string]any{"group": "g1"}, owners.SignalConsumerLag, "group:g1"},
		{owners.SignalGroupUnstable, map[string]any{"group": "g2"}, owners.SignalGroupUnstable, "group:g2"},
		{owners.SignalOffsetBelowLogStart, map[string]any{"group": "g3"}, owners.SignalOffsetBelowLogStart, "group:g3"},
		{owners.SignalSlotWAL, map[string]any{"slot": "debezium"}, owners.SignalSlotWAL, ""},
		{owners.SignalSlotInactive, map[string]any{"slot": "debezium"}, owners.SignalSlotInactive, ""},
		{driftID(events.SignalCountDrift, "public.orders"), map[string]any{"table": "public.orders"}, events.SignalCountDrift, "pg:public.orders"},
		{driftID(events.SignalRowDrift, "public.orders"), map[string]any{"table": "public.orders"}, events.SignalRowDrift, "pg:public.orders"},
		{driftID(events.SignalDupKeys, "p.public.orders"), map[string]any{"topic": "p.public.orders"}, events.SignalDupKeys, "topic:p.public.orders"},
		{driftID(events.SignalOrderSkew, "p.public.orders"), map[string]any{"topic": "p.public.orders"}, events.SignalOrderSkew, "topic:p.public.orders"},
		{configDriftID("pg-orders"), nil, events.SignalConfigDrift, "connect:pg-orders"},
		{forecastID(clients.WatchMetricSlotWALBytes), map[string]any{"object": "debezium"}, events.SignalForecast, ""},
		{forecastID(clients.WatchMetricTopicOldestUnreadAge), map[string]any{"object": "p.public.orders"}, events.SignalForecast, "topic:p.public.orders"},
	}
	for _, c := range cases {
		if got := signalName(c.id); got != c.name {
			t.Errorf("signalName(%s) = %s, want %s", c.id, got, c.name)
		}
		if got := signalNode(c.id, c.ev); got != c.wantNode {
			t.Errorf("signalNode(%s) = %q, want %q", c.id, got, c.wantNode)
		}
	}
}

// Forecast entries carry their metric and ETA; the list is worst first, then
// oldest, and capped at clients.MaxWatchSignals; an unknown severity falls
// back to the signal's own.
func TestBuildStatusesOrderCapAndForecast(t *testing.T) {
	h := newHarness(t, opts{})
	now := h.clk.Now()
	eta := now.Add(20 * time.Hour).UTC().Truncate(time.Second)
	h.w.mu.Lock()
	ps := h.w.pipe("orders")
	fc := ps.sig(forecastID(clients.WatchMetricSlotWALBytes))
	fc.phase, fc.breachingSince = phaseFiring, now.Add(-time.Hour)
	fc.last = reading{sev: "sev4", ev: map[string]any{"metric": "slot_wal_bytes", "eta": eta.Format(time.RFC3339)}}
	cd := ps.sig(configDriftID("c0"))
	cd.phase, cd.breachingSince, cd.last = phaseAcked, now.Add(-time.Minute), reading{sev: "bogus"}
	for i := range clients.MaxWatchSignals + 5 {
		ss := ps.sig(driftID(events.SignalCountDrift, fmt.Sprintf("public.t%02d", i)))
		ss.phase, ss.breachSev = phaseFiring, "sev2"
		ss.breachingSince = now.Add(-time.Duration(i) * time.Minute)
	}
	ts := h.w.targets(context.Background())
	rows := h.w.buildStatusesLocked(now, ts)
	h.w.mu.Unlock()
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	sigs := rows[0].Signals
	if len(sigs) != clients.MaxWatchSignals {
		t.Fatalf("signals = %d, want the cap %d", len(sigs), clients.MaxWatchSignals)
	}
	// The sev2 drift entries come first, oldest first; the sev3 and sev4 ones
	// fall past the cap.
	if sigs[0].Severity != "sev2" || !strings.HasPrefix(sigs[0].Node, "pg:public.t") || !sigs[0].Since.Equal(now.Add(-36*time.Minute)) {
		t.Fatalf("first = %+v", sigs[0])
	}
	for i := 1; i < len(sigs); i++ {
		if sigs[i].Since.Before(sigs[i-1].Since) && sigs[i].Severity == sigs[i-1].Severity {
			t.Fatalf("not oldest first at %d", i)
		}
	}

	// Without the drift flood the forecast and config_drift entries show.
	h.w.mu.Lock()
	for id := range ps.sigs {
		if driftKind(id) != "" {
			delete(ps.sigs, id)
		}
	}
	rows = h.w.buildStatusesLocked(now, ts)
	h.w.mu.Unlock()
	sigs = rows[0].Signals
	if len(sigs) != 2 || sigs[0].Signal != events.SignalConfigDrift || sigs[0].Severity != "sev3" || sigs[0].State != clients.WatchAcked ||
		sigs[1].Signal != events.SignalForecast || sigs[1].Metric != clients.WatchMetricSlotWALBytes || sigs[1].ETA == nil || !sigs[1].ETA.Equal(eta) {
		t.Fatalf("signals = %+v", sigs)
	}
}
