package watch

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

func connectorRow(t *testing.T, fs *fakeStatus) clients.WatchSignal {
	t.Helper()
	r, ok := fs.row("orders")
	if !ok || len(r.Signals) != 1 || r.Signals[0].Signal != owners.SignalConnectorDown {
		t.Fatalf("row = %+v (%v), want one connector_down entry", r, ok)
	}
	return r.Signals[0]
}

// A firing signal whose condition is gone reports when it was first seen
// gone, and nothing while it still breaches (H1).
func TestStatusClearingSince(t *testing.T) {
	h := newHarness(t, opts{})
	fs := &fakeStatus{}
	withStatus(t, h, fs)

	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	if g := connectorRow(t, fs); g.State != clients.WatchFiring || g.ClearingSince != nil {
		t.Fatalf("breaching entry = %+v, want firing and no clearing_since", g)
	}

	// Condition gone: the clear timer starts at the first clear reading and
	// does not move on later ticks; the signal stays firing meanwhile.
	h.fixConnector("orders")
	h.step(time.Minute)
	first := h.clk.Now()
	g := connectorRow(t, fs)
	if g.State != clients.WatchFiring || g.ClearingSince == nil || !g.ClearingSince.Equal(first) {
		t.Fatalf("first clear tick = %+v, want firing, clearing_since %v", g, first)
	}
	h.step(time.Minute)
	g = connectorRow(t, fs)
	if g.State != clients.WatchFiring || g.ClearingSince == nil || !g.ClearingSince.Equal(first) {
		t.Fatalf("second clear tick = %+v, want clearing_since to stay %v", g, first)
	}

	// An unknown reading (status call fails) changes nothing: not reported as
	// clearing from scratch, and the running timer is not reset.
	h.conn.setStatusErr("pg-orders", errors.New("connect down"))
	h.step(30 * time.Second)
	g = connectorRow(t, fs)
	if g.ClearingSince == nil || !g.ClearingSince.Equal(first) {
		t.Fatalf("after an unknown reading = %+v, want clearing_since still %v", g, first)
	}
	h.conn.setStatusErr("pg-orders", nil)

	// The condition returns before `for` ends: back to nil, still firing.
	h.breakConnector("orders", "boom")
	h.step(30 * time.Second)
	if g := connectorRow(t, fs); g.State != clients.WatchFiring || g.ClearingSince != nil {
		t.Fatalf("breach returned = %+v, want firing and no clearing_since", g)
	}
}

// A signal never seen clearing is not clearing just because an unknown
// reading arrives.
func TestStatusClearingSinceUnknownWhileBreaching(t *testing.T) {
	h := newHarness(t, opts{})
	fs := &fakeStatus{}
	withStatus(t, h, fs)
	h.breakConnector("orders", "boom")
	h.steps(4, time.Minute)
	h.conn.setStatusErr("pg-orders", errors.New("connect down"))
	h.steps(2, time.Minute)
	if g := connectorRow(t, fs); g.ClearingSince != nil {
		t.Fatalf("unknown reading while breaching = %+v, want no clearing_since", g)
	}
}

// An acked signal that clears reports it too.
func TestStatusClearingSinceAcked(t *testing.T) {
	h := newHarness(t, opts{})
	fs := &fakeStatus{}
	withStatus(t, h, fs)
	failedAndClosed(t, h)
	h.step(time.Minute)
	if g := connectorRow(t, fs); g.State != clients.WatchAcked || g.ClearingSince != nil {
		t.Fatalf("acked and breaching = %+v", g)
	}
	h.fixConnector("orders")
	h.step(time.Minute)
	g := connectorRow(t, fs)
	if g.State != clients.WatchAcked || g.ClearingSince == nil || !g.ClearingSince.Equal(h.clk.Now()) {
		t.Fatalf("acked and clearing = %+v, want clearing_since %v", g, h.clk.Now())
	}
}

func TestWatchSignalClearingSinceJSON(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	b, _ := json.Marshal(clients.WatchSignal{Signal: "connector_down", Since: now})
	if strings.Contains(string(b), "clearing_since") {
		t.Fatalf("nil clearing_since not omitted: %s", b)
	}
	in := clients.WatchSignal{Signal: "connector_down", Since: now, ClearingSince: &now}
	b, _ = json.Marshal(in)
	var out clients.WatchSignal
	if err := json.Unmarshal(b, &out); err != nil || out.ClearingSince == nil || !out.ClearingSince.Equal(now) {
		t.Fatalf("round trip = %+v (%v) from %s", out, err, b)
	}
}
