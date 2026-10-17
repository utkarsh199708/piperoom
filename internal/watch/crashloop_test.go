package watch

import (
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// taskFailed is a connector that reads RUNNING while its task is FAILED: the
// shape of a task that Connect (not a person) restarts.
func taskFailed(name, trace string) clients.ConnectorStatus {
	return clients.ConnectorStatus{Name: name, State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}}
}

// Q5 T10: three break/fix cycles of a person (connector FAILED, then restarted
// to RUNNING) in 15 minutes are fixes, not a crash loop.
func TestHumanFixesDoNotCountAsCrashLoop(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0)
	for range 3 {
		h.breakConnector("orders", "boom")
		h.step(time.Minute)
		h.fixConnector("orders")
		h.step(time.Minute)
	}
	if h.phase("orders", owners.SignalConnectorDown) != phaseOK || len(h.rooms.created()) != 0 {
		t.Fatal("human break/fix cycles fired a crash-loop")
	}
	if n := restarts(h.w.pipes["orders"].taskHist["pg-orders/0"].events, h.clk.Now().Add(-15*time.Minute)); n != 0 {
		t.Fatalf("restarts counted = %d, want 0", n)
	}
}

// A task restarted by Connect itself (the connector stays RUNNING) still counts.
func TestTaskRestartsByConnectStillCount(t *testing.T) {
	h := newHarness(t, opts{})
	h.step(0)
	for range 3 {
		h.conn.setStatus("pg-orders", taskFailed("pg-orders", "boom"))
		h.step(time.Minute)
		h.fixConnector("orders")
		h.step(time.Minute)
	}
	if n := restarts(h.w.pipes["orders"].taskHist["pg-orders/0"].events, h.clk.Now().Add(-15*time.Minute)); n != 3 {
		t.Fatalf("restarts counted = %d, want 3", n)
	}
}
