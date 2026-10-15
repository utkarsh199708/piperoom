package agent

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// slotsTool is pg_replication_slots whose reads return the given
// confirmed_flush / restart LSN and retained-bytes readings in order (the last
// one repeats).
func slotsTool(readings ...[3]any) *fakeVerifyTool {
	var n int
	return &fakeVerifyTool{name: "pg_replication_slots", schema: map[string]any{
		"type": "object", "properties": map[string]any{}, "additionalProperties": false,
	}, runFn: func() (tools.Result, error) {
		i := n
		if i >= len(readings) {
			i = len(readings) - 1
		}
		n++
		r := readings[i]
		return tools.Result{Summary: "slot s", Data: []map[string]any{{
			"name": "s", "active": true, "confirmed_flush_lsn": r[0], "restart_lsn": r[1], "retained_wal_bytes": r[2],
		}}}, nil
	}}
}

func runIdleCase(t *testing.T, slots *fakeVerifyTool, layerTool *fakeVerifyTool) (*fakeSink, []string) {
	t.Helper()
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 1: done")}, waitErr: context.Canceled}
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink,
		[]tools.Tool{layerTool, pipelineMapTool("analytics.orders"), staleWarehouseTool(), slots}, recoveredSteps())
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return sink, messageTexts(sink)
}

func TestIdleSourceDataFlowIsInconclusive(t *testing.T) {
	idle := [3]any{"0/5000", "0/4000", 100}
	sink, msgs := runIdleCase(t, slotsTool(idle, idle),
		&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()})
	assertVerdict(t, msgs, true)
	var found bool
	for _, m := range msgs {
		if strings.Contains(m, "no new source changes to confirm data flow") {
			found = true
		}
	}
	if !found {
		t.Fatalf("verdict must say the source was idle, got %q", msgs)
	}
	vs := verifications(t, sink)
	if len(vs) == 0 || vs[0].DataFlow == nil || vs[0].DataFlow.Checked || !strings.Contains(vs[0].DataFlow.Detail, "idle") {
		t.Fatalf("data flow must be unchecked with an idle detail, got %+v", vs)
	}
}

func TestBusySourceStaleWarehouseStaysNotRecovered(t *testing.T) {
	// The source wrote (retained WAL grew) but the slot did not move: real failure.
	_, msgs := runIdleCase(t, slotsTool([3]any{"0/5000", "0/4000", 100}, [3]any{"0/5000", "0/4000", 900}),
		&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()})
	assertVerdict(t, msgs, false)
}

func TestIdleSourceNeverOverridesFailedLayerCheck(t *testing.T) {
	idle := [3]any{"0/5000", "0/4000", 100}
	_, msgs := runIdleCase(t, slotsTool(idle, idle),
		&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectBrokenResult()})
	assertVerdict(t, msgs, false)
}

func TestNoSlotToolStaysNotRecovered(t *testing.T) {
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 1: done")}, waitErr: context.Canceled}
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink,
		[]tools.Tool{&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()},
			pipelineMapTool("analytics.orders"), staleWarehouseTool()}, recoveredSteps())
	if err := r.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertVerdict(t, messageTexts(sink), false)
}
