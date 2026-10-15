package agent

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// pipelineMapWithTopics is pipeline_map with a data topic, a heartbeat topic
// and a DLQ topic next to one sink table.
func pipelineMapWithTopics() *fakeVerifyTool {
	pm := pipelineMapTool("analytics.orders")
	d := pm.result.Data.(map[string]any)
	d["nodes"] = append(d["nodes"].([]map[string]any),
		map[string]any{"kind": "topic", "name": "pg.public.orders"},
		map[string]any{"kind": "topic", "name": "__debezium-heartbeat.pg"},
		map[string]any{"kind": "dlq_topic", "name": "orders.dlq"})
	return pm
}

// topicOffsetsTool answers kafka_topic_offsets with the given end offsets in
// order (the last repeats). Only the data topic is ever asked: a read of the
// heartbeat topic would be a bug, so it fails the test.
func topicOffsetsTool(t *testing.T, ends ...int64) *fakeVerifyTool {
	var n int
	tool := &fakeVerifyTool{name: "kafka_topic_offsets", schema: map[string]any{
		"type": "object", "properties": map[string]any{"topic": map[string]any{"type": "string"}},
		"additionalProperties": false, "required": []string{"topic"},
	}}
	tool.runFn = func() (tools.Result, error) {
		i := n
		if i >= len(ends) {
			i = len(ends) - 1
		}
		n++
		return tools.Result{Summary: "offsets", Data: map[string]any{"partitions": []map[string]any{{"partition": 0, "end": ends[i]}}}}, nil
	}
	return tool
}

func runTopicCase(t *testing.T, slots, topics *fakeVerifyTool) []string {
	t.Helper()
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 1: done")}, waitErr: context.Canceled}
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink,
		[]tools.Tool{&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()},
			pipelineMapWithTopics(), staleWarehouseTool(), slots, topics}, recoveredSteps())
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return messageTexts(sink)
}

func TestHeartbeatMovesSlotButDataTopicsUnchangedIsIdle(t *testing.T) {
	// The slot LSN advances on every read (heartbeat); the data topic does not.
	slots := slotsTool([3]any{"0/5000", "0/4000", 100}, [3]any{"0/5100", "0/4100", 100})
	msgs := runTopicCase(t, slots, topicOffsetsTool(t, 42))
	assertVerdict(t, msgs, true)
	if !strings.Contains(strings.Join(msgs, "\n"), "no new source changes to confirm data flow") {
		t.Fatalf("verdict must say the source was idle, got %q", msgs)
	}
}

func TestDataTopicGrewButNoSinkRowsIsNotRecovered(t *testing.T) {
	still := [3]any{"0/5000", "0/4000", 100}
	msgs := runTopicCase(t, slotsTool(still, still), topicOffsetsTool(t, 42, 50))
	assertVerdict(t, msgs, false)
}
