package agent

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// tableActivityTool answers pg_table_activity with the given n_tup_ins readings
// of public.orders in order (the last repeats). A negative reading fails.
func tableActivityTool(ins ...int64) *fakeVerifyTool {
	var n int
	return &fakeVerifyTool{name: "pg_table_activity", schema: map[string]any{
		"type": "object", "properties": map[string]any{}, "additionalProperties": false,
	}, runFn: func() (tools.Result, error) {
		i := n
		if i >= len(ins) {
			i = len(ins) - 1
		}
		n++
		if ins[i] < 0 {
			return tools.Result{}, context.DeadlineExceeded
		}
		return tools.Result{Summary: "public.orders", Data: map[string]any{"tables": []map[string]any{{
			"table": "public.orders", "found": true, "n_tup_ins": ins[i], "n_tup_upd": 5, "n_tup_del": 0, "n_live_tup": 100,
		}}}}, nil
	}}
}

func runTablesCase(t *testing.T, extra ...tools.Tool) (*fakeSink, []string) {
	sink, msgs, _ := runTablesCaseModel(t, extra...)
	return sink, msgs
}

// runTablesCaseModel is runTablesCase that also returns what the model read last.
func runTablesCaseModel(t *testing.T, extra ...tools.Tool) (*fakeSink, []string, string) {
	t.Helper()
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 1: done")}, waitErr: context.Canceled}
	base := []tools.Tool{&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()},
		pipelineMapWithTopics(), staleWarehouseTool()}
	model := &scriptedModel{}
	r := newVerifyRunner(t, model, inbox, sink, append(base, extra...), recoveredSteps())
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return sink, messageTexts(sink), requestText(model.calls[len(model.calls)-1])
}

// W1: the pipeline's tables did not change, so no rows could be expected: the
// layers decide, even when a slot's retained WAL grew (other tables wrote).
func TestUnchangedTablesAreIdle(t *testing.T) {
	still := [3]any{"0/5000", "0/4000", 100}
	grew := [3]any{"0/5000", "0/4000", 900}
	sink, msgs := runTablesCase(t, tableActivityTool(40, 40), slotsTool(still, grew), topicOffsetsTool(t, 42))
	assertVerdict(t, msgs, true)
	vs := verifications(t, sink)
	if len(vs) != 1 || vs[0].DataFlow == nil || vs[0].DataFlow.Checked || !strings.Contains(vs[0].DataFlow.Detail, "tables") {
		t.Fatalf("data flow must be inconclusive because the tables were idle, got %+v", vs)
	}
}

// W1: the tables changed but nothing reached the topics: the connector is not
// capturing. NOT recovered, even when the slots look idle.
func TestChangedTablesWithoutTopicRecordsIsNotRecovered(t *testing.T) {
	still := [3]any{"0/5000", "0/4000", 100}
	_, msgs, note := runTablesCaseModel(t, tableActivityTool(40, 47), slotsTool(still, still), topicOffsetsTool(t, 42))
	assertVerdict(t, msgs, false)
	if !strings.Contains(note, "public.orders: +7 inserts") {
		t.Fatalf("the evidence must name the table that changed, got %q", note)
	}
}

// W1: changed tables and grown topics without sink rows stay NOT recovered.
func TestChangedTablesAndTopicsWithoutSinkRowsIsNotRecovered(t *testing.T) {
	still := [3]any{"0/5000", "0/4000", 100}
	_, msgs := runTablesCase(t, tableActivityTool(40, 47), slotsTool(still, still), topicOffsetsTool(t, 42, 50))
	assertVerdict(t, msgs, false)
}

// W1: a failing table read falls back to the slot and topic rule.
func TestTableActivityErrorFallsBackToSlots(t *testing.T) {
	idle := [3]any{"0/5000", "0/4000", 100}
	sink, msgs := runTablesCase(t, tableActivityTool(-1), slotsTool(idle, idle), topicOffsetsTool(t, 42))
	assertVerdict(t, msgs, true)
	vs := verifications(t, sink)
	if len(vs) != 1 || !strings.Contains(vs[0].DataFlow.Detail, "replication slots") {
		t.Fatalf("the slot rule must decide, got %+v", vs)
	}
}

// W1: the counters taken when the fix was proposed are the "before" reading.
func TestTableActivitySnapshotAtProposal(t *testing.T) {
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 1: done")}, waitErr: context.Canceled}
	still := [3]any{"0/5000", "0/4000", 100}
	acts := tableActivityTool(10, 40, 40) // proposal, then the check reads
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink, []tools.Tool{
		&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()},
		pipelineMapWithTopics(), staleWarehouseTool(), slotsTool(still, still), topicOffsetsTool(t, 42), acts}, recoveredSteps())
	// The proposal is made mid-run, after the tool list exists.
	r.toolList = buildToolList(r.deps.Tools, newRoomTools(r.deps, r.proposals))
	r.snapshotBaseline(context.Background(), "p_1")
	if rec, _ := r.proposals.get("p_1"); len(rec.tables) != 1 {
		t.Fatalf("the proposal must keep the table counters, got %+v", rec.tables)
	}
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// 10 at proposal, 40 at the check: the tables changed since the fix.
	assertVerdict(t, messageTexts(sink), false)
}

// T11: the room sees a sentence for people; the model alone gets the guidance.
func TestCompatFollowUpRoomTextHasNoModelGuidance(t *testing.T) {
	steps := compatSteps("NONE")
	room := compatFollowUpRoom(steps)
	if !strings.Contains(room, "follow-up: "+compatSubject+" compatibility is still NONE") {
		t.Fatalf("room text = %q", room)
	}
	for _, bad := range []string{"Propose", "→", "schema_set_compatibility", "schema tools"} {
		if strings.Contains(room, bad) {
			t.Fatalf("room text carries model guidance %q: %q", bad, room)
		}
	}
	if !strings.Contains(compatFollowUp(steps), "Propose a separate fix") {
		t.Fatal("the model note must keep the guidance")
	}
}
