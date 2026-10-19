package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// countingProposals counts Create calls; a batch room must never reach it.
type countingProposals struct {
	mu sync.Mutex
	n  int
}

func (p *countingProposals) Create(context.Context, events.ProposalCreatedPayload) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	return "p_1", nil
}

func (p *countingProposals) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// A batch room is not offered propose_fix (no action serves batch yet), and
// a model that calls it anyway, with a well-formed CDC fix, gets an error
// tool_result: no proposal is created and no proposal.* event reaches the
// room (the #659 review). The registry holds CDC actions, as a mixed set's
// does, so the refusal does not rest on an empty registry.
func TestBatchRoomProposeFixIsRefused(t *testing.T) {
	reg, _ := kindTestRegistry(t)
	props := &countingProposals{}
	sink := &fakeSink{}
	fix := `{"title":"restart","steps":[{"action":"connect_restart_task","params":{},"description":"restart the task"}],"risk":"low","rollback":"none"}`
	m := &funcModel{fn: func(_ context.Context, i int, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		if i == 0 {
			return mustMessage(t, `{"stop_reason":"tool_use","content":[{"type":"tool_use","id":"tu_fix","name":"propose_fix","input":`+fix+`}]}`), nil
		}
		return mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`), nil
	}}
	r, err := newRunnerWith(Alert{Title: "revenue_daily failed", PipelineID: "p", PipelineKind: events.PipelineKindBatch}, Deps{
		Tools: reg, Actions: kindTestActions(t), Proposals: props,
		Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger(),
	}, Options{MaxTurns: 3}, m)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.run(context.Background())

	calls := m.calls()
	if len(calls) < 2 {
		t.Fatalf("model calls = %d, want the propose_fix result sent back", len(calls))
	}
	var res *anthropic.BetaToolResultBlockParam
	msgs := calls[1].Messages
	for _, b := range msgs[len(msgs)-1].Content {
		if b.OfToolResult != nil && b.OfToolResult.ToolUseID == "tu_fix" {
			res = b.OfToolResult
		}
	}
	if res == nil {
		t.Fatal("no tool_result for the propose_fix call")
	}
	js, _ := json.Marshal(res)
	if !strings.Contains(string(js), `"is_error":true`) {
		t.Fatalf("propose_fix in a batch room: tool_result is not an error: %s", js)
	}
	if n := props.count(); n != 0 {
		t.Fatalf("Proposals.Create called %d times in a batch room", n)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, e := range sink.events {
		if strings.HasPrefix(string(e.typ), "proposal.") {
			t.Fatalf("batch room emitted %s", e.typ)
		}
	}
}
