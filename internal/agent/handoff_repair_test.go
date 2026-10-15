package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// seqReply is one scripted answer of seqModel.
type seqReply struct {
	msg anthropic.BetaMessage
	err error
}

// seqModel answers requests in order and records them whole, so a test can read
// the repair request's text. Past the script it repeats the last reply.
type seqModel struct {
	mu      sync.Mutex
	replies []seqReply
	calls   []anthropic.BetaMessageNewParams
}

func (m *seqModel) Turn(_ context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, p)
	i := len(m.calls) - 1
	if i >= len(m.replies) {
		i = len(m.replies) - 1
	}
	return m.replies[i].msg, m.replies[i].err
}

// userText is every text block of a recorded request's user messages.
func userText(p anthropic.BetaMessageNewParams) string {
	return requestText(p.Messages)
}

const (
	goodSummary     = `{"what_broke":"orders stopped reaching the warehouse","why":"column amount changed from int to decimal","done":["found the schema error"],"ruled_out":["kafka lag"],"open":[],"risks":[]}`
	noWhatBroke     = `{"what_broke":"","why":"column amount changed from int to decimal","done":[],"ruled_out":[],"open":[],"risks":[]}`
	noWhy           = `{"what_broke":"orders stopped","why":"  ","done":[],"ruled_out":[],"open":[],"risks":[]}`
	noWhatBrokeOrWy = `{"what_broke":"","why":"","done":[],"ruled_out":[],"open":[],"risks":[]}`
)

func roomLog() []events.Event {
	return []events.Event{{Type: events.RoomCreated, Payload: json.RawMessage(`{"title":"orders stale"}`)}}
}

func TestWriteHandoffRepairsAMissingField(t *testing.T) {
	cases := []struct {
		name       string
		first      anthropic.BetaMessage
		wantNote   string // what the repair request must say went wrong
		notInNote  string
		wantCutOff bool
	}{
		{"what_broke empty", textTurn(noWhatBroke), "the required field what_broke was missing or empty", "what_broke and why", false},
		{"why blank", textTurn(noWhy), "the required field why was missing or empty", "what_broke", false},
		{"both empty", textTurn(noWhatBrokeOrWy), "the required fields what_broke and why were missing or empty", "", false},
		{"not json", textTurn("here is the summary: ..."), "it was not a valid JSON object", "was missing or empty", false},
		{"no text", textTurn("  "), "it was empty", "was missing or empty", false},
		{"cut off", anthropic.BetaMessage{StopReason: anthropic.BetaStopReasonMaxTokens, Content: []anthropic.BetaContentBlockUnion{{Type: "text", Text: `{"what_broke":"orders st`}}}, "cut off at the length limit", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &seqModel{replies: []seqReply{{msg: c.first}, {msg: textTurn(goodSummary)}}}
			got, err := writeHandoff(context.Background(), roomLog(), Options{}, m)
			if err != nil {
				t.Fatalf("writeHandoff: %v", err)
			}
			if got.WhatBroke == "" || got.Why == "" || len(got.Done) != 1 {
				t.Fatalf("summary = %+v, want the repaired one", got)
			}
			if len(m.calls) != 2 {
				t.Fatalf("model calls = %d, want the request and one repair", len(m.calls))
			}
			first, repair := userText(m.calls[0]), userText(m.calls[1])
			if strings.Contains(first, "Correction needed") {
				t.Fatalf("the first request must not carry a correction: %q", first)
			}
			if !strings.Contains(repair, c.wantNote) {
				t.Fatalf("repair request does not say %q:\n%s", c.wantNote, repair)
			}
			// Only the problem sentence is compared: the generic instruction after
			// it names both required fields every time.
			_, rest, _ := strings.Cut(repair, "could not be used: ")
			problem, _, _ := strings.Cut(rest, ". Answer again")
			if c.notInNote != "" && strings.Contains(problem, c.notInNote) {
				t.Fatalf("the problem sentence %q must not say %q", problem, c.notInNote)
			}
			if !strings.Contains(repair, "complete JSON object") {
				t.Fatalf("repair request must ask for the full JSON again:\n%s", repair)
			}
			// The repair request is the first one plus the note: same room text,
			// same instructions, same schema.
			if !strings.HasPrefix(repair, first) {
				t.Fatalf("repair request must keep the room text of the first request")
			}
			if m.calls[0].System[0].Text != m.calls[1].System[0].Text {
				t.Fatal("repair request must keep the system prompt")
			}
			if strings.Contains(repair, "here is the summary") {
				t.Fatal("the repair request must not echo the bad answer back")
			}
		})
	}
}

func TestWriteHandoffValidAnswerMakesOneRequest(t *testing.T) {
	m := &seqModel{replies: []seqReply{{msg: textTurn(goodSummary)}}}
	if _, err := writeHandoff(context.Background(), roomLog(), Options{}, m); err != nil {
		t.Fatal(err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("model calls = %d, want 1: a good answer needs no repair", len(m.calls))
	}
}

func TestWriteHandoffStillInvalidAfterRepair(t *testing.T) {
	cases := map[string]struct {
		second anthropic.BetaMessage
		want   string
	}{
		"still empty":    {textTurn(noWhatBroke), "model omitted what_broke"},
		"still not json": {textTurn("sorry"), "not valid JSON"},
		"now no text":    {textTurn(""), "no text"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := &seqModel{replies: []seqReply{{msg: textTurn(noWhatBroke)}, {msg: c.second}, {msg: textTurn(goodSummary)}}}
			got, err := writeHandoff(context.Background(), roomLog(), Options{}, m)
			if !errors.Is(err, ErrSummaryInvalid) {
				t.Fatalf("err = %v, want ErrSummaryInvalid", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %q, want it to say %q", err, c.want)
			}
			if got.WhatBroke != "" || got.Why != "" {
				t.Fatalf("a failed hand-off must return no summary, got %+v", got)
			}
			if len(m.calls) != 2 {
				t.Fatalf("model calls = %d, want exactly one repair, never a third request", len(m.calls))
			}
		})
	}
}

func TestWriteHandoffModelErrors(t *testing.T) {
	boom := errors.New("upstream 529")

	// The first request fails: nothing to repair. The error wraps the cause, and
	// is not "invalid": the model never answered.
	m := &seqModel{replies: []seqReply{{err: boom}}}
	_, err := writeHandoff(context.Background(), roomLog(), Options{}, m)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the model error", err)
	}
	if errors.Is(err, ErrSummaryInvalid) {
		t.Fatalf("a failed call is not an invalid summary: %v", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("model calls = %d, want 1", len(m.calls))
	}

	// The repair request fails: the answer stayed unusable, and the cause is kept.
	m = &seqModel{replies: []seqReply{{msg: textTurn(noWhy)}, {err: boom}}}
	_, err = writeHandoff(context.Background(), roomLog(), Options{}, m)
	if !errors.Is(err, ErrSummaryInvalid) || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want both ErrSummaryInvalid and the model error", err)
	}
	if len(m.calls) != 2 {
		t.Fatalf("model calls = %d, want 2", len(m.calls))
	}

	// A deadline during the repair stays visible to errors.Is.
	m = &seqModel{replies: []seqReply{{msg: textTurn(noWhy)}, {err: context.DeadlineExceeded}}}
	_, err = writeHandoff(context.Background(), roomLog(), Options{}, m)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrSummaryInvalid) {
		t.Fatalf("err = %v, want the deadline and ErrSummaryInvalid", err)
	}
}

// A refusal is final: asking again will not change the model's mind, and a
// second request would spend tokens for nothing.
func TestWriteHandoffRefusalIsInvalidWithoutRepair(t *testing.T) {
	m := &seqModel{replies: []seqReply{{msg: anthropic.BetaMessage{StopReason: anthropic.BetaStopReasonRefusal}}, {msg: textTurn(goodSummary)}}}
	_, err := writeHandoff(context.Background(), roomLog(), Options{}, m)
	if !errors.Is(err, ErrSummaryInvalid) || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err = %v, want a refusal wrapping ErrSummaryInvalid", err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("model calls = %d, want 1", len(m.calls))
	}
}

func TestWriteHandoffEmptyRoomMakesNoRequest(t *testing.T) {
	m := &seqModel{replies: []seqReply{{msg: textTurn(goodSummary)}}}
	if _, err := writeHandoff(context.Background(), nil, Options{}, m); err == nil {
		t.Fatal("an empty room must be an error")
	}
	if len(m.calls) != 0 {
		t.Fatalf("model calls = %d, want none", len(m.calls))
	}
}

// The repair request goes through the same model, so the endpoint reports and
// counts its tokens like the first one (the hand-off is never refused).
func TestHandoffRepairIsReportedAsUsage(t *testing.T) {
	bad := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"{\"what_broke\":\"\",\"why\":\"y\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}"}],"usage":{"input_tokens":40,"output_tokens":10}}`)
	good := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"{\"what_broke\":\"x\",\"why\":\"y\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}"}],"usage":{"input_tokens":45,"output_tokens":12}}`)
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{bad, good}}, 100)
	var (
		mu       sync.Mutex
		reported []events.AgentUsagePayload
	)
	if err := ep.SetSpend(SpendOptions{OnUsage: func(_ context.Context, _ string, p events.AgentUsagePayload) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, p)
	}}); err != nil {
		t.Fatal(err)
	}
	evs := []events.Event{{RoomID: "room-r", Seq: 1, Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)}}
	s, err := ep.WriteHandoff(context.Background(), evs, Options{})
	if err != nil {
		t.Fatalf("WriteHandoff: %v", err)
	}
	if s.WhatBroke != "x" {
		t.Fatalf("summary = %+v, want the repaired one", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 2 || reported[0].InputTokens != 40 || reported[1].InputTokens != 45 {
		t.Fatalf("usage events = %+v, want one per request", reported)
	}
	for _, p := range reported {
		if p.Role != events.UsageRoleHandoff {
			t.Fatalf("role = %q, want %q", p.Role, events.UsageRoleHandoff)
		}
	}
}

// The post-mortem model pass shares the request, so it gets the repair too.
func TestWritePostmortemRepairsAMissingField(t *testing.T) {
	m := &seqModel{replies: []seqReply{{msg: textTurn(noWhy)}, {msg: textTurn(goodSummary)}}}
	d, err := writePostmortem(context.Background(), postmortemLog(), Options{}, m)
	if err != nil {
		t.Fatalf("writePostmortem: %v", err)
	}
	if d.Summary.Why == "" || len(m.calls) != 2 {
		t.Fatalf("draft = %+v after %d calls, want the repaired summary after 2", d, len(m.calls))
	}

	m = &seqModel{replies: []seqReply{{msg: textTurn(noWhy)}}}
	if _, err := writePostmortem(context.Background(), postmortemLog(), Options{}, m); !errors.Is(err, ErrSummaryInvalid) {
		t.Fatalf("err = %v, want ErrSummaryInvalid", err)
	}
}
