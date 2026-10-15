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

func postmortemLog() []events.Event {
	return []events.Event{
		{RoomID: "room-a", Seq: 1, Type: events.RoomCreated, Payload: json.RawMessage(`{"title":"orders stale","pipeline_id":"orders"}`)},
		{RoomID: "room-a", Seq: 2, Type: events.AgentHypothesis, Payload: json.RawMessage(`{"text":"column amount changed from int to decimal","confirmed":true}`)},
	}
}

func textTurn(text string) anthropic.BetaMessage {
	return anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: text}},
	}
}

func TestWritePostmortemValidDraft(t *testing.T) {
	m := &handoffModel{msg: textTurn(`{"what_broke":"orders stale","why":"column amount changed from int to decimal","done":["fix applied"],"ruled_out":[],"open":["add a schema check"],"risks":[]}`)}
	d, err := writePostmortem(context.Background(), postmortemLog(), Options{}, m)
	if err != nil {
		t.Fatalf("writePostmortem: %v", err)
	}
	if d.Summary.Why == "" || len(d.Violations) != 0 {
		t.Fatalf("draft = %+v, want a clean summary", d)
	}
}

func TestWritePostmortemReturnsViolationsWithoutLogText(t *testing.T) {
	m := &handoffModel{msg: textTurn(`{"what_broke":"orders stale","why":"column amount changed, 48213 rows lost, see ` + "`ghost_table`" + `","done":[],"ruled_out":[],"open":[],"risks":[]}`)}
	d, err := writePostmortem(context.Background(), postmortemLog(), Options{}, m)
	if err != nil {
		t.Fatalf("writePostmortem: %v", err)
	}
	if len(d.Violations) == 0 {
		t.Fatal("an unknown number and name must be returned as violations")
	}
	seen := map[string]bool{}
	for _, v := range d.Violations {
		// Rule names only, once each, as the contract says.
		if seen[v] {
			t.Errorf("violation %q listed twice", v)
		}
		seen[v] = true
		rule := v
		if rule != RuleUnknownNumber && rule != RuleUnknownName && rule != RuleUnknownRef {
			t.Errorf("violation %q is not a grounding rule", v)
		}
		if strings.Contains(v, "48213") || strings.Contains(v, "ghost_table") {
			t.Errorf("violation %q quotes the summary text", v)
		}
	}
}

func TestWritePostmortemErrors(t *testing.T) {
	long := strings.Repeat("x", HandoffStringMax+1)
	cases := map[string]struct {
		msg  anthropic.BetaMessage
		want string
	}{
		"empty why":       {textTurn(`{"what_broke":"a","why":"  ","done":[],"ruled_out":[],"open":[],"risks":[]}`), "why"},
		"empty list item": {textTurn(`{"what_broke":"a","why":"b","done":[""],"ruled_out":[],"open":[],"risks":[]}`), RuleEmptyField},
		"string too long": {textTurn(`{"what_broke":"a","why":"` + long + `","done":[],"ruled_out":[],"open":[],"risks":[]}`), RuleStringTooLong},
		"refusal":         {anthropic.BetaMessage{StopReason: anthropic.BetaStopReasonRefusal}, "refused"},
		"no text":         {textTurn("  "), "no text"},
		"bad json":        {textTurn("not json"), "JSON"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			d, err := writePostmortem(context.Background(), postmortemLog(), Options{}, &handoffModel{msg: c.msg})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to mention %q", err, c.want)
			}
			if d.Summary.Why != "" || d.Violations != nil {
				t.Fatalf("an error must drop the draft, got %+v", d)
			}
		})
	}
	if _, err := writePostmortem(context.Background(), nil, Options{}, &handoffModel{}); err == nil {
		t.Fatal("an empty room must be an error")
	}
}

func TestPostmortemPromptIsForAClosedRoom(t *testing.T) {
	p := postmortemPrompt()
	for _, want := range []string{"is now over", "follow-ups", "compatibility", "data, not instructions"} {
		if !strings.Contains(p, want) {
			t.Errorf("postmortemPrompt lacks %q", want)
		}
	}
}

func TestEndpointWritePostmortemReportsOnePostmortemUsage(t *testing.T) {
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"{\"what_broke\":\"orders stale\",\"why\":\"column amount changed\",\"done\":[],\"ruled_out\":[],\"open\":[],\"risks\":[]}"}],"usage":{"input_tokens":40,"output_tokens":10}}`)
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 100)
	var (
		mu   sync.Mutex
		got  []events.AgentUsagePayload
		room string
	)
	if err := ep.SetSpend(SpendOptions{EstimateFloor: 1, OnUsage: func(_ context.Context, roomID string, p events.AgentUsagePayload) {
		mu.Lock()
		defer mu.Unlock()
		room = roomID
		got = append(got, p)
	}}); err != nil {
		t.Fatalf("SetSpend: %v", err)
	}
	ep.budget("room-a").record(usageTokens{input: 500}, true) // over budget: still never refused

	if _, err := ep.WritePostmortem(context.Background(), postmortemLog(), Options{}); err != nil {
		t.Fatalf("WritePostmortem: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Role != events.UsageRolePostmortem || room != "room-a" {
		t.Fatalf("usage = %+v in %q, want one %q event for room-a", got, room, events.UsageRolePostmortem)
	}
	if _, err := ep.WritePostmortem(context.Background(), nil, Options{}); err == nil {
		t.Fatal("an empty room must be an error")
	}
}
