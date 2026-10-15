package agent

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func TestHasContent(t *testing.T) {
	cases := []struct {
		name string
		text string
		want bool
	}{
		{"empty", "", false},
		{"spaces", "   ", false},
		{"newlines and tabs", "\n\t \r\n", false},
		{"br", "<br>", false},
		{"br slash", "<br/>", false},
		{"br spaced slash", "<br />", false},
		{"upper case tag", "<BR>", false},
		{"several tags", "<br><br>\n<br/>", false},
		{"empty paragraph", "<p></p>", false},
		{"paragraph with spaces", "<p>  \n </p>", false},
		{"tag with attributes", `<div class="note" data-x='1'>  </div>`, false},
		{"closing tag only", "</p>", false},
		{"comment", "<!-- nothing to see -->", false},
		{"multi-line comment", "<!--\nnothing\n-->", false},
		{"nbsp entity", "&nbsp;", false},
		{"nbsp numeric", "&#160;", false},
		{"nbsp hex", "&#xA0;", false},
		{"zero width space entity", "&#8203;", false},
		{"entities and tags", "<p>&nbsp;&nbsp;</p><br>", false},
		{"escaped tag", "&lt;br&gt;", false},
		{"escaped tag twice over", "&lt;p&gt;&nbsp;&lt;/p&gt;", false},
		{"real non-breaking space", string([]rune{0x00a0, 0x0020, 0x200b, 0xfeff}), false},
		{"control characters", "\x00\x07\x1b", false},

		{"plain text", "Recovered.", true},
		{"text after a tag", "<br>Recovered.", true},
		{"text between tags", "<p>Recovered</p>", true},
		{"markup around text", "<b>done</b>", true},
		{"less-than in prose", "lag < 10 now", true},
		{"greater-than in prose", "lag > 1000", true},
		{"comparison that looks like a tag", "a < b and c > d", true},
		{"angle brackets without a letter", "<<>>", true},
		{"an ampersand", "&amp;", true},
		{"unknown entity is text", "&notanentity;", true},
		{"one character", ".", true},
		{"non-latin text", "已恢复", true},
		{"tag-like word is text", "<br", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hasContent(c.text); got != c.want {
				t.Fatalf("hasContent(%q) = %v, want %v", c.text, got, c.want)
			}
		})
	}
}

func TestMessageKeyFoldsWhitespaceAndKeepsReplyTo(t *testing.T) {
	a := events.AgentMessagePayload{Text: "Connector  is\n running \t again."}
	b := events.AgentMessagePayload{Text: "  Connector is running again.  "}
	if messageKey(a) != messageKey(b) {
		t.Fatalf("keys differ: %q vs %q", messageKey(a), messageKey(b))
	}
	if messageKey(a) == messageKey(events.AgentMessagePayload{Text: "connector is running again."}) {
		t.Fatal("the comparison is case sensitive: only whitespace is folded")
	}
	b.ReplyTo = "c_1"
	if messageKey(a) == messageKey(b) {
		t.Fatal("an answer to a comment is not a repeat of an unanswered post")
	}
}

// failingSink fails its first failures writes, then records.
type failingSink struct {
	fakeSink
	failures int
}

func (s *failingSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if s.failures > 0 {
		s.failures--
		return errors.New("db is down")
	}
	return s.fakeSink.Emit(ctx, typ, payload)
}

func guardPostedTexts(s *fakeSink) []string {
	var out []string
	for _, e := range s.ofType(events.AgentMessage) {
		var p events.AgentMessagePayload
		_ = json.Unmarshal(e.payload, &p)
		out = append(out, p.Text)
	}
	return out
}

func TestGuardedSinkDropsEmptyAndRepeatedMessages(t *testing.T) {
	type msg struct {
		text, replyTo string
		want          string // "" = posted, else the drop reason
	}
	cases := []struct {
		name string
		msgs []msg
		want []string // texts that reach the room, in order
	}{
		{"markup only", []msg{{"<br>", "", dropReasonEmpty}}, nil},
		{"empty and blank", []msg{{"", "", dropReasonEmpty}, {"  \n", "", dropReasonEmpty}}, nil},
		{"entity only", []msg{{"&nbsp;", "", dropReasonEmpty}, {"<p></p>", "", dropReasonEmpty}}, nil},
		{"the last message of a room is <br>", []msg{{"The connector is running again.", "", ""}, {"<br>", "", dropReasonEmpty}}, []string{"The connector is running again."}},
		{"announced twice in a row", []msg{{"Recovered.", "", ""}, {"Recovered.", "", dropReasonRepeat}}, []string{"Recovered."}},
		{"announced three times", []msg{{"Recovered.", "", ""}, {"Recovered.", "", dropReasonRepeat}, {"Recovered.", "", dropReasonRepeat}}, []string{"Recovered."}},
		{"whitespace differs", []msg{{"Recovered.\nLag is 0.", "", ""}, {"  Recovered.   Lag is 0. ", "", dropReasonRepeat}}, []string{"Recovered.\nLag is 0."}},
		{"a different message in between", []msg{{"A", "", ""}, {"B", "", ""}, {"A", "", ""}}, []string{"A", "B", "A"}},
		{"an empty message does not reset the last one", []msg{{"A", "", ""}, {"<br>", "", dropReasonEmpty}, {"A", "", dropReasonRepeat}}, []string{"A"}},
		{"case differs", []msg{{"Recovered.", "", ""}, {"recovered.", "", ""}}, []string{"Recovered.", "recovered."}},
		{"same words to another comment", []msg{{"Yes.", "c_1", ""}, {"Yes.", "c_2", ""}, {"Yes.", "c_2", dropReasonRepeat}}, []string{"Yes.", "Yes."}},
		{"a post after an answer with the same words", []msg{{"Yes.", "c_1", ""}, {"Yes.", "", ""}}, []string{"Yes.", "Yes."}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := &fakeSink{}
			g := newGuardedSink(inner)
			for i, m := range c.msgs {
				err := g.Emit(context.Background(), events.AgentMessage, events.AgentMessagePayload{Text: m.text, ReplyTo: m.replyTo})
				var dropped *messageDroppedError
				switch {
				case m.want == "" && err != nil:
					t.Fatalf("message %d (%q) was refused: %v", i, m.text, err)
				case m.want != "" && !errors.As(err, &dropped):
					t.Fatalf("message %d (%q): err = %v, want a drop", i, m.text, err)
				case m.want != "" && dropped.reason != m.want:
					t.Fatalf("message %d (%q): reason = %q, want %q", i, m.text, dropped.reason, m.want)
				}
			}
			if got := guardPostedTexts(inner); !slices.Equal(got, c.want) {
				t.Fatalf("posted = %q, want %q", got, c.want)
			}
		})
	}
}

// Only agent.message is judged. The same payload twice in any other event type
// goes through twice, and a pointer payload is judged like a value.
func TestGuardedSinkOnlyJudgesMessages(t *testing.T) {
	inner := &fakeSink{}
	g := newGuardedSink(inner)
	ctx := context.Background()
	for range 3 {
		if err := g.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"}); err != nil {
			t.Fatalf("status: %v", err)
		}
	}
	if n := len(inner.ofType(events.AgentStatus)); n != 3 {
		t.Fatalf("status events = %d, want 3", n)
	}
	if err := g.Emit(ctx, events.AgentMessage, &events.AgentMessagePayload{Text: "<br>"}); err == nil {
		t.Fatal("a markup-only message passed as a pointer")
	}
	// A payload of an unexpected type is not ours to judge.
	if err := g.Emit(ctx, events.AgentMessage, map[string]string{"text": ""}); err != nil {
		t.Fatalf("unknown payload type: %v", err)
	}
	if err := g.Emit(ctx, events.AgentMessage, (*events.AgentMessagePayload)(nil)); err != nil {
		t.Fatalf("nil payload: %v", err)
	}
}

// A message counts as "the previous one" only once it was written. If the
// write failed, the same text sent again is not a repeat.
func TestGuardedSinkFailedWriteIsNotRemembered(t *testing.T) {
	inner := &failingSink{failures: 1}
	g := newGuardedSink(inner)
	ctx := context.Background()
	p := events.AgentMessagePayload{Text: "Recovered."}
	if err := g.Emit(ctx, events.AgentMessage, p); err == nil || strings.Contains(err.Error(), "not posted") {
		t.Fatalf("first write: err = %v, want the sink's error", err)
	}
	if err := g.Emit(ctx, events.AgentMessage, p); err != nil {
		t.Fatalf("retry after a failed write was dropped: %v", err)
	}
	if got := guardPostedTexts(&inner.fakeSink); !slices.Equal(got, []string{"Recovered."}) {
		t.Fatalf("posted = %q", got)
	}
}

// Through a whole run: the model ends its turn with "<br>", and says the same
// thing after two consecutive tool turns. The room sees neither the markup nor
// the repeat.
func TestRunDropsMarkupOnlyAndRepeatedMessages(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	m := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"text","text":"Task 0 is running again."},
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check","connector":"c1"}}]}`),
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"text","text":"Task 0 is running again."},
			{"type":"tool_use","id":"tu2","name":"connect_status","input":{"why":"check again","connector":"c1"}}]}`),
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"<br>"}]}`),
	}}
	sink := &fakeSink{}
	r := newTestRunner(t, m, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 5})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := guardPostedTexts(sink); !slices.Equal(got, []string{"Task 0 is running again."}) {
		t.Fatalf("messages in the room = %q, want the announcement once and no <br>", got)
	}
}

// post_message goes through the same guard, and the model is told when its
// message was not posted instead of believing people read it.
func TestPostMessageToolTellsTheModelWhenDropped(t *testing.T) {
	sink := &fakeSink{}
	g := newGuardedSink(sink)
	tool := &postMessageTool{sink: g}
	cases := []struct {
		name, input, wantSummary string
		wantPosted               int
	}{
		{"first message", `{"text":"Looking at the slot."}`, "message posted", 1},
		{"same again", `{"text":"Looking   at the slot."}`, "message not posted: " + dropReasonRepeat, 1},
		{"markup only", `{"text":"<br>"}`, "message not posted: " + dropReasonEmpty, 1},
		{"a new message", `{"text":"The slot is advancing."}`, "message posted", 2},
		{"reply with repeated words", `{"text":"The slot is advancing.","reply_to":"c_1"}`, "message posted", 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := tool.Run(context.Background(), json.RawMessage(c.input))
			if err != nil {
				t.Fatalf("a dropped message is not a tool error: %v", err)
			}
			if res.Summary != c.wantSummary {
				t.Fatalf("summary = %q, want %q", res.Summary, c.wantSummary)
			}
			if n := len(sink.ofType(events.AgentMessage)); n != c.wantPosted {
				t.Fatalf("posted = %d, want %d", n, c.wantPosted)
			}
		})
	}

	// A write failure is still an error, not a "dropped" result.
	broken := &postMessageTool{sink: newGuardedSink(&failingSink{failures: 1})}
	if _, err := broken.Run(context.Background(), json.RawMessage(`{"text":"hello"}`)); err == nil {
		t.Fatal("a failed write must be a tool error")
	}
}
