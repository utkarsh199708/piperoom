package loadtest

import (
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/events"
)

// serverMsg mirrors the ws gateway's server message (internal/ws), whose field
// order parseFrame depends on.
type serverMsg struct {
	Type    string        `json:"type"`
	Event   *events.Event `json:"event,omitempty"`
	Members []any         `json:"members,omitempty"`
}

func TestParseFrameMatchesJSON(t *testing.T) {
	at := time.Date(2026, 10, 13, 10, 0, 0, 123456000, time.UTC)
	tests := []struct {
		name    string
		typ     events.Type
		payload any
		mark    int64
		replica int
	}{
		{"comment with mark and replica", events.CommentAdded, events.CommentAddedPayload{CommentID: "c_1", Text: "lt:1760349600123456789:2 padding"}, 1760349600123456789, 2},
		{"comment with mark, no replica", events.CommentAdded, events.CommentAddedPayload{CommentID: "c_3", Text: "lt:77 padding"}, 77, -1},
		{"comment with mark, empty replica", events.CommentAdded, events.CommentAddedPayload{CommentID: "c_4", Text: "lt:78: padding"}, 78, -1},
		{"comment without mark", events.CommentAdded, events.CommentAddedPayload{CommentID: "c_2", Text: "hello \"seq\":5 at"}, 0, -1},
		{"tool result with nested keys", events.AgentToolResult, map[string]any{
			"step_id": "s_1", "summary": `{"seq":99,"type":"x","at":"2020-01-01T00:00:00Z"}`,
			"data": map[string]any{"seq": 77, "type": "nested", "at": "2021-01-01T00:00:00Z"},
		}, 0, -1},
		{"status", events.AgentStatus, events.AgentStatusPayload{State: "working"}, 0, -1},
		{"mark text on a non-comment is ignored", events.AgentMessage, map[string]string{"text": "lt:42:1 x"}, 0, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(serverMsg{Type: "event", Event: &events.Event{
				RoomID: "r_01ABC", Seq: 4321, Type: tc.typ, Actor: events.AgentActor, Payload: p, At: at,
			}})
			if err != nil {
				t.Fatal(err)
			}
			fast, ok := parseFrame(b)
			if !ok {
				t.Fatalf("parseFrame refused %s", b)
			}
			slow, err := decodeFrame(b)
			if err != nil {
				t.Fatal(err)
			}
			if fast.Kind != "event" || fast.Seq != 4321 || fast.Type != string(tc.typ) || !fast.At.Equal(at) || fast.MarkNS != tc.mark || fast.MarkReplica != tc.replica {
				t.Fatalf("fast = %+v", fast)
			}
			if fast.Kind != slow.Kind || fast.Seq != slow.Seq || fast.Type != slow.Type || !fast.At.Equal(slow.At) || fast.MarkNS != slow.MarkNS || fast.MarkReplica != slow.MarkReplica {
				t.Fatalf("fast %+v != slow %+v", fast, slow)
			}
		})
	}
}

func TestParseFrameOtherKinds(t *testing.T) {
	pres, _ := json.Marshal(serverMsg{Type: "presence", Members: []any{map[string]string{"id": "priya"}}})
	if f, ok := parseFrame(pres); !ok || f.Kind != "presence" {
		t.Fatalf("presence: %+v %v", f, ok)
	}
	if f, ok := parseFrame([]byte(`{"type":"error","error":{"code":"x"}}`)); !ok || f.Kind != "error" {
		t.Fatalf("error: %+v %v", f, ok)
	}
	for _, bad := range []string{
		``, `not json`, `{"type":"event"}`, `{"type":"event","event":{"seq":x}}`,
		`{"type":"event","event":{"seq":1}}`, `{"type":"event","event":{"seq":1,"type":"a"}}`,
		`{"type":"event","event":{"seq":1,"type":"a","at":"yesterday"}}`,
	} {
		if _, ok := parseFrame([]byte(bad)); ok {
			t.Fatalf("parseFrame accepted %q", bad)
		}
	}
}
