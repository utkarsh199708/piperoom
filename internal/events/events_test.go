package events_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestDecodeTable(t *testing.T) {
	tests := []struct {
		name    string
		typ     events.Type
		payload string
		want    events.RoomClosedPayload
		wantErr bool
	}{
		{
			name:    "valid payload",
			typ:     events.RoomClosed,
			payload: `{"resolution":"fixed"}`,
			want:    events.RoomClosedPayload{Resolution: "fixed"},
		},
		{
			name:    "unknown fields are ignored",
			typ:     events.RoomClosed,
			payload: `{"resolution":"fixed","added_later":true}`,
			want:    events.RoomClosedPayload{Resolution: "fixed"},
		},
		{
			name:    "null decodes to the zero value",
			typ:     events.RoomClosed,
			payload: `null`,
			want:    events.RoomClosedPayload{},
		},
		{
			name:    "empty object is the zero value",
			typ:     events.RoomClosed,
			payload: `{}`,
			want:    events.RoomClosedPayload{},
		},
		{
			name:    "truncated JSON is an error",
			typ:     events.RoomClosed,
			payload: `{"resolution":"fix`,
			wantErr: true,
		},
		{
			name:    "wrong JSON type is an error",
			typ:     events.RoomClosed,
			payload: `"fixed"`,
			wantErr: true,
		},
		{
			name:    "number for a string field is an error",
			typ:     events.RoomClosed,
			payload: `{"resolution":42}`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := events.Event{Type: tt.typ, Payload: json.RawMessage(tt.payload)}
			got, err := events.Decode[events.RoomClosedPayload](e)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Decode(%s) = %+v, want error", tt.payload, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Decode(%s): unexpected error: %v", tt.payload, err)
			}
			if got != tt.want {
				t.Fatalf("Decode(%s) = %+v, want %+v", tt.payload, got, tt.want)
			}
		})
	}
}

func TestDecodeEmptyPayloadIsAnError(t *testing.T) {
	// A nil payload is not valid JSON; an event without a payload must not
	// silently decode to a zero value.
	_, err := events.Decode[events.RoomClosedPayload](events.Event{Type: events.RoomClosed})
	if err == nil {
		t.Fatal("nil payload decoded without error")
	}
}

func TestDecodeErrorWrapsSyntaxErrorAndNamesType(t *testing.T) {
	e := events.Event{Type: events.RoomCreated, Payload: json.RawMessage(`{"title":`)}
	_, err := events.Decode[events.RoomCreatedPayload](e)
	if err == nil {
		t.Fatal("want error")
	}
	var syn *json.SyntaxError
	if !errors.As(err, &syn) {
		t.Fatalf("error %v does not wrap *json.SyntaxError", err)
	}
	if !strings.Contains(err.Error(), string(events.RoomCreated)) {
		t.Fatalf("error %q does not name the event type", err)
	}
}

func TestDecodeErrorDoesNotEchoPayload(t *testing.T) {
	// Payloads can carry alert text and record values; errors go to logs,
	// so they must name the type only.
	secret := "customer-email-jane@example.com"
	e := events.Event{Type: events.RoomCreated, Payload: json.RawMessage(`{"title":"` + secret + `",}`)}
	_, err := events.Decode[events.RoomCreatedPayload](e)
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaks payload content: %q", err)
	}
}

func TestDecodeTypeMismatchIsTyped(t *testing.T) {
	e := events.Event{Type: events.RoomCreated, Payload: json.RawMessage(`{"title":5}`)}
	_, err := events.Decode[events.RoomCreatedPayload](e)
	var ute *json.UnmarshalTypeError
	if !errors.As(err, &ute) {
		t.Fatalf("error %v does not wrap *json.UnmarshalTypeError", err)
	}
}

func TestEventJSONWireNames(t *testing.T) {
	// The browser and the hand-off read these keys; renaming one breaks them.
	e := events.Event{
		RoomID:  "r1",
		Seq:     7,
		Type:    events.CommentAdded,
		Actor:   events.Actor{Kind: events.ActorHuman, ID: "u1", Name: "Priya"},
		Payload: json.RawMessage(`{"text":"hi"}`),
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"room_id", "seq", "type", "actor", "payload", "at"} {
		if _, ok := m[k]; !ok {
			t.Errorf("event JSON lacks key %q: %s", k, b)
		}
	}
	var got events.Event
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Seq != 7 || got.Type != events.CommentAdded || got.Actor.Name != "Priya" {
		t.Fatalf("round trip lost fields: %+v", got)
	}
}
