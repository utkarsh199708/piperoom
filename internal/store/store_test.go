package store

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
)

func TestClampLimit(t *testing.T) {
	tests := []struct{ in, want int }{
		{-5, DefaultSinceLimit},
		{0, DefaultSinceLimit},
		{1, 1},
		{500, 500},
		{1000, MaxSinceLimit},
		{1001, MaxSinceLimit},
		{1 << 30, MaxSinceLimit},
	}
	for _, tc := range tests {
		if got := ClampLimit(tc.in); got != tc.want {
			t.Errorf("ClampLimit(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestMarshalPayload(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    string
		wantErr bool
	}{
		{"raw message kept as is", json.RawMessage(`{ "a" : 1 }`), `{ "a" : 1 }`, false},
		{"empty raw message is null", json.RawMessage(nil), `null`, false},
		{"nil is null", nil, `null`, false},
		{"struct", events.AgentMessagePayload{Text: "hi"}, `{"text":"hi"}`, false},
		{"empty struct", events.ControlRequestedPayload{}, `{}`, false},
		{"map", map[string]int{"n": 2}, `{"n":2}`, false},
		{"NUL in a string becomes U+FFFD", map[string]string{"k": "a\x00b"}, `{"k":"a\ufffdb"}`, false},
		{"NUL in a raw message", json.RawMessage(`{"a\u0000":"x\u0000"}`), `{"a\ufffd":"x\ufffd"}`, false},
		{"NUL escape twice in a row", json.RawMessage(`"\u0000\u0000"`), `"\ufffd\ufffd"`, false},
		{"NUL followed by a digit", json.RawMessage(`"\u00000"`), `"\ufffd0"`, false},
		{"escaped backslash before u0000 is text, not NUL", json.RawMessage(`"\\u0000"`), `"\\u0000"`, false},
		{"escaped backslash then a real NUL", json.RawMessage(`"\\\u0000"`), `"\\\ufffd"`, false},
		{"other escapes untouched", json.RawMessage(`"\n\t\"\u00e9"`), `"\n\t\"\u00e9"`, false},
		{"invalid raw message", json.RawMessage(`{oops`), "", true},
		{"unmarshalable", make(chan int), "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MarshalPayload(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
