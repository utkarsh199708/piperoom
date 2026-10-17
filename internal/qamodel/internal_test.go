package qamodel

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRoomHint(t *testing.T) {
	user := func(text string) *apiRequest {
		raw, _ := json.Marshal(text)
		return &apiRequest{Messages: []apiMessage{{Role: "user", Content: raw}}}
	}
	tests := []struct {
		name string
		req  *apiRequest
		want string
	}{
		{
			"agent opening",
			user("Investigate this pipeline incident.\n\nAlert: <untrusted>orders stale &amp; late</untrusted>\nPipeline: <untrusted>orders</untrusted>\nRoom: r_9f2\n\nWork through your method."),
			`r_9f2 "orders stale &amp; late"`,
		},
		{
			"title with a newline loses its closing tag",
			user("Alert: <untrusted>first line\nsecond line</untrusted>\nRoom: r_1\n"),
			`r_1 "first line"`,
		},
		{"room only", user("Room: r_7"), "r_7"},
		{"hand-off request has neither", user("Room events:\nseq 1 alert.fired\nseq 2 agent.status"), "Room events:"},
		{"long first line is cut", user(strings.Repeat("é", 100)), strings.Repeat("é", 40) + "..."},
		{"no user message", &apiRequest{Messages: []apiMessage{{Role: "assistant", Content: json.RawMessage(`"hi"`)}}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := roomHint(tt.req); got != tt.want {
				t.Errorf("roomHint = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestChunkStringKeepsRunes(t *testing.T) {
	for _, n := range []int{1, 2, 3, 5, 64, 256} {
		s := strings.Repeat("aé€😀", 50)
		parts := chunkString(s, n)
		if strings.Join(parts, "") != s {
			t.Fatalf("n=%d: the pieces do not rebuild the text", n)
		}
		for _, p := range parts {
			if !utf8.ValidString(p) {
				t.Fatalf("n=%d: piece %q is not valid UTF-8", n, p)
			}
			// A piece may exceed n only when n is smaller than its one rune.
			if len(p) > n && utf8.RuneCountInString(p) > 1 {
				t.Fatalf("n=%d: piece of %d bytes", n, len(p))
			}
		}
	}
	if got := chunkString("", 10); len(got) != 0 {
		t.Errorf("chunkString of nothing = %v", got)
	}
}

func TestVisibleControls(t *testing.T) {
	tests := []struct{ in, want string }{
		{"plain text\nsecond\tline", "plain text\nsecond\tline"},
		{"\x1b[31mred\x1b[0m", `\x1b[31mred\x1b[0m`},
		{"bell\x07 and \x7f del", `bell\x07 and \x7f del`},
		{"c1 \u009b csi", `c1 \u009b csi`},
		{"unicode é€😀 stays", "unicode é€😀 stays"},
		{"carriage\rreturn", `carriage\x0dreturn`},
	}
	for _, tt := range tests {
		if got := visibleControls(tt.in); got != tt.want {
			t.Errorf("visibleControls(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestDefaultUsageIsSmallAndDeterministic(t *testing.T) {
	rep := &Reply{Text: strings.Repeat("x", 400), ToolCalls: make([]ToolCall, 2)}
	a, b := defaultUsage(7, rep), defaultUsage(7, rep)
	if *a != *b {
		t.Errorf("not deterministic: %+v vs %+v", a, b)
	}
	if a.InputTokens != 170 || a.OutputTokens != 5+100+50 {
		t.Errorf("usage = %+v", a)
	}
}

func TestParseWait(t *testing.T) {
	tests := []struct {
		in      string
		wantErr bool
	}{
		{"60s", false}, {"1m", false}, {"2", false}, {"0.5", false}, {"10m", false},
		{"", true}, {"x", true}, {"0", true}, {"-1s", true}, {"11m", true},
	}
	for _, tt := range tests {
		d, err := parseWait(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseWait(%q) = %v, %v", tt.in, d, err)
		}
	}
}
