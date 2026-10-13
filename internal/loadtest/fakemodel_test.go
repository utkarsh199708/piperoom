package loadtest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// opening is the agent's first user message for a room (internal/agent
// firstUserMessage), which is where the fake model reads room and mode.
func opening(room, title string) string {
	return "Investigate this pipeline incident.\n\nAlert: <untrusted>" + title + "</untrusted>\nPipeline: <untrusted>pa</untrusted>\nRoom: " + room + "\n\nWork through your method and report what you find."
}

// requestBody builds a Messages request with the opening turn plus `turns`
// assistant/user pairs.
func requestBody(t *testing.T, room, title string, turns int) []byte {
	t.Helper()
	msgs := []map[string]any{{"role": "user", "content": []map[string]string{{"type": "text", "text": opening(room, title)}}}}
	for i := 0; i < turns; i++ {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": []map[string]any{{"type": "tool_use", "id": "x", "name": ProbeToolName, "input": map[string]int{"n": i}}}},
			map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "x", "content": "ok"}}})
	}
	b, err := json.Marshal(map[string]any{"model": "m", "max_tokens": 100, "stream": true, "messages": msgs})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func post(t *testing.T, url string, body []byte) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url+"/v1/messages?beta=true", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

func TestPlanFor(t *testing.T) {
	cfg := ModelConfig{Latency: 7 * time.Millisecond, ToolCallsPerRun: 5, ToolsPerTurn: 2, FloodToolsPerTurn: 6, FloodLatency: time.Millisecond}.withDefaults()
	tests := []struct {
		name      string
		title     string
		turn      int
		kind      replyKind
		calls     int
		latency   time.Duration
		mode      string
		runStart  bool
		roomMatch bool
	}{
		{"first turn calls tools", "plain", 0, replyTools, 2, 7 * time.Millisecond, "", true, true},
		{"second turn", "plain", 1, replyTools, 2, 7 * time.Millisecond, "", false, true},
		{"last tool turn gets the remainder", "plain", 2, replyTools, 1, 7 * time.Millisecond, "", false, true},
		{"then end_turn", "plain", 3, replyEnd, 0, 7 * time.Millisecond, "", false, true},
		{"and end_turn after a steer", "plain", 9, replyEnd, 0, 7 * time.Millisecond, "", false, true},
		{"quiet ends at once with no latency", "x " + ModeQuiet, 0, replyEnd, 0, 0, ModeQuiet, true, true},
		{"flood never ends", "x " + ModeFlood, 50, replyTools, 6, time.Millisecond, ModeFlood, false, true},
		{"propose after the tool turns", "x " + ModePropose, 3, replyPropose, 0, 7 * time.Millisecond, ModePropose, false, true},
		{"propose then end", "x " + ModePropose, 4, replyEnd, 0, 7 * time.Millisecond, ModePropose, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req modelRequest
			if err := json.Unmarshal(requestBody(t, "r_01ABC", tc.title, tc.turn), &req); err != nil {
				t.Fatal(err)
			}
			p := planFor(cfg, req)
			if p.kind != tc.kind || p.toolCalls != tc.calls || p.latency != tc.latency || p.mode != tc.mode || p.runStart != tc.runStart || p.turn != tc.turn {
				t.Fatalf("plan = %+v", p)
			}
			if (p.room == "r_01ABC") != tc.roomMatch {
				t.Fatalf("room = %q", p.room)
			}
		})
	}
	// No tool calls at all: end_turn on the first turn.
	var req modelRequest
	_ = json.Unmarshal(requestBody(t, "r_1", "plain", 0), &req)
	if p := planFor(ModelConfig{}.withDefaults(), req); p.kind != replyEnd {
		t.Fatalf("zero tool calls: %+v", p)
	}
}

func TestFakeModelDeterministic(t *testing.T) {
	m := NewFakeModel(ModelConfig{ToolCallsPerRun: 2, ToolsPerTurn: 1, OutputTokens: 50})
	srv := httptest.NewServer(m)
	defer srv.Close()
	body := requestBody(t, "r_01ABC", "plain", 1)
	c1, b1 := post(t, srv.URL, body)
	c2, b2 := post(t, srv.URL, body)
	if c1 != 200 || c2 != 200 {
		t.Fatalf("status %d %d", c1, c2)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("same request gave different replies")
	}
	// A fresh model with the same config gives the same bytes too.
	srv2 := httptest.NewServer(NewFakeModel(ModelConfig{ToolCallsPerRun: 2, ToolsPerTurn: 1, OutputTokens: 50}))
	defer srv2.Close()
	if _, b3 := post(t, srv2.URL, body); !bytes.Equal(b1, b3) {
		t.Fatal("reply depends on more than config and request")
	}
	// Another room's request gives another message id.
	if _, b4 := post(t, srv.URL, requestBody(t, "r_01XYZ", "plain", 1)); bytes.Equal(b1, b4) {
		t.Fatal("different requests gave identical replies")
	}
	_, end := post(t, srv.URL, requestBody(t, "r_01ABC", "plain", 2))
	if !strings.Contains(string(end), `"stop_reason":"end_turn"`) || !strings.Contains(string(b1), `"stop_reason":"tool_use"`) {
		t.Fatalf("stop reasons wrong:\n%s\n%s", b1, end)
	}
	st := m.Stats()
	rs := st.Rooms["r_01ABC"]
	if st.Requests != 4 || rs == nil || rs.Requests != 3 || rs.EndTurns != 1 || rs.ToolUses != 2 || rs.RunStarts != 0 {
		t.Fatalf("stats = %+v room %+v", st, rs)
	}
	m.Reset(ModelConfig{})
	if s := m.Stats(); s.Requests != 0 || len(s.Rooms) != 0 {
		t.Fatalf("Reset kept stats: %+v", s)
	}
}

// The real SDK, as the agent uses it (beta streaming + Accumulate), must read
// the fake's stream: tool_use blocks with their input, then a text end_turn.
func TestFakeModelSDKRoundTrip(t *testing.T) {
	m := NewFakeModel(ModelConfig{ToolCallsPerRun: 3, ToolsPerTurn: 2, OutputTokens: 40})
	srv := httptest.NewServer(m)
	defer srv.Close()
	client := anthropic.NewClient(option.WithBaseURL(srv.URL+"/"), option.WithAPIKey("test-key"), option.WithMaxRetries(0))
	msgs := []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(opening("r_01RT", "round trip")))}
	var stops []string
	var toolCalls int
	var finalText string
	for turn := 0; turn < 5; turn++ {
		stream := client.Beta.Messages.NewStreaming(context.Background(), anthropic.BetaMessageNewParams{Model: "m", MaxTokens: 100, Messages: msgs})
		var msg anthropic.BetaMessage
		for stream.Next() {
			if err := msg.Accumulate(stream.Current()); err != nil {
				t.Fatalf("accumulate: %v", err)
			}
		}
		if err := stream.Err(); err != nil {
			t.Fatalf("stream: %v", err)
		}
		stops = append(stops, string(msg.StopReason))
		if msg.Usage.OutputTokens != 40 {
			t.Fatalf("output tokens %d", msg.Usage.OutputTokens)
		}
		msgs = append(msgs, msg.ToParam())
		var results []anthropic.BetaContentBlockParamUnion
		for _, c := range msg.Content {
			switch c.Type {
			case "tool_use":
				toolCalls++
				var in struct {
					N   *int   `json:"n"`
					Why string `json:"why"`
				}
				if err := json.Unmarshal(c.Input, &in); err != nil || in.N == nil || c.Name != ProbeToolName {
					t.Fatalf("tool_use %s input %s: %v", c.Name, c.Input, err)
				}
				results = append(results, anthropic.NewBetaToolResultBlock(c.ID, "ok", false))
			case "text":
				finalText += c.Text
			}
		}
		if msg.StopReason == anthropic.BetaStopReasonEndTurn {
			break
		}
		msgs = append(msgs, anthropic.NewBetaUserMessage(results...))
	}
	if strings.Join(stops, ",") != "tool_use,tool_use,end_turn" || toolCalls != 3 {
		t.Fatalf("stops %v tool calls %d", stops, toolCalls)
	}
	if !strings.HasPrefix(finalText, "lt done. ") || len(finalText) != len("lt done. ")+4*40 {
		t.Fatalf("final text %q (%d bytes)", finalText, len(finalText))
	}
	rs := m.Stats().Rooms["r_01RT"]
	if rs == nil || rs.RunStarts != 1 || rs.EndTurns != 1 || rs.Requests != 3 {
		t.Fatalf("room stats %+v", rs)
	}
}

func TestFakeModelErrors(t *testing.T) {
	m := NewFakeModel(ModelConfig{})
	srv := httptest.NewServer(m)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/messages")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET: %d", resp.StatusCode)
	}
	for _, body := range []string{`not json`, `{"messages":[]}`, `{}`} {
		if code, _ := post(t, srv.URL, []byte(body)); code != http.StatusBadRequest {
			t.Fatalf("%s: status %d", body, code)
		}
	}
	if code, _ := post(t, srv.URL, bytes.Repeat([]byte("x"), maxModelRequest+10)); code != http.StatusBadRequest {
		t.Fatalf("oversized: %d", code)
	}
	if st := m.Stats(); st.BadRequests != 4 || st.Requests != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestFakeModelLatencyCancelAndInFlight(t *testing.T) {
	m := NewFakeModel(ModelConfig{Latency: 150 * time.Millisecond, ToolCallsPerRun: 1})
	srv := httptest.NewServer(m)
	defer srv.Close()
	body := requestBody(t, "r_01LAT", "plain", 0)

	t0 := time.Now()
	if code, _ := post(t, srv.URL, body); code != 200 {
		t.Fatalf("status %d", code)
	}
	if d := time.Since(t0); d < 150*time.Millisecond {
		t.Fatalf("reply after %s, want >= latency", d)
	}

	// Two concurrent requests for the same room: the duplicate-run detector.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			post(t, srv.URL, body)
		}()
	}
	wg.Wait()
	st := m.Stats()
	if rs := st.Rooms["r_01LAT"]; rs == nil || rs.MaxInFlight != 2 || rs.RunStarts != 3 || st.MaxInFlight != 2 {
		t.Fatalf("in-flight stats %+v %+v", st, rs)
	}

	// A caller that gives up stops the reply and is counted as canceled.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/messages", bytes.NewReader(body))
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("want a context error")
	}
	deadline := time.Now().Add(2 * time.Second)
	for m.Stats().Canceled != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("canceled = %d", m.Stats().Canceled)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCheckModelURL(t *testing.T) {
	for _, ok := range []string{"http://127.0.0.1:9/", "http://localhost:1234", "http://[::1]:80/"} {
		if err := checkModelURL(ok); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "https://api.anthropic.com", "http://10.0.0.1/", "http://example.com:80", "::bad"} {
		if err := checkModelURL(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestChildEnvDropsRealCredentials(t *testing.T) {
	env := childEnv([]string{"PATH=/bin", "ANTHROPIC_API_KEY=sk-real", "ANTHROPIC_BASE_URL=https://api.anthropic.com", "PIPEROOM_DB_URL=x",
		"AWS_SECRET_ACCESS_KEY=y", "LOADTEST_SERVER=old"}, "http://127.0.0.1:1/", []byte(`{}`), []string{"EXTRA=1"})
	got := strings.Join(env, "\n")
	for _, bad := range []string{"sk-real", "api.anthropic.com", "PIPEROOM_DB_URL", "AWS_SECRET", "LOADTEST_SERVER=old"} {
		if strings.Contains(got, bad) {
			t.Fatalf("child env keeps %q:\n%s", bad, got)
		}
	}
	for _, want := range []string{"PATH=/bin", "EXTRA=1", "ANTHROPIC_BASE_URL=http://127.0.0.1:1/", ServerEnv + "={}"} {
		if !strings.Contains(got, want) {
			t.Fatalf("child env lacks %q:\n%s", want, got)
		}
	}
}

func TestReadyWriter(t *testing.T) {
	ch := make(chan string, 1)
	w := &readyWriter{ready: ch}
	for _, chunk := range []string{"noise\nLOADTEST_RE", "ADY http://127.0.0.1:5/\nmore\n", "LOADTEST_READY http://other\n"} {
		if n, err := w.Write([]byte(chunk)); err != nil || n != len(chunk) {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	select {
	case u := <-ch:
		if u != "http://127.0.0.1:5/" {
			t.Fatalf("url %q", u)
		}
	default:
		t.Fatal("no ready line")
	}
	if len(ch) != 0 {
		t.Fatal("second ready line was passed on")
	}
}
