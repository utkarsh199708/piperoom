package qamodel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/qamodel"
)

func assistantMsg(blocks ...anthropic.BetaContentBlockParamUnion) anthropic.BetaMessageParam {
	return anthropic.BetaMessageParam{Role: anthropic.BetaMessageParamRoleAssistant, Content: blocks}
}

func TestEmptyPendingIsAnEmptyList(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	code, out := tester{t, srv.URL}.get("/qa/pending")
	if code != http.StatusOK || strings.TrimSpace(string(out)) != "[]" {
		t.Fatalf("GET /qa/pending = %d %q, want 200 []", code, out)
	}
}

func TestPendingAndTurnRendering(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}

	long := strings.Repeat("lag-row ", 600) // 4800 bytes: over the clip
	params := basicParams("r_abc123", "orders stale")
	params.Messages = append(params.Messages,
		assistantMsg(anthropic.NewBetaToolUseBlock("toolu_1", map[string]any{"connector": "pg-orders"}, "connect_status")),
		anthropic.NewBetaUserMessage(anthropic.NewBetaToolResultBlock("toolu_1", long, false)),
		assistantMsg(anthropic.NewBetaTextBlock("Connector looks fine.")),
		anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock("steer: check the consumer lag "+long[:30])),
	)
	call := sdkCall(context.Background(), srv.URL, params)

	list := x.pending(1)
	if len(list) != 1 {
		t.Fatalf("pending = %d turns, want 1", len(list))
	}
	got := list[0]
	if got.ID != "t1" || got.Model != "claude-test-1" || got.MessagesCount != 5 || got.Structured {
		t.Errorf("pending turn = %+v", got)
	}
	if !strings.Contains(got.RoomHint, "r_abc123") || !strings.Contains(got.RoomHint, "orders stale") {
		t.Errorf("room_hint = %q, want the room id and the alert title", got.RoomHint)
	}
	if got.SystemBytes != len("You are the test system prompt.") {
		t.Errorf("system_bytes = %d", got.SystemBytes)
	}
	if strings.Join(got.Tools, ",") != "connect_status,consumer_lag" {
		t.Errorf("tools = %v", got.Tools)
	}
	if !got.DeadlineAt.After(got.ReceivedAt) {
		t.Errorf("deadline %v not after received %v", got.DeadlineAt, got.ReceivedAt)
	}

	text := x.turnText("t1", "")
	for _, want := range []string{
		"turn t1", "Room: r_abc123", "== SYSTEM (31 bytes) ==", "You are the test system prompt.",
		"== MESSAGES (5) ==", "--- [0] user ---", "--- [1] assistant ---",
		`[tool_use toolu_1 connect_status] {"connector":"pg-orders"}`, "[tool_result toolu_1 ok]",
		"--- [4] user ---", "steer: check the consumer lag",
		"connect_status(connector*)", "consumer_lag(group*, topic)", "thinking adaptive",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text rendering lacks %q:\n%s", want, text)
		}
	}
	if !strings.Contains(text, "more bytes; add ?full=1]") {
		t.Errorf("the long tool_result of an earlier message was not clipped:\n%s", text)
	}
	if strings.Contains(text, "input_schema") {
		t.Error("tool schemas shown without ?tools=1")
	}

	full := x.turnText("t1", "?full=1&system=0&tools=1&from=3")
	if strings.Contains(full, "You are the test system prompt.") {
		t.Error("?system=0 still shows the system prompt")
	}
	if strings.Contains(full, "--- [0] user ---") || strings.Contains(full, "--- [2] user ---") || !strings.Contains(full, "--- [3] assistant ---") {
		t.Errorf("?from=3 shows the wrong messages:\n%s", full)
	}
	if !strings.Contains(full, `input_schema: {`) || !strings.Contains(full, `"required":["connector"]`) {
		t.Errorf("?tools=1 lacks the compact schema:\n%s", full)
	}
	fullAll := x.turnText("t1", "?full=1")
	if strings.Contains(fullAll, "add ?full=1") || strings.Count(fullAll, "lag-row") < 600 {
		t.Error("?full=1 still clips")
	}

	code, raw := x.get("/qa/turns/t1?format=json")
	var parsed struct {
		Model    string `json:"model"`
		Messages []any  `json:"messages"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &parsed) != nil || parsed.Model != "claude-test-1" || len(parsed.Messages) != 5 {
		t.Errorf("?format=json = %d %.200s", code, raw)
	}

	x.mustReply("t1", map[string]any{"text": "done"})
	if r := await(t, call); r.err != nil {
		t.Fatalf("model call: %v", r.err)
	}
	if got := x.pendingNow(); len(got) != 0 {
		t.Errorf("a replied turn is still pending: %+v", got)
	}
}

func TestReplyStreamsToTheSDK(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
	id := x.pending(1)[0].ID

	// Not ASCII, and longer than one SSE chunk: a chunk may not cut a rune.
	text := strings.Repeat("Überprüfung der Verbindung. ", 30)
	code, out := x.reply(id, map[string]any{
		"thinking": "Check the connector, then the lag.",
		"text":     text,
		"tool_calls": []map[string]any{
			{"name": "connect_status", "input": map[string]any{"connector": "pg-orders"}},
			{"name": "consumer_lag", "input": map[string]any{"group": "order-sync"}},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("reply: %d %s", code, out)
	}
	if !strings.Contains(string(out), `"notes":[]`) || !strings.Contains(string(out), `"state":"replied"`) {
		t.Errorf("reply answer = %s, want state replied and an empty notes list", out)
	}
	r := await(t, call)
	if r.err != nil {
		t.Fatalf("model call: %v", r.err)
	}
	msg := r.msg
	if msg.StopReason != anthropic.BetaStopReasonToolUse {
		t.Errorf("stop_reason = %q, want tool_use (the default with tool calls)", msg.StopReason)
	}
	if msg.Model != "claude-test-1" || !strings.HasPrefix(msg.ID, "msg_qa_") {
		t.Errorf("model/id = %q/%q", msg.Model, msg.ID)
	}
	var kinds []string
	for _, b := range msg.Content {
		kinds = append(kinds, b.Type)
	}
	if strings.Join(kinds, ",") != "thinking,text,tool_use,tool_use" {
		t.Fatalf("content blocks = %v", kinds)
	}
	if msg.Content[0].Thinking != "Check the connector, then the lag." || msg.Content[0].Signature == "" {
		t.Errorf("thinking block = %q signature %q", msg.Content[0].Thinking, msg.Content[0].Signature)
	}
	if msg.Content[1].Text != text {
		t.Errorf("text did not survive chunking: got %d bytes, want %d", len(msg.Content[1].Text), len(text))
	}
	if msg.Content[2].Name != "connect_status" || string(msg.Content[2].Input) != `{"connector":"pg-orders"}` || msg.Content[2].ID != "toolu_qa_t1_0" {
		t.Errorf("first tool_use = %+v", msg.Content[2])
	}
	if msg.Content[3].Name != "consumer_lag" || msg.Content[3].ID != "toolu_qa_t1_1" {
		t.Errorf("second tool_use = %+v", msg.Content[3])
	}
	// Small, deterministic, and the same for the same request and reply.
	if msg.Usage.InputTokens != 110 || msg.Usage.OutputTokens <= 0 || msg.Usage.OutputTokens > 1000 {
		t.Errorf("usage = in %d out %d", msg.Usage.InputTokens, msg.Usage.OutputTokens)
	}

	// The reply is in the log with its resolved tool-call ids.
	l := x.log()
	if len(l) != 1 || l[0].State != qamodel.StateReplied || l[0].Reply == nil || len(l[0].Reply.ToolCalls) != 2 || l[0].Delivered == nil || !*l[0].Delivered {
		t.Fatalf("log = %+v", l)
	}
}

func TestStopReasonsAndNotes(t *testing.T) {
	tests := []struct {
		name     string
		reply    map[string]any
		wantStop anthropic.BetaStopReason
	}{
		{"text only defaults to end_turn", map[string]any{"text": "all good"}, anthropic.BetaStopReasonEndTurn},
		{"max_tokens", map[string]any{"text": "cut", "stop": "max_tokens"}, anthropic.BetaStopReasonMaxTokens},
		{"pause_turn", map[string]any{"text": "pause", "stop": "pause_turn"}, anthropic.BetaStopReasonPauseTurn},
		{"refusal without text", map[string]any{"stop": "refusal"}, anthropic.BetaStopReasonRefusal},
		{"context window", map[string]any{"text": "x", "stop": "model_context_window_exceeded"}, anthropic.BetaStopReasonModelContextWindowExceeded},
		{"end_turn with a tool call", map[string]any{"stop": "end_turn", "tool_calls": []map[string]any{{"name": "connect_status", "input": map[string]any{"connector": "c"}}}}, anthropic.BetaStopReasonEndTurn},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, srv := newPuppet(t, qamodel.Config{})
			x := tester{t, srv.URL}
			call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
			x.mustReply(x.pending(1)[0].ID, tt.reply)
			r := await(t, call)
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.msg.StopReason != tt.wantStop {
				t.Errorf("stop_reason = %q, want %q", r.msg.StopReason, tt.wantStop)
			}
		})
	}
}

func TestThinkingIgnoredWhenNotEnabled(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	params := basicParams("r_1", "t")
	params.Thinking = anthropic.BetaThinkingConfigParamUnion{} // not enabled
	call := sdkCall(context.Background(), srv.URL, params)
	code, out := x.reply(x.pending(1)[0].ID, map[string]any{"text": "ok", "thinking": "hmm"})
	if code != http.StatusOK || !strings.Contains(string(out), "thinking ignored") {
		t.Fatalf("reply = %d %s, want an ignored-thinking note", code, out)
	}
	r := await(t, call)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if len(r.msg.Content) != 1 || r.msg.Content[0].Type != "text" {
		t.Errorf("content = %+v, want one text block", r.msg.Content)
	}
}

// A tool input that misses a required field is a case worth sending (the
// agent validates it), so the puppet sends it and only says so.
func TestMissingRequiredFieldIsANoteNotAnError(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
	code, out := x.reply(x.pending(1)[0].ID, map[string]any{
		"tool_calls": []map[string]any{{"name": "connect_status", "input": map[string]any{"wrong": 1}}, {"name": "consumer_lag"}},
	})
	if code != http.StatusOK {
		t.Fatalf("reply = %d %s", code, out)
	}
	var body struct {
		Notes []string `json:"notes"`
	}
	if err := json.Unmarshal(out, &body); err != nil || len(body.Notes) != 2 ||
		!strings.Contains(body.Notes[0], "connect_status") || !strings.Contains(body.Notes[0], "connector") ||
		!strings.Contains(body.Notes[1], "group") {
		t.Errorf("notes = %v (%v)", body.Notes, err)
	}
	r := await(t, call)
	if r.err != nil || len(r.msg.Content) != 2 || string(r.msg.Content[0].Input) != `{"wrong":1}` || string(r.msg.Content[1].Input) != `{}` {
		t.Errorf("the calls were not sent as given: %v %+v", r.err, r.msg.Content)
	}
}

func TestUsageOverride(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
	x.mustReply(x.pending(1)[0].ID, map[string]any{
		"text":  "big",
		"usage": map[string]any{"input_tokens": 123456, "cache_read_input_tokens": 7000},
	})
	r := await(t, call)
	if r.err != nil {
		t.Fatal(r.err)
	}
	u := r.msg.Usage
	if u.InputTokens != 123456 || u.CacheReadInputTokens != 7000 || u.OutputTokens <= 0 {
		t.Errorf("usage = in %d cache_read %d out %d", u.InputTokens, u.CacheReadInputTokens, u.OutputTokens)
	}
}

func TestErrorReply(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	// The SDK retries a 529 unless told not to; the puppet tells it not to, so
	// one injected error is one turn.
	client := anthropic.NewClient(optBase(srv.URL)...)
	errc := make(chan error, 1)
	go func() {
		stream := client.Beta.Messages.NewStreaming(context.Background(), basicParams("r_1", "t"))
		for stream.Next() {
		}
		errc <- stream.Err()
	}()
	x.mustReply(x.pending(1)[0].ID, map[string]any{"error": map[string]any{"status": 529}})
	select {
	case err := <-errc:
		var apiErr *anthropic.Error
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 529 || !strings.Contains(apiErr.Error(), "overloaded_error") {
			t.Fatalf("err = %v, want a 529 overloaded_error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no error reached the client")
	}
	if l := x.log(); len(l) != 1 {
		t.Errorf("the SDK retried an injected error: %d turns in the log", len(l))
	}
}

func TestErrorReplyRetryHeader(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	for _, tt := range []struct {
		retry bool
		want  string
	}{{false, "false"}, {true, "true"}} {
		done := make(chan *http.Response, 1)
		go func() {
			resp, err := http.Post(srv.URL+"/v1/messages?beta=true", "application/json", bytes.NewReader(rawRequest(t, "r_1", "t")))
			if err != nil {
				t.Error(err)
				done <- nil
				return
			}
			done <- resp
		}()
		list := x.pending(1)
		x.mustReply(list[len(list)-1].ID, map[string]any{"error": map[string]any{"status": 429, "retry": tt.retry, "message": "slow down"}})
		resp := <-done
		if resp == nil {
			return
		}
		body := new(bytes.Buffer)
		_, _ = body.ReadFrom(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 429 || resp.Header.Get("x-should-retry") != tt.want || !strings.Contains(body.String(), "rate_limit_error") || !strings.Contains(body.String(), "slow down") {
			t.Errorf("retry=%v: %d x-should-retry=%q body=%s", tt.retry, resp.StatusCode, resp.Header.Get("x-should-retry"), body)
		}
	}
}

func TestReplyValidation(t *testing.T) {
	tests := []struct {
		name     string
		body     any
		wantMsg  string
		wantTool bool // the error lists the request's tools
	}{
		{"unknown tool", map[string]any{"tool_calls": []map[string]any{{"name": "conect_status", "input": map[string]any{}}}}, `unknown tool "conect_status"`, true},
		{"missing tool name", map[string]any{"tool_calls": []map[string]any{{"input": map[string]any{}}}}, "name is required", true},
		{"input not an object", map[string]any{"tool_calls": []map[string]any{{"name": "connect_status", "input": []int{1}}}}, "input must be a JSON object", false},
		{"tool_use without calls", map[string]any{"text": "x", "stop": "tool_use"}, "needs at least one entry in tool_calls", true},
		{"bad stop", map[string]any{"text": "x", "stop": "done"}, `stop "done"`, false},
		{"empty reply", map[string]any{}, "empty reply", false},
		{"unknown field", `{"toolcalls": []}`, `unknown field "toolcalls"`, false},
		{"not json", `not json`, "bad reply JSON", false},
		{"two documents", `{"text":"a"} {"text":"b"}`, "unexpected data after the JSON reply", false},
		{"error with text", map[string]any{"text": "x", "error": map[string]any{"status": 500}}, "error cannot be combined", false},
		{"error status out of range", map[string]any{"error": map[string]any{"status": 200}}, "error.status 200", false},
		{"usage out of range", map[string]any{"text": "x", "usage": map[string]any{"input_tokens": -1}}, "usage.input_tokens -1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, srv := newPuppet(t, qamodel.Config{})
			x := tester{t, srv.URL}
			call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
			id := x.pending(1)[0].ID

			code, out := x.reply(id, tt.body)
			var body struct {
				Error string   `json:"error"`
				Tools []string `json:"tools"`
			}
			if err := json.Unmarshal(out, &body); err != nil {
				t.Fatalf("error body is not JSON: %s", out)
			}
			if code != http.StatusBadRequest || !strings.Contains(body.Error, tt.wantMsg) {
				t.Fatalf("reply = %d %s, want 400 containing %q", code, out, tt.wantMsg)
			}
			if tt.wantTool && strings.Join(body.Tools, ",") != "connect_status,consumer_lag" {
				t.Errorf("error lists tools %v, want the request's tools", body.Tools)
			}
			// A rejected reply changes nothing: the turn is still pending and
			// can be answered properly.
			if got := x.pending(1); len(got) != 1 || got[0].ID != id {
				t.Fatalf("pending after a rejected reply = %+v", got)
			}
			x.mustReply(id, map[string]any{"text": "fixed"})
			if r := await(t, call); r.err != nil {
				t.Fatal(r.err)
			}
		})
	}
}

func TestToolCallsOnARequestWithoutTools(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	params := basicParams("r_1", "t")
	params.Tools = nil
	call := sdkCall(context.Background(), srv.URL, params)
	id := x.pending(1)[0].ID
	code, out := x.reply(id, map[string]any{"tool_calls": []map[string]any{{"name": "connect_status"}}})
	if code != http.StatusBadRequest || !strings.Contains(string(out), "offers no tools") {
		t.Fatalf("reply = %d %s", code, out)
	}
	x.mustReply(id, map[string]any{"text": "plain"})
	if r := await(t, call); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestReplyToWrongTurnStates(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{ReplyTimeout: 80 * time.Millisecond})
	x := tester{t, srv.URL}

	if code, out := x.reply("t99", map[string]any{"text": "x"}); code != http.StatusNotFound || !strings.Contains(string(out), `"error"`) {
		t.Errorf("reply to an unknown turn = %d %s, want 404 JSON error", code, out)
	}
	if code, _ := x.get("/qa/turns/t99"); code != http.StatusNotFound {
		t.Errorf("GET unknown turn = %d, want 404", code)
	}

	// Timed out: the agent got the fallback; a late reply is refused.
	call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
	r := await(t, call)
	if r.err != nil {
		t.Fatal(r.err)
	}
	code, out := x.reply("t1", map[string]any{"text": "too late"})
	if code != http.StatusConflict || !strings.Contains(string(out), "timeout") {
		t.Errorf("late reply = %d %s, want 409 naming the timeout", code, out)
	}
	if code, out := x.get("/qa/turns/t1"); code != http.StatusGone || !strings.Contains(string(out), "timeout") {
		t.Errorf("GET finished turn = %d %s, want 410", code, out)
	}
}

func TestDoubleReply(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
	id := x.pending(1)[0].ID
	x.mustReply(id, map[string]any{"text": "first"})
	code, out := x.reply(id, map[string]any{"text": "second"})
	if code != http.StatusConflict || !strings.Contains(string(out), "already replied") {
		t.Errorf("second reply = %d %s, want 409", code, out)
	}
	r := await(t, call)
	if r.err != nil || r.msg.Content[0].Text != "first" {
		t.Errorf("the agent got %+v %v, want the first reply", r.msg.Content, r.err)
	}
}

func TestDeadlineFallback(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{ReplyTimeout: 60 * time.Millisecond})
	x := tester{t, srv.URL}
	start := time.Now()
	r := await(t, sdkCall(context.Background(), srv.URL, basicParams("r_1", "t")))
	if r.err != nil {
		t.Fatalf("the fallback must not be an error: %v", r.err)
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Errorf("answered after %v, before the deadline", d)
	}
	if r.msg.StopReason != anthropic.BetaStopReasonEndTurn || len(r.msg.Content) != 1 || r.msg.Content[0].Text != "qamodel: no reply in time" {
		t.Errorf("fallback = %q %+v", r.msg.StopReason, r.msg.Content)
	}
	if got := x.pendingNow(); len(got) != 0 {
		t.Errorf("a timed-out turn is still pending: %+v", got)
	}
	l := x.log()
	if len(l) != 1 || l[0].State != qamodel.StateTimeout || l[0].Reply == nil || !l[0].Reply.Fallback {
		t.Fatalf("log = %+v, want one timeout entry with the fallback reply", l)
	}
}

func TestClientGoneCancelsTheTurn(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	ctx, cancel := context.WithCancel(context.Background())
	call := sdkCall(ctx, srv.URL, basicParams("r_1", "t"))
	id := x.pending(1)[0].ID
	cancel()
	if r := await(t, call); r.err == nil {
		t.Fatal("a canceled call returned no error")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if l := x.log(); len(l) == 1 && l[0].State == qamodel.StateCanceled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the turn was never marked canceled: %+v", x.log())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := x.pendingNow(); len(got) != 0 {
		t.Errorf("a canceled turn is still pending: %+v", got)
	}
	if code, out := x.reply(id, map[string]any{"text": "x"}); code != http.StatusConflict || !strings.Contains(string(out), "canceled") {
		t.Errorf("reply to a canceled turn = %d %s, want 409", code, out)
	}
}

func TestWait(t *testing.T) {
	t.Run("times out with an empty list", func(t *testing.T) {
		_, srv := newPuppet(t, qamodel.Config{})
		x := tester{t, srv.URL}
		start := time.Now()
		code, out := x.get("/qa/wait?timeout=80ms")
		if code != http.StatusOK || strings.TrimSpace(string(out)) != "[]" {
			t.Fatalf("wait = %d %s", code, out)
		}
		if d := time.Since(start); d < 70*time.Millisecond || d > 2*time.Second {
			t.Errorf("wait took %v", d)
		}
	})
	t.Run("returns when a turn arrives", func(t *testing.T) {
		_, srv := newPuppet(t, qamodel.Config{})
		x := tester{t, srv.URL}
		type res struct {
			code int
			out  []byte
		}
		got := make(chan res, 1)
		go func() {
			code, out := x.get("/qa/wait?timeout=30s")
			got <- res{code, out}
		}()
		time.Sleep(50 * time.Millisecond) // the wait is parked now
		call := sdkCall(context.Background(), srv.URL, basicParams("r_wait", "t"))
		select {
		case r := <-got:
			var list []qamodel.PendingTurn
			if r.code != http.StatusOK || json.Unmarshal(r.out, &list) != nil || len(list) != 1 || !strings.Contains(list[0].RoomHint, "r_wait") {
				t.Fatalf("wait = %d %s", r.code, r.out)
			}
			x.mustReply(list[0].ID, map[string]any{"text": "ok"})
		case <-time.After(5 * time.Second):
			t.Fatal("wait did not return when a turn arrived")
		}
		await(t, call)
	})
	t.Run("returns at once when a turn is already pending", func(t *testing.T) {
		_, srv := newPuppet(t, qamodel.Config{})
		x := tester{t, srv.URL}
		sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
		x.pending(1)
		start := time.Now()
		if code, out := x.get("/qa/wait?timeout=30s"); code != http.StatusOK || !strings.Contains(string(out), `"t1"`) {
			t.Fatalf("wait = %d %s", code, out)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("wait took %v with a turn pending", d)
		}
	})
	t.Run("rejects a bad timeout", func(t *testing.T) {
		_, srv := newPuppet(t, qamodel.Config{})
		x := tester{t, srv.URL}
		for _, q := range []string{"abc", "0s", "-5s", "99h"} {
			if code, out := x.get("/qa/wait?timeout=" + q); code != http.StatusBadRequest || !strings.Contains(string(out), `"error"`) {
				t.Errorf("timeout=%s: %d %s, want 400 JSON error", q, code, out)
			}
		}
		if code, _ := x.get("/qa/wait?timeout=0.05"); code != http.StatusOK { // bare seconds
			t.Errorf("timeout=0.05 (seconds): %d", code)
		}
	})
	t.Run("close releases a waiting caller", func(t *testing.T) {
		p, srv := newPuppet(t, qamodel.Config{})
		x := tester{t, srv.URL}
		waiting := make(chan struct{})
		go func() {
			defer close(waiting)
			x.get("/qa/wait?timeout=30s")
		}()
		time.Sleep(50 * time.Millisecond)
		p.Close()
		select {
		case <-waiting:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not release /qa/wait")
		}
	})
	t.Run("close answers a parked request with 503", func(t *testing.T) {
		p, srv := newPuppet(t, qamodel.Config{})
		x := tester{t, srv.URL}
		call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
		x.pending(1)
		p.Close()
		r := await(t, call)
		var apiErr *anthropic.Error
		if !errors.As(r.err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("the parked request got %v, want 503", r.err)
		}
		if l := x.log(); len(l) != 1 || l[0].State != qamodel.StateCanceled {
			t.Errorf("log = %+v, want one canceled turn", l)
		}
	})
}

func TestBodyCaps(t *testing.T) {
	p, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}

	// The oversize cases call the handler directly: over a socket the server
	// answers before the client has sent the rest and closes the connection,
	// which a client may report as a reset instead of the 413.
	t.Run("model request over 16 MiB", func(t *testing.T) {
		big := `{"model":"m","stream":true,"messages":[{"role":"user","content":"` + strings.Repeat("a", qamodel.MaxModelRequestBytes) + `"}]}`
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", strings.NewReader(big)))
		var body struct {
			Type  string `json:"type"`
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if rec.Code != http.StatusRequestEntityTooLarge || body.Type != "error" || body.Error.Type != "request_too_large" {
			t.Errorf("oversized model request = %d %+v", rec.Code, body)
		}
		if got := x.pendingNow(); len(got) != 0 {
			t.Errorf("an oversized request was parked: %+v", got)
		}
	})

	t.Run("control body over 1 MiB", func(t *testing.T) {
		call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
		id := x.pending(1)[0].ID
		rec := httptest.NewRecorder()
		big := `{"text":"` + strings.Repeat("a", qamodel.MaxControlBytes) + `"}`
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/qa/turns/"+id+"/reply", strings.NewReader(big)))
		if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("oversized reply = %d %.200s, want 413 JSON error", rec.Code, rec.Body)
		}
		if got := x.pendingNow(); len(got) != 1 {
			t.Errorf("an oversized reply changed the turn: %+v", got)
		}
		x.mustReply(id, map[string]any{"text": "fits"})
		await(t, call)
	})

	t.Run("a reply just under 1 MiB is accepted", func(t *testing.T) {
		call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
		list := x.pending(1)
		x.mustReply(list[0].ID, map[string]any{"text": strings.Repeat("b", qamodel.MaxControlBytes-1024)})
		r := await(t, call)
		if r.err != nil || len(r.msg.Content) != 1 || len(r.msg.Content[0].Text) != qamodel.MaxControlBytes-1024 {
			t.Errorf("large reply: %v", r.err)
		}
	})
}

func TestModelEndpointRejections(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	valid := string(rawRequest(t, "r_1", "t"))
	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"GET on messages", http.MethodGet, "/v1/messages", "", http.StatusMethodNotAllowed},
		{"not JSON", http.MethodPost, "/v1/messages", "nope", http.StatusBadRequest},
		{"no messages", http.MethodPost, "/v1/messages", `{"model":"m","stream":true,"messages":[]}`, http.StatusBadRequest},
		{"not streaming", http.MethodPost, "/v1/messages", strings.Replace(valid, `"stream":true`, `"stream":false`, 1), http.StatusBadRequest},
		{"unknown path", http.MethodPost, "/v1/other", valid, http.StatusNotFound},
		{"prefixed base url", http.MethodGet, "/proxy/v1/messages", "", http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := x.do(tt.method, tt.path, tt.body)
			var body struct {
				Type string `json:"type"`
			}
			if code != tt.want || json.Unmarshal(out, &body) != nil || body.Type != "error" {
				t.Errorf("%s %s = %d %s, want %d with an API error body", tt.method, tt.path, code, out, tt.want)
			}
		})
	}
}

func TestControlRejections(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	tests := []struct {
		name, method, path string
		want               int
	}{
		{"unknown endpoint", http.MethodGet, "/qa/nope", http.StatusNotFound},
		{"POST on pending", http.MethodPost, "/qa/pending", http.StatusMethodNotAllowed},
		{"GET on reply", http.MethodGet, "/qa/turns/t1/reply", http.StatusMethodNotAllowed},
		{"bad format", http.MethodGet, "/qa/turns/t1?format=xml", http.StatusBadRequest},
		{"bad from", http.MethodGet, "/qa/turns/t1?from=x", http.StatusBadRequest},
		{"bad log n", http.MethodGet, "/qa/log?n=0", http.StatusBadRequest},
		{"index", http.MethodGet, "/qa/", http.StatusOK},
		{"index without slash", http.MethodGet, "/qa", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := x.do(tt.method, tt.path, nil)
			if code != tt.want || !json.Valid(out) {
				t.Errorf("%s %s = %d %s, want %d with a JSON body", tt.method, tt.path, code, out, tt.want)
			}
		})
	}
}

func TestLogKeepsTheLastFinishedTurns(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{LogSize: 3})
	x := tester{t, srv.URL}
	for i := 0; i < 5; i++ {
		call := sdkCall(context.Background(), srv.URL, basicParams("r_1", "t"))
		list := x.pending(1)
		x.mustReply(list[0].ID, map[string]any{"text": "ok"})
		await(t, call)
	}
	// One more stays pending: pending turns are never dropped.
	sdkCall(context.Background(), srv.URL, basicParams("r_pending", "t"))
	x.pending(1)

	l := x.log()
	var ids []string
	for _, e := range l {
		ids = append(ids, e.ID)
	}
	if strings.Join(ids, ",") != "t3,t4,t5,t6" {
		t.Fatalf("log ids = %v, want the last 3 finished plus the pending one", ids)
	}
	if l[0].State != qamodel.StateReplied || l[3].State != qamodel.StatePending {
		t.Errorf("states = %s ... %s", l[0].State, l[3].State)
	}
	if !strings.Contains(l[0].LastMessage, "Investigate this pipeline incident") {
		t.Errorf("last_message = %q", l[0].LastMessage)
	}
	if code, _ := x.get("/qa/turns/t1"); code != http.StatusNotFound {
		t.Errorf("a turn older than the log = %d, want 404", code)
	}
	var last []qamodel.LogEntry
	_, out := x.get("/qa/log?n=2")
	if err := json.Unmarshal(out, &last); err != nil || len(last) != 2 || last[1].ID != "t6" {
		t.Errorf("/qa/log?n=2 = %s", out)
	}
}

func TestStructuredRequestIsFlagged(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	params := basicParams("r_1", "t")
	params.Tools = nil
	params.OutputConfig = anthropic.BetaOutputConfigParam{
		Format: anthropic.BetaJSONOutputFormatParam{Schema: map[string]any{"type": "object", "required": []string{"what_broke"}}},
	}
	call := sdkCall(context.Background(), srv.URL, params)
	list := x.pending(1)
	if !list[0].Structured {
		t.Errorf("a request with an output schema is not marked structured: %+v", list[0])
	}
	text := x.turnText(list[0].ID, "")
	if !strings.Contains(text, "demands JSON output") || !strings.Contains(text, `"required":["what_broke"]`) {
		t.Errorf("rendering lacks the output schema:\n%s", text)
	}
	x.mustReply(list[0].ID, map[string]any{"text": `{"what_broke":"x"}`})
	await(t, call)
}

func TestRenderingUnknownBlocksAndErrors(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	body := `{"model":"m","max_tokens":10,"stream":true,"system":[{"type":"text","text":"sys A"},{"type":"text","text":"sys B"}],"messages":[
		{"role":"user","content":"plain string opening\nRoom: r_str"},
		{"role":"assistant","content":[{"type":"compaction","content":"summary of the past"},{"type":"tool_use","id":"toolu_9","name":"x","input":{}}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_9","is_error":true,"content":[{"type":"text","text":"boom\u001b[2J"}]}]}]}`
	go func() { // released by the puppet's Close in cleanup
		if resp, err := http.Post(srv.URL+"/v1/messages", "application/json", strings.NewReader(body)); err == nil {
			resp.Body.Close()
		}
	}()
	list := x.pending(1)
	text := x.turnText(list[0].ID, "")
	if strings.ContainsRune(text, '\x1b') || !strings.Contains(text, `\x1b[2J`) {
		t.Errorf("a terminal escape in tool output is shown raw:\n%q", text)
	}
	for _, want := range []string{"sys A\nsys B", "plain string opening", "[compaction]\nsummary of the past", "[tool_result toolu_9 error]\nboom"} {
		if !strings.Contains(text, want) {
			t.Errorf("rendering lacks %q:\n%s", want, text)
		}
	}
	if list[0].RoomHint != "r_str" {
		t.Errorf("room_hint = %q, want r_str", list[0].RoomHint)
	}
}
