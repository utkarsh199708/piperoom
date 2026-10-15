package qamodel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/qamodel"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// newPuppet starts a puppet behind an httptest server. Cleanup closes the
// puppet first, so requests still parked return and the server can stop.
func newPuppet(t *testing.T, cfg qamodel.Config) (*qamodel.Puppet, *httptest.Server) {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = quietLogger()
	}
	p := qamodel.New(cfg)
	srv := httptest.NewServer(p)
	t.Cleanup(func() {
		p.Close()
		srv.Close()
	})
	return p, srv
}

// tester plays the person or agent that drives the control API with curl.
type tester struct {
	t    testing.TB
	base string
}

func (x tester) do(method, path string, body any) (int, []byte) {
	x.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			x.t.Fatalf("marshal %s %s: %v", method, path, err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, x.base+path, rd)
	if err != nil {
		x.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		x.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		x.t.Fatalf("%s %s: read: %v", method, path, err)
	}
	return resp.StatusCode, out
}

func (x tester) get(path string) (int, []byte) { x.t.Helper(); return x.do(http.MethodGet, path, nil) }

func (x tester) reply(id string, body any) (int, []byte) {
	x.t.Helper()
	return x.do(http.MethodPost, "/qa/turns/"+id+"/reply", body)
}

// mustReply posts a reply that must be accepted.
func (x tester) mustReply(id string, body any) {
	x.t.Helper()
	if code, out := x.reply(id, body); code != http.StatusOK {
		x.t.Fatalf("reply to %s: status %d: %s", id, code, out)
	}
}

// pending returns the pending turns once at least n are there.
func (x tester) pending(n int) []qamodel.PendingTurn {
	x.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		code, out := x.get("/qa/wait?timeout=2s")
		if code != http.StatusOK {
			x.t.Fatalf("GET /qa/wait: status %d: %s", code, out)
		}
		var list []qamodel.PendingTurn
		if err := json.Unmarshal(out, &list); err != nil {
			x.t.Fatalf("GET /qa/wait: %v: %s", err, out)
		}
		if len(list) >= n {
			return list
		}
		time.Sleep(10 * time.Millisecond)
	}
	x.t.Fatalf("fewer than %d pending turns after 10s", n)
	return nil
}

// pendingNow returns the pending turns without waiting.
func (x tester) pendingNow() []qamodel.PendingTurn {
	x.t.Helper()
	code, out := x.get("/qa/pending")
	var list []qamodel.PendingTurn
	if code != http.StatusOK || json.Unmarshal(out, &list) != nil {
		x.t.Fatalf("GET /qa/pending: %d %s", code, out)
	}
	return list
}

func (x tester) log() []qamodel.LogEntry {
	x.t.Helper()
	code, out := x.get("/qa/log")
	if code != http.StatusOK {
		x.t.Fatalf("GET /qa/log: status %d: %s", code, out)
	}
	var l []qamodel.LogEntry
	if err := json.Unmarshal(out, &l); err != nil {
		x.t.Fatalf("GET /qa/log: %v: %s", err, out)
	}
	return l
}

func (x tester) turnText(id, query string) string {
	x.t.Helper()
	code, out := x.get("/qa/turns/" + id + query)
	if code != http.StatusOK {
		x.t.Fatalf("GET /qa/turns/%s%s: status %d: %s", id, query, code, out)
	}
	return string(out)
}

// openingText is the agent's first user message for a room, in the shape of
// internal/agent firstUserMessage.
func openingText(room, title string) string {
	return "Investigate this pipeline incident.\n\nAlert: <untrusted>" + title + "</untrusted>\nPipeline: <untrusted>orders</untrusted>\nRoom: " + room + "\n\nWork through your method and report what you find."
}

var testTools = []anthropic.BetaToolUnionParam{
	{OfTool: &anthropic.BetaToolParam{
		Name:        "connect_status",
		Description: anthropic.String("Returns the state of a connector."),
		InputSchema: anthropic.BetaToolInputSchemaParam{
			Properties: map[string]any{"connector": map[string]any{"type": "string"}},
			Required:   []string{"connector"},
		},
	}},
	{OfTool: &anthropic.BetaToolParam{
		Name:        "consumer_lag",
		Description: anthropic.String("Returns the lag of a group."),
		InputSchema: anthropic.BetaToolInputSchemaParam{
			Properties: map[string]any{"group": map[string]any{"type": "string"}, "topic": map[string]any{"type": "string"}},
			Required:   []string{"group"},
		},
	}},
}

type sdkResult struct {
	msg anthropic.BetaMessage
	err error
}

// sdkCall sends one streaming request through the real SDK, as the agent does,
// and returns the accumulated message on the channel. The SDK does not retry.
func sdkCall(ctx context.Context, base string, params anthropic.BetaMessageNewParams) <-chan sdkResult {
	out := make(chan sdkResult, 1)
	go func() {
		client := anthropic.NewClient(option.WithBaseURL(base), option.WithAPIKey("qa-puppet"), option.WithMaxRetries(0))
		stream := client.Beta.Messages.NewStreaming(ctx, params)
		var msg anthropic.BetaMessage
		for stream.Next() {
			if err := msg.Accumulate(stream.Current()); err != nil {
				out <- sdkResult{msg: msg, err: err}
				return
			}
		}
		out <- sdkResult{msg: msg, err: stream.Err()}
	}()
	return out
}

// basicParams is a request with the opening message of room, the test tools
// and adaptive thinking, like the investigation loop's.
func basicParams(room, title string) anthropic.BetaMessageNewParams {
	return anthropic.BetaMessageNewParams{
		Model:     "claude-test-1",
		MaxTokens: 1000,
		System:    []anthropic.BetaTextBlockParam{{Text: "You are the test system prompt."}},
		Messages:  []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(openingText(room, title)))},
		Tools:     testTools,
		Thinking:  anthropic.BetaThinkingConfigParamUnion{OfAdaptive: &anthropic.BetaThinkingConfigAdaptiveParam{}},
	}
}

func await(t *testing.T, ch <-chan sdkResult) sdkResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("the model call did not return in 10s")
		return sdkResult{}
	}
}

// optBase points an SDK client at the puppet; unlike sdkCall it keeps the SDK's
// default retries, which is what the agent has.
func optBase(base string) []option.RequestOption {
	return []option.RequestOption{option.WithBaseURL(base), option.WithAPIKey("qa-puppet")}
}

// rawRequest is a minimal streaming Messages request body with one tool.
func rawRequest(t testing.TB, room, title string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"model": "claude-test-1", "max_tokens": 100, "stream": true, "system": "sys",
		"messages": []map[string]any{{"role": "user", "content": openingText(room, title)}},
		"tools": []map[string]any{{
			"name": "connect_status", "description": "d",
			"input_schema": map[string]any{
				"type": "object", "properties": map[string]any{"connector": map[string]any{"type": "string"}}, "required": []string{"connector"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
