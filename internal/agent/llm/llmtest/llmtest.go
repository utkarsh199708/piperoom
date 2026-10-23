// Package llmtest is the conformance suite every llm.Model adapter passes
// (docs/design/model-providers.md section 4): an adapter's test supplies a
// Fixture that speaks its provider's wire format, and Run drives the adapter
// against an httptest server through the cases the agent relies on: a text
// turn, parallel tool calls, the history going back with tool results that
// arrive byte for byte, a refusal, the error kinds, no answer at all, the
// caller's deadline, and Capabilities without a network call.
//
// Each later adapter (OpenAI-compatible, Gemini, Bedrock Converse) adds its
// own provider-specific cases next to its Fixture; this suite holds only what
// every provider must do the same way.
package llmtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent/llm"
)

// Call is one tool call of a scripted answer.
type Call struct {
	ID    string
	Name  string
	Input string // a JSON object
}

// Answer is one scripted assistant turn, in neutral terms; the fixture
// renders it in its provider's wire format.
type Answer struct {
	Text    string
	Calls   []Call
	Refusal bool // the model refused (Text, when set, is the provider's explanation)
	// InputTokens and OutputTokens are the usage the answer reports.
	InputTokens, OutputTokens int64
}

// Fixture is one adapter's wire format.
type Fixture struct {
	// Provider is what the adapter's Provider() reports.
	Provider string
	// Model is the model id the suite sends.
	Model string
	// New builds the adapter pointed at baseURL. It must turn the
	// provider's own retries off: every error case answers the same error
	// each time and counts the requests (one per Turn).
	New func(t *testing.T, baseURL string) llm.Model
	// Render writes a as the provider's HTTP response (status 200).
	Render func(t *testing.T, a Answer) (contentType string, body []byte)
	// RenderError writes an error answer with status whose body carries
	// message.
	RenderError func(status int, message string) (contentType string, body []byte)
	// ToolResults reads, from a request body the adapter sent, the content of
	// every tool result in order, decoded to the string the model reads.
	ToolResults func(t *testing.T, body []byte) []string
}

// server answers each request with the next scripted reply and records the
// request bodies.
type server struct {
	*httptest.Server
	mu      sync.Mutex
	bodies  [][]byte
	replies []func(w http.ResponseWriter, r *http.Request)
}

func newServer(t *testing.T) *server {
	t.Helper()
	s := &server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.bodies = append(s.bodies, b)
		var reply func(http.ResponseWriter, *http.Request)
		if len(s.replies) > 0 {
			reply = s.replies[0]
			if len(s.replies) > 1 {
				s.replies = s.replies[1:]
			}
		}
		s.mu.Unlock()
		if reply == nil {
			http.Error(w, "llmtest: no reply scripted", http.StatusTeapot)
			return
		}
		reply(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *server) script(replies ...func(w http.ResponseWriter, r *http.Request)) {
	s.mu.Lock()
	s.replies = replies
	s.mu.Unlock()
}

func (s *server) requests() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.bodies...)
}

func reply(contentType string, status int, body []byte) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

func (f Fixture) answer(t *testing.T, a Answer) func(http.ResponseWriter, *http.Request) {
	ct, body := f.Render(t, a)
	return reply(ct, http.StatusOK, body)
}

// request is a minimal request with one user text.
func (f Fixture) request(text string, tools ...llm.ToolDef) llm.Request {
	return llm.Request{
		Model:     f.Model,
		System:    "You are the conformance suite.",
		MaxTokens: 256,
		Tools:     tools,
		Messages:  []llm.Message{{Role: llm.RoleUser, Parts: []llm.Part{{Kind: llm.PartText, Text: text}}}},
	}
}

var readTool = llm.ToolDef{
	Name:        "connect_status",
	Description: "Returns the state of a connector.",
	Schema: map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"connector": map[string]any{"type": "string"}},
		"required":             []string{"connector"},
		"additionalProperties": false,
	},
}

// Run runs the suite against the adapter f builds.
func Run(t *testing.T, f Fixture) {
	t.Run("provider and capabilities", func(t *testing.T) { capabilities(t, f) })
	t.Run("text turn", func(t *testing.T) { textTurn(t, f) })
	t.Run("parallel tool calls and history", func(t *testing.T) { toolCalls(t, f) })
	t.Run("refusal", func(t *testing.T) { refusal(t, f) })
	t.Run("error kinds", func(t *testing.T) { errorKinds(t, f) })
	t.Run("no answer", func(t *testing.T) { noAnswer(t, f) })
	t.Run("deadline", func(t *testing.T) { deadline(t, f) })
}

func capabilities(t *testing.T, f Fixture) {
	s := newServer(t)
	m := f.New(t, s.URL)
	if got := m.Provider(); got != f.Provider {
		t.Fatalf("Provider() = %q, want %q", got, f.Provider)
	}
	c := m.Capabilities(f.Model)
	if !c.ToolCalls {
		t.Fatal("an adapter the loop can run on calls tools")
	}
	if c.MaxStrictTools < 0 || c.MaxStrictBytes < 0 || c.ContextTokens < 0 || c.MaxTools < 0 || c.ToolNameMax < 0 {
		t.Fatalf("negative capability: %+v", c)
	}
	if n := len(s.requests()); n != 0 {
		t.Fatalf("Capabilities made %d requests; it must never call the network", n)
	}
}

func textOf(m llm.Message) string {
	var b strings.Builder
	for _, p := range m.Parts {
		if p.Kind == llm.PartText {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

func textTurn(t *testing.T, f Fixture) {
	s := newServer(t)
	s.script(f.answer(t, Answer{Text: "The connector task failed.", InputTokens: 120, OutputTokens: 9}))
	resp, err := f.New(t, s.URL).Turn(context.Background(), f.request("Why is the connector down?"))
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if resp.Stop != llm.StopEndTurn {
		t.Fatalf("Stop = %q (%q), want end_turn", resp.Stop, resp.ProviderStop)
	}
	if resp.Message.Role != llm.RoleAssistant {
		t.Fatalf("Role = %q, want assistant", resp.Message.Role)
	}
	if got := textOf(resp.Message); got != "The connector task failed." {
		t.Fatalf("text = %q", got)
	}
	if resp.Usage.Input+resp.Usage.CacheRead+resp.Usage.CacheWrite == 0 || resp.Usage.Output == 0 {
		t.Fatalf("usage not reported: %+v", resp.Usage)
	}
	for _, p := range resp.Message.Parts {
		if p.Native != nil && p.Native.Provider != f.Provider {
			t.Fatalf("a native part names provider %q, want %q", p.Native.Provider, f.Provider)
		}
	}
}

func toolCalls(t *testing.T, f Fixture) {
	s := newServer(t)
	calls := []Call{
		{ID: "call_1", Name: "connect_status", Input: `{"connector":"pg-orders"}`},
		{ID: "call_2", Name: "connect_status", Input: `{"connector":"pg-payments"}`},
	}
	s.script(
		f.answer(t, Answer{Text: "Reading both connectors.", Calls: calls, InputTokens: 200, OutputTokens: 40}),
		f.answer(t, Answer{Text: "Both are running.", InputTokens: 260, OutputTokens: 6}),
	)
	m := f.New(t, s.URL)
	req := f.request("Check both connectors.", readTool)
	resp, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if resp.Stop != llm.StopToolUse {
		t.Fatalf("Stop = %q (%q), want tool_use", resp.Stop, resp.ProviderStop)
	}
	var got []llm.ToolCall
	for _, p := range resp.Message.Parts {
		if p.Kind == llm.PartToolCall {
			if p.ToolCall == nil {
				t.Fatal("a tool_call part without its call")
			}
			got = append(got, *p.ToolCall)
		}
	}
	if len(got) != len(calls) {
		t.Fatalf("tool calls = %d, want %d", len(got), len(calls))
	}
	for i, c := range got {
		if c.ID == "" || c.Name != calls[i].Name || !sameJSON(t, c.Input, calls[i].Input) {
			t.Fatalf("call %d = %+v, want %+v", i, c, calls[i])
		}
	}

	// The turn goes back as it came, followed by the results. A result's
	// content is already wrapped by the agent; the adapter must not alter
	// one byte of it (escaping on the wire is fine, the decoded text is not).
	contents := []string{
		`<tool_output tool="connect_status" step="s_1" untrusted="true">RUNNING; task 0 RUNNING</tool_output>`,
		"<tool_output tool=\"connect_status\" step=\"s_2\" untrusted=\"true\">error: 404 \"pg-payments\" not found &amp;   tab\t</tool_output>",
	}
	results := llm.Message{Role: llm.RoleUser}
	for i, c := range got {
		results.Parts = append(results.Parts, llm.Part{Kind: llm.PartToolResult, ToolResult: &llm.ToolResult{
			CallID: c.ID, Name: c.Name, Content: contents[i], IsError: i == 1,
		}})
	}
	req.Messages = append(req.Messages, resp.Message, results)
	resp2, err := m.Turn(context.Background(), req)
	if err != nil {
		t.Fatalf("second Turn: %v", err)
	}
	if textOf(resp2.Message) != "Both are running." {
		t.Fatalf("second text = %q", textOf(resp2.Message))
	}
	bodies := s.requests()
	if len(bodies) != 2 {
		t.Fatalf("requests = %d, want 2", len(bodies))
	}
	sent := f.ToolResults(t, bodies[1])
	if len(sent) != len(contents) {
		t.Fatalf("tool results on the wire = %d, want %d", len(sent), len(contents))
	}
	for i := range contents {
		if sent[i] != contents[i] {
			t.Fatalf("tool result %d on the wire = %q, want %q byte for byte", i, sent[i], contents[i])
		}
	}
	for _, c := range got {
		if !bytes.Contains(bodies[1], []byte(c.ID)) {
			t.Fatalf("the second request lost call id %q", c.ID)
		}
	}
}

func sameJSON(t *testing.T, a json.RawMessage, b string) bool {
	t.Helper()
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return bytes.Equal(xb, yb)
}

func refusal(t *testing.T, f Fixture) {
	s := newServer(t)
	const explanation = "The request asks for help with an attack."
	s.script(f.answer(t, Answer{Refusal: true, Text: explanation, InputTokens: 50, OutputTokens: 1}))
	resp, err := f.New(t, s.URL).Turn(context.Background(), f.request("Something refused."))
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}
	if resp.Stop != llm.StopRefusal {
		t.Fatalf("Stop = %q (%q), want refusal", resp.Stop, resp.ProviderStop)
	}
	if !strings.Contains(resp.StopDetail, explanation) {
		t.Fatalf("StopDetail = %q, want the provider's explanation %q", resp.StopDetail, explanation)
	}
}

// secretInBody is error body text that must never reach the error's text,
// the APIError's or the returned error's (design 10.2: a body can echo
// prompt content, and callers put err.Error() in room events).
const secretInBody = "prompt-echo-7f3a"

func errorKinds(t *testing.T, f Fixture) {
	for _, tc := range []struct {
		status int
		want   llm.ErrorKind
	}{
		{http.StatusUnauthorized, llm.ErrAuth},
		{http.StatusNotFound, llm.ErrModelNotFound},
		{http.StatusTooManyRequests, llm.ErrRateLimited},
		{http.StatusInternalServerError, llm.ErrOverloaded},
		{http.StatusServiceUnavailable, llm.ErrOverloaded},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			s := newServer(t)
			ct, body := f.RenderError(tc.status, "rejected: "+secretInBody)
			s.script(reply(ct, tc.status, body))
			_, err := f.New(t, s.URL).Turn(context.Background(), f.request("hi"))
			var ae *llm.APIError
			if !errors.As(err, &ae) {
				t.Fatalf("err = %v (%T), want an *llm.APIError", err, err)
			}
			if ae.Kind != tc.want || ae.Status != tc.status || ae.Provider != f.Provider {
				t.Fatalf("APIError = {%s %d %s}, want {%s %d %s}", ae.Provider, ae.Status, ae.Kind, f.Provider, tc.status, tc.want)
			}
			if strings.Contains(ae.Error(), secretInBody) {
				t.Fatalf("the APIError's text carries the body: %q", ae.Error())
			}
			if strings.Contains(err.Error(), secretInBody) {
				t.Fatalf("the error's text carries the body: %q", err.Error())
			}
			// The server repeats its last reply, so a retry would go unseen
			// but for the count: an adapter built by Fixture.New must not
			// retry, and no kind may make it send twice.
			if n := len(s.requests()); n != 1 {
				t.Fatalf("requests = %d, want 1", n)
			}
		})
	}
}

func noAnswer(t *testing.T, f Fixture) {
	s := newServer(t)
	url := s.URL
	s.Close()
	_, err := f.New(t, url).Turn(context.Background(), f.request("hi"))
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrNetwork || ae.Status != 0 {
		t.Fatalf("err = %v, want an *llm.APIError of kind network without a status", err)
	}
	if llm.Unretryable(err) {
		t.Fatal("no answer is retryable")
	}
}

func deadline(t *testing.T, f Fixture) {
	s := newServer(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	s.script(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.New(t, s.URL).Turn(ctx, f.request("hi"))
	if err == nil {
		t.Fatal("Turn returned no error past the caller's deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want one that wraps context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Turn took %s; it must honour the caller's deadline", took)
	}
}
