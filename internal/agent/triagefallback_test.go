package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// apiErr builds an API error with a response body, wrapped like a stream
// error, as the Claude adapter returns it: classified into an *llm.APIError
// that keeps the SDK error.
func apiErr(code int, body string) error {
	e := &anthropic.Error{StatusCode: code}
	_ = e.UnmarshalJSON([]byte(body))
	return anthropicllm.ClassifyError(ProviderAnthropic, fmt.Errorf("agent: stream: %w", e), nil)
}

func TestTriageRejected(t *testing.T) {
	for code, want := range map[int]bool{
		400: true, 403: true, 404: true, 413: true, 422: true,
		401: false, 408: false, 409: false, 429: false,
		500: false, 529: false, 200: false,
	} {
		if got := triageRejected(apiErr(code, `{}`)); got != want {
			t.Errorf("status %d: triageRejected = %v, want %v", code, got, want)
		}
	}
	if triageRejected(context.DeadlineExceeded) || triageRejected(nil) {
		t.Error("a non-API error is not a rejected triage call")
	}
}

func TestModelRejection(t *testing.T) {
	tests := []struct {
		name string
		code int
		body string
		want bool
	}{
		{"404 is the model", 404, `{"type":"error","error":{"type":"not_found_error","message":"x"}}`, true},
		{"adaptive thinking", 400, `{"error":{"message":"thinking.type.adaptive is not supported for this model"}}`, true},
		{"effort", 400, `{"error":{"message":"output_config.effort: Extra inputs are not permitted"}}`, true},
		{"extra inputs", 400, `{"error":{"message":"eager_input_streaming: Extra inputs are not permitted"}}`, true},
		{"cache_control", 400, `{"error":{"message":"cache_control is not allowed at the top level"}}`, true},
		{"bedrock invalid id", 400, `{"message":"The provided model identifier is invalid."}`, true},
		{"bedrock access", 403, `{"message":"You don't have access to the model with the specified model ID."}`, true},
		{"vertex permission", 403, `{"error":{"status":"PERMISSION_DENIED","message":"Permission denied on resource model"}}`, true},
		{"prompt too long", 400, `{"error":{"message":"prompt is too long: 250000 tokens > 200000 maximum"}}`, false},
		{"too long mentions the model", 400, `{"error":{"message":"input is too long for this model"}}`, false},
		{"413", 413, `{"error":{"type":"request_too_large","message":"Request exceeds the maximum size for this model"}}`, false},
		{"low credit", 400, `{"error":{"message":"Your credit balance is too low to access the Anthropic API"}}`, false},
		{"unrelated 400", 400, `{"error":{"message":"messages: roles must alternate"}}`, false},
		{"403 without a marker", 403, `{"error":{"message":"forbidden"}}`, false},
		{"422 is per run", 422, `{"error":{"message":"model"}}`, false},
		{"empty body", 400, ``, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := modelRejection(apiErr(tt.code, tt.body)); got != tt.want {
				t.Fatalf("modelRejection(%d, %s) = %v, want %v", tt.code, tt.body, got, tt.want)
			}
		})
	}
	if modelRejection(context.DeadlineExceeded) {
		t.Error("a non-API error is not a model rejection")
	}
}

func TestTriageModelWarning(t *testing.T) {
	for id, want := range map[string]bool{
		"claude-haiku-4-5":                         true,
		"claude-haiku-4-5@20251001":                true,
		"anthropic.claude-haiku-4-5-20251001-v1:0": true,
		"us.anthropic.claude-sonnet-4-5-20250929":  true,
		"claude-3-5-haiku-latest":                  true,
		"claude-opus-4-5":                          true,
		"claude-sonnet-4-20250514":                 true,
		"claude-haiku-5-5":                         false,
		"claude-sonnet-5-5":                        false,
		"claude-sonnet-4-6":                        false,
		"in.anthropic.claude-haiku-5-5":            false,
		"some-custom-profile":                      false,
	} {
		if got := TriageModelWarning(id) != ""; got != want {
			t.Errorf("TriageModelWarning(%q) warns = %v, want %v", id, got, want)
		}
	}
}

func TestEscalationToTheMainModelIsSticky(t *testing.T) {
	propose := mustMessage(t, `{"stop_reason":"tool_use","content":[
		{"type":"tool_use","id":"tu9","name":"propose_fix","input":{"title":"t"}}
	]}`)
	m := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{
		propose,         // triage asks for a fix
		toolTurnN(t, 1), // main answer to the same request, then a read tool
		toolTurnN(t, 2),
	}}}
	r := triageRunner(t, m, &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, 6)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Triage turns left (5 of 6) must not bring the triage model back.
	if want := []string{"tri", "main", "main", "main"}; !reflect.DeepEqual(m.models, want) {
		t.Fatalf("models = %v, want %v", m.models, want)
	}
}

// sseOK is a minimal streamed answer: one text block and end_turn.
const sseOK = `event: message_start
data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":null,"usage":{"input_tokens":10,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello from main"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}

event: message_stop
data: {"type":"message_stop"}

`

// answer is what the fake server says to one request: a status and body, or
// status 0 for the streamed success.
type answer struct {
	status int
	body   string
}

const (
	bodyAdaptive  = `{"type":"error","error":{"type":"invalid_request_error","message":"thinking.type.adaptive is not supported for this model"}}`
	bodyTooLong   = `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 250000 tokens > 200000 maximum"}}`
	bodyMainFails = `{"type":"error","error":{"type":"invalid_request_error","message":"main model failure"}}`
	bodyNoAccess  = `{"message":"You don't have access to the model with the specified model ID."}`
)

// modelServer asks decide for every request (model id and how many requests
// that model had before) and records the model of each.
type modelServer struct {
	*httptest.Server
	mu     sync.Mutex
	models []string
}

func newModelServer(t *testing.T, decide func(model string, nth int) answer) *modelServer {
	t.Helper()
	ms := &modelServer{}
	ms.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var req struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(b, &req)
		ms.mu.Lock()
		nth := 0
		for _, m := range ms.models {
			if m == req.Model {
				nth++
			}
		}
		ms.models = append(ms.models, req.Model)
		ms.mu.Unlock()
		if a := decide(req.Model, nth); a.status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(a.status)
			_, _ = w.Write([]byte(a.body))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseOK)
	}))
	t.Cleanup(ms.Close)
	return ms
}

// rejectAlways makes one model answer with the same error on every request.
func rejectAlways(model string, status int, body string) func(string, int) answer {
	return func(m string, _ int) answer {
		if m == model {
			return answer{status, body}
		}
		return answer{}
	}
}

func triageEndpoint(t *testing.T, ms *modelServer, triage string) *Endpoint {
	t.Helper()
	ep, err := newEndpoint(context.Background(), EndpointConfig{TriageModel: triage}, envOf(nil), testLoaders(nil), discardLogger(),
		option.WithBaseURL(ms.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

func (ms *modelServer) seen() []string {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return append([]string(nil), ms.models...)
}

func runOnEndpoint(t *testing.T, ep *Endpoint, roomID string) (*fakeSink, error) {
	t.Helper()
	sink := &fakeSink{}
	err := ep.Run(context.Background(), Alert{RoomID: roomID, Title: "orders stale", PipelineID: "orders"}, Deps{
		Tools: tools.NewRegistry(), Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger(),
	}, Options{Model: "main-model"})
	return sink, err
}

func TestRejectedTriageModelFallsBackToMainAndTurnsTriageOff(t *testing.T) {
	ms := newModelServer(t, rejectAlways("claude-haiku-4-5", http.StatusBadRequest, bodyAdaptive))
	ep := triageEndpoint(t, ms, "claude-haiku-4-5")

	sink, err := runOnEndpoint(t, ep, "room-1")
	if err != nil {
		t.Fatalf("the first alert must not fail because of the triage model: %v", err)
	}
	if want := []string{"claude-haiku-4-5", "main-model"}; !reflect.DeepEqual(ms.seen(), want) {
		t.Fatalf("server saw %v, want %v", ms.seen(), want)
	}
	msgs := sink.ofType(events.AgentMessage)
	if len(msgs) != 1 || !strings.Contains(string(msgs[0].payload), "hello from main") {
		t.Fatalf("agent messages = %v, want the main model's answer", msgs)
	}
	for _, s := range sink.ofType(events.AgentStatus) {
		if strings.Contains(string(s.payload), `"error"`) {
			t.Fatalf("an error status was emitted: %s", s.payload)
		}
	}
	if !ep.triageSw.isOff() {
		t.Fatal("triage should be off for the endpoint")
	}

	// Another room does not try the triage model again.
	if _, err := runOnEndpoint(t, ep, "room-2"); err != nil {
		t.Fatalf("second room: %v", err)
	}
	if want := []string{"claude-haiku-4-5", "main-model", "main-model"}; !reflect.DeepEqual(ms.seen(), want) {
		t.Fatalf("server saw %v, want %v", ms.seen(), want)
	}
}

func TestTriageTransientErrorsDoNotTurnTriageOff(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusUnauthorized} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ms := newModelServer(t, rejectAlways("tri-model", status, `{"type":"error","error":{"message":"model overloaded"}}`))
			ep := triageEndpoint(t, ms, "tri-model")
			if _, err := runOnEndpoint(t, ep, "room-1"); err == nil {
				t.Fatal("a transient or credential error must still surface as a run error")
			}
			if ep.triageSw.isOff() {
				t.Fatal("triage must stay on")
			}
			if want := []string{"tri-model"}; !reflect.DeepEqual(ms.seen(), want) {
				t.Fatalf("server saw %v, want only the triage request", ms.seen())
			}
		})
	}
}

func TestTriage403FallsBackAndTurnsTriageOffWhenMainSucceeds(t *testing.T) {
	ms := newModelServer(t, rejectAlways("tri-model", http.StatusForbidden, bodyNoAccess))
	ep := triageEndpoint(t, ms, "tri-model")
	sink, err := runOnEndpoint(t, ep, "room-1")
	if err != nil {
		t.Fatalf("a 403 on the triage model must not kill the room: %v", err)
	}
	if want := []string{"tri-model", "main-model"}; !reflect.DeepEqual(ms.seen(), want) {
		t.Fatalf("server saw %v, want %v", ms.seen(), want)
	}
	if msgs := sink.ofType(events.AgentMessage); len(msgs) != 1 || !strings.Contains(string(msgs[0].payload), "hello from main") {
		t.Fatalf("agent messages = %v", msgs)
	}
	if !ep.triageSw.isOff() {
		t.Fatal("main succeeded and the 403 names the model: triage should be off")
	}
}

func TestTriageRejectionWhereMainAlsoFailsReturnsTheMainError(t *testing.T) {
	ms := newModelServer(t, func(model string, _ int) answer {
		if model == "tri-model" {
			return answer{http.StatusBadRequest, bodyAdaptive}
		}
		return answer{http.StatusBadRequest, bodyMainFails}
	})
	ep := triageEndpoint(t, ms, "tri-model")
	_, err := runOnEndpoint(t, ep, "room-1")
	// The bodies are read from the structured error: its text never carries
	// a provider body (it can echo the prompt).
	var ae *llm.APIError
	if !errors.As(err, &ae) || !strings.Contains(ae.Body, "main model failure") {
		t.Fatalf("err = %v, want the main model's error", err)
	}
	if strings.Contains(ae.Body, "adaptive") {
		t.Fatalf("err = %v (body %s) must not be the triage model's error", err, ae.Body)
	}
	if strings.Contains(err.Error(), "main model failure") {
		t.Fatalf("err.Error() = %q carries the provider's body", err.Error())
	}
	if ep.triageSw.isOff() {
		t.Fatal("the main model failed too, so the switch must stay on")
	}
	if want := []string{"tri-model", "main-model"}; !reflect.DeepEqual(ms.seen(), want) {
		t.Fatalf("server saw %v, want %v", ms.seen(), want)
	}
}

func TestPromptTooLongOnTriageEscalatesOnlyThatRun(t *testing.T) {
	cases := map[string]answer{
		"prompt is too long": {http.StatusBadRequest, bodyTooLong},
		"413":                {http.StatusRequestEntityTooLarge, `{"type":"error","error":{"type":"request_too_large","message":"Request exceeds the maximum size"}}`},
	}
	for name, first := range cases {
		t.Run(name, func(t *testing.T) {
			// Room 1's first triage request is too long; later ones are fine.
			ms := newModelServer(t, func(model string, nth int) answer {
				if model == "tri-model" && nth == 0 {
					return first
				}
				return answer{}
			})
			ep := triageEndpoint(t, ms, "tri-model")

			if _, err := runOnEndpoint(t, ep, "room-1"); err != nil {
				t.Fatalf("room 1: %v", err)
			}
			if ep.triageSw.isOff() {
				t.Fatal("a per-run error must not turn triage off for everyone")
			}
			sink2, err := runOnEndpoint(t, ep, "room-2")
			if err != nil {
				t.Fatalf("room 2: %v", err)
			}
			// room 1: triage then main; room 2: triage, which now works.
			if want := []string{"tri-model", "main-model", "tri-model"}; !reflect.DeepEqual(ms.seen(), want) {
				t.Fatalf("server saw %v, want %v", ms.seen(), want)
			}
			if msgs := sink2.ofType(events.AgentMessage); len(msgs) != 1 {
				t.Fatalf("room 2 messages = %v", msgs)
			}
		})
	}
}

func TestRunStaysOnMainAfterATriageRejection(t *testing.T) {
	// A run that was redone on main stays there for its later turns.
	rm := &modelRecorder{scriptedModel: scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1), toolTurnN(t, 2)}}}
	r := triageRunner(t, &rejectFirstTriage{modelRecorder: rm, err: apiErr(400, bodyTooLong)}, &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, 6)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []string{"tri", "main", "main", "main"}; !reflect.DeepEqual(rm.models, want) {
		t.Fatalf("models = %v, want %v", rm.models, want)
	}
}

// rejectFirstTriage fails the first request that names the triage model.
type rejectFirstTriage struct {
	*modelRecorder
	err  error
	done bool
}

func (m *rejectFirstTriage) Turn(ctx context.Context, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	if p.Model == "tri" && !m.done {
		m.done = true
		m.models = append(m.models, p.Model)
		return anthropic.BetaMessage{}, m.err
	}
	return m.modelRecorder.Turn(ctx, p)
}

func TestEndpointForgetResetsOneRoom(t *testing.T) {
	ep := testEndpoint(&scriptedModel{}, 100)
	ep.budget("a").record(usageTokens{input: 60}, true)
	ep.budget("b").record(usageTokens{input: 70}, true)
	ep.Forget("a")
	ep.Forget("never-seen")
	if got := ep.budget("a").Used(); got != 0 {
		t.Fatalf("room a used = %d after Forget, want 0", got)
	}
	if got := ep.budget("b").Used(); got != 70 {
		t.Fatalf("room b used = %d, want 70", got)
	}
}
