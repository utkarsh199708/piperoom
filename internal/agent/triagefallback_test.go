package agent

import (
	"context"
	"encoding/json"
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

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func TestRejectsRequestShape(t *testing.T) {
	for code, want := range map[int]bool{
		400: true, 404: true, 422: true, 413: true,
		401: false, 403: false, 408: false, 409: false, 429: false,
		500: false, 529: false, 200: false,
	} {
		err := fmt.Errorf("agent: stream: %w", &anthropic.Error{StatusCode: code})
		if got := rejectsRequestShape(err); got != want {
			t.Errorf("status %d: rejectsRequestShape = %v, want %v", code, got, want)
		}
	}
	if rejectsRequestShape(context.DeadlineExceeded) || rejectsRequestShape(nil) {
		t.Error("a non-API error is not a rejected request shape")
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

// modelServer answers 400 (or rejectStatus) for the rejected model and a
// streamed answer for any other, and records the model of every request.
type modelServer struct {
	*httptest.Server
	mu     sync.Mutex
	models []string
}

func newModelServer(t *testing.T, reject string, rejectStatus int) *modelServer {
	t.Helper()
	ms := &modelServer{}
	ms.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var req struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(b, &req)
		ms.mu.Lock()
		ms.models = append(ms.models, req.Model)
		ms.mu.Unlock()
		if req.Model == reject {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(rejectStatus)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"thinking.type.adaptive is not supported for this model"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseOK)
	}))
	t.Cleanup(ms.Close)
	return ms
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
	ms := newModelServer(t, "claude-haiku-4-5", http.StatusBadRequest)
	ep, err := newEndpoint(context.Background(), EndpointConfig{TriageModel: "claude-haiku-4-5"}, envOf(nil), testLoaders(nil), discardLogger(),
		option.WithBaseURL(ms.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}

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
			ms := newModelServer(t, "tri-model", status)
			ep, err := newEndpoint(context.Background(), EndpointConfig{TriageModel: "tri-model"}, envOf(nil), testLoaders(nil), discardLogger(),
				option.WithBaseURL(ms.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
			if err != nil {
				t.Fatal(err)
			}
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
