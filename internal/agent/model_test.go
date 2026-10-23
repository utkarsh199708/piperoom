package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/agent/llm"
	"piperoom/internal/events"
)

// headerServer records the headers of every request it receives and answers
// with an API error, which is enough: the header is checked on the way in.
type headerServer struct {
	*httptest.Server
	mu   sync.Mutex
	seen []http.Header
}

func newHeaderServer(t *testing.T) *headerServer {
	t.Helper()
	hs := &headerServer{}
	hs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hs.mu.Lock()
		hs.seen = append(hs.seen, r.Header.Clone())
		hs.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"test server"}}`))
	}))
	t.Cleanup(hs.Close)
	return hs
}

func (hs *headerServer) turn(t *testing.T, getenv func(string) string) http.Header {
	t.Helper()
	m := newModelWith(getenv, nil,
		option.WithBaseURL(hs.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
	_, err := m.Turn(context.Background(), llm.Request{
		Model:     "claude-opus-5-5",
		MaxTokens: 16,
		Messages:  []llm.Message{userMessage(textPart("hi"))},
	})
	if err == nil {
		t.Fatal("test server answers 400, want an error from Turn")
	}
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if len(hs.seen) == 0 {
		t.Fatal("the request never reached the server")
	}
	return hs.seen[len(hs.seen)-1]
}

func env(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func TestWorkspaceHeader(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string // "" means the header must be absent
	}{
		{"set", map[string]string{workspaceEnv: "wrkspc_01ABC"}, "wrkspc_01ABC"},
		{"surrounding blanks are trimmed", map[string]string{workspaceEnv: "  wrkspc_01ABC \n"}, "wrkspc_01ABC"},
		{"unset", map[string]string{}, ""},
		{"empty", map[string]string{workspaceEnv: ""}, ""},
		{"blank", map[string]string{workspaceEnv: "   "}, ""},
	}
	hs := newHeaderServer(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := hs.turn(t, env(c.env))
			got, present := h[http.CanonicalHeaderKey(workspaceHeader)]
			if c.want == "" {
				if present {
					t.Fatalf("header must be absent, got %q", got)
				}
			} else if !present || len(got) != 1 || got[0] != c.want {
				t.Fatalf("%s = %q, want %q", workspaceHeader, got, c.want)
			}
			if h.Get("X-Api-Key") != "sk-ant-test-not-real" {
				t.Fatalf("the API key must still be sent as before, got %q", h.Get("X-Api-Key"))
			}
		})
	}
}

// The agent loop (newModel in Run) and the hand-off writer (WriteHandoff) both
// build their client with newModel, which reads the real process environment.
// The SDK takes the server from ANTHROPIC_BASE_URL and the key from
// ANTHROPIC_API_KEY, so the whole path can be pointed at a test server.
func TestProductionClientsSendWorkspaceHeader(t *testing.T) {
	hs := newHeaderServer(t)
	t.Setenv("ANTHROPIC_BASE_URL", hs.URL)
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-not-real")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "")

	last := func() http.Header {
		hs.mu.Lock()
		defer hs.mu.Unlock()
		if len(hs.seen) == 0 {
			t.Fatal("no request reached the server")
		}
		return hs.seen[len(hs.seen)-1]
	}
	turn := func() {
		_, _ = newModel(Options{}, nil).Turn(context.Background(), llm.Request{
			Model: "claude-opus-5-5", MaxTokens: 16,
			Messages: []llm.Message{userMessage(textPart("hi"))},
		})
	}
	// WriteHandoff refuses an empty log before calling the model, so give it one event.
	log := []events.Event{{RoomID: "r_1", Seq: 1, Type: events.RoomCreated, Payload: json.RawMessage(`{"title":"t"}`)}}
	handoff := func() { _, _ = WriteHandoff(context.Background(), log, Options{}) }

	for name, call := range map[string]func(){"agent loop": turn, "hand-off": handoff} {
		t.Setenv(workspaceEnv, "wrkspc_from_env")
		call()
		if got := last().Get(workspaceHeader); got != "wrkspc_from_env" {
			t.Errorf("%s: %s = %q, want wrkspc_from_env", name, workspaceHeader, got)
		}
		t.Setenv(workspaceEnv, "")
		call()
		if _, present := last()[http.CanonicalHeaderKey(workspaceHeader)]; present {
			t.Errorf("%s: header must be absent when %s is empty", name, workspaceEnv)
		}
	}
}
