package wiring

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

func TestEndpointConfigCopiesTheModelSettings(t *testing.T) {
	got := EndpointConfig(config.Config{
		ModelProvider: "bedrock", AWSRegion: "ap-south-1",
		VertexProject: "p", VertexRegion: "r",
		TriageModel: "tri", RoomTokenBudget: "123",
	})
	want := agent.EndpointConfig{
		Provider: "bedrock", AWSRegion: "ap-south-1",
		VertexProject: "p", VertexRegion: "r",
		TriageModel: "tri", RoomTokenBudget: "123",
	}
	if got != want {
		t.Fatalf("EndpointConfig = %+v, want %+v", got, want)
	}
}

func TestNewAgentEndpointStartupErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want string
	}{
		{"unknown provider", config.Config{ModelProvider: "cohere"}, "PIPEROOM_MODEL_PROVIDER"},
		{"bedrock without region", config.Config{ModelProvider: "bedrock"}, "AWS_REGION"},
		{"vertex without project", config.Config{ModelProvider: "vertex", VertexRegion: "global"}, "VERTEX_PROJECT_ID"},
		{"vertex without region", config.Config{ModelProvider: "vertex", VertexProject: "p"}, "VERTEX_REGION"},
		{"bad budget", config.Config{ModelProvider: "anthropic", RoomTokenBudget: "-3"}, "PIPEROOM_ROOM_TOKEN_BUDGET"},
		{"bad first look switch", config.Config{TriagePass: "maybe"}, "PIPEROOM_TRIAGE_PASS"},
		{"first look timeout too long", config.Config{TriagePassTimeout: "5m"}, "PIPEROOM_TRIAGE_PASS_TIMEOUT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAgentEndpoint(context.Background(), tt.cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestNewAgentEndpointDefaultIsAnthropicWithTheDefaultCap(t *testing.T) {
	ep, err := NewAgentEndpoint(context.Background(), config.Config{ModelProvider: "anthropic"}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if ep.Provider() != agent.ProviderAnthropic || ep.TokenLimit() != agent.DefaultRoomTokenBudget {
		t.Fatalf("endpoint = %s / %d", ep.Provider(), ep.TokenLimit())
	}
	ep, err = NewAgentEndpoint(context.Background(), config.Config{RoomTokenBudget: "0"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ep.TokenLimit() != 0 {
		t.Fatalf("limit = %d, want 0 (no cap)", ep.TokenLimit())
	}
}

func TestNewAgentEndpointFirstLookSettings(t *testing.T) {
	ep, err := NewAgentEndpoint(context.Background(), config.Config{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if on, model, timeout := ep.FirstLook(); !on || model != config.DefaultTriagePassModel || timeout != 40*time.Second {
		t.Fatalf("default first look = %v %q %s", on, model, timeout)
	}
	ep, err = NewAgentEndpoint(context.Background(), config.Config{TriagePass: "OFF", TriagePassModel: "m", TriagePassTimeout: "15s"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if on, model, timeout := ep.FirstLook(); on || model != "m" || timeout != 15*time.Second {
		t.Fatalf("first look = %v %q %s", on, model, timeout)
	}
}

func TestNewAgentEndpointWarnsAboutIncompatibleTriageModel(t *testing.T) {
	for model, wantWarn := range map[string]bool{"claude-haiku-4-5": true, "claude-haiku-5-5": false} {
		var buf bytes.Buffer
		_, err := NewAgentEndpoint(context.Background(), config.Config{TriageModel: model}, slog.New(slog.NewTextHandler(&buf, nil)), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(buf.String(), "PIPEROOM_TRIAGE_MODEL"); got != wantWarn {
			t.Errorf("triage %q: warned=%v, want %v\n%s", model, got, wantWarn, buf.String())
		}
	}
}

func TestWarnIfNotBedrockModelID(t *testing.T) {
	for id, wantWarn := range map[string]bool{
		"claude-opus-5-5":                   true,
		"":                                  true,
		"anthropic.claude-opus-5-5":         false,
		"in.anthropic.claude-opus-5-5":      false,
		"us.anthropic.claude-opus-5-5-v1:0": false,
		"arn:aws:bedrock:ap-south-1:1:application-inference-profile/x": false,
	} {
		var buf bytes.Buffer
		warnIfNotBedrockModelID(slog.New(slog.NewTextHandler(&buf, nil)), "PIPEROOM_MODEL", id)
		if got := buf.Len() > 0; got != wantWarn {
			t.Errorf("id %q: warned=%v, want %v", id, got, wantWarn)
		}
	}
}

// PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS reaches the endpoint (V2a): it is
// copied as is, and a value other than all or non-claude refuses start.
func TestNewAgentEndpointNeutraliseControlTokens(t *testing.T) {
	if got := EndpointConfig(config.Config{ModelNeutraliseControlTokens: "non-claude"}); got.NeutraliseControlTokens != "non-claude" {
		t.Fatalf("NeutraliseControlTokens = %q, want non-claude", got.NeutraliseControlTokens)
	}
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	for _, ok := range []string{"", "all", "non-claude"} {
		if _, err := NewAgentEndpoint(context.Background(), config.Config{ModelProvider: "anthropic", ModelNeutraliseControlTokens: ok}, log, nil); err != nil {
			t.Fatalf("%q: %v", ok, err)
		}
	}
	_, err := NewAgentEndpoint(context.Background(), config.Config{ModelProvider: "anthropic", ModelNeutraliseControlTokens: "off"}, log, nil)
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS") {
		t.Fatalf("err = %v, want a refusal naming the variable", err)
	}
}

// TestNewAgentEndpointClaudeUnchanged: on anthropic, bedrock and vertex an
// unlisted id, a Bedrock inference-profile id, an application ARN and a
// versioned Vertex id start as before V6, with today's warnings and today's
// one log line; no start check, probe or role line runs (design 5, V6).
func TestNewAgentEndpointClaudeUnchanged(t *testing.T) {
	tests := []struct {
		name     string
		cfg      config.Config
		bedrockW bool // today's "does not look like a Bedrock model" warning
	}{
		{"anthropic default", config.Config{}, false},
		{"anthropic unlisted id", config.Config{Model: "claude-future-9"}, false},
		{"bedrock inference profile", config.Config{ModelProvider: "bedrock", AWSRegion: "us-east-1", Model: "us.anthropic.claude-opus-5-5-v1:0"}, false},
		{"bedrock application ARN", config.Config{ModelProvider: "bedrock", AWSRegion: "us-east-1", Model: "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/abc"}, false},
		{"bedrock first-party name keeps today's warning", config.Config{ModelProvider: "bedrock", AWSRegion: "us-east-1", Model: "claude-opus-5-5"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
			var buf bytes.Buffer
			ep, err := NewAgentEndpoint(context.Background(), tt.cfg, slog.New(slog.NewTextHandler(&buf, nil)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if ep.NeedsStartChecks() {
				t.Fatal("a Claude deployment needs start checks")
			}
			log := buf.String()
			if strings.Count(log, `msg="model endpoint ready"`) != 1 || strings.Contains(log, "model role") || strings.Contains(log, "probe") {
				t.Fatalf("log is not today's:\n%s", log)
			}
			// The first look's default (claude-haiku-5-5) warns on Bedrock today too.
			if got := strings.Contains(log, "setting=PIPEROOM_MODEL "); got != tt.bedrockW {
				t.Fatalf("bedrock warning = %v, want %v:\n%s", got, tt.bedrockW, log)
			}
		})
	}
}

func TestNewAgentEndpointNewProviderRunsTheStartChecks(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"1","type":"function","function":{"name":"piperoom_start_probe","arguments":"{\"status\":\"ready\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":%d,"completion_tokens":3}}`, len(b)/3)
	}))
	defer srv.Close()
	cfg := config.Config{ModelProvider: "openai", ModelFlavor: "ollama", Model: "qwen3:32b", ModelBaseURL: srv.URL}
	if _, err := NewAgentEndpoint(context.Background(), cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "PIPEROOM_MODEL_UNVERIFIED=on") {
		t.Fatalf("an unlisted model: err = %v", err)
	}
	if calls != 0 {
		t.Fatalf("a refused start sent %d requests", calls)
	}
	cfg.ModelUnverified = "on"
	var buf bytes.Buffer
	if _, err := NewAgentEndpoint(context.Background(), cfg, slog.New(slog.NewTextHandler(&buf, nil)), nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("probe requests = %d, want 1", calls)
	}
	if !strings.Contains(buf.String(), `msg="model role" role=main`) {
		t.Fatalf("no role line:\n%s", buf.String())
	}
}
