package wiring

import (
	"bytes"
	"context"
	"log/slog"
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
		{"unknown provider", config.Config{ModelProvider: "openai"}, "allowed values"},
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
