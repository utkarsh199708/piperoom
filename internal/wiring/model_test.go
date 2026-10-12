package wiring

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAgentEndpoint(context.Background(), tt.cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestNewAgentEndpointDefaultIsAnthropicWithTheDefaultCap(t *testing.T) {
	ep, err := NewAgentEndpoint(context.Background(), config.Config{ModelProvider: "anthropic"}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if ep.Provider() != agent.ProviderAnthropic || ep.TokenLimit() != agent.DefaultRoomTokenBudget {
		t.Fatalf("endpoint = %s / %d", ep.Provider(), ep.TokenLimit())
	}
	ep, err = NewAgentEndpoint(context.Background(), config.Config{RoomTokenBudget: "0"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ep.TokenLimit() != 0 {
		t.Fatalf("limit = %d, want 0 (no cap)", ep.TokenLimit())
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
