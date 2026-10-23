package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/oauth2/google"

	anthropicllm "piperoom/internal/agent/llm/anthropic"
)

// exportedTestLoaders is testLoaders through the exported type.
func exportedTestLoaders() ProviderLoaders {
	ld := testLoaders(nil)
	return ProviderLoaders{AWSConfig: ld.awsConfig, GoogleCreds: ld.googleCreds}
}

// callWith sends one request on a client built from opts to ws.
func callWith(t *testing.T, ws *wireServer, opts []option.RequestOption, modelID string) seenRequest {
	t.Helper()
	opts = append(opts, option.WithBaseURL(ws.URL), option.WithMaxRetries(0))
	ep := &Endpoint{model: anthropicllm.New(ProviderAnthropic, opts, discardLogger())}
	return ws.call(t, ep, modelID)
}

func TestProviderOptionsNormalizesAndValidates(t *testing.T) {
	never := ProviderLoaders{
		AWSConfig: func(context.Context, string) (aws.Config, error) {
			t.Fatal("AWS credentials read before validation")
			return aws.Config{}, nil
		},
		GoogleCreds: func(context.Context) (*google.Credentials, error) {
			t.Fatal("Google credentials read before validation")
			return nil, nil
		},
	}
	for _, tc := range []struct {
		name, want string
		cfg        EndpointConfig
	}{
		{"unknown provider", `unknown PIPEROOM_MODEL_PROVIDER "azure"`, EndpointConfig{Provider: " Azure "}},
		{"bedrock without a region", "AWS_REGION", EndpointConfig{Provider: "bedrock"}},
		{"vertex without a project", "VERTEX_PROJECT_ID", EndpointConfig{Provider: "vertex", VertexRegion: "global"}},
		{"vertex without a region", "VERTEX_REGION", EndpointConfig{Provider: "vertex", VertexProject: "p"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, opts, err := ProviderOptions(context.Background(), tc.cfg, envOf(nil), never)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one naming %q", err, tc.want)
			}
			if opts != nil {
				t.Fatalf("options returned with an error: %d", len(opts))
			}
		})
	}

	provider, _, err := ProviderOptions(context.Background(), EndpointConfig{Provider: " Anthropic "}, envOf(nil), never)
	if err != nil || provider != ProviderAnthropic {
		t.Fatalf("provider = %q, err = %v; want anthropic", provider, err)
	}
	provider, _, err = ProviderOptions(context.Background(), EndpointConfig{}, envOf(nil), never)
	if err != nil || provider != ProviderAnthropic {
		t.Fatalf("empty provider = %q, err = %v; want anthropic", provider, err)
	}
}

// The exported options reach the same place NewEndpoint's do, and the
// workspace header goes to the Anthropic API only.
func TestProviderOptionsOnTheWire(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-not-real")
	t.Setenv("AWS_BEARER_TOKEN_BEDROCK", "")
	env := envOf(map[string]string{workspaceEnv: "wrkspc_123"})

	t.Run("anthropic", func(t *testing.T) {
		_, opts, err := ProviderOptions(context.Background(), EndpointConfig{}, env, exportedTestLoaders())
		if err != nil {
			t.Fatal(err)
		}
		got := callWith(t, newWireServer(t), opts, "claude-opus-5-5")
		if got.path != "/v1/messages" || got.header.Get(workspaceHeader) != "wrkspc_123" {
			t.Fatalf("path = %q, workspace header = %q", got.path, got.header.Get(workspaceHeader))
		}
	})

	t.Run("bedrock", func(t *testing.T) {
		_, opts, err := ProviderOptions(context.Background(), EndpointConfig{Provider: "bedrock", AWSRegion: "ap-south-1"}, env, exportedTestLoaders())
		if err != nil {
			t.Fatal(err)
		}
		got := callWith(t, newWireServer(t), opts, "in.anthropic.claude-opus-5-5")
		if a := got.header.Get("Authorization"); !strings.Contains(a, "/ap-south-1/bedrock/aws4_request") {
			t.Fatalf("Authorization = %q, want SigV4 for ap-south-1/bedrock", a)
		}
		if got.header.Get(workspaceHeader) != "" || got.header.Get("x-api-key") != "" {
			t.Fatalf("Anthropic-only headers leaked to Bedrock: %v", got.header)
		}
	})

	t.Run("vertex", func(t *testing.T) {
		_, opts, err := ProviderOptions(context.Background(), EndpointConfig{Provider: "vertex", VertexProject: "acme", VertexRegion: "us-east5"}, env, exportedTestLoaders())
		if err != nil {
			t.Fatal(err)
		}
		got := callWith(t, newWireServer(t), opts, "claude-opus-5-5")
		if !strings.HasPrefix(got.path, "/v1/projects/acme/locations/us-east5/") || got.header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("path = %q, Authorization = %q", got.path, got.header.Get("Authorization"))
		}
		if got.header.Get(workspaceHeader) != "" || got.header.Get("x-api-key") != "" {
			t.Fatalf("Anthropic-only headers leaked to Vertex: %v", got.header)
		}
	})
}

func TestProviderOptionsVertexNilCredentials(t *testing.T) {
	ld := exportedTestLoaders()
	ld.GoogleCreds = func(context.Context) (*google.Credentials, error) { return nil, nil }
	_, opts, err := ProviderOptions(context.Background(), EndpointConfig{Provider: "vertex", VertexProject: "p", VertexRegion: "global"}, envOf(nil), ld)
	if err == nil || !strings.Contains(err.Error(), "application default credentials") || opts != nil {
		t.Fatalf("err = %v, opts = %d; want a credentials error and no options", err, len(opts))
	}
}
