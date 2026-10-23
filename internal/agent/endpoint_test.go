package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"piperoom/internal/agent/llm"
	"piperoom/internal/tools"
)

func newToolsRegistryWithOne(t *testing.T) *tools.Registry {
	t.Helper()
	reg := tools.NewRegistry()
	if err := reg.Register(&recordingTool{name: "connect_status", schema: testSchema("connector")}); err != nil {
		t.Fatal(err)
	}
	return reg
}

// seenRequest is what a provider client put on the wire.
type seenRequest struct {
	path   string
	header http.Header
	body   string
}

// wireServer records requests and answers 400, which ends the call without a
// retry. Nothing leaves the machine: the provider options point at it.
type wireServer struct {
	*httptest.Server
	mu   sync.Mutex
	seen []seenRequest
}

func newWireServer(t *testing.T) *wireServer {
	t.Helper()
	ws := &wireServer{}
	ws.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ws.mu.Lock()
		ws.seen = append(ws.seen, seenRequest{path: r.URL.Path, header: r.Header.Clone(), body: string(b)})
		ws.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"test server"}}`))
	}))
	t.Cleanup(ws.Close)
	return ws
}

// call sends one request through ep's model and returns what arrived.
func (ws *wireServer) call(t *testing.T, ep *Endpoint, modelID string) seenRequest {
	t.Helper()
	// Compaction asks for the compact-2026-01-12 beta, which every provider
	// must carry (a header, or anthropic_beta in a Bedrock body).
	_, err := ep.model.Turn(context.Background(), llm.Request{
		Model:      modelID,
		MaxTokens:  16,
		Compaction: &llm.Compaction{Instructions: compactionInstructions()},
		Messages:   []llm.Message{userMessage(textPart("hi"))},
	})
	if err == nil {
		t.Fatal("the test server answers 400, want an error from Turn")
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if len(ws.seen) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(ws.seen))
	}
	return ws.seen[0]
}

// loaders returns credential loaders that read nothing from the machine.
func testLoaders(awsRegionSeen *string) providerLoaders {
	return providerLoaders{
		awsConfig: func(_ context.Context, region string) (aws.Config, error) {
			if awsRegionSeen != nil {
				*awsRegionSeen = region
			}
			return aws.Config{
				Region: region,
				Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
					return aws.Credentials{AccessKeyID: "AKIDTEST", SecretAccessKey: "test-secret"}, nil
				}),
			}, nil
		},
		googleCreds: func(context.Context) (*google.Credentials, error) {
			return &google.Credentials{
				ProjectID:   "adc-project",
				TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}),
			}, nil
		},
	}
}

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestNewEndpointStartupErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  EndpointConfig
		want []string // every substring the error must contain
	}{
		{"unknown provider names the allowed values", EndpointConfig{Provider: "azure"},
			[]string{`"azure"`, "anthropic", "bedrock", "vertex"}},
		{"provider names are trimmed and lower-cased before the check", EndpointConfig{Provider: "Gemini"},
			[]string{`"gemini"`, "allowed values"}},
		{"bedrock without a region", EndpointConfig{Provider: "bedrock"}, []string{"AWS_REGION"}},
		{"bedrock with a blank region", EndpointConfig{Provider: "bedrock", AWSRegion: "  "}, []string{"AWS_REGION"}},
		{"vertex without a project", EndpointConfig{Provider: "vertex", VertexRegion: "global"}, []string{"VERTEX_PROJECT_ID"}},
		{"vertex without a region", EndpointConfig{Provider: "vertex", VertexProject: "p"}, []string{"VERTEX_REGION"}},
		{"bad token budget", EndpointConfig{RoomTokenBudget: "lots"}, []string{"PIPEROOM_ROOM_TOKEN_BUDGET"}},
		{"negative token budget", EndpointConfig{Provider: "bedrock", AWSRegion: "ap-south-1", RoomTokenBudget: "-5"}, []string{"PIPEROOM_ROOM_TOKEN_BUDGET"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A loader that fails the test proves validation runs before any
			// credential lookup.
			ld := providerLoaders{
				awsConfig: func(context.Context, string) (aws.Config, error) {
					t.Fatal("AWS credentials read before validation")
					return aws.Config{}, nil
				},
				googleCreds: func(context.Context) (*google.Credentials, error) {
					t.Fatal("Google credentials read before validation")
					return nil, nil
				},
			}
			_, err := newEndpoint(context.Background(), tt.cfg, envOf(nil), ld, discardLogger())
			if err == nil {
				t.Fatal("want a startup error")
			}
			for _, s := range tt.want {
				if !strings.Contains(err.Error(), s) {
					t.Fatalf("error %q does not contain %q", err, s)
				}
			}
		})
	}
}

func TestNewEndpointCredentialErrorsAreStartupErrors(t *testing.T) {
	boom := errors.New("no credentials")
	ld := providerLoaders{
		awsConfig:   func(context.Context, string) (aws.Config, error) { return aws.Config{}, boom },
		googleCreds: func(context.Context) (*google.Credentials, error) { return nil, boom },
	}
	for _, cfg := range []EndpointConfig{
		{Provider: "bedrock", AWSRegion: "ap-south-1"},
		{Provider: "vertex", VertexProject: "p", VertexRegion: "global"},
	} {
		if _, err := newEndpoint(context.Background(), cfg, envOf(nil), ld, discardLogger()); !errors.Is(err, boom) {
			t.Fatalf("%s: got %v, want the credential error wrapped", cfg.Provider, err)
		}
	}
}

// Missing AWS credentials stop the server at startup. The check fetches the
// credentials once; it sends no model request.
func TestBedrockMissingCredentialsFailAtStartup(t *testing.T) {
	cfg := EndpointConfig{Provider: "bedrock", AWSRegion: "ap-south-1"}
	withCreds := func(p aws.CredentialsProvider) providerLoaders {
		return providerLoaders{awsConfig: func(_ context.Context, r string) (aws.Config, error) {
			return aws.Config{Region: r, Credentials: p}, nil
		}}
	}

	t.Run("no provider at all", func(t *testing.T) {
		_, err := newEndpoint(context.Background(), cfg, envOf(nil), withCreds(nil), discardLogger())
		if err == nil || !strings.Contains(err.Error(), "AWS credentials") {
			t.Fatalf("err = %v, want one about AWS credentials", err)
		}
	})

	t.Run("retrieve fails", func(t *testing.T) {
		boom := errors.New("no valid providers in chain")
		p := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return aws.Credentials{}, boom })
		_, err := newEndpoint(context.Background(), cfg, envOf(nil), withCreds(p), discardLogger())
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the retrieve error wrapped", err)
		}
	})

	t.Run("retrieve is called once with a deadline", func(t *testing.T) {
		var calls int
		var hasDeadline bool
		p := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			calls++
			_, hasDeadline = ctx.Deadline()
			return aws.Credentials{AccessKeyID: "AKIDTEST", SecretAccessKey: "s"}, nil
		})
		if _, err := newEndpoint(context.Background(), cfg, envOf(nil), withCreds(p), discardLogger()); err != nil {
			t.Fatal(err)
		}
		if calls != 1 || !hasDeadline {
			t.Fatalf("calls = %d, deadline = %v, want 1 call with a deadline", calls, hasDeadline)
		}
	})

	t.Run("a Bedrock API key needs no AWS credentials", func(t *testing.T) {
		env := envOf(map[string]string{bedrockTokenEnv: "bedrock-api-key-test"})
		if _, err := newEndpoint(context.Background(), cfg, env, withCreds(nil), discardLogger()); err != nil {
			t.Fatalf("with %s set: %v", bedrockTokenEnv, err)
		}
	})
}

func TestEndpointConfigDefaults(t *testing.T) {
	ep, err := newEndpoint(context.Background(), EndpointConfig{}, envOf(nil), testLoaders(nil), discardLogger(), option.WithAPIKey("k"))
	if err != nil {
		t.Fatal(err)
	}
	if ep.Provider() != ProviderAnthropic {
		t.Fatalf("provider = %q, want anthropic", ep.Provider())
	}
	if ep.TokenLimit() != 2_000_000 {
		t.Fatalf("limit = %d, want 2000000", ep.TokenLimit())
	}
	if ep.triageTurns != 6 {
		t.Fatalf("triage turns = %d, want 6", ep.triageTurns)
	}
	ep, err = newEndpoint(context.Background(), EndpointConfig{Provider: " Anthropic ", RoomTokenBudget: "0", TriageTurns: 3, TriageModel: "tri"},
		envOf(nil), testLoaders(nil), discardLogger(), option.WithAPIKey("k"))
	if err != nil {
		t.Fatal(err)
	}
	if ep.TokenLimit() != 0 || ep.triageTurns != 3 || ep.triageModel != "tri" {
		t.Fatalf("endpoint = %+v", ep)
	}
}

func TestAnthropicProviderKeepsWorkspaceHeader(t *testing.T) {
	ws := newWireServer(t)
	ep, err := newEndpoint(context.Background(), EndpointConfig{Provider: "anthropic"},
		envOf(map[string]string{workspaceEnv: "wrkspc_123"}), testLoaders(nil), discardLogger(),
		option.WithBaseURL(ws.URL), option.WithAPIKey("sk-ant-test-not-real"), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	got := ws.call(t, ep, "claude-opus-5-5")
	if got.path != "/v1/messages" {
		t.Fatalf("path = %q, want /v1/messages", got.path)
	}
	if h := got.header.Get(workspaceHeader); h != "wrkspc_123" {
		t.Fatalf("workspace header = %q, want wrkspc_123", h)
	}
	if got.header.Get("x-api-key") != "sk-ant-test-not-real" {
		t.Fatal("the Anthropic API key was not sent")
	}
	if !strings.Contains(got.body, `"model":"claude-opus-5-5"`) {
		t.Fatalf("body must carry the model: %s", got.body)
	}
}

func TestBedrockProviderBuildsSignedBedrockRequests(t *testing.T) {
	t.Setenv("AWS_BEARER_TOKEN_BEDROCK", "") // keep SigV4 regardless of the machine's env
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-must-not-leak")
	ws := newWireServer(t)
	var regionSeen string
	// The workspace id and an API key are set on purpose: neither may reach Bedrock.
	ep, err := newEndpoint(context.Background(), EndpointConfig{Provider: "bedrock", AWSRegion: "ap-south-1"},
		envOf(map[string]string{workspaceEnv: "wrkspc_123"}), testLoaders(&regionSeen), discardLogger(),
		option.WithBaseURL(ws.URL), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	if regionSeen != "ap-south-1" {
		t.Fatalf("AWS config loaded for region %q, want ap-south-1", regionSeen)
	}
	const modelID = "in.anthropic.claude-opus-5-5"
	got := ws.call(t, ep, modelID)

	// PIPEROOM_MODEL goes through as the Bedrock model / inference-profile id.
	if got.path != "/model/"+modelID+"/invoke-with-response-stream" {
		t.Fatalf("path = %q", got.path)
	}
	if a := got.header.Get("Authorization"); !strings.HasPrefix(a, "AWS4-HMAC-SHA256 ") ||
		!strings.Contains(a, "AKIDTEST/") || !strings.Contains(a, "/ap-south-1/bedrock/aws4_request") {
		t.Fatalf("Authorization = %q, want a SigV4 signature for ap-south-1/bedrock", a)
	}
	if got.header.Get(workspaceHeader) != "" || got.header.Get("x-api-key") != "" {
		t.Fatalf("Anthropic-only headers leaked to Bedrock: %v", got.header)
	}
	for _, want := range []string{`"anthropic_version":"bedrock-2023-05-31"`, `"anthropic_beta":["compact-2026-01-12"]`} {
		if !strings.Contains(got.body, want) {
			t.Fatalf("body lacks %s: %s", want, got.body)
		}
	}
	if strings.Contains(got.body, `"model"`) || strings.Contains(got.body, `"stream"`) {
		t.Fatalf("model and stream belong in the URL on Bedrock: %s", got.body)
	}
}

func TestBedrockProviderUsesBearerTokenWhenTheEnvironmentHasOne(t *testing.T) {
	t.Setenv("AWS_BEARER_TOKEN_BEDROCK", "bedrock-api-key-test")
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-must-not-leak")
	ws := newWireServer(t)
	ep, err := newEndpoint(context.Background(), EndpointConfig{Provider: "bedrock", AWSRegion: "us-east-1"},
		envOf(nil), testLoaders(nil), discardLogger(), option.WithBaseURL(ws.URL), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	got := ws.call(t, ep, "us.anthropic.claude-opus-5-5")
	if got.header.Get("Authorization") != "Bearer bedrock-api-key-test" {
		t.Fatalf("Authorization = %q", got.header.Get("Authorization"))
	}
}

func TestVertexProviderBuildsVertexRequests(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-must-not-leak")
	ws := newWireServer(t)
	ep, err := newEndpoint(context.Background(), EndpointConfig{Provider: "vertex", VertexProject: "acme-prod", VertexRegion: "europe-west1"},
		envOf(map[string]string{workspaceEnv: "wrkspc_123"}), testLoaders(nil), discardLogger(),
		option.WithBaseURL(ws.URL), option.WithMaxRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	got := ws.call(t, ep, "claude-opus-5-5")

	// The configured project and region win over the project the credentials carry.
	const wantPath = "/v1/projects/acme-prod/locations/europe-west1/publishers/anthropic/models/claude-opus-5-5:streamRawPredict"
	if got.path != wantPath {
		t.Fatalf("path = %q, want %q", got.path, wantPath)
	}
	if got.header.Get("Authorization") != "Bearer test-token" {
		t.Fatalf("Authorization = %q, want the ADC token", got.header.Get("Authorization"))
	}
	if got.header.Get(workspaceHeader) != "" || got.header.Get("x-api-key") != "" {
		t.Fatalf("Anthropic-only headers leaked to Vertex: %v", got.header)
	}
	if !strings.Contains(got.body, `"anthropic_version":"vertex-2023-10-16"`) {
		t.Fatalf("body lacks the Vertex version: %s", got.body)
	}
	if strings.Contains(got.body, `"model"`) {
		t.Fatalf("model belongs in the URL on Vertex: %s", got.body)
	}
	if got.header.Get("anthropic-beta") != "compact-2026-01-12" {
		t.Fatalf("the compaction beta must reach Vertex as a header, got %q", got.header.Get("anthropic-beta"))
	}
}

// The request the loop builds carries only fields the cloud providers support.
// This pins the shape: adaptive thinking, effort, strict tools with eager input
// streaming, compaction and cache control all pass through the Bedrock and
// Vertex adapters unchanged.
func TestLoopRequestPassesThroughTheCloudAdapters(t *testing.T) {
	r, err := newRunner(Alert{Title: "t", PipelineID: "p"}, Deps{
		Tools: newToolsRegistryWithOne(t), Inbox: &fakeInbox{}, Sink: &fakeSink{}, Logger: discardLogger(),
	}, Options{Model: "x", Effort: "high"})
	if err != nil {
		t.Fatal(err)
	}
	r.toolList = buildToolList(r.deps.Tools, newRoomTools(r.deps, r.proposals))
	r.system = systemPrompt()

	for _, cfg := range []EndpointConfig{
		{Provider: "bedrock", AWSRegion: "us-east-1"},
		{Provider: "vertex", VertexProject: "p", VertexRegion: "global"},
	} {
		t.Run(cfg.Provider, func(t *testing.T) {
			t.Setenv("AWS_BEARER_TOKEN_BEDROCK", "")
			t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-must-not-leak")
			ws := newWireServer(t)
			ep, err := newEndpoint(context.Background(), cfg, envOf(nil), testLoaders(nil), discardLogger(),
				option.WithBaseURL(ws.URL), option.WithMaxRetries(0))
			if err != nil {
				t.Fatal(err)
			}
			req := r.request([]llm.Message{userMessage(textPart("go"))})
			if _, err := ep.model.Turn(context.Background(), req); err == nil {
				t.Fatal("want the test server's 400")
			}
			ws.mu.Lock()
			body := ws.seen[0].body
			ws.mu.Unlock()
			for _, want := range []string{
				`"thinking":{"type":"adaptive"}`,
				`"effort":"high"`,
				`"strict":true`,
				`"eager_input_streaming":true`,
				`"compact_20260112"`,
				`"cache_control":{"type":"ephemeral"}`,
				`"tool_choice":{"type":"auto"}`,
			} {
				if !strings.Contains(body, want) {
					t.Fatalf("%s body lacks %s", cfg.Provider, want)
				}
			}
			for _, bad := range []string{`"budget_tokens"`, `"type":"disabled"`, `"type":"any"`, `"type":"tool"`} {
				if strings.Contains(body, bad) {
					t.Fatalf("%s body must not contain %s", cfg.Provider, bad)
				}
			}
		})
	}
}
