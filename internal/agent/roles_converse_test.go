package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/converse"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/config"
)

// fakeBedrockRuntime is a TLS Converse endpoint that answers every request
// with reply and counts the requests.
type fakeBedrockRuntime struct {
	*httptest.Server
	calls atomic.Int32
	paths atomic.Value // the last request's path
}

func newFakeBedrockRuntime(t *testing.T, reply map[string]any) *fakeBedrockRuntime {
	t.Helper()
	f := &fakeBedrockRuntime{}
	f.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		f.paths.Store(r.URL.EscapedPath())
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	}))
	t.Cleanup(f.Close)
	return f
}

// converseLoaders are testLoaders whose AWS config trusts f's certificate
// and whose bedrock-converse roles reach f.
func converseLoaders(f *fakeBedrockRuntime) providerLoaders {
	ld := testLoaders(nil)
	load := ld.awsConfig
	ld.awsConfig = func(ctx context.Context, region string) (aws.Config, error) {
		c, err := load(ctx, region)
		c.HTTPClient = f.Client()
		return c, err
	}
	ld.converseEndpoint = f.URL
	return ld
}

func converseEndpointConfig(t *testing.T, c config.Config) EndpointConfig {
	t.Helper()
	tp, err := config.ParseTriagePass(c)
	if err != nil {
		t.Fatal(err)
	}
	return EndpointConfig{Provider: c.ModelProvider, AWSRegion: c.AWSRegion, Knobs: knobsOf(t, c), TriagePass: tp.On, TriagePassModel: tp.Model}
}

// A Nova main model builds its client at start and sends nothing: the
// adapter reads no credentials and calls no model in New.
func TestConverseNovaBuildsWithoutARequest(t *testing.T) {
	f := newFakeBedrockRuntime(t, map[string]any{})
	c := config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}
	ep, err := newEndpoint(context.Background(), converseEndpointConfig(t, c), envOf(nil), converseLoaders(f), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if ep.Provider() != llm.ProviderBedrockConverse || ep.model.Provider() != llm.ProviderBedrockConverse {
		t.Fatalf("provider = %q, client %q", ep.Provider(), ep.model.Provider())
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("requests at build = %d, want 0", n)
	}
}

// An unverified Nova model passes the start checks: the one-request probe
// goes through the adapter to the (fake) regional endpoint and the probe
// tool is called with valid arguments.
func TestConverseUnverifiedNovaPassesTheProbe(t *testing.T) {
	f := newFakeBedrockRuntime(t, map[string]any{
		"output": map[string]any{"message": map[string]any{"role": "assistant", "content": []any{
			map[string]any{"toolUse": map[string]any{"toolUseId": "t1", "name": probeToolName, "input": map[string]any{"status": "ready"}}},
		}}},
		"stopReason": "tool_use",
		"usage":      map[string]any{"inputTokens": 900, "outputTokens": 12, "totalTokens": 912},
	})
	c := config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1", ModelUnverified: "on"}
	ep, err := newEndpoint(context.Background(), converseEndpointConfig(t, c), envOf(nil), converseLoaders(f), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	list, err := llm.SupportedModels()
	if err != nil {
		t.Fatal(err)
	}
	if err := ep.checkStart(context.Background(), startSets(), list, time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("checkStart: %v", err)
	}
	if n := f.calls.Load(); n != 1 {
		t.Fatalf("probe requests = %d, want 1", n)
	}
	if p, _ := f.paths.Load().(string); !strings.Contains(p, "amazon.nova-pro-v1") || !strings.HasSuffix(p, "/converse") {
		t.Fatalf("probe path = %q, want the Nova model's converse path", p)
	}
}

// A Claude id on bedrock-converse is refused before any request, naming the
// role's own variable, and errors.Is still finds converse.ErrClaudeModel.
func TestConverseClaudeModelRefusedNamingTheRole(t *testing.T) {
	f := newFakeBedrockRuntime(t, map[string]any{})
	for _, tt := range []struct {
		name string
		cfg  config.Config
		want string
	}{
		{"main", config.Config{ModelProvider: "bedrock-converse", Model: "anthropic.claude-opus-5-5-v1:0", AWSRegion: "us-east-1"}, "PIPEROOM_MODEL="},
		{"summary", config.Config{Model: "claude-opus-5-5", SummaryProvider: "bedrock-converse", SummaryModel: "us.anthropic.claude-opus-5-5-v1:0", AWSRegion: "us-east-1"}, "PIPEROOM_SUMMARY_MODEL="},
		{"first look", config.Config{Model: "claude-opus-5-5", TriagePassProvider: "bedrock-converse", TriagePassModel: "claude-haiku-5-5", AWSRegion: "us-east-1"}, "PIPEROOM_TRIAGE_PASS_MODEL="},
		{"in-loop triage", config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", TriageModel: "us.anthropic.claude-haiku-5-5-v1:0", AWSRegion: "us-east-1"}, "PIPEROOM_TRIAGE_MODEL="},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newEndpoint(context.Background(), converseEndpointConfig(t, tt.cfg), envOf(nil), converseLoaders(f), discardLogger())
			if !errors.Is(err, converse.ErrClaudeModel) {
				t.Fatalf("err = %v, want converse.ErrClaudeModel", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %q, want it to name %s", err, tt.want)
			}
		})
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("requests = %d, want 0", n)
	}
}

// Two roles on bedrock-converse in the same region share one limiter (design
// 7.4), and each adapter sees every bedrock-converse role's model id.
func TestConverseRolesShareTheRegionLimiter(t *testing.T) {
	c := config.Config{Model: "claude-opus-5-5", AWSRegion: "us-east-1",
		SummaryProvider: "bedrock-converse", SummaryModel: "amazon.nova-lite-v1:0",
		JudgeProvider: "bedrock-converse", JudgeModel: "amazon.nova-pro-v1:0"}
	ec, _, err := converseEndpointConfig(t, c).normalized()
	if err != nil {
		t.Fatal(err)
	}
	b := clientBuilder{c: ec, getenv: envOf(nil), ld: testLoaders(nil), log: discardLogger(),
		limits: retry.NewLimiters(retry.Limits{MaxConcurrency: 2}, discardLogger())}
	sum, err := b.converseConfig(context.Background(), "PIPEROOM_SUMMARY", false)
	if err != nil {
		t.Fatal(err)
	}
	judge, err := b.converseConfig(context.Background(), "PIPEROOM_JUDGE", false)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Limiter == nil || sum.Limiter != judge.Limiter {
		t.Fatalf("limiters %p and %p, want one shared limiter", sum.Limiter, judge.Limiter)
	}
	if other := b.limits.For(bedrockOrigin("eu-west-1", "")); other == sum.Limiter {
		t.Fatal("another region shares the limiter")
	}
	want := []string{"amazon.nova-lite-v1:0", "amazon.nova-pro-v1:0"}
	if !slices.Equal(sum.Models, want) || !slices.Equal(judge.Models, want) {
		t.Fatalf("models = %v / %v, want %v", sum.Models, judge.Models, want)
	}
	if sum.Region != "us-east-1" || sum.AWS == nil || sum.AWS.Region != "us-east-1" {
		t.Fatalf("region = %q, aws %+v", sum.Region, sum.AWS)
	}

	// The endpoint builds both roles, each on its own bedrock-converse client.
	ep, err := newEndpoint(context.Background(), converseEndpointConfig(t, c), envOf(nil), testLoaders(nil), discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if ep.summaryClient().Provider() != llm.ProviderBedrockConverse || ep.judgeClient().Provider() != llm.ProviderBedrockConverse ||
		ep.model.Provider() != llm.ProviderAnthropic {
		t.Fatalf("clients: main %q, summary %q, judge %q", ep.model.Provider(), ep.summaryClient().Provider(), ep.judgeClient().Provider())
	}
}

// The shared AWS config from wiring.LoadAWS is used, not a second load, and
// a Bedrock API key replaces the credential check as on bedrock.
func TestConverseUsesTheSharedAWSConfig(t *testing.T) {
	c := config.Config{ModelProvider: "bedrock-converse", Model: "amazon.nova-pro-v1:0", AWSRegion: "us-east-1"}
	ld := testLoaders(nil)
	ld.awsConfig = func(context.Context, string) (aws.Config, error) {
		t.Fatal("the endpoint loaded its own AWS config")
		return aws.Config{}, nil
	}
	var fetched atomic.Int32
	shared := &aws.Config{Region: "eu-west-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		fetched.Add(1)
		return aws.Credentials{AccessKeyID: "AKIDSHARED", SecretAccessKey: "shared-secret"}, nil
	})}
	ec := converseEndpointConfig(t, c)
	ec.AWSConfig = shared
	if _, err := newEndpoint(context.Background(), ec, envOf(nil), ld, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if fetched.Load() != 1 || shared.Region != "eu-west-1" {
		t.Fatalf("fetches = %d, shared region %q: want 1 and unchanged", fetched.Load(), shared.Region)
	}

	// No credentials: refused, unless the Bedrock API key is set.
	ec.AWSConfig = &aws.Config{}
	_, err := newEndpoint(context.Background(), ec, envOf(nil), ld, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "PIPEROOM_MODEL_PROVIDER=bedrock-converse needs AWS credentials") {
		t.Fatalf("err = %v", err)
	}
	const key = "ABSKbedrock-api-key-not-real"
	if _, err := newEndpoint(context.Background(), ec, envOf(map[string]string{converse.BearerTokenEnv: key}), ld, discardLogger()); err != nil {
		t.Fatalf("with a Bedrock API key: %v", err)
	}

	// A broken chain fails at start, naming the provider variable.
	boom := errors.New("no valid providers in chain")
	ec.AWSConfig = &aws.Config{Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return aws.Credentials{}, boom })}
	if _, err := newEndpoint(context.Background(), ec, envOf(nil), ld, discardLogger()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "bedrock-converse") {
		t.Fatalf("err = %v, want the wrapped retrieve error", err)
	}
}
