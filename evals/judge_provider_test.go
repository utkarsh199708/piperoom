package evals

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"piperoom/internal/config"
)

// fakeJudgeLoaders returns test credentials; nothing reads the machine's.
func fakeJudgeLoaders() judgeLoaders {
	return judgeLoaders{
		awsConfig: func(_ context.Context, region string) (aws.Config, error) {
			return aws.Config{Region: region, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "AKIDTEST", SecretAccessKey: "test-secret"}, nil
			})}, nil
		},
		googleCreds: func(context.Context) (*google.Credentials, error) {
			return &google.Credentials{ProjectID: "adc-project", TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"})}, nil
		},
	}
}

func judgeEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// TestJudgeClientOptionsErrors checks that every missing setting or credential
// is a wrapped error, never a panic.
func TestJudgeClientOptionsErrors(t *testing.T) {
	boom := errors.New("no credentials")
	failing := judgeLoaders{
		awsConfig:   func(context.Context, string) (aws.Config, error) { return aws.Config{}, boom },
		googleCreds: func(context.Context) (*google.Credentials, error) { return nil, boom },
	}
	noAWSCreds := fakeJudgeLoaders()
	noAWSCreds.awsConfig = func(_ context.Context, r string) (aws.Config, error) { return aws.Config{Region: r}, nil }
	retrieveFails := fakeJudgeLoaders()
	retrieveFails.awsConfig = func(_ context.Context, r string) (aws.Config, error) {
		return aws.Config{Region: r, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{}, boom
		})}, nil
	}
	nilGoogle := fakeJudgeLoaders()
	nilGoogle.googleCreds = func(context.Context) (*google.Credentials, error) { return nil, nil }

	for _, tc := range []struct {
		name   string
		cfg    config.Config
		ld     judgeLoaders
		want   string
		wantIs error
	}{
		{"unknown provider", config.Config{ModelProvider: "azure"}, fakeJudgeLoaders(), `unknown PIPEROOM_MODEL_PROVIDER "azure"`, nil},
		{"bedrock without a region", config.Config{ModelProvider: "bedrock"}, fakeJudgeLoaders(), "AWS_REGION", nil},
		{"bedrock config fails", config.Config{ModelProvider: "bedrock", AWSRegion: "ap-south-1"}, failing, "load AWS config", boom},
		{"bedrock without credentials", config.Config{ModelProvider: "bedrock", AWSRegion: "ap-south-1"}, noAWSCreds, "AWS credentials", nil},
		{"bedrock retrieve fails", config.Config{ModelProvider: "bedrock", AWSRegion: "ap-south-1"}, retrieveFails, "retrieve AWS credentials", boom},
		{"vertex without a project", config.Config{ModelProvider: "vertex", VertexRegion: "global"}, fakeJudgeLoaders(), "VERTEX_PROJECT_ID", nil},
		{"vertex without a region", config.Config{ModelProvider: "vertex", VertexProject: "p"}, fakeJudgeLoaders(), "VERTEX_REGION", nil},
		{"vertex credentials fail", config.Config{ModelProvider: "vertex", VertexProject: "p", VertexRegion: "global"}, failing, "application default credentials", boom},
		{"vertex nil credentials", config.Config{ModelProvider: "vertex", VertexProject: "p", VertexRegion: "global"}, nilGoogle, "application default credentials", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, opts, err := judgeClientOptions(context.Background(), tc.cfg, judgeEnv(nil), tc.ld)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "evals: judge: ") {
				t.Fatalf("err = %v, want one naming %q", err, tc.want)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v does not wrap the loader error", err)
			}
			if opts != nil {
				t.Fatalf("options returned with an error: %d", len(opts))
			}
		})
	}
}

// TestJudgeClientOptionsRealLoadersNoCredentials runs the real credential
// loaders on a machine with no credentials (every lookup pointed at a missing
// file, instance metadata off): each provider returns an error, not a panic,
// and nothing is sent anywhere.
func TestJudgeClientOptionsRealLoadersNoCredentials(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for k, v := range map[string]string{
		"GOOGLE_APPLICATION_CREDENTIALS":         missing,
		"CLOUDSDK_CONFIG":                        missing,
		"AWS_CONFIG_FILE":                        missing,
		"AWS_SHARED_CREDENTIALS_FILE":            missing,
		"AWS_EC2_METADATA_DISABLED":              "true",
		"AWS_ACCESS_KEY_ID":                      "",
		"AWS_SECRET_ACCESS_KEY":                  "",
		"AWS_SESSION_TOKEN":                      "",
		"AWS_PROFILE":                            "",
		"AWS_WEB_IDENTITY_TOKEN_FILE":            "",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI":     "",
		"AWS_BEARER_TOKEN_BEDROCK":               "",
	} {
		t.Setenv(k, v)
	}
	for _, cfg := range []config.Config{
		{ModelProvider: "vertex", VertexProject: "p", VertexRegion: "global"},
		{ModelProvider: "bedrock", AWSRegion: "ap-south-1"},
	} {
		_, _, err := JudgeClientOptions(context.Background(), cfg, judgeEnv(nil))
		if err == nil || !strings.HasPrefix(err.Error(), "evals: judge: ") {
			t.Errorf("%s: err = %v, want a judge setup error", cfg.ModelProvider, err)
		}
	}
}

// judgeWire records the one request a client sends.
type judgeWire struct {
	path   string
	header http.Header
	body   string
}

// callThrough sends one non-streaming message (as the judge does) through
// opts to a local server and returns what reached it.
func callThrough(t *testing.T, model string, opts []option.RequestOption) judgeWire {
	t.Helper()
	var got judgeWire
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = judgeWire{path: r.URL.Path, header: r.Header.Clone(), body: string(b)}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"m","stop_reason":"end_turn",`+
			`"content":[{"type":"text","text":"{}"}],"usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer srv.Close()
	client := anthropic.NewClient(append(opts, option.WithBaseURL(srv.URL), option.WithMaxRetries(0))...)
	_, err := client.Messages.New(context.Background(), anthropic.MessageNewParams{
		Model:     model,
		MaxTokens: 16,
		Messages:  []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("x"))},
		OutputConfig: anthropic.OutputConfigParam{
			Effort: anthropic.OutputConfigEffortLow,
			Format: anthropic.JSONOutputFormatParam{Schema: map[string]any{"type": "object"}},
		},
	})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	return got
}

// TestJudgeClientOptionsPickTheProvider checks on the wire that each provider
// is the one the request goes to, that Anthropic-only headers stay off the
// cloud providers, and that the judge's effort and JSON-schema output reach
// every provider.
func TestJudgeClientOptionsPickTheProvider(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-must-not-leak")
	t.Setenv("AWS_BEARER_TOKEN_BEDROCK", "") // SigV4 regardless of the machine's env
	env := judgeEnv(map[string]string{"ANTHROPIC_WORKSPACE_ID": "wrkspc_123"})

	t.Run("anthropic is the default", func(t *testing.T) {
		p, opts, err := judgeClientOptions(context.Background(), config.Config{}, env, fakeJudgeLoaders())
		if err != nil || p != "anthropic" {
			t.Fatalf("provider = %q err = %v", p, err)
		}
		got := callThrough(t, "judge-model", opts)
		if got.path != "/v1/messages" || got.header.Get("anthropic-workspace-id") != "wrkspc_123" || got.header.Get("x-api-key") == "" {
			t.Fatalf("path = %q headers = %v", got.path, got.header)
		}
		if !strings.Contains(got.body, `"model":"judge-model"`) {
			t.Fatalf("body = %s", got.body)
		}
	})

	t.Run("vertex", func(t *testing.T) {
		p, opts, err := judgeClientOptions(context.Background(), config.Config{ModelProvider: " Vertex ", VertexProject: "acme", VertexRegion: "us-east5"}, env, fakeJudgeLoaders())
		if err != nil || p != "vertex" {
			t.Fatalf("provider = %q err = %v", p, err)
		}
		got := callThrough(t, "claude-vertex-id", opts)
		if want := "/v1/projects/acme/locations/us-east5/publishers/anthropic/models/claude-vertex-id:rawPredict"; got.path != want {
			t.Fatalf("path = %q, want %q", got.path, want)
		}
		if got.header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("Authorization = %q", got.header.Get("Authorization"))
		}
		if got.header.Get("anthropic-workspace-id") != "" || got.header.Get("x-api-key") != "" {
			t.Fatalf("Anthropic-only headers reached Vertex: %v", got.header)
		}
		for _, want := range []string{`"anthropic_version":"vertex-2023-10-16"`, `"effort":"low"`, `"json_schema"`} {
			if !strings.Contains(got.body, want) {
				t.Fatalf("body lacks %s: %s", want, got.body)
			}
		}
	})

	t.Run("bedrock", func(t *testing.T) {
		p, opts, err := judgeClientOptions(context.Background(), config.Config{ModelProvider: "bedrock", AWSRegion: "ap-south-1"}, env, fakeJudgeLoaders())
		if err != nil || p != "bedrock" {
			t.Fatalf("provider = %q err = %v", p, err)
		}
		const id = "in.anthropic.judge-id"
		got := callThrough(t, id, opts)
		if got.path != "/model/"+id+"/invoke" {
			t.Fatalf("path = %q", got.path)
		}
		if a := got.header.Get("Authorization"); !strings.HasPrefix(a, "AWS4-HMAC-SHA256 ") || !strings.Contains(a, "/ap-south-1/bedrock/aws4_request") {
			t.Fatalf("Authorization = %q, want SigV4 for ap-south-1/bedrock", a)
		}
		if got.header.Get("anthropic-workspace-id") != "" || got.header.Get("x-api-key") != "" {
			t.Fatalf("Anthropic-only headers reached Bedrock: %v", got.header)
		}
		for _, want := range []string{`"anthropic_version":"bedrock-2023-05-31"`, `"effort":"low"`, `"json_schema"`} {
			if !strings.Contains(got.body, want) {
				t.Fatalf("body lacks %s: %s", want, got.body)
			}
		}
	})

	t.Run("a Bedrock API key needs no AWS credentials", func(t *testing.T) {
		ld := fakeJudgeLoaders()
		ld.awsConfig = func(_ context.Context, r string) (aws.Config, error) { return aws.Config{Region: r}, nil }
		_, _, err := judgeClientOptions(context.Background(), config.Config{ModelProvider: "bedrock", AWSRegion: "us-east-1"},
			judgeEnv(map[string]string{"AWS_BEARER_TOKEN_BEDROCK": "bedrock-api-key-test"}), ld)
		if err != nil {
			t.Fatal(err)
		}
	})
}
