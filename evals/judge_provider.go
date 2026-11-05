package evals

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go/bedrock"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/vertex"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/oauth2/google"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// The judge reaches the model the same way the agent does, but internal/agent
// exports no constructor for the provider's SDK options (its providerOptions
// is unexported, and Endpoint offers no single-message call). So this file
// mirrors internal/agent/endpoint.go: the provider names, the required
// settings, the Bedrock API-key shortcut, and the credential handling that
// never lets the SDK panic. Keep the two in step; the follow-up is for A4 to
// export one shared constructor that both use.

// judgeCredentialTimeout bounds the one AWS credential fetch at judge
// construction (endpoint.go's credentialCheckTimeout).
const judgeCredentialTimeout = 5 * time.Second

// judgeBedrockTokenEnv is the Bedrock API key variable the SDK reads itself.
const judgeBedrockTokenEnv = "AWS_BEARER_TOKEN_BEDROCK"

// judgeGoogleScope is the OAuth scope Vertex AI calls need.
const judgeGoogleScope = "https://www.googleapis.com/auth/cloud-platform"

// judgeLoaders are the two places that read cloud credentials. Tests replace
// them so building the options never touches the network or the machine's
// real credentials.
type judgeLoaders struct {
	awsConfig   func(ctx context.Context, region string) (aws.Config, error)
	googleCreds func(ctx context.Context) (*google.Credentials, error)
}

func defaultJudgeLoaders() judgeLoaders {
	return judgeLoaders{
		awsConfig: func(ctx context.Context, region string) (aws.Config, error) {
			return awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		},
		googleCreds: func(ctx context.Context) (*google.Credentials, error) {
			return google.FindDefaultCredentials(ctx, judgeGoogleScope)
		},
	}
}

// workspaceOptions adds the workspace header when ANTHROPIC_WORKSPACE_ID is
// set. A user API key (sk-ant-usr-...) is not scoped to a workspace, and the
// API rejects its requests with a 400 unless they carry
// "anthropic-workspace-id"; internal/agent sends the same header for the
// agent. The id is not a secret. A blank value adds nothing, so a
// workspace-scoped key behaves as before. It applies to the anthropic
// provider only: Bedrock and Vertex sign their own requests.
func workspaceOptions(getenv func(string) string) []option.RequestOption {
	if id := strings.TrimSpace(getenv("ANTHROPIC_WORKSPACE_ID")); id != "" {
		return []option.RequestOption{option.WithHeader("anthropic-workspace-id", id)}
	}
	return nil
}

// JudgeClientOptions returns the SDK request options that point the LLM judge
// at cfg's model provider (PIPEROOM_MODEL_PROVIDER: anthropic, bedrock or
// vertex) and the normalized provider name. It reads cloud credentials (and,
// for Bedrock, fetches them once with a deadline, so missing credentials fail
// before the first episode) but sends no model request. A missing region or
// project, an unknown provider and a credential failure are errors, never a
// panic. The Anthropic API key is left to the SDK and never read here.
func JudgeClientOptions(ctx context.Context, cfg config.Config, getenv func(string) string) (string, []option.RequestOption, error) {
	return judgeClientOptions(ctx, cfg, getenv, defaultJudgeLoaders())
}

func judgeClientOptions(ctx context.Context, cfg config.Config, getenv func(string) string, ld judgeLoaders) (provider string, opts []option.RequestOption, err error) {
	provider = strings.ToLower(strings.TrimSpace(cfg.ModelProvider))
	if provider == "" {
		provider = agent.ProviderAnthropic
	}
	switch provider {
	case agent.ProviderAnthropic:
		return provider, workspaceOptions(getenv), nil

	case agent.ProviderBedrock:
		region := strings.TrimSpace(cfg.AWSRegion)
		if region == "" {
			return provider, nil, errors.New("evals: judge: PIPEROOM_MODEL_PROVIDER=bedrock needs AWS_REGION")
		}
		// bedrock.WithLoadDefaultConfig panics when the AWS config cannot be
		// loaded, so the config is loaded here and passed to WithConfig.
		awsCfg, err := ld.awsConfig(ctx, region)
		if err != nil {
			return provider, nil, fmt.Errorf("evals: judge: load AWS config for bedrock: %w", err)
		}
		awsCfg.Region = region
		// A Bedrock API key (AWS_BEARER_TOKEN_BEDROCK) replaces AWS credentials.
		if getenv(judgeBedrockTokenEnv) == "" {
			if awsCfg.Credentials == nil {
				return provider, nil, errors.New("evals: judge: bedrock needs AWS credentials (environment, shared config, or an instance/task role); none were found")
			}
			cctx, cancel := context.WithTimeout(ctx, judgeCredentialTimeout)
			_, err = awsCfg.Credentials.Retrieve(cctx)
			cancel()
			if err != nil {
				return provider, nil, fmt.Errorf("evals: judge: retrieve AWS credentials for bedrock: %w", err)
			}
		}
		return provider, []option.RequestOption{bedrock.WithConfig(awsCfg)}, nil

	case agent.ProviderVertex:
		project, region := strings.TrimSpace(cfg.VertexProject), strings.TrimSpace(cfg.VertexRegion)
		if project == "" {
			return provider, nil, errors.New("evals: judge: PIPEROOM_MODEL_PROVIDER=vertex needs VERTEX_PROJECT_ID")
		}
		if region == "" {
			return provider, nil, errors.New("evals: judge: PIPEROOM_MODEL_PROVIDER=vertex needs VERTEX_REGION")
		}
		// vertex.WithGoogleAuth panics when no Google default credentials are
		// found, so they are found here and passed to WithCredentials.
		creds, err := ld.googleCreds(ctx)
		if err != nil {
			return provider, nil, fmt.Errorf("evals: judge: find Google application default credentials for vertex (run gcloud auth application-default login): %w", err)
		}
		if creds == nil {
			return provider, nil, errors.New("evals: judge: no Google application default credentials for vertex")
		}
		// vertex.WithCredentials panics on a failure it cannot report.
		defer func() {
			if p := recover(); p != nil {
				opts, err = nil, fmt.Errorf("evals: judge: set up vertex client: %v", p)
			}
		}()
		return provider, []option.RequestOption{vertex.WithCredentials(ctx, region, project, creds)}, nil

	default:
		return provider, nil, fmt.Errorf("evals: judge: unknown PIPEROOM_MODEL_PROVIDER %q; allowed values: %s, %s, %s",
			provider, agent.ProviderAnthropic, agent.ProviderBedrock, agent.ProviderVertex)
	}
}
