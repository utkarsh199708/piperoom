package wiring

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"piperoom/internal/agent"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

const (
	// awsCredentialCheckTimeout bounds the one startup credential fetch.
	awsCredentialCheckTimeout = 5 * time.Second
	// mskIAMCredentialTimeout bounds the credential fetch of one MSK IAM SASL
	// session. The default chain caches, so this is a deadline for the rare
	// refresh (IMDS, STS for IRSA), not the common path.
	mskIAMCredentialTimeout = 10 * time.Second
	// bedrockTokenEnv is the Bedrock API key the SDK reads itself; with it,
	// Bedrock needs no AWS credentials (same rule as the agent endpoint).
	bedrockTokenEnv = "AWS_BEARER_TOKEN_BEDROCK"
)

// awsLoader loads the default AWS config in region. LoadAWS uses the SDK's
// default chain; tests pass a counting fake.
type awsLoader func(ctx context.Context, region string) (aws.Config, error)

func defaultAWSLoader(ctx context.Context, region string) (aws.Config, error) {
	return awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
}

// LoadAWS loads the default AWS credential chain (environment, shared config,
// instance or task role, IRSA) once, in cfg.AWSRegion, when something in this
// process needs it: a model role on bedrock or bedrock-converse
// (PIPEROOM_MODEL_PROVIDER or a role's PIPEROOM_<ROLE>_PROVIDER) or
// KAFKA_SASL_MECHANISM=AWS_MSK_IAM. Otherwise it returns nil, nil. It fetches
// the credentials once (bounded by 5 s) so a broken chain stops the process at
// start. The result is passed to both NewAgentEndpoint and Open so Bedrock,
// Bedrock Converse and MSK IAM share one config and one credentials cache.
func LoadAWS(ctx context.Context, cfg config.Config) (*aws.Config, error) {
	return loadAWS(ctx, cfg, os.Getenv, defaultAWSLoader)
}

func loadAWS(ctx context.Context, cfg config.Config, getenv func(string) string, load awsLoader) (*aws.Config, error) {
	models, mskIAM := awsUsers(cfg)
	if len(models) == 0 && !mskIAM {
		return nil, nil
	}
	users := models
	if mskIAM {
		users = append(users, "KAFKA_SASL_MECHANISM="+netsec.MechAWSMSKIAM)
	}
	who := strings.Join(users, " and ")

	region := strings.TrimSpace(cfg.AWSRegion)
	if region == "" {
		return nil, fmt.Errorf("wiring: %s needs AWS_REGION", who)
	}
	ac, err := load(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("wiring: load AWS config for %s: %w", who, err)
	}
	ac.Region = region

	// A Bedrock API key replaces AWS credentials for Bedrock and Bedrock
	// Converse only; MSK IAM always signs with real credentials.
	if mskIAM || getenv(bedrockTokenEnv) == "" {
		if ac.Credentials == nil {
			return nil, fmt.Errorf("wiring: %s needs AWS credentials (environment, shared config, or an instance/task role); none were found", who)
		}
		cctx, cancel := context.WithTimeout(ctx, awsCredentialCheckTimeout)
		_, err = ac.Credentials.Retrieve(cctx)
		cancel()
		if err != nil {
			// The SDK error names the providers it tried, never key material.
			return nil, fmt.Errorf("wiring: retrieve AWS credentials for %s: %w", who, err)
		}
	}
	return &ac, nil
}

// awsUsers reports which parts of the process need AWS credentials: models
// names each provider variable set to bedrock or bedrock-converse (the
// main one, then the first look, summary and judge roles that set their
// own), and mskIAM is KAFKA_SASL_MECHANISM=AWS_MSK_IAM. A role that sets no
// provider runs on the main one, which is already counted.
func awsUsers(cfg config.Config) (models []string, mskIAM bool) {
	for _, v := range []struct{ name, value string }{
		{"PIPEROOM_MODEL_PROVIDER", cfg.ModelProvider},
		{"PIPEROOM_TRIAGE_PASS_PROVIDER", cfg.TriagePassProvider},
		{"PIPEROOM_SUMMARY_PROVIDER", cfg.SummaryProvider},
		{"PIPEROOM_JUDGE_PROVIDER", cfg.JudgeProvider},
	} {
		p := strings.ToLower(strings.TrimSpace(v.value))
		if p == agent.ProviderBedrock || p == config.ModelProviderBedrockConverse {
			models = append(models, v.name+"="+p)
		}
	}
	return models, usesMSKIAM(cfg)
}

func usesMSKIAM(cfg config.Config) bool {
	return strings.EqualFold(strings.TrimSpace(cfg.KafkaSASLMechanism), netsec.MechAWSMSKIAM)
}

// errNoSharedAWS is returned by Open when MSK IAM is on but the caller passed
// no shared AWS config: a command that forgot LoadAWS fails loudly instead of
// loading the chain a second time.
var errNoSharedAWS = errors.New("wiring: KAFKA_SASL_MECHANISM=AWS_MSK_IAM needs the shared AWS config from wiring.LoadAWS; none was passed")

// mskIAMCredentials adapts the shared AWS config to netsec's per-session
// credentials func. The keys go straight to franz-go's signer; they are never
// logged and the error carries only the SDK's description of the lookup.
func mskIAMCredentials(awsCfg *aws.Config) func(context.Context) (netsec.AWSCredentials, error) {
	provider := awsCfg.Credentials
	return func(ctx context.Context) (netsec.AWSCredentials, error) {
		ctx, cancel := context.WithTimeout(ctx, mskIAMCredentialTimeout)
		defer cancel()
		c, err := provider.Retrieve(ctx)
		if err != nil {
			return netsec.AWSCredentials{}, fmt.Errorf("wiring: retrieve AWS credentials for MSK IAM: %w", err)
		}
		return netsec.AWSCredentials{AccessKey: c.AccessKeyID, SecretKey: c.SecretAccessKey, SessionToken: c.SessionToken}, nil
	}
}
