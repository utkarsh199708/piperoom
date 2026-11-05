package agent

// bedrock-converse roles (V5's adapter, internal/agent/llm/converse, wired
// into V6's roles): non-Claude models on Amazon Bedrock. They load AWS the
// way the Claude adapter's bedrock path does (the shared config from
// wiring.LoadAWS, else the default chain), and every role on the same region
// shares one limiter (design 7.4).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/converse"
)

// converseRole is one configured role on bedrock-converse, for the model
// checks: the variables that name its model and its provider, and the id.
type converseRole struct {
	modelVar, providerVar string
	model                 string
}

// converseRoles lists every role of c on bedrock-converse whose model id is
// known at start, the main one first. Without Knobs the main model comes
// from each request's Options.Model, so nothing is listed.
func converseRoles(c EndpointConfig) []converseRole {
	if c.Knobs == nil {
		return nil
	}
	k := c.Knobs.Roles
	var out []converseRole
	if k.Main.Provider == llm.ProviderBedrockConverse {
		out = append(out, converseRole{"PIPEROOM_MODEL", "PIPEROOM_MODEL_PROVIDER", k.Main.Model})
		// The in-loop triage turns run on the main client (one history).
		if k.Triage != "" {
			out = append(out, converseRole{"PIPEROOM_TRIAGE_MODEL", "PIPEROOM_MODEL_PROVIDER", k.Triage})
		}
	}
	// A first look that is off makes no model call (buildRoles skips it);
	// when on, its model is the triage pass's, as buildRoles sends it.
	if tp := triagePassSettings(c); tp.on && k.FirstLook.Provider == llm.ProviderBedrockConverse {
		out = append(out, converseRole{"PIPEROOM_TRIAGE_PASS_MODEL", "PIPEROOM_TRIAGE_PASS_PROVIDER", tp.model})
	}
	if k.Summary.Provider == llm.ProviderBedrockConverse {
		out = append(out, converseRole{"PIPEROOM_SUMMARY_MODEL", "PIPEROOM_SUMMARY_PROVIDER", k.Summary.Model})
	}
	if k.Judge.Provider == llm.ProviderBedrockConverse {
		out = append(out, converseRole{"PIPEROOM_JUDGE_MODEL", "PIPEROOM_JUDGE_PROVIDER", k.Judge.Model})
	}
	return out
}

// converseModels checks the model id of every bedrock-converse role and
// returns the ids, each once. A Claude id is refused naming the role's own
// variable, wrapping converse.ErrClaudeModel: the main client serves the
// roles that share it, so the main build must name a summary role's
// variable when that role's id is the bad one.
func converseModels(c EndpointConfig) ([]string, error) {
	var ids []string
	seen := map[string]bool{}
	for _, r := range converseRoles(c) {
		id := strings.TrimSpace(r.model)
		if id == "" {
			continue
		}
		if err := converse.CheckModel(id); err != nil {
			if errors.Is(err, converse.ErrClaudeModel) {
				if r.providerVar == "PIPEROOM_MODEL_PROVIDER" {
					return nil, fmt.Errorf("agent: %s=%q is a Claude model, which bedrock-converse does not serve: %w", r.modelVar, id, converse.ErrClaudeModel)
				}
				return nil, fmt.Errorf("agent: %s=%q is a Claude model, which bedrock-converse does not serve: set %s=bedrock for this role (%w)",
					r.modelVar, id, r.providerVar, converse.ErrClaudeModel)
			}
			return nil, fmt.Errorf("agent: %s: %w", r.modelVar, err)
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// bedrockOrigin is the limiter key of a Bedrock region: its runtime
// endpoint, or the test endpoint override when set. Every bedrock-converse
// role in the region shares the one limiter (design 7.4). The Claude
// adapter's bedrock path keeps the SDK's own behaviour and takes no limiter.
func bedrockOrigin(region, override string) string {
	if override != "" {
		return override
	}
	host := "https://bedrock-runtime." + region + ".amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		host += ".cn"
	}
	return host
}

// converseConfig is the adapter configuration of a bedrock-converse role:
// the shared AWS config (or the default chain in AWS_REGION), the region's
// shared limiter, and every bedrock-converse role's model id. Like the
// bedrock path it fetches the credentials once at start, unless a Bedrock
// API key replaces them. Every error names the variable to set and never a
// key.
func (b clientBuilder) converseConfig(ctx context.Context, env string, main bool) (converse.Config, error) {
	c := b.c
	pv := providerVar(env, main)
	models, err := converseModels(c)
	if err != nil {
		return converse.Config{}, err
	}
	region := strings.TrimSpace(c.AWSRegion)
	if region == "" {
		return converse.Config{}, fmt.Errorf("agent: %s=bedrock-converse needs AWS_REGION", pv)
	}
	var ac aws.Config
	if c.AWSConfig != nil {
		// A shallow copy: the region stays local, the credentials provider
		// and its cache are the process's shared ones.
		ac = c.AWSConfig.Copy()
	} else {
		if ac, err = b.ld.awsConfig(ctx, region); err != nil {
			return converse.Config{}, fmt.Errorf("agent: load AWS config for %s=bedrock-converse: %w", pv, err)
		}
	}
	ac.Region = region
	// A Bedrock API key (AWS_BEARER_TOKEN_BEDROCK) replaces AWS credentials.
	if b.getenv(converse.BearerTokenEnv) == "" {
		if ac.Credentials == nil {
			return converse.Config{}, fmt.Errorf("agent: %s=bedrock-converse needs AWS credentials (environment, shared config, or an instance/task role) or %s; none were found", pv, converse.BearerTokenEnv)
		}
		cctx, cancel := context.WithTimeout(ctx, credentialCheckTimeout)
		_, err = ac.Credentials.Retrieve(cctx)
		cancel()
		if err != nil {
			// The SDK error names the providers it tried, never key material.
			return converse.Config{}, fmt.Errorf("agent: retrieve AWS credentials for %s=bedrock-converse: %w", pv, err)
		}
	}
	return converse.Config{
		AWS:          &ac,
		Region:       region,
		BaseEndpoint: b.ld.converseEndpoint,
		Limiter:      b.limits.For(bedrockOrigin(region, b.ld.converseEndpoint)),
		Models:       models,
		Getenv:       b.getenv,
		Log:          b.log,
	}, nil
}

// bedrockConverse builds a bedrock-converse role's client.
func (b clientBuilder) bedrockConverse(ctx context.Context, env string, main bool) (model, error) {
	cfg, err := b.converseConfig(ctx, env, main)
	if err != nil {
		return nil, err
	}
	m, err := converse.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("agent: %s=bedrock-converse: %w", providerVar(env, main), err)
	}
	return m, nil
}
