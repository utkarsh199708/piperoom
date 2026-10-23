package agent

// One llm.Model per model role (V6, docs/design/model-providers.md section 9):
// the investigation loop and its in-loop triage turns on the main provider
// (they share one history), and the first look, the hand-off and post-mortem
// writer, and the judge each on their own provider. A role on the main
// provider shares the main client, so a Claude deployment that sets none of
// the V0 variables builds exactly one client, as before.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
	"piperoom/internal/agent/llm/gemini"
	"piperoom/internal/agent/llm/openai"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// Role names in logs, errors and the start checks.
const (
	roleMain      = "main"
	roleTriage    = "triage"
	roleFirstLook = "first_look"
	roleSummary   = "summary"
	roleJudge     = "judge"
)

// ModelFactory builds the client of a provider this package has no adapter
// for yet. It is the hook for bedrock-converse: V5's adapter is wired here
// when it lands (wiring passes a factory in EndpointConfig.Converse); until
// then a nil factory refuses that provider at start.
type ModelFactory interface {
	// NewModel builds the client of role. main is true for the main role,
	// whose key may come from PIPEROOM_MODEL_API_KEY; another role reads
	// only its provider's standard credentials.
	NewModel(ctx context.Context, role config.ModelRole, main bool) (llm.Model, error)
}

// roleClient is the client and model id of one single-shot role. A nil m
// means the main client (Endpoint.model); an empty modelID means the
// request's Options.Model, as before V6.
type roleClient struct {
	m       model
	modelID string
	quals   []string
}

// roleSpec is one configured role, for the start checks, the pricing rule and
// the startup log.
type roleSpec struct {
	name string           // roleMain, roleTriage, ...
	env  string           // the variable that names its model
	role config.ModelRole // provider, flavor, base URL and model id
	m    model            // the role's client
	// loop is true for the roles that send the investigation's tools.
	loop bool
	// region is where the role's requests are processed, for the log: the
	// cloud region, or the endpoint host.
	region string
}

// newProvider reports the providers the V wave added: the supported-model
// list, the probe and the fail-safe pricing apply to them only.
func newProvider(p string) bool { return !isClaudeProvider(p) }

// priceQualifiers are the prefixes a provider-qualified price key may carry
// for a role, lowest priority first (design 8.1): "<provider>:<model>", and
// for openai "<flavor>:<model>" over that, so "groq:<model>" wins over
// "openai:<model>", which wins over "<model>".
func priceQualifiers(r config.ModelRole) []string {
	q := []string{r.Provider}
	if r.Provider == llm.ProviderOpenAI && r.Flavor != "" && r.Flavor != r.Provider {
		q = append(q, r.Flavor)
	}
	return q
}

// mainRole is the main role of c: Knobs' when set, else the provider alone
// (the model id then comes from each request's Options.Model).
func (c EndpointConfig) mainRole() config.ModelRole {
	if c.Knobs != nil {
		return c.Knobs.Roles.Main
	}
	r := config.ModelRole{Provider: c.Provider}
	if r.Provider == llm.ProviderOpenAI {
		r.Flavor = openai.FlavorOpenAI
	}
	return r
}

// clientBuilder builds the role clients of one endpoint.
type clientBuilder struct {
	c      EndpointConfig
	getenv func(string) string
	ld     providerLoaders
	// limits hands out one limiter per OpenAI-compatible host, shared by
	// every role on it (design 7.4).
	limits *retry.Limiters
	log    *slog.Logger
	// extra are test options for the main Claude client.
	extra []anthropicllm.RequestOption
}

// build builds the client of one role. main is true for the main role: it
// alone takes the test options in extra and PIPEROOM_MODEL_API_KEY. Every
// error names the variable to set and never a key.
func (b clientBuilder) build(ctx context.Context, env string, r config.ModelRole, main bool) (model, error) {
	c, getenv := b.c, b.getenv
	switch r.Provider {
	case ProviderAnthropic, ProviderBedrock, ProviderVertex:
		rc := c
		rc.Provider = r.Provider
		if !main {
			// Knobs would put the main provider back (normalized).
			rc.Knobs = nil
			var err error
			if rc, _, err = rc.normalized(); err != nil {
				return nil, fmt.Errorf("%w (for %s_PROVIDER=%s)", err, env, r.Provider)
			}
			// The main Anthropic client keeps today's behaviour (the SDK reads
			// the key on the first request); a role moved to Anthropic from
			// another main provider is new configuration, checked at start.
			if r.Provider == ProviderAnthropic && getenv("ANTHROPIC_API_KEY") == "" && getenv("ANTHROPIC_AUTH_TOKEN") == "" {
				return nil, fmt.Errorf("agent: %s_PROVIDER=anthropic needs ANTHROPIC_API_KEY", env)
			}
		}
		opts, err := rc.providerOptions(ctx, getenv, b.ld)
		if err != nil {
			return nil, err
		}
		if main {
			opts = append(opts, b.extra...)
		}
		return anthropicllm.New(r.Provider, opts, b.log), nil
	case llm.ProviderOpenAI:
		return b.openAI(env, r, main)
	case llm.ProviderGemini:
		return b.gemini(ctx, env, r, main)
	case llm.ProviderBedrockConverse:
		if c.Converse == nil {
			return nil, fmt.Errorf("agent: %s: provider bedrock-converse is not built yet (V5, the Bedrock Converse adapter); use bedrock for Claude on Bedrock", providerVar(env, main))
		}
		m, err := c.Converse.NewModel(ctx, r, main)
		if err != nil {
			return nil, fmt.Errorf("agent: %s=bedrock-converse: %w", providerVar(env, main), err)
		}
		return m, nil
	}
	return nil, fmt.Errorf("agent: unknown %s %q; allowed values: %s", providerVar(env, main), r.Provider, allowedProviders)
}

// regionOf is the region or host a role's requests go to (design 10.4).
func regionOf(c EndpointConfig, r config.ModelRole) string {
	switch r.Provider {
	case ProviderBedrock, llm.ProviderBedrockConverse:
		return c.AWSRegion
	case ProviderVertex:
		return c.VertexRegion
	case llm.ProviderGemini:
		if c.knobs().GeminiBackend == config.GeminiBackendVertex {
			return c.VertexRegion
		}
		return strings.TrimPrefix(geminiOrigin(r, "", ""), "https://")
	case llm.ProviderOpenAI:
		o := origin(r)
		if i := strings.Index(o, "://"); i >= 0 {
			o = o[i+3:]
		}
		return o
	}
	return ""
}

// providerVar names the variable that chose a role's provider.
func providerVar(env string, main bool) string {
	if main {
		return "PIPEROOM_MODEL_PROVIDER"
	}
	return env + "_PROVIDER"
}

// mainKeyVars names the variables the main role's key may come from.
const mainKeyVars = "PIPEROOM_MODEL_API_KEY, PIPEROOM_MODEL_API_KEY_FILE or "

func (b clientBuilder) openAI(env string, r config.ModelRole, main bool) (model, error) {
	c := b.c
	flavor := r.Flavor
	if flavor == "" {
		flavor = openai.FlavorOpenAI
	}
	// Refused before the key is read: config.ParseModelKnobs checks the same
	// rule, and this repeats it on the adapter's own answer, so a hand-made
	// EndpointConfig cannot skip it (design 15, decision 7).
	if err := b.checkChina(env, main, openai.ChinaOperatedEndpoint(flavor, r.BaseURL)); err != nil {
		return nil, err
	}
	key, err := openai.LoadKey(flavor, main, b.getenv, b.readFile)
	if err != nil {
		return nil, fmt.Errorf("agent: %w", err)
	}
	// vllm and generic read the optional PIPEROOM_MODEL_API_KEY, ollama none;
	// the hosted flavors cannot start without their key.
	if std := openai.KeyEnv(flavor); key == "" && std != "" && std != config.EnvModelAPIKey {
		where := std
		if main {
			where = mainKeyVars + std
		}
		return nil, fmt.Errorf("agent: %s=openai with flavor %s needs an API key: set %s", providerVar(env, main), flavor, where)
	}
	o := openai.Options{
		Flavor:           flavor,
		BaseURL:          r.BaseURL,
		AllowPrivateHTTP: c.knobs().AllowPrivateHTTP,
		Proxy:            c.knobs().Proxy,
		Key:              key,
		Caps:             c.knobs().Caps,
		Limiter:          b.limits.For(origin(r)),
		Log:              b.log,
	}
	if main {
		o.ContextTokens = c.knobs().ContextTokens
	}
	m, err := openai.New(o)
	if err != nil {
		return nil, fmt.Errorf("agent: %s: %w", providerVar(env, main), err)
	}
	if err := b.checkChina(env, main, m.ChinaOperated()); err != nil {
		return nil, err
	}
	return m, nil
}

// checkChina refuses an endpoint operated in the People's Republic of China
// (host non-empty) without PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes.
func (b clientBuilder) checkChina(env string, main bool, host string) error {
	if host == "" || b.c.knobs().AckCNEndpoint {
		return nil
	}
	return fmt.Errorf("agent: %s=openai: endpoint %s is operated in the People's Republic of China: set PIPEROOM_MODEL_ACK_CN_ENDPOINT=yes to acknowledge where incident data goes",
		providerVar(env, main), host)
}

func (b clientBuilder) gemini(ctx context.Context, env string, r config.ModelRole, main bool) (model, error) {
	c := b.c
	k := c.knobs()
	o := gemini.Options{
		Backend: k.GeminiBackend,
		BaseURL: r.BaseURL,
		Proxy:   k.Proxy,
		Limiter: b.limits.For(geminiOrigin(r, k.GeminiBackend, c.VertexRegion)),
		Log:     b.log,
	}
	if main {
		o.ContextTokens = k.ContextTokens
	}
	if o.Backend == config.GeminiBackendVertex {
		o.Project, o.Region = c.VertexProject, c.VertexRegion
		if o.Project == "" || o.Region == "" {
			return nil, fmt.Errorf("agent: %s=gemini with PIPEROOM_GEMINI_BACKEND=vertex needs VERTEX_PROJECT_ID and VERTEX_REGION", providerVar(env, main))
		}
	} else {
		key, err := loadKey("GEMINI_API_KEY", main, b.getenv, b.readFile)
		if err != nil {
			return nil, err
		}
		if key == "" {
			where := "GEMINI_API_KEY"
			if main {
				where = mainKeyVars + where
			}
			return nil, fmt.Errorf("agent: %s=gemini needs an API key: set %s (or PIPEROOM_GEMINI_BACKEND=vertex)", providerVar(env, main), where)
		}
		o.APIKey = key
	}
	m, err := gemini.New(ctx, o)
	if err != nil {
		return nil, fmt.Errorf("agent: %s: %w", providerVar(env, main), err)
	}
	return m, nil
}

// loadKey is openai.LoadKey's rule for a provider with one standard
// variable: the main role reads PIPEROOM_MODEL_API_KEY, then the file
// PIPEROOM_MODEL_API_KEY_FILE names, then std; another role reads std only.
func loadKey(std string, main bool, getenv func(string) string, readFile func(string) ([]byte, error)) (netsec.Secret, error) {
	if main {
		if k := strings.TrimSpace(getenv(config.EnvModelAPIKey)); k != "" {
			return netsec.Secret(k), nil
		}
		if path := strings.TrimSpace(getenv(config.EnvModelAPIKeyFile)); path != "" {
			b, err := readFile(path)
			if err != nil {
				return "", errors.New("agent: PIPEROOM_MODEL_API_KEY_FILE: cannot read the file")
			}
			if len(b) > 64<<10 {
				return "", errors.New("agent: PIPEROOM_MODEL_API_KEY_FILE: larger than 64 KiB")
			}
			if k := strings.TrimSpace(string(b)); k != "" {
				return netsec.Secret(k), nil
			}
			return "", errors.New("agent: PIPEROOM_MODEL_API_KEY_FILE: the file is empty")
		}
	}
	return netsec.Secret(strings.TrimSpace(getenv(std))), nil
}

// origin is the scheme://host[:port] of an openai role's endpoint: the
// limiter key, shared by every role on the same host.
func origin(r config.ModelRole) string {
	if r.BaseURL != nil {
		return r.BaseURL.Redacted()
	}
	u, err := url.Parse(config.FlavorDefaultBaseURL(r.Flavor))
	if err != nil || u.Host == "" {
		return r.Flavor
	}
	return u.Scheme + "://" + u.Host
}

// geminiOrigin is the limiter key of a gemini role: its base URL's origin,
// else the backend's default host.
func geminiOrigin(r config.ModelRole, backend, region string) string {
	switch {
	case r.BaseURL != nil:
		return r.BaseURL.Redacted()
	case backend == config.GeminiBackendVertex && region == "global":
		return "https://aiplatform.googleapis.com"
	case backend == config.GeminiBackendVertex:
		return "https://" + region + "-aiplatform.googleapis.com"
	}
	return "https://generativelanguage.googleapis.com"
}

// knobs returns c.Knobs or the zero settings.
func (c EndpointConfig) knobs() config.ModelKnobs {
	if c.Knobs != nil {
		return *c.Knobs
	}
	return config.ModelKnobs{}
}

// readFile reads a key file: the loaders' reader, else os.ReadFile.
func (b clientBuilder) readFile(path string) ([]byte, error) {
	if b.ld.readFile != nil {
		return b.ld.readFile(path)
	}
	return os.ReadFile(path)
}

// allowedProviders lists every PIPEROOM_MODEL_PROVIDER value.
var allowedProviders = strings.Join([]string{
	ProviderAnthropic, ProviderBedrock, ProviderVertex,
	llm.ProviderOpenAI, llm.ProviderGemini, llm.ProviderBedrockConverse,
}, ", ")

// sameEndpoint reports whether a role can share the main client: the same
// provider means the same flavor and base URL (config.ParseModelRoles gives a
// role on the main provider the main endpoint).
func sameEndpoint(main, r config.ModelRole) bool { return r.Provider == main.Provider }
