package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/oauth2/google"

	"piperoom/internal/agent/llm"
	anthropicllm "piperoom/internal/agent/llm/anthropic"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/config"
	"piperoom/internal/events"
)

// Model providers an Endpoint can talk to: the Claude providers of the llm
// contract, all served by the Claude adapter (internal/agent/llm/anthropic).
const (
	ProviderAnthropic = llm.ProviderAnthropic
	ProviderBedrock   = llm.ProviderBedrock
	ProviderVertex    = llm.ProviderVertex
)

// defaultTriageTurns is how many turns run on the triage model before the
// investigation moves to the main model.
const defaultTriageTurns = 6

// credentialCheckTimeout bounds the startup fetch of AWS credentials.
const credentialCheckTimeout = 5 * time.Second

// bedrockTokenEnv is the Bedrock API key variable the SDK reads itself.
const bedrockTokenEnv = "AWS_BEARER_TOKEN_BEDROCK"

// googleCloudScope is the OAuth scope Vertex AI calls need.
const googleCloudScope = "https://www.googleapis.com/auth/cloud-platform"

// EndpointConfig says where the model runs and how much a room may spend. The
// wiring layer fills it from config.Config. It lives here, not in Options,
// because Options is a contract file shared with the room service.
type EndpointConfig struct {
	// Provider is "anthropic" (default when empty), "bedrock" or "vertex".
	Provider string
	// AWSRegion is required for bedrock.
	AWSRegion string
	// AWSConfig, when set, is the process's one shared AWS config
	// (wiring.LoadAWS) and bedrock uses it instead of loading its own, so
	// Bedrock and MSK IAM read the same credential chain once. Nil keeps the
	// endpoint's own loader.
	AWSConfig *aws.Config
	// VertexProject and VertexRegion are required for vertex.
	VertexProject string
	VertexRegion  string
	// TriageModel, when set, runs the first TriageTurns turns of an
	// investigation. Empty = always Options.Model.
	TriageModel string
	// TriageTurns is how many turns run on the triage model; 0 = 6.
	TriageTurns int
	// RoomTokenBudget is the raw PIPEROOM_ROOM_TOKEN_BUDGET value; see
	// ParseRoomTokenBudget.
	RoomTokenBudget string
	// TriagePass, TriagePassModel and TriagePassTimeout set the first look
	// (Endpoint.Triage), from config.ParseTriagePass. TriagePass false makes
	// every pass emit agent.triage "failed"/"off" without a model call. An
	// empty model or a zero timeout means the default; a timeout over
	// config.MaxTriagePassTimeout is cut to it.
	TriagePass        bool
	TriagePassModel   string
	TriagePassTimeout time.Duration
	// FixPRApplyTimeout is PIPEROOM_FIXPR_APPLY_TIMEOUT
	// (config.ParseFixPRApplyTimeout): how long after a fix PR merged the
	// agent waits for its applied_when before the outcome is not_deployed
	// (Q7f). Zero means the default, 2 h.
	FixPRApplyTimeout time.Duration
	// NeutraliseControlTokens is the raw
	// PIPEROOM_MODEL_NEUTRALISE_CONTROL_TOKENS value: all (default when
	// empty) neutralises chat-template control tokens in the text of every
	// request; non-claude skips the Claude providers (controltokens.go).
	NeutraliseControlTokens string

	// Knobs are the typed model settings of design section 9
	// (config.ParseModelKnobs): the roles, their providers and models, and the
	// knobs of the new providers. Nil keeps the behaviour before V6: every
	// role on Provider's one client, with the model of each request's
	// Options. When set, Provider is Knobs.Roles.Main.Provider.
	Knobs *config.ModelKnobs
	// MaxConcurrency bounds requests in flight per new-provider endpoint
	// (PIPEROOM_MODEL_MAX_CONCURRENCY, default PIPEROOM_MAX_AGENTS); 0 = no
	// bound. The Claude providers keep their SDK's own behaviour.
	MaxConcurrency int64

	// Getenv replaces os.Getenv for key variables (the standalone picker's
	// key); nil = os.Getenv. Every key the endpoint reads goes through it:
	// ANTHROPIC_API_KEY and ANTHROPIC_AUTH_TOKEN (the anthropic client then
	// skips the SDK's own environment autoload, so a process key never mixes
	// in, and a main anthropic role without either refuses to start),
	// ANTHROPIC_WORKSPACE_ID, PIPEROOM_MODEL_API_KEY(_FILE), each openai
	// flavor's and gemini's key variable, and AWS_BEARER_TOKEN_BEDROCK for
	// the start checks. The AWS SDK (bedrock, bedrock-converse) still reads
	// its own credentials and AWS_BEARER_TOKEN_BEDROCK from the process
	// environment, and Vertex uses Google application default credentials.
	Getenv func(string) string
	// WrapModel wraps each role's client after it is built (the main client
	// once, every other role's own client once); the evals model tap uses
	// it; production leaves it nil; it must return a model that behaves
	// exactly like m. A role that shares the main client is not wrapped
	// again. role is "main", "first_look", "summary" or "judge".
	WrapModel func(role string, m llm.Model) llm.Model
}

// providerLoaders are the two places that read cloud credentials. Tests
// replace them so building the options never touches the network or the
// machine's real credentials.
type providerLoaders struct {
	awsConfig   func(ctx context.Context, region string) (aws.Config, error)
	googleCreds func(ctx context.Context) (*google.Credentials, error)
	// readFile reads PIPEROOM_MODEL_API_KEY_FILE; nil = os.ReadFile.
	readFile func(path string) ([]byte, error)
	// converseEndpoint replaces the regional Bedrock endpoint of
	// bedrock-converse roles: a test hook for an httptest server, never set
	// from a PIPEROOM_* variable (it decides where the AWS credentials or
	// the Bedrock API key are sent; converse.Config.BaseEndpoint).
	converseEndpoint string
}

// ProviderLoaders replace the cloud-credential lookups ProviderOptions makes.
// A nil field means the real lookup (the AWS default config chain, Google
// application default credentials), so production callers pass the zero value
// and tests set both to keep the machine's credentials out.
type ProviderLoaders struct {
	AWSConfig   func(ctx context.Context, region string) (aws.Config, error)
	GoogleCreds func(ctx context.Context) (*google.Credentials, error)
}

func (l ProviderLoaders) loaders() providerLoaders {
	ld := defaultLoaders()
	if l.AWSConfig != nil {
		ld.awsConfig = l.AWSConfig
	}
	if l.GoogleCreds != nil {
		ld.googleCreds = l.GoogleCreds
	}
	return ld
}

// ProviderOptions checks cfg as NewEndpoint does and returns the normalized
// provider name and the SDK request options that reach that provider
// (anthropicllm.RequestOption is the SDK's option.RequestOption), for a
// caller that needs its own client on the agent's provider (the eval judge).
// It runs the same credential checks as NewEndpoint but sends no request.
// getenv nil means cfg.Getenv, else os.Getenv.
//
// For vertex, ctx must outlive the returned options: vertex.WithCredentials
// and the oauth2 token source it wraps keep ctx and use it for every later
// token refresh, so a cancelled ctx breaks the client's calls.
func ProviderOptions(ctx context.Context, cfg EndpointConfig, getenv func(string) string, ld ProviderLoaders) (string, []anthropicllm.RequestOption, error) {
	if getenv == nil {
		getenv = cfg.getenv()
	}
	cfg, _, err := cfg.normalized()
	if err != nil {
		return cfg.Provider, nil, err
	}
	opts, err := cfg.providerOptions(ctx, getenv, ld.loaders())
	if err != nil {
		return cfg.Provider, nil, err
	}
	return cfg.Provider, opts, nil
}

func defaultLoaders() providerLoaders {
	return providerLoaders{
		awsConfig: func(ctx context.Context, region string) (aws.Config, error) {
			return awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		},
		googleCreds: func(ctx context.Context) (*google.Credentials, error) {
			return google.FindDefaultCredentials(ctx, googleCloudScope)
		},
	}
}

// normalized checks cfg and returns it with defaults filled and the token
// budget parsed. Every error here is a startup error.
func (c EndpointConfig) normalized() (EndpointConfig, int64, error) {
	if c.Knobs != nil {
		c.Provider = c.Knobs.Roles.Main.Provider
	}
	c.Provider = strings.ToLower(strings.TrimSpace(c.Provider))
	if c.Provider == "" {
		c.Provider = ProviderAnthropic
	}
	c.AWSRegion = strings.TrimSpace(c.AWSRegion)
	c.VertexProject = strings.TrimSpace(c.VertexProject)
	c.VertexRegion = strings.TrimSpace(c.VertexRegion)
	c.TriageModel = strings.TrimSpace(c.TriageModel)
	if c.TriageTurns <= 0 {
		c.TriageTurns = defaultTriageTurns
	}

	switch c.Provider {
	case ProviderAnthropic:
	case ProviderBedrock:
		if c.AWSRegion == "" {
			return c, 0, fmt.Errorf("agent: PIPEROOM_MODEL_PROVIDER=bedrock needs AWS_REGION")
		}
	case ProviderVertex:
		if c.VertexProject == "" {
			return c, 0, fmt.Errorf("agent: PIPEROOM_MODEL_PROVIDER=vertex needs VERTEX_PROJECT_ID")
		}
		if c.VertexRegion == "" {
			return c, 0, fmt.Errorf("agent: PIPEROOM_MODEL_PROVIDER=vertex needs VERTEX_REGION")
		}
	case llm.ProviderOpenAI, llm.ProviderGemini, llm.ProviderBedrockConverse:
		// Checked when the client is built (roles.go): flavor, base URL, key.
	default:
		return c, 0, fmt.Errorf("agent: unknown PIPEROOM_MODEL_PROVIDER %q; allowed values: %s",
			c.Provider, allowedProviders)
	}

	mode, err := parseNeutraliseMode(c.NeutraliseControlTokens)
	if err != nil {
		return c, 0, err
	}
	c.NeutraliseControlTokens = mode

	limit, err := ParseRoomTokenBudget(c.RoomTokenBudget)
	if err != nil {
		return c, 0, err
	}
	return c, limit, nil
}

// providerOptions returns the SDK request options for the provider. It reads
// credentials through ld but sends nothing. The Anthropic workspace header is
// added for "anthropic" only: the cloud providers sign their own requests and
// ignore first-party credentials.
//
// The SDK's bedrock.WithLoadDefaultConfig panics when the AWS config cannot be
// loaded, and vertex.WithGoogleAuth panics when no Google default credentials
// are found. So this loads both itself, returns those failures as errors, and
// passes the result to the adapter's BedrockOption and VertexOption
// (bedrock.WithConfig and vertex.WithCredentials).
//
// For Bedrock it also fetches the credentials once (bounded by
// credentialCheckTimeout) so that missing or broken AWS credentials stop the
// server at startup instead of failing the first alert. That is a credential
// lookup (environment, shared files, instance/role metadata), not a model call.
func (c EndpointConfig) providerOptions(ctx context.Context, getenv func(string) string, ld providerLoaders) ([]anthropicllm.RequestOption, error) {
	switch c.Provider {
	case ProviderBedrock:
		var cfg aws.Config
		var err error
		if c.AWSConfig != nil {
			// A shallow copy: the region set below stays local, while the
			// credentials provider (and its cache) is the shared one.
			cfg = c.AWSConfig.Copy()
		} else {
			cfg, err = ld.awsConfig(ctx, c.AWSRegion)
			if err != nil {
				return nil, fmt.Errorf("agent: load AWS config for bedrock: %w", err)
			}
		}
		cfg.Region = c.AWSRegion
		// A Bedrock API key (AWS_BEARER_TOKEN_BEDROCK) replaces AWS credentials.
		if getenv(bedrockTokenEnv) == "" {
			if cfg.Credentials == nil {
				return nil, errors.New("agent: bedrock needs AWS credentials (environment, shared config, or an instance/task role); none were found")
			}
			cctx, cancel := context.WithTimeout(ctx, credentialCheckTimeout)
			_, err = cfg.Credentials.Retrieve(cctx)
			cancel()
			if err != nil {
				return nil, fmt.Errorf("agent: retrieve AWS credentials for bedrock: %w", err)
			}
		}
		return []anthropicllm.RequestOption{anthropicllm.BedrockOption(cfg)}, nil
	case ProviderVertex:
		creds, err := ld.googleCreds(ctx)
		if err != nil {
			return nil, fmt.Errorf("agent: find Google application default credentials for vertex: %w", err)
		}
		if creds == nil {
			return nil, errors.New("agent: no Google application default credentials for vertex")
		}
		opt, err := anthropicllm.VertexOption(ctx, c.VertexRegion, c.VertexProject, creds)
		if err != nil {
			return nil, fmt.Errorf("agent: %w", err)
		}
		return []anthropicllm.RequestOption{opt}, nil
	default:
		opts := clientOptions(getenv)
		if c.Getenv != nil {
			// The override replaces the process environment for the key:
			// the SDK's autoload would otherwise send a process key.
			env, ok := anthropicllm.EnvOptions(getenv)
			if !ok {
				return nil, errors.New("agent: PIPEROOM_MODEL_PROVIDER=anthropic needs ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN from the key lookup override")
			}
			opts = append(env, opts...)
		}
		return opts, nil
	}
}

// getenv is c.Getenv, else os.Getenv.
func (c EndpointConfig) getenv() func(string) string {
	if c.Getenv != nil {
		return c.Getenv
	}
	return os.Getenv
}

// wrap applies c.WrapModel to a role's client, when set.
func (c EndpointConfig) wrap(role string, m llm.Model) llm.Model {
	if c.WrapModel == nil {
		return m
	}
	return c.WrapModel(role, m)
}

// Endpoint is the model client plus the per-room state that must outlive one
// agent run: the token budgets. The room service restarts a failed agent with
// a fresh Run call, so the budget cannot live on the runner of one run.
type Endpoint struct {
	provider    string
	triageModel string
	triageTurns int
	limit       int64
	model       model
	log         *slog.Logger
	// mainRole and mainRegion describe the main role for Roles when no
	// Knobs are set (specs is nil then).
	mainRole   config.ModelRole
	mainRegion string

	// mainQuals are the price-key qualifiers of the main role (design 8.1).
	mainQuals []string
	// firstLook, summary and judge are the single-shot roles' clients; a
	// nil client is the main one (roles.go).
	firstLook, summary, judge roleClient
	// specs are the configured roles (nil without EndpointConfig.Knobs):
	// what the start checks, the pricing rule and the startup log read.
	specs []roleSpec
	// unverified is PIPEROOM_MODEL_UNVERIFIED=on; pricingStrict is
	// PIPEROOM_MODEL_PRICING_STRICT (config.PricingStrict*).
	unverified    bool
	pricingStrict string
	// gates are the supported-list results of the specs, by role name, set
	// by CheckStart.
	gates map[string]llm.Gate

	// triageSw is shared by every run: the first rejected triage request turns
	// triage off for all rooms (see modelTurn).
	triageSw triageSwitch

	// triagePass is the first-look setup (Triage, triage_pass.go);
	// firstLookWarned turns true once a failed first-look request is logged.
	triagePass      triagePassConfig
	firstLookWarned atomic.Bool

	// applyTimeout is EndpointConfig.FixPRApplyTimeout; zero keeps the
	// runner's default (Q7f).
	applyTimeout time.Duration

	mu      sync.Mutex
	budgets map[string]*tokenBudget
	// spend is the durable spend setup (SetSpend, spend.go); nil = in-memory
	// budgets only.
	spend *spendSettings
}

// maxModelConcurrency caps the requests in flight per new-provider endpoint;
// it is far above any provider's concurrent-request limit.
const maxModelConcurrency = 4096

// modelConcurrency converts the configured bound for the limiter. It clamps
// instead of refusing: the value can come from PIPEROOM_MAX_AGENTS, which a
// Claude-only deployment sets freely, and it sizes a semaphore channel, so a
// huge value must not allocate one (or overflow int on a 32-bit build).
func modelConcurrency(n int64) int {
	if n <= 0 {
		return 0
	}
	if n > maxModelConcurrency {
		return maxModelConcurrency
	}
	return int(n)
}

// NewEndpoint checks cfg and builds the model client. It returns an error for
// an unknown provider, a missing region or project, or a bad token budget, so
// the server refuses to start instead of failing on the first alert. It reads
// cloud credentials but calls no model.
func NewEndpoint(ctx context.Context, cfg EndpointConfig, log *slog.Logger) (*Endpoint, error) {
	return newEndpoint(ctx, cfg, cfg.getenv(), defaultLoaders(), log)
}

func newEndpoint(ctx context.Context, cfg EndpointConfig, getenv func(string) string, ld providerLoaders, log *slog.Logger, extra ...anthropicllm.RequestOption) (*Endpoint, error) {
	if log == nil {
		log = slog.Default()
	}
	cfg, limit, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	b := clientBuilder{c: cfg, getenv: getenv, ld: ld, log: log, extra: extra,
		limits: retry.NewLimiters(retry.Limits{MaxConcurrency: modelConcurrency(cfg.MaxConcurrency), TPM: cfg.knobs().TPM}, log)}
	mainRole := cfg.mainRole()
	mainRole.Provider = cfg.Provider
	m, err := b.build(ctx, "PIPEROOM_MODEL", mainRole, true)
	if err != nil {
		return nil, err
	}
	e := &Endpoint{
		provider:      cfg.Provider,
		triageModel:   cfg.TriageModel,
		triageTurns:   cfg.TriageTurns,
		triagePass:    triagePassSettings(cfg),
		applyTimeout:  cfg.FixPRApplyTimeout,
		limit:         limit,
		model:         cfg.wrap(roleMain, withNeutraliser(m, cfg.NeutraliseControlTokens)),
		log:           log,
		mainRole:      mainRole,
		mainRegion:    regionOf(cfg, mainRole),
		budgets:       map[string]*tokenBudget{},
		mainQuals:     priceQualifiers(mainRole),
		pricingStrict: config.PricingStrictNonClaude,
	}
	if cfg.Knobs != nil {
		if err := e.buildRoles(ctx, cfg, b); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// buildRoles builds the single-shot roles' clients and the role list from
// cfg.Knobs. A role on the main provider shares the main client; a model id
// equal to the main one is left to Options.Model, as before V6.
func (e *Endpoint) buildRoles(ctx context.Context, cfg EndpointConfig, b clientBuilder) error {
	k := cfg.Knobs
	e.unverified, e.pricingStrict = k.Unverified, k.PricingStrict
	if e.pricingStrict == "" {
		e.pricingStrict = config.PricingStrictNonClaude
	}
	main := k.Roles.Main
	e.specs = append(e.specs, roleSpec{name: roleMain, env: "PIPEROOM_MODEL", role: main, m: e.model, loop: true, region: regionOf(cfg, main)})
	if k.Roles.Triage != "" {
		tr := main
		tr.Model = k.Roles.Triage
		e.specs = append(e.specs, roleSpec{name: roleTriage, env: "PIPEROOM_TRIAGE_MODEL", role: tr, m: e.model, loop: true, region: regionOf(cfg, tr)})
	}
	for _, r := range []struct {
		name, env string
		role      config.ModelRole
		dst       *roleClient
		on, loop  bool
	}{
		// A first look that is off makes no model call, so its endpoint is
		// neither built nor checked.
		{roleFirstLook, "PIPEROOM_TRIAGE_PASS", k.Roles.FirstLook, &e.firstLook, e.triagePass.on, true},
		{roleSummary, "PIPEROOM_SUMMARY", k.Roles.Summary, &e.summary, true, false},
		{roleJudge, "PIPEROOM_JUDGE", k.Roles.Judge, &e.judge, true, false},
	} {
		if !r.on {
			continue
		}
		rc := roleClient{quals: priceQualifiers(r.role)}
		if r.name == roleFirstLook {
			// The first look's model id is triagePass.model (the same default
			// as ParseModelRoles); its spec names what will be sent.
			r.role.Model = e.triagePass.model
		} else if r.role.Model != main.Model {
			rc.modelID = r.role.Model
		}
		m := e.model
		if !sameEndpoint(main, r.role) {
			built, err := b.build(ctx, r.env, r.role, false)
			if err != nil {
				return err
			}
			m = cfg.wrap(r.name, withNeutraliser(built, cfg.NeutraliseControlTokens))
			rc.m = m
		}
		*r.dst = rc
		e.specs = append(e.specs, roleSpec{name: r.name, env: r.env, role: r.role, m: m, loop: r.loop, region: regionOf(cfg, r.role)})
	}
	return nil
}

// firstLookModel is the first look's client.
func (e *Endpoint) firstLookModel() model { return e.firstLook.client(e.model) }

// summaryClient is the hand-off and post-mortem writer's client.
func (e *Endpoint) summaryClient() model { return e.summary.client(e.model) }

// judgeClient is the hand-off judge's client.
func (e *Endpoint) judgeClient() model { return e.judge.client(e.model) }

// client is rc's client, or main when the role shares the main one.
func (rc roleClient) client(main model) model {
	if rc.m != nil {
		return rc.m
	}
	return main
}

// withModel returns opts with the role's model id when it names one.
func (rc roleClient) withModel(opts Options) Options {
	if rc.modelID != "" {
		opts.Model = rc.modelID
	}
	return opts
}

// RoleInfo is one configured role, for GET /api/models.
type RoleInfo struct {
	Name, Provider, Flavor, Model, Region string
	PromptCache                           bool
}

// Roles returns the configured roles in a stable order.
//
// The order is main, triage, first_look, summary, judge, each present when
// configured (a first look that is off is not listed). Without
// EndpointConfig.Knobs only the main role is known: its provider, flavor and
// region, with an empty Model (each request's Options.Model names it).
func (e *Endpoint) Roles() []RoleInfo {
	if len(e.specs) == 0 {
		return []RoleInfo{{
			Name: roleMain, Provider: e.provider, Flavor: e.mainRole.Flavor, Region: e.mainRegion,
			PromptCache: e.model.Capabilities(e.mainRole.Model).PromptCache,
		}}
	}
	out := make([]RoleInfo, 0, len(e.specs))
	for _, sp := range e.specs {
		out = append(out, RoleInfo{
			Name: sp.name, Provider: sp.role.Provider, Flavor: sp.role.Flavor, Model: sp.role.Model,
			Region: sp.region, PromptCache: sp.m.Capabilities(sp.role.Model).PromptCache,
		})
	}
	return out
}

// Provider reports the normalized provider name.
func (e *Endpoint) Provider() string { return e.provider }

// FirstLook reports the triage pass setup (Triage): whether it is on, its
// model and its deadline. The room service waits for a pass at most the
// deadline plus a margin.
func (e *Endpoint) FirstLook() (on bool, model string, timeout time.Duration) {
	return e.triagePass.on, e.triagePass.model, e.triagePass.timeout
}

// TokenLimit reports the per-room limit; 0 means no cap.
func (e *Endpoint) TokenLimit() int64 { return e.limit }

// budget returns the room's budget, creating it on first use.
func (e *Endpoint) budget(roomID string) *tokenBudget {
	return e.budgetWithLimit(roomID, 0)
}

// budgetWithLimit returns the room's budget like budget, and when limit > 0
// makes it the room's cap, whether the entry is new or not. An existing entry
// keeps its count: a hand-off written while the run was queued may have created
// it with e.limit, and that must not pin the room to the default.
func (e *Endpoint) budgetWithLimit(roomID string, limit int64) *tokenBudget {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.budgets[roomID]
	if !ok {
		b = newTokenBudget(e.limit)
		e.budgets[roomID] = b
	}
	if limit > 0 {
		b.setLimit(limit)
	}
	return b
}

// Forget drops a room's token count. Call it when a room closes so the map does
// not grow for the life of the process. A run that is still going keeps its own
// pointer and finishes its count; a later run for the same room id starts at 0
// in memory (with a spend store, the room's persisted total still counts).
func (e *Endpoint) Forget(roomID string) {
	e.mu.Lock()
	delete(e.budgets, roomID)
	e.mu.Unlock()
}

// Run is an agent.Runner: the investigation loop on this endpoint, with the
// room's token budget and, when configured, the triage model.
//
// Options.TokenBudget > 0 becomes the room's cap (see budgetWithLimit).
// Deps.Usage, when set, is called exactly once with the room's count when Run
// ends, from a defer so a panic still reports the tokens (and still
// propagates). When the budget stops the loop, Run returns an error wrapping
// *BudgetError.
func (e *Endpoint) Run(ctx context.Context, alert Alert, deps Deps, opts Options) error {
	opts = opts.withDefaults()
	limit := opts.TokenBudget
	if alert.Mode == ModeSetup {
		// A setup room's cap is its own token budget (Options.Setup, the
		// server fills it from PIPEROOM_SETUP_TOKEN_BUDGET; nil bounds mean
		// the design's default), not the room budget.
		sl, err := setupTokenBudget(opts.Setup)
		if err != nil {
			return err
		}
		limit = sl
	}
	b := e.budgetWithLimit(alert.RoomID, limit)
	if usage := deps.Usage; usage != nil {
		// b is this run's own pointer, so a Forget(roomID) from the room-closed
		// hook cannot change what is reported.
		defer func() { usage(b.Used()) }()
	}
	m := &budgetModel{inner: e.model, budget: b, loop: true, guard: e.guardWith(alert.RoomID, alert.PipelineID, e.mainQuals)}
	r, err := newRunnerWith(alert, deps, opts, m)
	if err != nil {
		return err
	}
	r.prices = e.pricesFor(e.mainQuals)
	if e.applyTimeout > 0 {
		r.timing.applyTimeout = e.applyTimeout
	}
	if e.triageModel != "" && e.triageModel != opts.Model {
		r.triageModel = e.triageModel
		r.triageTurns = e.triageTurns
		r.triageSw = &e.triageSw
	}
	return r.run(ctx)
}

// WriteHandoff is an agent.HandoffWriter. Its tokens count against the room's
// budget, but an exhausted budget does not block it (see budgetModel.loop).
func (e *Endpoint) WriteHandoff(ctx context.Context, roomEvents []events.Event, opts Options) (events.HandoffSummary, error) {
	opts = opts.withDefaults()
	if len(roomEvents) == 0 {
		return events.HandoffSummary{}, fmt.Errorf("agent: cannot write handoff from an empty room")
	}
	return writeHandoff(ctx, roomEvents, e.summary.withModel(opts), e.handoffModel(roomEvents))
}

// WritePostmortem is an agent.PostmortemWriter: the closing summary of a closed
// room, grounded like a hand-off. Its tokens count against the room's budget
// (never refused) and are reported with role events.UsageRolePostmortem.
func (e *Endpoint) WritePostmortem(ctx context.Context, roomEvents []events.Event, opts Options) (PostmortemDraft, error) {
	opts = opts.withDefaults()
	if len(roomEvents) == 0 {
		return PostmortemDraft{}, fmt.Errorf("agent: cannot write postmortem from an empty room")
	}
	return writePostmortem(ctx, roomEvents, e.summary.withModel(opts), e.summaryModel(roomEvents, events.UsageRolePostmortem))
}
