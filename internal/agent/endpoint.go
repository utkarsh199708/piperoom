package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/bedrock"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/vertex"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/oauth2/google"

	"piperoom/internal/events"
)

// Model providers an Endpoint can talk to.
const (
	ProviderAnthropic = "anthropic"
	ProviderBedrock   = "bedrock"
	ProviderVertex    = "vertex"
)

// defaultTriageTurns is how many turns run on the triage model before the
// investigation moves to the main model.
const defaultTriageTurns = 6

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
}

// providerLoaders are the two places that read cloud credentials. Tests
// replace them so building the options never touches the network or the
// machine's real credentials.
type providerLoaders struct {
	awsConfig   func(ctx context.Context, region string) (aws.Config, error)
	googleCreds func(ctx context.Context) (*google.Credentials, error)
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
	default:
		return c, 0, fmt.Errorf("agent: unknown PIPEROOM_MODEL_PROVIDER %q; allowed values: %s, %s, %s",
			c.Provider, ProviderAnthropic, ProviderBedrock, ProviderVertex)
	}

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
// The SDK's own bedrock.WithLoadDefaultConfig and vertex.WithGoogleAuth panic
// when credentials are missing, so this loads them itself and passes the
// result to bedrock.WithConfig and vertex.WithCredentials.
func (c EndpointConfig) providerOptions(ctx context.Context, getenv func(string) string, ld providerLoaders) (opts []option.RequestOption, err error) {
	switch c.Provider {
	case ProviderBedrock:
		var cfg aws.Config
		cfg, err = ld.awsConfig(ctx, c.AWSRegion)
		if err != nil {
			return nil, fmt.Errorf("agent: load AWS config for bedrock: %w", err)
		}
		cfg.Region = c.AWSRegion
		// Credentials are resolved lazily on the first request; bedrock.WithConfig
		// reports a config with none.
		return []option.RequestOption{bedrock.WithConfig(cfg)}, nil
	case ProviderVertex:
		var creds *google.Credentials
		creds, err = ld.googleCreds(ctx)
		if err != nil {
			return nil, fmt.Errorf("agent: find Google application default credentials for vertex: %w", err)
		}
		// vertex.WithCredentials panics on a failure it cannot report.
		defer func() {
			if p := recover(); p != nil {
				opts, err = nil, fmt.Errorf("agent: set up vertex client: %v", p)
			}
		}()
		return []option.RequestOption{vertex.WithCredentials(ctx, c.VertexRegion, c.VertexProject, creds)}, nil
	default:
		return clientOptions(getenv), nil
	}
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

	mu      sync.Mutex
	budgets map[string]*tokenBudget
}

// NewEndpoint checks cfg and builds the model client. It returns an error for
// an unknown provider, a missing region or project, or a bad token budget, so
// the server refuses to start instead of failing on the first alert. It reads
// cloud credentials but calls no model.
func NewEndpoint(ctx context.Context, cfg EndpointConfig, log *slog.Logger) (*Endpoint, error) {
	return newEndpoint(ctx, cfg, os.Getenv, defaultLoaders(), log)
}

func newEndpoint(ctx context.Context, cfg EndpointConfig, getenv func(string) string, ld providerLoaders, log *slog.Logger, extra ...option.RequestOption) (*Endpoint, error) {
	if log == nil {
		log = slog.Default()
	}
	cfg, limit, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	opts, err := cfg.providerOptions(ctx, getenv, ld)
	if err != nil {
		return nil, err
	}
	opts = append(opts, extra...)
	return &Endpoint{
		provider:    cfg.Provider,
		triageModel: cfg.TriageModel,
		triageTurns: cfg.TriageTurns,
		limit:       limit,
		model:       &anthropicModel{client: anthropic.NewClient(opts...), log: log},
		log:         log,
		budgets:     map[string]*tokenBudget{},
	}, nil
}

// Provider reports the normalized provider name.
func (e *Endpoint) Provider() string { return e.provider }

// TokenLimit reports the per-room limit; 0 means no cap.
func (e *Endpoint) TokenLimit() int64 { return e.limit }

// budget returns the room's budget, creating it on first use.
func (e *Endpoint) budget(roomID string) *tokenBudget {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.budgets[roomID]
	if !ok {
		b = newTokenBudget(e.limit)
		e.budgets[roomID] = b
	}
	return b
}

// Run is an agent.Runner: the investigation loop on this endpoint, with the
// room's token budget and, when configured, the triage model.
func (e *Endpoint) Run(ctx context.Context, alert Alert, deps Deps, opts Options) error {
	opts = opts.withDefaults()
	m := &budgetModel{inner: e.model, budget: e.budget(alert.RoomID), loop: true}
	r, err := newRunnerWith(alert, deps, opts, m)
	if err != nil {
		return err
	}
	if e.triageModel != "" && e.triageModel != opts.Model {
		r.triageModel = e.triageModel
		r.triageTurns = e.triageTurns
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
	m := &budgetModel{inner: e.model, budget: e.budget(roomEvents[0].RoomID)}
	return writeHandoff(ctx, roomEvents, opts, m)
}
