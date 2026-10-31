package setupcmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"piperoom/internal/clients"
	"piperoom/internal/clients/aiven"
	"piperoom/internal/clients/confluentcloud"
	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/events"
	"piperoom/internal/setup/runner"
	"piperoom/internal/setup/targets/managed"
)

// The managed-cloud targets (rows U13b2, U13c, U13d; docs/design/
// setup-managed.md §2): a Welcome target of kind aws registers the MSK
// Connect ops, confluent the Confluent Cloud ops, aiven the Aiven ops. Where
// the credentials come from is the design's one rule (S6): on the runner,
// never through the room.
//
//   - AWS: the SDK's default chain of this machine (environment, shared
//     config and SSO profiles, web identity, container or instance role),
//     loaded once at registration in the target's region with one bounded
//     credential fetch, so a broken chain stops pairing rather than a step.
//     Piperoom stores no AWS key.
//   - Confluent and Aiven: the setup keys and token live in the runner's own
//     secret store under fixed names, typed on the runner's terminal through
//     the secret_set op. They are read at each API call, not at
//     registration, because the person types them after pairing; a missing
//     one fails that call with the name to enter.
//
// The secret frame from the server refuses these names (secretops.go), so a
// server cannot point the runner at another cloud account's key.

// The runner-side secret names of the managed setup credentials (design
// §2.2, §2.3).
const (
	secretCCloudAPIKey        = "ccloud_api_key"
	secretCCloudAPISecret     = "ccloud_api_secret"
	secretCCloudClusterKey    = "ccloud_cluster_key"
	secretCCloudClusterSecret = "ccloud_cluster_secret"
	secretAivenToken          = "aiven_token"
)

// awsCredentialTimeout bounds the one credential fetch at registration
// (design §2.1: 5 s).
const awsCredentialTimeout = 5 * time.Second

// awsRegion is the region rule of the aws target's address (the managed
// driver's own rule); it is checked before the region reaches the SDK.
var awsRegion = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]$`)

// Errors callers branch on with errors.Is.
var (
	// errNoCloudCredential: the runner's store holds no value under a
	// managed setup credential's name. The error names it, never a value.
	errNoCloudCredential = errors.New("setup: no cloud setup credential in the runner's secret store")
	// errNoSecretStore: the registrar was built without the runner's store.
	errNoSecretStore = errors.New("setup: the runner's secret store is not available")
	// errManagedTargets: the room names two targets of one managed kind; a
	// runner drives one account per provider.
	errManagedTargets = errors.New("setup: the room names more than one managed target of one kind")
)

// storeOpener opens the runner's secret store (runner.Runner.SecretStore).
type storeOpener func() (*runner.Store, error)

// get reads one named value from the store.
func (o storeOpener) get(name string) (string, error) {
	if o == nil {
		return "", errNoSecretStore
	}
	st, err := o()
	if err != nil {
		return "", fmt.Errorf("setup: opening the secret store: %w", err)
	}
	v, ok := st.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s; enter it on the runner's terminal (secret_set)", errNoCloudCredential, name)
	}
	return v, nil
}

// lookup is the Strimzi driver's Secret func over the store: false when the
// store cannot be opened, holds no such name, or the name is a cloud setup
// credential (runner.Store.LookupRef), which never becomes a Kubernetes
// Secret.
func (o storeOpener) lookup(name string) (string, bool) {
	if o == nil {
		return "", false
	}
	st, err := o()
	if err != nil {
		return "", false
	}
	v, err := st.LookupRef(name)
	return v, err == nil
}

// storeMaterialiser is the managed drivers' Secrets: the runner's store,
// opened only when a connector config holds a ${secret:} reference.
type storeMaterialiser struct{ open storeOpener }

// Materialise resolves the ${secret:} references of text from the store.
func (m storeMaterialiser) Materialise(text string) (string, error) {
	if m.open == nil {
		return "", errNoSecretStore
	}
	st, err := m.open()
	if err != nil {
		return "", fmt.Errorf("setup: opening the secret store: %w", err)
	}
	return st.Materialise(text)
}

// managedClients builds the managed drivers' clients. Every field has a
// default (the real clients); tests replace them.
type managedClients struct {
	// msk builds the MSK Connect client of region from this machine's AWS
	// default chain.
	msk func(ctx context.Context, region string) (managed.MSKAPI, error)
	// confluent builds the Connect v1 and ACL clients of one environment
	// and cluster over the store's setup keys.
	confluent func(env, cluster string, open storeOpener) (managed.ConfluentAPI, managed.ACLAPI)
	// aiven builds the client of one project's Connect service over the
	// store's token.
	aiven func(project, service string, open storeOpener) managed.AivenAPI
}

// defaultManagedClients are the real clients.
func defaultManagedClients() managedClients {
	return managedClients{
		msk: loadMSK,
		confluent: func(env, cluster string, open storeOpener) (managed.ConfluentAPI, managed.ACLAPI) {
			c := &storeConfluent{env: env, cluster: cluster, open: open}
			return c, &storeACL{connect: c, open: open}
		},
		aiven: func(project, service string, open storeOpener) managed.AivenAPI {
			return &storeAiven{project: project, service: service, open: open}
		},
	}
}

// loadMSK loads the default AWS chain in region, fetches the credentials
// once (bounded), and builds the MSK Connect client. The SDK's errors name
// the providers it tried, never key material.
func loadMSK(ctx context.Context, region string) (managed.MSKAPI, error) {
	ctx, cancel := context.WithTimeout(ctx, awsCredentialTimeout)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("setup: loading the AWS config: %w", err)
	}
	if cfg.Credentials == nil {
		return nil, errors.New("setup: the aws target needs AWS credentials on the runner (aws sso login, a profile, or an instance role); none were found")
	}
	if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("setup: retrieving AWS credentials for the aws target: %w", err)
	}
	c, err := mskconnect.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("setup: building the MSK Connect client: %w", err)
	}
	return c, nil
}

// registerManaged registers the managed driver of every managed kind the
// targets name (aws, confluent, aiven), and reports whether it registered
// any. A kind named twice with different addresses is refused: the driver's
// op names are fixed, so one runner drives one account per provider.
func (t *targetRegistrar) registerManaged(ctx context.Context, roomID string, targets []events.SetupTarget) (bool, error) {
	byKind := map[string]events.SetupTarget{}
	for _, tg := range targets {
		switch tg.Kind {
		case managed.KindAWS, managed.KindConfluent, managed.KindAiven:
		default:
			continue
		}
		if prev, ok := byKind[tg.Kind]; ok {
			if prev.Address != tg.Address {
				return false, fmt.Errorf("%w: %s", errManagedTargets, tg.Kind)
			}
			if classRank(tg.Class) > classRank(prev.Class) {
				byKind[tg.Kind] = tg
			}
			continue
		}
		byKind[tg.Kind] = tg
	}
	if len(byKind) == 0 {
		return false, nil
	}
	secrets := storeMaterialiser{open: t.secrets}
	// Registration order is fixed so a failure is reproducible.
	if tg, ok := byKind[managed.KindAWS]; ok {
		region := ""
		if parts := strings.SplitN(tg.Address, "/", 3); len(parts) >= 2 {
			region = parts[1]
		}
		if !awsRegion.MatchString(region) {
			return false, fmt.Errorf("setup: the aws target address is not <account id>/<region>[/<cluster arn>]")
		}
		client, err := t.clients.msk(ctx, region)
		if err != nil {
			return false, err
		}
		if _, err := managed.RegisterMSK(t.cat, managed.MSKOptions{
			Client: client, Target: tg, ProdPatterns: t.prodPatterns, RoomID: roomID, Secrets: secrets,
		}); err != nil {
			return false, fmt.Errorf("setup: registering the MSK Connect target: %w", err)
		}
	}
	if tg, ok := byKind[managed.KindConfluent]; ok {
		env, cluster, _ := strings.Cut(tg.Address, "/")
		api, acl := t.clients.confluent(env, cluster, t.secrets)
		if _, err := managed.RegisterConfluent(t.cat, managed.ConfluentOptions{
			Client: api, ACL: acl, Target: tg, ProdPatterns: t.prodPatterns, Secrets: secrets,
		}); err != nil {
			return false, fmt.Errorf("setup: registering the Confluent Cloud target: %w", err)
		}
	}
	if tg, ok := byKind[managed.KindAiven]; ok {
		project, service, _ := strings.Cut(tg.Address, "/")
		if _, err := managed.RegisterAiven(t.cat, managed.AivenOptions{
			Client: t.clients.aiven(project, service, t.secrets), Target: tg, ProdPatterns: t.prodPatterns, Secrets: secrets,
		}); err != nil {
			return false, fmt.Errorf("setup: registering the Aiven target: %w", err)
		}
	}
	return true, nil
}

// storeConfluent is the Connect v1 client over the store's Cloud API key:
// it reads ccloud_api_key and ccloud_api_secret at each call and rebuilds
// the client only when they changed (a rotated key takes effect at once).
type storeConfluent struct {
	env, cluster string
	open         storeOpener
	// baseURL and transport replace the API and the network in tests.
	baseURL   string
	transport http.RoundTripper

	mu          sync.Mutex
	key, secret string
	c           *confluentcloud.Client
}

func (s *storeConfluent) client() (*confluentcloud.Client, error) {
	key, err := s.open.get(secretCCloudAPIKey)
	if err != nil {
		return nil, err
	}
	secret, err := s.open.get(secretCCloudAPISecret)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c != nil && s.key == key && s.secret == secret {
		return s.c, nil
	}
	c, err := confluentcloud.New(confluentcloud.Config{
		BaseURL: s.baseURL, APIKey: key, APISecret: netsec.Secret(secret),
		Environment: s.env, Cluster: s.cluster, Transport: s.transport,
	})
	if err != nil {
		return nil, fmt.Errorf("setup: building the Confluent Cloud client: %w", err)
	}
	s.key, s.secret, s.c = key, secret, c
	return c, nil
}

func (s *storeConfluent) Plugins(ctx context.Context) ([]confluentcloud.Plugin, error) {
	c, err := s.client()
	if err != nil {
		return nil, err
	}
	return c.Plugins(ctx)
}

func (s *storeConfluent) Validate(ctx context.Context, plugin string, cfg map[string]string) (confluentcloud.Validation, error) {
	c, err := s.client()
	if err != nil {
		return confluentcloud.Validation{}, err
	}
	return c.Validate(ctx, plugin, cfg)
}

func (s *storeConfluent) CreateConnector(ctx context.Context, name string, cfg map[string]string) (confluentcloud.Created, error) {
	c, err := s.client()
	if err != nil {
		return confluentcloud.Created{}, err
	}
	return c.CreateConnector(ctx, name, cfg)
}

func (s *storeConfluent) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	c, err := s.client()
	if err != nil {
		return clients.ConnectorStatus{}, err
	}
	return c.Status(ctx, name)
}

func (s *storeConfluent) Cluster(ctx context.Context) (confluentcloud.Cluster, error) {
	c, err := s.client()
	if err != nil {
		return confluentcloud.Cluster{}, err
	}
	return c.Cluster(ctx)
}

// storeACL is the Kafka REST v3 ACL client over the store's cluster API key.
// The cluster's REST endpoint comes from the CMK v2 read of the cluster
// (Cluster.HTTPEndpoint) the first time it is needed; NewACLClient accepts
// only an https endpoint under confluent.cloud.
type storeACL struct {
	connect   *storeConfluent
	open      storeOpener
	transport http.RoundTripper

	mu          sync.Mutex
	endpoint    string
	key, secret string
	c           *confluentcloud.ACLClient
}

func (s *storeACL) CreateACL(ctx context.Context, acl confluentcloud.ACL) error {
	key, kerr := s.open.get(secretCCloudClusterKey)
	secret, serr := s.open.get(secretCCloudClusterSecret)
	if kerr != nil || serr != nil {
		if errors.Is(kerr, errNoCloudCredential) || errors.Is(serr, errNoCloudCredential) {
			return managed.ErrNoACLKey
		}
		return errors.Join(kerr, serr)
	}
	c, err := s.client(ctx, key, secret)
	if err != nil {
		return err
	}
	return c.CreateACL(ctx, acl)
}

func (s *storeACL) client(ctx context.Context, key, secret string) (*confluentcloud.ACLClient, error) {
	s.mu.Lock()
	endpoint := s.endpoint
	if s.c != nil && s.key == key && s.secret == secret {
		c := s.c
		s.mu.Unlock()
		return c, nil
	}
	s.mu.Unlock()
	if endpoint == "" {
		cl, err := s.connect.Cluster(ctx)
		if err != nil {
			return nil, fmt.Errorf("setup: reading the cluster's REST endpoint: %w", err)
		}
		endpoint = cl.HTTPEndpoint
	}
	c, err := confluentcloud.NewACLClient(confluentcloud.ACLConfig{
		Endpoint: endpoint, Cluster: s.connect.cluster, Key: key, Secret: netsec.Secret(secret), Transport: s.transport,
	})
	if err != nil {
		return nil, fmt.Errorf("setup: building the Confluent ACL client: %w", err)
	}
	s.mu.Lock()
	s.endpoint, s.key, s.secret, s.c = endpoint, key, secret, c
	s.mu.Unlock()
	return c, nil
}

// storeAiven is the Aiven client over the store's aiven_token, read at each
// call and rebuilt only when it changed.
type storeAiven struct {
	project, service string
	open             storeOpener
	baseURL          string
	transport        http.RoundTripper

	mu    sync.Mutex
	token string
	c     *aiven.Client
}

func (s *storeAiven) client() (*aiven.Client, error) {
	token, err := s.open.get(secretAivenToken)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c != nil && s.token == token {
		return s.c, nil
	}
	c, err := aiven.New(aiven.Config{
		BaseURL: s.baseURL, Token: netsec.Secret(token), Project: s.project, Service: s.service, Transport: s.transport,
	})
	if err != nil {
		return nil, fmt.Errorf("setup: building the Aiven client: %w", err)
	}
	s.token, s.c = token, c
	return c, nil
}

func (s *storeAiven) Service(ctx context.Context) (aiven.Service, error) {
	c, err := s.client()
	if err != nil {
		return aiven.Service{}, err
	}
	return c.Service(ctx)
}

func (s *storeAiven) ServiceNamed(ctx context.Context, name string) (aiven.Service, error) {
	c, err := s.client()
	if err != nil {
		return aiven.Service{}, err
	}
	return c.ServiceNamed(ctx, name)
}

func (s *storeAiven) Plugins(ctx context.Context) ([]aiven.Plugin, error) {
	c, err := s.client()
	if err != nil {
		return nil, err
	}
	return c.Plugins(ctx)
}

func (s *storeAiven) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	c, err := s.client()
	if err != nil {
		return clients.ConnectorStatus{}, err
	}
	return c.Status(ctx, name)
}

func (s *storeAiven) CreateConnector(ctx context.Context, cfg map[string]string) (string, error) {
	c, err := s.client()
	if err != nil {
		return "", err
	}
	return c.CreateConnector(ctx, cfg)
}

func (s *storeAiven) CreateIntegration(ctx context.Context, source string) (string, error) {
	c, err := s.client()
	if err != nil {
		return "", err
	}
	return c.CreateIntegration(ctx, source)
}

// Compile-time checks: the store-backed clients are the drivers' APIs.
var (
	_ managed.ConfluentAPI = (*storeConfluent)(nil)
	_ managed.ACLAPI       = (*storeACL)(nil)
	_ managed.AivenAPI     = (*storeAiven)(nil)
	_ managed.Materialiser = storeMaterialiser{}
)
