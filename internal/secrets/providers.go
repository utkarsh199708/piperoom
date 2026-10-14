package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/integrations/spec"
)

// Provider types accepted in PIPEROOM_SECRET_PROVIDERS. Azure and Google
// are built by their sub-packages and passed in as extra factories.
const (
	TypeVault                = "vault"
	TypeOpenBao              = "openbao"
	TypeKubernetes           = "kubernetes"
	TypeAWSSecretsManager    = "aws-secrets-manager"
	TypeAWSSSM               = "aws-ssm"
	TypeEnv                  = "env"
	TypeAzureKeyVault        = "azure-key-vault"
	TypeGoogleSecretManager  = "gcp-secret-manager"
	typeListForErrorMessages = "vault, openbao, kubernetes, aws-secrets-manager, aws-ssm, env, azure-key-vault, gcp-secret-manager"
)

// knownTypes decides where a new entry starts in the declaration (see
// ParseProviders); it is read-only.
var knownTypes = map[string]bool{
	TypeVault: true, TypeOpenBao: true, TypeKubernetes: true, TypeAWSSecretsManager: true,
	TypeAWSSSM: true, TypeEnv: true, TypeAzureKeyVault: true, TypeGoogleSecretManager: true,
}

// secretOptions are option keys whose values are credentials. They are
// kept as netsec.Secret in ProviderSpec.Secrets, never in Options.
var secretOptions = map[string]bool{"token": true, "secret_id": true, "jwt": true, "client_secret": true}

// ProviderSpec is one provider declared in PIPEROOM_SECRET_PROVIDERS.
type ProviderSpec struct {
	ID      string            // provider id, e.g. "vault-corp"
	Type    string            // one of the Type* consts
	Address string            // type-specific: URL, region, env prefix; may be ""
	Options map[string]string // non-secret options
	Secrets map[string]netsec.Secret
}

// GoString is String, so %#v hides secret options too.
func (s ProviderSpec) GoString() string { return s.String() }

// String shows the declaration without secret option values.
func (s ProviderSpec) String() string {
	keys := make([]string, 0, len(s.Options)+len(s.Secrets))
	for k, v := range s.Options {
		keys = append(keys, k+"="+v)
	}
	for k := range s.Secrets {
		keys = append(keys, k+"=[redacted]")
	}
	sort.Strings(keys)
	out := s.ID + "=" + s.Type + ":" + s.Address
	if len(keys) > 0 {
		out += ";" + strings.Join(keys, ";")
	}
	return out
}

// ParseProviders parses PIPEROOM_SECRET_PROVIDERS:
//
//	<id>=<type>:<address>[;<key>=<value>]...[;<id>=<type>:<address>[;...]]
//
// e.g. "vault-corp=vault:https://vault.corp:8200;auth=kubernetes;role=piperoom;aws-prod=aws-secrets-manager:eu-west-1".
// A segment starts a new provider when its value begins with a known type
// and a colon and its key is not a secret option; every other "key=value" segment is an option of the provider
// before it. Values cannot contain ';'. Credentials should be given as
// files (token_file, secret_id_file, jwt_file); token, secret_id, jwt and
// client_secret are accepted and held as secrets. Errors name ids, types
// and option keys only, never option values.
func ParseProviders(s string) ([]ProviderSpec, error) {
	var out []ProviderSpec
	seen := map[string]bool{}
	for i, seg := range strings.Split(s, ";") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		k, v, ok := strings.Cut(seg, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			return nil, fmt.Errorf("%w: PIPEROOM_SECRET_PROVIDERS segment %d is not key=value", ErrBadProviderConfig, i+1)
		}
		// A secret option (token=vault:...) is never read as a new entry,
		// or the secret would become an Address that String prints.
		if typ, addr, isEntry := strings.Cut(v, ":"); isEntry && knownTypes[typ] && !secretOptions[k] {
			if !ValidProviderID(k) {
				return nil, fmt.Errorf("%w: provider id %q must match %s", ErrBadProviderConfig, k, spec.ProviderNamePattern)
			}
			if k == spec.ProviderFiles {
				return nil, fmt.Errorf("%w: provider id %q is reserved for mounted files", ErrBadProviderConfig, k)
			}
			if seen[k] {
				return nil, fmt.Errorf("%w: provider id %q declared twice", ErrBadProviderConfig, k)
			}
			seen[k] = true
			out = append(out, ProviderSpec{ID: k, Type: typ, Address: strings.TrimSpace(addr),
				Options: map[string]string{}, Secrets: map[string]netsec.Secret{}})
			continue
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("%w: PIPEROOM_SECRET_PROVIDERS must start with <id>=<type>:<address> (types: %s)",
				ErrBadProviderConfig, typeListForErrorMessages)
		}
		cur := &out[len(out)-1]
		if _, dup := cur.Options[k]; dup || cur.Secrets[k] != "" {
			return nil, fmt.Errorf("%w: provider %q: option %q given twice", ErrBadProviderConfig, cur.ID, k)
		}
		if secretOptions[k] {
			cur.Secrets[k] = netsec.Secret(v)
		} else {
			cur.Options[k] = v
		}
	}
	return out, nil
}

// Deps are what factories need from the process.
type Deps struct {
	// LoadAWS returns an AWS config for region. nil = the default credential
	// chain (aws-sdk-go-v2/config.LoadDefaultConfig).
	LoadAWS func(ctx context.Context, region string) (aws.Config, error)
	// HTTPClient, when set, is used by the Vault and Kubernetes providers
	// (for example one whose dialer enforces PIPEROOM_INTEGRATION_ALLOWED_HOSTS).
	HTTPClient *http.Client
	// LookupEnv, Environ and Getenv default to the os functions.
	LookupEnv func(string) (string, bool)
	Environ   func() []string
	Getenv    func(string) string
}

// Factory builds one provider from its declaration.
type Factory func(ctx context.Context, s ProviderSpec, d Deps) (Provider, error)

// DefaultFactories returns the factories of the providers in this package.
// Callers add azure and gcp factories to the returned map.
func DefaultFactories() map[string]Factory {
	return map[string]Factory{
		TypeVault: vaultFactory, TypeOpenBao: vaultFactory, TypeKubernetes: kubeFactory,
		TypeAWSSecretsManager: awsSMFactory, TypeAWSSSM: awsSSMFactory, TypeEnv: envFactory,
	}
}

// optionReader hands out options and reports the ones never asked for, so
// a typo ("namspace") fails at start instead of being ignored.
type optionReader struct {
	s    ProviderSpec
	used map[string]bool
}

func newOptionReader(s ProviderSpec) *optionReader {
	return &optionReader{s: s, used: map[string]bool{}}
}

func (o *optionReader) get(k string) string {
	o.used[k] = true
	return o.s.Options[k]
}

func (o *optionReader) secret(k string) netsec.Secret {
	o.used[k] = true
	return o.s.Secrets[k]
}

func (o *optionReader) duration(k string) (time.Duration, error) {
	v := o.get(k)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: provider %q: option %s must be a positive duration", ErrBadProviderConfig, o.s.ID, k)
	}
	return d, nil
}

func (o *optionReader) done() error {
	var extra []string
	for k := range o.s.Options {
		if !o.used[k] {
			extra = append(extra, k)
		}
	}
	for k := range o.s.Secrets {
		if !o.used[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return fmt.Errorf("%w: provider %q (%s): unknown options %s", ErrBadProviderConfig, o.s.ID, o.s.Type, strings.Join(extra, ", "))
	}
	return nil
}

func vaultFactory(_ context.Context, s ProviderSpec, d Deps) (Provider, error) {
	o := newOptionReader(s)
	timeout, err := o.duration("timeout")
	if err != nil {
		return nil, err
	}
	opts := VaultOptions{
		Address: s.Address, Namespace: o.get("namespace"), Auth: o.get("auth"), AuthMount: o.get("mount"),
		Role: o.get("role"), Token: o.secret("token"), TokenFile: o.get("token_file"), RoleID: o.get("role_id"),
		SecretID: o.secret("secret_id"), SecretIDFile: o.get("secret_id_file"), JWT: o.secret("jwt"),
		JWTFile: o.get("jwt_file"), Prefix: o.get("prefix"),
		TLS:        netsec.TLS{CAFile: o.get("ca_file"), ServerName: o.get("server_name")},
		HTTPClient: d.HTTPClient, Timeout: timeout,
	}
	if m, ok := s.Options["kv2_mounts"]; ok {
		o.used["kv2_mounts"] = true
		opts.KV2Mounts = []string{}
		for _, x := range strings.Split(m, ",") {
			if x = strings.TrimSpace(x); x != "" {
				opts.KV2Mounts = append(opts.KV2Mounts, x)
			}
		}
	}
	if err := o.done(); err != nil {
		return nil, err
	}
	return asProvider(NewVaultProvider(opts))
}

func kubeFactory(_ context.Context, s ProviderSpec, d Deps) (Provider, error) {
	o := newOptionReader(s)
	timeout, err := o.duration("timeout")
	if err != nil {
		return nil, err
	}
	opts := KubernetesOptions{
		Address: s.Address, Namespace: o.get("namespace"), TokenFile: o.get("token_file"),
		CAFile: o.get("ca_file"), HTTPClient: d.HTTPClient, Timeout: timeout, Getenv: d.Getenv,
	}
	if err := o.done(); err != nil {
		return nil, err
	}
	return asProvider(NewKubernetesProvider(opts))
}

func awsConfig(ctx context.Context, s ProviderSpec, d Deps) (aws.Config, string, error) {
	o := newOptionReader(s)
	endpoint := o.get("endpoint")
	if err := o.done(); err != nil {
		return aws.Config{}, "", err
	}
	region := s.Address
	if region == "" {
		return aws.Config{}, "", fmt.Errorf("%w: provider %q: give the region as the address (%s:<region>)", ErrBadProviderConfig, s.ID, s.Type)
	}
	load := d.LoadAWS
	if load == nil {
		load = func(ctx context.Context, region string) (aws.Config, error) {
			return awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		}
	}
	ac, err := load(ctx, region)
	if err != nil {
		return aws.Config{}, "", fmt.Errorf("secrets: provider %q: loading AWS config: %w", s.ID, err)
	}
	ac.Region = region
	return ac, endpoint, nil
}

func awsSMFactory(ctx context.Context, s ProviderSpec, d Deps) (Provider, error) {
	ac, endpoint, err := awsConfig(ctx, s, d)
	if err != nil {
		return nil, err
	}
	return asProvider(NewAWSSecretsManagerProvider(AWSOptions{Config: ac, Endpoint: endpoint}))
}

func awsSSMFactory(ctx context.Context, s ProviderSpec, d Deps) (Provider, error) {
	ac, endpoint, err := awsConfig(ctx, s, d)
	if err != nil {
		return nil, err
	}
	return asProvider(NewAWSSSMProvider(AWSOptions{Config: ac, Endpoint: endpoint}))
}

func envFactory(_ context.Context, s ProviderSpec, d Deps) (Provider, error) {
	if err := newOptionReader(s).done(); err != nil {
		return nil, err
	}
	return asProvider(NewEnvProvider(EnvOptions{Prefix: s.Address, LookupEnv: d.LookupEnv, Environ: d.Environ}))
}

// ParseTTL reads PIPEROOM_SECRET_CACHE_TTL ("" = config.DefaultSecretCacheTTL).
func ParseTTL(s string) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		s = config.DefaultSecretCacheTTL
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: PIPEROOM_SECRET_CACHE_TTL must be a positive Go duration such as 5m", ErrBadProviderConfig)
	}
	return d, nil
}

// ConfigOptions are the inputs of FromConfig besides config.Config.
type ConfigOptions struct {
	Deps Deps
	// Factories maps provider types to constructors; nil = DefaultFactories().
	Factories map[string]Factory
	OnChange  func(workspaceID string, ref Ref)
	Logger    *slog.Logger
}

// FromConfig builds the process's Registry: the mounted-files provider (id
// spec.ProviderFiles, roots from PIPEROOM_SECRET_FILE_ROOTS) and every
// provider in PIPEROOM_SECRET_PROVIDERS, all in spec.DefaultWorkspace.
// Hosted installs, where workspaces are tenants, should not register the
// files or env providers for customer workspaces: they read the server's
// own files and environment. Secret managers saved through the
// Integrations page are added later with Register.
func FromConfig(ctx context.Context, cfg config.Config, opts ConfigOptions) (*Registry, error) {
	ttl, err := ParseTTL(cfg.SecretCacheTTL)
	if err != nil {
		return nil, err
	}
	specs, err := ParseProviders(cfg.SecretProviders)
	if err != nil {
		return nil, err
	}
	factories := opts.Factories
	if factories == nil {
		factories = DefaultFactories()
	}
	r := NewRegistry(Options{TTL: ttl, OnChange: opts.OnChange, Logger: opts.Logger})
	if roots := ParseRoots(cfg.SecretFileRoots); len(roots) > 0 {
		fp, err := NewFilesProvider(FilesOptions{Roots: roots})
		if err != nil {
			r.Close()
			return nil, err
		}
		if err := r.Register(spec.DefaultWorkspace, spec.ProviderFiles, fp); err != nil {
			fp.Close()
			r.Close()
			return nil, err
		}
	}
	for _, s := range specs {
		f, ok := factories[s.Type]
		if !ok {
			r.Close()
			return nil, fmt.Errorf("%w: provider %q: type %s is not built into this binary", ErrBadProviderConfig, s.ID, s.Type)
		}
		p, err := f(ctx, s, opts.Deps)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("secrets: provider %q: %w", s.ID, err)
		}
		if err := r.Register(spec.DefaultWorkspace, s.ID, p); err != nil {
			p.Close()
			r.Close()
			return nil, err
		}
	}
	return r, nil
}

// ResolveEnv returns raw as a secret, or, when raw is a secret:// reference
// (an operator put one in a secret environment variable, §6), its value
// resolved in spec.DefaultWorkspace.
func ResolveEnv(ctx context.Context, r spec.Resolver, raw string) (netsec.Secret, error) {
	if !IsRef(raw) {
		return netsec.Secret(raw), nil
	}
	if r == nil {
		return "", fmt.Errorf("%w: a secret:// reference needs a secret resolver", ErrUnknownProvider)
	}
	return r.Resolve(ctx, spec.DefaultWorkspace, raw)
}

// asProvider avoids returning a typed nil pointer inside a non-nil Provider.
func asProvider[P Provider](p P, err error) (Provider, error) {
	if err != nil {
		return nil, err
	}
	return p, nil
}
