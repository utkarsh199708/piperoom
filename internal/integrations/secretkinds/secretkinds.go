// Package secretkinds puts the tier-1 secret managers in the Integrations
// catalog (docs/design/secret-providers.md §5): one spec.Kind per provider
// type, category secret_manager.
//
// A card builds its provider through the same secrets.Factory that
// PIPEROOM_SECRET_PROVIDERS uses, so a manager declared in the environment and
// one saved on the page behave the same. The package is separate from
// internal/integrations so that the core catalog does not pull in the AWS
// SDK and the Azure and Google providers.
package secretkinds

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/integrations"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
	"piperoom/internal/secrets/azure"
	"piperoom/internal/secrets/gcp"
)

// Kind ids.
const (
	KindVault      = "secrets-vault"
	KindAWSSM      = "secrets-aws-secrets-manager"
	KindAWSSSM     = "secrets-aws-ssm"
	KindKubernetes = "secrets-kubernetes"
	KindAzure      = "secrets-azure-key-vault"
	KindGCP        = "secrets-gcp-secret-manager"
)

// defaultTestTimeout is the whole-test budget from spec.Kind.Test.
const defaultTestTimeout = 20 * time.Second

// Deps are what the cards need from the process.
type Deps struct {
	// Factories build providers by type; nil = Factories().
	Factories map[string]secrets.Factory
	// Dial is used for every address an admin typed (Vault, a Kubernetes
	// API server, an Azure vault URL). Pass integrations.Guard.DialContext
	// so PIPEROOM_INTEGRATION_ALLOWED_HOSTS and the metadata block apply.
	// nil = a plain dialer.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// LoadAWS is passed to the AWS factories; nil = the default chain.
	LoadAWS func(ctx context.Context, region string) (aws.Config, error)
	// TestTimeout bounds one Test; 0 = 20 s.
	TestTimeout time.Duration
}

// Factories returns every built-in provider factory, the cloud ones included.
func Factories() map[string]secrets.Factory {
	f := secrets.DefaultFactories()
	azure.Register(f)
	gcp.Register(f)
	return f
}

// Register adds every secret-manager card to r.
func Register(r *integrations.Registry, d Deps) error {
	for _, k := range Kinds(d) {
		if err := r.Register(k); err != nil {
			return fmt.Errorf("secretkinds: %w", err)
		}
	}
	return nil
}

// card is one provider type's form and mapping.
type card struct {
	id, ptype, title, summary, logo string
	samplePath                      string // example for the sample field's help
	fields                          []spec.Field
	// spec maps form values to a provider declaration. caPEM is a pasted CA;
	// guarded reports whether the provider dials an admin-typed address and
	// so must get the guarded HTTP client.
	spec   func(v spec.Values) (ps secrets.ProviderSpec, caPEM string, guarded bool)
	script func(v spec.Values) string
}

// Kinds returns the secret-manager cards in catalog order.
func Kinds(d Deps) []spec.Kind {
	cards := []card{vaultCard(), awsSMCard(), awsSSMCard(), kubeCard(), azureCard(), gcpCard()}
	out := make([]spec.Kind, 0, len(cards))
	for _, c := range cards {
		out = append(out, d.kind(c))
	}
	return out
}

var reads = []string{
	"The names of secrets under the prefix you set, for the secret picker",
	"The values of secrets a reference points to, when a client connects; kept in memory only",
}

var never = []string{
	"Never creates, changes or deletes a secret",
	"Never stores a secret value on disk or in the database",
	"Never shows a secret value in the UI, the logs or to the agent",
}

func nameField() spec.Field {
	return spec.Field{
		Name: spec.ProviderNameField, Label: "Name", Type: spec.FieldText, Required: true,
		Help:    "Used in references: secret://<name>/<path>. Lower-case letters, digits, - and _",
		Pattern: spec.ProviderNamePattern,
	}
}

func sampleField(example string) spec.Field {
	return spec.Field{
		Name: "sample", Label: "Sample secret", Type: spec.FieldText,
		Help: "Optional: a path Piperoom should be able to read, e.g. " + example + ". Test reports its version and keys, never its value",
	}
}

func (d Deps) kind(c card) spec.Kind {
	fields := append([]spec.Field{nameField()}, c.fields...)
	fields = append(fields, sampleField(c.samplePath))
	return spec.Kind{
		ID: c.id, Category: spec.CategorySecretManager, Title: c.title, Summary: c.summary, Logo: c.logo,
		Reads: reads, Never: never, Fields: fields,
		SetupScript: c.script,
		Test:        func(ctx context.Context, v spec.Values) []spec.Check { return d.test(ctx, c, v) },
		Build: func(ctx context.Context, v spec.Values) (any, func(), error) {
			p, err := d.build(ctx, c, v)
			if err != nil {
				return nil, nil, err
			}
			return p, p.Close, nil
		},
	}
}

// errReservedName refuses the built-in mounted-files provider's id.
var errReservedName = errors.New("secretkinds: the name " + spec.ProviderFiles + " is reserved for the mounted-files provider")

func (d Deps) build(ctx context.Context, c card, v spec.Values) (secrets.Provider, error) {
	name := strings.TrimSpace(v.Get(spec.ProviderNameField))
	if !secrets.ValidProviderID(name) {
		return nil, fmt.Errorf("secretkinds: %w: the name must match %s", secrets.ErrBadProviderConfig, spec.ProviderNamePattern)
	}
	if name == spec.ProviderFiles {
		return nil, errReservedName
	}
	ps, caPEM, guarded := c.spec(v)
	ps.ID, ps.Type = name, c.ptype
	deps := secrets.Deps{LoadAWS: d.LoadAWS}
	if guarded {
		hc, err := d.httpClient(caPEM)
		if err != nil {
			return nil, err
		}
		deps.HTTPClient = hc
	}
	fs := d.Factories
	if fs == nil {
		fs = Factories()
	}
	f, ok := fs[c.ptype]
	if !ok {
		return nil, fmt.Errorf("secretkinds: %w: no provider for type %s", secrets.ErrBadProviderConfig, c.ptype)
	}
	p, err := f(ctx, ps, deps)
	if err != nil {
		return nil, fmt.Errorf("secretkinds: building %s: %w", c.ptype, err)
	}
	return p, nil
}

func (d Deps) test(ctx context.Context, c card, v spec.Values) []spec.Check {
	budget := d.TestTimeout
	if budget <= 0 {
		budget = defaultTestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var sample *secrets.Ref
	if s := strings.TrimSpace(v.Get("sample")); s != "" {
		name := strings.TrimSpace(v.Get(spec.ProviderNameField))
		r, err := secrets.ParseRef(spec.RefPrefix + name + "/" + strings.TrimPrefix(s, "/"))
		if err != nil {
			return []spec.Check{{
				ID: "sample", Title: "Sample secret path is valid", Status: spec.CheckFail,
				Detail: "the sample is not a valid path", Fix: "Give a path like " + c.samplePath + " (no .. segments, at most one #key)",
			}}
		}
		sample = &r
	}
	p, err := d.build(ctx, c, v)
	if err != nil {
		return []spec.Check{{
			ID: "config", Title: "Settings are valid", Status: spec.CheckFail,
			Detail: configDetail(err), Fix: "Correct the highlighted settings and test again.",
		}}
	}
	defer p.Close()
	return p.Check(ctx, sample)
}

// configDetail keeps only errors the secrets package words for people
// (ErrBadProviderConfig never quotes a secret); anything else is generic,
// because a lower-level error could carry a value from the form.
func configDetail(err error) string {
	switch {
	case errors.Is(err, errReservedName):
		return "the name " + spec.ProviderFiles + " is reserved for the mounted-files provider"
	case errors.Is(err, secrets.ErrBadProviderConfig):
		msg := err.Error()
		if i := strings.LastIndex(msg, secrets.ErrBadProviderConfig.Error()); i >= 0 {
			msg = strings.TrimLeft(msg[i+len(secrets.ErrBadProviderConfig.Error()):], ": ")
		}
		if msg == "" {
			return "the settings are not valid"
		}
		return msg
	}
	return "the provider could not be set up from these settings"
}

// httpClient dials through d.Dial, trusts a pasted CA (else the system
// roots), never uses an environment proxy and never follows a redirect: the
// same rules as the integrations test clients.
func (d Deps) httpClient(caPEM string) (*http.Client, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(caPEM) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caPEM)) {
			return nil, fmt.Errorf("secretkinds: %w: the CA certificate is not valid PEM", secrets.ErrBadProviderConfig)
		}
		tc.RootCAs = pool
	}
	dial := d.Dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: 10 * time.Second}).DialContext
	}
	tr := &http.Transport{
		DialContext: dial, TLSClientConfig: tc, Proxy: nil,
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second,
		MaxIdleConns: 4, IdleConnTimeout: 90 * time.Second,
	}
	return &http.Client{
		Transport: tr,
		// A whole-request bound (AGENTS.md §4 rule 7): body reads would
		// otherwise rely on every caller's ctx deadline.
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

// put sets opts[k] when the form value is not blank.
func put(opts map[string]string, v spec.Values, k string) {
	if s := strings.TrimSpace(v.Get(k)); s != "" {
		opts[k] = s
	}
}
