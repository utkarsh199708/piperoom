package secrets

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// EnvProvider reads secrets from the process environment, for operators
// whose orchestrator injects secrets as variables. It only reads variables
// whose names start with its prefix, and never the process's own secret
// settings (processSecretVars), even when the prefix covers them. Declared as "<id>=env:<PREFIX>" in
// PIPEROOM_SECRET_PROVIDERS; secret://<id>/<PREFIX>ORDERS_DB_PASSWORD.
type EnvProvider struct {
	prefix  string
	lookup  func(string) (string, bool)
	environ func() []string
}

var envNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// EnvOptions configure NewEnvProvider.
type EnvOptions struct {
	// Prefix is required: only variables starting with it are readable.
	Prefix string
	// LookupEnv and Environ default to os.LookupEnv and os.Environ (tests
	// inject fakes).
	LookupEnv func(string) (string, bool)
	Environ   func() []string
}

// NewEnvProvider returns an env provider.
func NewEnvProvider(opts EnvOptions) (*EnvProvider, error) {
	if opts.Prefix == "" || !envNameRe.MatchString(opts.Prefix) {
		return nil, fmt.Errorf("%w: env provider needs a variable name prefix such as PIPEROOM_INT_", ErrBadProviderConfig)
	}
	p := &EnvProvider{prefix: opts.Prefix, lookup: opts.LookupEnv, environ: opts.Environ}
	if p.lookup == nil {
		p.lookup = os.LookupEnv
	}
	if p.environ == nil {
		p.environ = os.Environ
	}
	return p, nil
}

// GoString keeps %#v to the String form.
func (p *EnvProvider) GoString() string { return p.String() }

// String names the provider.
func (p *EnvProvider) String() string { return "env provider " + p.prefix + "*" }

// processSecretVars are Piperoom's own secret settings. A page reference
// must never reach them: an admin could then send the sealing key or the
// server's credentials to a host they choose.
var processSecretVars = map[string]bool{
	"PIPEROOM_SECRET_KEY": true, "PIPEROOM_SECRET_KEY_PREVIOUS": true, "PIPEROOM_SECRET_PROVIDERS": true,
	"PIPEROOM_OIDC_CLIENT_SECRET": true, "ANTHROPIC_API_KEY": true,
	"PIPEROOM_DB_URL": true, "SOURCE_DB_URL": true, "WAREHOUSE_DB_URL": true,
	"KAFKA_SASL_PASSWORD": true, "SCHEMA_REGISTRY_PASSWORD": true, "CONNECT_PASSWORD": true,
	"SLACK_BOT_TOKEN": true, "ALERTMANAGER_TOKEN": true, "GRAFANA_ALERT_SECRET": true,
	"PAGERDUTY_ALERT_SECRET": true, "DATADOG_ALERT_TOKEN": true, "GENERIC_ALERT_SECRET": true,
}

func (p *EnvProvider) allowed(name string) bool {
	return envNameRe.MatchString(name) && strings.HasPrefix(name, p.prefix) &&
		!processSecretVars[name] && !strings.HasPrefix(name, "PIPEROOM_SECRET_KEY")
}

// Resolve reads one variable.
func (p *EnvProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	if err := ctx.Err(); err != nil {
		return Value{}, err
	}
	if ref.Version != "" {
		return Value{}, fmt.Errorf("%w: environment variables have no versions", ErrInvalidRef)
	}
	if !p.allowed(ref.Path) {
		return Value{}, fmt.Errorf("%w: variable %s does not start with %s", ErrAccessDenied, ref.Path, p.prefix)
	}
	v, ok := p.lookup(ref.Path)
	if !ok {
		return Value{}, fmt.Errorf("%w: variable %s is not set", ErrNotFound, ref.Path)
	}
	return Value{Data: netsec.Secret(v)}, nil
}

// List returns variable names under prefix (and the provider's prefix).
func (p *EnvProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := []string{}
	for _, kv := range p.environ() {
		name, _, _ := strings.Cut(kv, "=")
		if p.allowed(name) && strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return capList(out, limit), nil
}

// Keys returns the JSON keys of the variable's value.
func (p *EnvProvider) Keys(ctx context.Context, ref Ref) ([]string, error) {
	v, err := p.Resolve(ctx, ref.secretID())
	if err != nil {
		return nil, err
	}
	return KeysOf(v.Data), nil
}

// Check reads the sample.
func (p *EnvProvider) Check(ctx context.Context, sample *Ref) []spec.Check {
	c, _, _ := sampleCheck(ctx, p, sample)
	return []spec.Check{
		pass(CheckReach, "Environment is readable", "variables starting with "+p.prefix),
		c,
	}
}

// Close does nothing.
func (p *EnvProvider) Close() {}
