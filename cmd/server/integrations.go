package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/integrations"
	"piperoom/internal/integrations/secretkinds"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

// Integrations page wiring (docs/design/integrations.md §4, secret-providers.md).
//
// Start-up order in run():
//  1. newSecretsRegistry: PIPEROOM_SECRET_PROVIDERS and the mounted-files
//     provider; resolveEnvSecrets then swaps every secret:// reference in a
//     secret variable for its value, before anything uses the config.
//  2. newIntegrations: the sealing key, the host guard, the catalog, the
//     service over the Postgres store (memstore in --demo).
//  3. restore: every saved, enabled row of the default workspace is built and
//     swapped in (secret managers first, so references in the other rows
//     resolve). A row that fails is logged and its category stays "not
//     configured": the server starts in setup mode instead of refusing.

// startupRestoreTimeout bounds rebuilding all saved integrations at start.
const startupRestoreTimeout = 60 * time.Second

// secretBrowseTimeout bounds one name or key listing for the picker.
const secretBrowseTimeout = 10 * time.Second

// newSecretsRegistry builds the process's secret resolver. A bad
// PIPEROOM_SECRET_PROVIDERS or PIPEROOM_SECRET_CACHE_TTL refuses start.
func newSecretsRegistry(ctx context.Context, cfg config.Config, log *slog.Logger) (*secrets.Registry, error) {
	// Every built-in provider, Azure and Google included, for env-declared
	// managers; the catalog cards use the same factories.
	reg, err := secrets.FromConfig(ctx, cfg, secrets.ConfigOptions{Factories: secretkinds.Factories(), Logger: log})
	if err != nil {
		return nil, fmt.Errorf("server: secret providers: %w", err)
	}
	return reg, nil
}

// envSecretVar is one process variable that may hold a secret:// reference.
type envSecretVar struct {
	name string
	p    *string
}

// envSecretVars lists the secret variables of cfg (secret-providers.md §6).
func envSecretVars(cfg *config.Config) []envSecretVar {
	return []envSecretVar{
		{"PIPEROOM_DB_URL", &cfg.RoomDBURL},
		{"SOURCE_DB_URL", &cfg.SourceDBURL},
		{"WAREHOUSE_DB_URL", &cfg.WarehouseDBURL},
		{"KAFKA_SASL_PASSWORD", &cfg.KafkaSASLPassword},
		{"SCHEMA_REGISTRY_PASSWORD", &cfg.SchemaRegistryPassword},
		{"CONNECT_PASSWORD", &cfg.ConnectPassword},
		{"SLACK_BOT_TOKEN", &cfg.SlackBotToken},
		{"ALERTMANAGER_TOKEN", &cfg.AlertmanagerToken},
		{"GRAFANA_ALERT_SECRET", &cfg.GrafanaAlertSecret},
		{"PAGERDUTY_ALERT_SECRET", &cfg.PagerDutyAlertSecret},
		{"DATADOG_ALERT_TOKEN", &cfg.DatadogAlertToken},
		{"GENERIC_ALERT_SECRET", &cfg.GenericAlertSecret},
		{"PIPEROOM_OIDC_CLIENT_SECRET", &cfg.OIDCClientSecret},
		{"PIPEROOM_SECRET_KEY", &cfg.SecretKey},
		{"PIPEROOM_SECRET_KEY_PREVIOUS", &cfg.SecretKeyPrevious},
	}
}

// processSecretRefs returns the secret:// references the process's own
// secret variables hold. Call it before resolveEnvSecrets replaces them.
// Page rows may not use these (integrations.md §8): an admin could
// otherwise send the server's own credentials to a host they choose.
func processSecretRefs(cfg config.Config) []string {
	var out []string
	for _, f := range envSecretVars(&cfg) {
		if secrets.IsRef(*f.p) {
			out = append(out, *f.p)
		}
	}
	return out
}

// resolveEnvSecrets replaces each secret variable that holds a secret://
// reference with the referenced value (secret-providers.md §6). The values
// live only in this in-memory config; a reference that cannot be resolved
// refuses start, naming the variable and never a value.
func resolveEnvSecrets(ctx context.Context, cfg *config.Config, r spec.Resolver) error {
	fields := envSecretVars(cfg)
	for _, f := range fields {
		if !secrets.IsRef(*f.p) {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, secrets.DefaultResolveTimeout)
		v, err := secrets.ResolveEnv(rctx, r, *f.p)
		cancel()
		if err != nil {
			return fmt.Errorf("server: %s holds a secret:// reference that cannot be resolved: %w", f.name, err)
		}
		*f.p = v.Reveal()
	}
	return nil
}

// intSetup is what newIntegrations builds.
type intSetup struct {
	svc     *integrations.Service
	reg     *integrations.Registry
	keys    *integrations.Keyring // nil = PIPEROOM_SECRET_KEY not set
	store   spec.Store
	secrets *secrets.Registry
	applier *intApplier
	env     integrations.Env
	log     *slog.Logger
}

// intOptions are newIntegrations' inputs besides the config.
type intOptions struct {
	Store   spec.Store
	Secrets *secrets.Registry
	// Live swaps data-stack clients; nil in --demo.
	Live spec.Applier
	// Demo: saving or enabling a row stores it but builds no client at all,
	// not even a secret manager (which would dial out).
	Demo     bool
	Notifier spec.Notifier
	Env      integrations.Env
	Log      *slog.Logger
	// ReservedRefs are the references process variables use
	// (processSecretRefs); page rows may not use them.
	ReservedRefs []string
}

func newIntegrations(cfg config.Config, o intOptions) (*intSetup, error) {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	var keys *integrations.Keyring
	if cfg.SecretKey == "" {
		// Logged once: typed-in secrets cannot be saved, references still can.
		log.Warn("server: PIPEROOM_SECRET_KEY is not set: the Integrations page cannot save typed-in secrets (secret manager references still work)")
	} else {
		k, err := integrations.NewKeyring(netsec.Secret(cfg.SecretKey), netsec.Secret(cfg.SecretKeyPrevious))
		if err != nil {
			return nil, fmt.Errorf("server: PIPEROOM_SECRET_KEY: %w", err)
		}
		keys = k
	}
	guard, err := integrations.NewGuard(cfg.IntegrationAllowedHosts, nil)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	reg, err := integrations.NewBuiltinRegistry(integrations.Deps{Guard: guard})
	if err != nil {
		return nil, fmt.Errorf("server: integrations catalog: %w", err)
	}
	// Secret-manager cards dial admin-typed addresses through the same guard.
	if err := secretkinds.Register(reg, secretkinds.Deps{Dial: guard.DialContext}); err != nil {
		return nil, fmt.Errorf("server: integrations catalog: %w", err)
	}
	ap := &intApplier{live: o.Live, secrets: o.Secrets, demo: o.Demo, log: log, envProviders: envProviderIDs(cfg)}
	opts := integrations.Options{
		Store: o.Store, Registry: reg, Keys: keys, Notifier: o.Notifier, Env: o.Env,
		PublicURL: cfg.PublicURL, Log: log, Applier: ap, ReservedRefs: o.ReservedRefs,
	}
	if o.Secrets != nil {
		opts.Resolver = o.Secrets
		opts.Secrets = secretBrowser{r: o.Secrets}
	}
	svc, err := integrations.NewService(opts)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	return &intSetup{svc: svc, reg: reg, keys: keys, store: o.Store, secrets: o.Secrets, applier: ap, env: o.Env, log: log}, nil
}

// restoreResult is what restore found.
type restoreResult struct {
	// alertSecrets maps an alert source to the secret saved on the page.
	alertSecrets map[string]netsec.Secret
	// applied lists the categories now served from a saved row.
	applied []spec.Category
}

// restore builds and swaps in every enabled row of the default workspace.
// Nothing here refuses start: a row that cannot be built is logged by id
// and category, and its category stays "not configured" (setup mode).
func (s *intSetup) restore(ctx context.Context) restoreResult {
	out := restoreResult{alertSecrets: map[string]netsec.Secret{}}
	ctx, cancel := context.WithTimeout(ctx, startupRestoreTimeout)
	defer cancel()
	rows, err := s.store.List(ctx, spec.DefaultWorkspace)
	if err != nil {
		s.log.Error("server: integrations: cannot list saved integrations; starting with none", "err", err)
		return out
	}
	// Secret managers first: the other rows' references name them.
	var ordered []spec.Integration
	for _, pass := range []bool{true, false} {
		for _, r := range rows {
			if (r.Category == spec.CategorySecretManager) == pass {
				ordered = append(ordered, r)
			}
		}
	}
	seen := map[spec.Category]bool{}
	for _, r := range ordered {
		if !r.Enabled || r.WorkspaceID != spec.DefaultWorkspace {
			continue
		}
		if s.envManages(r) {
			s.log.Info("server: integrations: a saved row is shadowed by environment variables", "id", r.ID, "category", string(r.Category))
			continue
		}
		k, ok := s.reg.Kind(r.Kind)
		if !ok {
			s.log.Warn("server: integrations: saved row has an unknown kind; left not configured", "id", r.ID, "kind", r.Kind)
			continue
		}
		v, err := s.storedValues(ctx, r, k)
		if err != nil {
			s.log.Warn("server: integrations: cannot read a saved row's secrets; left not configured", "id", r.ID, "category", string(r.Category), "err", err)
			continue
		}
		if r.Category == spec.CategoryAlerts {
			if sec, ok := v.Secrets["secret"]; ok && sec != "" {
				out.alertSecrets[alertSourceOf(r.Kind)] = sec
			}
			continue
		}
		if err := s.applier.Apply(ctx, r, k, v); err != nil {
			s.log.Warn("server: integrations: cannot connect a saved integration; left not configured", "id", r.ID, "category", string(r.Category), "err", err)
			continue
		}
		if !seen[r.Category] {
			seen[r.Category] = true
			out.applied = append(out.applied, r.Category)
		}
	}
	return out
}

func (s *intSetup) envManages(r spec.Integration) bool {
	if r.Category == spec.CategoryAlerts {
		return s.env.AlertSecrets[alertSourceOf(r.Kind)]
	}
	_, ok := s.env.Categories[r.Category]
	return ok
}

// storedValues opens a saved row into what its kind's Build takes: the
// non-secret fields (with defaults), the sealed typed-in secrets and the
// resolved references. It mirrors the integrations service's own reading of
// a row, which that package does not export.
func (s *intSetup) storedValues(ctx context.Context, r spec.Integration, k spec.Kind) (spec.Values, error) {
	v := spec.Values{Config: map[string]string{}, Secrets: map[string]netsec.Secret{}}
	if len(r.Sealed) > 0 {
		if s.keys == nil {
			return v, spec.ErrSecretKeyMissing
		}
		typed, err := s.keys.Open(integrations.SealAAD(r.WorkspaceID, r.ID, r.Category), r.Sealed, r.KeyID)
		if err != nil {
			return v, fmt.Errorf("server: unsealing %s: %w", r.ID, err)
		}
		for name, val := range typed {
			v.Secrets[name] = val
		}
	}
	for _, f := range k.Fields {
		raw, present := r.Config[f.Name]
		if !f.Type.IsSecret() {
			t, _ := raw.(string)
			if t == "" {
				t = f.Default
			}
			if t != "" {
				v.Config[f.Name] = t
			}
			continue
		}
		ref, isRef := storedRef(raw)
		if !present || !isRef {
			continue
		}
		if s.secrets == nil {
			return v, fmt.Errorf("server: %s: field %s is a secret:// reference but no secret resolver is configured", r.ID, f.Name)
		}
		rctx, cancel := context.WithTimeout(ctx, secrets.DefaultResolveTimeout)
		val, err := s.secrets.Resolve(rctx, r.WorkspaceID, ref)
		cancel()
		if err != nil {
			return v, fmt.Errorf("server: %s: resolving field %s: %w", r.ID, f.Name, err)
		}
		v.Secrets[f.Name] = val
	}
	return v, nil
}

// storedRef reads a stored {"$ref": "secret://…"}.
func storedRef(v any) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", false
	}
	ref, ok := m[spec.RefKey].(string)
	return ref, ok && secrets.IsRef(ref)
}

// alertSourceOf is the source of an alert kind id ("alerts-grafana" → "grafana").
func alertSourceOf(kind string) string {
	const p = "alerts-"
	if len(kind) > len(p) && kind[:len(p)] == p {
		return kind[len(p):]
	}
	return ""
}

// withSavedAlertSecrets fills the alert sources the environment leaves
// empty with secrets saved on the Integrations page. Env wins (§4.4).
func withSavedAlertSecrets(a alertin.Config, saved map[string]netsec.Secret) alertin.Config {
	set := func(dst *string, src string) {
		if *dst == "" {
			if v, ok := saved[src]; ok {
				*dst = v.Reveal()
			}
		}
	}
	set(&a.AlertmanagerToken, "alertmanager")
	set(&a.GrafanaSecret, "grafana")
	set(&a.PagerDutySecret, "pagerduty")
	set(&a.DatadogToken, "datadog")
	set(&a.GenericSecret, "generic")
	return a
}

// intApplier implements spec.Applier for every category: data-stack and
// model clients go to wiring.Live, secret managers to the secrets registry.
type intApplier struct {
	live    spec.Applier // *wiring.Live; nil in --demo
	secrets *secrets.Registry
	// envProviders are the ids declared in PIPEROOM_SECRET_PROVIDERS (and
	// files). They live in the default workspace and the environment wins
	// (integrations.md §4.4): a saved row may not replace or unregister one.
	envProviders map[string]bool
	// demo builds nothing: --demo's clients are fakes.
	demo bool
	log  *slog.Logger
}

var _ spec.Applier = (*intApplier)(nil)

// Apply implements spec.Applier.
func (a *intApplier) Apply(ctx context.Context, rec spec.Integration, kind spec.Kind, v spec.Values) error {
	if a.demo {
		// The row is saved; --demo builds no client for any category.
		return nil
	}
	switch rec.Category {
	case spec.CategorySecretManager:
		return a.applySecretManager(ctx, rec, kind, v)
	case spec.CategoryAlerts:
		// The signed alert routes read their secrets once, at start
		// (withSavedAlertSecrets). An error here would turn enable into a
		// 500, so the gap is logged instead, and the rotate response says
		// restart_required (httpapi.Integrations.AlertRestartRequired).
		a.log.Warn("server: integrations: a saved alert secret takes effect after a server restart", "id", rec.ID, "kind", rec.Kind)
		return nil
	case spec.CategoryChat:
		// Slack mirroring is built from SLACK_BOT_TOKEN and SLACK_CHANNEL_ID only.
		a.log.Warn("server: integrations: Slack saved on the Integrations page is not used by the server yet; set SLACK_BOT_TOKEN and SLACK_CHANNEL_ID", "id", rec.ID)
		return nil
	}
	if a.live == nil {
		// --demo has fake clients only; the row is saved and nothing is built.
		return nil
	}
	return a.live.Apply(ctx, rec, kind, v)
}

func (a *intApplier) applySecretManager(ctx context.Context, rec spec.Integration, kind spec.Kind, v spec.Values) error {
	if a.secrets == nil {
		return errors.New("server: no secret registry")
	}
	name := v.Get(spec.ProviderNameField)
	if name == "" {
		return fmt.Errorf("server: secret manager %s has no %s", rec.ID, spec.ProviderNameField)
	}
	if a.shadowsEnv(rec.WorkspaceID, name) {
		return fmt.Errorf("server: secret manager %s: the name %q is declared in PIPEROOM_SECRET_PROVIDERS: %w", rec.ID, name, spec.ErrManagedByEnv)
	}
	client, closeFn, err := kind.Build(ctx, v)
	if err != nil {
		return fmt.Errorf("server: building secret manager %s: %w", rec.ID, err)
	}
	p, ok := client.(secrets.Provider)
	if !ok || p == nil {
		if closeFn != nil {
			closeFn()
		}
		return fmt.Errorf("server: kind %s built %T, not a secrets.Provider", kind.ID, client)
	}
	// The registry closes the provider on Replace and Unregister; the
	// kind's own closer is not needed then.
	if err := a.secrets.Replace(rec.WorkspaceID, name, p); err != nil {
		p.Close()
		return fmt.Errorf("server: registering secret manager %s: %w", rec.ID, err)
	}
	return nil
}

// Clear implements spec.Applier.
func (a *intApplier) Clear(ctx context.Context, rec spec.Integration) error {
	if a.demo {
		return nil
	}
	switch rec.Category {
	case spec.CategorySecretManager:
		if a.secrets != nil {
			if name, _ := rec.Config[spec.ProviderNameField].(string); name != "" && !a.shadowsEnv(rec.WorkspaceID, name) {
				a.secrets.Unregister(rec.WorkspaceID, name)
			}
		}
		return nil
	case spec.CategoryAlerts, spec.CategoryChat:
		return nil
	}
	if a.live == nil {
		return nil
	}
	return a.live.Clear(ctx, rec)
}

// shadowsEnv reports whether name in ws is an env-declared provider.
func (a *intApplier) shadowsEnv(ws, name string) bool {
	return ws == spec.DefaultWorkspace && (name == spec.ProviderFiles || a.envProviders[name])
}

// envProviderIDs lists the provider ids PIPEROOM_SECRET_PROVIDERS declares.
// newSecretsRegistry has already refused a declaration that does not parse.
func envProviderIDs(cfg config.Config) map[string]bool {
	out := map[string]bool{}
	specs, err := secrets.ParseProviders(cfg.SecretProviders)
	if err != nil {
		return out
	}
	for _, s := range specs {
		out[s.ID] = true
	}
	return out
}

// secretBrowser implements integrations.SecretBrowser over the registry's
// live providers. It returns names and keys, never values.
type secretBrowser struct{ r *secrets.Registry }

var _ integrations.SecretBrowser = secretBrowser{}

func (b secretBrowser) provider(rec spec.Integration) (secrets.Provider, error) {
	name, _ := rec.Config[spec.ProviderNameField].(string)
	p, err := b.r.Provider(rec.WorkspaceID, name)
	if err != nil {
		// A disabled row or one that failed to build has no live provider.
		return nil, fmt.Errorf("server: secret manager %s is not connected: %w", rec.ID, spec.ErrProviderUnreachable)
	}
	return p, nil
}

// Names implements integrations.SecretBrowser.
func (b secretBrowser) Names(ctx context.Context, rec spec.Integration, prefix string, limit int) ([]string, error) {
	p, err := b.provider(rec)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, secretBrowseTimeout)
	defer cancel()
	names, err := p.List(ctx, prefix, limit)
	return names, browseErr(err)
}

// Keys implements integrations.SecretBrowser.
func (b secretBrowser) Keys(ctx context.Context, rec spec.Integration, ref string) ([]string, error) {
	r, err := secrets.ParseRef(ref)
	if err != nil {
		return nil, &spec.ValidationError{Field: "ref", Message: "not a valid secret:// reference"}
	}
	p, err := b.provider(rec)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, secretBrowseTimeout)
	defer cancel()
	keys, err := p.Keys(ctx, r)
	return keys, browseErr(err)
}

// browseErr keeps ErrListUnsupported and maps a manager that did not answer
// to ErrProviderUnreachable (502); other errors pass through (500, logged).
func browseErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, spec.ErrListUnsupported), errors.Is(err, spec.ErrProviderUnreachable):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("server: secret manager timed out: %w", spec.ErrProviderUnreachable)
	}
	return err
}

// lateNotifier forwards to the room service, which is built after the
// integrations service (the room service needs the tools, which need the
// restored clients). Until set, notices are dropped.
type lateNotifier struct{ n atomic.Pointer[spec.Notifier] }

var _ spec.Notifier = (*lateNotifier)(nil)

func (l *lateNotifier) set(n spec.Notifier) { l.n.Store(&n) }

// IntegrationChanged implements spec.Notifier.
func (l *lateNotifier) IntegrationChanged(ctx context.Context, ws string, e spec.AuditEntry, title string) {
	if p := l.n.Load(); p != nil {
		(*p).IntegrationChanged(ctx, ws, e, title)
	}
}

// dataStackCategories are the five categories wiring.Open builds together.
var dataStackCategories = []spec.Category{
	spec.CategorySourceDB, spec.CategoryWarehouse, spec.CategoryKafka, spec.CategoryConnect, spec.CategoryRegistry,
}

// errPartialDataStack refuses a start where environment variables set some
// data-stack categories but not all of them.
var errPartialDataStack = errors.New("server: environment variables set only part of the data stack")

// dataStackFromEnv reports whether wiring.Open should run: true when every
// data-stack category is set by environment variables, false when none is
// (setup mode: the Integrations page fills them). A mix is refused, because
// wiring.Open builds all five at once and would dial the dev defaults
// (localhost) for the categories nobody set, failing the start when they do
// not answer and holding unused connections when they do. Mixing env and the
// page needs a per-category opener in internal/wiring.
func dataStackFromEnv(envCats []spec.Category) (bool, error) {
	set := map[spec.Category]bool{}
	for _, c := range envCats {
		set[c] = true
	}
	var have, missing []string
	for _, c := range dataStackCategories {
		if set[c] {
			have = append(have, string(c))
		} else {
			missing = append(missing, string(c))
		}
	}
	switch {
	case len(have) == 0:
		return false, nil
	case len(missing) == 0:
		return true, nil
	}
	return false, fmt.Errorf("%w: set %s, not set %s; set the variables of the missing categories too (e.g. SOURCE_DB_URL, WAREHOUSE_DB_URL, KAFKA_BROKERS, CONNECT_URL, SCHEMA_REGISTRY_URL), or remove them all and connect the stack on the Integrations page",
		errPartialDataStack, strings.Join(have, ", "), strings.Join(missing, ", "))
}

// errAnthropicKeyRef refuses a start where ANTHROPIC_API_KEY holds a
// secret:// reference.
var errAnthropicKeyRef = errors.New("server: ANTHROPIC_API_KEY holds a secret:// reference, which is not supported for this variable yet; set the key itself, or save the model on the Integrations page with a reference")

// checkAnthropicKey refuses a secret:// reference in ANTHROPIC_API_KEY. The
// Anthropic SDK reads that variable itself, so cmd/server cannot swap the
// reference for its value: it would be sent to Anthropic as the key and the
// model would fail with an unclear 401.
func checkAnthropicKey(lookup func(string) (string, bool)) error {
	if v, ok := lookup("ANTHROPIC_API_KEY"); ok && secrets.IsRef(strings.TrimSpace(v)) {
		return errAnthropicKeyRef
	}
	return nil
}

// secretFileRootsForOS drops the default PIPEROOM_SECRET_FILE_ROOTS on
// Windows: they are Unix mount points, which filepath.IsAbs rejects there,
// so the files provider would refuse the start of every Windows dev server.
// An explicitly set value is kept and checked as usual.
func secretFileRootsForOS(cfg config.Config, log *slog.Logger) config.Config {
	if runtime.GOOS == "windows" && cfg.SecretFileRoots == config.DefaultSecretFileRoots {
		log.Info("server: the default PIPEROOM_SECRET_FILE_ROOTS are Unix paths; the mounted-files secret provider is off on Windows unless the variable is set")
		cfg.SecretFileRoots = ""
	}
	return cfg
}
