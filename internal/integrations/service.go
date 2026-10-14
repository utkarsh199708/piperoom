package integrations

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
)

// DefaultTestsPerMinute is the per-admin test budget (docs/api.md, 429).
const DefaultTestsPerMinute = 10

// maxSecretNames caps a SecretNames answer (docs/api.md).
const maxSecretNames = 200

// maxAuditLimit caps an Audit answer (docs/api.md).
const maxAuditLimit = 500

// SecretBrowser lists names and keys in a connected secret manager, for the
// picker. internal/secrets implements it over its live providers. It never
// returns a secret value.
type SecretBrowser interface {
	// Names returns secret names under prefix in the manager of rec, at most
	// limit. spec.ErrListUnsupported when the manager cannot list;
	// spec.ErrProviderUnreachable when it does not answer.
	Names(ctx context.Context, rec spec.Integration, prefix string, limit int) ([]string, error)
	// Keys returns the field names of the JSON or key/value secret ref.
	Keys(ctx context.Context, rec spec.Integration, ref string) ([]string, error)
}

// ChangeFunc receives a newly built client for a category (client nil and
// close nil: the category is now "not configured"). The live wiring swaps it
// in and closes the old client.
type ChangeFunc func(category spec.Category, client any, close func())

// Options builds a Service. Store and Registry are required.
type Options struct {
	Store    spec.Store
	Registry *Registry
	// Keys seals typed-in secrets; nil = PIPEROOM_SECRET_KEY is not set.
	Keys *Keyring
	// Resolver resolves secret:// references; nil = references fail their check.
	Resolver spec.Resolver
	// Applier swaps live clients. When nil, OnChange (if set) receives the
	// client built by the kind instead.
	Applier  spec.Applier
	OnChange ChangeFunc
	Notifier spec.Notifier
	Secrets  SecretBrowser
	// Env is which categories environment variables configure (EnvFromLookup).
	Env Env
	// PublicURL is PIPEROOM_PUBLIC_URL, for the alert webhook URLs.
	PublicURL string
	// ReservedRefs are secret:// references the server's own settings use
	// (PIPEROOM_SECRET_KEY, PIPEROOM_DB_URL, ...). A page row naming the
	// same secret is refused, so an admin cannot send the operator's
	// credentials to a host of their choice.
	ReservedRefs []string
	// TestsPerMinute per admin; 0 = DefaultTestsPerMinute.
	TestsPerMinute int
	Now            func() time.Time
	Log            *slog.Logger
}

// Service implements spec.Service.
type Service struct {
	o        Options
	limiter  *limiter
	reserved map[string]bool // secretIDOf(ReservedRefs)
}

var _ spec.Service = (*Service)(nil)

// NewService checks o and returns a Service.
func NewService(o Options) (*Service, error) {
	if o.Store == nil || o.Registry == nil {
		return nil, errors.New("integrations: NewService needs a Store and a Registry")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.TestsPerMinute <= 0 {
		o.TestsPerMinute = DefaultTestsPerMinute
	}
	if o.Env.Categories == nil {
		o.Env.Categories = map[spec.Category]EnvCategory{}
	}
	if o.Env.AlertSecrets == nil {
		o.Env.AlertSecrets = map[string]bool{}
	}
	reserved := map[string]bool{}
	for _, ref := range o.ReservedRefs {
		if id, ok := secretIDOf(ref); ok {
			reserved[id] = true
		}
	}
	return &Service{o: o, limiter: newLimiter(o.TestsPerMinute, time.Minute), reserved: reserved}, nil
}

// secretIDOf names the secret a reference reads (provider and path; the
// key and version do not change which secret it is).
func secretIDOf(ref string) (string, bool) {
	r, err := secrets.ParseRef(ref)
	if err != nil {
		return "", false
	}
	return r.Provider + "/" + strings.Trim(r.Path, "/"), true
}

// checkReserved refuses a reference to a secret the server's own settings
// use (Options.ReservedRefs). The message does not quote the reference.
func (s *Service) checkReserved(m merged) error {
	if len(s.reserved) == 0 {
		return nil
	}
	for _, name := range sortedKeys(m.refs) {
		if id, ok := secretIDOf(m.refs[name]); ok && s.reserved[id] {
			return &spec.ValidationError{Field: name, Message: "names a secret this server's own settings use; give Piperoom's integrations their own secret"}
		}
	}
	return nil
}

// Catalog implements spec.Service.
func (s *Service) Catalog() []spec.Kind { return s.o.Registry.Kinds() }

// SetupScript implements spec.Service.
func (s *Service) SetupScript(kind string, config map[string]any) (string, error) {
	k, ok := s.o.Registry.Kind(kind)
	if !ok {
		return "", fmt.Errorf("%w: %s", spec.ErrUnknownKind, kind)
	}
	if k.SetupScript == nil {
		return "", nil
	}
	cfg := map[string]any{}
	for _, f := range k.Fields {
		if v, ok := config[f.Name]; ok && !f.Type.IsSecret() {
			t, err := textValue(f.Name, v)
			if err != nil {
				return "", err
			}
			// A value that fails its field's pattern is left out: the script
			// shows a placeholder instead of the bad text.
			if checkText(f, strings.TrimSpace(t)) == nil {
				cfg[f.Name] = strings.TrimSpace(t)
			}
		}
	}
	return k.SetupScript(spec.Values{Config: textConfig(k, cfg), Secrets: map[string]netsec.Secret{}}), nil
}

// List implements spec.Service.
func (s *Service) List(ctx context.Context, ws string) ([]spec.View, error) {
	rows, err := s.o.Store.List(ctx, ws)
	if err != nil {
		return nil, fmt.Errorf("integrations: listing: %w", err)
	}
	out := make([]spec.View, 0, len(rows)+len(s.o.Env.Categories))
	if ws == spec.DefaultWorkspace {
		for _, c := range spec.Categories {
			if ec, ok := s.o.Env.Categories[c]; ok {
				out = append(out, s.envView(ws, c, ec))
			}
		}
		for _, src := range AlertSources {
			if s.o.Env.AlertSecrets[src] {
				out = append(out, s.envView(ws, spec.CategoryAlerts, EnvCategory{Kind: AlertKindID(src), Config: map[string]any{}, SecretsSet: []string{alertSecretField}}))
			}
		}
	}
	for _, r := range rows {
		out = append(out, s.view(r))
	}
	return out, nil
}

func (s *Service) envView(ws string, c spec.Category, ec EnvCategory) spec.View {
	id := "env_" + string(c)
	if c == spec.CategoryAlerts {
		id += "_" + alertSource(ec.Kind)
	}
	k, known := s.o.Registry.Kind(ec.Kind)
	title := k.Title
	if !known {
		title = string(c)
	}
	return spec.View{
		ID: id, WorkspaceID: ws, Category: c, Kind: ec.Kind, Title: title,
		Config: viewConfig(k, known, ec.Config), SecretsSet: nonNilStrings(ec.SecretsSet),
		Enabled: true, Status: spec.StatusUntested, LastTest: []spec.Check{},
		UpdatedBy: spec.SourceEnv, Source: spec.SourceEnv,
	}
}

// view converts a stored row for the API: no secret values, ever.
func (s *Service) view(r spec.Integration) spec.View {
	k, known := s.o.Registry.Kind(r.Kind)
	title := k.Title
	if !known {
		title = r.Kind
	}
	v := spec.View{
		ID: r.ID, WorkspaceID: r.WorkspaceID, Category: r.Category, Kind: r.Kind, Title: title,
		Config: viewConfig(k, known, r.Config), SecretsSet: nonNilStrings(r.SecretsSet),
		Version: r.Version, Enabled: r.Enabled, LastTest: r.LastTest,
		UpdatedBy: r.UpdatedBy, UpdatedAt: r.UpdatedAt, Source: r.Source,
	}
	if v.Source == "" {
		v.Source = spec.SourceDB
	}
	if v.LastTest == nil {
		v.LastTest = []spec.Check{}
	}
	if !r.LastTestAt.IsZero() {
		t := r.LastTestAt
		v.LastTestedAt = &t
	}
	switch {
	case !r.Enabled:
		v.Status = spec.StatusDisabled
	case r.LastTest == nil:
		v.Status = spec.StatusUntested
	case spec.Failed(r.LastTest):
		v.Status = spec.StatusFailing
	default:
		v.Status = spec.StatusConnected
	}
	return v
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// kind looks up a kind id.
func (s *Service) kind(id string) (spec.Kind, error) {
	k, ok := s.o.Registry.Kind(id)
	if !ok {
		return spec.Kind{}, fmt.Errorf("%w: %s", spec.ErrUnknownKind, id)
	}
	return k, nil
}

// get loads a row of ws.
func (s *Service) get(ctx context.Context, ws, id string) (spec.Integration, error) {
	r, err := s.o.Store.Get(ctx, ws, id)
	if err != nil {
		return spec.Integration{}, fmt.Errorf("integrations: loading %s: %w", id, err)
	}
	if r.WorkspaceID != ws {
		// A store that ignored the workspace must not leak another tenant's row.
		return spec.Integration{}, fmt.Errorf("integrations: loading %s: %w", id, spec.ErrNotFound)
	}
	return r, nil
}

// unseal opens a row's typed-in secrets.
func (s *Service) unseal(r spec.Integration) (map[string]netsec.Secret, error) {
	if len(r.Sealed) == 0 {
		return map[string]netsec.Secret{}, nil
	}
	if s.o.Keys == nil {
		return nil, spec.ErrSecretKeyMissing
	}
	m, err := s.o.Keys.Open(SealAAD(r.WorkspaceID, r.ID, r.Category), r.Sealed, r.KeyID)
	if err != nil {
		return nil, fmt.Errorf("integrations: %s: %w", r.ID, err)
	}
	return m, nil
}

// values resolves the merged input into what a kind's Test and Build take.
// A reference that cannot be resolved becomes a failing check.
func (s *Service) values(ctx context.Context, ws string, k spec.Kind, m merged) (spec.Values, []spec.Check) {
	v := spec.Values{Config: textConfig(k, m.config), Secrets: map[string]netsec.Secret{}}
	for name, val := range m.typed {
		v.Secrets[name] = val
	}
	var bad []spec.Check
	for _, name := range sortedKeys(m.refs) {
		ref := m.refs[name]
		title := "Secret " + name + " can be read from its secret manager"
		if s.o.Resolver == nil {
			bad = append(bad, spec.Check{ID: "secret:" + name, Title: title, Status: spec.CheckFail,
				Detail: "no secret manager support is configured on this server", Fix: "Enter the value instead, or connect a secret manager."})
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, DefaultCheckTimeout)
		val, err := s.o.Resolver.Resolve(rctx, ws, ref)
		cancel()
		if err != nil {
			bad = append(bad, spec.Check{ID: "secret:" + name, Title: title, Status: spec.CheckFail,
				Detail: truncate(errText(err), maxDetail), Fix: "Check the reference and that Piperoom's identity may read it (see the secret manager card's setup script)."})
			continue
		}
		v.Secrets[name] = val
	}
	return v, bad
}

// runTest resolves references and runs the kind's checks, all within one
// DefaultTestTimeout: docs/api.md promises a test answers in at most 20 s,
// reference lookups included.
func (s *Service) runTest(ctx context.Context, ws string, k spec.Kind, m merged) (spec.Values, []spec.Check) {
	ctx, cancel := context.WithTimeout(ctx, DefaultTestTimeout)
	defer cancel()
	v, bad := s.values(ctx, ws, k, m)
	if len(bad) > 0 {
		return v, bad
	}
	checks := k.Test(ctx, v)
	if checks == nil {
		checks = []spec.Check{}
	}
	return v, checks
}

func (s *Service) allowTest(actor string) error {
	if !s.limiter.allow(actor, s.o.Now()) {
		return spec.ErrRateLimited
	}
	return nil
}

// Test implements spec.Service.
func (s *Service) Test(ctx context.Context, ws, actor string, in spec.Input) ([]spec.Check, error) {
	k, err := s.kind(in.Kind)
	if err != nil {
		return nil, err
	}
	var old *spec.Integration
	var oldTyped map[string]netsec.Secret
	if in.ID != "" {
		r, err := s.get(ctx, ws, in.ID)
		if err != nil {
			return nil, err
		}
		if r.Kind != in.Kind {
			return nil, &spec.ValidationError{Field: "kind", Message: "must equal the saved integration's kind"}
		}
		if oldTyped, err = s.unseal(r); err != nil {
			return nil, err
		}
		old = &r
	}
	// Sealing is not needed to test, so a missing key does not block it.
	m, err := mergeInput(k, in, old, oldTyped, true)
	if err != nil {
		return nil, err
	}
	if err := s.checkReserved(m); err != nil {
		return nil, err
	}
	if err := s.allowTest(actor); err != nil {
		return nil, err
	}
	_, checks := s.runTest(ctx, ws, k, m)
	return checks, nil
}

func (s *Service) checkNotEnv(ws string, k spec.Kind) error {
	if k.Category == spec.CategoryAlerts {
		if s.o.Env.alertManaged(ws, alertSource(k.ID)) {
			return spec.ErrManagedByEnv
		}
		return nil
	}
	if s.o.Env.managed(ws, k.Category) {
		return spec.ErrManagedByEnv
	}
	return nil
}

// envManagesRow reports whether the environment now configures r's category
// (or, for an alert row, its source). Such a row is stale: env wins (§4.4),
// so it must never clear the live client the environment set up.
func (s *Service) envManagesRow(ws string, r spec.Integration) bool {
	if r.Category == spec.CategoryAlerts {
		return s.o.Env.alertManaged(ws, alertSource(r.Kind))
	}
	return s.o.Env.managed(ws, r.Category)
}

// envViewID reports whether id names an env view List shows in ws
// ("env_<category>" or "env_alerts_<source>"), so writes to it answer
// managed_by_env instead of a misleading not_found.
func (s *Service) envViewID(ws, id string) bool {
	rest, ok := strings.CutPrefix(id, "env_")
	if !ok {
		return false
	}
	if src, ok := strings.CutPrefix(rest, string(spec.CategoryAlerts)+"_"); ok {
		return s.o.Env.alertManaged(ws, src)
	}
	return s.o.Env.managed(ws, spec.Category(rest))
}

// publicURLPlaceholder stands in for the server's address in webhook URLs
// when PIPEROOM_PUBLIC_URL is not set.
const publicURLPlaceholder = "<PIPEROOM_PUBLIC_URL>"

// Create implements spec.Service.
func (s *Service) Create(ctx context.Context, ws, actor string, in spec.Input) (spec.View, []spec.Check, error) {
	k, err := s.kind(in.Kind)
	if err != nil {
		return spec.View{}, nil, err
	}
	if err := s.checkNotEnv(ws, k); err != nil {
		return spec.View{}, nil, err
	}
	if k.Category == spec.CategoryAlerts && !pagerDutyOwnsSecret(alertSource(k.ID)) {
		return spec.View{}, nil, &spec.ValidationError{Field: "kind", Message: "alert sources are set up with POST /api/integrations/alerts/{source}/rotate"}
	}
	m, err := mergeInput(k, in, nil, nil, s.o.Keys != nil)
	if err != nil {
		return spec.View{}, nil, err
	}
	if err := s.checkReserved(m); err != nil {
		return spec.View{}, nil, err
	}
	if err := s.allowTest(actor); err != nil {
		return spec.View{}, nil, err
	}
	v, checks := s.runTest(ctx, ws, k, m)
	if spec.Failed(checks) {
		return spec.View{}, checks, &spec.ChecksFailedError{Checks: checks}
	}
	id, err := newID()
	if err != nil {
		return spec.View{}, nil, err
	}
	rec := spec.Integration{
		ID: id, WorkspaceID: ws, Category: k.Category, Kind: k.ID, Scope: spec.ScopeAll,
		Config: m.config, SecretsSet: m.secretsSet(), Enabled: true, UpdatedBy: actor, Source: spec.SourceDB,
	}
	if rec.Sealed, rec.KeyID, err = s.seal(rec, m.typed); err != nil {
		return spec.View{}, nil, err
	}
	stored, err := s.o.Store.Create(ctx, rec)
	if err != nil {
		return spec.View{}, nil, fmt.Errorf("integrations: creating: %w", err)
	}
	checks = s.saveTest(ctx, &stored, checks)
	changed := append(sortedKeys(m.config), sortedKeys(m.typed)...)
	s.audit(ctx, stored, spec.AuditCreated, actor, uniqueSorted(changed), k.Title)
	checks = s.applyOrWarn(ctx, stored, k, v, checks)
	return s.view(stored), checks, nil
}

// Update implements spec.Service.
func (s *Service) Update(ctx context.Context, ws, actor, id string, ifVersion int64, in spec.Input) (spec.View, []spec.Check, error) {
	if s.envViewID(ws, id) {
		return spec.View{}, nil, spec.ErrManagedByEnv
	}
	old, err := s.get(ctx, ws, id)
	if err != nil {
		return spec.View{}, nil, err
	}
	if old.Source == spec.SourceEnv {
		return spec.View{}, nil, spec.ErrManagedByEnv
	}
	if in.Kind != old.Kind {
		return spec.View{}, nil, &spec.ValidationError{Field: "kind", Message: "must equal the saved integration's kind"}
	}
	k, err := s.kind(old.Kind)
	if err != nil {
		return spec.View{}, nil, err
	}
	if err := s.checkNotEnv(ws, k); err != nil {
		return spec.View{}, nil, err
	}
	// Only PagerDuty's alert card has a form. The other alert kinds keep a
	// generated secret that is no Field, so a form update would drop it.
	if k.Category == spec.CategoryAlerts && !pagerDutyOwnsSecret(alertSource(k.ID)) {
		return spec.View{}, nil, &spec.ValidationError{Field: "kind", Message: "alert sources are changed with POST /api/integrations/alerts/{source}/rotate"}
	}
	if old.Version != ifVersion {
		return spec.View{}, nil, spec.ErrVersionConflict
	}
	oldTyped, err := s.unseal(old)
	if err != nil {
		return spec.View{}, nil, err
	}
	m, err := mergeInput(k, in, &old, oldTyped, s.o.Keys != nil)
	if err != nil {
		return spec.View{}, nil, err
	}
	if err := s.checkReserved(m); err != nil {
		return spec.View{}, nil, err
	}
	if len(m.typed) > 0 && s.o.Keys == nil {
		return spec.View{}, nil, spec.ErrSecretKeyMissing
	}
	if err := s.allowTest(actor); err != nil {
		return spec.View{}, nil, err
	}
	v, checks := s.runTest(ctx, ws, k, m)
	if spec.Failed(checks) {
		return spec.View{}, checks, &spec.ChecksFailedError{Checks: checks}
	}
	rec := old
	rec.Config, rec.SecretsSet, rec.UpdatedBy = m.config, m.secretsSet(), actor
	if rec.Sealed, rec.KeyID, err = s.seal(rec, m.typed); err != nil {
		return spec.View{}, nil, err
	}
	stored, err := s.o.Store.Update(ctx, rec, ifVersion)
	if err != nil {
		return spec.View{}, nil, fmt.Errorf("integrations: updating %s: %w", id, err)
	}
	checks = s.saveTest(ctx, &stored, checks)
	s.audit(ctx, stored, spec.AuditUpdated, actor, m.changed, k.Title)
	if stored.Enabled {
		checks = s.applyOrWarn(ctx, stored, k, v, checks)
	}
	return s.view(stored), checks, nil
}

// SetEnabled implements spec.Service.
func (s *Service) SetEnabled(ctx context.Context, ws, actor, id string, enabled bool) (spec.View, error) {
	if s.envViewID(ws, id) {
		return spec.View{}, spec.ErrManagedByEnv
	}
	r, err := s.get(ctx, ws, id)
	if err != nil {
		return spec.View{}, err
	}
	k, err := s.kind(r.Kind)
	if err != nil {
		return spec.View{}, err
	}
	if enabled {
		if err := s.checkNotEnv(ws, k); err != nil {
			return spec.View{}, err
		}
		v, err := s.storedValues(ctx, r, k)
		if err != nil {
			return spec.View{}, err
		}
		// Build first: a client that cannot be built leaves the row disabled.
		if err := s.apply(ctx, r, k, v); err != nil {
			return spec.View{}, fmt.Errorf("integrations: enabling %s: %w", id, err)
		}
	}
	stored, err := s.o.Store.SetEnabled(ctx, ws, id, enabled, actor)
	if err != nil {
		return spec.View{}, fmt.Errorf("integrations: setting enabled on %s: %w", id, err)
	}
	action := spec.AuditEnabled
	if !enabled {
		action = spec.AuditDisabled
		if s.envManagesRow(ws, stored) {
			s.o.Log.Info("integrations: disabled a row whose category the environment manages; the live client stays", "id", id, "category", string(stored.Category))
		} else if err := s.clear(ctx, stored); err != nil {
			s.o.Log.Error("integrations: clearing the live client failed", "id", id, "category", string(stored.Category), "err", err)
		}
	}
	s.audit(ctx, stored, action, actor, []string{}, k.Title)
	return s.view(stored), nil
}

// storedValues unseals and resolves a stored row for Build.
func (s *Service) storedValues(ctx context.Context, r spec.Integration, k spec.Kind) (spec.Values, error) {
	typed, err := s.unseal(r)
	if err != nil {
		return spec.Values{}, err
	}
	m, err := mergeInput(k, spec.Input{Kind: r.Kind, Config: stringConfig(k, r.Config)}, &r, typed, true)
	if err != nil {
		return spec.Values{}, fmt.Errorf("integrations: stored %s no longer validates: %w", r.ID, err)
	}
	v, bad := s.values(ctx, r.WorkspaceID, k, m)
	if len(bad) > 0 {
		return spec.Values{}, fmt.Errorf("integrations: %s: %s", r.ID, bad[0].Detail)
	}
	// A generated alert secret is no Field, so mergeInput leaves it out.
	if k.Category == spec.CategoryAlerts {
		if sec, ok := typed[alertSecretField]; ok {
			v.Secrets[alertSecretField] = sec
		}
	}
	return v, nil
}

// stringConfig returns the text fields of a stored config as an Input config.
func stringConfig(k spec.Kind, cfg map[string]any) map[string]any {
	out := map[string]any{}
	for _, f := range k.Fields {
		if s, ok := cfg[f.Name].(string); ok && !f.Type.IsSecret() {
			out[f.Name] = s
		}
	}
	return out
}

// Delete implements spec.Service.
func (s *Service) Delete(ctx context.Context, ws, actor, id string) error {
	if s.envViewID(ws, id) {
		return spec.ErrManagedByEnv
	}
	r, err := s.get(ctx, ws, id)
	if err != nil {
		return err
	}
	if err := s.o.Store.Delete(ctx, ws, id); err != nil {
		return fmt.Errorf("integrations: deleting %s: %w", id, err)
	}
	if r.Enabled && !s.envManagesRow(ws, r) {
		if err := s.clear(ctx, r); err != nil {
			s.o.Log.Error("integrations: clearing the live client failed", "id", id, "category", string(r.Category), "err", err)
		}
	}
	title := r.Kind
	if k, ok := s.o.Registry.Kind(r.Kind); ok {
		title = k.Title
	}
	s.audit(ctx, r, spec.AuditDeleted, actor, []string{}, title)
	return nil
}

// Audit implements spec.Service.
func (s *Service) Audit(ctx context.Context, ws, id string, limit int) ([]spec.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > maxAuditLimit {
		limit = maxAuditLimit
	}
	out, err := s.o.Store.ListAudit(ctx, ws, id, limit)
	if err != nil {
		return nil, fmt.Errorf("integrations: audit of %s: %w", id, err)
	}
	if out == nil {
		out = []spec.AuditEntry{}
	}
	return out, nil
}

// alertRow finds the row of an alert source in ws.
func (s *Service) alertRow(ctx context.Context, ws, source string) (*spec.Integration, error) {
	rows, err := s.o.Store.List(ctx, ws)
	if err != nil {
		return nil, fmt.Errorf("integrations: listing: %w", err)
	}
	for i := range rows {
		if rows[i].Category == spec.CategoryAlerts && rows[i].Kind == AlertKindID(source) {
			return &rows[i], nil
		}
	}
	return nil, nil
}

func (s *Service) alertSetup(ws, source string, row *spec.Integration) spec.AlertSetup {
	base := strings.TrimRight(s.o.PublicURL, "/")
	if base == "" {
		// A relative URL is accepted by no alert tool; name what to set.
		base = publicURLPlaceholder
	}
	url := base + "/api/alerts/" + source
	snippet, lang := alertSnippet(source, url)
	a := spec.AlertSetup{Source: source, WebhookURL: url, Snippet: snippet, SnippetLang: lang}
	if row != nil {
		a.IntegrationID = row.ID
		for _, n := range row.SecretsSet {
			if n == alertSecretField {
				a.SecretSet, a.SecretSource = true, spec.SourceDB
			}
		}
	}
	if s.o.Env.alertManaged(ws, source) {
		a.SecretSet, a.SecretSource = true, spec.SourceEnv
	}
	return a
}

// AlertSetup implements spec.Service.
func (s *Service) AlertSetup(ctx context.Context, ws, source string) (spec.AlertSetup, error) {
	if !validAlertSource(source) {
		return spec.AlertSetup{}, spec.ErrUnknownAlertSource
	}
	row, err := s.alertRow(ctx, ws, source)
	if err != nil {
		return spec.AlertSetup{}, err
	}
	return s.alertSetup(ws, source, row), nil
}

// RotateAlertSecret implements spec.Service.
func (s *Service) RotateAlertSecret(ctx context.Context, ws, actor, source string) (spec.AlertSetup, netsec.Secret, error) {
	if !validAlertSource(source) {
		return spec.AlertSetup{}, "", spec.ErrUnknownAlertSource
	}
	if s.o.Env.alertManaged(ws, source) {
		return spec.AlertSetup{}, "", spec.ErrManagedByEnv
	}
	if pagerDutyOwnsSecret(source) {
		return spec.AlertSetup{}, "", &spec.ValidationError{Field: alertSecretField, Message: "PagerDuty generates the signing secret; paste it into the PagerDuty card"}
	}
	if s.o.Keys == nil {
		return spec.AlertSetup{}, "", spec.ErrSecretKeyMissing
	}
	k, err := s.kind(AlertKindID(source))
	if err != nil {
		return spec.AlertSetup{}, "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return spec.AlertSetup{}, "", fmt.Errorf("integrations: generating a secret: %w", err)
	}
	secret := netsec.Secret(base64.RawURLEncoding.EncodeToString(raw))
	typed := map[string]netsec.Secret{alertSecretField: secret}
	row, err := s.alertRow(ctx, ws, source)
	if err != nil {
		return spec.AlertSetup{}, "", err
	}
	var stored spec.Integration
	if row == nil {
		id, err := newID()
		if err != nil {
			return spec.AlertSetup{}, "", err
		}
		rec := spec.Integration{ID: id, WorkspaceID: ws, Category: spec.CategoryAlerts, Kind: k.ID, Scope: spec.ScopeAll,
			Config: map[string]any{}, SecretsSet: []string{alertSecretField}, Enabled: true, UpdatedBy: actor, Source: spec.SourceDB}
		if rec.Sealed, rec.KeyID, err = s.seal(rec, typed); err != nil {
			return spec.AlertSetup{}, "", err
		}
		if stored, err = s.o.Store.Create(ctx, rec); err != nil {
			return spec.AlertSetup{}, "", fmt.Errorf("integrations: creating the %s alert source: %w", source, err)
		}
	} else {
		rec := *row
		rec.SecretsSet, rec.UpdatedBy = []string{alertSecretField}, actor
		if rec.Sealed, rec.KeyID, err = s.seal(rec, typed); err != nil {
			return spec.AlertSetup{}, "", err
		}
		if stored, err = s.o.Store.Update(ctx, rec, row.Version); err != nil {
			return spec.AlertSetup{}, "", fmt.Errorf("integrations: rotating the %s secret: %w", source, err)
		}
	}
	s.audit(ctx, stored, spec.AuditSecretRotated, actor, []string{alertSecretField}, k.Title)
	if stored.Enabled {
		v := spec.Values{Config: map[string]string{}, Secrets: typed}
		if err := s.apply(ctx, stored, k, v); err != nil {
			s.o.Log.Error("integrations: applying the rotated alert secret failed", "source", source, "err", err)
		}
	}
	return s.alertSetup(ws, source, &stored), secret, nil
}

// secretManagerRow loads a secret_manager row.
func (s *Service) secretManagerRow(ctx context.Context, ws, id string) (spec.Integration, error) {
	r, err := s.get(ctx, ws, id)
	if err != nil {
		return spec.Integration{}, err
	}
	if r.Category != spec.CategorySecretManager {
		return spec.Integration{}, spec.ErrNotSecretManager
	}
	return r, nil
}

// SecretNames implements spec.Service.
func (s *Service) SecretNames(ctx context.Context, ws, id, prefix string) ([]string, error) {
	r, err := s.secretManagerRow(ctx, ws, id)
	if err != nil {
		return nil, err
	}
	if s.o.Secrets == nil {
		return nil, spec.ErrListUnsupported
	}
	names, err := s.o.Secrets.Names(ctx, r, prefix, maxSecretNames)
	if err != nil {
		return nil, fmt.Errorf("integrations: listing secrets of %s: %w", id, err)
	}
	if len(names) > maxSecretNames {
		names = names[:maxSecretNames]
	}
	return nonNilStrings(names), nil
}

// SecretKeys implements spec.Service.
func (s *Service) SecretKeys(ctx context.Context, ws, id, ref string) ([]string, error) {
	r, err := s.secretManagerRow(ctx, ws, id)
	if err != nil {
		return nil, err
	}
	provider, err := ParseRef(ref)
	if err != nil {
		return nil, &spec.ValidationError{Field: "ref", Message: err.Error()}
	}
	if name, _ := r.Config[spec.ProviderNameField].(string); provider != name {
		return nil, &spec.ValidationError{Field: "ref", Message: "the reference must name this secret manager"}
	}
	if s.o.Secrets == nil {
		return nil, spec.ErrListUnsupported
	}
	keys, err := s.o.Secrets.Keys(ctx, r, ref)
	if err != nil {
		return nil, fmt.Errorf("integrations: listing keys in %s: %w", id, err)
	}
	return nonNilStrings(keys), nil
}

// seal seals typed secrets for rec.
func (s *Service) seal(rec spec.Integration, typed map[string]netsec.Secret) ([]byte, string, error) {
	if len(typed) == 0 {
		return nil, "", nil
	}
	if s.o.Keys == nil {
		return nil, "", spec.ErrSecretKeyMissing
	}
	return s.o.Keys.Seal(SealAAD(rec.WorkspaceID, rec.ID, rec.Category), typed)
}

// saveTest stores the test that preceded a save. A failure here does not
// undo the save; the card then shows "untested".
func (s *Service) saveTest(ctx context.Context, rec *spec.Integration, checks []spec.Check) []spec.Check {
	if err := s.o.Store.SaveLastTest(ctx, rec.WorkspaceID, rec.ID, checks); err != nil {
		s.o.Log.Error("integrations: saving the last test failed", "id", rec.ID, "err", err)
		return checks
	}
	rec.LastTest, rec.LastTestAt = checks, s.o.Now()
	return checks
}

// applyOrWarn swaps in the new client. A failure keeps the old client and
// adds a warning, since the row is already saved.
func (s *Service) applyOrWarn(ctx context.Context, rec spec.Integration, k spec.Kind, v spec.Values, checks []spec.Check) []spec.Check {
	if err := s.apply(ctx, rec, k, v); err != nil {
		s.o.Log.Error("integrations: applying the new client failed", "id", rec.ID, "category", string(rec.Category), "err", scrubErr(err, secretList(v)...))
		return append(checks, spec.Check{ID: "apply", Title: "The new connection is live", Status: spec.CheckWarn,
			Detail: truncate("saved, but the running server kept the previous connection: "+errText(scrubErr(err, secretList(v)...)), maxDetail),
			Fix:    "Fix the cause and save again, or restart the server."})
	}
	return checks
}

func secretList(v spec.Values) []netsec.Secret {
	out := make([]netsec.Secret, 0, len(v.Secrets))
	for _, s := range v.Secrets {
		out = append(out, s)
	}
	return out
}

func (s *Service) apply(ctx context.Context, rec spec.Integration, k spec.Kind, v spec.Values) error {
	if s.o.Applier != nil {
		return s.o.Applier.Apply(ctx, rec, k, v)
	}
	if s.o.OnChange == nil {
		return nil
	}
	client, closeFn, err := k.Build(ctx, v)
	if err != nil {
		return err
	}
	s.o.OnChange(rec.Category, client, closeFn)
	return nil
}

func (s *Service) clear(ctx context.Context, rec spec.Integration) error {
	if s.o.Applier != nil {
		return s.o.Applier.Clear(ctx, rec)
	}
	if s.o.OnChange != nil {
		s.o.OnChange(rec.Category, nil, nil)
	}
	return nil
}

// audit appends the audit row and notifies open rooms. The write already
// happened, so a failed audit append is logged, not returned.
func (s *Service) audit(ctx context.Context, rec spec.Integration, action spec.AuditAction, actor string, changed []string, kindTitle string) {
	e := spec.AuditEntry{WorkspaceID: rec.WorkspaceID, IntegrationID: rec.ID, Action: action, Actor: actor, Changed: nonNilStrings(changed), At: s.o.Now()}
	if err := s.o.Store.AppendAudit(ctx, e); err != nil {
		s.o.Log.Error("integrations: audit append failed", "id", rec.ID, "action", string(action), "err", err)
	}
	if s.o.Notifier != nil {
		s.o.Notifier.IntegrationChanged(ctx, rec.WorkspaceID, e, kindTitle)
	}
}

func uniqueSorted(s []string) []string {
	m := map[string]bool{}
	for _, x := range s {
		m[x] = true
	}
	return sortedKeys(m)
}

// newID returns "int_" and 24 random hex characters.
func newID() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("integrations: generating an id: %w", err)
	}
	return "int_" + hex.EncodeToString(b), nil
}

// limiter is a per-actor sliding window.
type limiter struct {
	mu     sync.Mutex
	n      int
	window time.Duration
	seen   map[string][]time.Time
}

func newLimiter(n int, window time.Duration) *limiter {
	return &limiter{n: n, window: window, seen: map[string][]time.Time{}}
}

func (l *limiter) allow(actor string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	cut := now.Add(-l.window)
	kept := l.seen[actor][:0]
	for _, t := range l.seen[actor] {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= l.n {
		l.seen[actor] = kept
		return false
	}
	l.seen[actor] = append(kept, now)
	// Drop idle actors so the map does not grow without bound.
	if len(l.seen) > 1024 {
		for a, ts := range l.seen {
			if len(ts) == 0 || !ts[len(ts)-1].After(cut) {
				delete(l.seen, a)
			}
		}
	}
	return true
}
