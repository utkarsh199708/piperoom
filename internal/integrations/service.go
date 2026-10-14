package integrations

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
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
	// ReservedValues are the values of the server's own secret settings
	// (after their references were resolved). A reference that resolves to
	// one of them is refused whatever its spelling: the check by reference
	// (ReservedRefs) cannot see every alias a secret manager has.
	ReservedValues []netsec.Secret
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

	// notLive holds, per enabled row id, why its client is not running
	// (restore at start or a live apply after a save failed). The card then
	// shows failing instead of the stored test's "connected".
	liveMu  sync.Mutex
	notLive map[string]string
}

// SecretIdentifier is implemented by a Resolver that can name the secret a
// reference reads, so different spellings of one secret compare equal
// (secrets.Registry: a files reference by the real path of its file).
type SecretIdentifier interface {
	SecretIdentity(workspaceID, ref string) (string, bool)
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
	return &Service{o: o, limiter: newLimiter(o.TestsPerMinute, time.Minute), reserved: reserved, notLive: map[string]string{}}, nil
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

// reservedMessage refuses a server secret without quoting the reference.
const reservedMessage = "names a secret this server's own settings use; give Piperoom's integrations their own secret"

// checkReserved refuses a reference to a secret the server's own settings
// use (Options.ReservedRefs). It compares the spelling and, when the
// resolver can tell, what the reference reads: a symlinked files root or a
// Kubernetes ..data link reaches the same file under another path. The
// identities are computed on each call, since a mounted secret's links move
// on every rotation.
func (s *Service) checkReserved(ws string, m merged) error {
	if len(s.reserved) == 0 {
		return nil
	}
	refs := m.allRefs()
	idr, _ := s.o.Resolver.(SecretIdentifier)
	var ids map[string]bool
	if idr != nil {
		ids = map[string]bool{}
		for _, ref := range s.o.ReservedRefs {
			// Process references live in the default workspace.
			if id, ok := idr.SecretIdentity(spec.DefaultWorkspace, ref); ok {
				ids[id] = true
			}
		}
	}
	for _, name := range sortedKeys(refs) {
		if id, ok := secretIDOf(refs[name]); ok && s.reserved[id] {
			return &spec.ValidationError{Field: name, Message: reservedMessage}
		}
		if idr != nil {
			if id, ok := idr.SecretIdentity(ws, refs[name]); ok && ids[id] {
				return &spec.ValidationError{Field: name, Message: reservedMessage}
			}
		}
	}
	return nil
}

// isReservedValue reports whether v equals a value of the server's own
// secret settings, comparing in constant time.
func (s *Service) isReservedValue(v netsec.Secret) bool {
	if v == "" {
		return false
	}
	found := false
	for _, r := range s.o.ReservedValues {
		if r != "" && subtle.ConstantTimeCompare([]byte(r.Reveal()), []byte(v.Reveal())) == 1 {
			found = true
		}
	}
	return found
}

// SecretRefErr turns a resolver or browse error into a *spec.SecretRefError
// for field, so the HTTP API can answer 400/403/404/502 instead of 500.
// spec.ErrListUnsupported passes through unchanged; nil stays nil.
func SecretRefErr(field string, err error) error {
	if err == nil || errors.Is(err, spec.ErrListUnsupported) {
		return err
	}
	var se *spec.SecretRefError
	if errors.As(err, &se) {
		return err
	}
	reason := spec.SecretRefOther
	switch {
	case errors.Is(err, secrets.ErrInvalidRef):
		reason = spec.SecretRefInvalid
	case errors.Is(err, secrets.ErrUnknownProvider):
		reason = spec.SecretRefUnknownProvider
	case errors.Is(err, secrets.ErrAccessDenied), errors.Is(err, secrets.ErrOutsideRoots), errors.Is(err, secrets.ErrLogin):
		reason = spec.SecretRefDenied
	case errors.Is(err, secrets.ErrNotFound), errors.Is(err, secrets.ErrKeyNotFound):
		reason = spec.SecretRefNotFound
	case errors.Is(err, spec.ErrProviderUnreachable), errors.Is(err, context.DeadlineExceeded):
		reason = spec.SecretRefUnreachable
	}
	return &spec.SecretRefError{Field: field, Reason: reason, Err: err}
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
			if _, isRef := refOf(v); isRef && f.AllowRef {
				continue // a reference: the script shows the placeholder
			}
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
	if r.Enabled {
		if why, bad := s.liveFailure(r.ID); bad {
			// The stored test passed, but the running server has no client
			// built from this row: it is not connected, whatever that test said.
			v.Status = spec.StatusFailing
			v.LastTest = append(append([]spec.Check{}, v.LastTest...), spec.Check{
				ID: "live", Title: "The connection is live", Status: spec.CheckFail, Detail: why,
				Fix: "Fix the cause, then enable the integration again or restart the server.",
			})
		}
	}
	return v
}

// MarkNotLive records that enabled row id has no running client, and why
// (restore at start could not build it). cause never holds a secret value:
// it is reduced to a fixed text or a scrubbed error line.
func (s *Service) MarkNotLive(id, prefix string, cause error) {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	s.notLive[id] = truncate(prefix+liveCause(cause), maxDetail)
}

// markLive forgets a recorded live failure of id.
func (s *Service) markLive(id string) {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	delete(s.notLive, id)
}

func (s *Service) liveFailure(id string) (string, bool) {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	why, ok := s.notLive[id]
	return why, ok
}

// liveCause is a one-line, value-free description of why a row is not live.
func liveCause(err error) string {
	var se *spec.SecretRefError
	switch {
	case err == nil:
		return "unknown cause"
	case errors.Is(err, spec.ErrSecretKeyMismatch):
		return "its stored secrets cannot be opened with PIPEROOM_SECRET_KEY or PIPEROOM_SECRET_KEY_PREVIOUS"
	case errors.Is(err, spec.ErrSecretKeyMissing):
		return "it has typed-in secrets and PIPEROOM_SECRET_KEY is not set"
	case errors.As(err, &se):
		return "secret field " + se.Field + " cannot be read (" + string(se.Reason) + "): " + errText(se.Err)
	}
	return errText(err)
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

// unsealForEdit opens r's typed-in secrets for an edit or a test with id.
// When no configured key opens them (the key changed), the edit may still
// repair the row: every typed-in field must then be entered again, cleared
// or turned into a reference, and the unreadable blob is dropped.
func (s *Service) unsealForEdit(r spec.Integration, in spec.Input) (map[string]netsec.Secret, error) {
	typed, err := s.unseal(r)
	if err == nil || !errors.Is(err, spec.ErrSecretKeyMismatch) {
		return typed, err
	}
	for _, name := range r.SecretsSet {
		if _, isRef := refOf(r.Config[name]); isRef {
			continue
		}
		if si, ok := in.Secrets[name]; !ok || si.Op == spec.SecretKeep {
			return nil, &spec.ValidationError{Field: name, Message: "must be entered again: the stored value cannot be opened with the server's PIPEROOM_SECRET_KEY"}
		}
	}
	return map[string]netsec.Secret{}, nil
}

// values resolves the merged input into what a kind's Test and Build take.
// A reference that cannot be resolved becomes a failing check; first is the
// first such failure as an error (a *spec.SecretRefError or a
// *spec.ValidationError), for callers that answer with an error instead.
func (s *Service) values(ctx context.Context, ws string, k spec.Kind, m merged) (v spec.Values, bad []spec.Check, first error) {
	v = spec.Values{Config: textConfig(k, m.config), Secrets: map[string]netsec.Secret{}}
	for name, val := range m.typed {
		v.Secrets[name] = val
	}
	refs := m.allRefs()
	for _, name := range sortedKeys(refs) {
		ref := refs[name]
		title := "Secret " + name + " can be read from its secret manager"
		failWith := func(c spec.Check, err error) {
			bad = append(bad, c)
			if first == nil {
				first = err
			}
		}
		if s.o.Resolver == nil {
			failWith(spec.Check{ID: "secret:" + name, Title: title, Status: spec.CheckFail,
				Detail: "no secret manager support is configured on this server", Fix: "Enter the value instead, or connect a secret manager."},
				&spec.SecretRefError{Field: name, Reason: spec.SecretRefOther, Err: errors.New("integrations: no secret resolver is configured")})
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, DefaultCheckTimeout)
		val, err := s.o.Resolver.Resolve(rctx, ws, ref)
		cancel()
		if err != nil {
			failWith(spec.Check{ID: "secret:" + name, Title: title, Status: spec.CheckFail,
				Detail: truncate(errText(err), maxDetail), Fix: "Check the reference and that Piperoom's identity may read it (see the secret manager card's setup script)."},
				SecretRefErr(name, err))
			continue
		}
		if s.isReservedValue(val) {
			// Another spelling or alias of a server secret: never send it.
			failWith(spec.Check{ID: "secret:" + name, Title: title, Status: spec.CheckFail,
				Detail: "the referenced secret holds a value this server's own settings use", Fix: "Give Piperoom's integrations their own secret."},
				&spec.ValidationError{Field: name, Message: reservedMessage})
			continue
		}
		if _, isText := m.textRefs[name]; isText {
			// A user name from a lease: Build reads it as text.
			v.Config[name] = val.Reveal()
			continue
		}
		v.Secrets[name] = val
	}
	return v, bad, first
}

// runTest resolves references and runs the kind's checks, all within one
// DefaultTestTimeout: docs/api.md promises a test answers in at most 20 s,
// reference lookups included.
func (s *Service) runTest(ctx context.Context, ws string, k spec.Kind, m merged) (spec.Values, []spec.Check) {
	ctx, cancel := context.WithTimeout(ctx, DefaultTestTimeout)
	defer cancel()
	v, bad, _ := s.values(ctx, ws, k, m)
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
		if oldTyped, err = s.unsealForEdit(r, in); err != nil {
			return nil, err
		}
		old = &r
	}
	// Sealing is not needed to test, so a missing key does not block it.
	m, err := mergeInput(k, in, old, oldTyped, true)
	if err != nil {
		return nil, err
	}
	if err := s.checkReserved(ws, m); err != nil {
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
	if err := s.checkReserved(ws, m); err != nil {
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
	oldTyped, err := s.unsealForEdit(old, in)
	if err != nil {
		return spec.View{}, nil, err
	}
	m, err := mergeInput(k, in, &old, oldTyped, s.o.Keys != nil)
	if err != nil {
		return spec.View{}, nil, err
	}
	if err := s.checkReserved(ws, m); err != nil {
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

// SetEnabled implements spec.Service. Disabling a disabled row changes
// nothing. Enabling an enabled row rebuilds and applies its client (the
// manual recovery after a rotation or a failed start) without a new
// version, audit row or room notice.
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
	if !enabled && !r.Enabled {
		return s.view(r), nil
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
		if r.Enabled {
			return s.view(r), nil
		}
	} else if err := s.checkNoDependents(ctx, r); err != nil {
		return spec.View{}, err
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

// rowRefs returns every secret:// reference a stored row's config holds.
func rowRefs(r spec.Integration) []string {
	var out []string
	for _, name := range sortedKeys(r.Config) {
		if ref, ok := refOf(r.Config[name]); ok {
			out = append(out, ref)
		}
	}
	return out
}

// dependents lists the enabled rows of r's workspace whose references name
// the secret manager r (by its provider name). nil for other categories.
func (s *Service) dependents(ctx context.Context, r spec.Integration) ([]spec.Dependent, error) {
	if r.Category != spec.CategorySecretManager {
		return nil, nil
	}
	name, _ := r.Config[spec.ProviderNameField].(string)
	if name == "" {
		return nil, nil
	}
	rows, err := s.o.Store.List(ctx, r.WorkspaceID)
	if err != nil {
		return nil, fmt.Errorf("integrations: listing: %w", err)
	}
	var out []spec.Dependent
	for _, o := range rows {
		if o.ID == r.ID || !o.Enabled || o.WorkspaceID != r.WorkspaceID {
			continue
		}
		for _, ref := range rowRefs(o) {
			if p, err := ParseRef(ref); err == nil && p == name {
				title := o.Kind
				if k, ok := s.o.Registry.Kind(o.Kind); ok {
					title = k.Title
				}
				out = append(out, spec.Dependent{ID: o.ID, Kind: o.Kind, Title: title})
				break
			}
		}
	}
	return out, nil
}

// checkNoDependents refuses to remove a secret manager that enabled rows
// still read from: they would keep cached values until the next restart and
// then silently stop working.
func (s *Service) checkNoDependents(ctx context.Context, r spec.Integration) error {
	deps, err := s.dependents(ctx, r)
	if err != nil {
		return err
	}
	if len(deps) > 0 {
		return &spec.InUseError{Dependents: deps}
	}
	return nil
}

// OpenRow unseals and resolves a stored row into what its kind's Build
// takes. cmd/server's restore uses it, so start-up reads a row exactly as
// enable does (reserved values and reference errors included).
func (s *Service) OpenRow(ctx context.Context, r spec.Integration) (spec.Values, error) {
	k, err := s.kind(r.Kind)
	if err != nil {
		return spec.Values{}, err
	}
	return s.storedValues(ctx, r, k)
}

// SecretChanged rebuilds the clients of the enabled rows of ws that read
// the secret provider/path: a mounted file changed, or a lease was lost.
// It is the registry's OnChange (secrets.Options). A row that cannot be
// rebuilt keeps its old client and is shown as not live.
func (s *Service) SecretChanged(ctx context.Context, ws, provider, path string) {
	rows, err := s.o.Store.List(ctx, ws)
	if err != nil {
		s.o.Log.Error("integrations: a secret changed but the integrations cannot be listed", "provider", provider, "path", path, "err", err)
		return
	}
	want := strings.Trim(path, "/")
	for _, r := range rows {
		if !r.Enabled || r.WorkspaceID != ws || s.envManagesRow(ws, r) || !readsSecret(r, provider, want) {
			continue
		}
		k, ok := s.o.Registry.Kind(r.Kind)
		if !ok {
			continue
		}
		v, err := s.storedValues(ctx, r, k)
		if err == nil {
			err = s.apply(ctx, r, k, v)
		}
		if err != nil {
			s.o.Log.Warn("integrations: rebuilding after a changed secret failed; the old client stays", "id", r.ID, "category", string(r.Category), "err", scrubErr(err, secretList(v)...))
			s.MarkNotLive(r.ID, "a secret it reads changed, and rebuilding failed: ", scrubErr(err, secretList(v)...))
			continue
		}
		s.o.Log.Info("integrations: rebuilt a client after a changed secret", "id", r.ID, "category", string(r.Category))
	}
}

// readsSecret reports whether a row references provider/path (any key).
func readsSecret(r spec.Integration, provider, path string) bool {
	for _, ref := range rowRefs(r) {
		p, err := secrets.ParseRef(ref)
		if err == nil && p.Provider == provider && strings.Trim(p.Path, "/") == path {
			return true
		}
	}
	return false
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
	v, _, first := s.values(ctx, r.WorkspaceID, k, m)
	if first != nil {
		// Wrapped, not flattened: the HTTP API maps the cause (502 for an
		// unreachable manager, 4xx naming the field otherwise).
		return spec.Values{}, fmt.Errorf("integrations: %s: %w", r.ID, first)
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
		if f.Type.IsSecret() {
			continue
		}
		if s, ok := cfg[f.Name].(string); ok {
			out[f.Name] = s
		} else if ref, ok := refOf(cfg[f.Name]); ok && f.AllowRef {
			out[f.Name] = map[string]any{spec.RefKey: ref}
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
	if err := s.checkNoDependents(ctx, r); err != nil {
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
	s.markLive(id)
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
		s.MarkNotLive(rec.ID, "saved, but not live: ", scrubErr(err, secretList(v)...))
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
	if err := s.applyClient(ctx, rec, k, v); err != nil {
		return err
	}
	s.markLive(rec.ID)
	return nil
}

func (s *Service) applyClient(ctx context.Context, rec spec.Integration, k spec.Kind, v spec.Values) error {
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
	s.markLive(rec.ID)
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
