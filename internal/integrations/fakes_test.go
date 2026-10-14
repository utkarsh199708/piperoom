package integrations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// memStore is an in-memory spec.Store with the contract's semantics.
type memStore struct {
	mu    sync.Mutex
	rows  map[string]spec.Integration // ws/id
	audit []spec.AuditEntry
	now   func() time.Time
	fail  error // returned by every write when set
}

func newMemStore() *memStore {
	return &memStore{rows: map[string]spec.Integration{}, now: func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }}
}

func (s *memStore) key(ws, id string) string { return ws + "/" + id }

func (s *memStore) List(_ context.Context, ws string) ([]spec.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []spec.Integration{}
	for _, r := range s.rows {
		if r.WorkspaceID == ws {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Category != out[j].Category {
			return out[i].Category < out[j].Category
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *memStore) Get(_ context.Context, ws, id string) (spec.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[s.key(ws, id)]
	if !ok {
		return spec.Integration{}, spec.ErrNotFound
	}
	return r, nil
}

func (s *memStore) dupLocked(rec spec.Integration) bool {
	for _, r := range s.rows {
		if r.WorkspaceID == rec.WorkspaceID && r.ID != rec.ID && spec.UniqueKey(r) == spec.UniqueKey(rec) {
			return true
		}
	}
	return false
}

func (s *memStore) Create(_ context.Context, rec spec.Integration) (spec.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return spec.Integration{}, s.fail
	}
	if _, ok := s.rows[s.key(rec.WorkspaceID, rec.ID)]; ok || s.dupLocked(rec) {
		return spec.Integration{}, fmt.Errorf("mem: %w", spec.ErrDuplicate)
	}
	rec.Version, rec.UpdatedAt = 1, s.now()
	s.rows[s.key(rec.WorkspaceID, rec.ID)] = rec
	return rec, nil
}

func (s *memStore) Update(_ context.Context, rec spec.Integration, ifVersion int64) (spec.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return spec.Integration{}, s.fail
	}
	old, ok := s.rows[s.key(rec.WorkspaceID, rec.ID)]
	if !ok {
		return spec.Integration{}, spec.ErrNotFound
	}
	if old.Version != ifVersion {
		return spec.Integration{}, spec.ErrVersionConflict
	}
	if s.dupLocked(rec) {
		return spec.Integration{}, spec.ErrDuplicate
	}
	old.Config, old.Sealed, old.KeyID, old.SecretsSet, old.Enabled, old.UpdatedBy = rec.Config, rec.Sealed, rec.KeyID, rec.SecretsSet, rec.Enabled, rec.UpdatedBy
	old.Version, old.UpdatedAt = ifVersion+1, s.now()
	s.rows[s.key(rec.WorkspaceID, rec.ID)] = old
	return old, nil
}

func (s *memStore) Delete(_ context.Context, ws, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[s.key(ws, id)]; !ok {
		return spec.ErrNotFound
	}
	delete(s.rows, s.key(ws, id))
	return nil
}

func (s *memStore) SetEnabled(_ context.Context, ws, id string, enabled bool, actor string) (spec.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[s.key(ws, id)]
	if !ok {
		return spec.Integration{}, spec.ErrNotFound
	}
	r.Enabled, r.UpdatedBy, r.Version, r.UpdatedAt = enabled, actor, r.Version+1, s.now()
	s.rows[s.key(ws, id)] = r
	return r, nil
}

func (s *memStore) SaveLastTest(_ context.Context, ws, id string, checks []spec.Check) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[s.key(ws, id)]
	if !ok {
		return spec.ErrNotFound
	}
	r.LastTest, r.LastTestAt = checks, s.now()
	s.rows[s.key(ws, id)] = r
	return nil
}

func (s *memStore) AppendAudit(_ context.Context, e spec.AuditEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.Seq, e.At = int64(len(s.audit)+1), s.now()
	s.audit = append(s.audit, e)
	return nil
}

func (s *memStore) ListAudit(_ context.Context, ws, id string, limit int) ([]spec.AuditEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := []spec.AuditEntry{}
	for i := len(s.audit) - 1; i >= 0 && len(out) < limit; i-- {
		if e := s.audit[i]; e.WorkspaceID == ws && e.IntegrationID == id {
			out = append(out, e)
		}
	}
	return out, nil
}

// fakeKind is a configurable connect kind.
type fakeKind struct {
	mu       sync.Mutex
	checks   []spec.Check
	buildErr error
	seen     []spec.Values
}

func (f *fakeKind) kind() spec.Kind {
	return spec.Kind{
		ID: "fake-connect", Category: spec.CategoryConnect, Title: "Fake Connect",
		Fields: []spec.Field{
			{Name: "url", Label: "URL", Type: spec.FieldText, Required: true, Pattern: urlPattern},
			{Name: "username", Label: "User", Type: spec.FieldText},
			{Name: "port", Label: "Port", Type: spec.FieldNumber},
			{Name: "tls", Label: "TLS", Type: spec.FieldBool},
			{Name: "mode", Label: "Mode", Type: spec.FieldSelect, Default: "a", Options: []string{"a", "b"}},
			{Name: "password", Label: "Password", Type: spec.FieldSecret, Required: true},
			{Name: "ca_cert", Label: "CA", Type: spec.FieldPEM},
		},
		SetupScript: func(v spec.Values) string { return "grant to " + v.Get("username") },
		Test: func(_ context.Context, v spec.Values) []spec.Check {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.seen = append(f.seen, v)
			if f.checks != nil {
				return f.checks
			}
			return []spec.Check{{ID: "reach", Title: "Can reach", Status: spec.CheckPass}}
		},
		Build: func(context.Context, spec.Values) (any, func(), error) {
			if f.buildErr != nil {
				return nil, nil, f.buildErr
			}
			return "client", func() {}, nil
		},
	}
}

func (f *fakeKind) last() spec.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.seen) == 0 {
		return spec.Values{}
	}
	return f.seen[len(f.seen)-1]
}

func fakeSecretManagerKind() spec.Kind {
	return spec.Kind{
		ID: "secrets-fake", Category: spec.CategorySecretManager, Title: "Fake vault",
		Fields: []spec.Field{{Name: "name", Label: "Name", Type: spec.FieldText, Required: true, Pattern: spec.ProviderNamePattern}},
		Test:   func(context.Context, spec.Values) []spec.Check { return []spec.Check{} },
		Build:  func(context.Context, spec.Values) (any, func(), error) { return nil, func() {}, nil },
	}
}

type fakeApplier struct {
	mu      sync.Mutex
	applied []spec.Values
	cleared []string
	err     error
}

func (a *fakeApplier) Apply(_ context.Context, rec spec.Integration, _ spec.Kind, v spec.Values) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.applied = append(a.applied, v)
	return nil
}

func (a *fakeApplier) Clear(_ context.Context, rec spec.Integration) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cleared = append(a.cleared, rec.ID)
	return nil
}

type fakeNotifier struct {
	mu      sync.Mutex
	entries []spec.AuditEntry
	titles  []string
}

func (n *fakeNotifier) IntegrationChanged(_ context.Context, _ string, e spec.AuditEntry, title string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.entries = append(n.entries, e)
	n.titles = append(n.titles, title)
}

type fakeResolverSecrets struct {
	vals map[string]string // ref → value
	ws   []string
}

func (r *fakeResolverSecrets) Resolve(_ context.Context, ws, ref string) (netsec.Secret, error) {
	r.ws = append(r.ws, ws)
	v, ok := r.vals[ref]
	if !ok {
		return "", errors.New("secret manager: 404 for " + ref)
	}
	return netsec.Secret(v), nil
}

type fakeBrowser struct {
	names []string
	err   error
}

func (b *fakeBrowser) Names(_ context.Context, _ spec.Integration, _ string, limit int) ([]string, error) {
	return b.names, b.err
}

func (b *fakeBrowser) Keys(context.Context, spec.Integration, string) ([]string, error) {
	return []string{"password", "username"}, b.err
}

func slogTo(w io.Writer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }
