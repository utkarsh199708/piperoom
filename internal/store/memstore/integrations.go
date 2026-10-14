package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"piperoom/internal/integrations/spec"
	"piperoom/internal/store"
)

// defaultAuditLimit is ListAudit's limit when the caller passes <= 0.
const defaultAuditLimit = 100

// Integrations is an in-memory spec.Store for unit tests and demo mode. It
// follows the Postgres store (internal/store.Integrations): the same input
// checks, the same uniqueness rules as the partial unique indexes in
// migrations/00010_integrations.sql, Config round-tripped through JSON (so
// numbers come back as float64, as from jsonb), and times at microsecond
// precision in UTC. Every record is copied in and out, so callers never share
// maps or slices with the store.
type Integrations struct {
	mu       sync.Mutex
	now      func() time.Time
	rows     map[intKey]spec.Integration
	audit    []spec.AuditEntry
	auditSeq int64
}

type intKey struct{ ws, id string }

var _ spec.Store = (*Integrations)(nil)

// NewIntegrations returns an empty in-memory integrations store.
func NewIntegrations() *Integrations {
	return NewIntegrationsWithClock(time.Now)
}

// NewIntegrationsWithClock is NewIntegrations with a clock, for tests that
// check UpdatedAt and LastTestAt.
func NewIntegrationsWithClock(now func() time.Time) *Integrations {
	return &Integrations{now: now, rows: map[intKey]spec.Integration{}}
}

func (m *Integrations) stamp() time.Time { return m.now().UTC().Truncate(time.Microsecond) }

// sameUniqueKey mirrors the three partial unique indexes of the integrations
// table: one row per category and scope, except alerts (per kind and scope)
// and secret managers (per provider name).
func sameUniqueKey(a, b spec.Integration) bool {
	if a.WorkspaceID != b.WorkspaceID || a.Category != b.Category {
		return false
	}
	switch a.Category {
	case spec.CategorySecretManager:
		return spec.UniqueKey(a) == spec.UniqueKey(b)
	case spec.CategoryAlerts:
		return a.Kind == b.Kind && a.Scope == b.Scope
	default:
		return a.Scope == b.Scope
	}
}

// roundTripConfig copies cfg through JSON, as the jsonb column does.
func roundTripConfig(cfg map[string]any) (map[string]any, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("memstore: integration: encode config: %w", err)
	}
	if err := store.CheckJSON(raw); err != nil {
		return nil, fmt.Errorf("memstore: integration: config: %w", err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("memstore: integration: decode config: %w", err)
	}
	return out, nil
}

// copyIntegration returns a deep copy of rec (Config is copied by the caller
// via roundTripConfig on writes, and here on reads).
func copyIntegration(rec spec.Integration) spec.Integration {
	out := rec
	out.Config, _ = roundTripConfig(rec.Config) // stored configs always encode
	if rec.Sealed != nil {
		out.Sealed = append([]byte(nil), rec.Sealed...)
	}
	out.SecretsSet = append([]string{}, rec.SecretsSet...)
	if rec.LastTest != nil {
		out.LastTest = append([]spec.Check{}, rec.LastTest...)
	}
	return out
}

func copyAudit(e spec.AuditEntry) spec.AuditEntry {
	e.Changed = append([]string{}, e.Changed...)
	return e
}

// clash reports whether rec's UniqueKey is taken by another row.
func (m *Integrations) clash(rec spec.Integration) bool {
	for k, other := range m.rows {
		if k.id == rec.ID && k.ws == rec.WorkspaceID {
			continue
		}
		if sameUniqueKey(rec, other) {
			return true
		}
	}
	return false
}

// List implements spec.Store.
func (m *Integrations) List(_ context.Context, workspaceID string) ([]spec.Integration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []spec.Integration{}
	for k, rec := range m.rows {
		if k.ws == workspaceID {
			out = append(out, copyIntegration(rec))
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

// Get implements spec.Store.
func (m *Integrations) Get(_ context.Context, workspaceID, id string) (spec.Integration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.rows[intKey{workspaceID, id}]
	if !ok {
		return spec.Integration{}, fmt.Errorf("memstore: get integration %s: %w", id, spec.ErrNotFound)
	}
	return copyIntegration(rec), nil
}

// Create implements spec.Store.
func (m *Integrations) Create(_ context.Context, rec spec.Integration) (spec.Integration, error) {
	if err := store.CheckIntegration(rec); err != nil {
		return spec.Integration{}, err
	}
	cfg, err := roundTripConfig(rec.Config)
	if err != nil {
		return spec.Integration{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec.Scope == "" {
		rec.Scope = spec.ScopeAll
	}
	if rec.Source == "" {
		rec.Source = spec.SourceDB
	}
	if _, ok := m.rows[intKey{rec.WorkspaceID, rec.ID}]; ok || m.clash(rec) {
		return spec.Integration{}, fmt.Errorf("memstore: create integration %s: %w", rec.ID, spec.ErrDuplicate)
	}
	rec = copyIntegration(rec)
	rec.Config = cfg
	if len(rec.Sealed) == 0 {
		rec.Sealed = nil
	}
	rec.Version = 1
	rec.UpdatedAt = m.stamp()
	if !rec.LastTestAt.IsZero() {
		rec.LastTestAt = rec.LastTestAt.UTC().Truncate(time.Microsecond)
	}
	m.rows[intKey{rec.WorkspaceID, rec.ID}] = rec
	return copyIntegration(rec), nil
}

// Update implements spec.Store.
func (m *Integrations) Update(_ context.Context, rec spec.Integration, ifVersion int64) (spec.Integration, error) {
	if err := store.CheckIntegration(rec); err != nil {
		return spec.Integration{}, err
	}
	cfg, err := roundTripConfig(rec.Config)
	if err != nil {
		return spec.Integration{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := intKey{rec.WorkspaceID, rec.ID}
	cur, ok := m.rows[k]
	if !ok {
		return spec.Integration{}, fmt.Errorf("memstore: update integration %s: %w", rec.ID, spec.ErrNotFound)
	}
	if cur.Version != ifVersion {
		return spec.Integration{}, fmt.Errorf("memstore: update integration %s: %w", rec.ID, spec.ErrVersionConflict)
	}
	next := cur
	next.Config = cfg
	next.Sealed = nil
	if len(rec.Sealed) > 0 {
		next.Sealed = append([]byte(nil), rec.Sealed...)
	}
	next.KeyID = rec.KeyID
	next.SecretsSet = append([]string{}, rec.SecretsSet...)
	next.Enabled = rec.Enabled
	next.UpdatedBy = rec.UpdatedBy
	if m.clash(next) {
		return spec.Integration{}, fmt.Errorf("memstore: update integration %s: %w", rec.ID, spec.ErrDuplicate)
	}
	next.Version = ifVersion + 1
	next.UpdatedAt = m.stamp()
	m.rows[k] = next
	return copyIntegration(next), nil
}

// Delete implements spec.Store. Audit rows stay.
func (m *Integrations) Delete(_ context.Context, workspaceID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := intKey{workspaceID, id}
	if _, ok := m.rows[k]; !ok {
		return fmt.Errorf("memstore: delete integration %s: %w", id, spec.ErrNotFound)
	}
	delete(m.rows, k)
	return nil
}

// SetEnabled implements spec.Store.
func (m *Integrations) SetEnabled(_ context.Context, workspaceID, id string, enabled bool, actor string) (spec.Integration, error) {
	if actor == "" {
		return spec.Integration{}, errors.New("memstore: set integration enabled: actor is required")
	}
	if err := store.CheckText(actor); err != nil {
		return spec.Integration{}, fmt.Errorf("memstore: set integration enabled: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := intKey{workspaceID, id}
	cur, ok := m.rows[k]
	if !ok {
		return spec.Integration{}, fmt.Errorf("memstore: set integration enabled %s: %w", id, spec.ErrNotFound)
	}
	cur.Enabled = enabled
	cur.UpdatedBy = actor
	cur.Version++
	cur.UpdatedAt = m.stamp()
	m.rows[k] = cur
	return copyIntegration(cur), nil
}

// SaveLastTest implements spec.Store. Version and UpdatedAt are unchanged.
func (m *Integrations) SaveLastTest(_ context.Context, workspaceID, id string, checks []spec.Check) error {
	if err := store.CheckChecks(checks); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := intKey{workspaceID, id}
	cur, ok := m.rows[k]
	if !ok {
		return fmt.Errorf("memstore: save integration test %s: %w", id, spec.ErrNotFound)
	}
	cur.LastTest = append([]spec.Check{}, checks...) // a test ran: never nil
	cur.LastTestAt = m.stamp()
	m.rows[k] = cur
	return nil
}

// AppendAudit implements spec.Store; Seq and At are set here.
func (m *Integrations) AppendAudit(_ context.Context, e spec.AuditEntry) error {
	if err := store.CheckAuditEntry(e); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.auditSeq++
	e = copyAudit(e)
	e.Seq = m.auditSeq
	e.At = m.stamp()
	m.audit = append(m.audit, e)
	return nil
}

// ListAudit implements spec.Store: newest first, at most limit (<= 0 means 100).
func (m *Integrations) ListAudit(_ context.Context, workspaceID, integrationID string, limit int) ([]spec.AuditEntry, error) {
	if limit <= 0 {
		limit = defaultAuditLimit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []spec.AuditEntry{}
	for i := len(m.audit) - 1; i >= 0 && len(out) < limit; i-- {
		e := m.audit[i]
		if e.WorkspaceID == workspaceID && e.IntegrationID == integrationID {
			out = append(out, copyAudit(e))
		}
	}
	return out, nil
}
