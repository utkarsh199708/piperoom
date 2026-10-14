package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/integrations/spec"
)

// pgUniqueViolation is the SQLSTATE of unique_violation: the primary key
// (workspace_id, id) or one of the UniqueKey partial indexes clashed.
const pgUniqueViolation = "23505"

// defaultAuditLimit is ListAudit's limit when the caller passes <= 0.
const defaultAuditLimit = 100

// Integrations is the Postgres spec.Store over the integrations and
// integration_audit tables (migrations/00010_integrations.sql). Every query is
// filtered by workspace_id, so one workspace never sees another's rows.
type Integrations struct {
	pool *pgxpool.Pool
}

var _ spec.Store = (*Integrations)(nil)

// NewIntegrations returns the integrations store over pool.
func NewIntegrations(pool *pgxpool.Pool) *Integrations { return &Integrations{pool: pool} }

// Integrations returns the integrations store on this store's pool.
func (s *PG) Integrations() *Integrations { return NewIntegrations(s.pool) }

// integrationCols is the column list every read selects, in scanIntegration order.
const integrationCols = `workspace_id, id, category, kind, scope, config, secrets, key_id, secrets_set,
	version, enabled, last_test, last_test_at, updated_by, updated_at, source`

// CheckIntegration is the input check both integration stores (this one and
// memstore) run before a write, so they refuse the same records. It checks
// shape only; secrets and field values are the service's job.
func CheckIntegration(rec spec.Integration) error {
	if rec.WorkspaceID == "" || rec.ID == "" {
		return errors.New("store: integration: workspace id and id are required")
	}
	if !rec.Category.Valid() {
		return fmt.Errorf("store: integration: unknown category %q", rec.Category)
	}
	if rec.Kind == "" {
		return errors.New("store: integration: kind is required")
	}
	if rec.UpdatedBy == "" {
		return errors.New("store: integration: updated_by is required")
	}
	fields := []string{rec.WorkspaceID, rec.ID, string(rec.Category), rec.Kind, rec.Scope, rec.KeyID, rec.UpdatedBy, rec.Source}
	fields = append(fields, rec.SecretsSet...)
	if err := CheckText(fields...); err != nil {
		return fmt.Errorf("store: integration: %w", err)
	}
	// The secret_manager unique index keys on a string name only; a name of
	// another JSON type would give different keys in Go (spec.UniqueKey) and
	// Postgres, so the stores refuse it.
	if rec.Category == spec.CategorySecretManager {
		if v, ok := rec.Config[spec.ProviderNameField]; ok {
			if _, isStr := v.(string); !isStr {
				return fmt.Errorf("store: integration: secret manager %s must be a string", spec.ProviderNameField)
			}
		}
	}
	if _, err := encodeChecks(rec.LastTest); err != nil {
		return err
	}
	return nil
}

// CheckChecks is the check both stores run on a test result before saving it
// (SaveLastTest): it must encode to JSON that Postgres jsonb accepts.
func CheckChecks(checks []spec.Check) error {
	_, err := encodeChecks(checks)
	return err
}

// CheckAuditEntry is the input check both stores run before AppendAudit.
func CheckAuditEntry(e spec.AuditEntry) error {
	if e.WorkspaceID == "" || e.IntegrationID == "" || e.Action == "" || e.Actor == "" {
		return errors.New("store: integration audit: workspace id, integration id, action and actor are required")
	}
	switch e.Action {
	case spec.AuditCreated, spec.AuditUpdated, spec.AuditTested, spec.AuditEnabled,
		spec.AuditDisabled, spec.AuditDeleted, spec.AuditSecretRotated:
	default:
		return fmt.Errorf("store: integration audit: unknown action %q", e.Action)
	}
	fields := append([]string{e.WorkspaceID, e.IntegrationID, string(e.Action), e.Actor}, e.Changed...)
	if err := CheckText(fields...); err != nil {
		return fmt.Errorf("store: integration audit: %w", err)
	}
	return nil
}

// integrationDefaults fills the zero values the table defaults to, so both
// stores return the same record.
func integrationDefaults(rec spec.Integration) spec.Integration {
	if rec.Scope == "" {
		rec.Scope = spec.ScopeAll
	}
	if rec.Source == "" {
		rec.Source = spec.SourceDB
	}
	return rec
}

// encodeConfig marshals Config for the jsonb column; nil is {}.
func encodeConfig(cfg map[string]any) ([]byte, error) {
	if cfg == nil {
		cfg = map[string]any{}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("store: integration: encode config: %w", err)
	}
	if err := CheckJSON(raw); err != nil {
		return nil, fmt.Errorf("store: integration: config: %w", err)
	}
	return raw, nil
}

// encodeChecks marshals a test result; nil checks are stored as SQL NULL.
func encodeChecks(checks []spec.Check) ([]byte, error) {
	if checks == nil {
		return nil, nil
	}
	raw, err := json.Marshal(checks)
	if err != nil {
		return nil, fmt.Errorf("store: integration: encode checks: %w", err)
	}
	if err := CheckJSON(raw); err != nil {
		return nil, fmt.Errorf("store: integration: checks: %w", err)
	}
	return raw, nil
}

// sealedOrNil stores an empty ciphertext as NULL ("no secrets").
func sealedOrNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func scanIntegration(row pgx.Row) (spec.Integration, error) {
	var (
		rec        spec.Integration
		category   string
		cfgRaw     []byte
		testRaw    []byte
		lastTestAt *time.Time
	)
	if err := row.Scan(&rec.WorkspaceID, &rec.ID, &category, &rec.Kind, &rec.Scope, &cfgRaw, &rec.Sealed,
		&rec.KeyID, &rec.SecretsSet, &rec.Version, &rec.Enabled, &testRaw, &lastTestAt,
		&rec.UpdatedBy, &rec.UpdatedAt, &rec.Source); err != nil {
		return spec.Integration{}, err
	}
	rec.Category = spec.Category(category)
	rec.Config = map[string]any{}
	if err := json.Unmarshal(cfgRaw, &rec.Config); err != nil {
		return spec.Integration{}, fmt.Errorf("decode config of %s: %w", rec.ID, err)
	}
	if rec.Config == nil { // a JSON null in the column
		rec.Config = map[string]any{}
	}
	if len(testRaw) > 0 {
		if err := json.Unmarshal(testRaw, &rec.LastTest); err != nil {
			return spec.Integration{}, fmt.Errorf("decode last_test of %s: %w", rec.ID, err)
		}
	}
	if lastTestAt != nil {
		rec.LastTestAt = lastTestAt.UTC()
	}
	rec.UpdatedAt = rec.UpdatedAt.UTC()
	rec.SecretsSet = nonNil(rec.SecretsSet)
	if len(rec.Sealed) == 0 {
		rec.Sealed = nil
	}
	return rec, nil
}

// isUniqueViolation reports a unique_violation from Postgres.
func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == pgUniqueViolation
}

// List implements spec.Store.
func (t *Integrations) List(ctx context.Context, workspaceID string) ([]spec.Integration, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := t.pool.Query(ctx,
		`SELECT `+integrationCols+` FROM integrations WHERE workspace_id = $1 ORDER BY category, id`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("store: list integrations: %w", err)
	}
	defer rows.Close()
	out := []spec.Integration{}
	for rows.Next() {
		rec, err := scanIntegration(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list integrations: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list integrations: %w", err)
	}
	return out, nil
}

// Get implements spec.Store.
func (t *Integrations) Get(ctx context.Context, workspaceID, id string) (spec.Integration, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rec, err := scanIntegration(t.pool.QueryRow(ctx,
		`SELECT `+integrationCols+` FROM integrations WHERE workspace_id = $1 AND id = $2`, workspaceID, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return spec.Integration{}, fmt.Errorf("store: get integration %s: %w", id, spec.ErrNotFound)
	}
	if err != nil {
		return spec.Integration{}, fmt.Errorf("store: get integration %s: %w", id, err)
	}
	return rec, nil
}

// Create implements spec.Store.
func (t *Integrations) Create(ctx context.Context, rec spec.Integration) (spec.Integration, error) {
	if err := CheckIntegration(rec); err != nil {
		return spec.Integration{}, err
	}
	rec = integrationDefaults(rec)
	cfg, err := encodeConfig(rec.Config)
	if err != nil {
		return spec.Integration{}, err
	}
	checks, err := encodeChecks(rec.LastTest)
	if err != nil {
		return spec.Integration{}, err
	}
	var lastTestAt *time.Time
	if !rec.LastTestAt.IsZero() {
		at := rec.LastTestAt
		lastTestAt = &at
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	out, err := scanIntegration(t.pool.QueryRow(ctx, `
INSERT INTO integrations (workspace_id, id, category, kind, scope, config, secrets, key_id, secrets_set,
	version, enabled, last_test, last_test_at, updated_by, updated_at, source)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11, $12, $13, now(), $14)
RETURNING `+integrationCols,
		rec.WorkspaceID, rec.ID, string(rec.Category), rec.Kind, rec.Scope, cfg, sealedOrNil(rec.Sealed),
		rec.KeyID, nonNil(rec.SecretsSet), rec.Enabled, checks, lastTestAt, rec.UpdatedBy, rec.Source))
	if isUniqueViolation(err) {
		return spec.Integration{}, fmt.Errorf("store: create integration %s: %w", rec.ID, spec.ErrDuplicate)
	}
	if err != nil {
		return spec.Integration{}, fmt.Errorf("store: create integration %s: %w", rec.ID, err)
	}
	return out, nil
}

// Update implements spec.Store. The version check and the write are one
// statement, so two concurrent saves with the same If-Match cannot both win.
func (t *Integrations) Update(ctx context.Context, rec spec.Integration, ifVersion int64) (spec.Integration, error) {
	if err := CheckIntegration(rec); err != nil {
		return spec.Integration{}, err
	}
	cfg, err := encodeConfig(rec.Config)
	if err != nil {
		return spec.Integration{}, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	out, err := scanIntegration(t.pool.QueryRow(ctx, `
UPDATE integrations
SET config = $4, secrets = $5, key_id = $6, secrets_set = $7, enabled = $8, updated_by = $9,
	version = version + 1, updated_at = now()
WHERE workspace_id = $1 AND id = $2 AND version = $3
RETURNING `+integrationCols,
		rec.WorkspaceID, rec.ID, ifVersion, cfg, sealedOrNil(rec.Sealed), rec.KeyID, nonNil(rec.SecretsSet),
		rec.Enabled, rec.UpdatedBy))
	switch {
	case err == nil:
		return out, nil
	case isUniqueViolation(err):
		return spec.Integration{}, fmt.Errorf("store: update integration %s: %w", rec.ID, spec.ErrDuplicate)
	case errors.Is(err, pgx.ErrNoRows):
		return spec.Integration{}, t.missOrConflict(ctx, "update", rec.WorkspaceID, rec.ID)
	default:
		return spec.Integration{}, fmt.Errorf("store: update integration %s: %w", rec.ID, err)
	}
}

// missOrConflict tells apart why a versioned UPDATE matched no row.
func (t *Integrations) missOrConflict(ctx context.Context, op, workspaceID, id string) error {
	var exists bool
	if err := t.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM integrations WHERE workspace_id = $1 AND id = $2)`, workspaceID, id,
	).Scan(&exists); err != nil {
		return fmt.Errorf("store: %s integration %s: check existence: %w", op, id, err)
	}
	if !exists {
		return fmt.Errorf("store: %s integration %s: %w", op, id, spec.ErrNotFound)
	}
	return fmt.Errorf("store: %s integration %s: %w", op, id, spec.ErrVersionConflict)
}

// Delete implements spec.Store. Audit rows stay.
func (t *Integrations) Delete(ctx context.Context, workspaceID, id string) error {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := t.pool.Exec(ctx, `DELETE FROM integrations WHERE workspace_id = $1 AND id = $2`, workspaceID, id)
	if err != nil {
		return fmt.Errorf("store: delete integration %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: delete integration %s: %w", id, spec.ErrNotFound)
	}
	return nil
}

// SetEnabled implements spec.Store.
func (t *Integrations) SetEnabled(ctx context.Context, workspaceID, id string, enabled bool, actor string) (spec.Integration, error) {
	if actor == "" {
		return spec.Integration{}, errors.New("store: set integration enabled: actor is required")
	}
	if err := CheckText(actor); err != nil {
		return spec.Integration{}, fmt.Errorf("store: set integration enabled: %w", err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	out, err := scanIntegration(t.pool.QueryRow(ctx, `
UPDATE integrations SET enabled = $3, updated_by = $4, version = version + 1, updated_at = now()
WHERE workspace_id = $1 AND id = $2
RETURNING `+integrationCols, workspaceID, id, enabled, actor))
	if errors.Is(err, pgx.ErrNoRows) {
		return spec.Integration{}, fmt.Errorf("store: set integration enabled %s: %w", id, spec.ErrNotFound)
	}
	if err != nil {
		return spec.Integration{}, fmt.Errorf("store: set integration enabled %s: %w", id, err)
	}
	return out, nil
}

// SaveLastTest implements spec.Store. It leaves version and updated_at alone.
func (t *Integrations) SaveLastTest(ctx context.Context, workspaceID, id string, checks []spec.Check) error {
	if checks == nil {
		checks = []spec.Check{} // a test ran: [] is "tested, no checks", NULL is "never tested"
	}
	raw, err := encodeChecks(checks)
	if err != nil {
		return err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := t.pool.Exec(ctx,
		`UPDATE integrations SET last_test = $3, last_test_at = now() WHERE workspace_id = $1 AND id = $2`,
		workspaceID, id, raw)
	if err != nil {
		return fmt.Errorf("store: save integration test %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: save integration test %s: %w", id, spec.ErrNotFound)
	}
	return nil
}

// AppendAudit implements spec.Store; Seq and At come from the database.
func (t *Integrations) AppendAudit(ctx context.Context, e spec.AuditEntry) error {
	if err := CheckAuditEntry(e); err != nil {
		return err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if _, err := t.pool.Exec(ctx, `
INSERT INTO integration_audit (workspace_id, integration, action, actor, changed)
VALUES ($1, $2, $3, $4, $5)`,
		e.WorkspaceID, e.IntegrationID, string(e.Action), e.Actor, nonNil(e.Changed)); err != nil {
		return fmt.Errorf("store: append integration audit %s: %w", e.IntegrationID, err)
	}
	return nil
}

// ListAudit implements spec.Store.
func (t *Integrations) ListAudit(ctx context.Context, workspaceID, integrationID string, limit int) ([]spec.AuditEntry, error) {
	if limit <= 0 {
		limit = defaultAuditLimit
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := t.pool.Query(ctx, `
SELECT seq, workspace_id, integration, action, actor, changed, at
FROM integration_audit
WHERE workspace_id = $1 AND integration = $2
ORDER BY seq DESC
LIMIT $3`, workspaceID, integrationID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list integration audit %s: %w", integrationID, err)
	}
	defer rows.Close()
	out := []spec.AuditEntry{}
	for rows.Next() {
		var (
			e      spec.AuditEntry
			action string
		)
		if err := rows.Scan(&e.Seq, &e.WorkspaceID, &e.IntegrationID, &action, &e.Actor, &e.Changed, &e.At); err != nil {
			return nil, fmt.Errorf("store: list integration audit %s: %w", integrationID, err)
		}
		e.Action = spec.AuditAction(action)
		e.Changed = nonNil(e.Changed)
		e.At = e.At.UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list integration audit %s: %w", integrationID, err)
	}
	return out, nil
}
