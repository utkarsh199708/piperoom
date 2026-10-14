package spec

import (
	"context"
	"errors"
)

// Store sentinel errors. Implementations wrap them; callers use errors.Is.
var (
	// ErrNotFound: no row with that id in that workspace (another
	// workspace's row is also ErrNotFound, never a different error).
	ErrNotFound = errors.New("integrations: not found")
	// ErrVersionConflict: Update's ifVersion is not the stored version.
	ErrVersionConflict = errors.New("integrations: version conflict")
	// ErrDuplicate: another row of the workspace has the same UniqueKey, or the id exists.
	ErrDuplicate = errors.New("integrations: duplicate")
)

// Store persists integrations and their audit log (A5's internal/store
// implements it over migrations/*_integrations.sql). Every method is scoped to
// one workspace: a row is never read, changed or listed across workspaces.
// It stores what it is given: sealing and stripping secrets is the caller's job.
type Store interface {
	// List returns the workspace's rows ordered by category, then id. [] when none.
	List(ctx context.Context, workspaceID string) ([]Integration, error)
	// Get returns one row, or ErrNotFound.
	Get(ctx context.Context, workspaceID, id string) (Integration, error)
	// Create inserts rec (rec.ID set by the caller) with Version 1 and
	// UpdatedAt now, and returns the stored row. ErrDuplicate on a UniqueKey clash.
	Create(ctx context.Context, rec Integration) (Integration, error)
	// Update replaces Config, Sealed, KeyID, SecretsSet, Enabled, UpdatedBy of
	// the row rec.WorkspaceID/rec.ID if its version is ifVersion, sets
	// Version = ifVersion+1 and UpdatedAt = now, and returns the stored row.
	// ErrNotFound, ErrVersionConflict, or ErrDuplicate.
	Update(ctx context.Context, rec Integration, ifVersion int64) (Integration, error)
	// Delete removes the row (its audit rows stay). ErrNotFound when absent.
	Delete(ctx context.Context, workspaceID, id string) error
	// SetEnabled sets Enabled, UpdatedBy = actor, Version+1, UpdatedAt = now. ErrNotFound.
	SetEnabled(ctx context.Context, workspaceID, id string, enabled bool, actor string) (Integration, error)
	// SaveLastTest stores the last test result and its time. It does not
	// change Version (a background re-test must not break an open edit). ErrNotFound.
	SaveLastTest(ctx context.Context, workspaceID, id string, checks []Check) error
	// AppendAudit adds one audit row; Seq and At are set by the store.
	AppendAudit(ctx context.Context, e AuditEntry) error
	// ListAudit returns the rows of one integration, newest first, at most
	// limit (limit <= 0 means 100). [] when none.
	ListAudit(ctx context.Context, workspaceID, integrationID string, limit int) ([]AuditEntry, error)
}
