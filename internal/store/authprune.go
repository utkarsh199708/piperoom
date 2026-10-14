package store

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/auth"
)

// pruneTimeout bounds one auth-table prune statement.
const pruneTimeout = 30 * time.Second

// The prune statements live here, not inline, so the EXPLAIN check in
// authprune_index_integration_test.go runs exactly the SQL the code runs.
// Their indexes: sessions_expires_at_idx and sessions_revoked_at_idx
// (migration 00010) and login_states_expires_idx (migration 00007).
const (
	pruneSessionsSQL    = `DELETE FROM sessions WHERE expires_at < $1 OR (revoked_at IS NOT NULL AND revoked_at < $1)`
	pruneLoginStatesSQL = `DELETE FROM login_states WHERE expires_at <= $1`
)

var (
	_ auth.SessionPruner    = (*Sessions)(nil)
	_ auth.LoginStatePruner = (*LoginStates)(nil)
)

// PruneSessions implements auth.SessionPruner: one DELETE of the rows revoked,
// or past expires_at, more than grace before now.
func (t *Sessions) PruneSessions(ctx context.Context, now time.Time, grace time.Duration) (int64, error) {
	cutoff := now.UTC().Add(-grace)
	ctx, cancel := context.WithTimeout(ctx, pruneTimeout)
	defer cancel()
	tag, err := t.pool.Exec(ctx, pruneSessionsSQL, cutoff)
	if err != nil {
		return 0, fmt.Errorf("store: prune sessions: %w", err)
	}
	return tag.RowsAffected(), nil
}

// PruneLoginStates implements auth.LoginStatePruner. The condition matches the
// one Create uses (expires_at <= now), and login_states_expires_idx serves it.
func (t *LoginStates) PruneLoginStates(ctx context.Context, now time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, pruneTimeout)
	defer cancel()
	tag, err := t.pool.Exec(ctx, pruneLoginStatesSQL, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: prune login states: %w", err)
	}
	return tag.RowsAffected(), nil
}
