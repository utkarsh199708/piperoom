package auth

import (
	"context"
	"time"
)

// SessionGrace is how long a revoked or expired session row is kept before the
// retention job deletes it. The row is useless for login after revoke or
// expiry; the week lets an operator look at a recent logout or expiry.
const SessionGrace = 7 * 24 * time.Hour

// SessionPruner is implemented by a Store that can delete dead sessions. It is
// separate from Store so a fake store need not have it.
type SessionPruner interface {
	// PruneSessions deletes sessions that were revoked, or reached their
	// absolute expiry, more than grace before now. Live sessions and recently
	// dead ones stay. It returns the number of rows deleted.
	PruneSessions(ctx context.Context, now time.Time, grace time.Duration) (int64, error)
}

// LoginStatePruner is implemented by a LoginStateStore that can delete expired
// rows. Create already prunes, but only when someone logs in; the retention job
// uses this so an idle server does not keep old rows.
type LoginStatePruner interface {
	// PruneLoginStates deletes every row whose expires_at is at or before now.
	// It returns the number of rows deleted.
	PruneLoginStates(ctx context.Context, now time.Time) (int64, error)
}

var (
	_ SessionPruner    = (*MemStore)(nil)
	_ LoginStatePruner = (*MemLoginStates)(nil)
)

// PruneSessions implements SessionPruner.
func (m *MemStore) PruneSessions(_ context.Context, now time.Time, grace time.Duration) (int64, error) {
	cutoff := now.Add(-grace)
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for id, s := range m.byID {
		if s.ExpiresAt.Before(cutoff) || (!s.RevokedAt.IsZero() && s.RevokedAt.Before(cutoff)) {
			delete(m.byTH, string(s.TokenHash))
			delete(m.byID, id)
			n++
		}
	}
	return n, nil
}

// PruneLoginStates implements LoginStatePruner.
func (m *MemLoginStates) PruneLoginStates(_ context.Context, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for id, r := range m.rows {
		if !now.Before(r.ExpiresAt) {
			delete(m.rows, id)
			n++
		}
	}
	return n, nil
}
