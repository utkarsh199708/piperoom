package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/auth"
)

// sessionTimeout bounds each sessions statement: every authenticated request
// reads one, so a slow database must not hang a request for long.
const sessionTimeout = 5 * time.Second

// Sessions is the Postgres auth.Store (table sessions, migrations/00006).
type Sessions struct{ pool *pgxpool.Pool }

var _ auth.Store = (*Sessions)(nil)

// NewSessions returns a Sessions over pool.
func NewSessions(pool *pgxpool.Pool) *Sessions { return &Sessions{pool: pool} }

// Sessions returns the sessions table on this store's pool.
func (s *PG) Sessions() *Sessions { return NewSessions(s.pool) }

const sessionCols = `id, token_hash, user_id, name, iss, sub, groups, groups_missing, auth_time,
 amr, acr, method, created_at, last_seen_at, expires_at, revoked_at`

// Create implements auth.Store.
func (t *Sessions) Create(ctx context.Context, s auth.Session) error {
	if err := CheckText(s.ID, s.UserID, s.Name, s.Iss, s.Sub, s.ACR, s.Method); err != nil {
		return fmt.Errorf("store: session create: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	_, err := t.pool.Exec(ctx, `
INSERT INTO sessions (id, token_hash, user_id, name, iss, sub, groups, groups_missing, auth_time,
                      amr, acr, method, created_at, last_seen_at, expires_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		s.ID, s.TokenHash, s.UserID, s.Name, s.Iss, s.Sub, nonNil(s.Groups), s.GroupsMissing, nullTime(s.AuthTime),
		nonNil(s.AMR), s.ACR, s.Method, s.CreatedAt.UTC(), s.LastSeenAt.UTC(), s.ExpiresAt.UTC(), nullTime(s.RevokedAt))
	if err != nil {
		return fmt.Errorf("store: session create: %w", err)
	}
	return nil
}

// ByTokenHash implements auth.Store.
func (t *Sessions) ByTokenHash(ctx context.Context, hash []byte) (auth.Session, error) {
	return t.one(ctx, `token_hash = $1`, hash)
}

// ByID implements auth.Store.
func (t *Sessions) ByID(ctx context.Context, id string) (auth.Session, error) {
	return t.one(ctx, `id = $1`, id)
}

// one reads the single session matching where (a fixed string, never client
// text) with one bound argument.
func (t *Sessions) one(ctx context.Context, where string, arg any) (auth.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	var (
		s                   auth.Session
		authTime, revokedAt *time.Time
	)
	err := t.pool.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE `+where, arg).Scan(
		&s.ID, &s.TokenHash, &s.UserID, &s.Name, &s.Iss, &s.Sub, &s.Groups, &s.GroupsMissing, &authTime,
		&s.AMR, &s.ACR, &s.Method, &s.CreatedAt, &s.LastSeenAt, &s.ExpiresAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.Session{}, auth.ErrNotFound
	}
	if err != nil {
		return auth.Session{}, fmt.Errorf("store: session get: %w", err)
	}
	s.CreatedAt, s.LastSeenAt, s.ExpiresAt = s.CreatedAt.UTC(), s.LastSeenAt.UTC(), s.ExpiresAt.UTC()
	if authTime != nil {
		s.AuthTime = authTime.UTC()
	}
	if revokedAt != nil {
		s.RevokedAt = revokedAt.UTC()
	}
	return s, nil
}

// Touch implements auth.Store.
func (t *Sessions) Touch(ctx context.Context, id string, at time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	if _, err := t.pool.Exec(ctx, `UPDATE sessions SET last_seen_at = $2 WHERE id = $1`, id, at.UTC()); err != nil {
		return fmt.Errorf("store: session touch: %w", err)
	}
	return nil
}

// Revoke implements auth.Store.
func (t *Sessions) Revoke(ctx context.Context, id string, at time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	if _, err := t.pool.Exec(ctx, `UPDATE sessions SET revoked_at = $2 WHERE id = $1 AND revoked_at IS NULL`, id, at.UTC()); err != nil {
		return fmt.Errorf("store: session revoke: %w", err)
	}
	return nil
}

// nullTime maps the zero time to SQL NULL.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

// nonNil keeps a nil slice from becoming SQL NULL in a NOT NULL array column.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
