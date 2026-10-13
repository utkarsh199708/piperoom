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

const sessionCols = `id, token_hash, user_id, name, email, iss, sub, groups, groups_missing, auth_time,
 amr, acr, method, created_at, last_seen_at, expires_at, revoked_at`

// Create implements auth.Store.
func (t *Sessions) Create(ctx context.Context, s auth.Session) error {
	if err := CheckText(s.ID, s.UserID, s.Name, s.Email, s.Iss, s.Sub, s.ACR, s.Method); err != nil {
		return fmt.Errorf("store: session create: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	_, err := t.pool.Exec(ctx, `
INSERT INTO sessions (id, token_hash, user_id, name, email, iss, sub, groups, groups_missing, auth_time,
                      amr, acr, method, created_at, last_seen_at, expires_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
		s.ID, s.TokenHash, s.UserID, s.Name, s.Email, s.Iss, s.Sub, nonNil(s.Groups), s.GroupsMissing, nullTime(s.AuthTime),
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

// LiveByUserID implements auth.Store. idle is added to last_seen_at in SQL with
// make_interval so the same rule as auth.Session.Check applies.
func (t *Sessions) LiveByUserID(ctx context.Context, userID string, now time.Time, idle time.Duration) (auth.Session, error) {
	return t.one(ctx, `user_id = $1 AND revoked_at IS NULL AND expires_at > $2
 AND ($3::float8 <= 0 OR last_seen_at + make_interval(secs => $3::float8) > $2)
 ORDER BY created_at DESC LIMIT 1`, userID, now.UTC(), idle.Seconds())
}

// LiveSessions implements auth.Store: the newest live session of each user,
// ordered by user id, with the same liveness rule as LiveByUserID.
func (t *Sessions) LiveSessions(ctx context.Context, now time.Time, idle time.Duration, limit int) ([]auth.Session, error) {
	if limit <= 0 {
		limit = 1000
	}
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	rows, err := t.pool.Query(ctx, `SELECT DISTINCT ON (user_id) `+sessionCols+` FROM sessions
 WHERE revoked_at IS NULL AND expires_at > $1
 AND ($2::float8 <= 0 OR last_seen_at + make_interval(secs => $2::float8) > $1)
 ORDER BY user_id, created_at DESC LIMIT $3`, now.UTC(), idle.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: live sessions: %w", err)
	}
	defer rows.Close()
	var out []auth.Session
	for rows.Next() {
		s, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("store: live sessions: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: live sessions: %w", err)
	}
	return out, nil
}

// one reads the single session matching where (a fixed string, never client
// text) with bound arguments.
func (t *Sessions) one(ctx context.Context, where string, args ...any) (auth.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	s, err := scanSession(t.pool.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE `+where, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.Session{}, auth.ErrNotFound
	}
	if err != nil {
		return auth.Session{}, fmt.Errorf("store: session get: %w", err)
	}
	return s, nil
}

// scanSession reads one row of sessionCols.
func scanSession(row pgx.Row) (auth.Session, error) {
	var (
		s                   auth.Session
		authTime, revokedAt *time.Time
	)
	err := row.Scan(
		&s.ID, &s.TokenHash, &s.UserID, &s.Name, &s.Email, &s.Iss, &s.Sub, &s.Groups, &s.GroupsMissing, &authTime,
		&s.AMR, &s.ACR, &s.Method, &s.CreatedAt, &s.LastSeenAt, &s.ExpiresAt, &revokedAt)
	if err != nil {
		return auth.Session{}, err
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
