package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/auth"
)

// LoginStates is the Postgres auth.LoginStateStore (table login_states,
// migrations/00007).
type LoginStates struct{ pool *pgxpool.Pool }

var _ auth.LoginStateStore = (*LoginStates)(nil)

// LoginStates returns the login_states table on this store's pool.
func (s *PG) LoginStates() *LoginStates { return &LoginStates{pool: s.pool} }

// Create implements auth.LoginStateStore. The replace, the prune and the insert
// run in one transaction so a failed insert keeps the old row.
func (t *LoginStates) Create(ctx context.Context, ls auth.LoginState, replaces string, now time.Time) error {
	if err := CheckText(ls.ID, ls.State, ls.Nonce, ls.Verifier, ls.ReturnPath); err != nil {
		return fmt.Errorf("store: login state create: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: login state create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM login_states WHERE id = $1 OR expires_at <= $2`, replaces, now.UTC()); err != nil {
		return fmt.Errorf("store: login state prune: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO login_states (id, state, nonce, pkce_verifier, return_path, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		ls.ID, ls.State, ls.Nonce, ls.Verifier, ls.ReturnPath, now.UTC(), ls.ExpiresAt.UTC()); err != nil {
		return fmt.Errorf("store: login state insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: login state create: %w", err)
	}
	return nil
}

// Take implements auth.LoginStateStore: DELETE ... RETURNING, so the row is
// gone after the first callback whether or not the rest of the login succeeds.
func (t *LoginStates) Take(ctx context.Context, id string, now time.Time) (auth.LoginState, error) {
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	var ls auth.LoginState
	err := t.pool.QueryRow(ctx, `
DELETE FROM login_states WHERE id = $1 AND expires_at > $2
RETURNING id, state, nonce, pkce_verifier, return_path, expires_at`, id, now.UTC()).
		Scan(&ls.ID, &ls.State, &ls.Nonce, &ls.Verifier, &ls.ReturnPath, &ls.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// An expired row is still deleted on the next prune; leaving it is fine.
		return auth.LoginState{}, auth.ErrLoginStateNotFound
	}
	if err != nil {
		return auth.LoginState{}, fmt.Errorf("store: login state take: %w", err)
	}
	ls.ExpiresAt = ls.ExpiresAt.UTC()
	return ls, nil
}

// Identities is the Postgres auth.IdentityStore (table identities,
// migrations/00007).
type Identities struct{ pool *pgxpool.Pool }

var _ auth.IdentityStore = (*Identities)(nil)

// Identities returns the identities table on this store's pool.
func (s *PG) Identities() *Identities { return &Identities{pool: s.pool} }

// UserFor implements auth.IdentityStore.
func (t *Identities) UserFor(ctx context.Context, iss, sub string) (string, error) {
	if err := CheckText(iss, sub); err != nil {
		return "", auth.ErrIdentityNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	var user string
	err := t.pool.QueryRow(ctx, `SELECT user_id FROM identities WHERE iss = $1 AND sub = $2`, iss, sub).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", auth.ErrIdentityNotFound
	}
	if err != nil {
		return "", fmt.Errorf("store: identity get: %w", err)
	}
	return user, nil
}

// Bind implements auth.IdentityStore. Both unique keys (the (iss, sub) primary
// key and user_id) decide a conflict in one statement, so two first logins of
// the same person racing cannot bind two pairs.
func (t *Identities) Bind(ctx context.Context, iss, sub, userID, email string) error {
	if err := CheckText(iss, sub, userID, email); err != nil {
		return fmt.Errorf("store: identity bind: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	// The same pair for the same user is a no-op; anything else that hits a
	// unique key is a conflict.
	tag, err := t.pool.Exec(ctx, `
INSERT INTO identities (iss, sub, user_id, email) VALUES ($1, $2, $3, $4)
ON CONFLICT (iss, sub) DO UPDATE SET email = identities.email WHERE identities.user_id = EXCLUDED.user_id`,
		iss, sub, userID, email)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" { // unique_violation: user_id already bound elsewhere
			return auth.ErrIdentityConflict
		}
		return fmt.Errorf("store: identity bind: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// (iss, sub) exists and belongs to another user.
		return auth.ErrIdentityConflict
	}
	return nil
}

// Unbind implements auth.IdentityStore.
func (t *Identities) Unbind(ctx context.Context, userID string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()
	tag, err := t.pool.Exec(ctx, `DELETE FROM identities WHERE user_id = $1`, userID)
	if err != nil {
		return 0, fmt.Errorf("store: identity unbind: %w", err)
	}
	return tag.RowsAffected(), nil
}
