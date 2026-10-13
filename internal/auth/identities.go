package auth

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Errors returned by an IdentityStore.
var (
	// ErrIdentityNotFound: no row for this (iss, sub).
	ErrIdentityNotFound = errors.New("auth: identity not found")
	// ErrIdentityConflict: the user is already bound to another (iss, sub), or
	// this (iss, sub) is bound to another user. A recycled email lands here.
	ErrIdentityConflict = errors.New("auth: identity is bound to another account")
)

// IdentityStore binds an IdP account (iss, sub) to a users.yaml user id
// (design §2.1). The Postgres store and MemIdentities implement it.
type IdentityStore interface {
	// UserFor returns the user bound to (iss, sub), or ErrIdentityNotFound.
	UserFor(ctx context.Context, iss, sub string) (userID string, err error)
	// Bind records (iss, sub) -> userID with the email that matched. Binding the
	// same pair to the same user again is fine. A user that already has another
	// pair, or a pair that belongs to another user, is ErrIdentityConflict.
	Bind(ctx context.Context, iss, sub, userID, email string) error
	// Unbind deletes the user's row and, in the same step, revokes (at) every
	// live session that belongs to the user or to the removed (iss, sub): the
	// account that was just unbound must not keep acting as the user until its
	// session expires (security audit A-ID-5). The result says how many rows
	// and sessions it touched.
	Unbind(ctx context.Context, userID string, at time.Time) (UnbindResult, error)
}

// UnbindResult is what IdentityStore.Unbind did.
type UnbindResult struct {
	// Unbound is the number of identities rows deleted (0 or 1).
	Unbound int64
	// Revoked is the number of live sessions revoked.
	Revoked int64
}

// SessionRevoker revokes sessions by who they belong to. MemStore implements
// it; the Postgres store does it inside Identities.Unbind's transaction.
type SessionRevoker interface {
	// RevokeBinding revokes every live session with user_id = userID, or with
	// iss and sub both equal to the given non-empty pair, and returns how many.
	RevokeBinding(ctx context.Context, userID, iss, sub string, at time.Time) (int64, error)
}

// MemIdentities is the in-memory IdentityStore, for tests and --demo.
type MemIdentities struct {
	mu     sync.Mutex
	byPair map[[2]string]string // (iss, sub) -> user id
	byUser map[string][2]string
	// sessions, when set, is revoked by Unbind like the Postgres store does.
	sessions SessionRevoker
}

var _ IdentityStore = (*MemIdentities)(nil)

// NewMemIdentities returns an empty MemIdentities.
func NewMemIdentities() *MemIdentities {
	return &MemIdentities{byPair: map[[2]string]string{}, byUser: map[string][2]string{}}
}

// WithSessions makes Unbind revoke sessions in r, and returns m.
func (m *MemIdentities) WithSessions(r SessionRevoker) *MemIdentities {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions = r
	return m
}

// UserFor implements IdentityStore.
func (m *MemIdentities) UserFor(_ context.Context, iss, sub string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok := m.byPair[[2]string{iss, sub}]; ok {
		return u, nil
	}
	return "", ErrIdentityNotFound
}

// Bind implements IdentityStore.
func (m *MemIdentities) Bind(_ context.Context, iss, sub, userID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	pair := [2]string{iss, sub}
	if u, ok := m.byPair[pair]; ok {
		if u == userID {
			return nil
		}
		return ErrIdentityConflict
	}
	if _, ok := m.byUser[userID]; ok {
		return ErrIdentityConflict
	}
	m.byPair[pair] = userID
	m.byUser[userID] = pair
	return nil
}

// Unbind implements IdentityStore.
func (m *MemIdentities) Unbind(ctx context.Context, userID string, at time.Time) (UnbindResult, error) {
	m.mu.Lock()
	pair, ok := m.byUser[userID]
	if ok {
		delete(m.byUser, userID)
		delete(m.byPair, pair)
	}
	rev := m.sessions
	m.mu.Unlock()
	var res UnbindResult
	if ok {
		res.Unbound = 1
	}
	if rev != nil {
		n, err := rev.RevokeBinding(ctx, userID, pair[0], pair[1], at)
		if err != nil {
			return res, err
		}
		res.Revoked = n
	}
	return res, nil
}
