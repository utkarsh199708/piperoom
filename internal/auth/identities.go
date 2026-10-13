package auth

import (
	"context"
	"errors"
	"sync"
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
	// Unbind deletes the user's row and reports how many rows it deleted.
	Unbind(ctx context.Context, userID string) (int64, error)
}

// MemIdentities is the in-memory IdentityStore, for tests and --demo.
type MemIdentities struct {
	mu     sync.Mutex
	byPair map[[2]string]string // (iss, sub) -> user id
	byUser map[string][2]string
}

var _ IdentityStore = (*MemIdentities)(nil)

// NewMemIdentities returns an empty MemIdentities.
func NewMemIdentities() *MemIdentities {
	return &MemIdentities{byPair: map[[2]string]string{}, byUser: map[string][2]string{}}
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
func (m *MemIdentities) Unbind(_ context.Context, userID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pair, ok := m.byUser[userID]
	if !ok {
		return 0, nil
	}
	delete(m.byUser, userID)
	delete(m.byPair, pair)
	return 1, nil
}
