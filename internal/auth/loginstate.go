package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"
)

// LoginStateTTL is how long a login_states row lives (design §1.1).
const LoginStateTTL = 10 * time.Minute

// ErrLoginStateNotFound: no live row has this id (never created, already
// used, or expired).
var ErrLoginStateNotFound = errors.New("auth: login state not found")

// LoginState is one in-flight login: what /auth/login decided and the callback
// must find again. ID is the value of the pre-login cookie.
type LoginState struct {
	ID         string
	State      string // the OAuth state parameter
	Nonce      string // the OIDC nonce
	Verifier   string // the PKCE code verifier
	ReturnPath string // where to go after login; already checked by SafeReturn
	ExpiresAt  time.Time
}

// LoginStateStore keeps login states. The Postgres store and MemLoginStates
// implement it.
type LoginStateStore interface {
	// Create inserts ls. It first deletes the row named replaces (the old row of
	// the same login cookie; "" for none) and every expired row, so one cookie
	// holds one live row and the table cannot fill up (design §1.1).
	Create(ctx context.Context, ls LoginState, replaces string, now time.Time) error
	// Take deletes the row with this id and returns it, if it has not expired
	// at now. A missing, used or expired row is ErrLoginStateNotFound. The
	// delete is what makes a callback single use (design §1.2 step 2).
	Take(ctx context.Context, id string, now time.Time) (LoginState, error)
}

// NewLoginStateID returns a fresh row id / cookie value: 24 random bytes,
// base64url.
func NewLoginStateID() (string, error) { return randomString(24) }

// NewStateNonce returns a random value for the OAuth state or the OIDC nonce.
func NewStateNonce() (string, error) { return randomString(24) }

func randomString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("auth: random value: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// MemLoginStates is the in-memory LoginStateStore, for tests and --demo.
type MemLoginStates struct {
	mu   sync.Mutex
	rows map[string]LoginState
}

var _ LoginStateStore = (*MemLoginStates)(nil)

// NewMemLoginStates returns an empty MemLoginStates.
func NewMemLoginStates() *MemLoginStates {
	return &MemLoginStates{rows: map[string]LoginState{}}
}

// Create implements LoginStateStore.
func (m *MemLoginStates) Create(_ context.Context, ls LoginState, replaces string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, replaces)
	for id, r := range m.rows {
		if !now.Before(r.ExpiresAt) {
			delete(m.rows, id)
		}
	}
	if _, dup := m.rows[ls.ID]; dup {
		return errors.New("auth: duplicate login state id")
	}
	m.rows[ls.ID] = ls
	return nil
}

// Take implements LoginStateStore.
func (m *MemLoginStates) Take(_ context.Context, id string, now time.Time) (LoginState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ls, ok := m.rows[id]
	if !ok {
		return LoginState{}, ErrLoginStateNotFound
	}
	delete(m.rows, id)
	if !now.Before(ls.ExpiresAt) {
		return LoginState{}, ErrLoginStateNotFound
	}
	return ls, nil
}

// Len returns the number of stored rows (tests).
func (m *MemLoginStates) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows)
}
