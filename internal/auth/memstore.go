package auth

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// errDuplicate is returned when a session id or token hash already exists.
var errDuplicate = errors.New("auth: duplicate session id or token hash")

// MemStore is the in-memory Store, for --demo and tests. Sessions are lost on
// restart.
type MemStore struct {
	mu   sync.Mutex
	byID map[string]*Session
	byTH map[string]string // string(token hash) -> id
}

var _ Store = (*MemStore)(nil)

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{byID: map[string]*Session{}, byTH: map[string]string{}}
}

// Create implements Store.
func (m *MemStore) Create(_ context.Context, s Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byID[s.ID]; ok {
		return errDuplicate
	}
	if _, ok := m.byTH[string(s.TokenHash)]; ok {
		return errDuplicate
	}
	c := clone(s)
	m.byID[s.ID] = &c
	m.byTH[string(s.TokenHash)] = s.ID
	return nil
}

// ByTokenHash implements Store.
func (m *MemStore) ByTokenHash(_ context.Context, hash []byte) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.byTH[string(hash)]
	if !ok {
		return Session{}, ErrNotFound
	}
	return clone(*m.byID[id]), nil
}

// ByID implements Store.
func (m *MemStore) ByID(_ context.Context, id string) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.byID[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return clone(*s), nil
}

// LiveByUserID implements Store.
func (m *MemStore) LiveByUserID(_ context.Context, userID string, now time.Time, idle time.Duration) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var best *Session
	for _, s := range m.byID {
		if s.UserID != userID || s.Check(now, idle) != nil {
			continue
		}
		if best == nil || s.CreatedAt.After(best.CreatedAt) {
			best = s
		}
	}
	if best == nil {
		return Session{}, ErrNotFound
	}
	return clone(*best), nil
}

// LiveSessions implements Store.
func (m *MemStore) LiveSessions(_ context.Context, now time.Time, idle time.Duration, limit int) ([]Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newest := map[string]*Session{}
	for _, s := range m.byID {
		if s.Check(now, idle) != nil {
			continue
		}
		if cur := newest[s.UserID]; cur == nil || s.CreatedAt.After(cur.CreatedAt) {
			newest[s.UserID] = s
		}
	}
	ids := make([]string, 0, len(newest))
	for id := range newest {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]Session, 0, len(ids))
	for _, id := range ids {
		out = append(out, clone(*newest[id]))
	}
	return out, nil
}

// Touch implements Store.
func (m *MemStore) Touch(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.byID[id]; ok {
		s.LastSeenAt = at.UTC()
	}
	return nil
}

// Revoke implements Store.
func (m *MemStore) Revoke(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.byID[id]; ok && s.RevokedAt.IsZero() {
		s.RevokedAt = at.UTC()
	}
	return nil
}

func clone(s Session) Session {
	s.TokenHash = append([]byte(nil), s.TokenHash...)
	s.Groups = append([]string(nil), s.Groups...)
	s.AMR = append([]string(nil), s.AMR...)
	return s
}
