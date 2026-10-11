package slackbridge

import "sync"

// Thread links one room to its Slack thread and remembers how far the room's
// event log has been mirrored, so a restart resumes instead of replaying.
type Thread struct {
	TS      string // Slack ts of the root message
	LastSeq int64  // last room event seq handled
}

// ThreadStore persists the room↔thread link.
type ThreadStore interface {
	Get(roomID string) (Thread, bool)
	Put(roomID string, t Thread)
	// RoomByThread finds the room for a Slack thread ts.
	RoomByThread(ts string) (roomID string, ok bool)
}

// MemThreads is an in-memory ThreadStore. It loses state on restart, so after
// a restart a room is mirrored again into a fresh thread; a Postgres-backed
// store (owned by the room service) is the follow-up.
type MemThreads struct {
	mu     sync.Mutex
	byRoom map[string]Thread
	byTS   map[string]string
}

// NewMemThreads returns an empty store.
func NewMemThreads() *MemThreads {
	return &MemThreads{byRoom: map[string]Thread{}, byTS: map[string]string{}}
}

// Get implements ThreadStore.
func (m *MemThreads) Get(roomID string) (Thread, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.byRoom[roomID]
	return t, ok
}

// Put implements ThreadStore.
func (m *MemThreads) Put(roomID string, t Thread) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byRoom[roomID] = t
	if t.TS != "" {
		m.byTS[t.TS] = roomID
	}
}

// RoomByThread implements ThreadStore.
func (m *MemThreads) RoomByThread(ts string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.byTS[ts]
	return r, ok
}
