package slackbridge

import (
	"context"
	"sync"
)

// Thread links a room to its Slack thread. LastSeq is the highest room event
// seq handled, so a restart resumes instead of replaying.
type Thread struct {
	Channel string // from the parent post's reply; replies always go here
	TS      string // ts of the parent message, kept as the string Slack sent
	LastSeq int64
}

// Threads persists room→thread links. A5 can back it with Postgres later.
type Threads interface {
	Get(ctx context.Context, roomID string) (Thread, bool, error)
	Put(ctx context.Context, roomID string, t Thread) error
}

// MemThreads is the default in-memory Threads. It forgets everything on
// restart, so the room then gets a new parent message.
type MemThreads struct {
	mu sync.Mutex
	m  map[string]Thread
}

// NewMemThreads returns an empty MemThreads.
func NewMemThreads() *MemThreads { return &MemThreads{m: map[string]Thread{}} }

// Get implements Threads.
func (t *MemThreads) Get(_ context.Context, roomID string) (Thread, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	th, ok := t.m[roomID]
	return th, ok, nil
}

// Put implements Threads.
func (t *MemThreads) Put(_ context.Context, roomID string, th Thread) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[roomID] = th
	return nil
}
