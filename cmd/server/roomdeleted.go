package main

import (
	"sync"

	"piperoom/internal/events"
)

// roomDeletedHook is room.Deps.OnRoomDeleted. The room service is built
// before the HTTP handler, which owns the WebSocket gateway, so the gateway's
// close is set afterwards; until then no socket can be open and the hook does
// nothing. It does no I/O itself: the gateway's CloseRoom returns at once.
type roomDeletedHook struct {
	mu    sync.RWMutex
	close func(roomID string, deleted events.Event)
}

// set installs the function that closes a room's sockets.
func (h *roomDeletedHook) set(f func(roomID string, deleted events.Event)) {
	h.mu.Lock()
	h.close = f
	h.mu.Unlock()
}

// onDeleted matches room.Deps.OnRoomDeleted.
func (h *roomDeletedHook) onDeleted(roomID string, deleted events.Event) {
	h.mu.RLock()
	f := h.close
	h.mu.RUnlock()
	if f != nil {
		f(roomID, deleted)
	}
}
