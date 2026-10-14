package httpapi

import (
	"context"

	"piperoom/internal/ws"
)

// SessionAliveForTest runs the WebSocket presence tick's session check
// (sessionAlive) for one session id, so a black-box test can pin it without a
// 20 second tick.
func (s *Server) SessionAliveForTest(ctx context.Context, sessionID string) bool {
	return s.sessionAlive(ctx, ws.Identity{SessionID: sessionID})
}

// PresenceTickForTest does for the given sessions what one presence tick does
// for every session with a live connection: it asks the gateway's SessionAlive
// hook (wired to sessionAlive by New) and closes that session's connections
// when the answer is no. It returns the ids it closed.
func (s *Server) PresenceTickForTest(ctx context.Context, sessionIDs ...string) []string {
	var closed []string
	for _, id := range sessionIDs {
		if !s.gw.SessionAlive(ctx, ws.Identity{SessionID: id}) {
			s.gw.CloseSession(id)
			closed = append(closed, id)
		}
	}
	return closed
}
