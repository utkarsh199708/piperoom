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
