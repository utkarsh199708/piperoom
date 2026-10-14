package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/room"
)

// Tenant visibility on the HTTP routes (docs/design/tenancy.md section 6).
// Every answer for an object outside the caller's tenants is the answer for
// an object that does not exist: 404, never 403, so ids do not leak.

// roomVisible wraps a /rooms/{id} handler: a room outside the caller's
// tenants answers 404 not_found before the handler runs, so no route (and no
// WebSocket upgrade) can tell such a room from a missing one.
func (s *Server) roomVisible(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.svc.CheckRoomVisible(r.Context(), userFrom(r.Context()), chi.URLParam(r, "id")); err != nil {
			s.writeErr(w, err)
			return
		}
		next(w, r)
	}
}

// pipelineVisible wraps a /pipelines/{pid} handler with the answer for an
// unknown pipeline.
func (s *Server) pipelineVisible(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.svc.CanSeePipeline(userFrom(r.Context()), chi.URLParam(r, "pid")) {
			writeError(w, http.StatusNotFound, "not_found", "pipeline not found")
			return
		}
		next(w, r)
	}
}

// signedAlertReply is the body of a signed alert that opened or joined a room
// while the tenant filter is on. One source secret serves every tenant, so the
// sender gets the room id and nothing of its content.
type signedAlertReply struct {
	RoomID  string `json:"room_id"`
	Created bool   `json:"created"`
}

// writeSignedAlertRoom is writeAlertRoom for the signed routes.
func (s *Server) writeSignedAlertRoom(w http.ResponseWriter, snap room.RoomSnapshot, created bool, err error) {
	if err != nil || !s.svc.TenancyOn() {
		s.writeAlertRoom(w, snap, created, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, signedAlertReply{RoomID: snap.ID, Created: created})
}
